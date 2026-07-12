"""Binance Spot broker — live crypto order execution.

Auth:     HMAC-SHA256 signed requests (API key + secret).
Endpoint: https://api.binance.com (live) or https://testnet.binance.vision (testnet).
Orders:   Market orders in QUOTE_ORDER_QTY mode (spend exact USDT amount).

Env vars required:
  BINANCE_API_KEY
  BINANCE_API_SECRET
  BINANCE_TESTNET=true|false   (default false — use true until ready for real money)
"""
from __future__ import annotations

import hashlib
import hmac
import os
import time
import urllib.parse
from typing import Any

import aiohttp
import structlog

from brokers.base import AbstractBroker, OrderResult

logger = structlog.get_logger(__name__)

_LIVE_URL    = "https://api.binance.com"
_TESTNET_URL = "https://testnet.binance.vision"

# Map our symbol → Binance trading pair (always quoted in USDT)
_SYMBOL_MAP: dict[str, str] = {
    "BTC":   "BTCUSDT",
    "ETH":   "ETHUSDT",
    "BNB":   "BNBUSDT",
    "SOL":   "SOLUSDT",
    "XRP":   "XRPUSDT",
    "AVAX":  "AVAXUSDT",
    "ADA":   "ADAUSDT",
    "DOGE":  "DOGEUSDT",
    "DOT":   "DOTUSDT",
    "LINK":  "LINKUSDT",
    # MATIC migrated to POL; MATICUSDT is frozen on Binance (no trades since
    # 2026-05-08) while the CoinGecko feed (and thus our quotes) carry POL prices.
    "MATIC": "POLUSDT",
    "ARB":   "ARBUSDT",
    "OP":    "OPUSDT",
    "UNI":   "UNIUSDT",
    "AAVE":  "AAVEUSDT",
    "SUI":   "SUIUSDT",
    "TON":   "TONUSDT",
    # Sprint 43 — fade-surface expansion: more liquid alts = more panic-burst
    # candidates per week. The router's fade stats are exchange-level, so
    # these inherit the measured edge immediately.
    "NEAR":  "NEARUSDT",
    "ATOM":  "ATOMUSDT",
    "LTC":   "LTCUSDT",
    "FIL":   "FILUSDT",
    "APT":   "APTUSDT",
    "INJ":   "INJUSDT",
    "ICP":   "ICPUSDT",
    "HBAR":  "HBARUSDT",
    "TRX":   "TRXUSDT",
    "BCH":   "BCHUSDT",
}

# Minimum notional per pair (Binance LOT_SIZE / MIN_NOTIONAL)
_MIN_NOTIONAL: dict[str, float] = {
    "BTCUSDT": 10.0, "ETHUSDT": 10.0, "BNBUSDT": 5.0,
    "SOLUSDT": 5.0,  "XRPUSDT": 5.0,  "AVAXUSDT": 5.0,
}
_DEFAULT_MIN_NOTIONAL = 5.0

# Per-pair LOT_SIZE step cache: pair → stepSize (fetched from exchangeInfo once).
# Module-level so all broker instances share it (brokers are constructed per call).
_STEP_SIZE_CACHE: dict[str, float] = {}


def _floor_to_step(qty: float, step: float) -> float:
    """Floor qty to the pair's LOT_SIZE step. Binance rejects any other precision
    with error -1111; a fixed 6dp format only works for pairs whose step is 1e-6."""
    if step <= 0:
        return qty
    return int(qty / step) * step


def _fee_in_usdt(fills: list[dict], avg_price: float) -> float:
    """Convert fill commissions to USDT. commissionAsset varies per account
    settings: USDT (quote), the base asset, or BNB (discount mode). Summing raw
    commission values across different assets is meaningless."""
    total = 0.0
    for f in fills:
        commission = float(f.get("commission", 0.0))
        asset = f.get("commissionAsset", "")
        if commission <= 0:
            continue
        if asset == "USDT":
            total += commission
        elif asset == "BNB":
            # No BNB price in the fill — approximate as 0 and surface in logs;
            # BNB-discount fees are ~0.075% and tracked by Binance statements.
            logger.debug("binance_bnb_fee_untracked", commission=commission)
        else:
            # Base-asset commission (typical for BUY fills): convert at fill price
            total += commission * avg_price
    return total


class BinanceBroker(AbstractBroker):
    broker_name = "binance"

    def __init__(self) -> None:
        self._api_key    = os.getenv("BINANCE_API_KEY", "").strip()
        self._api_secret = os.getenv("BINANCE_API_SECRET", "").strip()
        testnet_flag     = os.getenv("BINANCE_TESTNET", "true").strip().lower()
        self._testnet    = testnet_flag in ("1", "true", "yes")
        self._base_url   = _TESTNET_URL if self._testnet else _LIVE_URL
        self._configured = bool(self._api_key and self._api_secret)

        if not self._configured:
            logger.warning("binance_not_configured", hint="set BINANCE_API_KEY and BINANCE_API_SECRET")
        else:
            mode = "TESTNET" if self._testnet else "LIVE"
            logger.info("binance_broker_init", mode=mode)

    # ── AbstractBroker interface ──────────────────────────────────────────────

    async def place_order(
        self,
        asset: str,
        direction: str,
        notional_usd: float,
    ) -> OrderResult:
        if not self._configured:
            return self._err(asset, direction, "binance not configured")

        pair = _SYMBOL_MAP.get(asset)
        if not pair:
            return self._err(asset, direction, f"no Binance pair for {asset}")

        min_notional = _MIN_NOTIONAL.get(pair, _DEFAULT_MIN_NOTIONAL)
        if notional_usd < min_notional:
            return self._err(asset, direction, f"notional {notional_usd:.2f} < min {min_notional}")

        side = "BUY" if direction == "long" else "SELL"

        params: dict[str, Any] = {
            "symbol":          pair,
            "side":            side,
            "type":            "MARKET",
            "quoteOrderQty":   f"{notional_usd:.2f}",
            "newOrderRespType": "FULL",
        }

        try:
            resp = await self._signed_post("/api/v3/order", params)
            if "code" in resp:
                return self._err(asset, direction, f"Binance error {resp['code']}: {resp.get('msg')}")

            fills = resp.get("fills", [])
            total_qty      = sum(float(f["qty"]) for f in fills)
            total_notional = sum(float(f["qty"]) * float(f["price"]) for f in fills)
            avg_price      = total_notional / total_qty if total_qty else 0.0
            total_fee      = _fee_in_usdt(fills, avg_price)

            logger.info(
                "binance_order_filled",
                pair=pair, side=side,
                qty=round(total_qty, 6),
                avg_price=round(avg_price, 4),
                notional=round(total_notional, 2),
                fee=round(total_fee, 6),
                order_id=resp.get("orderId"),
                testnet=self._testnet,
            )
            return OrderResult(
                success=True,
                order_id=str(resp.get("orderId", "")),
                asset=asset,
                direction=direction,
                qty=total_qty,
                fill_price=avg_price,
                notional=total_notional,
                fee=total_fee,
            )
        except Exception as exc:
            logger.error("binance_order_error", asset=asset, error=str(exc))
            return self._err(asset, direction, str(exc))

    async def close_position(
        self,
        asset: str,
        qty: float,
        direction: str,
        full_exit: bool = False,  # full_exit: futures-only; ignored here
        maker: bool = False,  # maker: futures-only; ignored here
    ) -> OrderResult:
        """Close by selling (long→SELL) or buying back (short→BUY) exact qty."""
        if not self._configured:
            return self._err(asset, direction, "binance not configured")

        pair = _SYMBOL_MAP.get(asset)
        if not pair:
            return self._err(asset, direction, f"no Binance pair for {asset}")

        # Closing a long = sell; closing a short = buy
        side = "SELL" if direction == "long" else "BUY"

        try:
            # Closing a long: buy-side commission was deducted from the base
            # asset, so the wallet holds slightly less than the recorded size.
            # Selling the recorded size would be rejected with -2010
            # (insufficient balance) — clamp to the actual free balance.
            if direction == "long":
                free_base = await self._free_balance(asset)
                if 0 < free_base < qty:
                    logger.info("binance_close_qty_clamped", asset=asset,
                                requested=round(qty, 8), free=round(free_base, 8))
                    qty = free_base

            # Floor to the pair's LOT_SIZE step — any other precision is
            # rejected with -1111 (e.g. DOGEUSDT step=1, BTCUSDT step=1e-5).
            step = await self._step_size(pair)
            qty = _floor_to_step(qty, step)
            if qty <= 0:
                return self._err(asset, direction, f"qty rounds to 0 at step {step}")

            params: dict[str, Any] = {
                "symbol":           pair,
                "side":             side,
                "type":             "MARKET",
                "quantity":         f"{qty:.8f}".rstrip("0").rstrip("."),
                "newOrderRespType": "FULL",
            }

            resp = await self._signed_post("/api/v3/order", params)
            if "code" in resp:
                return self._err(asset, direction, f"Binance error {resp['code']}: {resp.get('msg')}")

            fills = resp.get("fills", [])
            total_qty      = sum(float(f["qty"]) for f in fills)
            total_notional = sum(float(f["qty"]) * float(f["price"]) for f in fills)
            avg_price      = total_notional / total_qty if total_qty else 0.0
            total_fee      = _fee_in_usdt(fills, avg_price)

            logger.info(
                "binance_close_filled",
                pair=pair, side=side,
                qty=round(total_qty, 6),
                avg_price=round(avg_price, 4),
                order_id=resp.get("orderId"),
                testnet=self._testnet,
            )
            return OrderResult(
                success=True,
                order_id=str(resp.get("orderId", "")),
                asset=asset,
                direction=direction,
                qty=total_qty,
                fill_price=avg_price,
                notional=total_notional,
                fee=total_fee,
            )
        except Exception as exc:
            logger.error("binance_close_error", asset=asset, error=str(exc))
            return self._err(asset, direction, str(exc))

    async def get_balance(self) -> float:
        """Return free USDT balance."""
        return await self._free_balance("USDT")

    async def get_all_balances(self) -> dict[str, float]:
        """asset → free balance for every non-zero spot holding (reconciliation)."""
        if not self._configured:
            return {}
        try:
            resp = await self._signed_get("/api/v3/account")
            return {b["asset"]: float(b["free"])
                    for b in resp.get("balances", []) if float(b["free"]) > 0}
        except Exception as exc:
            logger.error("binance_all_balances_error", error=str(exc))
            return {}

    async def _free_balance(self, asset: str) -> float:
        """Return the free balance of any asset (0.0 on error)."""
        if not self._configured:
            return 0.0
        try:
            resp = await self._signed_get("/api/v3/account")
            balances = {b["asset"]: float(b["free"]) for b in resp.get("balances", [])}
            return balances.get(asset, 0.0)
        except Exception as exc:
            logger.error("binance_balance_error", asset=asset, error=str(exc))
            return 0.0

    async def _step_size(self, pair: str) -> float:
        """LOT_SIZE stepSize for a pair, cached module-wide. 0.0 = unknown (no flooring)."""
        if pair in _STEP_SIZE_CACHE:
            return _STEP_SIZE_CACHE[pair]
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f"{self._base_url}/api/v3/exchangeInfo",
                    params={"symbol": pair},
                    timeout=aiohttp.ClientTimeout(total=8),
                ) as resp:
                    data = await resp.json()
            for sym in data.get("symbols", []):
                for filt in sym.get("filters", []):
                    if filt.get("filterType") == "LOT_SIZE":
                        step = float(filt.get("stepSize", 0.0))
                        _STEP_SIZE_CACHE[pair] = step
                        return step
        except Exception as exc:
            logger.warning("binance_exchange_info_error", pair=pair, error=str(exc))
        _STEP_SIZE_CACHE[pair] = 0.0
        return 0.0

    async def health_check(self) -> bool:
        if not self._configured:
            return False
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(f"{self._base_url}/api/v3/ping", timeout=aiohttp.ClientTimeout(total=5)) as r:
                    return r.status == 200
        except Exception:
            return False

    # ── Signing helpers ───────────────────────────────────────────────────────

    def _sign(self, params: dict) -> dict:
        params["timestamp"] = int(time.time() * 1000)
        query = urllib.parse.urlencode(params)
        sig = hmac.new(
            self._api_secret.encode(),
            query.encode(),
            hashlib.sha256,
        ).hexdigest()
        params["signature"] = sig
        return params

    async def _signed_post(self, path: str, params: dict) -> dict:
        signed = self._sign(params)
        headers = {"X-MBX-APIKEY": self._api_key}
        async with aiohttp.ClientSession(headers=headers) as session:
            async with session.post(
                f"{self._base_url}{path}",
                data=signed,
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                return await resp.json()

    async def _signed_get(self, path: str, params: dict | None = None) -> dict:
        p = self._sign(params or {})
        headers = {"X-MBX-APIKEY": self._api_key}
        async with aiohttp.ClientSession(headers=headers) as session:
            async with session.get(
                f"{self._base_url}{path}",
                params=p,
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                return await resp.json()

    @staticmethod
    def _err(asset: str, direction: str, msg: str) -> OrderResult:
        logger.warning("binance_order_rejected", asset=asset, reason=msg)
        return OrderResult(
            success=False, order_id="", asset=asset, direction=direction,
            qty=0.0, fill_price=0.0, notional=0.0, fee=0.0, error=msg,
        )
