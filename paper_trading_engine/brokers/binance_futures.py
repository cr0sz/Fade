"""Binance USDT-M Futures broker — live crypto execution with SHORT support.

Sprint 39. The system's validated edge (Truth Router OOS: fades +1.2%/trade,
short confirms +0.7%) is short-heavy, and Binance Spot cannot short. USDT-M
futures can: SELL opens a short, BUY with reduceOnly closes it.

Auth:     HMAC-SHA256 signed requests (same key scheme as spot, but the key
          must have Futures permission enabled; testnet keys come from
          https://testnet.binancefuture.com).
Endpoint: https://fapi.binance.com (live) / https://testnet.binancefuture.com (testnet).
Orders:   MARKET by base-asset quantity (futures has no quoteOrderQty) —
          notional is converted via the live mark price, floored to LOT_SIZE.
Safety:   leverage is explicitly set per symbol on first use (default 1x —
          never inherit the account's default, which can be 20x); closes are
          reduceOnly so they can never accidentally open the opposite position;
          hedge-mode accounts are detected and refused (we assume one-way).

Env vars:
  BINANCE_API_KEY / BINANCE_API_SECRET   (futures-enabled key)
  BINANCE_TESTNET=true|false             (default true)
  BINANCE_FUTURES_LEVERAGE               (default 1)
"""
from __future__ import annotations

import asyncio
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

_LIVE_URL    = "https://fapi.binance.com"
_TESTNET_URL = "https://testnet.binancefuture.com"

_TAKER_FEE = 0.0005  # 0.05% — order response carries no fee; estimated and logged
_MAKER_FEE = 0.0002  # 0.02% post-only maker fee

# Our symbol → USDT-M perpetual pair
_SYMBOL_MAP: dict[str, str] = {
    "BTC": "BTCUSDT", "ETH": "ETHUSDT", "BNB": "BNBUSDT", "SOL": "SOLUSDT",
    "XRP": "XRPUSDT", "AVAX": "AVAXUSDT", "ADA": "ADAUSDT", "DOGE": "DOGEUSDT",
    "DOT": "DOTUSDT", "LINK": "LINKUSDT", "ARB": "ARBUSDT", "OP": "OPUSDT",
    "UNI": "UNIUSDT", "AAVE": "AAVEUSDT", "SUI": "SUIUSDT", "TON": "TONUSDT",
    "MATIC": "POLUSDT",  # MATIC→POL migration; quotes under symbol MATIC are POL prices
    # Sprint 43 — fade-surface expansion (same pairs exist on USDT-M futures)
    "NEAR": "NEARUSDT", "ATOM": "ATOMUSDT", "LTC": "LTCUSDT", "FIL": "FILUSDT",
    "APT": "APTUSDT", "INJ": "INJUSDT", "ICP": "ICPUSDT", "HBAR": "HBARUSDT",
    "TRX": "TRXUSDT", "BCH": "BCHUSDT",
}

# pair → (stepSize, minNotional, tickSize) from /fapi/v1/exchangeInfo, cached module-wide
_FILTER_CACHE: dict[str, tuple[float, float, float]] = {}
_LEVERAGE_SET: set[str] = set()   # pairs whose leverage we've already configured
_DUAL_SIDE_CHECKED: list[bool] = []  # [True] once verified one-way mode


def _floor_to_step(qty: float, step: float) -> float:
    if step <= 0:
        return qty
    return int(qty / step) * step


def _fmt_qty(qty: float) -> str:
    return f"{qty:.8f}".rstrip("0").rstrip(".")


class BinanceFuturesBroker(AbstractBroker):
    broker_name = "binance_futures"

    def __init__(self) -> None:
        self._api_key    = os.getenv("BINANCE_API_KEY", "").strip()
        self._api_secret = os.getenv("BINANCE_API_SECRET", "").strip()
        testnet_flag     = os.getenv("BINANCE_TESTNET", "true").strip().lower()
        self._testnet    = testnet_flag in ("1", "true", "yes")
        self._base_url   = _TESTNET_URL if self._testnet else _LIVE_URL
        self._leverage   = max(1, int(os.getenv("BINANCE_FUTURES_LEVERAGE", "1")))
        self._configured = bool(self._api_key and self._api_secret)
        self._session: aiohttp.ClientSession | None = None

        if not self._configured:
            logger.warning("binance_futures_not_configured",
                           hint="set BINANCE_API_KEY/SECRET with Futures permission")
        else:
            logger.info("binance_futures_broker_init",
                        mode="TESTNET" if self._testnet else "LIVE",
                        leverage=self._leverage)

    # ── Session lifecycle ─────────────────────────────────────────────────────

    async def _get_session(self) -> aiohttp.ClientSession:
        """Return the shared ClientSession, creating it lazily if needed.

        The session is reused across all requests for the lifetime of this broker
        instance, eliminating repeated TLS handshakes on latency-sensitive paths
        (e.g. stop-loss exit: _mark_price + _filters + _signed_post in sequence).
        """
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                headers={"X-MBX-APIKEY": self._api_key} if self._api_key else {}
            )
        return self._session

    async def close(self) -> None:
        """Close the shared aiohttp session.  Call on engine shutdown.

        Note: BinanceFuturesBroker is currently instantiated in local scopes inside
        engine.py (not stored on self), so the engine's stop() does not call this
        automatically.  If the broker is ever promoted to an engine attribute, wire
        close() into the engine's stop() teardown.
        """
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None

    # ── AbstractBroker interface ──────────────────────────────────────────────

    async def place_order(self, asset: str, direction: str, notional_usd: float) -> OrderResult:
        if not self._configured:
            return self._err(asset, direction, "binance futures not configured")
        pair = _SYMBOL_MAP.get(asset)
        if not pair:
            return self._err(asset, direction, f"no futures pair for {asset}")

        try:
            if not await self._ensure_one_way_mode():
                return self._err(asset, direction,
                                 "account is in hedge mode — switch to one-way position mode")
            await self._ensure_leverage(pair)

            price = await self._mark_price(pair)
            if price <= 0:
                return self._err(asset, direction, f"no price for {pair}")

            step, min_notional, tick = await self._filters(pair)
            qty = _floor_to_step(notional_usd / price, step)
            if qty <= 0:
                return self._err(asset, direction, f"qty rounds to 0 at step {step}")
            if qty * price < min_notional:
                # Bump up to pair minimum instead of rejecting
                import math
                qty = _floor_to_step(
                    math.ceil(min_notional / price / step) * step if step > 0 else min_notional / price,
                    step,
                )
                if qty * price < min_notional:
                    return self._err(asset, direction,
                                     f"notional {qty * price:.2f} < pair minimum {min_notional}")

            side = "BUY" if direction == "long" else "SELL"

            # ── Accumulator state (populated by maker attempt and/or market fallback) ──
            fill_qty  = 0.0
            cum_quote = 0.0
            est_fee   = 0.0
            order_id  = ""
            avg_price = 0.0

            maker_enabled = os.getenv("MAKER_ENTRY", "true").strip().lower() == "true"
            maker_wait    = float(os.getenv("MAKER_WAIT_SEC", "12"))

            # ── Step 1: post-only LIMIT entry (maker fee 0.02%) ───────────────
            if maker_enabled:
                bt = await self._signed_get("/fapi/v1/ticker/bookTicker", {"symbol": pair})
                best_bid = float(bt.get("bidPrice", 0))
                best_ask = float(bt.get("askPrice", 0))
                if best_bid > 0 and best_ask > 0:
                    # Rest at the NBBO on our side so the order sits as maker
                    limit_price = best_ask if side == "SELL" else best_bid
                    if tick > 0:
                        limit_price = round(limit_price / tick) * tick
                    resp = await self._signed_post("/fapi/v1/order", {
                        "symbol": pair, "side": side, "type": "LIMIT",
                        "timeInForce": "GTX",   # post-only: rejected if it would take
                        "quantity": _fmt_qty(qty), "price": _fmt_qty(limit_price),
                        "newOrderRespType": "RESULT",
                    })
                    if "code" in resp and "orderId" not in resp:
                        # GTX rejected (would cross) or other error — fall through to market
                        logger.info("binance_futures_maker_rejected",
                                    pair=pair, code=resp.get("code"), msg=resp.get("msg"))
                    else:
                        order_id = str(resp.get("orderId", ""))
                        # Poll until fully filled or timeout
                        st: dict = resp
                        elapsed = 0.0
                        while elapsed < maker_wait:
                            if float(st.get("executedQty", 0)) >= qty:
                                break
                            await asyncio.sleep(1.0)
                            elapsed += 1.0
                            st = await self._signed_get("/fapi/v1/order",
                                                        {"symbol": pair, "orderId": order_id})
                        m_filled = float(st.get("executedQty", 0.0))
                        m_quote  = float(st.get("cumQuote", 0.0))
                        m_avg    = float(st.get("avgPrice", 0.0))
                        if m_filled < qty:
                            # Cancel remaining — ignore -2011 (already gone)
                            try:
                                await self._signed_delete("/fapi/v1/order",
                                                          {"symbol": pair, "orderId": order_id})
                            except Exception:
                                pass
                        maker_quote = m_quote if m_quote else (m_filled * m_avg if m_avg > 0 else 0.0)
                        fill_qty  += m_filled
                        cum_quote += maker_quote
                        est_fee   += maker_quote * _MAKER_FEE
                        logger.info("binance_futures_maker_filled",
                                    pair=pair, m_filled=m_filled, qty=qty,
                                    maker_quote=round(maker_quote, 2))
                else:
                    logger.info("binance_futures_maker_skip_no_ticker",
                                pair=pair, best_bid=best_bid, best_ask=best_ask)

            # ── Step 2: market fallback for any unfilled remainder ─────────────
            remaining = _floor_to_step(qty - fill_qty, step)
            if remaining > 0 and remaining * price >= min_notional:
                resp = await self._signed_post("/fapi/v1/order", {
                    "symbol": pair,
                    "side": side,
                    "type": "MARKET",
                    "quantity": _fmt_qty(remaining),
                    "newOrderRespType": "RESULT",
                })
                if "code" in resp and "orderId" not in resp:
                    if fill_qty > 0:
                        # Maker already partially filled — log and proceed with that
                        logger.warning("binance_futures_market_fallback_failed",
                                       pair=pair, fill_qty=fill_qty,
                                       code=resp.get("code"), msg=resp.get("msg"))
                    else:
                        return self._err(asset, direction,
                                         f"Binance error {resp['code']}: {resp.get('msg')}")
                else:
                    mk_filled = float(resp.get("executedQty", 0.0))
                    mk_quote  = float(resp.get("cumQuote", 0.0))
                    mk_avg    = (float(resp.get("avgPrice", 0.0))
                                 or (mk_quote / mk_filled if mk_filled else 0.0)
                                 or price)  # avgPrice=0 fallback to pre-order mark price
                    if not mk_quote and mk_filled and mk_avg:
                        mk_quote = mk_filled * mk_avg
                    fill_qty  += mk_filled
                    cum_quote += mk_quote
                    est_fee   += mk_quote * _TAKER_FEE
                    order_id  = order_id or str(resp.get("orderId", ""))

            if fill_qty <= 0:
                return self._err(asset, direction, "no fill (maker+market)")
            avg_price = (cum_quote / fill_qty) if fill_qty else price

            logger.info("binance_futures_order_filled",
                        pair=pair, side=side, qty=fill_qty,
                        avg_price=round(avg_price, 6),
                        notional=round(cum_quote, 2),
                        est_fee=round(est_fee, 6),
                        order_id=order_id, testnet=self._testnet)

            # Sprint 57: place a resting STOP_MARKET (reduceOnly) in live mode only.
            # Failure must NOT block the fill — log and continue.
            stop_order_id: str | None = None
            if os.getenv("TRADING_MODE", "paper").strip().lower() == "live" and avg_price > 0:
                try:
                    stop_pct = float(os.getenv("CRYPTO_STOP_LOSS_PCT", os.getenv("STOP_LOSS_PCT", "8.0"))) / 100.0
                    if direction == "long":
                        stop_price = avg_price * (1 - stop_pct)
                        stop_side = "SELL"
                    else:
                        stop_price = avg_price * (1 + stop_pct)
                        stop_side = "BUY"
                    stop_order_id = await self.place_stop_market_order(
                        pair, fill_qty, stop_side, stop_price
                    )
                except Exception as stop_exc:
                    logger.warning("binance_futures_stop_placement_failed",
                                   asset=asset, error=str(stop_exc))

            return OrderResult(
                success=True, order_id=order_id,
                asset=asset, direction=direction, qty=fill_qty,
                fill_price=avg_price, notional=cum_quote, fee=est_fee,
                stop_order_id=stop_order_id,
            )
        except Exception as exc:
            logger.error("binance_futures_order_error", asset=asset, error=str(exc))
            return self._err(asset, direction, str(exc))

    async def close_position(
        self,
        asset: str,
        qty: float,
        direction: str,
        full_exit: bool = False,
        maker: bool = False,
    ) -> OrderResult:
        """Close with a reduceOnly order (long→SELL, short→BUY).

        full_exit=True overrides qty with the actual exchange position size so
        the entire position is flattened with no dust remnant on the exchange.

        maker=True (take-profit + soft time-exit only): rest a post-only GTX LIMIT
        first to pay the 0.02% maker fee instead of 0.05% taker, then MARKET the
        unfilled remainder so the caller's tranche still fully closes within
        MAKER_EXIT_WAIT_SEC. OK for TP partials and max_hold age-out (no price
        urgency — position closes regardless). NEVER pass maker=True for a hard
        stop / trail / emergency exit — those must fill immediately at market.
        """
        if not self._configured:
            return self._err(asset, direction, "binance futures not configured")
        pair = _SYMBOL_MAP.get(asset)
        if not pair:
            return self._err(asset, direction, f"no futures pair for {asset}")

        try:
            # Fetch actual exchange size once; reused for full_exit override and
            # the existing partial-close clamp so we never call _position_qty twice.
            pos_qty = await self._position_qty(pair)
            if full_exit and pos_qty > 0:
                # Use the real exchange qty to avoid leaving a dust remnant.
                logger.info("binance_futures_close_full_exit", pair=pair,
                            engine_qty=qty, exchange_qty=pos_qty)
                qty = pos_qty
            elif pos_qty > 0 and pos_qty < qty:
                # Clamp to actual open position size: partial exits and fee
                # rounding can leave the recorded size slightly off the exchange's.
                logger.info("binance_futures_close_clamped", pair=pair,
                            requested=qty, actual=pos_qty)
                qty = pos_qty

            step, _, tick = await self._filters(pair)
            qty = _floor_to_step(qty, step)
            if qty <= 0:
                return self._err(asset, direction, f"qty rounds to 0 at step {step}")

            side = "SELL" if direction == "long" else "BUY"

            # Accumulators across the maker attempt and the market remainder.
            fill_qty  = 0.0
            cum_quote = 0.0
            est_fee   = 0.0
            order_id  = ""

            # ── Step 1: post-only LIMIT close (maker fee 0.02%) — TP exits only ──
            maker_enabled = (maker
                             and os.getenv("MAKER_EXIT", "true").strip().lower() == "true")
            if maker_enabled:
                maker_wait = float(os.getenv("MAKER_EXIT_WAIT_SEC", "10"))
                bt = await self._signed_get("/fapi/v1/ticker/bookTicker", {"symbol": pair})
                best_bid = float(bt.get("bidPrice", 0))
                best_ask = float(bt.get("askPrice", 0))
                if best_bid > 0 and best_ask > 0:
                    # Rest on our side of the NBBO so the order sits as maker.
                    limit_price = best_ask if side == "SELL" else best_bid
                    if tick > 0:
                        limit_price = round(limit_price / tick) * tick
                    resp = await self._signed_post("/fapi/v1/order", {
                        "symbol": pair, "side": side, "type": "LIMIT",
                        "timeInForce": "GTX",   # post-only: rejected if it would take
                        "quantity": _fmt_qty(qty), "price": _fmt_qty(limit_price),
                        "reduceOnly": "true", "newOrderRespType": "RESULT",
                    })
                    if "code" in resp and "orderId" not in resp:
                        logger.info("binance_futures_maker_close_rejected",
                                    pair=pair, code=resp.get("code"), msg=resp.get("msg"))
                    else:
                        order_id = str(resp.get("orderId", ""))
                        st: dict = resp
                        elapsed = 0.0
                        while elapsed < maker_wait:
                            if float(st.get("executedQty", 0)) >= qty:
                                break
                            await asyncio.sleep(1.0)
                            elapsed += 1.0
                            st = await self._signed_get("/fapi/v1/order",
                                                        {"symbol": pair, "orderId": order_id})
                        m_filled = float(st.get("executedQty", 0.0))
                        m_quote  = float(st.get("cumQuote", 0.0))
                        m_avg    = float(st.get("avgPrice", 0.0))
                        if m_filled < qty:
                            try:  # cancel remainder — ignore -2011 (already gone)
                                await self._signed_delete("/fapi/v1/order",
                                                          {"symbol": pair, "orderId": order_id})
                            except Exception:
                                pass
                        # A LIMIT fills at a known price: if Binance returns
                        # cumQuote=0 AND avgPrice=0, fall back to limit_price
                        # (exact for maker) so a real fill is never booked at $0.
                        m_px = m_avg if m_avg > 0 else limit_price
                        maker_quote = m_quote if m_quote else (m_filled * m_px if m_filled else 0.0)
                        fill_qty  += m_filled
                        cum_quote += maker_quote
                        est_fee   += maker_quote * _MAKER_FEE
                        logger.info("binance_futures_maker_close_filled",
                                    pair=pair, m_filled=m_filled, qty=qty,
                                    maker_quote=round(maker_quote, 2))
                else:
                    logger.info("binance_futures_maker_close_skip_no_ticker", pair=pair)

            # ── Step 2: MARKET reduceOnly for any unfilled remainder (taker) ────
            remaining = _floor_to_step(qty - fill_qty, step)
            if remaining > 0:
                resp = await self._signed_post("/fapi/v1/order", {
                    "symbol": pair,
                    "side": side,
                    "type": "MARKET",
                    "quantity": _fmt_qty(remaining),
                    "reduceOnly": "true",
                    "newOrderRespType": "RESULT",
                })
                if "code" in resp and "orderId" not in resp:
                    if fill_qty > 0:
                        logger.warning("binance_futures_close_market_remainder_failed",
                                       pair=pair, fill_qty=fill_qty,
                                       code=resp.get("code"), msg=resp.get("msg"))
                    else:
                        return self._err(asset, direction,
                                         f"Binance error {resp['code']}: {resp.get('msg')}")
                else:
                    mk_filled = float(resp.get("executedQty", 0.0))
                    mk_quote  = float(resp.get("cumQuote", 0.0))
                    _close_price = await self._mark_price(pair)
                    mk_avg    = (float(resp.get("avgPrice", 0.0))
                                 or (mk_quote / mk_filled if mk_filled else 0.0)
                                 or _close_price)  # Binance API may return avgPrice=0
                    if not mk_quote and mk_filled and mk_avg:
                        mk_quote = mk_filled * mk_avg
                    fill_qty  += mk_filled
                    cum_quote += mk_quote
                    est_fee   += mk_quote * _TAKER_FEE
                    order_id   = order_id or str(resp.get("orderId", ""))

            if fill_qty <= 0:
                # Binance accepted nothing (or filled nothing) — fail so the engine
                # retries rather than silently marking the DB position closed.
                return self._err(asset, direction, "close filled nothing (maker+market)")
            avg_price = (cum_quote / fill_qty) if cum_quote else 0.0
            # F2: never book a $0 fill — it persists garbage PnL.
            if avg_price <= 0:
                return self._err(asset, direction,
                                 f"close avgPrice unresolved (executedQty={fill_qty})")

            logger.info("binance_futures_close_filled",
                        pair=pair, side=side, qty=fill_qty,
                        avg_price=round(avg_price, 6), est_fee=round(est_fee, 6),
                        maker=maker_enabled, order_id=order_id, testnet=self._testnet)
            return OrderResult(
                success=True, order_id=order_id,
                asset=asset, direction=direction, qty=fill_qty,
                fill_price=avg_price, notional=cum_quote, fee=est_fee,
            )
        except Exception as exc:
            logger.error("binance_futures_close_error", asset=asset, error=str(exc))
            return self._err(asset, direction, str(exc))

    async def get_balance(self) -> float:
        """Total USDT wallet balance (realized P&L included, unrealized excluded)."""
        if not self._configured:
            return 0.0
        try:
            resp = await self._signed_get("/fapi/v2/balance")
            if isinstance(resp, dict):  # error payload
                logger.error("binance_futures_balance_error", resp=str(resp)[:200])
                return 0.0
            for b in resp:
                if b.get("asset") == "USDT":
                    # ponytail: use `balance` (total wallet) not `availableBalance`
                    # so daily-loss tracking reflects realized losses, not margin deployment
                    return float(b.get("balance", b.get("availableBalance", 0.0)))
        except Exception as exc:
            logger.error("binance_futures_balance_error", error=str(exc))
        return 0.0

    async def get_available_balance(self) -> float:
        """Free USDT (availableBalance) — funds NOT locked as position margin.

        Used to SIZE new orders so open-position margin can't be double-spent.
        get_balance() (total wallet) stays the daily-loss source.
        """
        if not self._configured:
            return 0.0
        try:
            resp = await self._signed_get("/fapi/v2/balance")
            if isinstance(resp, dict):  # error payload
                logger.error("binance_futures_avail_balance_error", resp=str(resp)[:200])
                return 0.0
            for b in resp:
                if b.get("asset") == "USDT":
                    return float(b.get("availableBalance", b.get("balance", 0.0)))
        except Exception as exc:
            logger.error("binance_futures_avail_balance_error", error=str(exc))
        return 0.0

    async def health_check(self) -> bool:
        try:
            session = await self._get_session()
            async with session.get(f"{self._base_url}/fapi/v1/ping",
                                   timeout=aiohttp.ClientTimeout(total=5)) as r:
                return r.status == 200
        except Exception:
            return False

    # ── Futures-specific helpers ──────────────────────────────────────────────

    async def _ensure_one_way_mode(self) -> bool:
        """One-way position mode is assumed (plain BUY/SELL + reduceOnly).
        Hedge mode would require positionSide on every order — refuse instead."""
        if _DUAL_SIDE_CHECKED:
            return _DUAL_SIDE_CHECKED[0]
        try:
            resp = await self._signed_get("/fapi/v1/positionSide/dual")
            one_way = not bool(resp.get("dualSidePosition", False))
            _DUAL_SIDE_CHECKED.append(one_way)
            if not one_way:
                logger.error("binance_futures_hedge_mode_detected",
                             hint="switch account to one-way position mode")
            return one_way
        except Exception as exc:
            logger.warning("binance_futures_position_mode_check_failed", error=str(exc))
            return True  # fail-open: the order itself will error if mode mismatches

    async def _ensure_leverage(self, pair: str) -> None:
        """Pin ISOLATED margin + explicit leverage — never trade on account defaults.

        Raises RuntimeError if leverage cannot be confirmed so place_order aborts
        rather than trading at an unknown (potentially 20x) account-default leverage.
        """
        if pair in _LEVERAGE_SET:
            return
        # Set ISOLATED margin first so each position has its own margin bucket.
        # Error -4046 = already isolated (safe to ignore). Error -4059 = open position
        # (also safe to ignore — the existing position already has a margin mode set).
        _EXPECTED_MARGIN_CODES = ("-4046", "-4048", "-4059")
        try:
            await self._signed_post("/fapi/v1/marginType", {
                "symbol": pair, "marginType": "ISOLATED",
            })
            logger.info("binance_futures_margin_isolated", pair=pair)
        except Exception as exc:
            _emsg = str(exc)
            if not any(code in _emsg for code in _EXPECTED_MARGIN_CODES):
                # Unexpected error — abort; don't risk trading at cross-margin
                raise RuntimeError(f"cannot set ISOLATED margin for {pair}: {_emsg}")
            # -4046 = already isolated. -4048/-4059 = existing position — verify
            # the position is actually ISOLATED before proceeding.
            if any(code in _emsg for code in ("-4048", "-4059")):
                try:
                    risk = await self._signed_get("/fapi/v2/positionRisk", {"symbol": pair})
                    margin_type = next(
                        (p.get("marginType", "").upper()
                         for p in (risk if isinstance(risk, list) else [])
                         if p.get("symbol") == pair),
                        None,
                    )
                    if margin_type and margin_type != "ISOLATED":
                        raise RuntimeError(
                            f"cannot set ISOLATED margin for {pair}: "
                            f"position already open in {margin_type} mode")
                    logger.info("binance_futures_margin_verified",
                                pair=pair, margin_type=margin_type or "unknown")
                except RuntimeError:
                    raise
                except Exception as verify_exc:
                    logger.warning("binance_futures_margin_verify_failed",
                                   pair=pair, error=str(verify_exc))
        try:
            resp = await self._signed_post("/fapi/v1/leverage", {
                "symbol": pair, "leverage": self._leverage,
            })
            logger.info("binance_futures_leverage_set", pair=pair,
                        leverage=resp.get("leverage", self._leverage))
        except Exception as exc:
            # Leverage not confirmed — abort; don't risk trading at unknown leverage
            raise RuntimeError(f"cannot set {self._leverage}x leverage for {pair}: {exc}")
        # Only mark as configured after both margin and leverage succeed
        _LEVERAGE_SET.add(pair)

    async def _mark_price(self, pair: str) -> float:
        try:
            session = await self._get_session()
            async with session.get(f"{self._base_url}/fapi/v1/ticker/price",
                                   params={"symbol": pair},
                                   timeout=aiohttp.ClientTimeout(total=8)) as r:
                data = await r.json()
            return float(data.get("price", 0.0))
        except Exception as exc:
            logger.warning("binance_futures_price_error", pair=pair, error=str(exc))
            return 0.0

    async def _position_qty(self, pair: str) -> float:
        """Absolute open position size for pair (0.0 if flat or on error)."""
        try:
            resp = await self._signed_get("/fapi/v2/positionRisk", {"symbol": pair})
            if isinstance(resp, list):
                for p in resp:
                    amt = abs(float(p.get("positionAmt", 0.0)))
                    if amt > 0:
                        return amt
        except Exception as exc:
            logger.warning("binance_futures_position_error", pair=pair, error=str(exc))
        return 0.0

    async def fetch_funding_income(self, start_ms: int) -> list[dict]:
        """FUNDING_FEE income records from Binance since start_ms (up to 1000 rows)."""
        try:
            resp = await self._signed_get("/fapi/v1/income", {
                "incomeType": "FUNDING_FEE",
                "startTime": start_ms,
                "limit": 1000,
            })
            return resp if isinstance(resp, list) else []
        except Exception as exc:
            logger.warning("binance_futures_funding_income_error", error=str(exc))
            return []

    async def _filters(self, pair: str) -> tuple[float, float, float]:
        """(stepSize, minNotional, tickSize) for pair from futures exchangeInfo, cached."""
        if pair in _FILTER_CACHE:
            return _FILTER_CACHE[pair]
        step, min_notional, tick = 0.0, 5.0, 0.0
        try:
            session = await self._get_session()
            async with session.get(f"{self._base_url}/fapi/v1/exchangeInfo",
                                   timeout=aiohttp.ClientTimeout(total=10)) as r:
                data = await r.json()
            for sym in data.get("symbols", []):
                if sym.get("symbol") != pair:
                    continue
                for filt in sym.get("filters", []):
                    if filt.get("filterType") == "LOT_SIZE":
                        step = float(filt.get("stepSize", 0.0))
                    elif filt.get("filterType") == "MIN_NOTIONAL":
                        min_notional = float(filt.get("notional", 5.0))
                    elif filt.get("filterType") == "PRICE_FILTER":
                        tick = float(filt.get("tickSize", 0.0))
            _FILTER_CACHE[pair] = (step, min_notional, tick)
        except Exception as exc:
            logger.warning("binance_futures_exchange_info_error", pair=pair, error=str(exc))
            # ponytail: don't cache on error — next call will retry rather than using bad values
        return step, min_notional, tick

    # ── Sprint 57: resting stop helpers ──────────────────────────────────────

    async def get_mark_price(self, asset: str) -> float | None:
        """Return current mark price for asset, or None on failure."""
        pair = _SYMBOL_MAP.get(asset)
        if not pair:
            return None
        try:
            return await self._mark_price(pair)
        except Exception:
            return None

    async def place_stop_for_asset(self, asset: str, direction: str, entry_price: float, qty: float = 0.0) -> str | None:
        """Re-place resting stop after partial exit. Failure is non-fatal."""
        pair = _SYMBOL_MAP.get(asset)
        if not pair:
            return None
        stop_pct = float(os.getenv("CRYPTO_STOP_LOSS_PCT", os.getenv("STOP_LOSS_PCT", "8.0"))) / 100.0
        if direction == "long":
            stop_price = entry_price * (1 - stop_pct)
            side = "SELL"
        else:
            stop_price = entry_price * (1 + stop_pct)
            side = "BUY"
        return await self.place_stop_market_order(pair, qty, side, stop_price)

    async def place_stop_market_order(
        self, pair: str, qty: float, side: str, stop_price: float
    ) -> str | None:
        """Place a reduceOnly STOP_MARKET order. Returns exchange order ID or None on failure."""
        try:
            step, _, tick = await self._filters(pair)
            qty = _floor_to_step(qty, step)
            if qty <= 0:
                logger.warning("binance_futures_stop_qty_zero", pair=pair)
                return None
            if tick > 0:
                stop_price = round(stop_price / tick) * tick
            resp = await self._signed_post("/fapi/v1/order", {
                "symbol": pair,
                "side": side,
                "type": "STOP_MARKET",
                "stopPrice": _fmt_qty(stop_price),
                "quantity": _fmt_qty(qty),
                "reduceOnly": "true",
            })
            if "code" in resp and "orderId" not in resp:
                if resp.get("code") == -4120:
                    logger.warning("stop_endpoint_unsupported_software_fallback", pair=pair)
                    return None
                logger.warning("binance_futures_stop_order_rejected",
                               pair=pair, code=resp.get("code"), msg=resp.get("msg"))
                return None
            order_id = str(resp.get("orderId", ""))
            logger.info("binance_futures_stop_placed",
                        pair=pair, side=side,
                        stop_price=round(stop_price, 6),
                        order_id=order_id)
            return order_id or None
        except Exception as exc:
            logger.warning("binance_futures_stop_placement_error", pair=pair, error=str(exc))
            return None

    async def cancel_stop_order(self, asset: str, stop_order_id: str) -> bool:
        """Cancel a resting stop order by ID. Returns True on success, logs warning on failure."""
        pair = _SYMBOL_MAP.get(asset)
        if not pair:
            logger.warning("binance_futures_cancel_stop_no_pair", asset=asset)
            return False
        try:
            resp = await self._signed_delete("/fapi/v1/order", {
                "symbol": pair,
                "orderId": stop_order_id,
            })
            if "code" in resp and resp.get("code") not in (200, -2011):
                # -2011 = unknown order (already filled/cancelled) — treat as success
                logger.warning("binance_futures_cancel_stop_failed",
                               asset=asset, order_id=stop_order_id,
                               code=resp.get("code"), msg=resp.get("msg"))
                return False
            logger.info("binance_futures_stop_cancelled",
                        asset=asset, order_id=stop_order_id)
            return True
        except Exception as exc:
            logger.warning("binance_futures_cancel_stop_error",
                           asset=asset, order_id=stop_order_id, error=str(exc))
            return False

    # ── Signing (identical scheme to spot) ────────────────────────────────────

    def _sign(self, params: dict) -> dict:
        params["timestamp"] = int(time.time() * 1000)
        params["recvWindow"] = 5000
        query = urllib.parse.urlencode(params)
        params["signature"] = hmac.new(
            self._api_secret.encode(), query.encode(), hashlib.sha256
        ).hexdigest()
        return params

    async def _signed_post(self, path: str, params: dict) -> dict:
        signed = self._sign(dict(params))
        session = await self._get_session()
        async with session.post(f"{self._base_url}{path}", data=signed,
                                timeout=aiohttp.ClientTimeout(total=10)) as resp:
            return await resp.json()

    async def _signed_get(self, path: str, params: dict | None = None):
        p = self._sign(dict(params or {}))
        session = await self._get_session()
        async with session.get(f"{self._base_url}{path}", params=p,
                               timeout=aiohttp.ClientTimeout(total=10)) as resp:
            return await resp.json()

    async def _signed_delete(self, path: str, params: dict) -> dict:
        """DELETE /fapi/v1/order — used for cancelling resting stop orders."""
        signed = self._sign(dict(params))
        session = await self._get_session()
        async with session.delete(f"{self._base_url}{path}", params=signed,
                                  timeout=aiohttp.ClientTimeout(total=10)) as resp:
            return await resp.json()

    @staticmethod
    def _err(asset: str, direction: str, msg: str) -> OrderResult:
        logger.warning("binance_futures_order_rejected", asset=asset, reason=msg)
        return OrderResult(
            success=False, order_id="", asset=asset, direction=direction,
            qty=0.0, fill_price=0.0, notional=0.0, fee=0.0, error=msg,
        )
