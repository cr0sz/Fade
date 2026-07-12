"""Alpaca broker — US equities (NASDAQ / NYSE) via Alpaca REST API v2.

Auth:     Header-based (APCA-API-KEY-ID + APCA-API-SECRET-KEY).
Endpoint: https://paper-api.alpaca.markets  (paper)
          https://api.alpaca.markets        (live)
          Set via ALPACA_BASE_URL env var.
Orders:   Fractional market orders (notional= for buys, qty= for sells).

Env vars required:
  ALPACA_API_KEY
  ALPACA_API_SECRET
  ALPACA_BASE_URL    (default: https://paper-api.alpaca.markets)
"""
from __future__ import annotations

import os
from typing import Any

import aiohttp
import structlog

from brokers.base import AbstractBroker, OrderResult

logger = structlog.get_logger(__name__)

_PAPER_URL = "https://paper-api.alpaca.markets"
_LIVE_URL  = "https://api.alpaca.markets"

# Alpaca uses plain ticker symbols — no mapping needed for US equities
# Only crypto uses a different symbol format (not handled here)
_MIN_NOTIONAL = 1.0  # Alpaca fractional minimum is $1


class AlpacaBroker(AbstractBroker):
    broker_name = "alpaca"

    def __init__(self) -> None:
        self._api_key    = os.getenv("ALPACA_API_KEY", "").strip()
        self._api_secret = os.getenv("ALPACA_API_SECRET", "").strip()
        self._base_url   = os.getenv("ALPACA_BASE_URL", _PAPER_URL).rstrip("/")
        self._configured = bool(self._api_key and self._api_secret)

        if not self._configured:
            logger.warning("alpaca_not_configured", hint="set ALPACA_API_KEY and ALPACA_API_SECRET")
        else:
            mode = "LIVE" if self._base_url == _LIVE_URL else "PAPER"
            logger.info("alpaca_broker_init", mode=mode, base_url=self._base_url)

    # ── AbstractBroker interface ──────────────────────────────────────────────

    async def place_order(
        self,
        asset: str,
        direction: str,
        notional_usd: float,
    ) -> OrderResult:
        if not self._configured:
            return self._err(asset, direction, "alpaca not configured")

        if notional_usd < _MIN_NOTIONAL:
            return self._err(asset, direction, f"notional {notional_usd:.2f} < min {_MIN_NOTIONAL}")

        side = "buy" if direction == "long" else "sell"

        # For buys: use notional (fractional). For sells via this path: also notional.
        payload: dict[str, Any] = {
            "symbol":        asset,
            "notional":      f"{notional_usd:.2f}",
            "side":          side,
            "type":          "market",
            "time_in_force": "day",
        }

        try:
            resp = await self._post("/v2/orders", payload)
            if "code" in resp or "message" in resp and "id" not in resp:
                msg = resp.get("message", resp.get("msg", str(resp)))
                return self._err(asset, direction, f"Alpaca error: {msg}")

            order_id  = resp.get("id", "")
            filled_qty = float(resp.get("filled_qty") or 0.0)
            filled_avg = float(resp.get("filled_avg_price") or 0.0)
            notional_filled = filled_qty * filled_avg

            logger.info(
                "alpaca_order_placed",
                asset=asset, side=side,
                order_id=order_id,
                filled_qty=round(filled_qty, 6),
                filled_avg=round(filled_avg, 4),
                status=resp.get("status"),
            )
            return OrderResult(
                success=True,
                order_id=order_id,
                asset=asset,
                direction=direction,
                qty=filled_qty,
                fill_price=filled_avg,
                notional=notional_filled or notional_usd,
                fee=0.0,  # Alpaca charges no commission
            )
        except Exception as exc:
            logger.error("alpaca_order_error", asset=asset, error=str(exc))
            return self._err(asset, direction, str(exc))

    async def close_position(
        self,
        asset: str,
        qty: float,
        direction: str,
        full_exit: bool = False,  # full_exit: futures-only; ignored here
        maker: bool = False,  # maker: futures-only; ignored here
    ) -> OrderResult:
        """Close by selling exact qty (long) or buying back (short)."""
        if not self._configured:
            return self._err(asset, direction, "alpaca not configured")

        # Alpaca DELETE /v2/positions/{symbol} closes the full position cleanly
        try:
            resp = await self._delete(f"/v2/positions/{asset}")

            # 204 No Content → already flat; treat as success
            if resp is None:
                return OrderResult(
                    success=True, order_id="", asset=asset, direction=direction,
                    qty=qty, fill_price=0.0, notional=0.0, fee=0.0,
                )

            if "code" in resp and "id" not in resp:
                msg = resp.get("message", str(resp))
                return self._err(asset, direction, f"Alpaca close error: {msg}")

            order_id   = resp.get("id", "")
            filled_qty = float(resp.get("filled_qty") or qty)
            filled_avg = float(resp.get("filled_avg_price") or 0.0)

            logger.info(
                "alpaca_position_closed",
                asset=asset, direction=direction,
                order_id=order_id,
                filled_qty=round(filled_qty, 6),
                filled_avg=round(filled_avg, 4),
            )
            return OrderResult(
                success=True,
                order_id=order_id,
                asset=asset,
                direction=direction,
                qty=filled_qty,
                fill_price=filled_avg,
                notional=filled_qty * filled_avg,
                fee=0.0,
            )
        except Exception as exc:
            logger.error("alpaca_close_error", asset=asset, error=str(exc))
            return self._err(asset, direction, str(exc))

    async def get_balance(self) -> float:
        """Return buying power (cash) in USD."""
        if not self._configured:
            return 0.0
        try:
            resp = await self._get("/v2/account")
            return float(resp.get("buying_power") or resp.get("cash") or 0.0)
        except Exception as exc:
            logger.error("alpaca_balance_error", error=str(exc))
            return 0.0

    async def health_check(self) -> bool:
        if not self._configured:
            return False
        try:
            resp = await self._get("/v2/account")
            return resp.get("status") == "ACTIVE"
        except Exception:
            return False

    # ── HTTP helpers ──────────────────────────────────────────────────────────

    def _headers(self) -> dict:
        return {
            "APCA-API-KEY-ID":     self._api_key,
            "APCA-API-SECRET-KEY": self._api_secret,
            "Content-Type":        "application/json",
        }

    async def _get(self, path: str) -> dict:
        async with aiohttp.ClientSession(headers=self._headers()) as session:
            async with session.get(
                f"{self._base_url}{path}",
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                if resp.status == 204:
                    return {}
                return await resp.json()

    async def _post(self, path: str, payload: dict) -> dict:
        async with aiohttp.ClientSession(headers=self._headers()) as session:
            async with session.post(
                f"{self._base_url}{path}",
                json=payload,
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                if resp.status == 204:
                    return {}
                return await resp.json()

    async def _delete(self, path: str) -> dict | None:
        async with aiohttp.ClientSession(headers=self._headers()) as session:
            async with session.delete(
                f"{self._base_url}{path}",
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                if resp.status == 204:
                    return None
                return await resp.json()

    @staticmethod
    def _err(asset: str, direction: str, msg: str) -> OrderResult:
        logger.warning("alpaca_order_rejected", asset=asset, reason=msg)
        return OrderResult(
            success=False, order_id="", asset=asset, direction=direction,
            qty=0.0, fill_price=0.0, notional=0.0, fee=0.0, error=msg,
        )
