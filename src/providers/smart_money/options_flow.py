"""Options flow smart-money signal via yfinance.

Computes the Put/Call (P/C) ratio for each NASDAQ/NYSE ticker using
Yahoo Finance options chains (free, no API key required).

Interpretation:
  P/C < 0.5  → heavy call buying = bullish institutional positioning
  P/C > 1.5  → heavy put buying  = bearish hedging / protective puts
  0.5–1.5    → neutral / balanced

Output: SmartMoneyEvent with direction "buy" (calls dominant) or "sell" (puts dominant).
event_at  = now()
amount_usd = approximate dollar value of unusual options volume

Cached per-ticker in Redis for 4 h to avoid hammering Yahoo Finance.
Falls back gracefully when yfinance unavailable or no options data.
"""
from __future__ import annotations

import asyncio
import json as _json
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from src.core.logging import get_logger
from src.models.market import SmartMoneyEvent
from src.providers.base import BaseProvider

if TYPE_CHECKING:
    pass

logger = get_logger(__name__)

_CACHE_KEY = "options_flow:"
_CACHE_TTL = 14400   # 4 hours
_PC_BULLISH = 0.60   # P/C below this = bullish signal
_PC_BEARISH = 1.40   # P/C above this = bearish signal

# US stocks removed Sprint 77 — only CRYPTO universe now; provider is a no-op
_OPTIONS_TICKERS: list[str] = []


class OptionsFlowProvider(BaseProvider):
    provider_name = "options_flow"

    def __init__(self, redis=None) -> None:
        self._redis = redis

    async def fetch(self, symbols: list[str] | None = None, **kwargs) -> list[SmartMoneyEvent]:
        tickers = symbols or _OPTIONS_TICKERS
        events: list[SmartMoneyEvent] = []

        for ticker in tickers:
            try:
                event = await self._get_flow_event(ticker)
                if event:
                    events.append(event)
                await asyncio.sleep(0.2)   # light rate limiting
            except Exception as exc:
                logger.debug("options_flow_error", ticker=ticker, error=str(exc))

        logger.info("options_flow_fetched", count=len(events))
        return events

    async def _get_flow_event(self, ticker: str) -> SmartMoneyEvent | None:
        cache_key = f"{_CACHE_KEY}{ticker}"
        if self._redis:
            try:
                cached = await self._redis.get(cache_key)
                if cached:
                    data = _json.loads(cached)
                    return self._to_event(ticker, **data) if data else None
            except Exception:
                pass

        result = await asyncio.get_event_loop().run_in_executor(
            None, _fetch_options_sync, ticker
        )
        if result is None:
            return None

        if self._redis:
            try:
                await self._redis.setex(cache_key, _CACHE_TTL, _json.dumps(result))
            except Exception:
                pass

        return self._to_event(ticker, **result) if result else None

    @staticmethod
    def _to_event(ticker: str, pc_ratio: float, call_vol: float, put_vol: float,
                  approx_notional: float) -> SmartMoneyEvent | None:
        if pc_ratio < _PC_BULLISH:
            direction = "buy"
        elif pc_ratio > _PC_BEARISH:
            direction = "sell"
        else:
            return None  # neutral — not worth emitting

        return SmartMoneyEvent(
            event_type="options_flow",
            ticker=ticker,
            actor="options_market",
            amount_usd=approx_notional,
            direction=direction,
            source="options_flow",
            event_at=datetime.now(timezone.utc),
            raw={
                "pc_ratio": round(pc_ratio, 3),
                "call_volume": int(call_vol),
                "put_volume": int(put_vol),
            },
        )

    async def health_check(self) -> bool:
        try:
            import yfinance as yf  # noqa: F401
            return True
        except ImportError:
            return False

    async def cost_estimate(self, request_count: int) -> float:
        return 0.0

    async def normalize(self, raw: dict) -> SmartMoneyEvent | None:
        return None

    async def rate_limit_handler(self) -> None:
        await asyncio.sleep(0.2)


def _fetch_options_sync(ticker: str) -> dict | None:
    """Blocking yfinance options chain fetch."""
    try:
        import yfinance as yf
        t = yf.Ticker(ticker)
        exps = t.options
        if not exps:
            return None

        # Use nearest-expiry options for most relevant flow
        chain = t.option_chain(exps[0])
        calls = chain.calls
        puts  = chain.puts

        call_vol = float(calls["volume"].fillna(0).sum())
        put_vol  = float(puts["volume"].fillna(0).sum())
        total    = call_vol + put_vol

        if total < 100:   # too thin — ignore
            return None

        pc_ratio = put_vol / max(call_vol, 1.0)

        # Rough notional: use last price × volume × 100 (standard contract)
        call_price = float(calls["lastPrice"].fillna(0).mean())
        put_price  = float(puts["lastPrice"].fillna(0).mean())
        notional   = (call_vol * call_price + put_vol * put_price) * 100

        return {
            "pc_ratio": round(pc_ratio, 4),
            "call_vol": call_vol,
            "put_vol": put_vol,
            "approx_notional": round(notional, 2),
        }
    except Exception as exc:
        logger.debug("options_sync_error", ticker=ticker, error=str(exc))
        return None
