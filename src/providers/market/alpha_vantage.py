from __future__ import annotations

import asyncio
from datetime import datetime, timezone

import aiohttp

from src.core.config import get_settings
from src.core.logging import get_logger
from src.models.market import TickerQuote
from src.providers.base import BaseProvider

logger = get_logger(__name__)


class AlphaVantageProvider(BaseProvider):
    provider_name = "alpha_vantage"

    def __init__(self) -> None:
        self._settings = get_settings()
        self._api_key = self._settings.ALPHA_VANTAGE_API_KEY
        self._base = self._settings.ALPHA_VANTAGE_BASE_URL

    async def fetch(self, symbols: list[str] | None = None, **kwargs) -> list[TickerQuote]:
        if not self._api_key:
            logger.warning("alpha_vantage_key_missing")
            return []

        # Alpha Vantage free tier: 25 requests/day — use sparingly as fallback
        targets = (symbols or self._settings.DEFAULT_US_WATCHLIST)[:5]
        quotes: list[TickerQuote] = []

        async with aiohttp.ClientSession() as session:
            for symbol in targets:
                await self.rate_limit_handler()
                try:
                    raw = await self._fetch_global_quote(session, symbol)
                    if raw:
                        quote = await self.normalize(raw)
                        quotes.append(quote)
                except Exception as exc:
                    logger.error("alpha_vantage_error", symbol=symbol, error=str(exc))

        return quotes

    async def _fetch_global_quote(
        self, session: aiohttp.ClientSession, symbol: str
    ) -> dict | None:
        params = {
            "function": "GLOBAL_QUOTE",
            "symbol": symbol,
            "apikey": self._api_key,
        }
        async with session.get(f"{self._base}/query", params=params) as resp:
            if resp.status != 200:
                return None
            data = await resp.json()
            if "Note" in data:  # rate limit note
                logger.warning("alpha_vantage_rate_limited")
                return None
            q = data.get("Global Quote", {})
            if not q:
                return None
            q["_symbol"] = symbol
            return q

    async def normalize(self, raw: dict) -> TickerQuote:
        price = float(raw.get("05. price", 0))
        change = float(raw.get("09. change", 0))
        change_pct_str = raw.get("10. change percent", "0%").rstrip("%")
        change_pct = float(change_pct_str) if change_pct_str else 0.0
        volume = float(raw.get("06. volume", 0))

        return TickerQuote(
            symbol=raw.get("_symbol", raw.get("01. symbol", "")),
            price=price,
            change=round(change, 4),
            change_pct=round(change_pct, 2),
            volume=volume,
            exchange="US",
            currency="USD",
            timestamp=datetime.now(timezone.utc),
        )

    async def health_check(self) -> bool:
        if not self._api_key:
            return False
        try:
            async with aiohttp.ClientSession() as session:
                params = {"function": "GLOBAL_QUOTE", "symbol": "AAPL", "apikey": self._api_key}
                async with session.get(f"{self._base}/query", params=params) as resp:
                    if resp.status != 200:
                        return False
                    data = await resp.json()
                    return "Global Quote" in data and "Note" not in data
        except Exception:
            return False

    async def cost_estimate(self, request_count: int) -> float:
        return 0.0  # Free tier

    async def rate_limit_handler(self) -> None:
        await asyncio.sleep(12.0)  # 5 calls/min free tier
