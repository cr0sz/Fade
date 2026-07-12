from __future__ import annotations

import asyncio
from datetime import datetime, timezone

import aiohttp

from src.core.config import get_settings
from src.core.logging import get_logger
from src.models.market import TickerQuote
from src.providers.base import BaseProvider

logger = get_logger(__name__)


class PolygonProvider(BaseProvider):
    provider_name = "polygon"

    def __init__(self) -> None:
        self._settings = get_settings()
        self._api_key = self._settings.POLYGON_API_KEY
        self._base = self._settings.POLYGON_BASE_URL

    async def fetch(self, symbols: list[str] | None = None, **kwargs) -> list[TickerQuote]:
        if not self._api_key:
            logger.warning("polygon_key_missing")
            return []

        targets = symbols or self._settings.DEFAULT_US_WATCHLIST
        quotes: list[TickerQuote] = []

        async with aiohttp.ClientSession() as session:
            for symbol in targets:
                await self.rate_limit_handler()
                try:
                    raw = await self._fetch_snapshot(session, symbol)
                    if raw:
                        quote = await self.normalize(raw)
                        quotes.append(quote)
                except Exception as exc:
                    logger.error("polygon_fetch_error", symbol=symbol, error=str(exc))

        return quotes

    async def _fetch_snapshot(
        self, session: aiohttp.ClientSession, symbol: str
    ) -> dict | None:
        url = f"{self._base}/v2/snapshot/locale/us/markets/stocks/tickers/{symbol}"
        params = {"apiKey": self._api_key}
        async with session.get(url, params=params) as resp:
            if resp.status == 429:
                await asyncio.sleep(60)
                return None
            if resp.status != 200:
                return None
            data = await resp.json()
            ticker_data = data.get("ticker")
            if ticker_data:
                ticker_data["_symbol"] = symbol
            return ticker_data

    async def normalize(self, raw: dict) -> TickerQuote:
        day = raw.get("day", {})
        prev_day = raw.get("prevDay", {})
        last_quote = raw.get("lastQuote", {})

        price = day.get("c") or last_quote.get("P", 0.0)
        prev_close = prev_day.get("c", price)
        change = price - prev_close
        change_pct = (change / prev_close * 100) if prev_close else 0.0

        return TickerQuote(
            symbol=raw.get("_symbol", raw.get("ticker", "")),
            price=float(price),
            change=round(change, 4),
            change_pct=round(change_pct, 2),
            volume=float(day.get("v", 0)),
            exchange="US",
            currency="USD",
            timestamp=datetime.now(timezone.utc),
        )

    async def fetch_all_tickers(self, exchange: str = "NYSE") -> list[dict]:
        if not self._api_key:
            return []

        tickers = []
        url = f"{self._base}/v3/reference/tickers"
        params = {
            "exchange": exchange,
            "active": "true",
            "limit": 1000,
            "apiKey": self._api_key,
        }

        async with aiohttp.ClientSession() as session:
            while url:
                await self.rate_limit_handler()
                async with session.get(url, params=params) as resp:
                    if resp.status != 200:
                        break
                    data = await resp.json()
                    for t in data.get("results", []):
                        tickers.append({
                            "symbol": t.get("ticker"),
                            "name": t.get("name"),
                            "exchange": exchange,
                            "sector": None,
                            "is_active": True,
                        })
                    url = data.get("next_url")
                    params = {"apiKey": self._api_key}  # next_url has params built-in

        return tickers

    async def health_check(self) -> bool:
        if not self._api_key:
            return False
        try:
            async with aiohttp.ClientSession() as session:
                url = f"{self._base}/v2/snapshot/locale/us/markets/stocks/tickers/AAPL"
                async with session.get(url, params={"apiKey": self._api_key}) as resp:
                    return resp.status == 200
        except Exception:
            return False

    async def cost_estimate(self, request_count: int) -> float:
        return 0.0  # Free tier available

    async def rate_limit_handler(self) -> None:
        # Free tier: 5 calls/min
        await asyncio.sleep(12.0)
