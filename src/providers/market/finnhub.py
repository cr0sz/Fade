from __future__ import annotations

import asyncio
from datetime import datetime, timezone

import aiohttp

from src.core.config import get_settings
from src.core.logging import get_logger
from src.models.market import MacroIndicator, TickerQuote
from src.providers.base import BaseProvider

logger = get_logger(__name__)


class FinnhubProvider(BaseProvider):
    provider_name = "finnhub"

    # Symbols the API key has no entitlement for (HTTP 403) — e.g. BIST (.IS)
    # on the free tier. Class-level so the skip survives across fetch cycles;
    # without it the same 10 BIST symbols 403'd every cycle (~2,900 errors/day).
    _forbidden_symbols: set[str] = set()

    def __init__(self) -> None:
        self._settings = get_settings()
        self._api_key = self._settings.FINNHUB_API_KEY
        self._base = self._settings.FINNHUB_BASE_URL

    async def fetch(self, symbols: list[str] | None = None, **kwargs) -> list[TickerQuote]:
        if not self._api_key:
            logger.warning("finnhub_key_missing")
            return []

        targets = symbols or (
            self._settings.DEFAULT_US_WATCHLIST + self._settings.DEFAULT_BIST_WATCHLIST
        )
        quotes: list[TickerQuote] = []

        async with aiohttp.ClientSession() as session:
            for symbol in targets:
                if symbol in self._forbidden_symbols:
                    continue
                await self.rate_limit_handler()
                try:
                    raw = await self._fetch_quote(session, symbol)
                    if raw and raw.get("c"):
                        quote = await self.normalize({**raw, "_symbol": symbol})
                        quotes.append(quote)
                except aiohttp.ClientResponseError as exc:
                    if exc.status == 403:
                        self._forbidden_symbols.add(symbol)
                        logger.warning(
                            "finnhub_symbol_forbidden",
                            symbol=symbol,
                            hint="API key has no entitlement for this exchange; "
                                 "symbol skipped until restart",
                        )
                    else:
                        logger.error("finnhub_fetch_error", symbol=symbol, error=str(exc))
                except Exception as exc:
                    logger.error("finnhub_fetch_error", symbol=symbol, error=str(exc))

        return quotes

    async def _fetch_quote(
        self, session: aiohttp.ClientSession, symbol: str
    ) -> dict | None:
        params = {"symbol": symbol, "token": self._api_key}
        async with session.get(f"{self._base}/quote", params=params) as resp:
            if resp.status == 429:
                await asyncio.sleep(30)
                return None
            resp.raise_for_status()
            return await resp.json()

    async def normalize(self, raw: dict) -> TickerQuote:
        price = raw.get("c", 0.0)
        prev_close = raw.get("pc", price)
        change = price - prev_close
        change_pct = (change / prev_close * 100) if prev_close else 0.0
        symbol = raw.get("_symbol", "")
        exchange = "BIST" if symbol.endswith(".IS") else "US"

        return TickerQuote(
            symbol=symbol,
            price=float(price),
            change=round(change, 4),
            change_pct=round(change_pct, 2),
            volume=float(raw.get("v", 0)),
            high_52w=raw.get("h"),
            low_52w=raw.get("l"),
            exchange=exchange,
            currency="TRY" if exchange == "BIST" else "USD",
            timestamp=datetime.now(timezone.utc),
        )

    async def fetch_macro_indicators(self) -> list[MacroIndicator]:
        """Fetch economic calendar / macro indicators."""
        if not self._api_key:
            return []

        indicators = []
        async with aiohttp.ClientSession() as session:
            params = {"token": self._api_key}
            try:
                async with session.get(
                    f"{self._base}/economic/calendar", params=params
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        for item in (data.get("economicCalendar") or [])[:20]:
                            indicators.append(MacroIndicator(
                                name=item.get("event", ""),
                                value=float(item.get("actual") or 0),
                                previous=item.get("prev"),
                                country=item.get("country", ""),
                                release_date=datetime.now(timezone.utc),
                                source="finnhub",
                            ))
            except Exception as exc:
                logger.error("finnhub_macro_error", error=str(exc))

        return indicators

    async def health_check(self) -> bool:
        if not self._api_key:
            return False
        try:
            async with aiohttp.ClientSession() as session:
                params = {"symbol": "AAPL", "token": self._api_key}
                async with session.get(f"{self._base}/quote", params=params) as resp:
                    return resp.status == 200
        except Exception:
            return False

    async def cost_estimate(self, request_count: int) -> float:
        return 0.0

    async def rate_limit_handler(self) -> None:
        await asyncio.sleep(0.1)  # 30 calls/sec
