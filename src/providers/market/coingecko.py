"""CoinGecko crypto market data provider.

Public API — no API key required for free tier (30 calls/min).
Fetches OHLCV + market cap + 24h change for all CRYPTO-exchange tickers
whose metadata contains a 'coingecko_id' field.

Endpoint used:
  GET /coins/markets?vs_currency=usd&ids=bitcoin,ethereum,...
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any

import aiohttp

from src.core.config import get_settings
from src.core.logging import get_logger
from src.models.market import TickerQuote
from src.providers.base import BaseProvider

logger = get_logger(__name__)

_BASE_URL = "https://api.coingecko.com/api/v3"
_BATCH_SIZE = 50   # max IDs per single request (well within CoinGecko limits)
_RATE_LIMIT_DELAY = 2.5  # seconds between requests (free tier: 30 req/min)


class CoinGeckoProvider(BaseProvider):
    provider_name = "coingecko"

    def __init__(self) -> None:
        self._settings = get_settings()
        self._base = _BASE_URL

    async def fetch(self, symbols: list[str] | None = None, **kwargs) -> list[TickerQuote]:
        """Fetch crypto quotes for all CRYPTO tickers in the DB (or explicit symbols)."""
        coingecko_map = await self._load_coingecko_map()
        if not coingecko_map:
            logger.warning("coingecko_no_ids_found", hint="seed tickers with coingecko_id in metadata")
            return []

        # Filter to requested symbols if provided
        if symbols:
            coingecko_map = {k: v for k, v in coingecko_map.items() if k in symbols}

        all_quotes: list[TickerQuote] = []
        ids = list(coingecko_map.values())
        returned_ids: set[str] = set()
        # CoinGecko's own symbol field can diverge from our ticker symbol after
        # token migrations (polygon-ecosystem-token reports "pol" while our
        # ticker is MATIC) — the coingecko_id is the authoritative join key,
        # so map back to OUR symbol or quotes land under an orphan symbol.
        id_to_symbol = {cg_id: sym for sym, cg_id in coingecko_map.items()}

        # Chunk into batches
        for i in range(0, len(ids), _BATCH_SIZE):
            batch_ids = ids[i : i + _BATCH_SIZE]
            await self.rate_limit_handler()
            try:
                raw_items = await self._fetch_markets(batch_ids)
                for raw in raw_items:
                    raw_id = raw.get("id", "")
                    quote = await self.normalize(raw)
                    if quote:
                        returned_ids.add(raw_id)
                        mapped = id_to_symbol.get(raw_id)
                        if mapped and quote.symbol != mapped:
                            quote.symbol = mapped
                        all_quotes.append(quote)
            except Exception as exc:
                logger.error("coingecko_fetch_error", batch=i, error=str(exc))

        # A requested id that produced no usable quote (API returned null
        # price, dropped the id entirely, or it was renamed/delisted) vanishes
        # silently otherwise. This silent drop has killed the MATIC feed twice
        # — surface it loudly so a dead mapping is caught within one cycle.
        dropped = {
            sym: cg_id
            for sym, cg_id in coingecko_map.items()
            if cg_id not in returned_ids
        }
        if dropped:
            logger.warning(
                "coingecko_ids_dropped",
                dropped=dropped,
                hint="stale/renamed coingecko_id or null price — feed will go stale",
            )

        logger.info("coingecko_fetched", count=len(all_quotes), requested=len(ids))
        return all_quotes

    async def _load_coingecko_map(self) -> dict[str, str]:
        """Return {symbol: coingecko_id} from the tickers table."""
        from src.db.connection import get_db_pool
        try:
            pool = await get_db_pool()
            async with pool.acquire() as conn:
                rows = await conn.fetch(
                    """
                    SELECT symbol, metadata->>'coingecko_id' AS coingecko_id
                      FROM tickers
                     WHERE exchange = 'CRYPTO'
                       AND is_active = true
                       AND metadata->>'coingecko_id' IS NOT NULL
                    """
                )
            return {r["symbol"]: r["coingecko_id"] for r in rows}
        except Exception as exc:
            logger.error("coingecko_map_load_failed", error=str(exc))
            return {}

    async def _fetch_markets(self, ids: list[str]) -> list[dict]:
        params = {
            "vs_currency": "usd",
            "ids": ",".join(ids),
            "order": "market_cap_desc",
            "per_page": len(ids),
            "page": 1,
            "sparkline": "false",
            "price_change_percentage": "24h",
        }
        async with aiohttp.ClientSession(
            headers={"User-Agent": "MarketIntelBot/0.1"},
            timeout=aiohttp.ClientTimeout(total=15),
        ) as session:
            async with session.get(f"{self._base}/coins/markets", params=params) as resp:
                if resp.status == 429:
                    logger.warning("coingecko_rate_limited")
                    await asyncio.sleep(60)
                    return []
                resp.raise_for_status()
                return await resp.json()

    async def normalize(self, raw: dict[str, Any]) -> TickerQuote | None:
        try:
            cg_id = raw.get("id", "")
            symbol = raw.get("symbol", "").upper()
            price = float(raw.get("current_price") or 0.0)
            if not price:
                return None

            prev_price = price / (1 + float(raw.get("price_change_percentage_24h") or 0) / 100)
            change = price - prev_price
            change_pct = float(raw.get("price_change_percentage_24h") or 0.0)

            return TickerQuote(
                symbol=symbol,
                name=raw.get("name"),
                price=price,
                change=round(change, 6),
                change_pct=round(change_pct, 4),
                volume=float(raw.get("total_volume") or 0.0),
                market_cap=float(raw.get("market_cap") or 0.0),
                high_52w=float(raw.get("ath") or 0.0) or None,
                low_52w=float(raw.get("atl") or 0.0) or None,
                exchange="CRYPTO",
                currency="USD",
                timestamp=datetime.now(timezone.utc),
            )
        except Exception as exc:
            logger.error("coingecko_normalize_error", id=raw.get("id"), error=str(exc))
            return None

    async def health_check(self) -> bool:
        try:
            async with aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=8)
            ) as session:
                async with session.get(f"{self._base}/ping") as resp:
                    return resp.status == 200
        except Exception:
            return False

    async def cost_estimate(self, request_count: int) -> float:
        return 0.0  # CoinGecko free tier is free

    async def rate_limit_handler(self) -> None:
        await asyncio.sleep(_RATE_LIMIT_DELAY)
