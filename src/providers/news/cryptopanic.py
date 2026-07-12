"""CryptoPanic news provider.

Requires a free API token from https://cryptopanic.com/developers/api/
Set CRYPTOPANIC_API_KEY in .env — without it the provider silently skips.
Rate limit: 1 request per 3 seconds (conservative for free tier, 100 req/day).
"""
from __future__ import annotations

import asyncio
import hashlib
import os
from datetime import datetime, timezone

import aiohttp

from src.core.logging import get_logger
from src.models.news import NewsItem
from src.providers.base import BaseProvider
from src.providers.resilience import ResilientProviderMixin

logger = get_logger(__name__)

LIVE_TICKERS = ["BTC", "ETH", "SOL", "BNB", "XRP", "AVAX", "AAVE", "OP", "DOGE"]
BASE_URL = "https://cryptopanic.com/api/free/v1/posts/"
_BATCH_SIZE = 5
_RATE_LIMIT_DELAY = 3.0  # seconds between requests (free tier)
_API_KEY = os.getenv("CRYPTOPANIC_API_KEY", "")


class CryptoPanicsNewsProvider(ResilientProviderMixin, BaseProvider):
    provider_name = "cryptopanic"

    def __init__(self) -> None:
        self._rate_limit_delay = _RATE_LIMIT_DELAY

    async def fetch(self, limit: int = 100, **kwargs) -> list[NewsItem]:
        if not _API_KEY:
            logger.debug("cryptopanic_skipped", reason="CRYPTOPANIC_API_KEY not set")
            return []
        tickers = kwargs.get("tickers", LIVE_TICKERS)
        results: list[NewsItem] = []

        # Batch tickers into groups of BATCH_SIZE
        batches = [tickers[i : i + _BATCH_SIZE] for i in range(0, len(tickers), _BATCH_SIZE)]

        async with aiohttp.ClientSession(
            headers={"User-Agent": "MarketIntelBot/0.1"},
            timeout=aiohttp.ClientTimeout(total=15),
        ) as session:
            for batch in batches:
                await self.rate_limit_handler()
                try:
                    items = await self._call_with_resilience(
                        self._fetch_batch,
                        session,
                        batch,
                        operation="fetch_batch",
                    )
                    results.extend(items)
                    logger.info(
                        "cryptopanic_batch_fetched",
                        tickers=batch,
                        count=len(items),
                    )
                except Exception as exc:
                    logger.error(
                        "cryptopanic_batch_error",
                        tickers=batch,
                        error=str(exc),
                    )

        logger.info("cryptopanic_fetched_total", count=len(results))
        return results[:limit]

    async def _fetch_batch(
        self,
        session: aiohttp.ClientSession,
        tickers: list[str],
    ) -> list[NewsItem]:
        params = {
            "auth_token": _API_KEY,
            "currencies": ",".join(tickers),
            "public": "true",
            "kind": "news",
        }
        async with session.get(BASE_URL, params=params) as resp:
            if resp.status == 429:
                logger.warning("cryptopanic_rate_limited", status=resp.status)
                return []
            if resp.status != 200:
                logger.error("cryptopanic_bad_status", status=resp.status)
                return []
            data = await resp.json()

        return self._parse_results(data.get("results", []))

    def _parse_results(self, results: list[dict]) -> list[NewsItem]:
        items: list[NewsItem] = []
        for r in results:
            try:
                item = self._parse_result(r)
                if item:
                    items.append(item)
            except Exception as exc:
                logger.debug("cryptopanic_item_error", error=str(exc))
        return items

    def _parse_result(self, r: dict) -> NewsItem | None:
        title = (r.get("title") or "").strip()
        if not title:
            return None

        url = (r.get("url") or "").strip()
        source = r.get("source") or {}
        source_name = (source.get("title") or "CryptoPanic").strip()
        source_domain = (source.get("domain") or "cryptopanic.com").strip()

        # Published timestamp
        pub_str = r.get("published_at") or ""
        try:
            published_at = datetime.fromisoformat(
                pub_str.replace("Z", "+00:00")
            ).astimezone(timezone.utc)
        except Exception:
            published_at = datetime.now(timezone.utc)

        # Ticker symbols from currencies field
        ticker_symbols = [
            c["code"]
            for c in (r.get("currencies") or [])
            if c.get("code")
        ]

        content_hash = hashlib.sha256(
            f"{title}{source_domain}{published_at.date().isoformat()}".encode()
        ).hexdigest()

        return NewsItem(
            title=title,
            url=url,
            source=source_name,
            source_domain=source_domain,
            published_at=published_at,
            ticker_symbols=ticker_symbols,
            content_hash=content_hash,
            sentiment_score=None,
        )

    async def normalize(self, raw: dict) -> NewsItem | None:
        return self._parse_result(raw)

    async def health_check(self) -> bool:
        if not _API_KEY:
            return False  # disabled until key set
        try:
            async with aiohttp.ClientSession(
                headers={"User-Agent": "MarketIntelBot/0.1"},
                timeout=aiohttp.ClientTimeout(total=8),
            ) as session:
                params = {"auth_token": _API_KEY, "currencies": "BTC", "public": "true", "kind": "news"}
                async with session.get(BASE_URL, params=params) as resp:
                    return resp.status == 200
        except Exception:
            return False

    async def cost_estimate(self, request_count: int = 0) -> float:
        return 0.0

    async def rate_limit_handler(self) -> None:
        await asyncio.sleep(self._rate_limit_delay)
