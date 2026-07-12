from __future__ import annotations

import asyncio
import hashlib
from datetime import datetime, timezone
from urllib.parse import urlparse

import aiohttp

from src.core.config import get_settings
from src.core.logging import get_logger
from src.models.news import NewsItem
from src.providers.base import BaseProvider

logger = get_logger(__name__)


class NewsAPIProvider(BaseProvider):
    provider_name = "newsapi"

    def __init__(self) -> None:
        self._settings = get_settings()
        self._api_key = self._settings.NEWSAPI_KEY
        self._base_url = self._settings.NEWSAPI_BASE_URL
        self._call_count = 0

    async def fetch(self, query: str = "stock market", limit: int = 100) -> list[NewsItem]:
        if not self._api_key:
            logger.warning("newsapi_key_missing")
            return []

        results: list[NewsItem] = []
        queries = [
            query,
            "Borsa Istanbul BIST stocks",
            "US equities earnings",
            "Federal Reserve interest rates",
        ]

        async with aiohttp.ClientSession() as session:
            for q in queries:
                await self.rate_limit_handler()
                try:
                    raw_items = await self._fetch_raw(session, q, limit // len(queries))
                    for raw in raw_items:
                        item = await self.normalize(raw)
                        if item:
                            results.append(item)
                except Exception as exc:
                    logger.error("newsapi_fetch_error", query=q, error=str(exc))

        logger.info("newsapi_fetched", count=len(results))
        return results

    async def _fetch_raw(
        self, session: aiohttp.ClientSession, query: str, limit: int
    ) -> list[dict]:
        params = {
            "q": query,
            "language": "en",
            "sortBy": "publishedAt",
            "pageSize": min(limit, 100),
            "apiKey": self._api_key,
        }
        async with session.get(f"{self._base_url}/everything", params=params) as resp:
            if resp.status == 429:
                logger.warning("newsapi_rate_limited")
                await asyncio.sleep(60)
                return []
            resp.raise_for_status()
            data = await resp.json()
            self._call_count += 1
            return data.get("articles", [])

    async def normalize(self, raw: dict) -> NewsItem | None:
        if not raw.get("title") or raw.get("title") == "[Removed]":
            return None

        url = raw.get("url", "")
        source_domain = urlparse(url).netloc

        content_hash = hashlib.sha256(
            f"{raw.get('title', '')}{source_domain}{raw.get('publishedAt', '')}".encode()
        ).hexdigest()

        published_at = datetime.now(timezone.utc)
        if raw.get("publishedAt"):
            try:
                published_at = datetime.fromisoformat(
                    raw["publishedAt"].replace("Z", "+00:00")
                ).astimezone(timezone.utc)
            except (ValueError, AttributeError):
                pass

        return NewsItem(
            title=raw.get("title", ""),
            content=raw.get("content") or raw.get("description"),
            url=url,
            source=raw.get("source", {}).get("name", ""),
            source_domain=source_domain,
            author=raw.get("author"),
            published_at=published_at,
            image_url=raw.get("urlToImage"),
            content_hash=content_hash,
            raw=raw,
        )

    async def health_check(self) -> bool:
        if not self._api_key:
            return False
        try:
            async with aiohttp.ClientSession() as session:
                params = {"q": "test", "pageSize": 1, "apiKey": self._api_key}
                async with session.get(
                    f"{self._base_url}/everything", params=params
                ) as resp:
                    return resp.status == 200
        except Exception:
            return False

    async def cost_estimate(self, request_count: int) -> float:
        return 0.0  # NewsAPI free tier is free

    async def rate_limit_handler(self) -> None:
        # Free tier: 100 requests/day — we throttle to 1 req/sec to be safe
        await asyncio.sleep(1.0)
