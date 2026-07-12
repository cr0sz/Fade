"""
Financial Times news provider — RSS-based.

Uses FT's public RSS feeds (no API key required for headlines).
Full article content requires FT subscription, but headlines + summaries
from RSS are sufficient for market intelligence purposes.

RSS feeds used:
  - https://www.ft.com/rss/home           (front page)
  - https://www.ft.com/markets?format=rss  (markets)
  - https://www.ft.com/world/economy?format=rss (economy)
"""
from __future__ import annotations

import asyncio
import hashlib
import re
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from urllib.parse import urlparse
from xml.etree import ElementTree as ET

import aiohttp

from src.core.config import get_settings
from src.core.logging import get_logger
from src.models.news import NewsItem
from src.providers.base import BaseProvider
from src.providers.resilience import ResilientProviderMixin, latency_context

logger = get_logger(__name__)

FT_RSS_FEEDS = [
    "https://www.ft.com/rss/home",
    "https://www.ft.com/markets?format=rss",
    "https://www.ft.com/world/economy?format=rss",
]

# Namespaces used in FT RSS
NS = {
    "content": "http://purl.org/rss/1.0/modules/content/",
    "dc": "http://purl.org/dc/elements/1.1/",
    "media": "http://search.yahoo.com/mrss/",
}

_HTML_TAG_RE = re.compile(r"<[^>]+>")


def _strip_html(text: str) -> str:
    return _HTML_TAG_RE.sub(" ", text).strip()


class FTProvider(ResilientProviderMixin, BaseProvider):
    provider_name = "ft"

    def __init__(self) -> None:
        self._settings = get_settings()
        self._feeds = getattr(self._settings, "FT_RSS_FEEDS", FT_RSS_FEEDS)
        self._rate_limit_delay = getattr(self._settings, "FT_RATE_LIMIT_DELAY", 2.0)

    async def fetch(self, limit: int = 100, **kwargs) -> list[NewsItem]:
        results: list[NewsItem] = []
        per_feed = max(10, limit // len(self._feeds))

        async with aiohttp.ClientSession(
            headers={"User-Agent": "MarketIntelBot/0.1 RSS Reader"},
            timeout=aiohttp.ClientTimeout(total=15),
        ) as session:
            for feed_url in self._feeds:
                await self.rate_limit_handler()
                try:
                    items = await self._call_with_resilience(
                        self._fetch_feed,
                        session,
                        feed_url,
                        per_feed,
                        operation="fetch_rss",
                    )
                    results.extend(items)
                    logger.info(
                        "ft_feed_fetched",
                        feed=feed_url,
                        count=len(items),
                    )
                except Exception as exc:
                    logger.error("ft_feed_error", feed=feed_url, error=str(exc))

        logger.info("ft_fetched_total", count=len(results))
        return results[:limit]

    async def _fetch_feed(
        self,
        session: aiohttp.ClientSession,
        feed_url: str,
        limit: int,
    ) -> list[NewsItem]:
        async with session.get(feed_url) as resp:
            resp.raise_for_status()
            xml_bytes = await resp.read()

        return self._parse_rss(xml_bytes, limit)

    def _parse_rss(self, xml_bytes: bytes, limit: int) -> list[NewsItem]:
        items: list[NewsItem] = []
        try:
            root = ET.fromstring(xml_bytes)
        except ET.ParseError as exc:
            logger.error("ft_xml_parse_error", error=str(exc))
            return []

        channel = root.find("channel")
        if channel is None:
            return []

        for entry in channel.findall("item")[:limit]:
            try:
                item = self._parse_item(entry)
                if item:
                    items.append(item)
            except Exception as exc:
                logger.debug("ft_item_parse_error", error=str(exc))

        return items

    def _parse_item(self, entry: ET.Element) -> NewsItem | None:
        title = (entry.findtext("title") or "").strip()
        if not title:
            return None

        url = entry.findtext("link") or entry.findtext("guid") or ""
        url = url.strip()

        description = entry.findtext("description") or ""
        content = _strip_html(description)[:2000]

        source_domain = urlparse(url).netloc or "ft.com"

        pub_date_str = entry.findtext("pubDate") or ""
        published_at = datetime.now(timezone.utc)
        if pub_date_str:
            try:
                published_at = parsedate_to_datetime(pub_date_str).astimezone(timezone.utc)
            except Exception:
                pass

        content_hash = hashlib.sha256(
            f"{title}{source_domain}{published_at.date().isoformat()}".encode()
        ).hexdigest()

        author = entry.findtext(f"{{{NS['dc']}}}creator") or "FT"

        return NewsItem(
            title=title,
            content=content or None,
            url=url,
            source="Financial Times",
            source_domain=source_domain,
            author=author,
            published_at=published_at,
            content_hash=content_hash,
        )

    async def normalize(self, raw: dict) -> NewsItem | None:
        # RSS parsing is done inline in _parse_item; this satisfies the interface
        return None

    async def health_check(self) -> bool:
        try:
            async with aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=8)
            ) as session:
                async with session.get(self._feeds[0]) as resp:
                    return resp.status == 200
        except Exception:
            return False

    async def cost_estimate(self, request_count: int) -> float:
        return 0.0  # RSS is free

    async def rate_limit_handler(self) -> None:
        await asyncio.sleep(self._rate_limit_delay)
