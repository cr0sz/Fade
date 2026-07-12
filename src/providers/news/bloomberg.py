"""
Bloomberg News Provider — RSS adapter (replaces stub).

Bloomberg provides public RSS feeds for market and technology news.
No enterprise API key required for this feed-based approach.

For enterprise Bloomberg API (B-PIPE / BLPAPI), replace this module
following runbook: .claude/runbooks/add_news_source.md

RSS feeds used:
  - https://feeds.bloomberg.com/markets/news.rss      (markets)
  - https://feeds.bloomberg.com/technology/news.rss   (tech)
  - https://feeds.bloomberg.com/politics/news.rss     (politics/macro)
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
from src.providers.resilience import ResilientProviderMixin

logger = get_logger(__name__)

BLOOMBERG_RSS_FEEDS = [
    "https://feeds.bloomberg.com/markets/news.rss",
    "https://feeds.bloomberg.com/technology/news.rss",
    "https://feeds.bloomberg.com/politics/news.rss",
]

_HTML_TAG_RE = re.compile(r"<[^>]+>")
NS = {
    "media": "http://search.yahoo.com/mrss/",
    "dc": "http://purl.org/dc/elements/1.1/",
}


def _strip_html(text: str) -> str:
    return _HTML_TAG_RE.sub(" ", text).strip()


class BloombergProvider(ResilientProviderMixin, BaseProvider):
    provider_name = "bloomberg"

    def __init__(self) -> None:
        self._settings = get_settings()
        self._feeds = getattr(self._settings, "BLOOMBERG_RSS_FEEDS", BLOOMBERG_RSS_FEEDS)
        # Enterprise API key — if set, future implementation can use it
        self._api_key = getattr(self._settings, "BLOOMBERG_API_KEY", "")
        self._rate_limit_delay = getattr(self._settings, "BLOOMBERG_RATE_LIMIT_DELAY", 2.0)

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
                        max_attempts=2,
                    )
                    results.extend(items)
                    logger.info(
                        "bloomberg_feed_fetched",
                        feed=feed_url.split("/")[-2],
                        count=len(items),
                    )
                except Exception as exc:
                    logger.error("bloomberg_feed_error", feed=feed_url, error=str(exc))

        logger.info("bloomberg_fetched_total", count=len(results))
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
        try:
            root = ET.fromstring(xml_bytes.decode("utf-8", errors="replace"))
        except ET.ParseError as exc:
            logger.error("bloomberg_xml_parse_error", error=str(exc))
            return []

        channel = root.find("channel")
        if channel is None:
            return []

        items: list[NewsItem] = []
        for entry in channel.findall("item")[:limit]:
            try:
                item = self._parse_item(entry)
                if item:
                    items.append(item)
            except Exception as exc:
                logger.debug("bloomberg_item_parse_error", error=str(exc))

        return items

    def _parse_item(self, entry: ET.Element) -> NewsItem | None:
        title = _strip_html(entry.findtext("title") or "").strip()
        if not title:
            return None

        url = (entry.findtext("link") or entry.findtext("guid") or "").strip()
        description = _strip_html(entry.findtext("description") or "")[:2000]
        source_domain = urlparse(url).netloc or "bloomberg.com"

        pub_date_str = entry.findtext("pubDate") or ""
        published_at = datetime.now(timezone.utc)
        if pub_date_str:
            try:
                published_at = parsedate_to_datetime(pub_date_str.strip()).astimezone(timezone.utc)
            except Exception:
                pass

        author = entry.findtext(f"{{{NS['dc']}}}creator") or "Bloomberg"

        content_hash = hashlib.sha256(
            f"{title}{source_domain}{published_at.date().isoformat()}".encode()
        ).hexdigest()

        return NewsItem(
            title=title,
            content=description or None,
            url=url,
            source="Bloomberg",
            source_domain=source_domain,
            author=author,
            published_at=published_at,
            content_hash=content_hash,
        )

    async def normalize(self, raw: dict) -> NewsItem | None:
        return None

    async def health_check(self) -> bool:
        try:
            async with aiohttp.ClientSession(
                headers={"User-Agent": "MarketIntelBot/0.1"},
                timeout=aiohttp.ClientTimeout(total=8),
            ) as session:
                async with session.get(self._feeds[0]) as resp:
                    return resp.status == 200
        except Exception:
            return False

    async def cost_estimate(self, request_count: int) -> float:
        return 0.0  # RSS is free

    async def rate_limit_handler(self) -> None:
        await asyncio.sleep(self._rate_limit_delay)
