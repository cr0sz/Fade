"""
CNBC news provider — RSS-based.

CNBC provides public RSS feeds for major topics. No API key required.

Feeds used:
  - Top News  (id=100003114)
  - Business  (id=10001147)
  - Markets   (id=15839135)
  - Technology (id=19854910)
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

CNBC_FEEDS = [
    "https://www.cnbc.com/id/100003114/device/rss/rss.html",   # Top News
    "https://www.cnbc.com/id/10001147/device/rss/rss.html",    # Business
    "https://www.cnbc.com/id/15839135/device/rss/rss.html",    # Markets
    "https://www.cnbc.com/id/19854910/device/rss/rss.html",    # Technology
]

_HTML_TAG_RE = re.compile(r"<[^>]+>")
_CDATA_RE = re.compile(r"<!\[CDATA\[(.*?)\]\]>", re.DOTALL)


def _clean_text(text: str) -> str:
    text = _CDATA_RE.sub(r"\1", text)
    return _HTML_TAG_RE.sub(" ", text).strip()


class CNBCProvider(ResilientProviderMixin, BaseProvider):
    provider_name = "cnbc"

    def __init__(self) -> None:
        self._settings = get_settings()
        self._feeds = getattr(self._settings, "CNBC_RSS_FEEDS", CNBC_FEEDS)
        self._rate_limit_delay = getattr(self._settings, "CNBC_RATE_LIMIT_DELAY", 2.0)

    async def fetch(self, limit: int = 100, **kwargs) -> list[NewsItem]:
        results: list[NewsItem] = []
        per_feed = max(10, limit // len(self._feeds))

        async with aiohttp.ClientSession(
            headers={
                "User-Agent": "Mozilla/5.0 (compatible; MarketIntelBot/0.1; RSS Reader)",
                "Accept": "application/rss+xml, application/xml, text/xml",
            },
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
                    # Feed ids live in the path (/id/<n>/), not a query
                    # string. Splitting on "id=" raised IndexError here, and
                    # because that happens after results.extend() every
                    # successful CNBC fetch was logged as cnbc_feed_error.
                    feed_id = next(
                        (seg for seg in feed_url.split("/") if seg.isdigit()), feed_url
                    )
                    logger.info("cnbc_feed_fetched", feed=feed_id, count=len(items))
                except Exception as exc:
                    logger.error("cnbc_feed_error", feed=feed_url, error=str(exc))

        logger.info("cnbc_fetched_total", count=len(results))
        return results[:limit]

    async def _fetch_feed(
        self,
        session: aiohttp.ClientSession,
        feed_url: str,
        limit: int,
    ) -> list[NewsItem]:
        async with session.get(feed_url) as resp:
            resp.raise_for_status()
            # CNBC RSS sometimes returns non-UTF8 — handle gracefully
            raw = await resp.read()

        return self._parse_rss(raw, limit)

    def _parse_rss(self, xml_bytes: bytes, limit: int) -> list[NewsItem]:
        # Try UTF-8 first, then latin-1 as fallback
        for encoding in ("utf-8", "latin-1"):
            try:
                xml_text = xml_bytes.decode(encoding, errors="replace")
                # Strip CDATA wrappers before parsing
                xml_text = _CDATA_RE.sub(lambda m: m.group(1), xml_text)
                root = ET.fromstring(xml_text)
                break
            except ET.ParseError:
                continue
        else:
            logger.error("cnbc_xml_parse_failed")
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
                logger.debug("cnbc_item_parse_error", error=str(exc))

        return items

    def _parse_item(self, entry: ET.Element) -> NewsItem | None:
        title = _clean_text(entry.findtext("title") or "").strip()
        if not title or title.lower() == "cnbc":
            return None

        url = (entry.findtext("link") or entry.findtext("guid") or "").strip()
        description = _clean_text(entry.findtext("description") or "")[:2000]
        source_domain = urlparse(url).netloc or "cnbc.com"

        pub_date_str = entry.findtext("pubDate") or ""
        published_at = datetime.now(timezone.utc)
        if pub_date_str:
            try:
                published_at = parsedate_to_datetime(pub_date_str.strip()).astimezone(timezone.utc)
            except Exception:
                pass

        content_hash = hashlib.sha256(
            f"{title}{source_domain}{published_at.date().isoformat()}".encode()
        ).hexdigest()

        return NewsItem(
            title=title,
            content=description or None,
            url=url,
            source="CNBC",
            source_domain=source_domain,
            author=entry.findtext("author"),
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
        return 0.0

    async def rate_limit_handler(self) -> None:
        await asyncio.sleep(self._rate_limit_delay)
