"""Crypto-specific RSS news provider.

Aggregates feeds from:
  - CoinDesk   (coindesk.com)
  - CoinTelegraph (cointelegraph.com)
  - Decrypt    (decrypt.co)
  - The Block  (theblock.co)

No API key required. All public RSS feeds.
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

from src.core.logging import get_logger
from src.models.news import NewsItem
from src.providers.base import BaseProvider
from src.providers.resilience import ResilientProviderMixin

logger = get_logger(__name__)

_HTML_TAG_RE = re.compile(r"<[^>]+>")
_CDATA_RE = re.compile(r"<!\[CDATA\[(.*?)\]\]>", re.DOTALL)

# (name, url)
_FEEDS: list[tuple[str, str]] = [
    ("CoinDesk",      "https://www.coindesk.com/arc/outboundfeeds/rss/"),
    ("CoinTelegraph", "https://cointelegraph.com/rss"),
    ("Decrypt",       "https://decrypt.co/feed"),
    ("TheBlock",      "https://www.theblock.co/rss.xml"),
]

# Crypto tickers that should always be tagged when a feed article is ingested
_CRYPTO_DEFAULT_TICKERS = ["BTC", "ETH"]


def _clean(text: str) -> str:
    text = _CDATA_RE.sub(r"\1", text)
    return _HTML_TAG_RE.sub(" ", text).strip()


def _find_el(entry: ET.Element, *tags: str) -> ET.Element | None:
    """Return first Element found for any tag name. Avoids bool(Element) pitfall."""
    for tag in tags:
        el = entry.find(tag)
        if el is not None:
            return el
    return None


class CryptoRSSProvider(ResilientProviderMixin, BaseProvider):
    provider_name = "crypto_rss"

    def __init__(self) -> None:
        self._feeds = _FEEDS
        self._rate_limit_delay = 2.0

    async def fetch(self, limit: int = 100, **kwargs) -> list[NewsItem]:
        results: list[NewsItem] = []
        per_feed = max(10, limit // len(self._feeds))

        async with aiohttp.ClientSession(
            headers={
                "User-Agent": "Mozilla/5.0 (compatible; MarketIntelBot/0.1; RSS Reader)",
                "Accept": "application/rss+xml, application/xml, text/xml, */*",
            },
            timeout=aiohttp.ClientTimeout(total=15),
        ) as session:
            for feed_name, feed_url in self._feeds:
                await self.rate_limit_handler()
                try:
                    items = await self._call_with_resilience(
                        self._fetch_feed,
                        session,
                        feed_name,
                        feed_url,
                        per_feed,
                        operation="fetch_rss",
                    )
                    results.extend(items)
                    logger.info("crypto_rss_feed_fetched", source=feed_name, count=len(items))
                except Exception as exc:
                    logger.error("crypto_rss_feed_error", source=feed_name, error=str(exc))

        logger.info("crypto_rss_fetched_total", count=len(results))
        return results[:limit]

    async def _fetch_feed(
        self,
        session: aiohttp.ClientSession,
        feed_name: str,
        feed_url: str,
        limit: int,
    ) -> list[NewsItem]:
        async with session.get(feed_url) as resp:
            resp.raise_for_status()
            raw = await resp.read()
        return self._parse_rss(raw, feed_name, limit)

    def _parse_rss(self, xml_bytes: bytes, source_name: str, limit: int) -> list[NewsItem]:
        for encoding in ("utf-8", "latin-1"):
            try:
                root = ET.fromstring(xml_bytes.decode(encoding, errors="replace"))
                break
            except ET.ParseError:
                continue
        else:
            logger.error("crypto_rss_xml_parse_failed", source=source_name)
            return []

        # Support both RSS 2.0 (<channel><item>) and Atom (<feed><entry>)
        channel = root.find("channel")
        entries = channel.findall("item") if channel is not None else root.findall(
            "{http://www.w3.org/2005/Atom}entry"
        )

        items: list[NewsItem] = []
        for entry in entries[:limit]:
            try:
                item = self._parse_item(entry, source_name)
                if item:
                    items.append(item)
            except Exception as exc:
                logger.debug("crypto_rss_item_error", source=source_name, error=str(exc))

        return items

    def _parse_item(self, entry: ET.Element, source_name: str) -> NewsItem | None:
        ns = "{http://www.w3.org/2005/Atom}"

        # Title
        title_el = _find_el(entry, "title", f"{ns}title")
        title = _clean(title_el.text or "") if title_el is not None else ""
        if not title:
            return None

        # URL
        link_el = _find_el(entry, "link", "guid")
        atom_link = entry.find(f"{ns}link")
        if link_el is not None:
            url = (_clean(link_el.text or "") or link_el.get("href") or "").strip()
        elif atom_link is not None:
            url = atom_link.get("href") or ""
        else:
            url = ""

        source_domain = urlparse(url).netloc or source_name.lower().replace(" ", "") + ".com"

        # Description / summary
        desc_el = _find_el(entry, "description", f"{ns}summary", f"{ns}content")
        description = _clean(desc_el.text or "")[:2000] if desc_el is not None else ""

        # Published date
        pub_el = _find_el(
            entry, "pubDate", "published", f"{ns}published", f"{ns}updated"
        )
        published_at = datetime.now(timezone.utc)
        if pub_el is not None and pub_el.text:
            try:
                published_at = parsedate_to_datetime(pub_el.text.strip()).astimezone(timezone.utc)
            except Exception:
                try:
                    published_at = datetime.fromisoformat(
                        pub_el.text.strip().replace("Z", "+00:00")
                    ).astimezone(timezone.utc)
                except Exception:
                    pass

        content_hash = hashlib.sha256(
            f"{title}{source_domain}{published_at.date().isoformat()}".encode()
        ).hexdigest()

        return NewsItem(
            title=title,
            content=description or None,
            url=url,
            source=source_name,
            source_domain=source_domain,
            author=None,
            published_at=published_at,
            content_hash=content_hash,
            # Crypto feeds are always about crypto — pre-tag with BTC+ETH so
            # the dedup/ticker-extraction pipeline doesn't miss them.
            ticker_symbols=list(_CRYPTO_DEFAULT_TICKERS),
        )

    async def normalize(self, raw: dict) -> NewsItem | None:
        return None  # parsing done inline in _parse_item

    async def health_check(self) -> bool:
        try:
            async with aiohttp.ClientSession(
                headers={"User-Agent": "MarketIntelBot/0.1"},
                timeout=aiohttp.ClientTimeout(total=8),
            ) as session:
                async with session.get(_FEEDS[0][1]) as resp:
                    return resp.status == 200
        except Exception:
            return False

    async def cost_estimate(self, request_count: int) -> float:
        return 0.0

    async def rate_limit_handler(self) -> None:
        await asyncio.sleep(self._rate_limit_delay)
