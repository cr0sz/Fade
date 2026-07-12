"""Reddit RSS news provider.

Fetches hot posts from crypto-relevant subreddits via Reddit's public
Atom RSS feeds — no API key or authentication required.

Feed list:
  - r/CryptoCurrency  (general crypto, tagged BTC+ETH)
  - r/Bitcoin         (BTC)
  - r/ethfinance      (ETH)
  - r/avax            (AVAX)
  - r/defi            (AAVE, UNI, LINK)
  - r/solana          (SOL)

Reddit enforces a descriptive User-Agent and rate-limits aggressively;
a 2-second delay is inserted between feed requests.
"""
from __future__ import annotations

import asyncio
import hashlib
import re
from datetime import datetime, timezone
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

# (subreddit_name, url, default_tickers)
_FEEDS: list[tuple[str, str, list[str]]] = [
    ("r/CryptoCurrency", "https://www.reddit.com/r/CryptoCurrency/hot/.rss", ["BTC", "ETH"]),
    ("r/Bitcoin",        "https://www.reddit.com/r/Bitcoin/hot/.rss",        ["BTC"]),
    ("r/ethfinance",     "https://www.reddit.com/r/ethfinance/hot/.rss",     ["ETH"]),
    ("r/avax",           "https://www.reddit.com/r/avax/hot/.rss",           ["AVAX"]),
    ("r/defi",           "https://www.reddit.com/r/defi/hot/.rss",           ["AAVE", "UNI", "LINK"]),
    ("r/solana",         "https://www.reddit.com/r/solana/hot/.rss",         ["SOL"]),
]

_MAX_PER_FEED = 20
_RATE_LIMIT_DELAY = 2.0  # seconds between feed requests (Reddit is strict)
_USER_AGENT = "MarketIntelBot/0.1 (crypto market research)"

# Atom namespace
_NS = "{http://www.w3.org/2005/Atom}"


def _clean(text: str) -> str:
    text = _CDATA_RE.sub(r"\1", text)
    return _HTML_TAG_RE.sub(" ", text).strip()


def _find_el(entry: ET.Element, *tags: str) -> ET.Element | None:
    """Return the first Element found for any of the given tag names."""
    for tag in tags:
        el = entry.find(tag)
        if el is not None:
            return el
    return None


class RedditRSSProvider(ResilientProviderMixin, BaseProvider):
    provider_name = "reddit_rss"

    def __init__(self) -> None:
        self._feeds = _FEEDS
        self._rate_limit_delay = _RATE_LIMIT_DELAY

    async def fetch(self, limit: int = 120, **kwargs) -> list[NewsItem]:
        results: list[NewsItem] = []

        async with aiohttp.ClientSession(
            headers={
                "User-Agent": _USER_AGENT,
                "Accept": "application/atom+xml, application/xml, text/xml, */*",
            },
            timeout=aiohttp.ClientTimeout(total=15),
        ) as session:
            for subreddit_name, feed_url, default_tickers in self._feeds:
                await self.rate_limit_handler()
                try:
                    items = await self._call_with_resilience(
                        self._fetch_feed,
                        session,
                        subreddit_name,
                        feed_url,
                        default_tickers,
                        _MAX_PER_FEED,
                        operation="fetch_rss",
                    )
                    results.extend(items)
                    logger.info(
                        "reddit_rss_feed_fetched",
                        source=subreddit_name,
                        count=len(items),
                    )
                except Exception as exc:
                    logger.error(
                        "reddit_rss_feed_error",
                        source=subreddit_name,
                        error=str(exc),
                    )

        logger.info("reddit_rss_fetched_total", count=len(results))
        return results[:limit]

    async def _fetch_feed(
        self,
        session: aiohttp.ClientSession,
        subreddit_name: str,
        feed_url: str,
        default_tickers: list[str],
        limit: int,
    ) -> list[NewsItem]:
        async with session.get(feed_url) as resp:
            resp.raise_for_status()
            raw = await resp.read()
        return self._parse_atom(raw, subreddit_name, default_tickers, limit)

    def _parse_atom(
        self,
        xml_bytes: bytes,
        subreddit_name: str,
        default_tickers: list[str],
        limit: int,
    ) -> list[NewsItem]:
        for encoding in ("utf-8", "latin-1"):
            try:
                root = ET.fromstring(xml_bytes.decode(encoding, errors="replace"))
                break
            except ET.ParseError:
                continue
        else:
            logger.error("reddit_rss_xml_parse_failed", source=subreddit_name)
            return []

        # Reddit RSS is Atom format — entries live directly under root
        entries = root.findall(f"{_NS}entry")

        items: list[NewsItem] = []
        for entry in entries[:limit]:
            try:
                item = self._parse_entry(entry, subreddit_name, default_tickers)
                if item:
                    items.append(item)
            except Exception as exc:
                logger.debug(
                    "reddit_rss_entry_error", source=subreddit_name, error=str(exc)
                )

        return items

    def _parse_entry(
        self,
        entry: ET.Element,
        subreddit_name: str,
        default_tickers: list[str],
    ) -> NewsItem | None:
        # Title
        title_el = _find_el(entry, f"{_NS}title", "title")
        title = _clean(title_el.text or "") if title_el is not None else ""
        if not title:
            return None

        # Link — prefer <link rel="alternate">
        url = ""
        for link_el in entry.findall(f"{_NS}link"):
            rel = link_el.get("rel", "alternate")
            href = link_el.get("href", "")
            if rel == "alternate" and href:
                url = href
                break
        if not url:
            # Fallback: any <link> with href
            link_el = entry.find(f"{_NS}link")
            if link_el is not None:
                url = link_el.get("href", "")

        source_domain = urlparse(url).netloc or "reddit.com"

        # Content — <content type="html"> or <summary>
        content_el = _find_el(entry, f"{_NS}content", f"{_NS}summary")
        content = _clean(content_el.text or "")[:2000] if content_el is not None else ""

        # Published date — <published> then <updated>
        pub_el = _find_el(entry, f"{_NS}published", f"{_NS}updated")
        published_at = datetime.now(timezone.utc)
        if pub_el is not None and pub_el.text:
            try:
                published_at = datetime.fromisoformat(
                    pub_el.text.strip().replace("Z", "+00:00")
                ).astimezone(timezone.utc)
            except Exception:
                pass

        content_hash = hashlib.sha256(
            f"{title}{subreddit_name}{published_at.date().isoformat()}".encode()
        ).hexdigest()

        return NewsItem(
            title=title,
            content=content or None,
            url=url,
            source=subreddit_name,
            source_domain=source_domain,
            author=None,
            published_at=published_at,
            content_hash=content_hash,
            ticker_symbols=list(default_tickers),
        )

    async def normalize(self, raw: dict) -> NewsItem | None:
        return None  # parsing done inline in _parse_entry

    async def health_check(self) -> bool:
        try:
            async with aiohttp.ClientSession(
                headers={"User-Agent": _USER_AGENT},
                timeout=aiohttp.ClientTimeout(total=8),
            ) as session:
                _, url, _ = self._feeds[0]
                async with session.get(url) as resp:
                    return resp.status == 200
        except Exception:
            return False

    async def cost_estimate(self, request_count: int) -> float:
        return 0.0

    async def rate_limit_handler(self) -> None:
        await asyncio.sleep(self._rate_limit_delay)
