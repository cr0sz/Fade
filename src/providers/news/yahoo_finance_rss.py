"""Yahoo Finance + US financial news RSS provider.

Aggregates free RSS feeds covering US equities:
  - Yahoo Finance per-ticker headline feeds (top NASDAQ/NYSE tickers)
  - MarketWatch top stories
  - CNBC markets news

No API key required. All public feeds.
Articles are ingested as-is; NewsIntelligenceEngine._extract_tickers() tags
them with the relevant ticker symbols via text matching.
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

# Per-ticker Yahoo Finance RSS — NASDAQ/NYSE + BIST + crypto
_YAHOO_TICKERS = [
    "NVDA", "AMD", "AAPL", "MSFT", "GOOGL", "META",
    "TSLA", "AMZN", "NFLX", "INTC", "COIN", "MSTR",
    "QCOM", "AVGO", "CRM", "ORCL", "PYPL", "SPY", "QQQ",
    # BIST top tickers — Yahoo Finance carries English-language company news
    "THYAO.IS", "GARAN.IS", "KCHOL.IS", "AKBNK.IS", "YKBNK.IS",
    "TUPRS.IS", "SAHOL.IS", "TCELL.IS",
    # Crypto pairs — Yahoo uses {SYM}-USD; we strip -USD when tagging
    "BTC-USD", "ETH-USD", "SOL-USD", "XRP-USD", "BNB-USD",
    "AVAX-USD", "AAVE-USD", "ADA-USD", "DOGE-USD", "LINK-USD", "DOT-USD",
    "OP-USD", "MATIC-USD", "ARB-USD",
]

# Yahoo crypto pairs use {SYM}-USD — strip suffix to get the canonical DB ticker
def _canonical_ticker(ticker: str) -> str:
    return ticker.removesuffix("-USD")

# General market/tech feeds — broad coverage, ticker extraction picks up the rest
_GENERAL_FEEDS: list[tuple[str, str]] = [
    ("MarketWatch",  "https://feeds.marketwatch.com/marketwatch/topstories/"),
    ("MarketWatch-Tech", "https://feeds.marketwatch.com/marketwatch/marketpulse/"),
    ("CNBC-Markets", "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=15839135"),
    ("CNBC-Tech",    "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=19854910"),
]

_YAHOO_RSS_URL = "https://feeds.finance.yahoo.com/rss/2.0/headline?s={ticker}&region=US&lang=en-US"


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


class YahooFinanceRSSProvider(ResilientProviderMixin, BaseProvider):
    provider_name = "yahoo_rss"

    def __init__(self) -> None:
        self._rate_limit_delay = 1.0

    async def fetch(self, limit: int = 200, **kwargs) -> list[NewsItem]:
        results: list[NewsItem] = []

        async with aiohttp.ClientSession(
            headers={
                "User-Agent": "Mozilla/5.0 (compatible; MarketIntelBot/0.1; RSS Reader)",
                "Accept": "application/rss+xml, application/xml, text/xml, */*",
            },
            timeout=aiohttp.ClientTimeout(total=15),
        ) as session:
            # Per-ticker Yahoo Finance feeds
            for ticker in _YAHOO_TICKERS:
                await asyncio.sleep(self._rate_limit_delay)
                url = _YAHOO_RSS_URL.format(ticker=ticker)
                canonical = _canonical_ticker(ticker)
                try:
                    items = await self._fetch_feed(session, f"Yahoo/{canonical}", url, 10)
                    # Tag articles with the canonical ticker — feed is explicitly about it
                    for item in items:
                        if canonical not in item.ticker_symbols:
                            item.ticker_symbols = [canonical] + item.ticker_symbols
                    results.extend(items)
                except Exception as exc:
                    logger.debug("yahoo_rss_ticker_error", ticker=ticker, error=str(exc))

            # General financial feeds
            for feed_name, feed_url in _GENERAL_FEEDS:
                await asyncio.sleep(self._rate_limit_delay)
                try:
                    items = await self._call_with_resilience(
                        self._fetch_feed,
                        session,
                        feed_name,
                        feed_url,
                        20,
                        operation="fetch_rss",
                    )
                    results.extend(items)
                    logger.debug("yahoo_rss_feed_fetched", source=feed_name, count=len(items))
                except Exception as exc:
                    logger.warning("yahoo_rss_feed_error", source=feed_name, error=str(exc))

        # Deduplicate by content_hash within this batch
        seen: set[str] = set()
        unique: list[NewsItem] = []
        for item in results:
            if item.content_hash not in seen:
                seen.add(item.content_hash)
                unique.append(item)

        logger.info("yahoo_rss_fetched_total", count=len(unique))
        return unique[:limit]

    async def _fetch_feed(
        self,
        session: aiohttp.ClientSession,
        feed_name: str,
        feed_url: str,
        limit: int,
    ) -> list[NewsItem]:
        async with session.get(feed_url) as resp:
            if resp.status == 404:
                return []
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
            logger.debug("yahoo_rss_xml_parse_failed", source=source_name)
            return []

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
            except Exception:
                pass
        return items

    def _parse_item(self, entry: ET.Element, source_name: str) -> NewsItem | None:
        ns = "{http://www.w3.org/2005/Atom}"

        title_el = _find_el(entry, "title", f"{ns}title")
        title = _clean(title_el.text or "") if title_el is not None else ""
        if not title:
            return None

        link_el = _find_el(entry, "link", "guid")
        atom_link = entry.find(f"{ns}link")
        if link_el is not None:
            url = (_clean(link_el.text or "") or link_el.get("href") or "").strip()
        elif atom_link is not None:
            url = atom_link.get("href") or ""
        else:
            url = ""

        source_domain = urlparse(url).netloc or source_name.lower().replace(" ", "") + ".com"

        desc_el = _find_el(entry, "description", f"{ns}summary", f"{ns}content")
        description = _clean(desc_el.text or "")[:2000] if desc_el is not None else ""

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
            ticker_symbols=[],
        )

    async def normalize(self, raw: dict) -> NewsItem | None:
        return None

    async def health_check(self) -> bool:
        try:
            async with aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=8)
            ) as session:
                async with session.get(_GENERAL_FEEDS[0][1]) as resp:
                    return resp.status == 200
        except Exception:
            return False

    async def cost_estimate(self, request_count: int) -> float:
        return 0.0

    async def rate_limit_handler(self) -> None:
        await asyncio.sleep(self._rate_limit_delay)
