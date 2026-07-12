"""Turkish financial news RSS provider for BIST tickers.

Aggregates free Turkish financial news RSS feeds:
  - Bloomberg HT  (bloomberght.com) — Turkish Bloomberg affiliate
  - Haberler.com  (haberler.com)    — General Turkish news with economy section
  - Mynet Finans  (mynet.com)       — Turkish financial portal
  - Dünya Gazetesi (dunya.com)      — Turkish business/economy newspaper

No API key required. All public RSS feeds.

BIST ticker mapping: articles mentioning company names in Turkish are tagged
via the _BIST_NAME_MAP below. NewsIntelligenceEngine also runs _extract_tickers
on every article, so company names in English or ticker symbols will be caught.
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

_FEEDS: list[tuple[str, str]] = [
    ("BloombergHT",      "https://www.bloomberght.com/rss"),
    ("Haberler-Ekonomi", "https://www.haberler.com/rss/ekonomi-haberleri.asp"),
    ("Haberler-Borsa",   "https://www.haberler.com/rss/borsa-haberleri.asp"),
    ("Borsa-Gundem",     "https://www.borsagundem.com/rss"),
    ("Investing-TR",     "https://tr.investing.com/rss/news_1.rss"),
]

# Turkish company name → BIST ticker (lower-cased for matching)
_BIST_NAME_MAP: dict[str, str] = {
    "türk hava": "THYAO.IS", "türk havayolları": "THYAO.IS", "thy": "THYAO.IS",
    "garanti": "GARAN.IS", "garanti bankası": "GARAN.IS", "garanti bbva": "GARAN.IS",
    "aselsan": "ASELS.IS",
    "koç holding": "KCHOL.IS", "koç": "KCHOL.IS",
    "ereğli": "EREGL.IS", "erdemir": "EREGL.IS",
    "bim": "BIMAS.IS", "bimeks": "BIMAS.IS",
    "şişecam": "SISE.IS", "şişe cam": "SISE.IS",
    "turkcell": "TCELL.IS",
    "akbank": "AKBNK.IS",
    "yapı kredi": "YKBNK.IS", "yapıkredi": "YKBNK.IS",
    "tüpraş": "TUPRS.IS", "tüpraş": "TUPRS.IS",
    "sabancı": "SAHOL.IS", "sabancı holding": "SAHOL.IS",
    "ford otosan": "FROTO.IS",
    "tofaş": "TOASO.IS",
    "pegasus": "PGSUS.IS",
    "borsa istanbul": "BIST",
    "bist": "BIST",
}


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


def _extract_bist_tickers(text: str) -> list[str]:
    """Tag articles with BIST ticker symbols by matching Turkish company names."""
    found: set[str] = set()
    lower = text.lower()
    for name, ticker in _BIST_NAME_MAP.items():
        if name in lower and ticker != "BIST":
            found.add(ticker)
    return list(found)


class TurkishRSSProvider(ResilientProviderMixin, BaseProvider):
    provider_name = "turkish_rss"

    def __init__(self) -> None:
        self._rate_limit_delay = 1.5

    async def fetch(self, limit: int = 100, **kwargs) -> list[NewsItem]:
        results: list[NewsItem] = []
        per_feed = max(10, limit // len(_FEEDS))

        async with aiohttp.ClientSession(
            headers={
                "User-Agent": "Mozilla/5.0 (compatible; MarketIntelBot/0.1; RSS Reader)",
                "Accept": "application/rss+xml, application/xml, text/xml, */*",
            },
            timeout=aiohttp.ClientTimeout(total=15),
        ) as session:
            for feed_name, feed_url in _FEEDS:
                await asyncio.sleep(self._rate_limit_delay)
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
                    logger.debug("turkish_rss_feed_fetched", source=feed_name, count=len(items))
                except Exception as exc:
                    logger.warning("turkish_rss_feed_error", source=feed_name, error=str(exc))

        # Deduplicate within batch
        seen: set[str] = set()
        unique: list[NewsItem] = []
        for item in results:
            if item.content_hash not in seen:
                seen.add(item.content_hash)
                unique.append(item)

        logger.info("turkish_rss_fetched_total", count=len(unique))
        return unique[:limit]

    async def _fetch_feed(
        self,
        session: aiohttp.ClientSession,
        feed_name: str,
        feed_url: str,
        limit: int,
    ) -> list[NewsItem]:
        async with session.get(feed_url) as resp:
            if resp.status in (404, 403):
                logger.debug("turkish_rss_feed_unavailable", source=feed_name, status=resp.status)
                return []
            resp.raise_for_status()
            raw = await resp.read()
        return self._parse_rss(raw, feed_name, limit)

    def _parse_rss(self, xml_bytes: bytes, source_name: str, limit: int) -> list[NewsItem]:
        for encoding in ("utf-8", "windows-1254", "latin-1"):
            try:
                root = ET.fromstring(xml_bytes.decode(encoding, errors="replace"))
                break
            except ET.ParseError:
                continue
        else:
            logger.debug("turkish_rss_xml_parse_failed", source=source_name)
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

        source_domain = urlparse(url).netloc or source_name.lower() + ".com"

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

        combined_text = f"{title} {description}"
        ticker_symbols = _extract_bist_tickers(combined_text)

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
            ticker_symbols=ticker_symbols,
        )

    async def normalize(self, raw: dict) -> NewsItem | None:
        return None

    async def health_check(self) -> bool:
        try:
            async with aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=8)
            ) as session:
                async with session.get(_FEEDS[0][1]) as resp:
                    return resp.status == 200
        except Exception:
            return False

    async def cost_estimate(self, request_count: int) -> float:
        return 0.0

    async def rate_limit_handler(self) -> None:
        await asyncio.sleep(self._rate_limit_delay)
