from __future__ import annotations

import asyncio
import html
import re
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from typing import Any

import aiohttp

from src.core.config import get_settings
from src.core.logging import get_logger
from src.models.social import SocialPost
from src.providers.base import BaseProvider

logger = get_logger(__name__)

# $TICKER and $TICKER.IS (explicit mentions)
_DOLLAR_TICKER = re.compile(r'\$([A-Z]{1,5}(?:\.IS)?)\b')

# Crypto majors: bare uppercase words (no $ needed in crypto subs)
_CRYPTO_BARE = re.compile(
    r'\b(BTC|ETH|SOL|BNB|XRP|AVAX|ADA|DOGE|DOT|LINK|MATIC|ARB|OP|UNI|AAVE|SUI|TON)\b'
)

# US/ETF tickers: whitelist prevents false positives from common words
_US_BARE = re.compile(
    r'\b(NVDA|AMD|AAPL|MSFT|GOOGL|META|TSLA|AMZN|NFLX|INTC|QCOM|AVGO|CRM|ORCL|PYPL|COIN|MSTR|SPY|QQQ|GLD|TLT|IWM|VIX)\b'
)

# BIST tickers: with or without .IS suffix
_BIST_BARE = re.compile(
    r'\b(THYAO|GARAN|ASELS|KCHOL|EREGL|BIMAS|SISE|TCELL|AKBNK|YKBNK|TUPRS|SAHOL|FROTO|TOASO|PGSUS)(?:\.IS)?\b'
)

# Crypto full-name → ticker (case-insensitive keyword lookup)
_CRYPTO_NAMES: dict[str, str] = {
    "bitcoin":      "BTC",
    "ethereum":     "ETH",
    "solana":       "SOL",
    "binance coin": "BNB",
    "ripple":       "XRP",
    "avalanche":    "AVAX",
    "cardano":      "ADA",
    "dogecoin":     "DOGE",
    "polkadot":     "DOT",
    "chainlink":    "LINK",
    "polygon":      "MATIC",
    "arbitrum":     "ARB",
    "optimism":     "OP",
    "uniswap":      "UNI",
    "aave":         "AAVE",
    "sui network":  "SUI",
    "toncoin":      "TON",
}

# Company full-name → ticker (only unambiguous multi-word names to avoid false positives)
_COMPANY_NAMES: dict[str, str] = {
    "nvidia":                 "NVDA",
    "microsoft":              "MSFT",
    "advanced micro devices": "AMD",
    "alphabet":               "GOOGL",
    "tesla motors":           "TSLA",
}

_HTML_TAG = re.compile(r'<[^>]+>')

# Reddit Atom RSS feed — works with a browser User-Agent, no credentials needed
_RSS_BASE = "https://www.reddit.com/r/{subreddit}/new.rss"
_ATOM_NS  = "http://www.w3.org/2005/Atom"
_UA       = (
    "Mozilla/5.0 (X11; Linux x86_64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)


def _extract_tickers(content: str) -> list[str]:
    tickers: set[str] = set()
    tickers.update(_DOLLAR_TICKER.findall(content))
    tickers.update(_CRYPTO_BARE.findall(content))
    tickers.update(_US_BARE.findall(content))
    for m in _BIST_BARE.findall(content):
        tickers.add(f"{m}.IS" if not m.endswith(".IS") else m)
    content_lower = content.lower()
    for keyword, ticker in _CRYPTO_NAMES.items():
        if keyword in content_lower:
            tickers.add(ticker)
    for keyword, ticker in _COMPANY_NAMES.items():
        if keyword in content_lower:
            tickers.add(ticker)
    return list(tickers)


def _strip_html(raw_html: str) -> str:
    return _HTML_TAG.sub(" ", html.unescape(raw_html))


def _parse_atom_entries(xml_text: str, subreddit: str) -> list[dict]:
    """Parse Reddit Atom RSS feed into dicts compatible with _normalize()."""
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return []

    entries = []
    for entry in root.findall(f"{{{_ATOM_NS}}}entry"):
        def _text(tag: str) -> str:
            el = entry.find(f"{{{_ATOM_NS}}}{tag}")
            return (el.text or "") if el is not None else ""

        title = _text("title")
        body  = _strip_html(_text("content"))
        raw_id = _text("id")
        post_id = raw_id.replace("t3_", "").lstrip("/")

        link_el = entry.find(f"{{{_ATOM_NS}}}link")
        url = link_el.get("href", "") if link_el is not None else ""
        permalink = url.replace("https://www.reddit.com", "")

        author_el = entry.find(f"{{{_ATOM_NS}}}author/{{{_ATOM_NS}}}name")
        author = (author_el.text or "").lstrip("/u/") if author_el is not None else ""

        updated = _text("updated")
        try:
            ts = datetime.fromisoformat(updated.replace("Z", "+00:00")).timestamp()
        except Exception:
            ts = 0.0

        entries.append({
            "id":          post_id,
            "title":       title,
            "selftext":    body,
            "author":      author,
            "permalink":   permalink,
            "subreddit":   subreddit,
            "created_utc": ts,
            "score":       0,
            "num_comments": 0,
        })
    return entries


class RedditProvider(BaseProvider):
    provider_name = "reddit"

    def __init__(self) -> None:
        self._settings = get_settings()

    async def fetch(self, limit: int = 200) -> list[SocialPost]:
        posts: list[SocialPost] = []
        per_sub = max(10, limit // len(self._settings.REDDIT_SUBREDDITS))

        async with aiohttp.ClientSession() as session:
            for sub in self._settings.REDDIT_SUBREDDITS:
                await self.rate_limit_handler()
                try:
                    raw_posts = await self._fetch_subreddit(session, sub, per_sub)
                    for raw in raw_posts:
                        post = self._normalize(raw)
                        if post:
                            posts.append(post)
                except Exception as exc:
                    logger.error("reddit_fetch_error", subreddit=sub, error=str(exc))

        logger.info("reddit_fetched", count=len(posts))
        return posts

    async def _fetch_subreddit(
        self,
        session: aiohttp.ClientSession,
        subreddit: str,
        limit: int,
    ) -> list[dict]:
        url = _RSS_BASE.format(subreddit=subreddit)
        params = {"limit": min(limit, 100)}
        headers = {"User-Agent": _UA}
        async with session.get(url, params=params, headers=headers,
                               timeout=aiohttp.ClientTimeout(total=15)) as resp:
            if resp.status == 429:
                logger.warning("reddit_rate_limited", subreddit=subreddit)
                await asyncio.sleep(60)
                return []
            if resp.status == 403:
                logger.warning("reddit_forbidden", subreddit=subreddit)
                return []
            if resp.status != 200:
                logger.warning("reddit_bad_status", subreddit=subreddit, status=resp.status)
                return []
            return _parse_atom_entries(await resp.text(), subreddit)

    def _normalize(self, raw: dict) -> SocialPost | None:
        content = f"{raw.get('title', '')} {raw.get('selftext', '')}"
        if not content.strip():
            return None

        ticker_mentions = _extract_tickers(content)
        if not ticker_mentions:
            return None  # skip posts with no recognizable tickers

        score = raw.get("score", 0)
        num_comments = raw.get("num_comments", 0)

        return SocialPost(
            id=raw.get("id", ""),
            platform="reddit",
            content=content[:2000],
            author=raw.get("author", ""),
            ticker_mentions=ticker_mentions,
            engagement_score=score + num_comments * 2.0,
            upvotes=score,
            comments=num_comments,
            url=f"https://reddit.com{raw.get('permalink', '')}",
            subreddit=raw.get("subreddit", ""),
            created_at=datetime.fromtimestamp(
                raw.get("created_utc", 0), tz=timezone.utc
            ),
            raw={},
        )

    async def normalize(self, raw: dict[str, Any]) -> SocialPost:
        post = self._normalize(raw)
        if post is None:
            raise ValueError("no recognized tickers in post")
        return post

    async def health_check(self) -> bool:
        """Always available — uses public RSS feed, no credentials required."""
        try:
            async with aiohttp.ClientSession() as session:
                url = _RSS_BASE.format(subreddit="stocks")
                headers = {"User-Agent": _UA}
                async with session.get(url, params={"limit": 1}, headers=headers,
                                       timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    return resp.status == 200
        except Exception:
            return False

    async def cost_estimate(self, request_count: int) -> float:
        return 0.0  # free

    async def rate_limit_handler(self) -> None:
        await asyncio.sleep(2.0)  # ~30 req/min recommended
