"""CryptoPanic social score provider.

Aggregates CryptoPanic vote counts (positive - negative) per ticker and
normalizes them to [-1, 1] using tanh. Results are cached in-memory for
10 minutes to avoid redundant requests when multiple callers hit it per
cycle.
"""
from __future__ import annotations

import math
import os
import time
from collections import defaultdict

import aiohttp

from src.core.logging import get_logger

logger = get_logger(__name__)

_BASE_URL = "https://cryptopanic.com/api/free/v1/posts/"
_API_KEY = os.getenv("CRYPTOPANIC_API_KEY", "")
_CACHE_TTL = 600  # 10 minutes
_BATCH_SIZE = 9   # all live tickers in one request (≤ free-tier limit)


class CryptoPanicSocialProvider:
    """Derives a social_score per ticker from CryptoPanic vote counts."""

    def __init__(self) -> None:
        self._cache: dict[str, float] = {}
        self._cache_ts: float = 0.0

    async def get_social_scores(self, tickers: list[str]) -> dict[str, float]:
        """Return a mapping of ticker → social score in [-1, 1].

        Results are cached for _CACHE_TTL seconds. On any failure returns
        an empty dict (callers fall back to 0.0).
        """
        now = time.monotonic()
        if self._cache and (now - self._cache_ts) < _CACHE_TTL:
            # Return cached values for the requested tickers only
            return {t: self._cache[t] for t in tickers if t in self._cache}

        if not _API_KEY:
            return {}
        scores = await self._fetch_scores(tickers)
        self._cache = scores
        self._cache_ts = now
        return {t: scores[t] for t in tickers if t in scores}

    async def _fetch_scores(self, tickers: list[str]) -> dict[str, float]:
        pos: dict[str, int] = defaultdict(int)
        neg: dict[str, int] = defaultdict(int)

        # Fetch in batches of _BATCH_SIZE
        batches = [tickers[i : i + _BATCH_SIZE] for i in range(0, len(tickers), _BATCH_SIZE)]

        try:
            async with aiohttp.ClientSession(
                headers={"User-Agent": "MarketIntelBot/0.1"},
                timeout=aiohttp.ClientTimeout(total=15),
            ) as session:
                for batch in batches:
                    params = {
                        "auth_token": _API_KEY,
                        "currencies": ",".join(batch),
                        "public": "true",
                        "kind": "news",
                    }
                    async with session.get(_BASE_URL, params=params) as resp:
                        if resp.status == 429:
                            logger.warning("cryptopanic_social_rate_limited")
                            return {}
                        if resp.status != 200:
                            logger.error("cryptopanic_social_bad_status", status=resp.status)
                            return {}
                        data = await resp.json()

                    for article in data.get("results", []):
                        votes = article.get("votes") or {}
                        p = int(votes.get("positive") or 0)
                        n = int(votes.get("negative") or 0)
                        for currency in article.get("currencies") or []:
                            code = currency.get("code")
                            if code:
                                pos[code] += p
                                neg[code] += n

        except Exception as exc:
            logger.error("cryptopanic_social_fetch_error", error=str(exc))
            return {}

        scores: dict[str, float] = {}
        for ticker in tickers:
            p_total = pos.get(ticker, 0)
            n_total = neg.get(ticker, 0)
            total = p_total + n_total
            # tanh normalizes: neutral vote split → ~0, strong positive → +1
            score = math.tanh((p_total - n_total) / max(1, total) * 5)
            scores[ticker] = round(score, 4)

        logger.info(
            "cryptopanic_social_scores_computed",
            tickers=list(scores.keys()),
        )
        return scores
