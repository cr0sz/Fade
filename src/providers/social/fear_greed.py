"""Alternative.me Fear & Greed Index social score provider.

Fetches the crypto Fear & Greed Index (0-100) from api.alternative.me
and normalises it to [-1.0, +1.0]:

    score = (value - 50) / 50

  0  (Extreme Fear)   → -1.0
  50 (Neutral)        →  0.0
  100 (Extreme Greed) → +1.0

No API key required. Results are cached for 15 minutes.
The signal is market-wide, so every CRYPTO ticker receives the same score.
On any failure an empty dict is returned (callers fall back to 0.0).
"""
from __future__ import annotations

import time

import aiohttp

from src.core.logging import get_logger

logger = get_logger(__name__)

_API_URL = "https://api.alternative.me/fng/?limit=1&format=json"
_CACHE_TTL = 900  # 15 minutes


class FearGreedProvider:
    """Returns a market-wide Fear & Greed score for all CRYPTO tickers."""

    def __init__(self) -> None:
        self._cached_score: float | None = None
        self._cache_ts: float = 0.0

    async def get_social_scores(self, tickers: list[str]) -> dict[str, float]:
        """Return ``{ticker: score}`` for every ticker in *tickers*.

        All tickers receive the same market-wide score.  Returns an empty dict
        on any failure so callers can fall back to 0.0 gracefully.
        """
        if not tickers:
            return {}

        now = time.monotonic()
        if self._cached_score is not None and (now - self._cache_ts) < _CACHE_TTL:
            return {t: self._cached_score for t in tickers}

        score = await self._fetch_score()
        if score is None:
            return {}

        self._cached_score = score
        self._cache_ts = now
        return {t: score for t in tickers}

    async def _fetch_score(self) -> float | None:
        try:
            async with aiohttp.ClientSession(
                headers={"User-Agent": "MarketIntelBot/0.1"},
                timeout=aiohttp.ClientTimeout(total=10),
            ) as session:
                async with session.get(_API_URL) as resp:
                    if resp.status != 200:
                        logger.error("fear_greed_bad_status", status=resp.status)
                        return None
                    data = await resp.json(content_type=None)

            entry = (data.get("data") or [{}])[0]
            raw_value = int(entry.get("value", 50))
            classification = entry.get("value_classification", "Unknown")

            score = round((raw_value - 50) / 50, 4)

            logger.info(
                "fear_greed_fetched",
                value=raw_value,
                classification=classification,
                score=score,
            )
            return score

        except Exception as exc:
            logger.error("fear_greed_fetch_error", error=str(exc))
            return None
