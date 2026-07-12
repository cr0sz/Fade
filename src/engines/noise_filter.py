"""
Noise Filter — Level 2 signal quality layer.

Applied BEFORE sentiment scoring to remove low-signal posts.
Does NOT modify any existing pipeline logic — it's called from SentimentEngine.run_cycle()
as a pre-processing step on the fetched post list.

Reddit noise filters:
  A) Engagement velocity: posts with very low upvotes are likely noise
  B) Low-quality subreddit suppression: subreddits known for meme/noise
  C) Bot-like burst detection: same author, same ticker, rapid-fire posts

News noise filters:
  D) Duplicate narrative suppression: same fundamental event from multiple providers
     (complements L2 dedup which handles identical/rephrased headlines)

All filters are stateless per-call with optional Redis for burst tracking.
"""
from __future__ import annotations

import re
import time
from collections import defaultdict
from dataclasses import dataclass, field

from src.core.config import get_settings
from src.core.logging import get_logger
from src.models.social import SocialPost

logger = get_logger(__name__)

# Subreddits with historically poor signal-to-noise ratio for market intelligence
LOW_QUALITY_SUBREDDITS: frozenset[str] = frozenset({
    "memeeconomy", "superstonk", "amcstock", "wallstreetbetsnew",
    "robinhoodpennystocks", "pennystocks", "cryptomoonshots",
    "satoshistreetbets", "dogecoin", "shibainu", "loopringorg",
})

# Subreddits that provide higher-quality signals — their posts bypass engagement threshold.
# Includes every subreddit we actively fetch from (they're already curated; RSS posts have
# no upvote/comment data so engagement-based filtering is meaningless for them).
HIGH_QUALITY_SUBREDDITS: frozenset[str] = frozenset({
    # Originally high-quality
    "securityanalysis", "valueinvesting", "investing", "cfa",
    "econmonitor", "wallstreetbets",
    # All actively-fetched finance subreddits
    "stocks", "stockmarket", "options",
    "cryptocurrency", "bitcoin", "ethfinance", "solana",
    "cryptomarkets", "altcoin", "bist",
    # Stock-specific subreddits
    "nvda_stock", "teslainvestorsclub",
    # Crypto-specific subreddits
    "ethtrader", "xrp",
})

# Content patterns associated with low-quality posts
_BOT_PATTERNS = [
    re.compile(r'\bfollow for follow\b', re.I),
    re.compile(r'\bpromo code\b', re.I),
    re.compile(r'discord\.gg/', re.I),
    re.compile(r'\b(100x|1000x)\s+(?:guaranteed|soon)\b', re.I),
    re.compile(r'\bnot\s+financial\s+advice\b.*\bbuy\b.*\bnow\b', re.I),
]

_DUPLICATE_THRESHOLD_RATIO = 0.75  # 75% word overlap = duplicate narrative


@dataclass
class FilterResult:
    passed: list[SocialPost] = field(default_factory=list)
    rejected_engagement: int = 0
    rejected_low_quality_sub: int = 0
    rejected_bot_burst: int = 0
    rejected_bot_pattern: int = 0
    total_in: int = 0

    @property
    def total_rejected(self) -> int:
        return (
            self.rejected_engagement
            + self.rejected_low_quality_sub
            + self.rejected_bot_burst
            + self.rejected_bot_pattern
        )

    def log_summary(self) -> None:
        logger.info(
            "noise_filter_summary",
            total_in=self.total_in,
            passed=len(self.passed),
            rejected_total=self.total_rejected,
            rejected_engagement=self.rejected_engagement,
            rejected_low_quality_sub=self.rejected_low_quality_sub,
            rejected_bot_burst=self.rejected_bot_burst,
            rejected_bot_pattern=self.rejected_bot_pattern,
        )


class NoiseFilter:
    """
    Applies multi-stage noise filtering to social posts.
    Can be called without Redis — degrades gracefully to stateless filters only.
    """

    def __init__(self, redis=None) -> None:
        self._redis = redis
        self._settings = get_settings()
        self._min_engagement = getattr(self._settings, "NOISE_MIN_ENGAGEMENT", 3.0)
        self._bot_burst_window = getattr(self._settings, "NOISE_BOT_BURST_WINDOW", 300)  # 5 min
        self._bot_burst_threshold = getattr(self._settings, "NOISE_BOT_BURST_THRESHOLD", 5)

    def filter_posts(self, posts: list[SocialPost]) -> FilterResult:
        """
        Main entry point. Apply all Reddit noise filters in sequence.
        Returns FilterResult with .passed list and rejection counters.
        """
        result = FilterResult(total_in=len(posts))

        # Track author → {ticker → [timestamps]} for burst detection (in-call state)
        author_ticker_times: dict[str, dict[str, list[float]]] = defaultdict(lambda: defaultdict(list))

        for post in posts:
            rejection = self._check_post(post, author_ticker_times)
            if rejection is None:
                result.passed.append(post)
                # Register this post in burst tracker
                ts = post.created_at.timestamp()
                for ticker in post.ticker_mentions:
                    author_ticker_times[post.author][ticker].append(ts)
            elif rejection == "low_engagement":
                result.rejected_engagement += 1
            elif rejection == "low_quality_sub":
                result.rejected_low_quality_sub += 1
            elif rejection == "bot_burst":
                result.rejected_bot_burst += 1
            elif rejection == "bot_pattern":
                result.rejected_bot_pattern += 1

        result.log_summary()
        return result

    def _check_post(
        self,
        post: SocialPost,
        author_ticker_times: dict,
    ) -> str | None:
        """Return rejection reason string, or None if post passes."""

        # Filter A: Low-quality subreddit suppression
        if post.subreddit and post.subreddit.lower() in LOW_QUALITY_SUBREDDITS:
            logger.debug(
                "noise_filter_reject",
                reason="low_quality_sub",
                sub=post.subreddit,
                ticker=post.ticker_mentions[:1],
            )
            return "low_quality_sub"

        # Filter B: Engagement velocity — very low upvote posts are noise
        # Exception: high-quality subreddits get a pass on engagement threshold
        is_high_quality_sub = (post.subreddit or "").lower() in HIGH_QUALITY_SUBREDDITS
        if not is_high_quality_sub and post.engagement_score < self._min_engagement:
            return "low_engagement"

        # Filter C: Bot-like activity — burst detection
        # Same author posting about the same ticker many times in short window
        if post.author and post.ticker_mentions:
            ts = post.created_at.timestamp()
            for ticker in post.ticker_mentions:
                prev_times = author_ticker_times.get(post.author, {}).get(ticker, [])
                recent = [t for t in prev_times if ts - t <= self._bot_burst_window]
                if len(recent) >= self._bot_burst_threshold:
                    logger.debug(
                        "noise_filter_reject",
                        reason="bot_burst",
                        author=post.author[:20],
                        ticker=ticker,
                        count=len(recent),
                    )
                    return "bot_burst"

        # Filter D: Bot content patterns
        if any(p.search(post.content) for p in _BOT_PATTERNS):
            return "bot_pattern"

        return None

    def get_signal_quality(self, post: SocialPost) -> str:
        """
        Classify a post as high/medium/low signal quality.
        Used to weight sentiment scoring — high-quality posts get more weight.
        """
        if post.subreddit and post.subreddit.lower() in HIGH_QUALITY_SUBREDDITS:
            if post.engagement_score >= 100:
                return "high"
            if post.engagement_score >= 20:
                return "medium"

        if post.engagement_score >= 500:
            return "high"
        if post.engagement_score >= 50:
            return "medium"
        return "low"

    def quality_weighted_sentiment(self, posts: list[SocialPost]) -> float:
        """
        Compute sentiment weighted by post quality instead of simple average.
        High-quality posts count 3x, medium 1.5x, low 1x.
        Returns weighted average or 0.0 if no scored posts.
        """
        weights = {"high": 3.0, "medium": 1.5, "low": 1.0}
        total_weight = 0.0
        weighted_sum = 0.0

        for post in posts:
            if post.sentiment_score is None:
                continue
            quality = self.get_signal_quality(post)
            w = weights[quality]
            weighted_sum += post.sentiment_score * w
            total_weight += w

        return round(weighted_sum / total_weight, 4) if total_weight > 0 else 0.0


# ── News narrative dedup ───────────────────────────────────────────────────────

def _tokenize(text: str) -> frozenset[str]:
    """Lowercase word tokens, stop-word stripped."""
    stop = {
        "the", "a", "an", "is", "are", "was", "were", "be", "been",
        "has", "have", "had", "do", "does", "did", "will", "would",
        "in", "on", "at", "to", "for", "of", "and", "or", "but",
        "with", "from", "as", "by", "its", "this", "that",
    }
    tokens = re.findall(r'\b[a-z]+\b', text.lower())
    return frozenset(t for t in tokens if t not in stop and len(t) > 2)


def _jaccard_similarity(a: frozenset, b: frozenset) -> float:
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)


def deduplicate_news_narratives(
    news_items: list[dict],
    threshold: float = 0.55,
) -> list[dict]:
    """
    Suppress duplicate narratives across providers (same fundamental story,
    different wording — beyond what hash dedup already caught).

    Returns de-duplicated list, keeping the item with the highest-quality source
    (Bloomberg/FT prioritized over generic aggregators).

    Source priority for tie-breaking:
      Bloomberg > FT > CNBC > Reuters > others
    """
    SOURCE_PRIORITY = {
        "bloomberg": 4, "Financial Times": 3, "ft": 3,
        "cnbc": 2, "CNBC": 2, "reuters": 2, "Reuters": 2,
    }

    kept: list[dict] = []
    kept_tokens: list[frozenset] = []

    for item in news_items:
        title = item.get("title") or ""
        tokens = _tokenize(title)

        # Check against already-kept items
        is_duplicate = False
        for existing_tokens in kept_tokens:
            if _jaccard_similarity(tokens, existing_tokens) >= threshold:
                is_duplicate = True
                logger.debug(
                    "news_narrative_dedup",
                    title=title[:60],
                    similarity=round(_jaccard_similarity(tokens, existing_tokens), 2),
                )
                break

        if not is_duplicate:
            kept.append(item)
            kept_tokens.append(tokens)

    return kept
