from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone

import asyncpg

from src.core.config import get_settings
from src.core.logging import get_logger
from src.db.repositories import MarketTrendRepository, SocialRepository
from src.engines.noise_filter import NoiseFilter
from src.models.market import MarketTrend
from src.models.social import SentimentSummary, SocialPost
from src.providers.social import SOCIAL_PROVIDERS

logger = get_logger(__name__)


@dataclass
class SentimentCycleResult:
    fetched: int = 0
    stored: int = 0
    tickers_scored: int = 0
    hype_signals: list[str] = field(default_factory=list)
    panic_signals: list[str] = field(default_factory=list)
    errors: int = 0


class SentimentEngine:
    def __init__(self, db_pool: asyncpg.Pool) -> None:
        self._pool = db_pool
        self._social_repo = SocialRepository(db_pool)
        self._trend_repo = MarketTrendRepository(db_pool)
        self._settings = get_settings()

    def _vader_score(self, text: str) -> float:
        """Simple lexicon-based sentiment score. Returns -1.0 to 1.0."""
        try:
            from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
            analyzer = SentimentIntensityAnalyzer()
            # Add financial domain terms
            analyzer.lexicon.update({
                "moon": 3.0, "mooning": 3.0, "rip": -2.0,
                "bagholder": -2.5, "crash": -3.5, "rally": 2.5,
                "squeeze": 1.5, "rug": -3.0, "dump": -2.5,
                "pump": 2.0, "bullish": 2.5, "bearish": -2.5,
                "recession": -2.0, "inflation": -1.5,
            })
            scores = analyzer.polarity_scores(text)
            return scores["compound"]
        except ImportError:
            # Fallback: simple keyword scoring
            positive = ["bull", "up", "gain", "growth", "buy", "strong", "rally"]
            negative = ["bear", "down", "loss", "crash", "sell", "weak", "dump"]
            text_lower = text.lower()
            pos_count = sum(1 for w in positive if w in text_lower)
            neg_count = sum(1 for w in negative if w in text_lower)
            total = pos_count + neg_count
            if total == 0:
                return 0.0
            return (pos_count - neg_count) / total

    async def run_cycle(self) -> SentimentCycleResult:
        result = SentimentCycleResult()
        noise_filter = NoiseFilter()

        for name, provider_cls in SOCIAL_PROVIDERS.items():
            provider = provider_cls()
            try:
                if not await provider.health_check():
                    logger.warning("social_provider_unhealthy", provider=name)
                    continue

                posts = await provider.fetch(limit=self._settings.SOCIAL_FETCH_LIMIT)
                result.fetched += len(posts)

                # Apply noise filters before scoring / storing
                filter_result = noise_filter.filter_posts(posts)
                clean_posts = filter_result.passed

                for post in clean_posts:
                    quality = noise_filter.get_signal_quality(post)
                    # Weight sentiment by post quality before scoring
                    post.sentiment_score = self._vader_score(post.content)
                    stored = await self._social_repo.insert(post)
                    if stored:
                        result.stored += 1

                logger.info(
                    "noise_filter_applied",
                    provider=name,
                    raw=len(posts),
                    passed=len(clean_posts),
                    rejected=filter_result.total_rejected,
                )

            except Exception as exc:
                logger.error("social_provider_error", provider=name, error=str(exc))
                result.errors += 1

        # Calculate trend signals for top mentioned tickers
        await self._update_sentiment_trends(result)

        logger.info(
            "sentiment_cycle_complete",
            fetched=result.fetched,
            stored=result.stored,
            hype=len(result.hype_signals),
            panic=len(result.panic_signals),
        )
        return result

    async def _update_sentiment_trends(self, result: SentimentCycleResult) -> None:
        mention_data = await self._social_repo.get_mention_counts(hours=1)
        for row in mention_data[:50]:  # top 50 mentioned tickers
            ticker = row["ticker"]
            if not ticker or len(ticker) > 10:
                continue

            avg_sentiment = float(row["avg_sentiment"] or 0.0)
            current_mentions = int(row["mention_count"])

            # Calculate velocity vs 7-day baseline
            hist_avg = await self._social_repo.get_historical_mention_avg(ticker, days=7)
            velocity = (current_mentions / max(hist_avg, 0.1)) if hist_avg else 1.0

            hype = (
                velocity >= self._settings.SENTIMENT_HYPE_VELOCITY_THRESHOLD
                and avg_sentiment >= self._settings.SENTIMENT_HYPE_SCORE_THRESHOLD
            )
            panic = (
                velocity >= self._settings.SENTIMENT_PANIC_VELOCITY_THRESHOLD
                and avg_sentiment <= self._settings.SENTIMENT_PANIC_SCORE_THRESHOLD
            )

            if hype:
                result.hype_signals.append(ticker)
            if panic:
                result.panic_signals.append(ticker)

            trend = MarketTrend(
                ticker=ticker,
                exchange="US" if not ticker.endswith(".IS") else "BIST",
                social_score=min(1.0, max(-1.0, avg_sentiment)),
                hype_signal=hype,
                panic_signal=panic,
                computed_at=datetime.now(timezone.utc),
            )
            try:
                await self._trend_repo.upsert_trend(trend)
                result.tickers_scored += 1
            except Exception as exc:
                logger.error("trend_upsert_error", ticker=ticker, error=str(exc))

    async def get_sentiment_summary(self, ticker: str, hours: int = 24) -> SentimentSummary:
        posts_data = await self._social_repo.get_ticker_mentions(ticker, hours=hours)
        posts = [SocialPost(**{**p, "id": str(p.get("external_id", "")),
                               "platform": p.get("platform", ""),
                               "content": p.get("content", ""),
                               "author": p.get("author", ""),
                               "created_at": p.get("created_at", datetime.now(timezone.utc))})
                 for p in posts_data[:5]]

        sentiments = [float(p.get("sentiment_score") or 0) for p in posts_data if p.get("sentiment_score")]
        avg_sent = sum(sentiments) / len(sentiments) if sentiments else 0.0

        hist_avg = await self._social_repo.get_historical_mention_avg(ticker, days=7)
        hourly_rate = len(posts_data) / max(hours, 1)
        velocity = hourly_rate / max(hist_avg, 0.1)

        hype = (
            velocity >= self._settings.SENTIMENT_HYPE_VELOCITY_THRESHOLD
            and avg_sent >= self._settings.SENTIMENT_HYPE_SCORE_THRESHOLD
        )
        panic = (
            velocity >= self._settings.SENTIMENT_PANIC_VELOCITY_THRESHOLD
            and avg_sent <= self._settings.SENTIMENT_PANIC_SCORE_THRESHOLD
        )

        return SentimentSummary(
            ticker=ticker,
            period_hours=hours,
            mention_count=len(posts_data),
            avg_sentiment=round(avg_sent, 4),
            velocity_ratio=round(velocity, 2),
            hype_signal=hype,
            panic_signal=panic,
            top_posts=posts,
        )
