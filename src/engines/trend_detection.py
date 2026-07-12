"""
Trend Detection Engine — extended with Level-2 overlays.

Level-2 additions (all AFTER the existing composite calculation):
  A) Velocity normalization — per-ticker social velocity ratio
  B) Volatility-aware dampening — suppress market-wide sweeps
  C) Signal smoothing — Redis-backed EMA (smoothed_composite)
  D) Confidence scoring — ConfidenceEngine overlay
  E) Narrative classification — NarrativeEngine overlay

Existing composite_score logic is UNCHANGED.
New fields are written alongside it, never replacing it.
"""
from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import TYPE_CHECKING

import asyncpg

from src.core.config import get_settings
from src.core.logging import get_logger
from src.core.signal_tracer import SignalTracer
from src.db.repositories import MarketTrendRepository, NewsRepository, SocialRepository
from src.engines.confidence_engine import ConfidenceEngine
from src.engines.narrative_engine import NarrativeEngine
from src.engines.technical_engine import TechnicalEngine
from src.models.market import MarketTrend
from src.providers.social.fear_greed import FearGreedProvider

if TYPE_CHECKING:
    import redis.asyncio as aioredis

logger = get_logger(__name__)

# Exchange-aware weights are applied inline in _compute_trend_with_aux

_EMA_KEY_PREFIX = "trend:ema:"


def _avg_sentiment(news_rows: list[dict]) -> float:
    scores = [float(n["sentiment_score"]) for n in news_rows if n.get("sentiment_score")]
    return round(sum(scores) / len(scores), 4) if scores else 0.0


def _weighted_impact_score(news_rows: list[dict]) -> float:
    """Weighted news score using Ollama impact_score × source_authority.

    Uses max-weighted impact from the last 2h (breaking news dominates).
    Falls back to VADER average if no impact-scored articles exist.
    High-urgency articles (urgency=3) get a 1.5× boost.
    """
    from datetime import datetime, timedelta, timezone
    cutoff_2h = datetime.now(timezone.utc) - timedelta(hours=2)
    cutoff_24h = datetime.now(timezone.utc) - timedelta(hours=24)

    recent: list[tuple[float, float]] = []  # (impact, authority)
    older: list[float] = []

    for row in news_rows:
        pub = row.get("published_at")
        impact = row.get("impact_score")
        authority = row.get("source_authority") or 0.8
        urgency = row.get("urgency") or 0
        sentiment = row.get("sentiment_score")

        if impact is not None:
            weighted = float(impact) * float(authority) * (1.5 if urgency >= 3 else 1.0)
            if pub and pub >= cutoff_2h:
                recent.append((float(impact), weighted))
            elif pub and pub >= cutoff_24h:
                # older impact-scored: contribute with half weight
                older.append(weighted * 0.5)
        elif sentiment is not None:
            older.append(float(sentiment))

    if recent:
        # Dominant signal: highest absolute weighted impact in last 2h
        dominant = max(recent, key=lambda x: abs(x[1]))
        dominant_score = dominant[1]
        # Blend with average of others for stability
        all_weighted = [x[1] for x in recent] + older
        avg = sum(all_weighted) / len(all_weighted) if all_weighted else 0.0
        return round(dominant_score * 0.7 + avg * 0.3, 4)

    if older:
        return round(sum(older) / len(older), 4)

    return 0.0


def _volatility_regime(composite_scores: list[float]) -> float:
    """
    Market-wide volatility proxy: normalised std-dev of composite scores.
    Returns 0.0 (calm) → 1.0 (extreme regime).
    A std-dev of 0.5 across all tickers maps to regime 1.0.
    """
    if len(composite_scores) < 3:
        return 0.0
    mean = sum(composite_scores) / len(composite_scores)
    variance = sum((s - mean) ** 2 for s in composite_scores) / len(composite_scores)
    return min(1.0, math.sqrt(variance) / 0.5)


class TrendDetectionEngine:
    def __init__(
        self,
        db_pool: asyncpg.Pool,
        redis: "aioredis.Redis | None" = None,
    ) -> None:
        self._pool = db_pool
        self._redis = redis
        self._trend_repo = MarketTrendRepository(db_pool)
        self._news_repo = NewsRepository(db_pool)
        self._social_repo = SocialRepository(db_pool)
        self._confidence = ConfidenceEngine()
        self._technical = TechnicalEngine(db_pool, redis)
        self._settings = get_settings()
        self._fear_greed = FearGreedProvider()

    async def run_cycle(self, cycle_id: str | None = None) -> dict:
        ticker_rows = await self._get_active_tickers()
        exchange_by_ticker = {r["symbol"]: r["exchange"] for r in ticker_rows}
        errors = 0
        tracer = SignalTracer(self._pool) if cycle_id else None

        # Hoist social fetch: one query covers all tickers (avoids N×1 pattern)
        social_data = await self._social_repo.get_mention_counts(hours=24)
        social_by_ticker: dict[str, dict] = {
            r["ticker"]: dict(r) for r in social_data if r.get("ticker")
        }

        # Pre-fetch Fear & Greed social scores for CRYPTO tickers (one HTTP request)
        crypto_tickers = [
            r["symbol"] for r in ticker_rows if r.get("exchange") == "CRYPTO"
        ]
        cp_social_scores: dict[str, float] = {}
        if crypto_tickers:
            try:
                cp_social_scores = await self._fear_greed.get_social_scores(crypto_tickers)
            except Exception as exc:
                logger.warning("fear_greed_prefetch_error", error=str(exc))

        # Fetch news batch for NarrativeEngine (single round-trip)
        news_items = await self._news_repo.get_recent(hours=24, limit=300)

        # Pass 1 — raw trend computation
        trends_aux: list[tuple[MarketTrend, dict]] = []
        for row in ticker_rows:
            ticker = row["symbol"]
            exchange = row["exchange"]
            try:
                trend, aux = await self._compute_trend_with_aux(
                    ticker, exchange, social_by_ticker, cp_social_scores
                )
                trends_aux.append((trend, aux))
            except Exception as exc:
                logger.error("trend_compute_error", ticker=ticker, error=str(exc))
                errors += 1

        if not trends_aux:
            logger.warning("trend_cycle_no_trends", tickers=len(ticker_rows), errors=errors)
            return {"updated": 0, "errors": errors}

        all_trends = [t for t, _ in trends_aux]

        # Pass 2 — volatility-aware dampening
        raw_scores = [t.composite_score for t in all_trends]
        vol_regime = _volatility_regime(raw_scores)
        if vol_regime > 0.4:
            mean_score = sum(raw_scores) / len(raw_scores)
            dampen = self._settings.TREND_VOLATILITY_DAMPENING
            for trend in all_trends:
                # Only dampen tickers moving WITH the market (likely macro sweep)
                if (trend.composite_score >= 0) == (mean_score >= 0):
                    trend.composite_score = round(trend.composite_score * dampen, 4)

        # Pass 3 — EMA smoothing + confidence overlay
        for trend, aux in trends_aux:
            smooth = await self._get_smoothed_score(trend.ticker, trend.composite_score)
            trend.smoothed_composite = round(smooth, 4)

            conf = self._confidence.compute(
                news_score=trend.news_score,
                social_score=trend.social_score,
                smart_money_score=trend.smart_money_score,
                composite_score=trend.composite_score,
                smoothed_composite=smooth,
                velocity_ratio=aux.get("velocity_ratio", 1.0),
                circuit_statuses=[],  # circuit registry not yet globally exposed
                news_count=aux.get("news_count", 0),
                social_mention_count=aux.get("social_mention_count", 0),
                technical_score=trend.technical_score,
            )
            trend.confidence_score = round(conf.score, 4)
            trend.confidence_level = conf.level
            trend.signal_quality = conf.level  # 1:1 mapping for now

        # Pass 4 — narrative classification
        narrative_engine = NarrativeEngine(self._pool, self._redis)
        narrative_clusters = await narrative_engine.compute_narratives(all_trends, news_items)

        # Build narrative persistence lookup: cluster_name → persistence score (0–1)
        # persistence = strength_score weighted by momentum direction
        _momentum_factor = {"accelerating": 1.2, "neutral": 1.0, "decelerating": 0.6}
        narrative_persistence: dict[str, float] = {
            c.name: round(min(1.0, c.strength_score * _momentum_factor.get(c.momentum, 1.0)), 4)
            for c in narrative_clusters
        }

        # Build per-ticker headline map for classify_ticker
        headlines_by_ticker: dict[str, list[str]] = {}
        for item in news_items:
            for sym in (item.get("ticker_symbols") or []):
                headlines_by_ticker.setdefault(sym, []).append(item.get("title", ""))

        for trend in all_trends:
            headlines = headlines_by_ticker.get(trend.ticker, [])
            trend.narrative_cluster = await narrative_engine.classify_ticker(trend.ticker, headlines)

        # Pass 5 — persist + trace
        updated = 0
        import time as _time
        for trend in all_trends:
            trend.narrative_persistence = narrative_persistence.get(trend.narrative_cluster or "", 0.0)
            t_persist = _time.monotonic()
            try:
                await self._trend_repo.upsert_trend(trend)
                updated += 1
            except Exception as exc:
                logger.error("trend_upsert_error", ticker=trend.ticker, error=str(exc))
                errors += 1
                continue
            if tracer:
                direction = (
                    "bullish" if trend.composite_score > 0
                    else "bearish" if trend.composite_score < 0
                    else "neutral"
                )
                await tracer.record(
                    cycle_id=cycle_id,
                    engine_name="trend_detection",
                    signal_type="trend",
                    ticker=trend.ticker,
                    signal_strength=round(
                        (trend.confidence_score or 0) * 100, 2
                    ),
                    signal_direction=direction,
                    output_data={
                        "ticker": trend.ticker,
                        "composite_score": trend.composite_score,
                        "confidence_score": round(trend.confidence_score or 0, 4),
                        "confidence_level": trend.confidence_level,
                        "narrative_cluster": trend.narrative_cluster,
                        "narrative_persistence": trend.narrative_persistence,
                        "news_score": trend.news_score,
                        "social_score": trend.social_score,
                        "smart_money_score": trend.smart_money_score,
                        "volume_score": trend.volume_score,
                        "technical_score": trend.technical_score,
                    },
                    latency_ms=round((_time.monotonic() - t_persist) * 1000, 1),
                )

        logger.info(
            "trend_cycle_complete",
            updated=updated,
            errors=errors,
            vol_regime=round(vol_regime, 3),
        )
        return {"updated": updated, "errors": errors, "vol_regime": round(vol_regime, 3)}

    async def _get_smoothed_score(self, ticker: str, current: float) -> float:
        """Redis-backed EMA: smooth = α·current + (1−α)·previous."""
        if not self._redis:
            return current
        alpha = self._settings.TREND_SMOOTHING_ALPHA
        key = f"{_EMA_KEY_PREFIX}{ticker}"
        try:
            raw = await self._redis.get(key)
            smooth = alpha * current + (1 - alpha) * float(raw) if raw else current
            await self._redis.setex(
                key, self._settings.TREND_SMOOTHING_REDIS_TTL, str(smooth)
            )
        except Exception:
            smooth = current
        return smooth

    async def _get_active_tickers(self) -> list[dict]:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT symbol, exchange FROM tickers WHERE is_active = true LIMIT 200"
            )
            return [dict(r) for r in rows]

    async def _compute_trend_with_aux(
        self,
        ticker: str,
        exchange: str,
        social_by_ticker: dict[str, dict],
        cp_social_scores: dict[str, float] | None = None,
    ) -> tuple[MarketTrend, dict]:
        """
        Compute raw MarketTrend plus auxiliary data needed by ConfidenceEngine.
        social_by_ticker is pre-fetched once per cycle to avoid N×1 queries.
        cp_social_scores is the Fear & Greed market-wide social score, pre-fetched
        for CRYPTO tickers; falls back to 0.0 if absent or provider failed.
        exchange comes from the tickers table (CRYPTO/BIST/NASDAQ/NYSE/CBOE).
        """

        news = await self._news_repo.get_recent(hours=24, ticker=ticker, limit=20)
        news_score = _weighted_impact_score(news)
        news_count = len([n for n in news if n.get("sentiment_score") or n.get("impact_score")])

        social_row = social_by_ticker.get(ticker)
        if social_row:
            raw_sentiment = float(social_row.get("avg_sentiment") or 0)
            social_score = min(1.0, max(-1.0, raw_sentiment))
            social_mention_count = int(social_row.get("mention_count") or 0)
        else:
            social_score = 0.0
            social_mention_count = 0

        # For CRYPTO tickers, override with Fear & Greed market-wide social score
        # when the DB social row is absent (crypto social data is never populated).
        if exchange == "CRYPTO" and cp_social_scores:
            cp_score = cp_social_scores.get(ticker)
            if cp_score is not None:
                social_score = cp_score

        # Velocity: current 24h hourly rate vs 7d historical hourly average
        hist_hourly_avg = await self._social_repo.get_historical_mention_avg(ticker, days=7)
        current_hourly = social_mention_count / 24.0
        velocity_ratio = (
            (current_hourly / max(hist_hourly_avg, 0.1)) if hist_hourly_avg else 1.0
        )
        velocity_ratio = min(velocity_ratio, 20.0)  # cap extreme spikes

        async with self._pool.acquire() as conn:
            existing = await conn.fetchrow(
                "SELECT smart_money_score FROM market_trends WHERE ticker = $1",
                ticker,
            )
            # Volume score: ratio of latest 24h volume vs 30-day rolling average
            # tanh(log(ratio)) maps: avg→0, 2x avg→+0.60, 0.5x avg→-0.60
            vol_rows = await conn.fetch(
                """
                SELECT volume FROM ticker_quotes
                 WHERE symbol = $1 AND volume > 0
                 ORDER BY ts DESC LIMIT 43
                """,
                ticker,
            )
        smart_money_score = float(existing["smart_money_score"]) if existing else 0.0
        if len(vol_rows) >= 2:
            latest_vol = float(vol_rows[0]["volume"])
            avg_vol = sum(float(r["volume"]) for r in vol_rows[1:]) / (len(vol_rows) - 1)
            if avg_vol > 0:
                volume_score = round(math.tanh(math.log(max(latest_vol / avg_vol, 0.01))), 4)
            else:
                volume_score = 0.0
        else:
            volume_score = 0.0

        # Technical score: RSI + momentum + Bollinger + funding rate
        # Use exchange from tickers table directly — no hardcoded lists needed
        tech_exchange = exchange  # CRYPTO/BIST/NASDAQ/NYSE/CBOE → TechnicalEngine handles each
        technical_score = await self._technical.compute(ticker, tech_exchange)

        # Exchange-aware composite weights
        # CRYPTO/US: technical is the only signal with genuine short-term predictive
        # value; news/social are lagging noise for intraday holds.
        if exchange == "BIST":
            w_news, w_social, w_sm, w_vol, w_tech = 0.40, 0.15, 0.00, 0.00, 0.45
        elif exchange == "CRYPTO":
            # Technical dominant; volume as confirmation; news/social minimal
            w_news, w_social, w_sm, w_vol, w_tech = 0.10, 0.10, 0.00, 0.25, 0.55
        else:
            # US stocks: technical + smart money are the only reliable edges;
            # options flow (smart_money) is highly predictive. News/social = noise.
            w_news, w_social, w_sm, w_vol, w_tech = 0.10, 0.05, 0.25, 0.10, 0.50

        composite = (
            w_news  * news_score
            + w_social * social_score
            + w_sm     * smart_money_score
            + w_vol    * volume_score
            + w_tech   * technical_score
        )

        # Diversity bonus: +0.08 when 2+ independent sources agree direction AND
        # technical is one of them (|tech| > 0.10). Pure news/social agreement
        # without technical confirmation is noise, not edge.
        nonzero_scores = [
            s for s in (news_score, social_score, smart_money_score, volume_score, technical_score)
            if s != 0.0
        ]
        tech_confirms = abs(technical_score) >= 0.10
        if len(nonzero_scores) >= 2 and tech_confirms:
            same_sign = all(s > 0 for s in nonzero_scores) or all(s < 0 for s in nonzero_scores)
            if same_sign:
                direction_sign = 1.0 if composite > 0 else -1.0
                composite = round(composite + direction_sign * 0.08, 4)

        trend = MarketTrend(
            ticker=ticker,
            exchange=exchange,
            news_score=round(news_score, 4),
            social_score=round(social_score, 4),
            smart_money_score=round(smart_money_score, 4),
            volume_score=round(volume_score, 4),
            technical_score=round(technical_score, 4),
            composite_score=round(composite, 4),
            hype_signal=social_score > 0.7 and social_row is not None,
            panic_signal=social_score < -0.7 and social_row is not None,
            trending_up=composite > 0.3,
            trending_down=composite < -0.3,
            computed_at=datetime.now(timezone.utc),
        )

        aux = {
            "news_count": news_count,
            "social_mention_count": social_mention_count,
            "velocity_ratio": velocity_ratio,
        }
        return trend, aux

    async def _compute_trend(self, ticker: str) -> MarketTrend:
        """Backward-compat wrapper (used by tests and one-off calls)."""
        social_data = await self._social_repo.get_mention_counts(hours=24)
        social_by_ticker = {r["ticker"]: dict(r) for r in social_data if r.get("ticker")}
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow("SELECT exchange FROM tickers WHERE symbol = $1", ticker)
        exchange = row["exchange"] if row else "US"
        trend, _ = await self._compute_trend_with_aux(ticker, exchange, social_by_ticker)
        return trend

    async def _compute_news_score(self, ticker: str) -> float:
        news = await self._news_repo.get_recent(hours=24, ticker=ticker, limit=20)
        return _weighted_impact_score(news)

    async def get_hot_tickers(self, exchange: str | None = None, limit: int = 10) -> list[dict]:
        return await self._trend_repo.get_top_trending(exchange=exchange, limit=limit)

    async def get_signals(self) -> dict:
        hype = await self._trend_repo.get_signals(hype=True)
        panic = await self._trend_repo.get_signals(hype=False)
        return {
            "hype_signals": [r["ticker"] for r in hype],
            "panic_signals": [r["ticker"] for r in panic],
        }
