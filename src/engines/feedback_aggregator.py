"""
Feedback Aggregator — Feedback Loop Layer

Reads signal_feedback and provider_ingestion_log to compute:

  Engine scores   → upserted into engine_performance_metrics
  Provider scores → upserted into provider_performance_metrics

Runs hourly.  All writes are upserts — safe to re-run at any time.
Never touches signal_trace, signals_registry, or ingestion tables.

Scoring formulas:
  win_rate           = wins / (wins + losses)     for each window
  avg_pnl_score      = AVG(pnl_score) across all evaluated rows
  noise_ratio        = (dup_exact + dup_fuzzy) / MAX(fetched, 1)
"""
from __future__ import annotations

import asyncio
import os
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import TYPE_CHECKING

import asyncpg

from src.core import http_server as _http
from src.core.logging import configure_logging, get_logger

if TYPE_CHECKING:
    import redis.asyncio as aioredis

logger = get_logger(__name__)

_WINDOWS_H = (1, 4, 24)
_LOOKBACK_DAYS = 30   # aggregate rolling 30-day window into today's metric row


# ── Result ────────────────────────────────────────────────────────────────────

@dataclass
class AggregationResult:
    engines_updated: int = 0
    providers_updated: int = 0
    errors: int = 0


# ── Aggregator ────────────────────────────────────────────────────────────────

class FeedbackAggregator:
    """
    Aggregates signal_feedback and provider_ingestion_log into daily metric rows.
    """

    def __init__(
        self,
        db_pool: asyncpg.Pool,
        redis: "aioredis.Redis | None" = None,
        lookback_days: int = _LOOKBACK_DAYS,
    ) -> None:
        self._pool = db_pool
        self._redis = redis
        self._lookback_days = lookback_days

    async def run_cycle(self, cycle_id: str | None = None) -> AggregationResult:
        result = AggregationResult()

        try:
            result.engines_updated = await self._aggregate_engines()
        except Exception as exc:
            logger.error("aggregator_engine_error", error=str(exc))
            result.errors += 1

        try:
            result.providers_updated = await self._aggregate_providers()
        except Exception as exc:
            logger.error("aggregator_provider_error", error=str(exc))
            result.errors += 1

        logger.info(
            "feedback_aggregation_complete",
            engines_updated=result.engines_updated,
            providers_updated=result.providers_updated,
            errors=result.errors,
        )
        return result

    # ── Engine metrics ────────────────────────────────────────────────────────

    async def _aggregate_engines(self) -> int:
        """
        Compute win_rate_1h/24h/7d and avg_pnl per engine, upsert into
        engine_performance_metrics for today's date.
        """
        since = datetime.now(timezone.utc) - timedelta(days=self._lookback_days)
        today = date.today()

        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT
                    engine_name,
                    evaluation_window_h,
                    COUNT(*) FILTER (WHERE outcome_type = 'win')  AS wins,
                    COUNT(*) FILTER (WHERE outcome_type = 'loss') AS losses,
                    COUNT(*)                                       AS total,
                    AVG(pnl_score)                                AS avg_pnl,
                    AVG(confidence_score)                         AS avg_confidence
                  FROM signal_feedback
                 WHERE evaluated_at > $1
                   AND outcome_type != 'pending'
                 GROUP BY engine_name, evaluation_window_h
                """,
                since,
            )

        # Pivot: group by engine_name, collect per-window stats
        engine_stats: dict[str, dict] = {}
        for row in rows:
            name = row["engine_name"]
            wh = row["evaluation_window_h"]
            if name not in engine_stats:
                engine_stats[name] = {"avg_pnl": 0.0, "total": 0, "avg_conf": 0.0}
            wins = int(row["wins"] or 0)
            losses = int(row["losses"] or 0)
            directional = wins + losses
            win_rate = (wins / directional) if directional > 0 else None
            engine_stats[name][f"win_rate_{wh}h"] = win_rate
            # Accumulate for overall avg_pnl (weighted by total)
            engine_stats[name]["avg_pnl"] += float(row["avg_pnl"] or 0) * float(row["total"] or 0)
            engine_stats[name]["total"] += int(row["total"] or 0)
            engine_stats[name]["avg_conf"] += float(row["avg_confidence"] or 0) * float(row["total"] or 0)

        updated = 0
        async with self._pool.acquire() as conn:
            for engine_name, stats in engine_stats.items():
                total = stats["total"] or 1
                avg_pnl = stats["avg_pnl"] / total
                avg_conf = stats["avg_conf"] / total
                win_rate_1h  = stats.get("win_rate_1h")
                win_rate_4h  = stats.get("win_rate_4h")   # not in column; store as 24h proxy
                win_rate_24h = stats.get("win_rate_24h")

                # Use 7d window if we have it; fall back to overall win_rate
                all_window_rates = [
                    v for k, v in stats.items()
                    if k.startswith("win_rate_") and v is not None
                ]
                win_rate_7d = (sum(all_window_rates) / len(all_window_rates)) if all_window_rates else None

                await conn.execute(
                    """
                    INSERT INTO engine_performance_metrics (
                        engine_name, metric_date,
                        total_signals, avg_confidence,
                        win_rate_1h, win_rate_24h, win_rate_7d,
                        avg_pnl_score, feedback_signals_total, computed_at
                    ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
                    ON CONFLICT (engine_name, metric_date) DO UPDATE SET
                        win_rate_1h            = EXCLUDED.win_rate_1h,
                        win_rate_24h           = EXCLUDED.win_rate_24h,
                        win_rate_7d            = EXCLUDED.win_rate_7d,
                        avg_pnl_score          = EXCLUDED.avg_pnl_score,
                        feedback_signals_total = EXCLUDED.feedback_signals_total,
                        avg_confidence         = EXCLUDED.avg_confidence,
                        computed_at            = EXCLUDED.computed_at
                    """,
                    engine_name,
                    today,
                    stats["total"],
                    round(avg_conf, 4),
                    win_rate_1h,
                    win_rate_24h,
                    win_rate_7d,
                    round(avg_pnl, 6),
                    stats["total"],
                    datetime.now(timezone.utc),
                )
                updated += 1
                logger.debug(
                    "engine_metrics_updated",
                    engine=engine_name,
                    win_rate_1h=win_rate_1h,
                    win_rate_24h=win_rate_24h,
                    avg_pnl=round(avg_pnl, 4),
                )

        return updated

    # ── Provider metrics ──────────────────────────────────────────────────────

    async def _aggregate_providers(self) -> int:
        """
        Join signal_feedback with provider_ingestion_log via cycle_id to compute
        per-provider win_rate and noise_ratio.  Upsert into provider_performance_metrics.
        """
        since = datetime.now(timezone.utc) - timedelta(days=self._lookback_days)
        today = date.today()

        async with self._pool.acquire() as conn:
            # Outcome stats: from signal_feedback with provider set
            outcome_rows = await conn.fetch(
                """
                SELECT
                    provider,
                    COUNT(*) FILTER (WHERE outcome_type = 'win')  AS wins,
                    COUNT(*) FILTER (WHERE outcome_type = 'loss') AS losses,
                    COUNT(*)                                       AS total,
                    AVG(pnl_score)                                AS avg_pnl
                  FROM signal_feedback
                 WHERE evaluated_at > $1
                   AND provider IS NOT NULL
                   AND outcome_type != 'pending'
                 GROUP BY provider
                """,
                since,
            )

            # Ingestion stats: noise and latency from provider_ingestion_log
            ingestion_rows = await conn.fetch(
                """
                SELECT
                    provider,
                    SUM(fetched)                          AS total_fetched,
                    SUM(dup_exact + dup_fuzzy)            AS total_dupes,
                    AVG(latency_ms)                       AS avg_latency_ms
                  FROM provider_ingestion_log
                 WHERE ts > $1
                 GROUP BY provider
                """,
                since,
            )

        ingestion_by_provider = {r["provider"]: r for r in ingestion_rows}

        updated = 0
        async with self._pool.acquire() as conn:
            for row in outcome_rows:
                provider = row["provider"]
                wins   = int(row["wins"]  or 0)
                losses = int(row["losses"] or 0)
                total  = int(row["total"]  or 0)
                directional = wins + losses
                win_rate = (wins / directional) if directional > 0 else None
                avg_pnl  = float(row["avg_pnl"] or 0)

                ing = ingestion_by_provider.get(provider, {})
                fetched = int((ing.get("total_fetched") or 0))
                dupes   = int((ing.get("total_dupes")   or 0))
                noise_ratio  = dupes / max(fetched, 1) if fetched else None
                avg_latency  = float(ing["avg_latency_ms"] or 0) if ing else None

                await conn.execute(
                    """
                    INSERT INTO provider_performance_metrics (
                        provider, metric_date,
                        total_signals, win_count, loss_count, neutral_count,
                        win_rate, avg_pnl_score, noise_ratio, avg_latency_ms, computed_at
                    ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
                    ON CONFLICT (provider, metric_date) DO UPDATE SET
                        total_signals  = EXCLUDED.total_signals,
                        win_count      = EXCLUDED.win_count,
                        loss_count     = EXCLUDED.loss_count,
                        neutral_count  = EXCLUDED.neutral_count,
                        win_rate       = EXCLUDED.win_rate,
                        avg_pnl_score  = EXCLUDED.avg_pnl_score,
                        noise_ratio    = EXCLUDED.noise_ratio,
                        avg_latency_ms = EXCLUDED.avg_latency_ms,
                        computed_at    = EXCLUDED.computed_at
                    """,
                    provider,
                    today,
                    total,
                    wins,
                    losses,
                    total - directional,
                    win_rate,
                    round(avg_pnl, 6),
                    round(noise_ratio, 4) if noise_ratio is not None else None,
                    round(avg_latency, 1) if avg_latency is not None else None,
                    datetime.now(timezone.utc),
                )
                updated += 1
                logger.debug(
                    "provider_metrics_updated",
                    provider=provider,
                    win_rate=win_rate,
                    noise_ratio=noise_ratio,
                )

        return updated

    # ── Read API ──────────────────────────────────────────────────────────────

    async def get_engine_ranking(self, window_h: int = 24) -> list[dict]:
        """Rank engines by win_rate for the given window, latest metric date."""
        col = "win_rate_24h" if window_h >= 24 else "win_rate_1h"
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT DISTINCT ON (engine_name)
                    engine_name, metric_date,
                    win_rate_1h, win_rate_24h, win_rate_7d,
                    avg_pnl_score, feedback_signals_total, avg_confidence
                  FROM engine_performance_metrics
                 WHERE {col} IS NOT NULL
                 ORDER BY engine_name, metric_date DESC
                """
            )
            result = [dict(r) for r in rows]
            result.sort(key=lambda r: (r.get(col) or 0), reverse=True)
            return result

    async def get_provider_ranking(self) -> list[dict]:
        """Rank providers by win_rate + noise_ratio, latest metric date."""
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT DISTINCT ON (provider)
                    provider, metric_date,
                    total_signals, win_count, loss_count,
                    win_rate, avg_pnl_score, noise_ratio, avg_latency_ms
                  FROM provider_performance_metrics
                 ORDER BY provider, metric_date DESC
                """
            )
            result = [dict(r) for r in rows]
            result.sort(key=lambda r: (r.get("win_rate") or 0), reverse=True)
            return result

    async def get_signal_type_comparison(self) -> list[dict]:
        """Compare win_rate and avg_pnl by signal_type across all engines."""
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT
                    signal_type,
                    evaluation_window_h,
                    COUNT(*) FILTER (WHERE outcome_type = 'win')  AS wins,
                    COUNT(*) FILTER (WHERE outcome_type = 'loss') AS losses,
                    COUNT(*)                                       AS total,
                    AVG(pnl_score)                                AS avg_pnl,
                    AVG(confidence_score)                         AS avg_confidence
                  FROM signal_feedback
                 WHERE outcome_type != 'pending'
                   AND evaluated_at > now() - interval '30 days'
                 GROUP BY signal_type, evaluation_window_h
                 ORDER BY signal_type, evaluation_window_h
                """
            )
            return [
                {
                    **dict(r),
                    "win_rate": (
                        int(r["wins"]) / (int(r["wins"]) + int(r["losses"]))
                        if (int(r["wins"] or 0) + int(r["losses"] or 0)) > 0
                        else None
                    ),
                }
                for r in rows
            ]

    async def get_performance_over_time(
        self, engine_name: str | None = None, days: int = 30
    ) -> list[dict]:
        """Return daily win_rate_24h and avg_pnl trend for the given engine."""
        conditions = ["metric_date > current_date - $1"]
        params: list = [days]
        if engine_name:
            params.append(engine_name)
            conditions.append(f"engine_name = ${len(params)}")
        where = " AND ".join(conditions)

        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT engine_name, metric_date,
                       win_rate_1h, win_rate_24h, win_rate_7d,
                       avg_pnl_score, feedback_signals_total
                  FROM engine_performance_metrics
                 WHERE {where}
                 ORDER BY engine_name, metric_date ASC
                """,
                *params,
            )
            return [dict(r) for r in rows]


# ── Standalone entrypoint ─────────────────────────────────────────────────────

async def _main() -> None:
    configure_logging()

    from src.db.connection import get_db_pool, close_db_pool
    from src.cache.redis_client import get_redis, close_redis
    import uuid as _uuid

    http_port     = int(os.getenv("AGGREGATOR_HTTP_PORT", "8020"))
    run_interval  = int(os.getenv("AGGREGATOR_INTERVAL",  "3600"))

    _http.start(http_port, "feedback-aggregator")

    db    = await get_db_pool()
    redis = await get_redis()
    _http.set_ready("feedback-aggregator")

    aggregator = FeedbackAggregator(db, redis)
    logger.info("feedback_aggregator_started", interval_s=run_interval)

    try:
        while True:
            t0 = time.monotonic()
            cycle_id = str(_uuid.uuid4())
            try:
                result = await aggregator.run_cycle(cycle_id=cycle_id)
                logger.info(
                    "feedback_aggregator_cycle",
                    cycle_id=cycle_id,
                    engines_updated=result.engines_updated,
                    providers_updated=result.providers_updated,
                    errors=result.errors,
                    elapsed_s=round(time.monotonic() - t0, 2),
                )
            except Exception as exc:
                logger.error("feedback_aggregator_error", cycle_id=cycle_id, error=str(exc))
            await asyncio.sleep(run_interval)
    finally:
        _http.set_not_ready()
        await close_db_pool()
        await close_redis()


if __name__ == "__main__":
    asyncio.run(_main())
