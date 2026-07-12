"""
Engine Performance Dashboard — Phase 2: Orchestration + Replay System

Read-only aggregation layer that surfaces historical evaluation data as
structured analytics for the API layer and Telegram reporting.

Reads from:
  replay_metrics          — comprehensive metrics from replay runs
  signal_scores           — predictive strength + half-life (SignalScoringEngine)
  signal_outcomes         — raw outcome records (for ad-hoc queries)
  signals_registry        — signal metadata
  engine_performance_metrics — daily rollup (BacktestWorker)
  macro_regimes           — regime context
  narrative_clusters      — narrative context

Writes: nothing.

All methods accept period_days as the lookback window (default 30).
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

import asyncpg

from src.core.logging import get_logger

if TYPE_CHECKING:
    import redis.asyncio as aioredis

logger = get_logger(__name__)


class EnginePerformanceDashboard:
    """
    Analytics aggregator over the Signal Truth Layer and Replay System.
    All methods are read-only SELECT operations.
    """

    def __init__(
        self,
        db_pool: asyncpg.Pool,
        redis: "aioredis.Redis | None" = None,
    ) -> None:
        self._pool = db_pool
        self._redis = redis

    # ── Engine Ranking ────────────────────────────────────────────────────────

    async def get_engine_ranking(
        self,
        window_hours: int = 24,
        period_days: int = 30,
    ) -> list[dict]:
        """
        Rank engines by predictive_strength at the given evaluation window.
        Includes precision, recall, FPR, F1, half-life, and calibration error.
        Returns engines with at least one completed replay metric, newest first.
        """
        since = datetime.now(timezone.utc) - timedelta(days=period_days)

        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT
                    engine_name,
                    signal_type,
                    window_hours,
                    AVG(directional_accuracy)  AS directional_accuracy,
                    AVG(precision_score)       AS precision_score,
                    AVG(recall_score)          AS recall_score,
                    AVG(false_positive_rate)   AS false_positive_rate,
                    AVG(f1_score)              AS f1_score,
                    AVG(predictive_strength)   AS predictive_strength,
                    AVG(half_life_hours)       AS half_life_hours,
                    AVG(calibration_error)     AS calibration_error,
                    SUM(evaluated_count)       AS total_evaluated
                  FROM replay_metrics
                 WHERE window_hours = $1
                   AND period_start >= $2
                 GROUP BY engine_name, signal_type, window_hours
                 ORDER BY AVG(predictive_strength) DESC NULLS LAST
                """,
                window_hours,
                since,
            )

            # Supplement with signal_scores where replay_metrics are absent
            if not rows:
                rows = await conn.fetch(
                    """
                    SELECT DISTINCT ON (engine_name, signal_type)
                        engine_name, signal_type, window_hours,
                        accuracy AS directional_accuracy,
                        NULL::float AS precision_score,
                        NULL::float AS recall_score,
                        NULL::float AS false_positive_rate,
                        NULL::float AS f1_score,
                        predictive_strength,
                        half_life_hours,
                        calibration_error,
                        sample_size AS total_evaluated
                      FROM signal_scores
                     WHERE window_hours = $1
                       AND computed_at >= $2
                     ORDER BY engine_name, signal_type, computed_at DESC
                    """,
                    window_hours,
                    since,
                )

        result = [dict(r) for r in rows]
        for r in result:
            for k, v in r.items():
                if isinstance(v, float) and (v != v):  # NaN check
                    r[k] = None
        return result

    # ── Narrative Reliability ─────────────────────────────────────────────────

    async def get_narrative_reliability(
        self,
        period_days: int = 30,
    ) -> list[dict]:
        """
        Accuracy per narrative cluster across all replay runs in the period.
        Aggregates regime_breakdown and narrative_breakdown JSONB fields.

        Returns list of {narrative, avg_accuracy, signal_count, engines}.
        """
        since = datetime.now(timezone.utc) - timedelta(days=period_days)

        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT narrative_breakdown, signal_count, engine_name
                  FROM replay_metrics
                 WHERE period_start >= $1
                   AND narrative_breakdown != '{}'::jsonb
                """,
                since,
            )

        # Aggregate narrative accuracies in Python
        agg: dict[str, list[float]] = {}
        counts: dict[str, int] = {}
        engines: dict[str, set[str]] = {}

        for row in rows:
            bd = row["narrative_breakdown"]
            if isinstance(bd, str):
                bd = json.loads(bd)
            engine = row["engine_name"]
            for narrative, accuracy in (bd or {}).items():
                if accuracy is None:
                    continue
                agg.setdefault(narrative, []).append(float(accuracy))
                counts[narrative] = counts.get(narrative, 0) + (row["signal_count"] or 0)
                engines.setdefault(narrative, set()).add(engine)

        result = []
        for narrative, acc_list in sorted(agg.items()):
            avg = sum(acc_list) / len(acc_list)
            result.append({
                "narrative":        narrative,
                "avg_accuracy":     round(avg, 4),
                "observation_count": len(acc_list),
                "signal_count":     counts.get(narrative, 0),
                "engines":          sorted(engines.get(narrative, set())),
            })
        result.sort(key=lambda r: r["avg_accuracy"], reverse=True)
        return result

    # ── Regime-Specific Accuracy ──────────────────────────────────────────────

    async def get_regime_accuracy(
        self,
        engine_name: str | None = None,
        period_days: int = 30,
    ) -> list[dict]:
        """
        Accuracy per macro regime.
        If engine_name is given, scoped to that engine.
        Returns {regime, engine_name, avg_accuracy, observation_count}.
        """
        since = datetime.now(timezone.utc) - timedelta(days=period_days)

        async with self._pool.acquire() as conn:
            params: list = [since]
            engine_clause = ""
            if engine_name:
                params.append(engine_name)
                engine_clause = f"AND engine_name = ${len(params)}"

            rows = await conn.fetch(
                f"""
                SELECT regime_breakdown, engine_name
                  FROM replay_metrics
                 WHERE period_start >= $1
                   AND regime_breakdown != '{{}}'::jsonb
                   {engine_clause}
                """,
                *params,
            )

        agg: dict[tuple[str, str], list[float]] = {}
        for row in rows:
            bd = row["regime_breakdown"]
            if isinstance(bd, str):
                bd = json.loads(bd)
            eng = row["engine_name"]
            for regime, accuracy in (bd or {}).items():
                if accuracy is None:
                    continue
                key = (regime, eng)
                agg.setdefault(key, []).append(float(accuracy))

        result = []
        for (regime, eng), acc_list in agg.items():
            result.append({
                "regime":            regime,
                "engine_name":       eng,
                "avg_accuracy":      round(sum(acc_list) / len(acc_list), 4),
                "observation_count": len(acc_list),
            })
        result.sort(key=lambda r: (r["regime"], -r["avg_accuracy"]))
        return result

    # ── Engine Comparison ─────────────────────────────────────────────────────

    async def compare_engines(
        self,
        engine_names: list[str],
        window_hours: int = 24,
        period_days: int = 30,
    ) -> dict:
        """
        Side-by-side metric comparison for a set of engines.
        Returns a matrix: {engine_name → {metric → value}}.
        """
        since = datetime.now(timezone.utc) - timedelta(days=period_days)

        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT
                    engine_name,
                    AVG(directional_accuracy)  AS accuracy,
                    AVG(precision_score)       AS precision,
                    AVG(recall_score)          AS recall,
                    AVG(false_positive_rate)   AS fpr,
                    AVG(f1_score)              AS f1,
                    AVG(predictive_strength)   AS strength,
                    AVG(half_life_hours)       AS half_life,
                    AVG(calibration_error)     AS calibration,
                    SUM(evaluated_count)       AS evaluated
                  FROM replay_metrics
                 WHERE engine_name = ANY($1)
                   AND window_hours = $2
                   AND period_start >= $3
                 GROUP BY engine_name
                """,
                engine_names,
                window_hours,
                since,
            )

        return {
            row["engine_name"]: {
                "accuracy":          _safe_round(row["accuracy"]),
                "precision":         _safe_round(row["precision"]),
                "recall":            _safe_round(row["recall"]),
                "false_positive_rate": _safe_round(row["fpr"]),
                "f1_score":          _safe_round(row["f1"]),
                "predictive_strength": _safe_round(row["strength"]),
                "half_life_hours":   _safe_round(row["half_life"]),
                "calibration_error": _safe_round(row["calibration"]),
                "total_evaluated":   row["evaluated"] or 0,
            }
            for row in rows
        }

    # ── Performance Trend ─────────────────────────────────────────────────────

    async def get_performance_trend(
        self,
        engine_name: str,
        window_hours: int = 24,
        weeks: int = 8,
    ) -> list[dict]:
        """
        Weekly directional accuracy time-series for an engine.
        Uses engine_performance_metrics (daily rollup from BacktestWorker) for
        historical data; supplements with replay_metrics for deeper analysis.
        """
        since = datetime.now(timezone.utc) - timedelta(weeks=weeks)

        async with self._pool.acquire() as conn:
            # Try replay_metrics first (richer, newer)
            rows = await conn.fetch(
                """
                SELECT
                    date_trunc('week', period_start) AS week,
                    AVG(directional_accuracy)        AS accuracy,
                    AVG(predictive_strength)         AS strength,
                    SUM(evaluated_count)             AS evaluated
                  FROM replay_metrics
                 WHERE engine_name  = $1
                   AND window_hours = $2
                   AND period_start >= $3
                 GROUP BY week
                 ORDER BY week DESC
                """,
                engine_name,
                window_hours,
                since,
            )

            # Fall back to engine_performance_metrics
            if not rows:
                col = (
                    "accuracy_24h" if window_hours == 24
                    else "accuracy_4h" if window_hours <= 6
                    else "accuracy_72h"
                )
                rows = await conn.fetch(
                    f"""
                    SELECT
                        date_trunc('week', metric_date::timestamptz) AS week,
                        AVG({col})        AS accuracy,
                        NULL::float       AS strength,
                        SUM(total_signals) AS evaluated
                      FROM engine_performance_metrics
                     WHERE engine_name = $1
                       AND metric_date >= $2::date
                     GROUP BY week
                     ORDER BY week DESC
                    """,
                    engine_name,
                    since,
                )

        return [
            {
                "week":         row["week"].isoformat() if row["week"] else None,
                "accuracy":     _safe_round(row["accuracy"]),
                "strength":     _safe_round(row["strength"]),
                "evaluated":    row["evaluated"] or 0,
            }
            for row in rows
        ]

    # ── Full Summary ──────────────────────────────────────────────────────────

    async def get_full_summary(
        self,
        window_hours: int = 24,
        period_days: int = 30,
    ) -> dict:
        """
        Complete system performance summary:
          - engine rankings
          - top narratives
          - regime accuracy
          - system-wide stats
        """
        ranking    = await self.get_engine_ranking(window_hours, period_days)
        narratives = await self.get_narrative_reliability(period_days)
        regimes    = await self.get_regime_accuracy(period_days=period_days)

        since = datetime.now(timezone.utc) - timedelta(days=period_days)

        async with self._pool.acquire() as conn:
            total_signals = await conn.fetchval(
                "SELECT COUNT(*) FROM signals_registry WHERE emitted_at >= $1", since
            ) or 0
            total_outcomes = await conn.fetchval(
                """
                SELECT COUNT(*) FROM signal_outcomes so
                  JOIN signals_registry sr ON sr.id = so.signal_id
                 WHERE sr.emitted_at >= $1
                """, since
            ) or 0
            total_replays = await conn.fetchval(
                "SELECT COUNT(*) FROM replay_runs WHERE status = 'completed' AND queued_at >= $1",
                since,
            ) or 0

        return {
            "period_days":    period_days,
            "window_hours":   window_hours,
            "system_stats": {
                "total_signals_archived":    total_signals,
                "total_outcomes_evaluated":  total_outcomes,
                "completed_replay_runs":     total_replays,
                "coverage_pct": round(
                    total_outcomes / total_signals * 100, 1
                ) if total_signals > 0 else 0.0,
            },
            "engine_ranking": ranking[:10],
            "top_narratives": narratives[:6],
            "regime_accuracy": regimes,
            "disclaimer": "For informational purposes only. Not financial advice.",
        }


# ── Helpers ───────────────────────────────────────────────────────────────────

def _safe_round(v, decimals: int = 4) -> float | None:
    if v is None:
        return None
    try:
        f = float(v)
        return None if f != f else round(f, decimals)  # NaN check
    except (TypeError, ValueError):
        return None
