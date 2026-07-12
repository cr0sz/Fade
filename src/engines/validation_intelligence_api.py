"""
Validation Intelligence API — Phase 3 Data Aggregation Layer

Read-only analytics aggregator. Provides system-wide intelligence summaries,
engine decay metrics, calibration analysis, failure analysis, and reliability
summaries. Zero writes. Used by the validation_intelligence route layer.
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import asyncpg


# ─── Data Classes ─────────────────────────────────────────────────────────────

@dataclass
class SystemHealth:
    overall_precision: Optional[float]
    overall_recall: Optional[float]
    overall_f1: Optional[float]
    coverage_pct: float
    total_signals: int
    evaluated_signals: int
    engines_healthy: int
    engines_degraded: int
    engines_failing: int
    last_replay_at: Optional[datetime]
    last_outcome_at: Optional[datetime]
    queue_depth: int
    data_freshness_hours: float
    active_alerts: int
    status: str  # healthy | degraded | critical


@dataclass
class WeeklyAccuracyPoint:
    week_start: datetime
    directional_accuracy: float
    precision: Optional[float]
    sample_count: int


@dataclass
class EngineDecayMetrics:
    engine_name: str
    half_life_hours: Optional[float]
    predictive_strength: Optional[float]
    accuracy_trend: List[WeeklyAccuracyPoint]
    is_decaying: bool
    decay_rate: float  # slope per week; negative = decaying
    latest_precision: Optional[float]
    latest_recall: Optional[float]


@dataclass
class ConfidenceBucket:
    confidence_level: str      # high | medium | low
    expected_accuracy: float   # calibrated target
    actual_accuracy: float
    sample_count: int
    error: float               # |expected - actual|


@dataclass
class CalibrationAnalysis:
    engine_name: str
    calibration_error: float
    is_well_calibrated: bool   # calibration_error < 0.10
    buckets: List[ConfidenceBucket]


@dataclass
class FailureAnalysis:
    failed_replays_24h: int
    failed_replays_7d: int
    retry_exhausted_7d: int
    engines_with_no_outcomes: List[str]
    uncovered_signal_count: int
    pending_outcome_windows: Dict[int, int]  # window_hours → count
    oldest_uncovered_signal_hours: Optional[float]


@dataclass
class EngineRankEntry:
    engine_name: str
    rank: int
    predictive_strength: float
    directional_accuracy: Optional[float]
    precision: Optional[float]
    f1_score: Optional[float]


@dataclass
class ReliabilitySummary:
    engine_ranking: List[EngineRankEntry]
    best_regime: Optional[str]
    worst_regime: Optional[str]
    best_narrative: Optional[str]
    worst_narrative: Optional[str]
    overall_f1: Optional[float]
    overall_directional_accuracy: Optional[float]


# ─── Engine ───────────────────────────────────────────────────────────────────

_CALIBRATION_TARGETS = {"high": 0.80, "medium": 0.65, "low": 0.50}
_WELL_CALIBRATED_THRESHOLD = 0.10
_HEALTHY_PRECISION_FLOOR = 0.60
_FAILING_PRECISION_FLOOR = 0.40
_DECAY_SLOPE_THRESHOLD = -0.02


class ValidationIntelligenceAPI:
    """Read-only aggregation over replay_metrics, signal_outcomes, signal_scores."""

    def __init__(self, db: asyncpg.Pool, redis=None):
        self._db = db
        self._redis = redis

    # ── System Health ──────────────────────────────────────────────────────────

    async def get_system_health(self) -> SystemHealth:
        async with self._db.acquire() as conn:
            # Aggregate precision/recall/F1 over last 30 days, 24h window
            agg = await conn.fetchrow("""
                SELECT
                    AVG(precision_score)       AS avg_precision,
                    AVG(recall_score)          AS avg_recall,
                    AVG(f1_score)              AS avg_f1
                FROM replay_metrics
                WHERE window_hours = 24
                  AND computed_at >= NOW() - INTERVAL '30 days'
            """)

            # Coverage
            total = await conn.fetchval(
                "SELECT COUNT(*) FROM signals_registry"
            ) or 0
            evaluated = await conn.fetchval(
                "SELECT COUNT(DISTINCT signal_id) FROM signal_outcomes"
            ) or 0

            # Engine health buckets (latest 14-day precision per engine)
            engine_rows = await conn.fetch("""
                SELECT engine_name, AVG(precision_score) AS avg_prec
                FROM replay_metrics
                WHERE window_hours = 24
                  AND computed_at >= NOW() - INTERVAL '14 days'
                GROUP BY engine_name
            """)
            healthy = degraded = failing = 0
            for row in engine_rows:
                p = row["avg_prec"]
                if p is None:
                    continue
                if p >= _HEALTHY_PRECISION_FLOOR:
                    healthy += 1
                elif p >= _FAILING_PRECISION_FLOOR:
                    degraded += 1
                else:
                    failing += 1

            # Last replay and outcome timestamps
            last_replay = await conn.fetchval(
                "SELECT MAX(completed_at) FROM replay_runs WHERE status = 'completed'"
            )
            last_outcome = await conn.fetchval(
                "SELECT MAX(evaluated_at) FROM signal_outcomes"
            )

            # Data freshness (hours since last archived signal)
            last_archived = await conn.fetchval(
                "SELECT MAX(archived_at) FROM signals_registry"
            )
            if last_archived:
                now = datetime.now(timezone.utc)
                freshness_h = (now - last_archived).total_seconds() / 3600.0
            else:
                freshness_h = float("inf")

            # Active alerts
            active_alerts = await conn.fetchval(
                "SELECT COUNT(*) FROM validation_alerts WHERE is_active = TRUE"
            ) or 0

        queue_depth = 0
        if self._redis:
            try:
                queue_depth = await self._redis.llen("replay:queue") or 0
            except Exception:
                pass

        coverage_pct = (evaluated / total) if total > 0 else 0.0

        # Derive status
        if failing > 0 or freshness_h > 48 or coverage_pct < 0.20:
            status = "critical"
        elif degraded > 0 or freshness_h > 12 or coverage_pct < 0.40 or queue_depth > 10:
            status = "degraded"
        else:
            status = "healthy"

        return SystemHealth(
            overall_precision=_maybe_round(agg["avg_precision"]),
            overall_recall=_maybe_round(agg["avg_recall"]),
            overall_f1=_maybe_round(agg["avg_f1"]),
            coverage_pct=round(coverage_pct, 4),
            total_signals=total,
            evaluated_signals=evaluated,
            engines_healthy=healthy,
            engines_degraded=degraded,
            engines_failing=failing,
            last_replay_at=last_replay,
            last_outcome_at=last_outcome,
            queue_depth=queue_depth,
            data_freshness_hours=round(freshness_h, 2),
            active_alerts=active_alerts,
            status=status,
        )

    # ── Engine Decay ───────────────────────────────────────────────────────────

    async def get_engine_decay(
        self,
        window_hours: int = 24,
        weeks: int = 8,
    ) -> List[EngineDecayMetrics]:
        async with self._db.acquire() as conn:
            # Get all engines
            engines = await conn.fetch(
                "SELECT DISTINCT engine_name FROM signals_registry ORDER BY engine_name"
            )

            # Latest half_life + predictive_strength from signal_scores
            scores = await conn.fetch("""
                SELECT DISTINCT ON (engine_name)
                    engine_name,
                    half_life_hours,
                    predictive_strength
                FROM signal_scores
                WHERE window_hours = $1
                ORDER BY engine_name, computed_at DESC
            """, window_hours)
            score_map = {r["engine_name"]: r for r in scores}

            # Weekly trend from replay_metrics
            trend_rows = await conn.fetch("""
                SELECT
                    engine_name,
                    date_trunc('week', computed_at) AS week_start,
                    AVG(directional_accuracy)       AS dir_acc,
                    AVG(precision_score)            AS prec,
                    COUNT(*)                        AS samples
                FROM replay_metrics
                WHERE window_hours = $1
                  AND computed_at >= NOW() - ($2 || ' weeks')::INTERVAL
                GROUP BY engine_name, week_start
                ORDER BY engine_name, week_start
            """, window_hours, str(weeks))

            trend_map: Dict[str, List] = {}
            for row in trend_rows:
                trend_map.setdefault(row["engine_name"], []).append(row)

            # Latest precision/recall
            latest_metrics = await conn.fetch("""
                SELECT DISTINCT ON (engine_name)
                    engine_name,
                    precision_score,
                    recall_score
                FROM replay_metrics
                WHERE window_hours = $1
                ORDER BY engine_name, computed_at DESC
            """, window_hours)
            latest_map = {r["engine_name"]: r for r in latest_metrics}

        results = []
        for eng_row in engines:
            name = eng_row["engine_name"]
            score = score_map.get(name)
            trend_data = trend_map.get(name, [])
            latest = latest_map.get(name)

            weekly_points = [
                WeeklyAccuracyPoint(
                    week_start=r["week_start"],
                    directional_accuracy=float(r["dir_acc"]) if r["dir_acc"] is not None else 0.0,
                    precision=float(r["prec"]) if r["prec"] is not None else None,
                    sample_count=int(r["samples"]),
                )
                for r in trend_data
            ]

            decay_rate, is_decaying = _compute_slope(
                [p.directional_accuracy for p in weekly_points]
            )

            results.append(EngineDecayMetrics(
                engine_name=name,
                half_life_hours=float(score["half_life_hours"]) if score and score["half_life_hours"] else None,
                predictive_strength=float(score["predictive_strength"]) if score and score["predictive_strength"] else None,
                accuracy_trend=weekly_points,
                is_decaying=is_decaying,
                decay_rate=round(decay_rate, 4),
                latest_precision=float(latest["precision_score"]) if latest and latest["precision_score"] else None,
                latest_recall=float(latest["recall_score"]) if latest and latest["recall_score"] else None,
            ))

        return results

    # ── Calibration Analysis ───────────────────────────────────────────────────

    async def get_calibration_analysis(self, days: int = 30) -> List[CalibrationAnalysis]:
        async with self._db.acquire() as conn:
            engines = await conn.fetch(
                "SELECT DISTINCT engine_name FROM signals_registry ORDER BY engine_name"
            )

            rows = await conn.fetch("""
                SELECT
                    sr.engine_name,
                    sr.confidence_level,
                    AVG(so.direction_correct::int) AS actual_accuracy,
                    COUNT(*)                       AS samples
                FROM signal_outcomes so
                JOIN signals_registry sr ON sr.id = so.signal_id
                WHERE so.evaluated_at >= NOW() - ($1 || ' days')::INTERVAL
                  AND sr.confidence_level IS NOT NULL
                  AND so.direction_correct IS NOT NULL
                GROUP BY sr.engine_name, sr.confidence_level
                ORDER BY sr.engine_name, sr.confidence_level
            """, str(days))

        # Group by engine
        engine_buckets: Dict[str, List] = {}
        for row in rows:
            engine_buckets.setdefault(row["engine_name"], []).append(row)

        results = []
        for eng_row in engines:
            name = eng_row["engine_name"]
            bucket_rows = engine_buckets.get(name, [])

            buckets: List[ConfidenceBucket] = []
            weighted_error = 0.0
            total_weight = 0.0
            weights = {"high": 1.0, "medium": 0.5, "low": 0.25}

            for br in bucket_rows:
                lvl = br["confidence_level"]
                target = _CALIBRATION_TARGETS.get(lvl, 0.65)
                actual = float(br["actual_accuracy"]) if br["actual_accuracy"] is not None else 0.0
                err = abs(target - actual)
                w = weights.get(lvl, 0.5) * int(br["samples"])
                weighted_error += err * w
                total_weight += w
                buckets.append(ConfidenceBucket(
                    confidence_level=lvl,
                    expected_accuracy=target,
                    actual_accuracy=round(actual, 4),
                    sample_count=int(br["samples"]),
                    error=round(err, 4),
                ))

            cal_error = (weighted_error / total_weight) if total_weight > 0 else 0.0
            results.append(CalibrationAnalysis(
                engine_name=name,
                calibration_error=round(cal_error, 4),
                is_well_calibrated=cal_error < _WELL_CALIBRATED_THRESHOLD,
                buckets=buckets,
            ))

        return results

    # ── Failure Analysis ───────────────────────────────────────────────────────

    async def get_failure_analysis(self, days: int = 7) -> FailureAnalysis:
        async with self._db.acquire() as conn:
            failed_24h = await conn.fetchval("""
                SELECT COUNT(*) FROM replay_runs
                WHERE status IN ('failed', 'error')
                  AND updated_at >= NOW() - INTERVAL '24 hours'
            """) or 0

            failed_7d = await conn.fetchval("""
                SELECT COUNT(*) FROM replay_runs
                WHERE status IN ('failed', 'error')
                  AND updated_at >= NOW() - ($1 || ' days')::INTERVAL
            """, str(days)) or 0

            retry_exhausted = await conn.fetchval("""
                SELECT COUNT(*) FROM replay_runs
                WHERE status = 'failed'
                  AND retry_count >= 3
                  AND updated_at >= NOW() - ($1 || ' days')::INTERVAL
            """, str(days)) or 0

            # Engines with signals but no outcomes
            no_outcome_rows = await conn.fetch("""
                SELECT DISTINCT sr.engine_name
                FROM signals_registry sr
                WHERE NOT EXISTS (
                    SELECT 1 FROM signal_outcomes so WHERE so.signal_id = sr.id
                )
                ORDER BY sr.engine_name
            """)
            engines_no_outcomes = [r["engine_name"] for r in no_outcome_rows]

            uncovered = await conn.fetchval("""
                SELECT COUNT(*) FROM signals_registry sr
                WHERE NOT EXISTS (
                    SELECT 1 FROM signal_outcomes so WHERE so.signal_id = sr.id
                )
            """) or 0

            # Pending per evaluation window
            pending_rows = await conn.fetch("""
                SELECT window_hours, COUNT(*) AS cnt
                FROM (
                    SELECT unnest(ARRAY[1,6,24,72,168]) AS window_hours
                ) w,
                LATERAL (
                    SELECT sr.id AS signal_id
                    FROM signals_registry sr
                    WHERE sr.emitted_at <= NOW() - (w.window_hours || ' hours')::INTERVAL
                      AND NOT EXISTS (
                          SELECT 1 FROM signal_outcomes so
                           WHERE so.signal_id = sr.id
                             AND so.evaluation_window_h = w.window_hours
                      )
                ) pending
                GROUP BY window_hours
                ORDER BY window_hours
            """)
            pending_map = {int(r["window_hours"]): int(r["cnt"]) for r in pending_rows}

            oldest_row = await conn.fetchrow("""
                SELECT MIN(archived_at) AS oldest
                FROM signals_registry sr
                WHERE NOT EXISTS (
                    SELECT 1 FROM signal_outcomes so WHERE so.signal_id = sr.id
                )
            """)
            oldest_hours: Optional[float] = None
            if oldest_row and oldest_row["oldest"]:
                now = datetime.now(timezone.utc)
                oldest_hours = round((now - oldest_row["oldest"]).total_seconds() / 3600, 1)

        return FailureAnalysis(
            failed_replays_24h=int(failed_24h),
            failed_replays_7d=int(failed_7d),
            retry_exhausted_7d=int(retry_exhausted),
            engines_with_no_outcomes=engines_no_outcomes,
            uncovered_signal_count=int(uncovered),
            pending_outcome_windows=pending_map,
            oldest_uncovered_signal_hours=oldest_hours,
        )

    # ── Reliability Summary ────────────────────────────────────────────────────

    async def get_reliability_summary(
        self,
        window_hours: int = 24,
        days: int = 30,
    ) -> ReliabilitySummary:
        async with self._db.acquire() as conn:
            # Engine ranking
            ranking_rows = await conn.fetch("""
                SELECT
                    rm.engine_name,
                    COALESCE(
                        AVG(ss.predictive_strength),
                        GREATEST(0, (AVG(rm.directional_accuracy) - 0.5) * 200)
                    )                            AS pred_strength,
                    AVG(rm.directional_accuracy) AS dir_acc,
                    AVG(rm.precision_score)      AS avg_prec,
                    AVG(rm.f1_score)             AS avg_f1
                FROM replay_metrics rm
                LEFT JOIN signal_scores ss
                    ON ss.engine_name = rm.engine_name
                   AND ss.window_hours = rm.window_hours
                   AND ss.computed_at >= NOW() - INTERVAL '7 days'
                WHERE rm.window_hours = $1
                  AND rm.computed_at >= NOW() - ($2 || ' days')::INTERVAL
                GROUP BY rm.engine_name
                ORDER BY pred_strength DESC NULLS LAST
            """, window_hours, str(days))

            # Regime accuracy aggregation
            regime_rows = await conn.fetch("""
                SELECT
                    kv.key   AS regime,
                    AVG((kv.value->>'accuracy')::numeric) AS avg_acc,
                    SUM((kv.value->>'sample_count')::int)  AS total_samples
                FROM replay_metrics,
                     jsonb_each(regime_breakdown) AS kv
                WHERE computed_at >= NOW() - ($1 || ' days')::INTERVAL
                  AND jsonb_typeof(regime_breakdown) = 'object'
                GROUP BY kv.key
                HAVING SUM((kv.value->>'sample_count')::int) >= 10
                ORDER BY avg_acc DESC
            """, str(days))

            best_regime  = regime_rows[0]["regime"]  if regime_rows else None
            worst_regime = regime_rows[-1]["regime"] if regime_rows else None

            # Narrative accuracy aggregation
            narr_rows = await conn.fetch("""
                SELECT
                    kv.key   AS narrative,
                    AVG((kv.value->>'accuracy')::numeric) AS avg_acc,
                    SUM((kv.value->>'sample_count')::int)  AS total_samples
                FROM replay_metrics,
                     jsonb_each(narrative_breakdown) AS kv
                WHERE computed_at >= NOW() - ($1 || ' days')::INTERVAL
                  AND jsonb_typeof(narrative_breakdown) = 'object'
                GROUP BY kv.key
                HAVING SUM((kv.value->>'sample_count')::int) >= 5
                ORDER BY avg_acc DESC
            """, str(days))

            best_narrative  = narr_rows[0]["narrative"]  if narr_rows else None
            worst_narrative = narr_rows[-1]["narrative"] if narr_rows else None

            # Overall F1 + directional accuracy
            overall = await conn.fetchrow("""
                SELECT AVG(f1_score) AS f1, AVG(directional_accuracy) AS dir_acc
                FROM replay_metrics
                WHERE window_hours = $1
                  AND computed_at >= NOW() - ($2 || ' days')::INTERVAL
            """, window_hours, str(days))

        engine_ranking = [
            EngineRankEntry(
                engine_name=r["engine_name"],
                rank=i + 1,
                predictive_strength=float(r["pred_strength"]) if r["pred_strength"] is not None else 0.0,
                directional_accuracy=_maybe_round(r["dir_acc"]),
                precision=_maybe_round(r["avg_prec"]),
                f1_score=_maybe_round(r["avg_f1"]),
            )
            for i, r in enumerate(ranking_rows)
        ]

        return ReliabilitySummary(
            engine_ranking=engine_ranking,
            best_regime=best_regime,
            worst_regime=worst_regime,
            best_narrative=best_narrative,
            worst_narrative=worst_narrative,
            overall_f1=_maybe_round(overall["f1"]) if overall else None,
            overall_directional_accuracy=_maybe_round(overall["dir_acc"]) if overall else None,
        )


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _maybe_round(v, digits: int = 4) -> Optional[float]:
    return round(float(v), digits) if v is not None else None


def _compute_slope(values: List[float]) -> tuple[float, bool]:
    """Ordinary least squares slope over a list of values. Returns (slope, is_decaying)."""
    n = len(values)
    if n < 2:
        return 0.0, False
    xs = list(range(n))
    x_mean = sum(xs) / n
    y_mean = sum(values) / n
    num = sum((x - x_mean) * (y - y_mean) for x, y in zip(xs, values))
    den = sum((x - x_mean) ** 2 for x in xs)
    slope = (num / den) if den != 0 else 0.0
    return slope, slope < _DECAY_SLOPE_THRESHOLD
