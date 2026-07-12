"""
Replay Observability Dashboard — Phase 3

Read-only analytics for replay system health, engine decay curves,
throughput metrics, queue analytics, and outcome coverage tracking.
Zero writes. Used by validation_intelligence routes.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional

import asyncpg


# ─── Data Classes ─────────────────────────────────────────────────────────────

@dataclass
class ReplayHealthSnapshot:
    total_runs: int
    completed_runs: int
    failed_runs: int
    pending_runs: int
    running_runs: int
    success_rate: float
    avg_duration_seconds: Optional[float]
    p95_duration_seconds: Optional[float]
    last_completed_at: Optional[datetime]
    last_failed_at: Optional[datetime]


@dataclass
class DailyThroughput:
    date: datetime
    jobs_completed: int
    jobs_failed: int
    signals_evaluated: int
    engines_covered: int


@dataclass
class EngineDecayCurvePoint:
    week_start: datetime
    predictive_strength: float
    sample_count: int


@dataclass
class QueueAnalytics:
    queue_depth: int
    processing_locks_active: int
    oldest_queued_job_minutes: Optional[float]
    auto_schedule_enabled: bool


@dataclass
class CoverageRow:
    engine_name: str
    window_hours: int
    total_signals: int
    evaluated_signals: int
    coverage_pct: float


# ─── Dashboard ────────────────────────────────────────────────────────────────

class ReplayObservabilityDashboard:
    """Analytics aggregator for replay system observability."""

    def __init__(self, db: asyncpg.Pool, redis=None):
        self._db = db
        self._redis = redis

    # ── Replay Health ──────────────────────────────────────────────────────────

    async def get_replay_health(self, days: int = 30) -> ReplayHealthSnapshot:
        async with self._db.acquire() as conn:
            row = await conn.fetchrow("""
                SELECT
                    COUNT(*)                                            AS total,
                    COUNT(*) FILTER (WHERE status = 'completed')       AS completed,
                    COUNT(*) FILTER (WHERE status IN ('failed','error'))AS failed,
                    COUNT(*) FILTER (WHERE status = 'queued')          AS pending,
                    COUNT(*) FILTER (WHERE status = 'running')         AS running,
                    AVG(EXTRACT(EPOCH FROM (completed_at - started_at)))
                        FILTER (WHERE status = 'completed'
                                  AND completed_at IS NOT NULL
                                  AND started_at IS NOT NULL)          AS avg_dur,
                    MAX(completed_at) FILTER (WHERE status = 'completed') AS last_ok,
                    MAX(updated_at)   FILTER (WHERE status IN ('failed','error')) AS last_fail
                FROM replay_runs
                WHERE created_at >= NOW() - ($1 || ' days')::INTERVAL
            """, str(days))

            # p95 duration
            p95_row = await conn.fetchrow("""
                SELECT PERCENTILE_CONT(0.95) WITHIN GROUP (
                    ORDER BY EXTRACT(EPOCH FROM (completed_at - started_at))
                ) AS p95
                FROM replay_runs
                WHERE status = 'completed'
                  AND completed_at IS NOT NULL
                  AND started_at IS NOT NULL
                  AND created_at >= NOW() - ($1 || ' days')::INTERVAL
            """, str(days))

        total = int(row["total"] or 0)
        completed = int(row["completed"] or 0)
        failed = int(row["failed"] or 0)
        success_rate = (completed / total) if total > 0 else 0.0

        return ReplayHealthSnapshot(
            total_runs=total,
            completed_runs=completed,
            failed_runs=failed,
            pending_runs=int(row["pending"] or 0),
            running_runs=int(row["running"] or 0),
            success_rate=round(success_rate, 4),
            avg_duration_seconds=float(row["avg_dur"]) if row["avg_dur"] else None,
            p95_duration_seconds=float(p95_row["p95"]) if p95_row and p95_row["p95"] else None,
            last_completed_at=row["last_ok"],
            last_failed_at=row["last_fail"],
        )

    # ── Throughput ─────────────────────────────────────────────────────────────

    async def get_replay_throughput(self, days: int = 30) -> List[DailyThroughput]:
        async with self._db.acquire() as conn:
            rows = await conn.fetch("""
                SELECT
                    DATE(completed_at)                             AS day,
                    COUNT(*) FILTER (WHERE status = 'completed')  AS completed,
                    COUNT(*) FILTER (WHERE status IN ('failed','error')) AS failed,
                    SUM(evaluated_signals) FILTER (WHERE status = 'completed') AS sigs,
                    COUNT(DISTINCT engine_name) FILTER (WHERE status = 'completed') AS engines
                FROM replay_runs
                WHERE created_at >= NOW() - ($1 || ' days')::INTERVAL
                GROUP BY day
                ORDER BY day
            """, str(days))

        return [
            DailyThroughput(
                date=datetime.combine(r["day"], datetime.min.time(), tzinfo=timezone.utc),
                jobs_completed=int(r["completed"] or 0),
                jobs_failed=int(r["failed"] or 0),
                signals_evaluated=int(r["sigs"] or 0),
                engines_covered=int(r["engines"] or 0),
            )
            for r in rows
        ]

    # ── Engine Decay Curves ────────────────────────────────────────────────────

    async def get_engine_decay_curves(
        self,
        window_hours: int = 24,
        weeks: int = 12,
    ) -> Dict[str, List[EngineDecayCurvePoint]]:
        async with self._db.acquire() as conn:
            rows = await conn.fetch("""
                SELECT
                    engine_name,
                    date_trunc('week', computed_at)   AS week_start,
                    AVG(COALESCE(
                        (SELECT predictive_strength
                           FROM signal_scores ss
                          WHERE ss.engine_name = rm.engine_name
                            AND ss.window_hours = rm.window_hours
                            AND ss.computed_at  <= rm.computed_at
                          ORDER BY ss.computed_at DESC
                          LIMIT 1),
                        GREATEST(0, (AVG(rm.directional_accuracy) - 0.5) * 200)
                    ))                                AS pred_strength,
                    COUNT(*)                          AS samples
                FROM replay_metrics rm
                WHERE window_hours = $1
                  AND computed_at >= NOW() - ($2 || ' weeks')::INTERVAL
                GROUP BY engine_name, week_start
                ORDER BY engine_name, week_start
            """, window_hours, str(weeks))

        result: Dict[str, List[EngineDecayCurvePoint]] = {}
        for row in rows:
            name = row["engine_name"]
            result.setdefault(name, []).append(EngineDecayCurvePoint(
                week_start=row["week_start"],
                predictive_strength=float(row["pred_strength"]) if row["pred_strength"] else 0.0,
                sample_count=int(row["samples"]),
            ))
        return result

    # ── Queue Analytics ────────────────────────────────────────────────────────

    async def get_queue_analytics(self) -> QueueAnalytics:
        queue_depth = 0
        processing_locks = 0
        auto_schedule = True

        if self._redis:
            try:
                queue_depth = await self._redis.llen("replay:queue") or 0
                # Count processing locks (pattern scan is O(N) but list is small)
                keys = []
                async for key in self._redis.scan_iter("replay:processing:*"):
                    keys.append(key)
                processing_locks = len(keys)
            except Exception:
                pass

        # Oldest pending (queued) job age from DB
        async with self._db.acquire() as conn:
            oldest_row = await conn.fetchrow("""
                SELECT MIN(created_at) AS oldest
                FROM replay_runs
                WHERE status = 'queued'
            """)

        oldest_minutes: Optional[float] = None
        if oldest_row and oldest_row["oldest"]:
            now = datetime.now(timezone.utc)
            oldest_minutes = round(
                (now - oldest_row["oldest"]).total_seconds() / 60, 1
            )

        return QueueAnalytics(
            queue_depth=queue_depth,
            processing_locks_active=processing_locks,
            oldest_queued_job_minutes=oldest_minutes,
            auto_schedule_enabled=auto_schedule,
        )

    # ── Coverage Tracking ──────────────────────────────────────────────────────

    async def get_coverage_tracking(self, days: int = 7) -> List[CoverageRow]:
        async with self._db.acquire() as conn:
            rows = await conn.fetch("""
                WITH windows AS (
                    SELECT unnest(ARRAY[1,6,24,72,168]) AS window_hours
                ),
                engine_totals AS (
                    SELECT engine_name, COUNT(*) AS total
                    FROM signals_registry
                    WHERE archived_at >= NOW() - ($1 || ' days')::INTERVAL
                    GROUP BY engine_name
                ),
                engine_evaluated AS (
                    SELECT sr.engine_name, so.evaluation_window_h, COUNT(*) AS evaluated
                    FROM signal_outcomes so
                    JOIN signals_registry sr ON sr.id = so.signal_id
                    WHERE sr.archived_at >= NOW() - ($1 || ' days')::INTERVAL
                    GROUP BY sr.engine_name, so.evaluation_window_h
                )
                SELECT
                    et.engine_name,
                    w.window_hours,
                    et.total,
                    COALESCE(ee.evaluated, 0) AS evaluated
                FROM engine_totals et
                CROSS JOIN windows w
                LEFT JOIN engine_evaluated ee
                    ON ee.engine_name = et.engine_name
                   AND ee.evaluation_window_h = w.window_hours
                ORDER BY et.engine_name, w.window_hours
            """, str(days))

        return [
            CoverageRow(
                engine_name=r["engine_name"],
                window_hours=int(r["window_hours"]),
                total_signals=int(r["total"]),
                evaluated_signals=int(r["evaluated"]),
                coverage_pct=round(
                    int(r["evaluated"]) / int(r["total"]) if int(r["total"]) > 0 else 0.0,
                    4,
                ),
            )
            for r in rows
        ]

    # ── Full Observability Bundle ──────────────────────────────────────────────

    async def get_full_observability(
        self,
        window_hours: int = 24,
        days: int = 30,
        weeks: int = 12,
    ) -> Dict[str, Any]:
        health = await self.get_replay_health(days=days)
        throughput = await self.get_replay_throughput(days=days)
        decay_curves = await self.get_engine_decay_curves(window_hours=window_hours, weeks=weeks)
        queue = await self.get_queue_analytics()
        coverage = await self.get_coverage_tracking(days=7)

        return {
            "replay_health": _asdict(health),
            "throughput": [_asdict(t) for t in throughput],
            "engine_decay_curves": {
                name: [_asdict(p) for p in points]
                for name, points in decay_curves.items()
            },
            "queue_analytics": _asdict(queue),
            "coverage_tracking": [_asdict(c) for c in coverage],
        }


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _asdict(obj) -> Dict[str, Any]:
    """Convert a dataclass to a JSON-serialisable dict."""
    import dataclasses
    if dataclasses.is_dataclass(obj):
        result = {}
        for f in dataclasses.fields(obj):
            v = getattr(obj, f.name)
            if isinstance(v, datetime):
                result[f.name] = v.isoformat()
            elif dataclasses.is_dataclass(v):
                result[f.name] = _asdict(v)
            elif isinstance(v, list):
                result[f.name] = [_asdict(i) if dataclasses.is_dataclass(i) else i for i in v]
            else:
                result[f.name] = v
        return result
    return obj
