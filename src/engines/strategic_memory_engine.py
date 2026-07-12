"""
Strategic Memory Engine — Phase 5

Orchestrates the Meta-Intelligence layer. Persists long-term system behavior
summaries and coordinates the regime transition intelligence, narrative lifecycle,
and failure analysis sub-engines.

Responsibilities:
  - Detect and log significant system behavior events to strategic_memory_events
  - Track multi-month historical behavior: regime transitions, degradation patterns,
    calibration failures, recovery milestones
  - Build recurring pattern index via cluster IDs
  - Produce daily meta-intelligence snapshots (strategic_instability_index, etc.)
  - Orchestrate RegimeTransitionIntelligence, NarrativeLifecycleEngine,
    StrategicFailureAnalysisEngine each cycle

Standalone service on port 8018; writes to 5 Phase-5 tables.
"""
from __future__ import annotations

import asyncio
import json
import os
import traceback
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from statistics import mean, stdev
from typing import Any, Dict, List, Optional, Tuple

import asyncpg
from prometheus_client import Counter, Gauge, Histogram

from src.core import http_server as _http
from src.core.logging import get_logger

logger = get_logger(__name__)

# ─── Prometheus Metrics ───────────────────────────────────────────────────────
meta_cycles_total = Counter(
    "meta_intelligence_cycles_total",
    "Total meta-intelligence cycles completed",
)
meta_cycle_seconds = Histogram(
    "meta_intelligence_cycle_seconds",
    "Duration of meta-intelligence cycle",
    buckets=[5.0, 15.0, 30.0, 60.0, 120.0, 300.0, 600.0],
)
meta_errors_total = Counter(
    "meta_intelligence_errors_total",
    "Errors in meta-intelligence cycles",
)
strategic_instability_gauge = Gauge(
    "strategic_instability_index",
    "System-wide strategic instability index (0–100)",
)
recurring_failure_density_gauge = Gauge(
    "recurring_failure_density",
    "Density of recurring failure patterns (0–100)",
)
governance_stress_gauge = Gauge(
    "governance_stress_score",
    "Governance stress score (0–100)",
)
memory_events_gauge = Gauge(
    "strategic_memory_events_30d",
    "Strategic memory events logged in last 30 days",
)


# ─── Constants ────────────────────────────────────────────────────────────────
_TRUST_DROP_THRESHOLD      = 15.0   # trust drop magnitude that qualifies as event
_CALIB_FAILURE_THRESHOLD   = 0.20   # calibration error above which = failure
_PRECISION_COLLAPSE_THRESH = 0.45   # precision below this = collapse
_INSTABILITY_CRITICAL      = 70.0
_INSTABILITY_HIGH          = 50.0
_STRESS_CRITICAL           = 75.0
_MEMORY_WINDOW_DAYS        = 180    # 6-month memory window


# ─── Data Classes ─────────────────────────────────────────────────────────────

@dataclass
class MemoryCycleResult:
    events_logged: int
    transitions_processed: int
    narratives_processed: int
    failure_clusters_updated: int
    snapshot_at: datetime
    strategic_instability_index: float
    governance_stress_score: float
    recurring_failure_density: float
    cycle_duration_s: float


# ─── Engine ───────────────────────────────────────────────────────────────────

class StrategicMemoryEngine:
    """
    Orchestrates the Phase 5 meta-intelligence layer.
    Call run_cycle() periodically; all outputs are additive DB writes.
    """

    def __init__(
        self,
        db: asyncpg.Pool,
        redis=None,
        memory_window_days: int = _MEMORY_WINDOW_DAYS,
    ):
        self._db = db
        self._redis = redis
        self._memory_window_days = memory_window_days

    # ── Main cycle ─────────────────────────────────────────────────────────────

    async def run_cycle(self) -> MemoryCycleResult:
        import time
        t0 = time.monotonic()
        events_logged = 0
        transitions_processed = 0
        narratives_processed = 0
        clusters_updated = 0

        with meta_cycle_seconds.time():
            try:
                # 1. Detect and log trust score drop events
                events_logged += await self._detect_trust_drop_events()

                # 2. Detect calibration failure events
                events_logged += await self._detect_calibration_failure_events()

                # 3. Detect precision collapse events
                events_logged += await self._detect_precision_collapse_events()

                # 4. Detect recovery milestones
                events_logged += await self._detect_recovery_milestones()

                # 5. Detect regime transition events (from macro_regimes table)
                transitions_processed = await self._detect_regime_transition_events()

                # 6. Compute and persist meta snapshot
                snapshot = await self._compute_meta_snapshot()
                await self._persist_snapshot(snapshot)

                # Update Prometheus
                strategic_instability_gauge.set(snapshot["strategic_instability_index"])
                recurring_failure_density_gauge.set(snapshot["recurring_failure_density"])
                governance_stress_gauge.set(snapshot["governance_stress_score"])
                memory_events_gauge.set(snapshot["total_memory_events_30d"])

                meta_cycles_total.inc()

            except Exception:
                meta_errors_total.inc()
                logger.error("meta_intelligence_cycle_error", exc=traceback.format_exc())

        duration = round(time.monotonic() - t0, 2)
        return MemoryCycleResult(
            events_logged=events_logged,
            transitions_processed=transitions_processed,
            narratives_processed=narratives_processed,
            failure_clusters_updated=clusters_updated,
            snapshot_at=datetime.now(timezone.utc),
            strategic_instability_index=snapshot.get("strategic_instability_index", 0),
            governance_stress_score=snapshot.get("governance_stress_score", 0),
            recurring_failure_density=snapshot.get("recurring_failure_density", 0),
            cycle_duration_s=duration,
        )

    # ── Event Detection ────────────────────────────────────────────────────────

    async def _detect_trust_drop_events(self) -> int:
        """Log trust score drop events for engines that dropped significantly."""
        async with self._db.acquire() as conn:
            rows = await conn.fetch("""
                WITH ranked AS (
                    SELECT
                        engine_name,
                        trust_score,
                        computed_at,
                        health_status,
                        LAG(trust_score) OVER (PARTITION BY engine_name ORDER BY computed_at) AS prev_trust,
                        LAG(computed_at) OVER (PARTITION BY engine_name ORDER BY computed_at) AS prev_at
                    FROM engine_trust_scores
                    WHERE computed_at >= NOW() - INTERVAL '7 days'
                )
                SELECT engine_name, trust_score, prev_trust, computed_at, health_status
                FROM ranked
                WHERE prev_trust IS NOT NULL
                  AND (prev_trust - trust_score) >= $1
                  AND computed_at >= NOW() - INTERVAL '26 hours'
                ORDER BY (prev_trust - trust_score) DESC
                LIMIT 20
            """, _TRUST_DROP_THRESHOLD)

        count = 0
        for row in rows:
            drop = float(row["prev_trust"]) - float(row["trust_score"])
            severity = "critical" if drop >= 30 else ("high" if drop >= 20 else "medium")
            cluster_id = f"trust_drop_{row['engine_name']}"
            recurrence = await self._get_recurrence_index(cluster_id)
            await self._log_event(
                event_type="trust_score_drop",
                severity=severity,
                engine_name=row["engine_name"],
                cluster_id=cluster_id,
                recurrence_index=recurrence,
                event_data={
                    "trust_score_before": float(row["prev_trust"]),
                    "trust_score_after": float(row["trust_score"]),
                    "drop_magnitude": round(drop, 2),
                    "health_status": row["health_status"],
                },
                metrics_snapshot={
                    "trust_score": float(row["trust_score"]),
                    "drop": round(drop, 2),
                },
            )
            count += 1
        return count

    async def _detect_calibration_failure_events(self) -> int:
        """Log calibration failure events from signal_scores."""
        async with self._db.acquire() as conn:
            rows = await conn.fetch("""
                SELECT DISTINCT ON (engine_name)
                    engine_name, calibration_error, predictive_strength, computed_at
                FROM signal_scores
                WHERE window_hours = 24
                  AND calibration_error >= $1
                  AND computed_at >= NOW() - INTERVAL '26 hours'
                ORDER BY engine_name, computed_at DESC
            """, _CALIB_FAILURE_THRESHOLD)

        count = 0
        for row in rows:
            err = float(row["calibration_error"])
            severity = "critical" if err >= 0.35 else ("high" if err >= 0.25 else "medium")
            cluster_id = f"calib_failure_{row['engine_name']}"
            async with self._db.acquire() as conn:
                recent = await conn.fetchval(
                    "SELECT 1 FROM strategic_memory_events WHERE cluster_id=$1 "
                    "AND occurred_at >= NOW() - INTERVAL '24 hours' LIMIT 1",
                    cluster_id,
                )
            if recent:
                continue  # already recorded this ongoing failure today
            recurrence = await self._get_recurrence_index(cluster_id)
            await self._log_event(
                event_type="calibration_failure",
                severity=severity,
                engine_name=row["engine_name"],
                cluster_id=cluster_id,
                recurrence_index=recurrence,
                event_data={
                    "calibration_error": round(err, 4),
                    "threshold": _CALIB_FAILURE_THRESHOLD,
                    "predictive_strength": float(row["predictive_strength"] or 0),
                },
                metrics_snapshot={"calibration_error": round(err, 4)},
            )
            count += 1
        return count

    async def _detect_precision_collapse_events(self) -> int:
        """Log precision collapse events from recent replay metrics."""
        async with self._db.acquire() as conn:
            rows = await conn.fetch("""
                SELECT DISTINCT ON (engine_name)
                    engine_name, precision_score, directional_accuracy,
                    f1_score, false_positive_rate, computed_at
                FROM replay_metrics
                WHERE window_hours = 24
                  AND precision_score < $1
                  AND computed_at >= NOW() - INTERVAL '26 hours'
                ORDER BY engine_name, computed_at DESC
                LIMIT 20
            """, _PRECISION_COLLAPSE_THRESH)

        count = 0
        for row in rows:
            prec = float(row["precision_score"] or 0)
            severity = "critical" if prec < 0.30 else ("high" if prec < 0.38 else "medium")
            cluster_id = f"precision_collapse_{row['engine_name']}"
            async with self._db.acquire() as conn:
                recent = await conn.fetchval(
                    "SELECT 1 FROM strategic_memory_events WHERE cluster_id=$1 "
                    "AND occurred_at >= NOW() - INTERVAL '24 hours' LIMIT 1",
                    cluster_id,
                )
            if recent:
                continue  # already recorded this ongoing failure today
            recurrence = await self._get_recurrence_index(cluster_id)
            await self._log_event(
                event_type="degradation_event",
                severity=severity,
                engine_name=row["engine_name"],
                cluster_id=cluster_id,
                recurrence_index=recurrence,
                event_data={
                    "precision_score": round(prec, 4),
                    "directional_accuracy": round(float(row["directional_accuracy"] or 0), 4),
                    "f1_score": round(float(row["f1_score"] or 0), 4),
                    "false_positive_rate": round(float(row["false_positive_rate"] or 0), 4),
                    "threshold": _PRECISION_COLLAPSE_THRESH,
                },
                metrics_snapshot={"precision_score": round(prec, 4)},
            )
            count += 1
        return count

    async def _detect_recovery_milestones(self) -> int:
        """Log recovery milestones when engines return to healthy status."""
        async with self._db.acquire() as conn:
            rows = await conn.fetch("""
                WITH ranked AS (
                    SELECT
                        engine_name,
                        trust_score,
                        health_status,
                        computed_at,
                        LAG(health_status) OVER (PARTITION BY engine_name ORDER BY computed_at) AS prev_status
                    FROM engine_trust_scores
                    WHERE computed_at >= NOW() - INTERVAL '48 hours'
                )
                SELECT engine_name, trust_score, health_status, computed_at
                FROM ranked
                WHERE prev_status IN ('degraded', 'failing')
                  AND health_status = 'healthy'
                  AND computed_at >= NOW() - INTERVAL '26 hours'
            """)

        count = 0
        for row in rows:
            await self._log_event(
                event_type="recovery_event",
                severity="low",
                engine_name=row["engine_name"],
                cluster_id=f"recovery_{row['engine_name']}",
                recurrence_index=1,
                event_data={
                    "recovered_trust_score": float(row["trust_score"]),
                    "new_health_status": "healthy",
                },
                metrics_snapshot={"trust_score": float(row["trust_score"])},
            )
            count += 1
        return count

    async def _detect_regime_transition_events(self) -> int:
        """Detect macro regime transitions and log as memory events."""
        async with self._db.acquire() as conn:
            rows = await conn.fetch("""
                WITH lag_regimes AS (
                    SELECT
                        regime,
                        computed_at,
                        LAG(regime) OVER (ORDER BY computed_at) AS prev_regime,
                        LAG(computed_at) OVER (ORDER BY computed_at) AS prev_at
                    FROM macro_regimes
                    WHERE computed_at >= NOW() - INTERVAL '48 hours'
                    ORDER BY computed_at
                )
                SELECT regime, prev_regime, computed_at, prev_at
                FROM lag_regimes
                WHERE prev_regime IS NOT NULL
                  AND regime != prev_regime
                  AND computed_at >= NOW() - INTERVAL '26 hours'
            """)

        count = 0
        for row in rows:
            cluster_id = f"regime_transition_{row['prev_regime']}_to_{row['regime']}"
            recurrence = await self._get_recurrence_index(cluster_id)
            severity = "high" if recurrence >= 3 else "medium"
            await self._log_event(
                event_type="regime_transition",
                severity=severity,
                engine_name=None,
                cluster_id=cluster_id,
                recurrence_index=recurrence,
                regime_context=row["regime"],
                event_data={
                    "from_regime": row["prev_regime"],
                    "to_regime": row["regime"],
                    "transition_at": row["computed_at"].isoformat(),
                    "recurrence": recurrence,
                },
                metrics_snapshot={"from": row["prev_regime"], "to": row["regime"]},
            )
            count += 1
        return count

    # ── Core Persistence ───────────────────────────────────────────────────────

    async def _get_recurrence_index(self, cluster_id: str) -> int:
        """Count how many times this cluster_id has been seen in memory."""
        async with self._db.acquire() as conn:
            count = await conn.fetchval("""
                SELECT COUNT(*) FROM strategic_memory_events
                WHERE cluster_id = $1
                  AND occurred_at >= NOW() - ($2 || ' days')::INTERVAL
            """, cluster_id, str(self._memory_window_days))
        return int(count or 0) + 1

    async def _log_event(
        self,
        event_type: str,
        severity: str,
        cluster_id: str,
        recurrence_index: int,
        event_data: Dict[str, Any],
        metrics_snapshot: Dict[str, Any],
        engine_name: Optional[str] = None,
        regime_context: Optional[str] = None,
        narrative_context: Optional[str] = None,
    ) -> None:
        """Append a strategic memory event."""
        async with self._db.acquire() as conn:
            await conn.execute("""
                INSERT INTO strategic_memory_events (
                    event_type, severity, engine_name, regime_context,
                    narrative_context, cluster_id, recurrence_index,
                    event_data, metrics_snapshot
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            """,
                event_type, severity, engine_name, regime_context,
                narrative_context, cluster_id, recurrence_index,
                json.dumps(event_data), json.dumps(metrics_snapshot),
            )

    # ── Meta Snapshot ──────────────────────────────────────────────────────────

    async def _compute_meta_snapshot(self) -> Dict[str, Any]:
        """Compute the system-wide meta-intelligence summary."""
        async with self._db.acquire() as conn:
            # Strategic instability: fraction of engines that are degraded/failing
            trust_rows = await conn.fetch("""
                SELECT DISTINCT ON (engine_name)
                    engine_name, trust_score, health_status, is_unstable,
                    degradation_velocity
                FROM engine_trust_scores
                WHERE window_hours = 24
                ORDER BY engine_name, computed_at DESC
            """)

            # Active failure clusters
            active_clusters = await conn.fetchval("""
                SELECT COUNT(*) FROM strategic_failure_clusters WHERE is_active = TRUE
            """) or 0

            # Dangerous transitions in last 30 days
            dangerous_transitions = await conn.fetchval("""
                SELECT COUNT(*) FROM regime_transition_records
                WHERE is_historically_dangerous = TRUE
                  AND detected_at >= NOW() - INTERVAL '30 days'
            """) or 0

            # Collapsing narratives
            collapsing_narratives = await conn.fetchval("""
                SELECT COUNT(DISTINCT narrative_name)
                FROM narrative_stability_scores
                WHERE is_collapsing = TRUE
                  AND computed_at >= NOW() - INTERVAL '7 days'
            """) or 0

            # Memory events in last 30 days
            memory_events_30d = await conn.fetchval("""
                SELECT COUNT(*) FROM strategic_memory_events
                WHERE occurred_at >= NOW() - INTERVAL '30 days'
            """) or 0

            # Calibration drift from signal_scores
            calib_rows = await conn.fetch("""
                SELECT DISTINCT ON (engine_name)
                    engine_name, calibration_error
                FROM signal_scores
                WHERE window_hours = 24
                ORDER BY engine_name, computed_at DESC
            """)

        # ── Strategic Instability Index ────────────────────────────────────────
        if trust_rows:
            unhealthy_frac = sum(
                1 for r in trust_rows
                if r["health_status"] in ("degraded", "failing")
            ) / len(trust_rows)
            avg_trust = mean([float(r["trust_score"]) for r in trust_rows])
            unstable_frac = sum(1 for r in trust_rows if r["is_unstable"]) / len(trust_rows)
            strategic_instability = min(100.0, (
                unhealthy_frac * 40 +
                max(0, (60 - avg_trust) / 60) * 35 +
                unstable_frac * 25
            ))
        else:
            strategic_instability = 0.0

        # ── Recurring Failure Density ──────────────────────────────────────────
        recurring_failure_density = min(100.0, (
            int(active_clusters) * 10.0 +
            int(dangerous_transitions) * 5.0 +
            int(collapsing_narratives) * 8.0
        ))

        # ── Governance Stress Score ────────────────────────────────────────────
        if calib_rows:
            avg_calib_error = mean([float(r["calibration_error"] or 0) for r in calib_rows])
            calib_stress = min(100.0, avg_calib_error * 300)
        else:
            calib_stress = 0.0

        decaying_engines = sum(
            1 for r in trust_rows
            if r.get("degradation_velocity") and float(r["degradation_velocity"]) < -0.03
        )
        governance_stress = min(100.0, (
            calib_stress * 0.4 +
            (decaying_engines / max(1, len(trust_rows))) * 60
        ))

        engine_summary = {
            r["engine_name"]: {
                "trust_score": float(r["trust_score"]),
                "health_status": r["health_status"],
                "is_unstable": r["is_unstable"],
            }
            for r in trust_rows
        }

        return {
            "strategic_instability_index": round(strategic_instability, 2),
            "recurring_failure_density": round(recurring_failure_density, 2),
            "governance_stress_score": round(governance_stress, 2),
            "strategic_stability_rank": round(max(0, 100 - strategic_instability), 2),
            "active_failure_clusters": int(active_clusters),
            "dangerous_transitions_30d": int(dangerous_transitions),
            "collapsing_narratives": int(collapsing_narratives),
            "degrading_engines": decaying_engines,
            "total_memory_events_30d": int(memory_events_30d),
            "engine_stability_summary": engine_summary,
        }

    async def _persist_snapshot(self, snapshot: Dict[str, Any]) -> None:
        async with self._db.acquire() as conn:
            await conn.execute("""
                INSERT INTO meta_intelligence_snapshots (
                    strategic_instability_index, recurring_failure_density,
                    governance_stress_score, strategic_stability_rank,
                    active_failure_clusters, dangerous_transitions_30d,
                    collapsing_narratives, degrading_engines,
                    total_memory_events_30d, engine_stability_summary
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            """,
                snapshot["strategic_instability_index"],
                snapshot["recurring_failure_density"],
                snapshot["governance_stress_score"],
                snapshot["strategic_stability_rank"],
                snapshot["active_failure_clusters"],
                snapshot["dangerous_transitions_30d"],
                snapshot["collapsing_narratives"],
                snapshot["degrading_engines"],
                snapshot["total_memory_events_30d"],
                json.dumps(snapshot["engine_stability_summary"]),
            )

    # ── Read / Report Methods ──────────────────────────────────────────────────

    async def get_historical_failure_atlas(
        self,
        days: int = 90,
        event_types: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """Historical Failure Atlas: clusters, frequencies, patterns."""
        async with self._db.acquire() as conn:
            rows = await conn.fetch("""
                SELECT
                    cluster_id,
                    event_type,
                    severity,
                    engine_name,
                    regime_context,
                    COUNT(*)           AS occurrence_count,
                    MIN(occurred_at)   AS first_seen,
                    MAX(occurred_at)   AS last_seen,
                    MAX(recurrence_index) AS max_recurrence
                FROM strategic_memory_events
                WHERE occurred_at >= NOW() - ($1 || ' days')::INTERVAL
                GROUP BY cluster_id, event_type, severity, engine_name, regime_context
                ORDER BY occurrence_count DESC, last_seen DESC
                LIMIT 100
            """, str(days))

        clusters: Dict[str, Any] = {}
        for r in rows:
            cid = r["cluster_id"]
            if cid not in clusters:
                clusters[cid] = {
                    "cluster_id": cid,
                    "event_type": r["event_type"],
                    "severity": r["severity"],
                    "affected_engine": r["engine_name"],
                    "regime_context": r["regime_context"],
                    "occurrence_count": int(r["occurrence_count"]),
                    "first_seen": r["first_seen"].isoformat() if r["first_seen"] else None,
                    "last_seen": r["last_seen"].isoformat() if r["last_seen"] else None,
                    "max_recurrence": int(r["max_recurrence"]),
                }

        return {
            "window_days": days,
            "total_clusters": len(clusters),
            "failure_atlas": list(clusters.values()),
            "summary": _atlas_summary(list(clusters.values())),
        }

    async def get_governance_stress_timeline(self, days: int = 90) -> List[Dict[str, Any]]:
        """Governance stress timeline from meta_intelligence_snapshots."""
        async with self._db.acquire() as conn:
            rows = await conn.fetch("""
                SELECT
                    snapshot_at,
                    strategic_instability_index,
                    governance_stress_score,
                    recurring_failure_density,
                    strategic_stability_rank,
                    active_failure_clusters,
                    collapsing_narratives,
                    degrading_engines
                FROM meta_intelligence_snapshots
                WHERE snapshot_at >= NOW() - ($1 || ' days')::INTERVAL
                ORDER BY snapshot_at DESC
                LIMIT 500
            """, str(days))

        return [
            {
                **{k: (float(v) if isinstance(v, (int, float)) else v) for k, v in dict(r).items()},
                "snapshot_at": r["snapshot_at"].isoformat(),
            }
            for r in rows
        ]

    async def get_strategic_stability_ranking(self) -> List[Dict[str, Any]]:
        """Long-term engine stability ranking from meta snapshots."""
        async with self._db.acquire() as conn:
            rows = await conn.fetch("""
                SELECT DISTINCT ON (engine_name)
                    engine_name,
                    trust_score,
                    health_status,
                    stability_score,
                    degradation_velocity,
                    noise_contribution,
                    recovery_capability,
                    computed_at
                FROM engine_trust_scores
                WHERE window_hours = 24
                ORDER BY engine_name, computed_at DESC
            """)

        result = [
            {
                **{k: (float(v) if isinstance(v, (int, float)) else v) for k, v in dict(r).items()},
                "computed_at": r["computed_at"].isoformat() if r["computed_at"] else None,
            }
            for r in rows
        ]
        result.sort(key=lambda x: (x.get("trust_score") or 0) - (
            abs(x.get("degradation_velocity") or 0) * 500
        ), reverse=True)
        for i, r in enumerate(result):
            r["strategic_rank"] = i + 1
        return result

    async def get_historical_similarity(
        self,
        current_instability: float,
        current_regime: str,
        top_n: int = 5,
    ) -> List[Dict[str, Any]]:
        """Find historical periods most similar to current system state."""
        async with self._db.acquire() as conn:
            rows = await conn.fetch("""
                SELECT
                    s.snapshot_at,
                    s.strategic_instability_index,
                    s.governance_stress_score,
                    s.recurring_failure_density,
                    s.strategic_stability_rank,
                    ABS(s.strategic_instability_index - $1) AS instability_dist,
                    me.regime_context
                FROM meta_intelligence_snapshots s
                LEFT JOIN LATERAL (
                    SELECT regime_context
                    FROM strategic_memory_events me
                    WHERE me.occurred_at BETWEEN s.snapshot_at - INTERVAL '1 hour'
                                             AND s.snapshot_at + INTERVAL '1 hour'
                      AND me.event_type = 'regime_transition'
                    LIMIT 1
                ) me ON TRUE
                WHERE s.snapshot_at >= NOW() - INTERVAL '180 days'
                ORDER BY instability_dist ASC
                LIMIT $2
            """, current_instability, top_n)

        return [
            {
                "snapshot_at": r["snapshot_at"].isoformat(),
                "strategic_instability_index": float(r["strategic_instability_index"]),
                "governance_stress_score": float(r["governance_stress_score"]),
                "recurring_failure_density": float(r["recurring_failure_density"]),
                "regime_context": r["regime_context"],
                "similarity_score": round(
                    max(0, 1.0 - float(r["instability_dist"]) / 100.0), 4
                ),
            }
            for r in rows
        ]


# ─── Helper ───────────────────────────────────────────────────────────────────

def _atlas_summary(clusters: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not clusters:
        return {"total": 0}
    by_type: Dict[str, int] = {}
    by_severity: Dict[str, int] = {}
    for c in clusters:
        t = c.get("event_type", "unknown")
        s = c.get("severity", "medium")
        by_type[t] = by_type.get(t, 0) + c.get("occurrence_count", 1)
        by_severity[s] = by_severity.get(s, 0) + 1

    recurring = sum(1 for c in clusters if c.get("occurrence_count", 1) > 1)
    return {
        "total": len(clusters),
        "recurring_patterns": recurring,
        "by_type": by_type,
        "by_severity": by_severity,
        "most_frequent": max(clusters, key=lambda c: c.get("occurrence_count", 0)).get("cluster_id")
        if clusters else None,
    }


# ─── Standalone Entry Point ───────────────────────────────────────────────────

async def _main() -> None:
    from src.core.logging import configure_logging
    from src.db.connection import get_db_pool
    from src.cache.redis_client import get_redis
    from src.engines.regime_transition_intelligence import RegimeTransitionIntelligence
    from src.engines.narrative_lifecycle_engine import NarrativeLifecycleEngine
    from src.engines.strategic_failure_analysis_engine import StrategicFailureAnalysisEngine

    configure_logging()
    port     = int(os.getenv("META_HTTP_PORT", "8018"))
    interval = int(os.getenv("META_INTERVAL",  "3600"))

    _http.start(port, "meta-intelligence")
    logger.info("meta_intelligence_starting", port=port, interval=interval)

    db    = await get_db_pool()
    redis = await get_redis()
    _http.set_ready("meta-intelligence")

    mem_engine      = StrategicMemoryEngine(db=db, redis=redis)
    regime_engine   = RegimeTransitionIntelligence(db=db)
    narr_engine     = NarrativeLifecycleEngine(db=db)
    failure_engine  = StrategicFailureAnalysisEngine(db=db)

    while True:
        try:
            result = await mem_engine.run_cycle()
            logger.info("meta_memory_cycle_done",
                        events=result.events_logged,
                        instability=result.strategic_instability_index,
                        stress=result.governance_stress_score,
                        duration_s=result.cycle_duration_s)

            # Regime transition analysis
            tr_count = await regime_engine.run_cycle()
            logger.info("regime_transitions_processed", count=tr_count)

            # Narrative lifecycle
            nl_count = await narr_engine.run_cycle()
            logger.info("narrative_lifecycles_processed", count=nl_count)

            # Failure cluster analysis
            fc_count = await failure_engine.run_cycle()
            logger.info("failure_clusters_updated", count=fc_count)

        except Exception:
            meta_errors_total.inc()
            logger.error("meta_main_error", exc=traceback.format_exc())

        await asyncio.sleep(interval)


if __name__ == "__main__":
    asyncio.run(_main())
