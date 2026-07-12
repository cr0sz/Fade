"""
Strategic Failure Analysis Engine — Phase 5

Identifies, clusters, and tracks recurring failure modes across the system.

Cluster types detected:
  - precision_collapse:       engine precision < threshold for multiple recent periods
  - governance_instability:   trust score volatility high (large swings)
  - false_positive_spike:     FPR > threshold sustained across periods
  - calibration_drift:        calibration error above threshold sustained
  - systemic_degradation:     3+ engines failing simultaneously
  - narrative_collapse_pattern: narrative collapsed 2+ times
  - regime_transition_fragility: engine fragility score > threshold during transitions

Each failure cluster:
  - Has a stable cluster_id (deterministic, based on type + engine + regime)
  - Tracks recurrence_count, first_seen, last_seen
  - Is marked active/inactive based on recency (30 days)
  - Is flagged as systemic if affecting 3+ engines

Reads from: engine_trust_scores, replay_metrics, signal_scores,
            regime_transition_records, narrative_lifecycle_records
Writes to: strategic_failure_clusters
"""
from __future__ import annotations

import json
import hashlib
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from statistics import mean, stdev
from typing import Any, Dict, List, Optional, Tuple

import asyncpg

from src.core.logging import get_logger

logger = get_logger(__name__)

_PRECISION_COLLAPSE_THRESH  = 0.45
_FPR_HIGH_THRESH            = 0.40
_CALIB_DRIFT_THRESH         = 0.20
_TRUST_VOLATILITY_THRESH    = 20.0   # std-dev of trust scores = unstable
_FRAGILITY_THRESH           = 30.0   # transition fragility > this = fragile
_SYSTEMIC_ENGINE_COUNT      = 3      # simultaneous failures = systemic
_INACTIVE_DAYS              = 30     # cluster not seen in 30d = inactive
_MIN_RECURRENCE             = 2      # must occur >= 2 times to form cluster


@dataclass
class FailureCluster:
    cluster_id: str
    cluster_type: str
    severity: str
    first_seen_at: datetime
    last_seen_at: datetime
    recurrence_count: int
    affected_engines: List[str]
    regime_context: Optional[str]
    narrative_context: Optional[str]
    cluster_signature: Dict[str, Any]
    latest_evidence: Dict[str, Any]
    is_active: bool
    is_systemic: bool


class StrategicFailureAnalysisEngine:
    """
    Detects and maintains recurring failure clusters.
    Call run_cycle() each meta-intelligence interval.
    """

    def __init__(self, db: asyncpg.Pool, analysis_days: int = 90):
        self._db = db
        self._analysis_days = analysis_days

    # ── Main cycle ─────────────────────────────────────────────────────────────

    async def run_cycle(self) -> int:
        """Detect all cluster types and upsert records. Returns count updated."""
        clusters = []
        clusters.extend(await self._detect_precision_collapses())
        clusters.extend(await self._detect_governance_instability())
        clusters.extend(await self._detect_false_positive_spikes())
        clusters.extend(await self._detect_calibration_drift())
        clusters.extend(await self._detect_systemic_degradation())
        clusters.extend(await self._detect_narrative_collapse_patterns())
        clusters.extend(await self._detect_transition_fragility())

        count = 0
        for cluster in clusters:
            try:
                await self._upsert_cluster(cluster)
                count += 1
            except Exception as e:
                logger.error("failure_cluster_upsert_error",
                             cluster_id=cluster.cluster_id, error=str(e))

        # Mark clusters inactive if not seen recently
        await self._mark_inactive()
        return count

    # ── Cluster Detectors ──────────────────────────────────────────────────────

    async def _detect_precision_collapses(self) -> List[FailureCluster]:
        """Engines with precision below threshold in multiple recent windows."""
        async with self._db.acquire() as conn:
            rows = await conn.fetch("""
                SELECT
                    engine_name,
                    COUNT(*)        AS collapse_count,
                    MIN(precision_score) AS min_precision,
                    AVG(precision_score) AS avg_precision,
                    MIN(computed_at)     AS first_seen,
                    MAX(computed_at)     AS last_seen
                FROM replay_metrics
                WHERE window_hours = 24
                  AND precision_score < $1
                  AND computed_at >= NOW() - ($2 || ' days')::INTERVAL
                GROUP BY engine_name
                HAVING COUNT(*) >= $3
            """, _PRECISION_COLLAPSE_THRESH, str(self._analysis_days), _MIN_RECURRENCE)

        result = []
        for r in rows:
            cid = f"precision_collapse_{r['engine_name']}"
            severity = (
                "critical" if float(r["avg_precision"]) < 0.30
                else "high" if float(r["avg_precision"]) < 0.38
                else "medium"
            )
            result.append(FailureCluster(
                cluster_id=cid,
                cluster_type="precision_collapse",
                severity=severity,
                first_seen_at=r["first_seen"],
                last_seen_at=r["last_seen"],
                recurrence_count=int(r["collapse_count"]),
                affected_engines=[r["engine_name"]],
                regime_context=None,
                narrative_context=None,
                cluster_signature={
                    "threshold": _PRECISION_COLLAPSE_THRESH,
                    "min_precision": round(float(r["min_precision"]), 4),
                    "avg_precision": round(float(r["avg_precision"]), 4),
                },
                latest_evidence={"recurrence_count": int(r["collapse_count"])},
                is_active=True,
                is_systemic=False,
            ))
        return result

    async def _detect_governance_instability(self) -> List[FailureCluster]:
        """Engines with high trust score volatility (large swings)."""
        async with self._db.acquire() as conn:
            rows = await conn.fetch("""
                SELECT engine_name,
                       STDDEV(trust_score)  AS trust_stddev,
                       COUNT(*)             AS sample_count,
                       MIN(trust_score)     AS min_trust,
                       MAX(trust_score)     AS max_trust,
                       MIN(computed_at)     AS first_seen,
                       MAX(computed_at)     AS last_seen
                FROM engine_trust_scores
                WHERE window_hours = 24
                  AND computed_at >= NOW() - ($1 || ' days')::INTERVAL
                GROUP BY engine_name
                HAVING STDDEV(trust_score) >= $2
                   AND COUNT(*) >= 4
            """, str(self._analysis_days), _TRUST_VOLATILITY_THRESH)

        result = []
        for r in rows:
            cid = f"governance_instability_{r['engine_name']}"
            vol = float(r["trust_stddev"])
            severity = "critical" if vol >= 35 else ("high" if vol >= 25 else "medium")
            result.append(FailureCluster(
                cluster_id=cid,
                cluster_type="governance_instability",
                severity=severity,
                first_seen_at=r["first_seen"],
                last_seen_at=r["last_seen"],
                recurrence_count=int(r["sample_count"]),
                affected_engines=[r["engine_name"]],
                regime_context=None,
                narrative_context=None,
                cluster_signature={
                    "trust_stddev": round(vol, 2),
                    "min_trust": round(float(r["min_trust"]), 2),
                    "max_trust": round(float(r["max_trust"]), 2),
                    "swing": round(float(r["max_trust"]) - float(r["min_trust"]), 2),
                },
                latest_evidence={"trust_volatility": round(vol, 2)},
                is_active=True,
                is_systemic=False,
            ))
        return result

    async def _detect_false_positive_spikes(self) -> List[FailureCluster]:
        """Engines with sustained high FPR across multiple replay periods."""
        async with self._db.acquire() as conn:
            rows = await conn.fetch("""
                SELECT
                    engine_name,
                    COUNT(*)            AS spike_count,
                    AVG(false_positive_rate) AS avg_fpr,
                    MAX(false_positive_rate) AS max_fpr,
                    MIN(computed_at)    AS first_seen,
                    MAX(computed_at)    AS last_seen
                FROM replay_metrics
                WHERE window_hours = 24
                  AND false_positive_rate >= $1
                  AND computed_at >= NOW() - ($2 || ' days')::INTERVAL
                GROUP BY engine_name
                HAVING COUNT(*) >= $3
            """, _FPR_HIGH_THRESH, str(self._analysis_days), _MIN_RECURRENCE)

        result = []
        for r in rows:
            cid = f"false_positive_spike_{r['engine_name']}"
            avg_fpr = float(r["avg_fpr"])
            severity = "critical" if avg_fpr >= 0.60 else ("high" if avg_fpr >= 0.50 else "medium")
            result.append(FailureCluster(
                cluster_id=cid,
                cluster_type="false_positive_spike",
                severity=severity,
                first_seen_at=r["first_seen"],
                last_seen_at=r["last_seen"],
                recurrence_count=int(r["spike_count"]),
                affected_engines=[r["engine_name"]],
                regime_context=None,
                narrative_context=None,
                cluster_signature={
                    "fpr_threshold": _FPR_HIGH_THRESH,
                    "avg_fpr": round(avg_fpr, 4),
                    "max_fpr": round(float(r["max_fpr"]), 4),
                },
                latest_evidence={"spike_count": int(r["spike_count"])},
                is_active=True,
                is_systemic=False,
            ))
        return result

    async def _detect_calibration_drift(self) -> List[FailureCluster]:
        """Engines with sustained calibration errors above threshold."""
        async with self._db.acquire() as conn:
            rows = await conn.fetch("""
                SELECT
                    engine_name,
                    COUNT(*)            AS drift_count,
                    AVG(calibration_error) AS avg_error,
                    MAX(calibration_error) AS max_error,
                    MIN(computed_at)    AS first_seen,
                    MAX(computed_at)    AS last_seen
                FROM signal_scores
                WHERE window_hours = 24
                  AND calibration_error >= $1
                  AND computed_at >= NOW() - ($2 || ' days')::INTERVAL
                GROUP BY engine_name
                HAVING COUNT(*) >= $3
            """, _CALIB_DRIFT_THRESH, str(self._analysis_days), _MIN_RECURRENCE)

        result = []
        for r in rows:
            cid = f"calibration_drift_{r['engine_name']}"
            avg_err = float(r["avg_error"])
            severity = "critical" if avg_err >= 0.35 else ("high" if avg_err >= 0.25 else "medium")
            result.append(FailureCluster(
                cluster_id=cid,
                cluster_type="calibration_drift",
                severity=severity,
                first_seen_at=r["first_seen"],
                last_seen_at=r["last_seen"],
                recurrence_count=int(r["drift_count"]),
                affected_engines=[r["engine_name"]],
                regime_context=None,
                narrative_context=None,
                cluster_signature={
                    "calib_threshold": _CALIB_DRIFT_THRESH,
                    "avg_error": round(avg_err, 4),
                    "max_error": round(float(r["max_error"]), 4),
                },
                latest_evidence={"drift_periods": int(r["drift_count"])},
                is_active=True,
                is_systemic=False,
            ))
        return result

    async def _detect_systemic_degradation(self) -> List[FailureCluster]:
        """Periods where 3+ engines failed simultaneously."""
        async with self._db.acquire() as conn:
            rows = await conn.fetch("""
                SELECT
                    date_trunc('day', computed_at) AS day,
                    COUNT(DISTINCT engine_name)    AS failing_engines,
                    ARRAY_AGG(DISTINCT engine_name) AS engine_list,
                    MIN(computed_at)               AS first_seen
                FROM engine_trust_scores
                WHERE window_hours = 24
                  AND health_status IN ('degraded', 'failing')
                  AND computed_at >= NOW() - ($1 || ' days')::INTERVAL
                GROUP BY day
                HAVING COUNT(DISTINCT engine_name) >= $2
                ORDER BY day DESC
            """, str(self._analysis_days), _SYSTEMIC_ENGINE_COUNT)

        if not rows:
            return []

        engines_seen = set()
        dates_seen = []
        for r in rows:
            engines_seen.update(r["engine_list"])
            dates_seen.append(r["first_seen"])

        cid = "systemic_degradation_multi_engine"
        engines = sorted(engines_seen)
        return [FailureCluster(
            cluster_id=cid,
            cluster_type="systemic_degradation",
            severity="critical" if len(rows) >= 5 else "high",
            first_seen_at=min(dates_seen),
            last_seen_at=max(dates_seen),
            recurrence_count=len(rows),
            affected_engines=engines,
            regime_context=None,
            narrative_context=None,
            cluster_signature={
                "min_simultaneous_failures": _SYSTEMIC_ENGINE_COUNT,
                "unique_engines_affected": len(engines),
            },
            latest_evidence={
                "simultaneous_failure_days": len(rows),
                "max_simultaneous": max(int(r["failing_engines"]) for r in rows),
            },
            is_active=True,
            is_systemic=True,
        )]

    async def _detect_narrative_collapse_patterns(self) -> List[FailureCluster]:
        """Narratives that have collapsed 2+ times (recurring collapse pattern)."""
        async with self._db.acquire() as conn:
            rows = await conn.fetch("""
                SELECT DISTINCT ON (narrative_name)
                    narrative_name, collapse_count, lifecycle_phase,
                    current_accuracy, recorded_at
                FROM narrative_lifecycle_records
                WHERE collapse_count >= $1
                ORDER BY narrative_name, recorded_at DESC
            """, _MIN_RECURRENCE)

        result = []
        for r in rows:
            cid = f"narrative_collapse_pattern_{r['narrative_name']}"
            count = int(r["collapse_count"])
            severity = "critical" if count >= 4 else ("high" if count >= 3 else "medium")
            result.append(FailureCluster(
                cluster_id=cid,
                cluster_type="narrative_collapse_pattern",
                severity=severity,
                first_seen_at=r["recorded_at"],
                last_seen_at=r["recorded_at"],
                recurrence_count=count,
                affected_engines=[],
                regime_context=None,
                narrative_context=r["narrative_name"],
                cluster_signature={
                    "collapse_count": count,
                    "current_phase": r["lifecycle_phase"],
                },
                latest_evidence={
                    "current_accuracy": float(r["current_accuracy"])
                    if r["current_accuracy"] else None,
                },
                is_active=r["lifecycle_phase"] in ("collapse", "degradation"),
                is_systemic=False,
            ))
        return result

    async def _detect_transition_fragility(self) -> List[FailureCluster]:
        """Engines with high average fragility across regime transitions."""
        async with self._db.acquire() as conn:
            rows = await conn.fetch("""
                SELECT
                    kv.key AS engine_name,
                    AVG((kv.value)::numeric)  AS avg_fragility,
                    COUNT(*)                   AS transition_count,
                    MAX((kv.value)::numeric)   AS max_fragility,
                    MIN(r.detected_at)         AS first_seen,
                    MAX(r.detected_at)         AS last_seen
                FROM regime_transition_records r,
                     jsonb_each_text(r.engine_fragility_scores) AS kv
                WHERE r.detected_at >= NOW() - ($1 || ' days')::INTERVAL
                GROUP BY kv.key
                HAVING AVG((kv.value)::numeric) >= $2
                   AND COUNT(*) >= 2
            """, str(self._analysis_days), _FRAGILITY_THRESH)

        result = []
        for r in rows:
            cid = f"regime_transition_fragility_{r['engine_name']}"
            avg_frag = float(r["avg_fragility"])
            severity = "high" if avg_frag >= 50 else "medium"
            result.append(FailureCluster(
                cluster_id=cid,
                cluster_type="regime_transition_fragility",
                severity=severity,
                first_seen_at=r["first_seen"],
                last_seen_at=r["last_seen"],
                recurrence_count=int(r["transition_count"]),
                affected_engines=[r["engine_name"]],
                regime_context=None,
                narrative_context=None,
                cluster_signature={
                    "fragility_threshold": _FRAGILITY_THRESH,
                    "avg_fragility": round(avg_frag, 2),
                    "max_fragility": round(float(r["max_fragility"]), 2),
                },
                latest_evidence={"transition_count": int(r["transition_count"])},
                is_active=True,
                is_systemic=False,
            ))
        return result

    # ── Persistence ────────────────────────────────────────────────────────────

    async def _upsert_cluster(self, cluster: FailureCluster) -> None:
        """Insert or update a failure cluster record."""
        async with self._db.acquire() as conn:
            existing = await conn.fetchrow(
                "SELECT id, recurrence_count FROM strategic_failure_clusters WHERE cluster_id = $1",
                cluster.cluster_id,
            )
            if existing:
                await conn.execute("""
                    UPDATE strategic_failure_clusters
                    SET last_seen_at     = $2,
                        recurrence_count = GREATEST(recurrence_count, $3),
                        affected_engines = $4,
                        latest_evidence  = $5,
                        is_active        = $6,
                        is_systemic      = $7,
                        severity         = $8
                    WHERE cluster_id = $1
                """,
                    cluster.cluster_id,
                    cluster.last_seen_at,
                    cluster.recurrence_count,
                    json.dumps(cluster.affected_engines),
                    json.dumps(cluster.latest_evidence),
                    cluster.is_active,
                    cluster.is_systemic,
                    cluster.severity,
                )
            else:
                await conn.execute("""
                    INSERT INTO strategic_failure_clusters (
                        cluster_id, cluster_type, severity,
                        first_seen_at, last_seen_at, recurrence_count,
                        affected_engines, regime_context, narrative_context,
                        cluster_signature, latest_evidence, is_active, is_systemic
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
                """,
                    cluster.cluster_id,
                    cluster.cluster_type,
                    cluster.severity,
                    cluster.first_seen_at,
                    cluster.last_seen_at,
                    cluster.recurrence_count,
                    json.dumps(cluster.affected_engines),
                    cluster.regime_context,
                    cluster.narrative_context,
                    json.dumps(cluster.cluster_signature),
                    json.dumps(cluster.latest_evidence),
                    cluster.is_active,
                    cluster.is_systemic,
                )

    async def _mark_inactive(self) -> None:
        """Mark clusters as inactive if last_seen_at > _INACTIVE_DAYS ago."""
        async with self._db.acquire() as conn:
            await conn.execute("""
                UPDATE strategic_failure_clusters
                SET is_active = FALSE
                WHERE is_active = TRUE
                  AND last_seen_at < NOW() - ($1 || ' days')::INTERVAL
            """, str(_INACTIVE_DAYS))

    # ── Read Methods ───────────────────────────────────────────────────────────

    async def get_failure_clusters(
        self,
        active_only: bool = True,
        cluster_type: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Return failure clusters sorted by severity then recurrence."""
        conditions = ["TRUE"]
        params: List[Any] = []
        if active_only:
            conditions.append("is_active = TRUE")
        if cluster_type:
            params.append(cluster_type)
            conditions.append(f"cluster_type = ${len(params)}")

        where = " AND ".join(conditions)
        async with self._db.acquire() as conn:
            rows = await conn.fetch(f"""
                SELECT
                    cluster_id, cluster_type, severity,
                    first_seen_at, last_seen_at, recurrence_count,
                    affected_engines, regime_context, narrative_context,
                    cluster_signature, latest_evidence, is_active, is_systemic
                FROM strategic_failure_clusters
                WHERE {where}
                ORDER BY
                    CASE severity WHEN 'critical' THEN 0 WHEN 'high' THEN 1 ELSE 2 END,
                    recurrence_count DESC,
                    last_seen_at DESC
            """, *params)

        result = []
        for r in rows:
            row_dict = dict(r)
            row_dict["first_seen_at"] = r["first_seen_at"].isoformat() if r["first_seen_at"] else None
            row_dict["last_seen_at"] = r["last_seen_at"].isoformat() if r["last_seen_at"] else None
            row_dict["affected_engines"] = json.loads(r["affected_engines"]) if r["affected_engines"] else []
            row_dict["cluster_signature"] = json.loads(r["cluster_signature"]) if r["cluster_signature"] else {}
            row_dict["latest_evidence"] = json.loads(r["latest_evidence"]) if r["latest_evidence"] else {}
            result.append(row_dict)
        return result

    async def get_systemic_risk_summary(self) -> Dict[str, Any]:
        """Summary of systemic (multi-engine) failure risk."""
        async with self._db.acquire() as conn:
            systemic = await conn.fetch("""
                SELECT cluster_id, severity, recurrence_count,
                       affected_engines, last_seen_at
                FROM strategic_failure_clusters
                WHERE is_systemic = TRUE AND is_active = TRUE
                ORDER BY recurrence_count DESC
            """)

            total_active = await conn.fetchval(
                "SELECT COUNT(*) FROM strategic_failure_clusters WHERE is_active = TRUE"
            ) or 0

            critical_count = await conn.fetchval("""
                SELECT COUNT(*) FROM strategic_failure_clusters
                WHERE is_active = TRUE AND severity = 'critical'
            """) or 0

        return {
            "total_active_clusters": int(total_active),
            "critical_clusters": int(critical_count),
            "systemic_clusters": [
                {
                    "cluster_id": r["cluster_id"],
                    "severity": r["severity"],
                    "recurrence_count": r["recurrence_count"],
                    "affected_engines": json.loads(r["affected_engines"]) if r["affected_engines"] else [],
                    "last_seen_at": r["last_seen_at"].isoformat() if r["last_seen_at"] else None,
                }
                for r in systemic
            ],
            "risk_level": (
                "critical" if int(critical_count) >= 2
                else "high" if int(critical_count) >= 1 or len(systemic) >= 1
                else "medium" if int(total_active) >= 5
                else "low"
            ),
        }
