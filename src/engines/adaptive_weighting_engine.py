"""
Adaptive Weighting Engine — Phase 4

Generates evidence-based governance recommendations for engine weighting,
confidence calibration, and narrative scoring adjustments.

IMPORTANT: This engine generates RECOMMENDATIONS ONLY.
It never mutates any live signal generation, scoring formula, or engine parameter.
Recommendations are stored in governance_recommendations table for human review.

Recommendation types:
  investigate    — trust_score < 30 or noise_contribution > 40%
  reduce_weight  — trust_score 30–50 with active decay
  increase_weight — trust_score > 75, stable, well-calibrated
  recalibrate    — calibration_error > 0.15 for 2+ periods
  monitor        — trust_score 50–60 with early-stage decay
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple

import asyncpg

from src.core.logging import get_logger

logger = get_logger(__name__)

# ─── Calibration targets (per DEC-031) ───────────────────────────────────────
_CALIB_TARGETS = {"high": 0.80, "medium": 0.65, "low": 0.50}
_CALIB_DRIFT_THRESHOLD = 0.15       # calibration error above this → recalibrate
_OVERCONFIDENT_THRESHOLD = 0.60     # high-conf accuracy below this → overconfident
_UNDERCONFIDENT_THRESHOLD = 0.70    # low-conf accuracy above this → underconfident

# Trust score thresholds
_INVESTIGATE_BELOW = 30.0
_REDUCE_BELOW      = 50.0
_INCREASE_ABOVE    = 75.0
_MONITOR_RANGE     = (50.0, 60.0)
_NOISE_INVESTIGATE = 40.0            # noise_contribution % above this → investigate

# Recommendation expiry
_EXPIRES_DAYS = 7


# ─── Data Classes ─────────────────────────────────────────────────────────────

@dataclass
class WeightingRecommendation:
    target_type: str            # engine | confidence_level | narrative
    target_name: str
    action: str                 # investigate | reduce_weight | increase_weight | recalibrate | monitor
    recommended_adjustment: Optional[float]  # e.g. -0.20 = reduce by 20%
    priority: str               # high | medium | low
    confidence_score: float     # 0–1: confidence in this recommendation
    evidence: Dict[str, Any]
    rationale: str


# ─── Engine ───────────────────────────────────────────────────────────────────

class AdaptiveWeightingEngine:
    """
    Reads engine_trust_scores and signal_outcomes to generate governance
    recommendations. Persists to governance_recommendations table.
    """

    def __init__(self, db: asyncpg.Pool):
        self._db = db

    # ── Main: generate + persist ───────────────────────────────────────────────

    async def generate_and_persist(self) -> int:
        """Generate all recommendations and write to DB. Returns count written."""
        recs: List[WeightingRecommendation] = []
        recs.extend(await self._engine_recommendations())
        recs.extend(await self._calibration_recommendations())
        recs.extend(await self._narrative_recommendations())

        # Deduplicate: keep highest-confidence recommendation per target
        seen: Dict[Tuple[str, str], WeightingRecommendation] = {}
        for r in recs:
            key = (r.target_type, r.target_name)
            if key not in seen or r.confidence_score > seen[key].confidence_score:
                seen[key] = r

        final = list(seen.values())
        if final:
            await self._persist_recommendations(final)
        return len(final)

    async def get_active_recommendations(
        self,
        priority: Optional[str] = None,
        target_type: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Read unacknowledged, non-expired recommendations from DB."""
        async with self._db.acquire() as conn:
            rows = await conn.fetch("""
                SELECT id, generated_at, expires_at, target_type, target_name,
                       action, recommended_adjustment, priority,
                       confidence_score, evidence, rationale
                FROM governance_recommendations
                WHERE is_acknowledged = FALSE
                  AND expires_at > NOW()
                  AND ($1::text IS NULL OR priority = $1)
                  AND ($2::text IS NULL OR target_type = $2)
                ORDER BY
                    CASE priority WHEN 'high' THEN 0 WHEN 'medium' THEN 1 ELSE 2 END,
                    confidence_score DESC,
                    generated_at DESC
            """, priority, target_type)

        return [
            {
                "id": r["id"],
                "generated_at": r["generated_at"].isoformat() if r["generated_at"] else None,
                "expires_at": r["expires_at"].isoformat() if r["expires_at"] else None,
                "target_type": r["target_type"],
                "target_name": r["target_name"],
                "action": r["action"],
                "recommended_adjustment": float(r["recommended_adjustment"]) if r["recommended_adjustment"] else None,
                "priority": r["priority"],
                "confidence_score": float(r["confidence_score"]),
                "evidence": r["evidence"],
                "rationale": r["rationale"],
            }
            for r in rows
        ]

    # ── Engine Recommendations ─────────────────────────────────────────────────

    async def _engine_recommendations(self) -> List[WeightingRecommendation]:
        recs: List[WeightingRecommendation] = []
        async with self._db.acquire() as conn:
            rows = await conn.fetch("""
                SELECT DISTINCT ON (engine_name)
                    engine_name, trust_score, noise_contribution,
                    degradation_velocity, is_decaying, is_unstable,
                    health_status, stability_score,
                    avg_precision_7d, avg_precision_30d,
                    calibration_stability
                FROM engine_trust_scores
                WHERE window_hours = 24
                ORDER BY engine_name, computed_at DESC
            """)

        for r in rows:
            ts = float(r["trust_score"] or 0)
            noise = float(r["noise_contribution"] or 0)
            dv = float(r["degradation_velocity"] or 0)
            stability = float(r["stability_score"] or 50)
            evidence = {
                "trust_score": ts,
                "noise_contribution_pct": noise,
                "degradation_velocity_per_week": dv,
                "stability_score": stability,
                "health_status": r["health_status"],
                "is_decaying": r["is_decaying"],
                "avg_precision_7d": float(r["avg_precision_7d"]) if r["avg_precision_7d"] else None,
                "avg_precision_30d": float(r["avg_precision_30d"]) if r["avg_precision_30d"] else None,
            }

            # Investigate: failing trust or high noise
            if ts < _INVESTIGATE_BELOW or noise > _NOISE_INVESTIGATE:
                recs.append(WeightingRecommendation(
                    target_type="engine",
                    target_name=r["engine_name"],
                    action="investigate",
                    recommended_adjustment=None,
                    priority="high",
                    confidence_score=min(1.0, ((_INVESTIGATE_BELOW - ts) / _INVESTIGATE_BELOW * 0.6) +
                                        (noise / 100.0 * 0.4)),
                    evidence=evidence,
                    rationale=(
                        f"Trust score {ts:.0f}/100 is below failure threshold "
                        f"({_INVESTIGATE_BELOW}). "
                        f"Noise contribution: {noise:.1f}%. "
                        "Review engine inputs, data freshness, and signal quality."
                    ),
                ))

            # Reduce weight: degraded trust with active decay
            elif _INVESTIGATE_BELOW <= ts < _REDUCE_BELOW and r["is_decaying"]:
                adj = max(-0.40, dv * 10)  # decay velocity → weight reduction
                recs.append(WeightingRecommendation(
                    target_type="engine",
                    target_name=r["engine_name"],
                    action="reduce_weight",
                    recommended_adjustment=round(adj, 2),
                    priority="medium",
                    confidence_score=min(1.0, (_REDUCE_BELOW - ts) / (_REDUCE_BELOW - _INVESTIGATE_BELOW)),
                    evidence=evidence,
                    rationale=(
                        f"Trust score {ts:.0f}/100 in degraded zone with active decay "
                        f"(velocity {dv:.3f}/week). "
                        f"Suggested weight reduction: {abs(adj)*100:.0f}%."
                    ),
                ))

            # Monitor: borderline trust with early-stage decay
            elif _MONITOR_RANGE[0] <= ts < _MONITOR_RANGE[1] and r["is_decaying"]:
                recs.append(WeightingRecommendation(
                    target_type="engine",
                    target_name=r["engine_name"],
                    action="monitor",
                    recommended_adjustment=None,
                    priority="low",
                    confidence_score=0.5,
                    evidence=evidence,
                    rationale=(
                        f"Trust score {ts:.0f}/100 approaching degraded zone "
                        f"with decay velocity {dv:.3f}/week. "
                        "No action required yet. Re-evaluate in 2 weeks."
                    ),
                ))

            # Increase weight: high-trust stable engine
            elif ts >= _INCREASE_ABOVE and stability >= 70.0 and not r["is_decaying"]:
                adj = min(0.30, (ts - _INCREASE_ABOVE) / 100.0 * 2)
                recs.append(WeightingRecommendation(
                    target_type="engine",
                    target_name=r["engine_name"],
                    action="increase_weight",
                    recommended_adjustment=round(adj, 2),
                    priority="low",
                    confidence_score=min(1.0, ts / 100.0),
                    evidence=evidence,
                    rationale=(
                        f"Trust score {ts:.0f}/100 in high-reliability zone. "
                        f"Stability: {stability:.0f}/100. No active decay. "
                        f"Suggested weight increase: {adj*100:.0f}%."
                    ),
                ))

        return recs

    # ── Calibration Recommendations ────────────────────────────────────────────

    async def _calibration_recommendations(self) -> List[WeightingRecommendation]:
        recs: List[WeightingRecommendation] = []
        async with self._db.acquire() as conn:
            rows = await conn.fetch("""
                SELECT
                    sr.engine_name,
                    sr.confidence_level,
                    AVG(so.direction_correct::int) AS actual_accuracy,
                    COUNT(*)                       AS samples
                FROM signal_outcomes so
                JOIN signals_registry sr ON sr.id = so.signal_id
                WHERE so.evaluated_at >= NOW() - INTERVAL '30 days'
                  AND sr.confidence_level IS NOT NULL
                  AND so.direction_correct IS NOT NULL
                GROUP BY sr.engine_name, sr.confidence_level
                HAVING COUNT(*) >= 20
            """)

            # Also get per-engine calibration_error from signal_scores
            calib_rows = await conn.fetch("""
                SELECT DISTINCT ON (engine_name)
                    engine_name, calibration_error
                FROM signal_scores
                WHERE window_hours = 24
                ORDER BY engine_name, computed_at DESC
            """)
        calib_map = {r["engine_name"]: float(r["calibration_error"] or 0) for r in calib_rows}

        # Group by engine
        engine_data: Dict[str, Dict[str, Any]] = {}
        for row in rows:
            eng = row["engine_name"]
            lvl = row["confidence_level"]
            actual = float(row["actual_accuracy"])
            target = _CALIB_TARGETS.get(lvl, 0.65)
            engine_data.setdefault(eng, {})
            engine_data[eng][lvl] = {"actual": actual, "target": target,
                                     "error": abs(actual - target), "samples": int(row["samples"])}

        for eng, buckets in engine_data.items():
            calib_err = calib_map.get(eng, 0.0)

            # Overconfident detection: high-confidence accuracy < threshold
            high = buckets.get("high", {})
            if high.get("actual") and high["actual"] < _OVERCONFIDENT_THRESHOLD:
                evidence = {
                    "confidence_level": "high",
                    "actual_accuracy": round(high["actual"], 4),
                    "target_accuracy": _CALIB_TARGETS["high"],
                    "gap": round(_CALIB_TARGETS["high"] - high["actual"], 4),
                    "samples": high.get("samples", 0),
                    "overall_calibration_error": calib_err,
                }
                recs.append(WeightingRecommendation(
                    target_type="confidence_level",
                    target_name=f"{eng}::high",
                    action="recalibrate",
                    recommended_adjustment=round(high["actual"] - _CALIB_TARGETS["high"], 3),
                    priority="high",
                    confidence_score=min(1.0, (_OVERCONFIDENT_THRESHOLD - high["actual"]) * 4),
                    evidence=evidence,
                    rationale=(
                        f"{eng} high-confidence signals accurate only {high['actual']*100:.1f}% "
                        f"of the time (target: {_CALIB_TARGETS['high']*100:.0f}%). "
                        "Engine is overconfident. Tighten high-confidence classification threshold."
                    ),
                ))

            # Underconfident detection: low-confidence accuracy exceeds threshold
            low = buckets.get("low", {})
            if low.get("actual") and low["actual"] > _UNDERCONFIDENT_THRESHOLD:
                evidence = {
                    "confidence_level": "low",
                    "actual_accuracy": round(low["actual"], 4),
                    "target_accuracy": _CALIB_TARGETS["low"],
                    "gap": round(low["actual"] - _CALIB_TARGETS["low"], 4),
                    "samples": low.get("samples", 0),
                }
                recs.append(WeightingRecommendation(
                    target_type="confidence_level",
                    target_name=f"{eng}::low",
                    action="recalibrate",
                    recommended_adjustment=round(low["actual"] - _CALIB_TARGETS["low"], 3),
                    priority="medium",
                    confidence_score=min(1.0, (low["actual"] - _UNDERCONFIDENT_THRESHOLD) * 3),
                    evidence=evidence,
                    rationale=(
                        f"{eng} low-confidence signals accurate {low['actual']*100:.1f}% "
                        f"of the time (target: {_CALIB_TARGETS['low']*100:.0f}%). "
                        "Engine is underconfident. Loosen low-confidence threshold."
                    ),
                ))

            # General calibration drift
            elif calib_err > _CALIB_DRIFT_THRESHOLD:
                recs.append(WeightingRecommendation(
                    target_type="engine",
                    target_name=f"{eng}::calibration",
                    action="recalibrate",
                    recommended_adjustment=None,
                    priority="medium",
                    confidence_score=min(1.0, calib_err / 0.30),
                    evidence={"calibration_error": calib_err, "buckets": buckets},
                    rationale=(
                        f"{eng} calibration error {calib_err:.3f} exceeds threshold "
                        f"({_CALIB_DRIFT_THRESHOLD}). Review confidence assignment logic."
                    ),
                ))

        return recs

    # ── Narrative Recommendations ──────────────────────────────────────────────

    async def _narrative_recommendations(self) -> List[WeightingRecommendation]:
        recs: List[WeightingRecommendation] = []
        async with self._db.acquire() as conn:
            rows = await conn.fetch("""
                SELECT DISTINCT ON (narrative_name)
                    narrative_name, persistence_score, degradation_rate,
                    predictive_longevity_w, is_stable, is_collapsing, half_life_weeks
                FROM narrative_stability_scores
                ORDER BY narrative_name, computed_at DESC
            """)

        for r in rows:
            ps = float(r["persistence_score"] or 0)
            dr = float(r["degradation_rate"] or 0)
            name = r["narrative_name"]

            # Collapsing narrative: reduce weight strongly
            if r["is_collapsing"]:
                recs.append(WeightingRecommendation(
                    target_type="narrative",
                    target_name=name,
                    action="reduce_weight",
                    recommended_adjustment=-0.50,
                    priority="high",
                    confidence_score=0.85,
                    evidence={
                        "persistence_score": ps,
                        "degradation_rate_per_week": dr,
                        "is_collapsing": True,
                        "half_life_weeks": float(r["half_life_weeks"]) if r["half_life_weeks"] else None,
                    },
                    rationale=(
                        f"Narrative '{name}' is collapsing (persistence: {ps:.2f}, "
                        f"decay: {dr:.3f}/week). Reduce contribution to confluence scoring."
                    ),
                ))

            # Unstable narrative: monitor
            elif not r["is_stable"] and dr < -0.01:
                recs.append(WeightingRecommendation(
                    target_type="narrative",
                    target_name=name,
                    action="monitor",
                    recommended_adjustment=None,
                    priority="medium",
                    confidence_score=0.6,
                    evidence={
                        "persistence_score": ps,
                        "degradation_rate_per_week": dr,
                        "predictive_longevity_weeks": r["predictive_longevity_w"],
                    },
                    rationale=(
                        f"Narrative '{name}' showing degradation (persistence: {ps:.2f}, "
                        f"decay: {dr:.3f}/week). Monitor for 2 more weeks."
                    ),
                ))

        return recs

    # ── Persistence ────────────────────────────────────────────────────────────

    async def _persist_recommendations(
        self,
        recs: List[WeightingRecommendation],
    ) -> None:
        expires = datetime.now(timezone.utc) + timedelta(days=_EXPIRES_DAYS)
        async with self._db.acquire() as conn:
            for r in recs:
                await conn.execute("""
                    INSERT INTO governance_recommendations (
                        generated_at, expires_at,
                        target_type, target_name,
                        action, recommended_adjustment,
                        priority, confidence_score,
                        evidence, rationale
                    ) VALUES (NOW(), $1, $2, $3, $4, $5, $6, $7, $8, $9)
                """,
                    expires,
                    r.target_type, r.target_name,
                    r.action,
                    r.recommended_adjustment,
                    r.priority,
                    r.confidence_score,
                    json.dumps(r.evidence),
                    r.rationale,
                )
        logger.info("recommendations_persisted", count=len(recs))
