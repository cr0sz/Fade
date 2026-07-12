"""
Engine Governance Engine — Phase 4

Computes multi-dimensional trust scores for each intelligence engine using
measured historical evidence from replay_metrics and signal_scores.

Trust Score (0–100) is a weighted composite of:
  30%  predictive_strength (signal quality, from signal_scores)
  25%  decay_adjusted_precision (precision × max(0, 1 + decay_velocity×5))
  20%  calibration_stability (1 – calibration_error, scaled to 0–100)
  15%  regime_reliability (avg accuracy across macro regimes)
  10%  recovery_capability (fraction of dip periods followed by recovery)

Standalone service on port 8017; writes to engine_trust_scores table hourly.
Generates governance recommendations via AdaptiveWeightingEngine.
"""
from __future__ import annotations

import asyncio
import json
import math
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
from src.engines.engine_math import ols_slope as _ols_slope, _DECAY_WEEKS as _DECAY_WEEKS_SHARED

logger = get_logger(__name__)

# ─── Prometheus Metrics ───────────────────────────────────────────────────────
governance_cycles_total = Counter(
    "governance_cycles_total",
    "Total governance scoring cycles completed",
)
governance_cycle_seconds = Histogram(
    "governance_cycle_seconds",
    "Duration of governance scoring cycle",
    buckets=[1.0, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0],
)
governance_errors_total = Counter(
    "governance_errors_total",
    "Errors in governance scoring cycles",
)
engine_trust_gauge = Gauge(
    "engine_trust_score",
    "Current trust score per engine (0–100)",
    ["engine_name"],
)
engine_instability_gauge = Gauge(
    "engine_instability_flag",
    "1 if engine is currently flagged as unstable",
    ["engine_name"],
)

# ─── Constants ────────────────────────────────────────────────────────────────
_HEALTHY_TRUST    = 60.0
_DEGRADED_TRUST   = 30.0
_DECAY_WEEKS      = _DECAY_WEEKS_SHARED  # weeks of history for OLS (shared with engine_math)
_STABILITY_FLOOR  = 0.55    # accuracy below this = "bad week"
_RECOVERY_WINDOW  = 4       # weeks to look for recovery after dip
_CALIB_MAX_ERROR  = 0.40    # calibration error at which calibration_stability = 0
_MIN_REPLAY_WEEKS = 4       # fewer weeks → thin data, OLS/CV math unreliable → neutral fallback

# Trust score weights
_W_STRENGTH  = 0.30
_W_PRECISION = 0.25
_W_CALIB     = 0.20
_W_REGIME    = 0.15
_W_RECOVERY  = 0.10


# ─── Data Classes ─────────────────────────────────────────────────────────────

@dataclass
class EngineTrustScore:
    engine_name: str
    trust_score: float                        # 0–100
    predictive_strength: Optional[float]      # 0–100
    decay_adj_precision: Optional[float]      # 0–1
    calibration_stability: Optional[float]    # 0–100
    regime_reliability: Optional[float]       # 0–1
    recovery_capability: Optional[float]      # 0–100
    stability_score: Optional[float]          # 0–100
    noise_contribution: Optional[float]       # 0–100
    degradation_velocity: float               # OLS slope/week
    health_status: str                        # healthy | degraded | failing
    is_decaying: bool
    is_unstable: bool
    avg_precision_7d: Optional[float]
    avg_precision_30d: Optional[float]
    weekly_accuracies: List[float]
    computed_at: datetime


@dataclass
class GovernanceCycleResult:
    engines_scored: int
    engines_healthy: int
    engines_degraded: int
    engines_failing: int
    unstable_engines: List[str]
    recommendations_generated: int
    cycle_duration_s: float


# ─── Engine ───────────────────────────────────────────────────────────────────

class EngineGovernanceEngine:
    """
    Computes and persists per-engine trust scores.
    Call run_cycle() periodically; results stored in engine_trust_scores.
    """

    def __init__(
        self,
        db: asyncpg.Pool,
        redis=None,
        window_hours: int = 24,
        period_days: int = int(os.getenv("GOVERNANCE_PERIOD_DAYS", "90")),
    ):
        self._db = db
        self._redis = redis
        self._window = window_hours
        self._period_days = period_days

    # ── Main cycle ─────────────────────────────────────────────────────────────

    async def run_cycle(self) -> GovernanceCycleResult:
        import time
        t0 = time.monotonic()
        healthy = degraded = failing = 0
        unstable: List[str] = []
        scored = 0

        with governance_cycle_seconds.time():
            try:
                engines = await self._list_engines()
                # Fetch noise contributions once for all engines
                noise_map = await self._compute_noise_map()

                for eng in engines:
                    try:
                        score = await self._compute_trust(eng, noise_map.get(eng, 0.0))
                        await self._persist(score)
                        engine_trust_gauge.labels(engine_name=eng).set(score.trust_score)
                        engine_instability_gauge.labels(engine_name=eng).set(
                            1 if score.is_unstable else 0
                        )
                        scored += 1
                        if score.health_status == "healthy":
                            healthy += 1
                        elif score.health_status == "degraded":
                            degraded += 1
                        else:
                            failing += 1
                        if score.is_unstable:
                            unstable.append(eng)
                    except Exception:
                        governance_errors_total.inc()
                        logger.error("engine_trust_compute_error", engine=eng,
                                     exc=traceback.format_exc())

                governance_cycles_total.inc()
            except Exception:
                governance_errors_total.inc()
                logger.error("governance_cycle_error", exc=traceback.format_exc())

        return GovernanceCycleResult(
            engines_scored=scored,
            engines_healthy=healthy,
            engines_degraded=degraded,
            engines_failing=failing,
            unstable_engines=unstable,
            recommendations_generated=0,  # set by caller
            cycle_duration_s=round(time.monotonic() - t0, 2),
        )

    # ── Core Trust Computation ─────────────────────────────────────────────────

    async def _compute_trust(
        self,
        engine_name: str,
        noise_contribution: float,
    ) -> EngineTrustScore:
        # 1. Predictive strength + calibration error from signal_scores
        score_row = await self._latest_signal_score(engine_name)
        if score_row:
            ps = score_row["predictive_strength"]
            if ps and float(ps) > 0:
                pred_strength = float(ps)
            elif score_row["accuracy"] and float(score_row["accuracy"]) > 0:
                # predictive_strength unpopulated; use accuracy (0–1) scaled to 0–100
                pred_strength = float(score_row["accuracy"]) * 100.0
            else:
                pred_strength = None
        else:
            pred_strength = None
        calib_error   = float(score_row["calibration_error"])   if score_row and score_row["calibration_error"]   else 0.0

        # 2. Weekly precision and directional accuracy from replay_metrics
        weekly = await self._weekly_accuracy(engine_name)
        weekly_acc = [float(r["dir_acc"]) for r in weekly if r["dir_acc"] is not None]
        weekly_prec = [float(r["prec"]) for r in weekly if r["prec"] is not None]

        # 3. Short-window precision averages
        prec_7d  = await self._avg_precision(engine_name, 7)
        prec_30d = await self._avg_precision(engine_name, 30)

        # 4. Regime reliability
        regime_rel = await self._regime_reliability(engine_name)

        # 5. OLS slope (decay velocity)
        decay_velocity, is_decaying = _ols_slope(weekly_acc, _DECAY_WEEKS)

        # 6. Recovery capability
        recovery = _recovery_score(weekly_acc, _STABILITY_FLOOR, _RECOVERY_WINDOW)

        # 7. Stability (1 – CV over last N weeks)
        stability = _stability_score(weekly_acc)

        # ── Compose trust score ────────────────────────────────────────────────
        # Decay-adjusted precision
        if weekly_prec:
            avg_prec = mean(weekly_prec[-8:]) if len(weekly_prec) >= 2 else weekly_prec[-1]
            decay_pen = max(0.0, min(1.0, 1.0 + decay_velocity * 5))
            dap = avg_prec * decay_pen
        else:
            avg_prec = prec_30d or 0.0
            dap = avg_prec

        # Calibration stability 0–100
        cs = max(0.0, 1.0 - calib_error / _CALIB_MAX_ERROR) * 100.0

        # Normalize components to 0–1
        s_strength  = (pred_strength or 0.0) / 100.0
        s_precision = dap                           # already 0–1
        s_calib     = cs / 100.0
        s_regime    = regime_rel
        s_recovery  = recovery / 100.0

        raw = (
            _W_STRENGTH  * s_strength +
            _W_PRECISION * s_precision +
            _W_CALIB     * s_calib +
            _W_REGIME    * s_regime +
            _W_RECOVERY  * s_recovery
        ) * 100.0

        trust = max(0.0, min(100.0, raw))

        # Health classification
        if trust >= _HEALTHY_TRUST:
            health = "healthy"
        elif trust >= _DEGRADED_TRUST:
            health = "degraded"
        else:
            health = "failing"

        # Unstable: high variance in recent accuracy
        is_unstable = stability < 40.0 or (is_decaying and decay_velocity < -0.04)

        return EngineTrustScore(
            engine_name=engine_name,
            trust_score=round(trust, 2),
            predictive_strength=round(pred_strength, 2) if pred_strength else None,
            decay_adj_precision=round(dap, 4),
            calibration_stability=round(cs, 2),
            regime_reliability=round(regime_rel, 4),
            recovery_capability=round(recovery, 2),
            stability_score=round(stability, 2),
            noise_contribution=round(noise_contribution, 2),
            degradation_velocity=round(decay_velocity, 6),
            health_status=health,
            is_decaying=is_decaying,
            is_unstable=is_unstable,
            avg_precision_7d=round(prec_7d, 4) if prec_7d else None,
            avg_precision_30d=round(prec_30d, 4) if prec_30d else None,
            weekly_accuracies=weekly_acc[-_DECAY_WEEKS:],
            computed_at=datetime.now(timezone.utc),
        )

    # ── DB Queries ─────────────────────────────────────────────────────────────

    async def _list_engines(self) -> List[str]:
        async with self._db.acquire() as conn:
            rows = await conn.fetch(
                "SELECT DISTINCT engine_name FROM signals_registry ORDER BY engine_name"
            )
        return [r["engine_name"] for r in rows]

    async def _latest_signal_score(self, engine_name: str):
        async with self._db.acquire() as conn:
            return await conn.fetchrow("""
                SELECT DISTINCT ON (engine_name)
                    predictive_strength, calibration_error, accuracy
                FROM signal_scores
                WHERE engine_name = $1 AND window_hours = $2
                ORDER BY engine_name, computed_at DESC
            """, engine_name, self._window)

    async def _weekly_accuracy(self, engine_name: str) -> List[Any]:
        async with self._db.acquire() as conn:
            return await conn.fetch("""
                SELECT
                    date_trunc('week', computed_at) AS wk,
                    AVG(directional_accuracy)       AS dir_acc,
                    AVG(precision_score)            AS prec
                FROM replay_metrics
                WHERE engine_name = $1
                  AND window_hours = $2
                  AND computed_at >= NOW() - ($3 || ' weeks')::INTERVAL
                GROUP BY wk
                ORDER BY wk
            """, engine_name, self._window, str(_DECAY_WEEKS))

    async def _avg_precision(self, engine_name: str, days: int) -> Optional[float]:
        async with self._db.acquire() as conn:
            v = await conn.fetchval("""
                SELECT AVG(precision_score)
                FROM replay_metrics
                WHERE engine_name = $1
                  AND window_hours = $2
                  AND computed_at >= NOW() - ($3 || ' days')::INTERVAL
            """, engine_name, self._window, str(days))
        return float(v) if v is not None else None

    async def _regime_reliability(self, engine_name: str) -> float:
        async with self._db.acquire() as conn:
            rows = await conn.fetch("""
                SELECT kv.value->>'accuracy' AS acc
                FROM replay_metrics,
                     jsonb_each(regime_breakdown) AS kv
                WHERE engine_name = $1
                  AND computed_at >= NOW() - ($2 || ' days')::INTERVAL
                  AND jsonb_typeof(regime_breakdown) = 'object'
                  AND (kv.value->>'sample_count')::int >= 5
            """, engine_name, str(self._period_days))
        accs = [float(r["acc"]) for r in rows if r["acc"]]
        return mean(accs) if accs else 0.5  # neutral if no regime data

    async def _compute_noise_map(self) -> Dict[str, float]:
        """Compute noise contribution score (0–100) for each engine."""
        async with self._db.acquire() as conn:
            rows = await conn.fetch("""
                SELECT engine_name,
                       SUM(false_positives)::numeric AS total_fp
                FROM replay_metrics
                WHERE window_hours = $1
                  AND computed_at >= NOW() - INTERVAL '30 days'
                GROUP BY engine_name
            """, self._window)
        if not rows:
            return {}
        total = sum(float(r["total_fp"]) for r in rows if r["total_fp"])
        if total == 0:
            return {r["engine_name"]: 0.0 for r in rows}
        n = len(rows)
        return {
            r["engine_name"]: min(100.0, (float(r["total_fp"] or 0) / total) * n * 100.0)
            for r in rows
        }

    async def _persist(self, score: EngineTrustScore) -> None:
        async with self._db.acquire() as conn:
            await conn.execute("""
                INSERT INTO engine_trust_scores (
                    engine_name, computed_at, window_hours,
                    trust_score, predictive_strength, decay_adj_precision,
                    calibration_stability, regime_reliability,
                    recovery_capability, stability_score,
                    noise_contribution, degradation_velocity,
                    health_status, is_decaying, is_unstable,
                    avg_precision_7d, avg_precision_30d,
                    weekly_accuracy_json, regime_breakdown_json
                ) VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                    $11, $12, $13, $14, $15, $16, $17, $18, $19
                )
            """,
                score.engine_name,
                score.computed_at,
                self._window,
                score.trust_score,
                score.predictive_strength,
                score.decay_adj_precision,
                score.calibration_stability,
                score.regime_reliability,
                score.recovery_capability,
                score.stability_score,
                score.noise_contribution,
                score.degradation_velocity,
                score.health_status,
                score.is_decaying,
                score.is_unstable,
                score.avg_precision_7d,
                score.avg_precision_30d,
                json.dumps(score.weekly_accuracies),
                "{}",
            )

    # ── Read Methods ───────────────────────────────────────────────────────────

    async def get_trust_ranking(self, window_hours: int = 24) -> List[Dict[str, Any]]:
        async with self._db.acquire() as conn:
            rows = await conn.fetch("""
                SELECT DISTINCT ON (engine_name)
                    engine_name, trust_score, predictive_strength,
                    decay_adj_precision, calibration_stability,
                    regime_reliability, recovery_capability,
                    stability_score, noise_contribution,
                    degradation_velocity, health_status,
                    is_decaying, is_unstable,
                    avg_precision_7d, avg_precision_30d, computed_at
                FROM engine_trust_scores
                WHERE window_hours = $1
                ORDER BY engine_name, computed_at DESC
            """, window_hours)
        result = [dict(r) for r in rows]
        result.sort(key=lambda x: x["trust_score"] or 0, reverse=True)
        for i, r in enumerate(result):
            r["rank"] = i + 1
            r["computed_at"] = r["computed_at"].isoformat() if r["computed_at"] else None
        return result

    async def detect_unstable_engines(self, window_hours: int = 24) -> List[Dict[str, Any]]:
        async with self._db.acquire() as conn:
            rows = await conn.fetch("""
                SELECT DISTINCT ON (engine_name)
                    engine_name, trust_score, stability_score,
                    degradation_velocity, is_decaying, health_status, computed_at
                FROM engine_trust_scores
                WHERE window_hours = $1 AND is_unstable = TRUE
                ORDER BY engine_name, computed_at DESC
            """, window_hours)
        return [
            {**dict(r), "computed_at": r["computed_at"].isoformat() if r["computed_at"] else None}
            for r in rows
        ]

    async def compute_recovery_analysis(
        self, window_hours: int = 24, weeks: int = 12
    ) -> List[Dict[str, Any]]:
        async with self._db.acquire() as conn:
            rows = await conn.fetch("""
                SELECT DISTINCT ON (engine_name)
                    engine_name, recovery_capability, trust_score,
                    weekly_accuracy_json, health_status, computed_at
                FROM engine_trust_scores
                WHERE window_hours = $1
                ORDER BY engine_name, computed_at DESC
            """, window_hours)
        result = []
        for r in rows:
            accs = json.loads(r["weekly_accuracy_json"]) if r["weekly_accuracy_json"] else []
            dips = _count_dips(accs, _STABILITY_FLOOR)
            result.append({
                "engine_name": r["engine_name"],
                "recovery_capability": float(r["recovery_capability"] or 0),
                "trust_score": float(r["trust_score"] or 0),
                "health_status": r["health_status"],
                "total_dip_weeks": dips["total"],
                "recovered_dips": dips["recovered"],
                "non_recovered_dips": dips["non_recovered"],
                "current_dip_streak": dips["current_streak"],
                "weekly_accuracies": accs,
                "computed_at": r["computed_at"].isoformat() if r["computed_at"] else None,
            })
        result.sort(key=lambda x: x["recovery_capability"])
        return result


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _stability_score(values: List[float]) -> float:
    """Inverse coefficient of variation, scaled 0–100."""
    vals = [v for v in values if v is not None]
    if len(vals) < _MIN_REPLAY_WEEKS:
        return 50.0  # neutral — too few weeks for reliable CV/OLS math
    m = mean(vals)
    sd = stdev(vals)
    if m <= 0:
        return 0.0
    cv = sd / m
    # cv=0 → 100, cv=0.5 → 0 (high variance)
    return max(0.0, min(100.0, (1.0 - cv / 0.5) * 100.0))


def _recovery_score(values: List[float], floor: float, window: int) -> float:
    """Fraction of dip periods followed by recovery within `window` weeks."""
    vals = [v for v in values if v is not None]
    if len(vals) < 4:
        return 50.0
    if not vals:
        return 50.0
    median_acc = sorted(vals)[len(vals) // 2]
    dips = 0
    recovered = 0
    i = 0
    while i < len(vals):
        if vals[i] < floor:
            dips += 1
            # look for recovery in next `window` weeks
            for j in range(i + 1, min(i + window + 1, len(vals))):
                if vals[j] >= median_acc * 0.9:
                    recovered += 1
                    break
        i += 1
    if dips == 0:
        return 70.0  # no dips = mostly stable, above neutral
    return min(100.0, (recovered / dips) * 100.0)


def _count_dips(values: List[float], floor: float) -> Dict[str, int]:
    total = recovered = 0
    streak = 0
    in_streak = False
    for i, v in enumerate(values):
        if v < floor:
            total += 1
            in_streak = True
            streak += 1
            for j in range(i + 1, min(i + _RECOVERY_WINDOW + 1, len(values))):
                if values[j] >= floor:
                    recovered += 1
                    break
        else:
            if in_streak:
                in_streak = False
                streak = 0
    return {"total": total, "recovered": recovered, "non_recovered": total - recovered,
            "current_streak": streak if in_streak else 0}


# ─── Standalone Entry Point ───────────────────────────────────────────────────

async def _main() -> None:
    from src.core.logging import configure_logging
    from src.db.connection import get_db_pool
    from src.cache.redis_client import get_redis
    from src.engines.adaptive_weighting_engine import AdaptiveWeightingEngine
    from src.engines.narrative_stability_engine import NarrativeStabilityEngine

    configure_logging()
    port     = int(os.getenv("GOVERNANCE_HTTP_PORT", "8017"))
    interval = int(os.getenv("GOVERNANCE_INTERVAL",  "3600"))

    _http.start(port, "governance-engine")
    logger.info("governance_engine_starting", port=port, interval=interval)

    db    = await get_db_pool()
    redis = await get_redis()
    _http.set_ready("governance-engine")

    gov_engine  = EngineGovernanceEngine(db=db, redis=redis)
    weight_eng  = AdaptiveWeightingEngine(db=db)
    narr_eng    = NarrativeStabilityEngine(db=db)

    while True:
        try:
            # Trust scores
            result = await gov_engine.run_cycle()
            logger.info("governance_cycle_done", **{
                k: v for k, v in result.__dict__.items() if k != "unstable_engines"
            }, unstable=result.unstable_engines)

            # Recommendations: run when ≥1 engine healthy, or when not ALL are failing
            # (degraded engines still provide relative signal for weighting)
            if result.engines_healthy >= 1 or (result.engines_failing < result.engines_scored):
                rec_count = await weight_eng.generate_and_persist()
                logger.info("recommendations_generated", count=rec_count)
            else:
                logger.warning("recommendations_skipped", reason="all_engines_failing",
                               engines_scored=result.engines_scored)

            # Narrative stability
            narr_count = await narr_eng.run_cycle()
            logger.info("narrative_stability_computed", narratives=narr_count)

        except Exception:
            governance_errors_total.inc()
            logger.error("governance_main_error", exc=traceback.format_exc())

        await asyncio.sleep(interval)


if __name__ == "__main__":
    asyncio.run(_main())
