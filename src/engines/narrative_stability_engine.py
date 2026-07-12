"""
Narrative Stability Engine — Phase 4

Tracks long-term narrative persistence, degradation velocity, and predictive
stability. Identifies narratives with recurring predictive power vs those that
have degraded to noise-level accuracy.

Stability floor: accuracy > 0.55 = "stable week"
Persistence score: fraction of evaluation weeks above stability floor
Half-life: weeks until persistence drops below 0.55 (OLS extrapolation)
Collapsing: was stable (>0.60), now below floor for 2+ consecutive weeks
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from statistics import mean
from typing import Any, Dict, List, Optional, Tuple

import asyncpg

from src.core.logging import get_logger
from src.engines.engine_math import ols_slope as _ols_slope

logger = get_logger(__name__)

_STABILITY_FLOOR      = 0.55   # accuracy above this = stable
_COLLAPSE_PREV_FLOOR  = 0.60   # must have been above this to be "collapsing"
_MIN_SAMPLES          = 3      # minimum weeks before scoring
_HALF_LIFE_MAX_WEEKS  = 26


@dataclass
class NarrativeStabilityScore:
    narrative_name: str
    persistence_score: Optional[float]   # 0–1
    degradation_rate: float              # OLS slope per week
    predictive_longevity_w: int          # consecutive stable weeks at end of series
    half_life_weeks: Optional[float]
    is_stable: bool
    is_collapsing: bool
    sample_weeks: int
    regime_breakdown: Dict[str, Any]
    computed_at: datetime


class NarrativeStabilityEngine:
    """Computes and persists narrative stability scores from replay_metrics."""

    def __init__(self, db: asyncpg.Pool, period_weeks: int = 12):
        self._db = db
        self._period_weeks = period_weeks

    # ── Main cycle ─────────────────────────────────────────────────────────────

    async def run_cycle(self) -> int:
        """Compute stability for all known narratives. Returns count persisted."""
        narratives = await self._list_narratives()
        count = 0
        for name in narratives:
            try:
                score = await self._compute(name)
                if score.sample_weeks >= _MIN_SAMPLES:
                    await self._persist(score)
                    count += 1
            except Exception as e:
                logger.error("narrative_stability_error", narrative=name, error=str(e))
        return count

    # ── Read Methods ───────────────────────────────────────────────────────────

    async def get_all_stability(self) -> List[Dict[str, Any]]:
        async with self._db.acquire() as conn:
            rows = await conn.fetch("""
                SELECT DISTINCT ON (narrative_name)
                    narrative_name, persistence_score, degradation_rate,
                    predictive_longevity_w, half_life_weeks,
                    is_stable, is_collapsing, sample_weeks,
                    regime_breakdown, computed_at
                FROM narrative_stability_scores
                ORDER BY narrative_name, computed_at DESC
            """)
        result = [
            {
                **{k: (float(v) if isinstance(v, (int, float)) else v) for k, v in dict(r).items()},
                "computed_at": r["computed_at"].isoformat() if r["computed_at"] else None,
            }
            for r in rows
        ]
        result.sort(key=lambda x: x.get("persistence_score") or 0, reverse=True)
        return result

    # ── Computation ────────────────────────────────────────────────────────────

    async def _list_narratives(self) -> List[str]:
        async with self._db.acquire() as conn:
            rows = await conn.fetch("""
                SELECT DISTINCT kv.key AS narrative
                FROM replay_metrics, jsonb_each(narrative_breakdown) AS kv
                WHERE jsonb_typeof(narrative_breakdown) = 'object'
                  AND kv.key != 'unknown'
                ORDER BY narrative
            """)
        return [r["narrative"] for r in rows]

    async def _compute(self, narrative_name: str) -> NarrativeStabilityScore:
        async with self._db.acquire() as conn:
            rows = await conn.fetch("""
                SELECT
                    date_trunc('week', computed_at)            AS wk,
                    AVG((kv.value->>'accuracy')::numeric)      AS acc,
                    SUM((kv.value->>'sample_count')::int)      AS samples
                FROM replay_metrics,
                     jsonb_each(narrative_breakdown) AS kv
                WHERE kv.key = $1
                  AND jsonb_typeof(narrative_breakdown) = 'object'
                  AND computed_at >= NOW() - ($2 || ' weeks')::INTERVAL
                GROUP BY wk
                HAVING SUM((kv.value->>'sample_count')::int) >= 5
                ORDER BY wk
            """, narrative_name, str(self._period_weeks))

            # Regime-specific accuracy
            regime_rows = await conn.fetch("""
                SELECT
                    rr.key   AS regime,
                    AVG((kv.value->>'accuracy')::numeric)  AS narr_acc,
                    SUM((kv.value->>'sample_count')::int)  AS total
                FROM replay_metrics,
                     jsonb_each(narrative_breakdown) AS kv,
                     jsonb_each(regime_breakdown)    AS rr
                WHERE kv.key = $1
                  AND jsonb_typeof(narrative_breakdown) = 'object'
                  AND jsonb_typeof(regime_breakdown)    = 'object'
                  AND computed_at >= NOW() - ($2 || ' weeks')::INTERVAL
                GROUP BY regime
                HAVING SUM((kv.value->>'sample_count')::int) >= 5
            """, narrative_name, str(self._period_weeks))

        weekly_acc = [float(r["acc"]) for r in rows if r["acc"] is not None]
        n = len(weekly_acc)

        if n == 0:
            return NarrativeStabilityScore(
                narrative_name=narrative_name,
                persistence_score=None,
                degradation_rate=0.0,
                predictive_longevity_w=0,
                half_life_weeks=None,
                is_stable=False,
                is_collapsing=False,
                sample_weeks=0,
                regime_breakdown={},
                computed_at=datetime.now(timezone.utc),
            )

        persistence = sum(1 for a in weekly_acc if a >= _STABILITY_FLOOR) / n
        slope, _ = _ols_slope(weekly_acc)

        # Consecutive stable weeks at the tail of the series
        longevity = 0
        for a in reversed(weekly_acc):
            if a >= _STABILITY_FLOOR:
                longevity += 1
            else:
                break

        # Collapsing: was stable (mean of first half > COLLAPSE_PREV_FLOOR)
        # but recent 2 weeks both below floor
        mid = max(1, n // 2)
        was_stable = mean(weekly_acc[:mid]) >= _COLLAPSE_PREV_FLOOR if n >= 4 else False
        recently_bad = all(a < _STABILITY_FLOOR for a in weekly_acc[-2:]) if n >= 2 else False
        is_collapsing = was_stable and recently_bad

        is_stable = (persistence >= 0.65 and not is_collapsing)

        # Half-life estimate via extrapolation of OLS to floor crossing
        half_life = _estimate_half_life(weekly_acc, _STABILITY_FLOOR, slope)

        regime_breakdown = {
            r["regime"]: {
                "accuracy": round(float(r["narr_acc"]), 4),
                "sample_count": int(r["total"]),
            }
            for r in regime_rows
        }

        return NarrativeStabilityScore(
            narrative_name=narrative_name,
            persistence_score=round(persistence, 4),
            degradation_rate=round(slope, 6),
            predictive_longevity_w=longevity,
            half_life_weeks=half_life,
            is_stable=is_stable,
            is_collapsing=is_collapsing,
            sample_weeks=n,
            regime_breakdown=regime_breakdown,
            computed_at=datetime.now(timezone.utc),
        )

    async def _persist(self, score: NarrativeStabilityScore) -> None:
        async with self._db.acquire() as conn:
            await conn.execute("""
                INSERT INTO narrative_stability_scores (
                    narrative_name, computed_at,
                    persistence_score, degradation_rate,
                    predictive_longevity_w, half_life_weeks,
                    is_stable, is_collapsing,
                    sample_weeks, regime_breakdown
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            """,
                score.narrative_name,
                score.computed_at,
                score.persistence_score,
                score.degradation_rate,
                score.predictive_longevity_w,
                score.half_life_weeks,
                score.is_stable,
                score.is_collapsing,
                score.sample_weeks,
                json.dumps(score.regime_breakdown),
            )


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _estimate_half_life(
    values: List[float],
    floor: float,
    slope: float,
) -> Optional[float]:
    """
    Estimate weeks until accuracy crosses floor using OLS extrapolation.
    Returns None if values are already below floor, already stable, or
    extrapolation would exceed _HALF_LIFE_MAX_WEEKS.
    """
    if not values:
        return None
    current = values[-1]
    if current <= floor:
        return 0.0
    if slope >= 0:
        return None   # not decaying toward floor
    # weeks until current + slope × t = floor
    weeks = (floor - current) / slope
    if weeks <= 0 or weeks > _HALF_LIFE_MAX_WEEKS:
        return None
    return round(weeks, 1)
