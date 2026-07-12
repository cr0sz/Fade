"""
Narrative Lifecycle Engine — Phase 5

Tracks the complete lifecycle of each market narrative across all evaluation stages:
  emergence → acceleration → peak → degradation → collapse → dormant

Detects:
  - Recurring narratives (re-emerge after collapse)
  - Hype narratives (short-lived, high-peak, rapid collapse)
  - Structural narratives (long-term persistent, >12 weeks stable)

Computes per narrative:
  - survivability_score (0–100): long-term predictive durability
  - recurrence_probability (0–1): probability of re-emergence after collapse
  - decay_velocity: OLS slope of accuracy over time
  - lifecycle_duration_weeks: total weeks since first emergence
  - collapse_count: how many times this narrative has collapsed

Reads from: narrative_stability_scores, replay_metrics (narrative_breakdown)
Writes to: narrative_lifecycle_records
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from statistics import mean
from typing import Any, Dict, List, Optional, Tuple

import asyncpg

from src.core.logging import get_logger
from src.engines.engine_governance_engine import _ols_slope

logger = get_logger(__name__)

_STABILITY_FLOOR     = 0.55
_PEAK_THRESHOLD      = 0.70    # accuracy >= this = "peak" phase
_HYPE_MAX_WEEKS      = 4       # narratives at peak < this many weeks = hype
_STRUCTURAL_MIN_WEEKS = 12     # stable for >= this many weeks = structural
_EMERGENCE_THRESHOLD  = 0.50   # first appearance above this = emergence
_COLLAPSE_THRESHOLD   = 0.40   # collapse if accuracy drops below this


@dataclass
class NarrativeLifecycle:
    narrative_name: str
    lifecycle_phase: str
    phase_started_at: Optional[datetime]
    survivability_score: Optional[float]
    recurrence_probability: Optional[float]
    decay_velocity: float
    lifecycle_duration_weeks: int
    peak_accuracy: Optional[float]
    current_accuracy: Optional[float]
    accuracy_at_emergence: Optional[float]
    is_hype: bool
    is_structural: bool
    is_recurring: bool
    collapse_count: int
    emergence_count: int
    avg_lifecycle_weeks: Optional[float]
    regime_performance: Dict[str, Any]


class NarrativeLifecycleEngine:
    """
    Computes and persists the full lifecycle state for every known narrative.
    """

    def __init__(self, db: asyncpg.Pool, history_weeks: int = 52):
        self._db = db
        self._history_weeks = history_weeks

    # ── Main cycle ─────────────────────────────────────────────────────────────

    async def run_cycle(self) -> int:
        """Process all narratives and persist lifecycle records. Returns count."""
        narratives = await self._list_narratives()
        count = 0
        for name in narratives:
            try:
                lc = await self._compute(name)
                await self._persist(lc)
                count += 1
            except Exception as e:
                logger.error("narrative_lifecycle_error", narrative=name, error=str(e))
        return count

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

    async def _compute(self, narrative_name: str) -> NarrativeLifecycle:
        weekly_acc, first_seen_at, regime_perf = await self._fetch_history(narrative_name)

        n = len(weekly_acc)
        current_acc = weekly_acc[-1] if weekly_acc else None
        peak_acc = max(weekly_acc) if weekly_acc else None
        first_acc = weekly_acc[0] if weekly_acc else None

        # OLS decay velocity
        slope, _ = _ols_slope(weekly_acc) if n >= 2 else (0.0, False)

        # Lifecycle phase detection
        phase, phase_started_at = self._detect_phase(weekly_acc)

        # Survivability: fraction of weeks above floor, weighted by recency
        survivability = self._compute_survivability(weekly_acc)

        # Collapse events: sequences where accuracy drops below COLLAPSE_THRESHOLD
        collapse_count, emergence_count, avg_lc_weeks = self._analyze_cycles(weekly_acc)

        # Recurrence probability
        recurrence_prob = self._compute_recurrence(collapse_count, emergence_count)

        # Hype: peaked high but lasted < _HYPE_MAX_WEEKS above _STABILITY_FLOOR
        is_hype = self._detect_hype(weekly_acc, peak_acc)

        # Structural: stable for _STRUCTURAL_MIN_WEEKS weeks
        is_structural = self._detect_structural(weekly_acc)

        is_recurring = emergence_count >= 2

        lifecycle_duration = n

        return NarrativeLifecycle(
            narrative_name=narrative_name,
            lifecycle_phase=phase,
            phase_started_at=phase_started_at,
            survivability_score=survivability,
            recurrence_probability=recurrence_prob,
            decay_velocity=slope,
            lifecycle_duration_weeks=lifecycle_duration,
            peak_accuracy=round(float(peak_acc), 4) if peak_acc else None,
            current_accuracy=round(float(current_acc), 4) if current_acc else None,
            accuracy_at_emergence=round(float(first_acc), 4) if first_acc else None,
            is_hype=is_hype,
            is_structural=is_structural,
            is_recurring=is_recurring,
            collapse_count=collapse_count,
            emergence_count=emergence_count,
            avg_lifecycle_weeks=avg_lc_weeks,
            regime_performance=regime_perf,
        )

    async def _fetch_history(
        self, narrative_name: str
    ) -> Tuple[List[float], Optional[datetime], Dict[str, Any]]:
        """Fetch weekly accuracy series and regime-specific performance."""
        async with self._db.acquire() as conn:
            weekly_rows = await conn.fetch("""
                SELECT
                    date_trunc('week', computed_at)           AS wk,
                    AVG((kv.value->>'accuracy')::numeric)     AS acc,
                    MIN(computed_at)                          AS first_in_week
                FROM replay_metrics,
                     jsonb_each(narrative_breakdown) AS kv
                WHERE kv.key = $1
                  AND jsonb_typeof(narrative_breakdown) = 'object'
                  AND computed_at >= NOW() - ($2 || ' weeks')::INTERVAL
                GROUP BY wk
                HAVING SUM((kv.value->>'sample_count')::int) >= 3
                ORDER BY wk
            """, narrative_name, str(self._history_weeks))

            regime_rows = await conn.fetch("""
                SELECT
                    rr.key  AS regime,
                    AVG((kv.value->>'accuracy')::numeric) AS acc,
                    SUM((kv.value->>'sample_count')::int) AS samples
                FROM replay_metrics,
                     jsonb_each(narrative_breakdown) AS kv,
                     jsonb_each(regime_breakdown)    AS rr
                WHERE kv.key = $1
                  AND jsonb_typeof(narrative_breakdown) = 'object'
                  AND jsonb_typeof(regime_breakdown)    = 'object'
                  AND computed_at >= NOW() - ($2 || ' weeks')::INTERVAL
                GROUP BY regime
                HAVING SUM((kv.value->>'sample_count')::int) >= 5
            """, narrative_name, str(self._history_weeks))

        weekly_acc = [float(r["acc"]) for r in weekly_rows if r["acc"] is not None]
        first_at = weekly_rows[0]["first_in_week"] if weekly_rows else None

        regime_perf = {
            r["regime"]: {
                "accuracy": round(float(r["acc"]), 4),
                "sample_count": int(r["samples"]),
            }
            for r in regime_rows if r["acc"] is not None
        }

        return weekly_acc, first_at, regime_perf

    # ── Phase Detection ────────────────────────────────────────────────────────

    def _detect_phase(
        self,
        weekly_acc: List[float],
    ) -> Tuple[str, Optional[datetime]]:
        """Determine the current lifecycle phase from the weekly accuracy series."""
        if not weekly_acc:
            return "unknown", None

        n = len(weekly_acc)
        current = weekly_acc[-1]
        prev = weekly_acc[-2] if n >= 2 else current

        # Dormant: no data recently (handled by caller)
        if current < 0.35:
            return "collapse", None
        if current < _STABILITY_FLOOR:
            # Was it higher before?
            if n >= 4 and max(weekly_acc[:-2]) >= _STABILITY_FLOOR:
                return "degradation", None
            return "emergence", None
        if current >= _PEAK_THRESHOLD:
            if prev < current:
                return "acceleration", None
            return "peak", None
        if current >= _STABILITY_FLOOR:
            if n >= 2 and prev > current:
                return "degradation", None
            return "acceleration", None

        return "emergence", None

    # ── Survivability ──────────────────────────────────────────────────────────

    def _compute_survivability(self, weekly_acc: List[float]) -> Optional[float]:
        """
        Survivability: fraction of weeks above STABILITY_FLOOR,
        with recency weighting (recent weeks count more).
        """
        n = len(weekly_acc)
        if n == 0:
            return None
        weights = [1.0 + i * 0.1 for i in range(n)]
        total_weight = sum(weights)
        weighted_stable = sum(
            w for w, acc in zip(weights, weekly_acc)
            if acc >= _STABILITY_FLOOR
        )
        return round(min(100.0, (weighted_stable / total_weight) * 100.0), 2)

    # ── Cycle Analysis ─────────────────────────────────────────────────────────

    def _analyze_cycles(
        self, weekly_acc: List[float]
    ) -> Tuple[int, int, Optional[float]]:
        """
        Count collapses (drops below _COLLAPSE_THRESHOLD) and re-emergences.
        Returns (collapse_count, emergence_count, avg_lifecycle_weeks).
        """
        if not weekly_acc:
            return 0, 1, None

        collapses = 0
        emergences = 1  # initial emergence counts
        cycle_lengths = []
        current_cycle_start = 0
        in_collapse = weekly_acc[0] < _COLLAPSE_THRESHOLD

        for i in range(1, len(weekly_acc)):
            was_collapsed = weekly_acc[i - 1] < _COLLAPSE_THRESHOLD
            is_collapsed  = weekly_acc[i] < _COLLAPSE_THRESHOLD

            if not was_collapsed and is_collapsed:
                # Just entered collapse
                collapses += 1
                cycle_len = i - current_cycle_start
                if cycle_len > 0:
                    cycle_lengths.append(cycle_len)

            elif was_collapsed and not is_collapsed:
                # Re-emerged from collapse
                emergences += 1
                current_cycle_start = i

        avg_cycle = mean(cycle_lengths) if cycle_lengths else None
        return collapses, emergences, round(avg_cycle, 1) if avg_cycle else None

    # ── Classification ─────────────────────────────────────────────────────────

    def _compute_recurrence(
        self, collapse_count: int, emergence_count: int
    ) -> float:
        """P(re-emerge after collapse) = emergences after first / collapse count."""
        if collapse_count == 0:
            return 0.0
        reemergences = max(0, emergence_count - 1)
        return round(min(1.0, reemergences / collapse_count), 4)

    def _detect_hype(
        self, weekly_acc: List[float], peak_acc: Optional[float]
    ) -> bool:
        """
        Hype: reached peak (>= _PEAK_THRESHOLD) but sustained it < _HYPE_MAX_WEEKS.
        """
        if not peak_acc or peak_acc < _PEAK_THRESHOLD:
            return False
        weeks_at_peak = sum(1 for a in weekly_acc if a >= _PEAK_THRESHOLD)
        return weeks_at_peak <= _HYPE_MAX_WEEKS

    def _detect_structural(self, weekly_acc: List[float]) -> bool:
        """
        Structural: >= _STRUCTURAL_MIN_WEEKS of continuous stability.
        """
        if len(weekly_acc) < _STRUCTURAL_MIN_WEEKS:
            return False
        # Count longest run above STABILITY_FLOOR
        max_run = current_run = 0
        for acc in weekly_acc:
            if acc >= _STABILITY_FLOOR:
                current_run += 1
                max_run = max(max_run, current_run)
            else:
                current_run = 0
        return max_run >= _STRUCTURAL_MIN_WEEKS

    # ── Persistence ────────────────────────────────────────────────────────────

    async def _persist(self, lc: NarrativeLifecycle) -> None:
        async with self._db.acquire() as conn:
            await conn.execute("""
                INSERT INTO narrative_lifecycle_records (
                    narrative_name, lifecycle_phase, phase_started_at,
                    survivability_score, recurrence_probability, decay_velocity,
                    lifecycle_duration_weeks, peak_accuracy, current_accuracy,
                    accuracy_at_emergence, is_hype, is_structural, is_recurring,
                    collapse_count, emergence_count, avg_lifecycle_weeks,
                    regime_performance
                ) VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                    $11, $12, $13, $14, $15, $16, $17
                )
            """,
                lc.narrative_name,
                lc.lifecycle_phase,
                lc.phase_started_at,
                lc.survivability_score,
                lc.recurrence_probability,
                lc.decay_velocity,
                lc.lifecycle_duration_weeks,
                lc.peak_accuracy,
                lc.current_accuracy,
                lc.accuracy_at_emergence,
                lc.is_hype,
                lc.is_structural,
                lc.is_recurring,
                lc.collapse_count,
                lc.emergence_count,
                lc.avg_lifecycle_weeks,
                json.dumps(lc.regime_performance),
            )

    # ── Read Methods ───────────────────────────────────────────────────────────

    async def get_lifecycle_intelligence_report(self) -> Dict[str, Any]:
        """Narrative Lifecycle Intelligence Report."""
        async with self._db.acquire() as conn:
            rows = await conn.fetch("""
                SELECT DISTINCT ON (narrative_name)
                    narrative_name, lifecycle_phase, survivability_score,
                    recurrence_probability, decay_velocity,
                    lifecycle_duration_weeks, peak_accuracy, current_accuracy,
                    is_hype, is_structural, is_recurring,
                    collapse_count, emergence_count, avg_lifecycle_weeks,
                    regime_performance, recorded_at
                FROM narrative_lifecycle_records
                ORDER BY narrative_name, recorded_at DESC
            """)

        narratives = [
            {
                **{k: (float(v) if isinstance(v, (int, float)) else v) for k, v in dict(r).items()},
                "recorded_at": r["recorded_at"].isoformat() if r["recorded_at"] else None,
            }
            for r in rows
        ]
        narratives.sort(key=lambda x: x.get("survivability_score") or 0, reverse=True)

        hype = [n for n in narratives if n.get("is_hype")]
        structural = [n for n in narratives if n.get("is_structural")]
        recurring = [n for n in narratives if n.get("is_recurring")]
        collapsing = [n for n in narratives if n.get("lifecycle_phase") == "collapse"]

        return {
            "narratives": narratives,
            "summary": {
                "total": len(narratives),
                "hype_count": len(hype),
                "structural_count": len(structural),
                "recurring_count": len(recurring),
                "currently_collapsing": len(collapsing),
                "phase_distribution": _count_phases(narratives),
            },
            "hype_narratives": [n["narrative_name"] for n in hype],
            "structural_narratives": [n["narrative_name"] for n in structural],
            "collapsing_narratives": [n["narrative_name"] for n in collapsing],
        }


def _count_phases(narratives: List[Dict[str, Any]]) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for n in narratives:
        p = n.get("lifecycle_phase", "unknown")
        counts[p] = counts.get(p, 0) + 1
    return counts
