"""
Regime Transition Intelligence — Phase 5

Analyzes how intelligence engines behave during macro regime transitions.
Detects early degradation signatures, identifies historically dangerous
transition pairs, and quantifies per-engine fragility.

Metrics computed:
  - transition_instability_score: trust score drop magnitude during transition
  - transition_volatility_score: std-dev of engine trust scores during transition
  - engine_fragility_scores: per-engine fragility map for this transition
  - is_historically_dangerous: whether similar past transitions caused precision collapses

Reads from: macro_regimes, engine_trust_scores, replay_metrics
Writes to: regime_transition_records
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from statistics import mean, stdev
from typing import Any, Dict, List, Optional, Tuple

import asyncpg

from src.core.logging import get_logger

logger = get_logger(__name__)

_PRE_WINDOW_DAYS  = 7
_POST_WINDOW_DAYS = 7
_FRAGILITY_THRESH = 10.0   # trust drop >= this during transition = fragile
_DANGER_THRESH    = 2       # past transitions with same pair and precision collapse
_INSTABILITY_HIGH = 40.0
_VOLATILITY_HIGH  = 20.0


@dataclass
class TransitionRecord:
    from_regime: str
    to_regime: str
    transition_at: datetime
    transition_instability_score: float
    transition_volatility_score: float
    engine_fragility_scores: Dict[str, float]
    is_historically_dangerous: bool
    danger_evidence: Dict[str, Any]
    degradation_signatures: Dict[str, Any]
    similarity_score: Optional[float]
    most_similar_transition_id: Optional[int]


class RegimeTransitionIntelligence:
    """
    Detects macro regime transitions and analyses engine fragility.
    Call run_cycle() each meta-intelligence interval.
    """

    def __init__(
        self,
        db: asyncpg.Pool,
        pre_window_days: int = _PRE_WINDOW_DAYS,
        post_window_days: int = _POST_WINDOW_DAYS,
    ):
        self._db = db
        self._pre = pre_window_days
        self._post = post_window_days

    # ── Main cycle ─────────────────────────────────────────────────────────────

    async def run_cycle(self) -> int:
        """Process unrecorded regime transitions. Returns count recorded."""
        transitions = await self._find_new_transitions()
        count = 0
        for (from_regime, to_regime, transition_at) in transitions:
            try:
                record = await self._analyze_transition(from_regime, to_regime, transition_at)
                await self._persist(record)
                count += 1
            except Exception as e:
                logger.error("regime_transition_analysis_error",
                             from_regime=from_regime, to_regime=to_regime, error=str(e))
        return count

    # ── Transition Detection ───────────────────────────────────────────────────

    async def _find_new_transitions(self) -> List[Tuple[str, str, datetime]]:
        """Find regime transitions not yet recorded in regime_transition_records."""
        async with self._db.acquire() as conn:
            regime_rows = await conn.fetch("""
                WITH lag_regimes AS (
                    SELECT
                        regime,
                        computed_at,
                        LAG(regime) OVER (ORDER BY computed_at) AS prev_regime
                    FROM macro_regimes
                    ORDER BY computed_at
                )
                SELECT regime AS to_regime, prev_regime AS from_regime, computed_at AS transition_at
                FROM lag_regimes
                WHERE prev_regime IS NOT NULL AND regime != prev_regime
                ORDER BY computed_at DESC
                LIMIT 50
            """)

            already_recorded = set()
            if regime_rows:
                recorded_rows = await conn.fetch("""
                    SELECT transition_at FROM regime_transition_records
                    WHERE transition_at >= NOW() - INTERVAL '30 days'
                """)
                already_recorded = {r["transition_at"] for r in recorded_rows}

        result = []
        for r in regime_rows:
            if r["transition_at"] not in already_recorded:
                result.append((r["from_regime"], r["to_regime"], r["transition_at"]))
        return result

    # ── Transition Analysis ────────────────────────────────────────────────────

    async def _analyze_transition(
        self,
        from_regime: str,
        to_regime: str,
        transition_at: datetime,
    ) -> TransitionRecord:
        pre_start  = transition_at - timedelta(days=self._pre)
        post_end   = transition_at + timedelta(days=self._post)

        # Engine trust scores in pre/post windows
        pre_trusts, post_trusts = await self._get_trust_windows(pre_start, transition_at, post_end)

        # Compute instability and volatility
        instability, volatility = self._compute_instability(pre_trusts, post_trusts)

        # Engine fragility scores
        fragility = self._compute_engine_fragility(pre_trusts, post_trusts)

        # Historical danger assessment
        danger, danger_evidence = await self._assess_historical_danger(from_regime, to_regime)

        # Early degradation signatures (engines that started dropping before transition)
        signatures = await self._detect_degradation_signatures(
            pre_start, transition_at
        )

        # Historical similarity
        sim_score, sim_id = await self._find_most_similar_transition(
            from_regime, to_regime, instability
        )

        return TransitionRecord(
            from_regime=from_regime,
            to_regime=to_regime,
            transition_at=transition_at,
            transition_instability_score=round(instability, 2),
            transition_volatility_score=round(volatility, 2),
            engine_fragility_scores=fragility,
            is_historically_dangerous=danger,
            danger_evidence=danger_evidence,
            degradation_signatures=signatures,
            similarity_score=sim_score,
            most_similar_transition_id=sim_id,
        )

    async def _get_trust_windows(
        self,
        pre_start: datetime,
        transition_at: datetime,
        post_end: datetime,
    ) -> Tuple[Dict[str, List[float]], Dict[str, List[float]]]:
        """Fetch trust scores before and after transition per engine."""
        async with self._db.acquire() as conn:
            rows = await conn.fetch("""
                SELECT engine_name, trust_score, computed_at
                FROM engine_trust_scores
                WHERE computed_at BETWEEN $1 AND $2
                  AND window_hours = 24
                ORDER BY engine_name, computed_at
            """, pre_start, post_end)

        pre: Dict[str, List[float]] = {}
        post: Dict[str, List[float]] = {}
        for r in rows:
            eng = r["engine_name"]
            val = float(r["trust_score"])
            if r["computed_at"] <= transition_at:
                pre.setdefault(eng, []).append(val)
            else:
                post.setdefault(eng, []).append(val)
        return pre, post

    def _compute_instability(
        self,
        pre: Dict[str, List[float]],
        post: Dict[str, List[float]],
    ) -> Tuple[float, float]:
        """
        Instability: average trust score drop across engines.
        Volatility: std-dev of all post-transition trust scores.
        """
        drops = []
        all_post_vals = []
        for eng in pre:
            if eng in post and pre[eng] and post[eng]:
                avg_pre  = mean(pre[eng])
                avg_post = mean(post[eng])
                drop = avg_pre - avg_post
                drops.append(drop)
            if eng in post:
                all_post_vals.extend(post[eng])

        instability = max(0.0, mean(drops)) if drops else 0.0
        volatility = (
            stdev(all_post_vals) if len(all_post_vals) >= 2 else 0.0
        )
        return instability, volatility

    def _compute_engine_fragility(
        self,
        pre: Dict[str, List[float]],
        post: Dict[str, List[float]],
    ) -> Dict[str, float]:
        """Per-engine fragility score (0–100): how much each engine dropped."""
        fragility = {}
        for eng in pre:
            if eng in post and pre[eng] and post[eng]:
                avg_pre  = mean(pre[eng])
                avg_post = mean(post[eng])
                drop = max(0.0, avg_pre - avg_post)
                # Scale: 0 drop = 0 fragility, 50 point drop = 100 fragility
                fragility[eng] = round(min(100.0, drop * 2.0), 2)
        return fragility

    async def _assess_historical_danger(
        self,
        from_regime: str,
        to_regime: str,
    ) -> Tuple[bool, Dict[str, Any]]:
        """
        A transition is historically dangerous if past same-pair transitions
        caused precision collapses (>= _DANGER_THRESH occurrences).
        """
        async with self._db.acquire() as conn:
            past = await conn.fetch("""
                SELECT id, transition_at, transition_instability_score,
                       post_transition_precision_delta
                FROM regime_transition_records
                WHERE from_regime = $1
                  AND to_regime = $2
                  AND transition_at < NOW()
                ORDER BY transition_at DESC
                LIMIT 20
            """, from_regime, to_regime)

        collapses = [
            r for r in past
            if r["post_transition_precision_delta"] is not None
            and float(r["post_transition_precision_delta"]) < -0.08
        ]
        is_dangerous = len(collapses) >= _DANGER_THRESH

        evidence = {
            "past_transitions_count": len(past),
            "precision_collapse_count": len(collapses),
            "avg_instability": round(
                mean([float(r["transition_instability_score"]) for r in past]), 2
            ) if past else None,
            "last_collapse_at": collapses[0]["transition_at"].isoformat()
            if collapses else None,
        }
        return is_dangerous, evidence

    async def _detect_degradation_signatures(
        self,
        window_start: datetime,
        transition_at: datetime,
    ) -> Dict[str, Any]:
        """
        Identify engines that started degrading BEFORE the regime transition
        (early warning signatures).
        """
        async with self._db.acquire() as conn:
            rows = await conn.fetch("""
                WITH ranked AS (
                    SELECT
                        engine_name,
                        trust_score,
                        computed_at,
                        LAG(trust_score) OVER (PARTITION BY engine_name ORDER BY computed_at) AS prev_trust
                    FROM engine_trust_scores
                    WHERE computed_at BETWEEN $1 AND $2
                      AND window_hours = 24
                )
                SELECT engine_name,
                       MIN(computed_at) AS first_drop_at,
                       COUNT(*) AS drop_count,
                       MIN(trust_score) AS min_trust
                FROM ranked
                WHERE prev_trust IS NOT NULL AND (prev_trust - trust_score) >= 5.0
                GROUP BY engine_name
                HAVING COUNT(*) >= 2
            """, window_start, transition_at)

        return {
            "engines_with_pre_degradation": [r["engine_name"] for r in rows],
            "details": [
                {
                    "engine": r["engine_name"],
                    "first_drop_at": r["first_drop_at"].isoformat() if r["first_drop_at"] else None,
                    "drop_events": int(r["drop_count"]),
                    "min_trust_before_transition": float(r["min_trust"]),
                }
                for r in rows
            ],
        }

    async def _find_most_similar_transition(
        self,
        from_regime: str,
        to_regime: str,
        current_instability: float,
    ) -> Tuple[Optional[float], Optional[int]]:
        """Find the historically most similar transition for pattern matching."""
        async with self._db.acquire() as conn:
            row = await conn.fetchrow("""
                SELECT id,
                       ABS(transition_instability_score - $3) AS dist
                FROM regime_transition_records
                WHERE from_regime = $1 AND to_regime = $2
                ORDER BY dist ASC
                LIMIT 1
            """, from_regime, to_regime, current_instability)

        if not row:
            return None, None
        dist = float(row["dist"])
        similarity = max(0.0, 1.0 - dist / 100.0)
        return round(similarity, 4), int(row["id"])

    # ── Persistence ────────────────────────────────────────────────────────────

    async def _persist(self, record: TransitionRecord) -> None:
        async with self._db.acquire() as conn:
            await conn.execute("""
                INSERT INTO regime_transition_records (
                    transition_at, from_regime, to_regime,
                    transition_instability_score, transition_volatility_score,
                    engine_fragility_scores, is_historically_dangerous,
                    danger_evidence, degradation_signatures,
                    similarity_score, most_similar_transition_id,
                    pre_window_days, post_window_days
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
            """,
                record.transition_at,
                record.from_regime,
                record.to_regime,
                record.transition_instability_score,
                record.transition_volatility_score,
                json.dumps(record.engine_fragility_scores),
                record.is_historically_dangerous,
                json.dumps(record.danger_evidence),
                json.dumps(record.degradation_signatures),
                record.similarity_score,
                record.most_similar_transition_id,
                self._pre,
                self._post,
            )

    # ── Read Methods ───────────────────────────────────────────────────────────

    async def get_transition_risk_report(
        self,
        days: int = 90,
    ) -> Dict[str, Any]:
        """Regime Transition Risk Report: instability, fragility, dangerous pairs."""
        async with self._db.acquire() as conn:
            rows = await conn.fetch("""
                SELECT
                    from_regime, to_regime,
                    COUNT(*)                                    AS transition_count,
                    AVG(transition_instability_score)           AS avg_instability,
                    AVG(transition_volatility_score)            AS avg_volatility,
                    SUM(CASE WHEN is_historically_dangerous THEN 1 ELSE 0 END) AS dangerous_count,
                    MAX(transition_instability_score)           AS max_instability,
                    MAX(detected_at)                            AS last_seen
                FROM regime_transition_records
                WHERE detected_at >= NOW() - ($1 || ' days')::INTERVAL
                GROUP BY from_regime, to_regime
                ORDER BY avg_instability DESC
                LIMIT 30
            """, str(days))

            # Per-engine fragility aggregated across all transitions
            frag_rows = await conn.fetch("""
                SELECT
                    kv.key AS engine_name,
                    AVG((kv.value)::numeric) AS avg_fragility,
                    COUNT(*) AS sample_count
                FROM regime_transition_records,
                     jsonb_each_text(engine_fragility_scores) AS kv
                WHERE detected_at >= NOW() - ($1 || ' days')::INTERVAL
                GROUP BY kv.key
                HAVING COUNT(*) >= 2
                ORDER BY avg_fragility DESC
            """, str(days))

        transitions = [
            {
                "from_regime": r["from_regime"],
                "to_regime": r["to_regime"],
                "transition_count": int(r["transition_count"]),
                "avg_instability": round(float(r["avg_instability"]), 2),
                "avg_volatility": round(float(r["avg_volatility"]), 2),
                "dangerous_count": int(r["dangerous_count"]),
                "max_instability": round(float(r["max_instability"]), 2),
                "last_seen": r["last_seen"].isoformat() if r["last_seen"] else None,
                "risk_level": (
                    "critical" if float(r["avg_instability"]) >= 30
                    else "high" if float(r["avg_instability"]) >= 15
                    else "medium" if float(r["avg_instability"]) >= 5
                    else "low"
                ),
            }
            for r in rows
        ]

        engine_fragility = [
            {
                "engine_name": r["engine_name"],
                "avg_fragility": round(float(r["avg_fragility"]), 2),
                "sample_count": int(r["sample_count"]),
            }
            for r in frag_rows
        ]

        return {
            "window_days": days,
            "transition_risk_matrix": transitions,
            "engine_fragility_ranking": engine_fragility,
            "summary": {
                "total_transitions": sum(t["transition_count"] for t in transitions),
                "dangerous_pairs": [
                    f"{t['from_regime']}→{t['to_regime']}"
                    for t in transitions if t["dangerous_count"] > 0
                ],
                "most_fragile_engine": engine_fragility[0]["engine_name"]
                if engine_fragility else None,
            },
        }

    async def get_similarity_analysis(
        self,
        from_regime: str,
        to_regime: str,
    ) -> List[Dict[str, Any]]:
        """Historical similarity analysis for a specific transition pair."""
        async with self._db.acquire() as conn:
            rows = await conn.fetch("""
                SELECT
                    transition_at, transition_instability_score, transition_volatility_score,
                    is_historically_dangerous, engine_fragility_scores,
                    post_transition_precision_delta, similarity_score
                FROM regime_transition_records
                WHERE from_regime = $1 AND to_regime = $2
                ORDER BY transition_at DESC
                LIMIT 20
            """, from_regime, to_regime)

        return [
            {
                "transition_at": r["transition_at"].isoformat(),
                "transition_instability_score": float(r["transition_instability_score"]),
                "transition_volatility_score": float(r["transition_volatility_score"]),
                "is_historically_dangerous": r["is_historically_dangerous"],
                "engine_fragility": json.loads(r["engine_fragility_scores"])
                if r["engine_fragility_scores"] else {},
                "post_transition_precision_delta": float(r["post_transition_precision_delta"])
                if r["post_transition_precision_delta"] else None,
                "similarity_score": float(r["similarity_score"])
                if r["similarity_score"] else None,
            }
            for r in rows
        ]
