"""Meta-Strategy Selection Layer — regime-aware per-engine weight computation.

Runs every 30 min (via signal_edge_analyzer/scheduler.py).
Reads: macro_regimes, replay_metrics, engine_trust_scores, strategic_failure_clusters,
       regime_transition_records.
Writes: trading:engine_weights:meta (Redis hash) + composed final to trading:engine_weights.
Logs: meta_strategy_runs table (migration 043).

Gate: META_STRATEGY_ENABLED=false (default). When disabled, job exits immediately.
"""
from __future__ import annotations

import json
from typing import Any, Optional

import structlog

logger = structlog.get_logger(__name__)

_SEVERITY_ORDER = {"critical": 4, "high": 3, "medium": 2, "low": 1}


class MetaStrategySelector:
    GOV_KEY       = "trading:engine_weights:governance"
    META_KEY      = "trading:engine_weights:meta"
    PRECISION_KEY = "trading:engine_weights:precision"
    FINAL_KEY     = "trading:engine_weights"

    def __init__(self, pool, redis) -> None:
        self._pool  = pool
        self._redis = redis

    # ── Public entry point ────────────────────────────────────────────────────

    async def run_once(self) -> dict[str, float]:
        """Compute per-engine weight multipliers and publish to Redis.

        Returns the final composed weights dict, or {} on any error.
        """
        try:
            async with self._pool.acquire() as conn:
                regime, regime_conf = await self._current_regime(conn)
                regime_acc          = await self._regime_accuracy(conn)
                health_map          = await self._engine_health(conn)
                transition          = await self._recent_transition(conn)
                all_engines: set[str] = set(health_map.keys()) | set(regime_acc.keys())
                clusters, systemic_mult = await self._failure_clusters(conn, len(all_engines))

            meta_factors: dict[str, float] = {}
            breakdowns:   dict[str, dict]  = {}
            for engine in all_engines:
                factor, bd = self._compute_factor(
                    engine, regime, regime_acc, health_map, clusters, transition,
                    systemic_mult,
                )
                meta_factors[engine] = factor
                breakdowns[engine]   = bd

            # Write meta layer (TTL 7200s: stale weights must not persist if job dies)
            if self._redis and meta_factors:
                pipe = self._redis.pipeline()
                for engine, factor in meta_factors.items():
                    pipe.hset(self.META_KEY, engine, str(round(factor, 4)))
                pipe.expire(self.META_KEY, 7200)
                await pipe.execute()

            # Compose with governance and publish final
            final = await self._compose_and_publish(meta_factors)

            # Log to DB (best-effort)
            try:
                async with self._pool.acquire() as conn:
                    await conn.execute(
                        """
                        INSERT INTO meta_strategy_runs
                            (regime, regime_confidence, transition_active, weights, inputs)
                        VALUES ($1, $2, $3, $4::jsonb, $5::jsonb)
                        """,
                        regime,
                        regime_conf,
                        transition is not None,
                        json.dumps(final),
                        json.dumps(breakdowns),
                    )
            except Exception as log_exc:
                logger.warning("meta_strategy_log_failed", error=str(log_exc))

            logger.info(
                "meta_strategy_run_complete",
                regime=regime,
                regime_confidence=round(regime_conf, 3),
                engines=len(final),
                transition_active=transition is not None,
            )
            return final

        except Exception as exc:
            logger.exception("meta_strategy_run_failed", error=str(exc))
            return {}

    # ── DB queries ────────────────────────────────────────────────────────────

    @staticmethod
    async def _current_regime(conn) -> tuple[str, float]:
        row = await conn.fetchrow(
            "SELECT regime, confidence FROM macro_regimes ORDER BY computed_at DESC LIMIT 1"
        )
        if row is None:
            return ("unknown", 0.0)
        return (str(row["regime"]), float(row["confidence"] or 0.0))

    @staticmethod
    async def _regime_accuracy(conn) -> dict[str, dict[str, float]]:
        rows = await conn.fetch(
            "SELECT engine_name, regime_breakdown FROM replay_metrics"
        )
        result: dict[str, dict[str, float]] = {}
        for row in rows:
            rb = row["regime_breakdown"]
            if rb is None:
                continue
            # asyncpg returns JSONB as already-parsed dict
            if isinstance(rb, str):
                try:
                    rb = json.loads(rb)
                except Exception:
                    continue
            if isinstance(rb, dict):
                result[str(row["engine_name"])] = {k: float(v) for k, v in rb.items()}
        return result

    @staticmethod
    async def _engine_health(conn) -> dict[str, dict[str, Any]]:
        rows = await conn.fetch(
            """
            SELECT engine_name, health_status, trust_score, regime_breakdown_json, is_decaying
            FROM engine_trust_scores
            """
        )
        result: dict[str, dict[str, Any]] = {}
        for row in rows:
            rbj = row["regime_breakdown_json"]
            if rbj is not None:
                if isinstance(rbj, str):
                    try:
                        rbj = json.loads(rbj)
                    except Exception:
                        rbj = {}
                if not isinstance(rbj, dict):
                    rbj = {}
            else:
                rbj = {}
            result[str(row["engine_name"])] = {
                "health_status":        str(row["health_status"] or "unknown"),
                "trust_score":          float(row["trust_score"] or 1.0),
                "regime_breakdown_json": rbj,
                "is_decaying":          bool(row["is_decaying"] or False),
            }
        return result

    @staticmethod
    async def _failure_clusters(conn, total_engines: int) -> tuple[dict[str, str], float]:
        """Return (per_engine_worst_severity, systemic_multiplier).

        Clusters affecting > half the engine fleet are systemic — apply a single
        global multiplier (0.7 per critical, 0.85 per high) instead of the harsh
        per-engine ×0.3/×0.5 that eliminates all differentiation.
        """
        rows = await conn.fetch(
            """
            SELECT affected_engines, severity
            FROM strategic_failure_clusters
            WHERE is_active = true
            """
        )
        worst: dict[str, str] = {}
        systemic_mult = 1.0
        _SYSTEMIC_THRESHOLD = max(2, total_engines // 2 + 1)
        for row in rows:
            engines = row["affected_engines"]
            if engines is None:
                continue
            if isinstance(engines, str):
                try:
                    engines = json.loads(engines)
                except Exception:
                    continue
            if not isinstance(engines, list):
                continue
            sev = str(row["severity"] or "low").lower()
            if len(engines) >= _SYSTEMIC_THRESHOLD:
                # ponytail: systemic cluster → global penalty, not per-engine
                penalty = 0.7 if sev == "critical" else (0.85 if sev == "high" else 1.0)
                systemic_mult = min(systemic_mult, penalty)
            else:
                for eng in engines:
                    eng = str(eng)
                    current = worst.get(eng)
                    if current is None or _SEVERITY_ORDER.get(sev, 0) > _SEVERITY_ORDER.get(current, 0):
                        worst[eng] = sev
        return worst, systemic_mult

    @staticmethod
    async def _recent_transition(conn) -> Optional[dict]:
        row = await conn.fetchrow(
            """
            SELECT from_regime, to_regime, transition_instability_score, engine_fragility_scores
            FROM regime_transition_records
            WHERE transition_at > NOW() - INTERVAL '4 hours'
              AND transition_instability_score > 20.0
            ORDER BY transition_at DESC LIMIT 1
            """
        )
        if row is None:
            return None
        fragility = row["engine_fragility_scores"]
        if fragility is None:
            fragility = {}
        elif isinstance(fragility, str):
            try:
                fragility = json.loads(fragility)
            except Exception:
                fragility = {}
        if not isinstance(fragility, dict):
            fragility = {}
        return {
            "instability":            float(row["transition_instability_score"] or 0.0),
            "engine_fragility_scores": {k: float(v) for k, v in fragility.items()},
        }

    # ── Factor computation ────────────────────────────────────────────────────

    @staticmethod
    def _compute_factor(
        engine: str,
        regime: str,
        regime_acc: dict[str, dict[str, float]],
        health_map: dict[str, dict[str, Any]],
        clusters: dict[str, str],
        transition: Optional[dict],
        systemic_mult: float = 1.0,
    ) -> tuple[float, dict]:
        factor = 1.0
        acc: Optional[float] = None
        health_status: Optional[str] = None
        cluster_sev: Optional[str] = None
        frag: Optional[float] = None

        # 1. Regime accuracy adjustment
        acc = regime_acc.get(engine, {}).get(regime)
        if acc is None:
            acc = health_map.get(engine, {}).get("regime_breakdown_json", {}).get(regime)
        if acc is not None:
            if acc > 0.60:
                factor *= 1.3
            elif acc < 0.45:
                factor *= 0.6
            # else: acc in [0.45, 0.60] → no change

        # 2. Health cap
        health_status = health_map.get(engine, {}).get("health_status")
        if health_status == "failing":
            factor = min(factor, 0.4)
        elif health_status == "degraded":
            factor = min(factor, 0.7)

        # 3. Individual failure cluster penalty (systemic clusters excluded — handled globally)
        cluster_sev = clusters.get(engine)
        if cluster_sev == "critical":
            factor *= 0.3
        elif cluster_sev == "high":
            factor *= 0.5

        # 4. Systemic cluster: uniform fleet-wide dampening (preserves differentiation)
        factor *= systemic_mult

        # 5. Transition fragility
        if transition is not None:
            raw_frag = transition["engine_fragility_scores"].get(engine)
            if raw_frag is not None:
                frag = max(0.5, min(1.0, float(raw_frag)))
                factor *= frag

        # 6. Clamp
        factor = max(0.2, min(1.5, factor))

        breakdown = {
            "regime_acc":     acc,
            "health_status":  health_status,
            "cluster_sev":    cluster_sev,
            "systemic_mult":  systemic_mult,
            "frag":           frag,
            "final":          factor,
        }
        return (factor, breakdown)

    # ── Compose and publish ───────────────────────────────────────────────────

    async def _compose_and_publish(self, meta_factors: dict[str, float]) -> dict[str, float]:
        gov_raw: dict = {}
        precision_raw: dict = {}
        if self._redis:
            try:
                raw = await self._redis.hgetall(self.GOV_KEY)
                for k, v in raw.items():
                    key = k.decode() if isinstance(k, bytes) else str(k)
                    val = v.decode() if isinstance(v, bytes) else str(v)
                    try:
                        gov_raw[key] = float(val)
                    except ValueError:
                        pass
            except Exception as exc:
                logger.warning("meta_strategy_gov_read_failed", error=str(exc))

            try:
                raw = await self._redis.hgetall(self.PRECISION_KEY)
                for k, v in raw.items():
                    key = k.decode() if isinstance(k, bytes) else str(k)
                    val = v.decode() if isinstance(v, bytes) else str(v)
                    try:
                        precision_raw[key] = float(val)
                    except ValueError:
                        pass
            except Exception as exc:
                logger.warning("meta_strategy_precision_read_failed", error=str(exc))

        all_engines: set[str] = set(meta_factors.keys()) | set(gov_raw.keys()) | set(precision_raw.keys())
        final: dict[str, float] = {}
        for engine in all_engines:
            gov       = gov_raw.get(engine, 1.0)
            meta      = meta_factors.get(engine, 1.0)
            precision = precision_raw.get(engine, 1.0)
            final[engine] = max(0.2, min(2.0, gov * meta * precision))

        if self._redis and final:
            pipe = self._redis.pipeline()
            pipe.delete(self.FINAL_KEY)
            for engine, w in final.items():
                pipe.hset(self.FINAL_KEY, engine, str(round(w, 4)))
            await pipe.execute()

        return final
