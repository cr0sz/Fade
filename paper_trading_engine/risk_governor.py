"""Drawdown state machine and cluster-level edge aggregation."""
from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass

import structlog

logger = structlog.get_logger(__name__)

# ---------------------------------------------------------------------------
# Cluster / regime constants
# ---------------------------------------------------------------------------

CLUSTERS: dict[str, list[str]] = {
    "TREND":  ["TrendDetectionEngine", "MacroRegimeEngine"],
    "FLOW":   ["InstitutionalConfluenceEngine", "NarrativeEngine", "NewsBreakingEngine"],
    "STRESS": ["MeanReversionEngine"],
}

REGIME_CLUSTER_WEIGHTS: dict[str, dict[str, float]] = {
    "bull":    {"TREND": 1.3, "FLOW": 1.0, "STRESS": 1.0},
    "bear":    {"TREND": 0.5, "FLOW": 1.0, "STRESS": 1.5},
    "neutral": {"TREND": 1.0, "FLOW": 1.0, "STRESS": 1.0},
}

# ---------------------------------------------------------------------------
# DrawdownGovernor
# ---------------------------------------------------------------------------


class DrawdownState:
    NORMAL      = "NORMAL"
    SOFT_DERISK = "SOFT_DERISK"
    HARD_DERISK = "HARD_DERISK"
    HARD_STOP   = "HARD_STOP"
    RECOVERY    = "RECOVERY"


@dataclass(frozen=True)
class GovernorDecision:
    state: str
    size_scale: float
    max_open_delta: int
    block_new: bool
    transitioned: bool
    prev_state: str
    rebase_peak: bool = False


_STATE_TABLE: dict[str, tuple[float, int, bool]] = {
    DrawdownState.NORMAL:      (1.0, 0,  False),
    DrawdownState.SOFT_DERISK: (0.7, -1, False),
    DrawdownState.HARD_DERISK: (0.4, -2, False),
    DrawdownState.HARD_STOP:   (0.0, 0,  True),
    DrawdownState.RECOVERY:    (0.7, -1, False),
}


def _decision(state: str, prev: str, transitioned: bool, rebase_peak: bool = False) -> GovernorDecision:
    scale, delta, block = _STATE_TABLE[state]
    return GovernorDecision(
        state=state,
        size_scale=scale,
        max_open_delta=delta,
        block_new=block,
        transitioned=transitioned,
        prev_state=prev,
        rebase_peak=rebase_peak,
    )


class DrawdownGovernor:
    def __init__(self, redis, key_fn, notifier=None):
        self._redis    = redis
        self._notifier = notifier
        self._state    = DrawdownState.NORMAL
        self._state_key = key_fn("paper_trading:risk_governor_state")

        self._enabled        = os.environ.get("DD_GOVERNOR_ENABLED", "true").lower() != "false"
        self._soft_pct       = float(os.environ.get("DD_SOFT_PCT",       "0.03"))
        self._hard_pct       = float(os.environ.get("DD_HARD_PCT",       "0.05"))
        self._stop_pct       = float(os.environ.get("DD_STOP_PCT",       "0.10"))
        self._recovery_ratio = float(os.environ.get("DD_RECOVERY_RATIO", "0.95"))

    async def load(self) -> None:
        try:
            raw = await self._redis.get(self._state_key)
            if raw:
                data = json.loads(raw)
                candidate = data.get("state", DrawdownState.NORMAL)
                if candidate in _STATE_TABLE:
                    self._state = candidate
        except Exception as exc:
            logger.warning("risk_governor.load_failed", error=str(exc))

    def evaluate(self, balance: float, peak_balance: float) -> GovernorDecision:
        if not self._enabled:
            return _decision(DrawdownState.NORMAL, DrawdownState.NORMAL, False)

        if peak_balance <= 0:
            return _decision(DrawdownState.NORMAL, self._state, False)

        dd = 1.0 - balance / peak_balance

        # Determine target state
        if self._state == DrawdownState.HARD_STOP:
            if balance >= peak_balance * self._recovery_ratio:
                target = DrawdownState.RECOVERY
            else:
                target = DrawdownState.HARD_STOP
        elif self._state == DrawdownState.RECOVERY:
            if dd < self._soft_pct:
                target = DrawdownState.NORMAL
            elif dd >= self._stop_pct:
                target = DrawdownState.HARD_STOP
            elif dd >= self._hard_pct:
                target = DrawdownState.HARD_DERISK
            else:
                target = DrawdownState.RECOVERY
        else:
            if dd >= self._stop_pct:
                target = DrawdownState.HARD_STOP
            elif dd >= self._hard_pct:
                target = DrawdownState.HARD_DERISK
            elif dd >= self._soft_pct:
                target = DrawdownState.SOFT_DERISK
            else:
                target = DrawdownState.NORMAL

        transitioned = target != self._state
        prev = self._state
        rebase = transitioned and prev == DrawdownState.HARD_STOP and target == DrawdownState.RECOVERY
        self._state = target
        return _decision(target, prev, transitioned, rebase_peak=rebase)

    async def apply(self, decision: GovernorDecision) -> None:
        if not decision.transitioned:
            return

        try:
            payload = json.dumps({"state": decision.state, "entered_at": time.time()})
            await self._redis.set(self._state_key, payload)
        except Exception as exc:
            logger.warning("risk_governor.persist_failed", error=str(exc))

        if self._notifier:
            try:
                await self._notifier.send_message(
                    f"⚠️ Risk Governor: {decision.prev_state} → {decision.state} "
                    f"(size_scale={decision.size_scale})"
                )
            except Exception as exc:
                logger.warning("risk_governor.notify_failed", error=str(exc))


# ---------------------------------------------------------------------------
# ClusterEdgeCalculator
# ---------------------------------------------------------------------------


class ClusterEdgeCalculator:
    def __init__(self):
        self._clusters = CLUSTERS
        self._regime_weights = REGIME_CLUSTER_WEIGHTS

    def compute_cluster_edge(
        self,
        lift_lookup,
        bucket: str,
        exch: str,
        nominal_direction: str,
        regime_profile: str,
    ) -> float:
        weights = self._regime_weights.get(regime_profile, self._regime_weights["neutral"])

        weighted_sum   = 0.0
        weight_total   = 0.0

        for cluster_name, engines in self._clusters.items():
            cluster_weight = weights.get(cluster_name, 1.0)

            num   = 0.0
            denom = 0.0

            for engine in engines:
                result = lift_lookup(engine, bucket, exch)
                if result is None:
                    continue
                lift, n = result
                edge_for_engine = lift if nominal_direction == "long" else -lift
                num   += edge_for_engine * n
                denom += n

            if denom == 0:
                continue

            cluster_edge   = num / denom
            weighted_sum  += cluster_edge * cluster_weight
            weight_total  += cluster_weight

        if weight_total == 0:
            return 0.0

        overall_edge = weighted_sum / weight_total
        return float(max(-0.6, min(0.6, overall_edge)))


def cluster_of(engine_name: str) -> str | None:
    """Return the cluster name ('TREND'/'FLOW'/'STRESS') for a given engine, or None."""
    for cluster, engines in CLUSTERS.items():
        if engine_name in engines:
            return cluster
    return None
