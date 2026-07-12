"""Capital Injection Decision Engine (CIDe).

Statistically determines whether additional capital should be injected into the
live trading system. Does NOT affect trading execution.

Usage:
    from capital_injection_engine import CIDe
    engine = CIDe(pool)
    result = await engine.evaluate(requested_capital=65.0)
"""
from __future__ import annotations

import json
import logging
import math
import os
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)

_DECISION_REJECT        = "REJECT"
_DECISION_WAIT          = "WAIT_48H"
_DECISION_PARTIAL       = "PARTIAL_INJECT"
_DECISION_FULL          = "FULL_INJECT"

_DRAWDOWN_HARD_LIMIT    = 0.12
_LOSS_STREAK_HARD_LIMIT = 6
_REJECT_RATE_HARD_LIMIT = 0.50


class CIDe:
    """Capital Injection Decision Engine."""

    def __init__(self, pool) -> None:
        self._pool = pool

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def evaluate(
        self,
        requested_capital: float,
        metrics: Optional[dict] = None,
    ) -> dict:
        """Evaluate whether capital injection is safe.

        Args:
            requested_capital: Amount (USD) the caller wishes to inject.
            metrics: Pre-computed metrics dict; if None, fetched from DB live.

        Returns:
            Structured decision dict.
        """
        requested_capital = max(0.0, float(requested_capital))

        if metrics is None:
            metrics = await self._fetch_metrics()

        metrics = self._clamp_metrics(metrics)

        # Step 2 first — hard blockers override score entirely
        blocked, block_reasons = self._check_hard_blockers(metrics)

        if blocked:
            decision = _DECISION_REJECT
            recommended = 0.0
            score = 0.0
            reasoning = block_reasons
        else:
            # Step 1 — edge stability score
            score = self._compute_edge_stability_score(metrics)
            reasoning = self._build_reasoning(score, metrics)

            # Step 3 — decision thresholds
            decision, recommended = self._apply_thresholds(
                score, requested_capital
            )

        result = {
            "decision": decision,
            "recommended_capital": round(recommended, 2),
            "edge_stability_score": round(score, 4),
            "metrics_used": metrics,
            "reasoning": reasoning,
            "evaluated_at": datetime.utcnow().isoformat() + "Z",
        }

        await self._log_decision(requested_capital, result)
        return result

    # ------------------------------------------------------------------
    # Metrics fetching
    # ------------------------------------------------------------------

    async def _fetch_metrics(self) -> dict:
        async with self._pool.acquire() as conn:
            # net_pnl_7d
            net_pnl_7d = await conn.fetchval(
                "SELECT COALESCE(SUM(pnl), 0) FROM paper_trades "
                "WHERE closed_at > NOW() - INTERVAL '7 days'"
            )

            # net_pnl_14d
            net_pnl_14d = await conn.fetchval(
                "SELECT COALESCE(SUM(pnl), 0) FROM paper_trades "
                "WHERE closed_at > NOW() - INTERVAL '14 days'"
            )

            # max_drawdown_14d — worst cumulative PnL trough / starting balance proxy.
            # Running sum approach: find max peak-to-trough of the cumulative PnL curve.
            dd_rows = await conn.fetch(
                "SELECT pnl FROM paper_trades "
                "WHERE closed_at > NOW() - INTERVAL '14 days' AND pnl IS NOT NULL "
                "ORDER BY closed_at ASC"
            )
            if dd_rows:
                pnls = [float(r["pnl"]) for r in dd_rows]
                # Normalize by initial balance so a $30 loss on $10k = 0.3% dd.
                _init_bal = float(os.getenv("INITIAL_BALANCE", "10000"))
                peak, max_dd = 0.0, 0.0
                running = 0.0
                for p in pnls:
                    running += p
                    if running > peak:
                        peak = running
                    dd = (peak - running) / (_init_bal + max(peak, 0.0))
                    if dd > max_dd:
                        max_dd = dd
                max_drawdown_14d = min(1.0, max_dd)
            else:
                max_drawdown_14d = 0.0

            # win_rate_100_trades
            win_rate = await conn.fetchval(
                "SELECT COALESCE("
                "  COUNT(*) FILTER (WHERE pnl > 0)::float / NULLIF(COUNT(*), 0), 0"
                ") FROM ("
                "  SELECT pnl FROM paper_trades WHERE closed_at IS NOT NULL"
                "  ORDER BY closed_at DESC LIMIT 100"
                ") t"
            )

            # avg_trade_return (last 100)
            avg_trade_return = await conn.fetchval(
                "SELECT COALESCE(AVG(pnl), 0) FROM ("
                "  SELECT pnl FROM paper_trades WHERE closed_at IS NOT NULL"
                "  ORDER BY closed_at DESC LIMIT 100"
                ") t"
            )

            # loss_streak_max — fetch last 50 pnl values, compute in Python
            rows = await conn.fetch(
                "SELECT pnl FROM paper_trades WHERE closed_at IS NOT NULL"
                " ORDER BY closed_at DESC LIMIT 50"
            )
            pnl_series = [float(r["pnl"]) for r in rows]
            loss_streak_max = _compute_max_loss_streak(pnl_series)

            # truth_router confirm/reject rates from signal_outcomes last 7d
            so_rows = await conn.fetch(
                "SELECT direction_correct FROM signal_outcomes"
                " WHERE evaluated_at > NOW() - INTERVAL '7 days'"
                "   AND direction_correct IS NOT NULL"
            )
            if so_rows:
                total = len(so_rows)
                confirm_count = sum(1 for r in so_rows if r["direction_correct"])
                confirm_rate = confirm_count / total
                reject_rate = 1.0 - confirm_rate
            else:
                confirm_rate = 0.5
                reject_rate = 0.5

            # cluster_stability_score — 1.0 - min(1.0, stddev(outcome_score_delta) / 0.10)
            stddev_val = await conn.fetchval(
                "SELECT COALESCE(STDDEV(outcome_score_delta), 0.10)"
                " FROM signal_outcomes"
                " WHERE evaluated_at > NOW() - INTERVAL '7 days'"
                "   AND outcome_score_delta IS NOT NULL"
            )
            cluster_stability = 1.0 - min(1.0, float(stddev_val) / 0.10)

            # regime_consistency_score — fewer distinct regimes in 7d = more consistent
            try:
                n_regimes = await conn.fetchval(
                    "SELECT COUNT(DISTINCT to_regime) FROM regime_transition_records"
                    " WHERE transition_at > NOW() - INTERVAL '7 days'"
                ) or 1
                regime_consistency = max(0.0, 1.0 - (int(n_regimes) - 1) / 4.0)
            except Exception:
                regime_consistency = 0.6

            # engine_noise_score — fraction of engines with direction_correct < 0.45
            noise_rows = await conn.fetch(
                "SELECT sr.engine_name,"
                "  AVG(CASE WHEN so.direction_correct THEN 1.0 ELSE 0.0 END) AS accuracy"
                " FROM signal_outcomes so"
                " JOIN signals_registry sr ON sr.id = so.signal_id"
                " WHERE so.evaluated_at > NOW() - INTERVAL '7 days'"
                "   AND so.direction_correct IS NOT NULL"
                " GROUP BY sr.engine_name"
            )
            if noise_rows:
                noisy = sum(1 for r in noise_rows if float(r["accuracy"]) < 0.45)
                engine_noise_score = noisy / len(noise_rows)
            else:
                engine_noise_score = 0.5

        return {
            "net_pnl_7d": float(net_pnl_7d or 0),
            "net_pnl_14d": float(net_pnl_14d or 0),
            "max_drawdown_14d": float(max_drawdown_14d or 0),
            "win_rate_100_trades": float(win_rate or 0),
            "avg_trade_return": float(avg_trade_return or 0),
            "loss_streak_max": loss_streak_max,
            "truth_router_confirm_rate": confirm_rate,
            "truth_router_reject_rate": reject_rate,
            "cluster_stability_score": cluster_stability,
            "regime_consistency_score": regime_consistency,
            "engine_noise_score": engine_noise_score,
        }

    # ------------------------------------------------------------------
    # Input clamping
    # ------------------------------------------------------------------

    @staticmethod
    def _clamp_metrics(m: dict) -> dict:
        out = dict(m)
        out["max_drawdown_14d"]        = max(0.0, min(1.0, float(m.get("max_drawdown_14d", 0))))
        out["win_rate_100_trades"]     = max(0.0, min(1.0, float(m.get("win_rate_100_trades", 0))))
        out["truth_router_confirm_rate"] = max(0.0, min(1.0, float(m.get("truth_router_confirm_rate", 0.5))))
        out["truth_router_reject_rate"]  = max(0.0, min(1.0, float(m.get("truth_router_reject_rate", 0.5))))
        out["cluster_stability_score"] = max(0.0, min(1.0, float(m.get("cluster_stability_score", 0))))
        out["regime_consistency_score"] = max(0.0, min(1.0, float(m.get("regime_consistency_score", 0.6))))
        out["engine_noise_score"]      = max(0.0, min(1.0, float(m.get("engine_noise_score", 0.5))))
        out["loss_streak_max"]         = max(0, int(m.get("loss_streak_max", 0)))
        out["net_pnl_14d"]             = float(m.get("net_pnl_14d", 0))
        out["net_pnl_7d"]              = float(m.get("net_pnl_7d", 0))
        out["avg_trade_return"]        = float(m.get("avg_trade_return", 0))
        return out

    # ------------------------------------------------------------------
    # Step 2 — Hard blockers
    # ------------------------------------------------------------------

    @staticmethod
    def _check_hard_blockers(m: dict) -> tuple[bool, list[str]]:
        reasons: list[str] = []
        if m["max_drawdown_14d"] > _DRAWDOWN_HARD_LIMIT:
            reasons.append(
                f"Drawdown exceeds 12% hard limit ({m['max_drawdown_14d']:.1%})"
            )
        if m["loss_streak_max"] >= _LOSS_STREAK_HARD_LIMIT:
            reasons.append(
                f"Loss streak >= 6 trades (current: {m['loss_streak_max']})"
            )
        if m["truth_router_reject_rate"] > _REJECT_RATE_HARD_LIMIT:
            reasons.append(
                f"Router reject rate > 50% ({m['truth_router_reject_rate']:.1%})"
            )
        return bool(reasons), reasons

    # ------------------------------------------------------------------
    # Step 1 — Edge stability score
    # ------------------------------------------------------------------

    @staticmethod
    def _normalize_pnl(pnl: float) -> float:
        """Map PnL to [-1, +1]. $100 = 1.0, -$100 = -1.0. Clamp beyond."""
        return max(-1.0, min(1.0, pnl / 100.0))

    def _compute_edge_stability_score(self, m: dict) -> float:
        pnl14_norm = (self._normalize_pnl(m["net_pnl_14d"]) + 1) / 2  # [0,1]

        score = (
            0.25 * pnl14_norm
            + 0.20 * max(0.0, 1.0 - m["max_drawdown_14d"])
            + 0.15 * max(0.0, min(1.0, m["win_rate_100_trades"]))
            + 0.15 * max(0.0, min(1.0, m["truth_router_confirm_rate"]))
            + 0.10 * max(0.0, min(1.0, m["cluster_stability_score"]))
            + 0.10 * max(0.0, min(1.0, m["regime_consistency_score"]))
            + 0.05 * max(0.0, 1.0 - min(1.0, m["engine_noise_score"]))
        )
        return round(max(0.0, min(1.0, score)), 6)

    # ------------------------------------------------------------------
    # Step 3 — Decision thresholds
    # ------------------------------------------------------------------

    @staticmethod
    def _apply_thresholds(score: float, requested: float) -> tuple[str, float]:
        if score < 0.50:
            return _DECISION_REJECT, 0.0
        if score < 0.55:
            return _DECISION_WAIT, 0.0
        if score < 0.72:
            frac = 0.30 + 0.30 * (score - 0.55) / 0.17
            return _DECISION_PARTIAL, requested * frac
        return _DECISION_FULL, requested

    # ------------------------------------------------------------------
    # Step 4 — Reasoning
    # ------------------------------------------------------------------

    @staticmethod
    def _build_reasoning(score: float, m: dict) -> list[str]:
        reasons: list[str] = []
        reasons.append(f"Edge stability score: {score:.4f}")
        reasons.append(f"14d PnL: ${m['net_pnl_14d']:.2f}")
        reasons.append(f"7d PnL: ${m['net_pnl_7d']:.2f}")
        reasons.append(f"Win rate (last 100): {m['win_rate_100_trades']:.1%}")
        reasons.append(f"Max drawdown (14d): {m['max_drawdown_14d']:.1%}")
        reasons.append(f"Max loss streak (last 50): {m['loss_streak_max']}")
        reasons.append(f"Router confirm rate (7d): {m['truth_router_confirm_rate']:.1%}")
        reasons.append(f"Cluster stability: {m['cluster_stability_score']:.3f}")
        reasons.append(f"Regime consistency (constant): {m['regime_consistency_score']:.2f}")
        reasons.append(f"Engine noise score: {m['engine_noise_score']:.3f}")

        if score >= 0.72:
            reasons.append("Decision: FULL_INJECT — strong edge across all dimensions")
        elif score >= 0.55:
            reasons.append("Decision: PARTIAL_INJECT — moderate edge, inject fraction only")
        elif score >= 0.50:
            reasons.append("Decision: WAIT_48H — marginal score, reassess in 48h")
        else:
            reasons.append("Decision: REJECT — insufficient statistical edge")

        return reasons

    # ------------------------------------------------------------------
    # Logging
    # ------------------------------------------------------------------

    async def _log_decision(self, requested_capital: float, result: dict) -> None:
        try:
            async with self._pool.acquire() as conn:
                await conn.execute(
                    "INSERT INTO cide_decisions "
                    "(requested_capital, decision, recommended_capital, "
                    " edge_stability_score, metrics, reasoning, evaluated_at) "
                    "VALUES ($1, $2, $3, $4, $5::jsonb, $6::jsonb, $7)",
                    requested_capital,
                    result["decision"],
                    result["recommended_capital"],
                    result["edge_stability_score"],
                    json.dumps(result["metrics_used"]),
                    json.dumps(result["reasoning"]),
                    datetime.utcnow(),
                )
        except Exception as exc:  # noqa: BLE001
            logger.warning("CIDe: failed to log decision (table may not exist): %s", exc)


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def _compute_max_loss_streak(pnl_series: list[float]) -> int:
    """Return the longest consecutive run of negative PnL values."""
    max_streak = 0
    current = 0
    for pnl in pnl_series:
        if pnl < 0:
            current += 1
            max_streak = max(max_streak, current)
        else:
            current = 0
    return max_streak
