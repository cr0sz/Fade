"""Portfolio Edge Intelligence — read-only view of open position quality.

GET /api/v1/dashboard/edge-intelligence
Returns per-position edge scores + portfolio aggregate.

Auth: same DASHBOARD_TOKEN as other dashboard endpoints.
"""
from __future__ import annotations

import os
from collections import defaultdict

from fastapi import APIRouter, Depends, Header, HTTPException, Query, Request

from src.core.logging import get_logger

logger = get_logger(__name__)

# ── Token auth (copied verbatim from dashboard.py — avoid circular import) ────

def require_dashboard_token(
    x_dashboard_token: str | None = Header(default=None, alias="X-Dashboard-Token"),
    token: str | None = Query(default=None),
) -> None:
    expected = os.getenv("DASHBOARD_TOKEN", "").strip()
    if not expected:
        raise HTTPException(
            status_code=401,
            detail="Dashboard auth not configured (DASHBOARD_TOKEN unset on server).",
        )
    supplied = (x_dashboard_token or token or "").strip()
    if supplied != expected:
        raise HTTPException(status_code=401, detail="Invalid or missing dashboard token.")


router = APIRouter(dependencies=[Depends(require_dashboard_token)])

# ── Engine → cluster mapping ───────────────────────────────────────────────────

_ENGINE_CLUSTER = {
    "TrendDetectionEngine": "TREND",
    "MacroRegimeEngine": "TREND",
    "NarrativeEngine": "FLOW",
    "NewsBreakingEngine": "FLOW",
    "InstitutionalConfluenceEngine": "FLOW",
    "MeanReversionEngine": "STRESS",
    "InstitutionalFlowEngine": "STRESS",
}

# ── SQL ────────────────────────────────────────────────────────────────────────

_POSITIONS_SQL = """
SELECT pt.asset,
       pt.direction,
       pt.notional,
       pt.entry_price,
       pt.signal_id,
       pt.router_action,
       pt.router_edge,
       EXTRACT(EPOCH FROM (now() - pt.opened_at)) / 3600.0 AS age_hours,
       sr.engine_name,
       ss.accuracy,
       ss.predictive_strength
  FROM paper_trades pt
  LEFT JOIN signals_registry sr ON pt.signal_id ~ '^[0-9]+$' AND sr.id = pt.signal_id::bigint
  LEFT JOIN LATERAL (
      SELECT accuracy, predictive_strength
        FROM signal_scores
       WHERE engine_name = sr.engine_name
         AND window_hours = 24
       ORDER BY computed_at DESC
       LIMIT 1
  ) ss ON true
 WHERE pt.closed_at IS NULL
   AND pt.close_reason IS NULL
   AND pt.mode = 'live'
"""

_REGIME_SQL = """
SELECT regime, confidence FROM macro_regimes ORDER BY computed_at DESC LIMIT 1
"""

_REPLAY_SQL = """
SELECT engine_name, regime_breakdown FROM replay_metrics
"""


@router.get("/dashboard/edge-intelligence")
async def get_edge_intelligence(request: Request) -> dict:
    """Return per-position edge scores and portfolio aggregate.

    Gracefully degrades: any DB error returns empty result rather than 500.
    """
    db = request.app.state.db

    # ── 1. Fetch open positions with engine metrics ───────────────────────────
    positions: list[dict] = []
    try:
        async with db.acquire() as conn:
            rows = await conn.fetch(_POSITIONS_SQL)
    except Exception as exc:
        logger.warning("edge_intelligence_positions_failed", error=str(exc))
        return {"positions": [], "aggregate": _empty_aggregate()}

    for row in rows:
        router_edge = float(row["router_edge"] or 0.0)
        accuracy = float(row["accuracy"] or 0.5)
        pred_strength = float(row["predictive_strength"] or 0.0)
        position_edge_score = round(
            router_edge * 0.5
            + max(0.0, accuracy - 0.5) * 2.0 * 0.3
            + pred_strength / 100.0 * 0.2,
            4,
        )
        cluster = _ENGINE_CLUSTER.get(row["engine_name"] or "", "UNKNOWN")
        positions.append({
            "asset": row["asset"],
            "direction": row["direction"],
            "notional": float(row["notional"] or 0.0),
            "entry_price": float(row["entry_price"] or 0.0),
            "signal_id": row["signal_id"],
            "router_action": row["router_action"],
            "router_edge": router_edge,
            "age_hours": round(float(row["age_hours"] or 0.0), 2),
            "engine_name": row["engine_name"],
            "accuracy": accuracy,
            "predictive_strength": pred_strength,
            "position_edge_score": position_edge_score,
            "cluster": cluster,
        })

    # ── 2. Regime alignment ───────────────────────────────────────────────────
    current_regime: str = "unknown"
    regime_confidence: float = 0.0
    replay_breakdown: dict[str, dict] = {}  # engine_name → regime_breakdown dict

    try:
        async with db.acquire() as conn:
            regime_row = await conn.fetchrow(_REGIME_SQL)
            if regime_row:
                current_regime = regime_row["regime"] or "unknown"
                regime_confidence = float(regime_row["confidence"] or 0.0)

            replay_rows = await conn.fetch(_REPLAY_SQL)
            for rrow in replay_rows:
                eng = rrow["engine_name"]
                # asyncpg returns JSONB as a Python dict already
                rb = rrow["regime_breakdown"]
                if eng and isinstance(rb, dict):
                    replay_breakdown[eng] = rb
    except Exception as exc:
        logger.warning("edge_intelligence_regime_failed", error=str(exc))

    # Count how many open positions have their engine aligned with current regime
    if positions:
        count_aligned = 0
        for p in positions:
            eng = p["engine_name"] or ""
            rb = replay_breakdown.get(eng, {})
            if rb.get(current_regime, 0.5) > 0.5:
                count_aligned += 1
        regime_alignment = round(count_aligned / len(positions), 4)
    else:
        regime_alignment = 0.0

    # ── 3. Portfolio aggregate ────────────────────────────────────────────────
    total_notional = sum(p["notional"] for p in positions)
    portfolio_edge_score = (
        sum(p["position_edge_score"] * p["notional"] for p in positions) / total_notional
        if total_notional > 0 else 0.0
    )

    cluster_totals: dict[str, float] = defaultdict(float)
    for p in positions:
        cluster_totals[p["cluster"]] += p["notional"]
    cluster_distribution = {
        k: round(v / total_notional * 100, 1) if total_notional > 0 else 0.0
        for k, v in cluster_totals.items()
    }

    high_edge = sum(1 for p in positions if p["position_edge_score"] > 0.3)
    low_edge = sum(1 for p in positions if p["position_edge_score"] < 0.1)
    edge_at_risk = sum(p["notional"] for p in positions if p["position_edge_score"] < 0.1)

    aggregate = {
        "total_positions": len(positions),
        "total_notional": round(total_notional, 2),
        "portfolio_edge_score": round(portfolio_edge_score, 4),
        "high_edge_positions": high_edge,
        "low_edge_positions": low_edge,
        "edge_at_risk_notional": round(edge_at_risk, 2),
        "cluster_distribution": cluster_distribution,
        "regime_alignment": regime_alignment,
        "current_regime": current_regime,
        "regime_confidence": round(regime_confidence, 4),
    }

    return {"positions": positions, "aggregate": aggregate}


def _empty_aggregate() -> dict:
    return {
        "total_positions": 0,
        "total_notional": 0.0,
        "portfolio_edge_score": 0.0,
        "high_edge_positions": 0,
        "low_edge_positions": 0,
        "edge_at_risk_notional": 0.0,
        "cluster_distribution": {},
        "regime_alignment": 0.0,
        "current_regime": "unknown",
        "regime_confidence": 0.0,
    }
