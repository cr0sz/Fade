"""
Feedback Loop API Routes — Phase 6

4 read-only endpoints exposing the closed-loop learning layer.
Mounted at /api/v1/feedback/* in validation_main.py.

Answers:
  GET /engines/ranking           — which engine has the highest win rate?
  GET /providers/ranking         — which provider produces the best signals?
  GET /signal-types/comparison   — trend vs macro vs confluence performance
  GET /engines/performance-over-time — daily win_rate trend per engine
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Query, Request

from src.engines.feedback_aggregator import FeedbackAggregator

router = APIRouter(prefix="/feedback", tags=["feedback"])


def _aggregator(request: Request) -> FeedbackAggregator:
    return FeedbackAggregator(request.app.state.db, request.app.state.redis)


@router.get("/engines/ranking")
async def engine_ranking(
    request: Request,
    window_h: int = Query(default=24, ge=1, le=168, description="Evaluation window hours (1/4/24)"),
) -> List[Dict[str, Any]]:
    """
    Rank all engines by win_rate for the given evaluation window.

    Returns a list sorted by win_rate descending.  Each row includes
    win_rate_1h, win_rate_24h, win_rate_7d, avg_pnl_score, and
    feedback_signals_total.  Rows with no feedback data yet are omitted.
    Not financial advice — for system governance only.
    """
    return await _aggregator(request).get_engine_ranking(window_h=window_h)


@router.get("/providers/ranking")
async def provider_ranking(
    request: Request,
) -> List[Dict[str, Any]]:
    """
    Rank data providers by win_rate, noise_ratio, and avg_latency.

    win_rate     = fraction of directional outcomes that were correct
    noise_ratio  = (dup_exact + dup_fuzzy) / fetched  (lower is better)
    Sorted by win_rate descending.  Not financial advice.
    """
    return await _aggregator(request).get_provider_ranking()


@router.get("/signal-types/comparison")
async def signal_type_comparison(
    request: Request,
) -> List[Dict[str, Any]]:
    """
    Compare trend, macro, and confluence signal types by win_rate and avg_pnl
    across each evaluation window (1h / 4h / 24h).

    Useful for deciding which signal types to weight more heavily.
    Not financial advice.
    """
    return await _aggregator(request).get_signal_type_comparison()


@router.get("/engines/performance-over-time")
async def engine_performance_over_time(
    request: Request,
    engine_name: Optional[str] = Query(default=None, description="Filter to a single engine"),
    days: int = Query(default=30, ge=1, le=365, description="Lookback window in days"),
) -> List[Dict[str, Any]]:
    """
    Daily win_rate and avg_pnl trend for all engines (or a specific one).

    Use engine_name to filter to a single engine.
    Use days to control how far back to look (default: 30 days).
    Not financial advice.
    """
    return await _aggregator(request).get_performance_over_time(
        engine_name=engine_name, days=days
    )
