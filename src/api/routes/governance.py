"""
Governance API Routes — Phase 4

6 read-only endpoints exposing adaptive governance intelligence.
Mounted at /api/v1/governance/* in validation_main.py.

All endpoints are evidence-based and recommendation-only.
No mutations to live engines, scoring formulas, or signal generation.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Query, Request

from src.engines.engine_governance_engine import EngineGovernanceEngine
from src.engines.adaptive_weighting_engine import AdaptiveWeightingEngine
from src.engines.narrative_stability_engine import NarrativeStabilityEngine
from src.engines.noise_contribution_engine import NoiseContributionEngine, _report_to_dict

router = APIRouter(prefix="/governance", tags=["governance"])


@router.get("/trust")
async def engine_trust_ranking(
    request: Request,
    window_hours: int = Query(24, description="Evaluation window in hours"),
) -> List[Dict[str, Any]]:
    """
    Engine trust ranking sorted by composite trust score (0–100).

    Trust score combines predictive strength, decay-adjusted precision,
    calibration stability, regime reliability, and recovery capability.
    Not financial advice — for system governance only.
    """
    engine = EngineGovernanceEngine(request.app.state.db, request.app.state.redis)
    ranking = await engine.get_trust_ranking(window_hours=window_hours)
    if not ranking:
        # Cold start: compute on-demand if no pre-computed scores exist
        await engine.run_cycle()
        ranking = await engine.get_trust_ranking(window_hours=window_hours)
    return ranking


@router.get("/stability")
async def engine_stability(
    request: Request,
    window_hours: int = Query(24, description="Evaluation window in hours"),
) -> Dict[str, Any]:
    """
    Unstable engine detection and recovery analysis.

    Returns engines flagged as unstable (high variance, rapid decay)
    alongside recovery capability scores and current dip streaks.
    """
    engine = EngineGovernanceEngine(request.app.state.db, request.app.state.redis)
    unstable  = await engine.detect_unstable_engines(window_hours=window_hours)
    recovery  = await engine.compute_recovery_analysis(window_hours=window_hours)
    ranking   = await engine.get_trust_ranking(window_hours=window_hours)

    return {
        "unstable_engines": unstable,
        "recovery_analysis": recovery,
        "stability_summary": {
            "total_engines": len(ranking),
            "healthy":  sum(1 for r in ranking if r.get("health_status") == "healthy"),
            "degraded": sum(1 for r in ranking if r.get("health_status") == "degraded"),
            "failing":  sum(1 for r in ranking if r.get("health_status") == "failing"),
            "unstable": len(unstable),
        },
    }


@router.get("/noise")
async def noise_report(
    request: Request,
    period_days: int = Query(30, ge=7, le=180, description="Look-back period in days"),
    window_hours: int = Query(24, description="Evaluation window in hours"),
) -> Dict[str, Any]:
    """
    Noise contribution analysis: FP share per engine, amplification chains,
    and reliability degradation hotspots.

    Amplification chains are engine pairs with correlated high FPR patterns
    (Pearson r > 0.70, both FPR > 0.30).
    """
    engine = NoiseContributionEngine(request.app.state.db, window_hours=window_hours)
    report = await engine.compute_report(period_days=period_days)
    return _report_to_dict(report)


@router.get("/narratives")
async def narrative_stability(
    request: Request,
) -> List[Dict[str, Any]]:
    """
    Narrative stability analysis: persistence scores, degradation rates,
    predictive longevity, and regime-specific accuracy.

    Collapsing narratives are flagged — consider reducing their contribution
    to the confluence scoring until stability recovers.
    """
    engine = NarrativeStabilityEngine(request.app.state.db)
    return await engine.get_all_stability()


@router.get("/recommendations")
async def governance_recommendations(
    request: Request,
    priority: Optional[str] = Query(None, description="Filter: high | medium | low"),
    target_type: Optional[str] = Query(None, description="Filter: engine | confidence_level | narrative"),
) -> List[Dict[str, Any]]:
    """
    Active governance recommendations (unacknowledged, non-expired).

    Recommendations are evidence-based and sorted by priority then confidence.
    They MUST be reviewed and applied manually — no automatic action is taken.
    """
    engine = AdaptiveWeightingEngine(request.app.state.db)
    return await engine.get_active_recommendations(
        priority=priority, target_type=target_type
    )


@router.get("/recovery")
async def recovery_analysis(
    request: Request,
    window_hours: int = Query(24, description="Evaluation window in hours"),
    weeks: int = Query(12, ge=4, le=52, description="Weeks of history"),
) -> List[Dict[str, Any]]:
    """
    Recovery capability analysis per engine.

    Shows historical dip periods, how many resolved vs remained unresolved,
    and current degradation streak length. Sorted by recovery capability (worst first).
    """
    engine = EngineGovernanceEngine(
        request.app.state.db,
        request.app.state.redis,
    )
    return await engine.compute_recovery_analysis(window_hours=window_hours, weeks=weeks)
