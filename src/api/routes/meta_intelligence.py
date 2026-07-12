"""
Meta-Intelligence API Routes — Phase 5

6 read-only endpoints exposing strategic memory and meta-intelligence analytics.
Mounted at /api/v1/meta/* in validation_main.py.

Endpoints:
  GET /api/v1/meta/failures   — Historical Failure Atlas + systemic risk
  GET /api/v1/meta/transitions — Regime Transition Risk Report
  GET /api/v1/meta/narratives  — Narrative Lifecycle Intelligence Report
  GET /api/v1/meta/stability   — Strategic Stability Ranking + governance stress
  GET /api/v1/meta/similarity  — Historical Similarity Detection
  GET /api/v1/meta/risk        — Composite risk dashboard (all signals combined)

All outputs are analytical only. NOT financial advice.
No mutations to live engines, signal generation, or scoring formulas.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Query, Request

from src.engines.strategic_memory_engine import StrategicMemoryEngine
from src.engines.regime_transition_intelligence import RegimeTransitionIntelligence
from src.engines.narrative_lifecycle_engine import NarrativeLifecycleEngine
from src.engines.strategic_failure_analysis_engine import StrategicFailureAnalysisEngine

router = APIRouter(prefix="/meta", tags=["meta-intelligence"])


def _disclaimer() -> str:
    return "For informational purposes only. Not financial advice."


@router.get("/failures")
async def historical_failure_atlas(
    request: Request,
    days: int = Query(90, ge=7, le=365, description="Look-back window in days"),
    active_only: bool = Query(False, description="Return only active failure clusters"),
) -> Dict[str, Any]:
    """
    Historical Failure Atlas: recurring failure patterns, cluster analysis,
    false-positive environments, and systemic degradation history.

    Returns failure event clusters sorted by recurrence and severity.
    Recurring patterns (same failure mode reappearing) are the primary signal.
    """
    mem_engine     = StrategicMemoryEngine(request.app.state.db, request.app.state.redis)
    failure_engine = StrategicFailureAnalysisEngine(request.app.state.db, analysis_days=days)

    atlas   = await mem_engine.get_historical_failure_atlas(days=days)
    clusters = await failure_engine.get_failure_clusters(active_only=active_only)
    systemic = await failure_engine.get_systemic_risk_summary()

    return {
        "disclaimer": _disclaimer(),
        "window_days": days,
        "historical_failure_atlas": atlas,
        "failure_clusters": clusters,
        "systemic_risk": systemic,
    }


@router.get("/transitions")
async def regime_transition_risk_report(
    request: Request,
    days: int = Query(90, ge=14, le=365, description="Look-back window in days"),
    from_regime: Optional[str] = Query(None, description="Filter: source regime"),
    to_regime: Optional[str] = Query(None, description="Filter: destination regime"),
) -> Dict[str, Any]:
    """
    Regime Transition Risk Report: transition instability scores,
    engine fragility during transitions, and historically dangerous pairs.

    Includes similarity analysis for specific transition pairs when requested.
    """
    engine = RegimeTransitionIntelligence(request.app.state.db)
    report = await engine.get_transition_risk_report(days=days)

    similarity = None
    if from_regime and to_regime:
        similarity = await engine.get_similarity_analysis(from_regime, to_regime)

    return {
        "disclaimer": _disclaimer(),
        "window_days": days,
        "transition_risk_report": report,
        "similarity_analysis": similarity,
    }


@router.get("/narratives")
async def narrative_lifecycle_intelligence(
    request: Request,
) -> Dict[str, Any]:
    """
    Narrative Lifecycle Intelligence Report: full lifecycle state for all
    known market narratives (emergence → acceleration → peak → degradation → collapse).

    Identifies hype narratives, structurally persistent narratives, recurring
    narratives, and currently collapsing narratives.
    """
    engine = NarrativeLifecycleEngine(request.app.state.db)
    report = await engine.get_lifecycle_intelligence_report()

    return {
        "disclaimer": _disclaimer(),
        **report,
    }


@router.get("/stability")
async def strategic_stability_ranking(
    request: Request,
    days: int = Query(90, ge=7, le=365, description="Governance stress timeline window"),
) -> Dict[str, Any]:
    """
    Strategic Stability Ranking: long-term engine reliability scores combining
    trust score trends, decay velocity, and recovery consistency.

    Also includes the Governance Stress Timeline showing historical
    strategic_instability_index, governance_stress_score, and recurring failure density.
    """
    engine = StrategicMemoryEngine(request.app.state.db, request.app.state.redis)

    ranking  = await engine.get_strategic_stability_ranking()
    timeline = await engine.get_governance_stress_timeline(days=days)

    # Current snapshot (latest entry)
    current_snapshot = timeline[0] if timeline else {}

    return {
        "disclaimer": _disclaimer(),
        "strategic_stability_ranking": ranking,
        "current_strategic_state": {
            "strategic_instability_index": current_snapshot.get("strategic_instability_index"),
            "governance_stress_score": current_snapshot.get("governance_stress_score"),
            "recurring_failure_density": current_snapshot.get("recurring_failure_density"),
            "strategic_stability_rank": current_snapshot.get("strategic_stability_rank"),
            "snapshot_at": current_snapshot.get("snapshot_at"),
        },
        "governance_stress_timeline": timeline,
    }


@router.get("/similarity")
async def historical_similarity_detection(
    request: Request,
    top_n: int = Query(5, ge=1, le=20, description="Number of similar periods to return"),
    current_regime: str = Query("unknown", description="Current macro regime"),
) -> Dict[str, Any]:
    """
    Historical Similarity Detection: identifies past periods most similar
    to the current system state (instability index, regime, stress score).

    Helps operators understand what happened historically in similar conditions.
    """
    mem_engine = StrategicMemoryEngine(request.app.state.db, request.app.state.redis)

    # Get current instability from latest snapshot
    timeline = await mem_engine.get_governance_stress_timeline(days=7)
    current_instability = (
        timeline[0].get("strategic_instability_index", 0.0) if timeline else 0.0
    )

    similar = await mem_engine.get_historical_similarity(
        current_instability=current_instability,
        current_regime=current_regime,
        top_n=top_n,
    )

    return {
        "disclaimer": _disclaimer(),
        "current_instability_index": current_instability,
        "current_regime": current_regime,
        "similar_historical_periods": similar,
        "interpretation": (
            "Periods shown had similar strategic instability. "
            "Governance stress and failure density at those times provide "
            "context for evaluating current system risk."
        ),
    }


@router.get("/risk")
async def meta_risk_dashboard(
    request: Request,
    days: int = Query(30, ge=7, le=180, description="Look-back window"),
) -> Dict[str, Any]:
    """
    Composite Meta-Intelligence Risk Dashboard.

    Combines all Phase 5 signals into a unified risk view:
      - Strategic instability index (current + trend)
      - Active failure cluster count and severity
      - Dangerous regime transitions
      - Collapsing narratives
      - Long-term engine reliability ranking
      - Governance stress score timeline

    This is the primary operational endpoint for Phase 5 monitoring.
    Not financial advice — for system governance only.
    """
    mem_engine     = StrategicMemoryEngine(request.app.state.db, request.app.state.redis)
    failure_engine = StrategicFailureAnalysisEngine(request.app.state.db, analysis_days=days)
    narr_engine    = NarrativeLifecycleEngine(request.app.state.db)
    regime_engine  = RegimeTransitionIntelligence(request.app.state.db)

    # Gather all signals concurrently
    import asyncio
    (
        atlas,
        systemic_risk,
        narr_report,
        transition_report,
        stability_ranking,
        timeline,
    ) = await asyncio.gather(
        mem_engine.get_historical_failure_atlas(days=days),
        failure_engine.get_systemic_risk_summary(),
        narr_engine.get_lifecycle_intelligence_report(),
        regime_engine.get_transition_risk_report(days=days),
        mem_engine.get_strategic_stability_ranking(),
        mem_engine.get_governance_stress_timeline(days=days),
        return_exceptions=True,
    )

    # Safely unwrap exceptions
    def safe(v, default):
        return default if isinstance(v, Exception) else v

    atlas            = safe(atlas, {"failure_atlas": [], "summary": {}})
    systemic_risk    = safe(systemic_risk, {"total_active_clusters": 0, "risk_level": "unknown"})
    narr_report      = safe(narr_report, {"narratives": [], "summary": {}})
    transition_report = safe(transition_report, {"transition_risk_matrix": [], "summary": {}})
    stability_ranking = safe(stability_ranking, [])
    timeline          = safe(timeline, [])

    current = timeline[0] if timeline else {}

    # Compute composite risk level
    instability = float(current.get("strategic_instability_index") or 0)
    stress      = float(current.get("governance_stress_score") or 0)
    failure_density = float(current.get("recurring_failure_density") or 0)
    composite = (instability * 0.4 + stress * 0.35 + failure_density * 0.25)
    risk_level = (
        "critical" if composite >= 70
        else "high" if composite >= 50
        else "medium" if composite >= 30
        else "low"
    )

    return {
        "disclaimer": _disclaimer(),
        "composite_risk_level": risk_level,
        "composite_risk_score": round(composite, 2),
        "current_state": {
            "strategic_instability_index": current.get("strategic_instability_index"),
            "governance_stress_score": current.get("governance_stress_score"),
            "recurring_failure_density": current.get("recurring_failure_density"),
            "strategic_stability_rank": current.get("strategic_stability_rank"),
            "snapshot_at": current.get("snapshot_at"),
        },
        "failure_summary": atlas.get("summary", {}),
        "systemic_risk": systemic_risk,
        "narrative_lifecycle_summary": narr_report.get("summary", {}),
        "transition_risk_summary": transition_report.get("summary", {}),
        "top_stable_engines": stability_ranking[:3],
        "top_fragile_engines": sorted(
            stability_ranking,
            key=lambda x: (x.get("trust_score") or 100)
        )[:3],
        "governance_stress_trend": timeline[:7],  # last 7 snapshots
    }
