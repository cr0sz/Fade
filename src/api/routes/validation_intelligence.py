"""
Validation Intelligence Routes — Phase 3

5 read-only endpoints aggregating system-wide validation quality metrics.
Mounted at /api/v1/validation/* in validation_main.py.
"""
from __future__ import annotations

import dataclasses
from datetime import datetime
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Query, Request

from src.engines.validation_intelligence_api import ValidationIntelligenceAPI
from src.engines.replay_observability_dashboard import ReplayObservabilityDashboard

router = APIRouter(prefix="/validation", tags=["validation-intelligence"])


def _serialize(obj: Any) -> Any:
    """Recursively convert dataclasses and datetimes to JSON-safe types."""
    if dataclasses.is_dataclass(obj) and not isinstance(obj, type):
        return {f.name: _serialize(getattr(obj, f.name)) for f in dataclasses.fields(obj)}
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, list):
        return [_serialize(i) for i in obj]
    if isinstance(obj, dict):
        return {k: _serialize(v) for k, v in obj.items()}
    return obj


@router.get("/system-health")
async def system_health(request: Request) -> Dict[str, Any]:
    """
    System-wide validation health snapshot.

    Returns overall precision/recall, coverage percentage, engine health distribution,
    queue depth, data freshness, and a status string (healthy | degraded | critical).
    """
    api = ValidationIntelligenceAPI(request.app.state.db, request.app.state.redis)
    result = await api.get_system_health()
    return _serialize(result)


@router.get("/engine-decay")
async def engine_decay(
    request: Request,
    window_hours: int = Query(24, description="Evaluation window in hours"),
    weeks: int = Query(8, ge=1, le=52, description="Weeks of trend history"),
) -> List[Dict[str, Any]]:
    """
    Per-engine decay metrics including half-life, predictive strength,
    weekly accuracy trend, and whether the engine is currently decaying.
    """
    api = ValidationIntelligenceAPI(request.app.state.db, request.app.state.redis)
    results = await api.get_engine_decay(window_hours=window_hours, weeks=weeks)
    return _serialize(results)


@router.get("/calibration")
async def calibration(
    request: Request,
    days: int = Query(30, ge=1, le=365, description="Look-back period in days"),
) -> List[Dict[str, Any]]:
    """
    Confidence calibration analysis per engine.

    Shows actual accuracy per confidence tier (high/medium/low) vs expected,
    weighted calibration error, and whether the engine is well-calibrated.
    """
    api = ValidationIntelligenceAPI(request.app.state.db, request.app.state.redis)
    results = await api.get_calibration_analysis(days=days)
    return _serialize(results)


@router.get("/failures")
async def failures(
    request: Request,
    days: int = Query(7, ge=1, le=90, description="Look-back period in days"),
) -> Dict[str, Any]:
    """
    Failure and coverage analysis.

    Returns failed replay counts, retry-exhausted jobs, engines with no outcome
    data, uncovered signal count, and pending outcome windows.
    """
    api = ValidationIntelligenceAPI(request.app.state.db, request.app.state.redis)
    result = await api.get_failure_analysis(days=days)
    return _serialize(result)


@router.get("/reliability")
async def reliability(
    request: Request,
    window_hours: int = Query(24, description="Evaluation window in hours"),
    days: int = Query(30, ge=1, le=365, description="Look-back period in days"),
) -> Dict[str, Any]:
    """
    System reliability summary.

    Engine ranking by predictive strength, best/worst regimes and narratives,
    and overall F1 + directional accuracy.
    """
    api = ValidationIntelligenceAPI(request.app.state.db, request.app.state.redis)
    result = await api.get_reliability_summary(window_hours=window_hours, days=days)
    return _serialize(result)


@router.get("/observability")
async def observability(
    request: Request,
    window_hours: int = Query(24, description="Evaluation window in hours"),
    days: int = Query(30, ge=1, le=365, description="Look-back period in days"),
    weeks: int = Query(12, ge=1, le=52, description="Weeks of decay curve history"),
) -> Dict[str, Any]:
    """
    Full replay observability bundle: health, throughput, decay curves,
    queue analytics, and coverage tracking. Useful for dashboard preloading.
    """
    dash = ReplayObservabilityDashboard(request.app.state.db, request.app.state.redis)
    return await dash.get_full_observability(
        window_hours=window_hours, days=days, weeks=weeks
    )
