from __future__ import annotations

from fastapi import APIRouter, Request, Response
from pydantic import BaseModel

from src.core.config import get_settings
from src.core.cost_control import get_cost_controller
from src.providers.resilience import get_all_circuit_statuses

router = APIRouter()


class HealthResponse(BaseModel):
    status: str
    version: str
    cost_summary: dict


@router.get("/health", response_model=HealthResponse)
async def health_check(request: Request) -> HealthResponse:
    settings = get_settings()
    try:
        cost_summary = await get_cost_controller().get_cost_summary()
    except RuntimeError:
        cost_summary = {}

    return HealthResponse(
        status="ok",
        version=settings.APP_VERSION,
        cost_summary=cost_summary,
    )


@router.get("/ready")
async def readiness_check(request: Request) -> Response:
    """Deep readiness probe: verifies DB and Redis connectivity."""
    checks: dict[str, str] = {}
    ok = True

    try:
        pool = request.app.state.db
        async with pool.acquire() as conn:
            await conn.fetchval("SELECT 1")
        checks["db"] = "ok"
    except Exception as exc:
        checks["db"] = f"error: {exc}"
        ok = False

    try:
        redis = request.app.state.redis
        await redis.ping()
        checks["redis"] = "ok"
    except Exception as exc:
        checks["redis"] = f"error: {exc}"
        ok = False

    status_code = 200 if ok else 503
    return Response(
        content=str({"status": "ready" if ok else "not_ready", "checks": checks}),
        status_code=status_code,
        media_type="application/json",
    )


@router.get("/health/circuits")
async def circuit_status() -> dict:
    """Returns the state of all provider circuit breakers."""
    statuses = get_all_circuit_statuses()
    open_count = sum(1 for s in statuses if s["state"] == "open")
    return {
        "circuits": statuses,
        "summary": {
            "total": len(statuses),
            "open": open_count,
            "closed": sum(1 for s in statuses if s["state"] == "closed"),
            "half_open": sum(1 for s in statuses if s["state"] == "half_open"),
        },
    }
