"""
Backtest Analytics API — Phase 2: Orchestration + Replay System

Read-only endpoints exposing replay run results and engine performance analytics.
All computation is pre-computed by BacktestingEngine / ReplayScheduler.
No SQL aggregation at request time beyond simple SELECTs.

Endpoints:
  GET  /backtest/summary               — full system performance summary
  GET  /backtest/engines               — per-engine metric ranking
  GET  /backtest/narratives            — narrative reliability breakdown
  GET  /backtest/regimes               — regime-specific accuracy
  POST /backtest/replay/trigger        — enqueue a replay job
  GET  /backtest/replay/runs           — list replay runs
  GET  /backtest/replay/runs/{run_id}  — detail + metrics for a run
"""
from __future__ import annotations

import json
import time
from datetime import datetime
from uuid import UUID

from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel

from src.engines.backtesting_engine import BacktestingEngine
from src.engines.engine_performance_dashboard import EnginePerformanceDashboard
from src.validation.metrics import validation_requests_total, validation_request_seconds

router = APIRouter(prefix="/backtest", tags=["backtest-analytics"])


def _track(endpoint: str, status: int, t0: float) -> None:
    validation_requests_total.labels(endpoint=endpoint, status_code=str(status)).inc()
    validation_request_seconds.labels(endpoint=endpoint).observe(time.monotonic() - t0)


# ── System Summary ────────────────────────────────────────────────────────────

@router.get("/summary")
async def get_summary(
    request: Request,
    window_hours: int = Query(24, description="Evaluation window in hours"),
    days: int = Query(30, ge=1, le=365, description="Lookback period in days"),
) -> dict:
    """
    Full system performance summary: engine rankings, narrative reliability,
    regime accuracy, and system-wide coverage statistics.

    Data is pre-computed by BacktestingEngine replay runs.
    """
    t0 = time.monotonic()
    try:
        dash = EnginePerformanceDashboard(request.app.state.db, request.app.state.redis)
        summary = await dash.get_full_summary(window_hours=window_hours, period_days=days)
        _track("/backtest/summary", 200, t0)
        return summary
    except Exception as exc:
        _track("/backtest/summary", 500, t0)
        raise HTTPException(status_code=500, detail=str(exc))


# ── Engine Analytics ──────────────────────────────────────────────────────────

@router.get("/engines")
async def get_engine_metrics(
    request: Request,
    window_hours: int = Query(24, description="Evaluation window in hours"),
    days: int = Query(30, ge=1, le=365),
) -> dict:
    """
    Per-engine metric ranking with precision, recall, FPR, F1, predictive strength,
    half-life, and calibration error. Ordered by predictive_strength descending.
    """
    t0 = time.monotonic()
    try:
        dash = EnginePerformanceDashboard(request.app.state.db, request.app.state.redis)
        ranking = await dash.get_engine_ranking(window_hours=window_hours, period_days=days)
        _track("/backtest/engines", 200, t0)
        return {
            "window_hours": window_hours,
            "period_days":  days,
            "count":        len(ranking),
            "engines":      ranking,
            "note": (
                "predictive_strength: 0=coin-flip, 100=perfect. "
                "half_life_hours: null=accuracy sustains past 168h window."
            ),
        }
    except Exception as exc:
        _track("/backtest/engines", 500, t0)
        raise HTTPException(status_code=500, detail=str(exc))


@router.get("/engines/{engine_name}/trend")
async def get_engine_trend(
    request: Request,
    engine_name: str,
    window_hours: int = Query(24),
    weeks: int = Query(8, ge=1, le=52),
) -> dict:
    """Weekly performance trend (accuracy + predictive strength) for one engine."""
    t0 = time.monotonic()
    try:
        dash = EnginePerformanceDashboard(request.app.state.db, request.app.state.redis)
        trend = await dash.get_performance_trend(engine_name, window_hours=window_hours, weeks=weeks)
        _track("/backtest/engines/trend", 200, t0)
        return {
            "engine_name":  engine_name,
            "window_hours": window_hours,
            "weeks":        weeks,
            "trend":        trend,
        }
    except Exception as exc:
        _track("/backtest/engines/trend", 500, t0)
        raise HTTPException(status_code=500, detail=str(exc))


# ── Narrative Analytics ───────────────────────────────────────────────────────

@router.get("/narratives")
async def get_narrative_metrics(
    request: Request,
    days: int = Query(30, ge=1, le=365),
) -> dict:
    """
    Narrative cluster reliability ranking.
    Shows which market narratives (ai_semiconductors, macro_liquidity, etc.)
    have historically produced the most accurate directional signals.
    """
    t0 = time.monotonic()
    try:
        dash = EnginePerformanceDashboard(request.app.state.db, request.app.state.redis)
        narratives = await dash.get_narrative_reliability(period_days=days)
        _track("/backtest/narratives", 200, t0)
        return {
            "period_days": days,
            "count":       len(narratives),
            "narratives":  narratives,
            "note": "avg_accuracy: directional accuracy for signals tagged with this narrative.",
        }
    except Exception as exc:
        _track("/backtest/narratives", 500, t0)
        raise HTTPException(status_code=500, detail=str(exc))


# ── Regime Analytics ─────────────────────────────────────────────────────────

@router.get("/regimes")
async def get_regime_metrics(
    request: Request,
    days: int = Query(30, ge=1, le=365),
    engine: str | None = Query(None, description="Filter to a specific engine"),
) -> dict:
    """
    Regime-specific signal accuracy.
    Shows how accurately each engine predicted direction during each macro regime
    (risk_on, risk_off, tightening, liquidity_expansion, volatility_regime).
    """
    t0 = time.monotonic()
    try:
        dash = EnginePerformanceDashboard(request.app.state.db, request.app.state.redis)
        regimes = await dash.get_regime_accuracy(engine_name=engine, period_days=days)
        _track("/backtest/regimes", 200, t0)
        return {
            "period_days":  days,
            "engine_filter": engine,
            "count":        len(regimes),
            "regimes":      regimes,
        }
    except Exception as exc:
        _track("/backtest/regimes", 500, t0)
        raise HTTPException(status_code=500, detail=str(exc))


# ── Replay Job Trigger ────────────────────────────────────────────────────────

class ReplayTriggerRequest(BaseModel):
    window_type:  str               # weekly | monthly | yearly | custom
    period_start: datetime
    period_end:   datetime
    engine_name:  str | None = None


@router.post("/replay/trigger")
async def trigger_replay(
    request: Request,
    body: ReplayTriggerRequest,
) -> dict:
    """
    Enqueue a replay job for the given time window.
    The replay-scheduler picks it up from Redis and executes BacktestingEngine.

    window_type: weekly | monthly | yearly | custom
    period_start / period_end: ISO 8601 timestamps (UTC recommended)
    engine_name: null = all engines

    Returns immediately with run_id. Poll GET /backtest/replay/runs/{run_id} for status.
    """
    t0 = time.monotonic()
    try:
        if body.period_end <= body.period_start:
            raise HTTPException(
                status_code=422,
                detail="period_end must be after period_start",
            )
        if body.window_type not in ("weekly", "monthly", "yearly", "custom"):
            raise HTTPException(
                status_code=422,
                detail="window_type must be: weekly | monthly | yearly | custom",
            )

        # Enqueue via Redis (replay-scheduler pops and executes)
        payload = json.dumps({
            "window_type":  body.window_type,
            "period_start": body.period_start.isoformat(),
            "period_end":   body.period_end.isoformat(),
            "engine_name":  body.engine_name,
            "retry_count":  0,
        })

        # Create DB record first so the run_id is trackable
        engine = BacktestingEngine(request.app.state.db, request.app.state.redis)
        run_id = await engine.create_run(
            window_type=body.window_type,
            period_start=body.period_start,
            period_end=body.period_end,
            engine_name=body.engine_name,
        )

        # Push to Redis queue (scheduler will pick up and overwrite with run_id)
        await request.app.state.redis.lpush(
            "replay:queue",
            json.dumps({**json.loads(payload), "run_id": run_id}),
        )

        _track("/backtest/replay/trigger", 202, t0)
        return {
            "run_id": run_id,
            "status": "queued",
            "message": "Replay job enqueued. Poll GET /backtest/replay/runs/{run_id} for results.",
        }
    except HTTPException:
        raise
    except Exception as exc:
        _track("/backtest/replay/trigger", 500, t0)
        raise HTTPException(status_code=500, detail=str(exc))


# ── Replay Run List + Detail ──────────────────────────────────────────────────

@router.get("/replay/runs")
async def list_replay_runs(
    request: Request,
    limit: int = Query(20, ge=1, le=100),
    status: str | None = Query(None, description="Filter by status"),
) -> dict:
    """List recent replay runs with status and summary counts."""
    t0 = time.monotonic()
    try:
        conditions = ["1=1"]
        params: list = [limit]

        if status:
            params = [status, limit]
            conditions.append("status = $1")
            params_final = [status, limit]
        else:
            params_final = [limit]

        where = " AND ".join(conditions)
        p_limit = f"${len(params_final)}"

        async with request.app.state.db.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT run_id, window_type, period_start, period_end,
                       engine_name, status, retry_count,
                       queued_at, started_at, completed_at,
                       total_signals, evaluated_signals, error_message
                  FROM replay_runs
                 WHERE {where}
                 ORDER BY queued_at DESC
                 LIMIT {p_limit}
                """,
                *params_final,
            )

        _track("/backtest/replay/runs", 200, t0)
        return {
            "count": len(rows),
            "runs":  [dict(r) for r in rows],
        }
    except Exception as exc:
        _track("/backtest/replay/runs", 500, t0)
        raise HTTPException(status_code=500, detail=str(exc))


@router.get("/replay/runs/{run_id}")
async def get_replay_run(
    request: Request,
    run_id: UUID,
) -> dict:
    """
    Detail for a specific replay run: metadata + full metric breakdown
    per engine / signal_type / window_hours including regime and narrative analysis.
    """
    t0 = time.monotonic()
    try:
        engine = BacktestingEngine(request.app.state.db)
        run = await engine.get_run(str(run_id))
        if not run:
            raise HTTPException(status_code=404, detail=f"Replay run {run_id} not found.")
        _track("/backtest/replay/runs/id", 200, t0)
        return run
    except HTTPException:
        _track("/backtest/replay/runs/id", 404, t0)
        raise
    except Exception as exc:
        _track("/backtest/replay/runs/id", 500, t0)
        raise HTTPException(status_code=500, detail=str(exc))
