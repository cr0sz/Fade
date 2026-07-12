"""
Level 4 Validation API routes.

All endpoints are READ-ONLY — they query pre-computed results from
signals_registry, signal_outcomes, backtest_runs, backtest_results,
and engine_performance_metrics.

No computation at request time. Backtest runs are dispatched to
backtest-worker via a Redis queue key.
"""
from __future__ import annotations

import json
import time
from uuid import UUID

from fastapi import APIRouter, HTTPException, Query, Request

from src.validation.metrics import (
    validation_requests_total,
    validation_request_seconds,
)

router = APIRouter(prefix="/validation", tags=["validation"])


# ─── Helpers ─────────────────────────────────────────────────────────────────

def _track(endpoint: str, status: int, t0: float) -> None:
    validation_requests_total.labels(endpoint=endpoint, status_code=str(status)).inc()
    validation_request_seconds.labels(endpoint=endpoint).observe(time.monotonic() - t0)


# ─── Engine Accuracy ─────────────────────────────────────────────────────────

@router.get("/accuracy")
async def get_accuracy_summary(
    request: Request,
    days: int = Query(7, ge=1, le=90),
) -> dict:
    """
    Directional accuracy per engine over the last N days,
    broken down by evaluation window (4h / 24h / 72h).
    """
    t0 = time.monotonic()
    try:
        async with request.app.state.db.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT
                    engine_name,
                    AVG(accuracy_4h)         AS accuracy_4h,
                    AVG(accuracy_24h)        AS accuracy_24h,
                    AVG(accuracy_72h)        AS accuracy_72h,
                    SUM(total_signals)       AS total_signals,
                    AVG(high_conf_accuracy)  AS high_conf_accuracy
                FROM engine_performance_metrics
                WHERE metric_date > (now() - ($1 || ' days')::interval)::date
                GROUP BY engine_name
                ORDER BY AVG(accuracy_24h) DESC NULLS LAST
                """,
                str(days),
            )
        result = [dict(r) for r in rows]
        _track("/accuracy", 200, t0)
        return {
            "period_days": days,
            "engines": result,
            "disclaimer": "Accuracy is directional only — not financial performance.",
        }
    except Exception as exc:
        _track("/accuracy", 500, t0)
        raise HTTPException(status_code=500, detail=str(exc))


@router.get("/accuracy/{engine_name}")
async def get_engine_accuracy_history(
    request: Request,
    engine_name: str,
    days: int = Query(30, ge=1, le=90),
) -> dict:
    """Daily accuracy time-series for a specific engine."""
    t0 = time.monotonic()
    try:
        async with request.app.state.db.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT metric_date, total_signals,
                       accuracy_4h, accuracy_24h, accuracy_72h,
                       avg_confidence, high_conf_accuracy
                FROM engine_performance_metrics
                WHERE engine_name = $1
                  AND metric_date > (now() - ($2 || ' days')::interval)::date
                ORDER BY metric_date DESC
                """,
                engine_name, str(days),
            )
        if not rows:
            raise HTTPException(status_code=404, detail=f"No metrics found for engine '{engine_name}'")
        _track(f"/accuracy/{engine_name}", 200, t0)
        return {
            "engine_name": engine_name,
            "period_days": days,
            "history": [dict(r) for r in rows],
        }
    except HTTPException:
        _track(f"/accuracy/engine", 404, t0)
        raise
    except Exception as exc:
        _track("/accuracy/engine", 500, t0)
        raise HTTPException(status_code=500, detail=str(exc))


# ─── Calibration ─────────────────────────────────────────────────────────────

@router.get("/calibration")
async def get_confidence_calibration(
    request: Request,
    days: int = Query(30, ge=1, le=90),
) -> dict:
    """
    Confidence calibration: does high confidence_level predict higher accuracy?
    Returns accuracy broken down by confidence tier for each evaluation window.
    """
    t0 = time.monotonic()
    try:
        async with request.app.state.db.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT
                    sr.confidence_level,
                    so.evaluation_window_h,
                    COUNT(*) AS total,
                    COUNT(*) FILTER (WHERE so.direction_correct = true)  AS correct,
                    COUNT(*) FILTER (WHERE so.direction_correct = false) AS wrong,
                    ROUND(
                        (COUNT(*) FILTER (WHERE so.direction_correct = true))::numeric /
                        NULLIF(COUNT(*) FILTER (WHERE so.direction_correct IS NOT NULL), 0) * 100,
                        1
                    ) AS accuracy_pct
                FROM signal_outcomes so
                JOIN signals_registry sr ON sr.id = so.signal_id
                WHERE so.evaluated_at > now() - ($1 || ' days')::interval
                  AND sr.confidence_level IS NOT NULL
                GROUP BY sr.confidence_level, so.evaluation_window_h
                ORDER BY sr.confidence_level, so.evaluation_window_h
                """,
                str(days),
            )
        _track("/calibration", 200, t0)
        return {
            "period_days": days,
            "calibration": [dict(r) for r in rows],
            "note": "Well-calibrated: high confidence should yield higher accuracy_pct.",
        }
    except Exception as exc:
        _track("/calibration", 500, t0)
        raise HTTPException(status_code=500, detail=str(exc))


# ─── Signals Registry ────────────────────────────────────────────────────────

@router.get("/signals")
async def list_signals(
    request: Request,
    signal_type: str | None = Query(None),
    engine: str | None = Query(None),
    ticker: str | None = Query(None),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
) -> dict:
    """Query the signals registry with optional filters."""
    t0 = time.monotonic()
    try:
        conditions = ["1=1"]
        params: list = []

        if signal_type:
            params.append(signal_type)
            conditions.append(f"signal_type = ${len(params)}")
        if engine:
            params.append(engine)
            conditions.append(f"engine_name = ${len(params)}")
        if ticker:
            params.append(ticker.upper())
            conditions.append(f"ticker = ${len(params)}")

        params.extend([limit, offset])
        where = " AND ".join(conditions)

        async with request.app.state.db.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT id, signal_type, engine_name, ticker, direction,
                       composite_score, confidence_level, source_table,
                       source_id, emitted_at, archived_at
                FROM signals_registry
                WHERE {where}
                ORDER BY emitted_at DESC
                LIMIT ${len(params) - 1} OFFSET ${len(params)}
                """,
                *params,
            )
            total = await conn.fetchval(
                f"SELECT COUNT(*) FROM signals_registry WHERE {where}",
                *params[:-2],
            )
        _track("/signals", 200, t0)
        return {
            "total": total,
            "limit": limit,
            "offset": offset,
            "signals": [dict(r) for r in rows],
        }
    except Exception as exc:
        _track("/signals", 500, t0)
        raise HTTPException(status_code=500, detail=str(exc))


@router.get("/signals/{ticker}")
async def get_ticker_signals(
    request: Request,
    ticker: str,
    days: int = Query(7, ge=1, le=30),
    limit: int = Query(50, ge=1, le=200),
) -> dict:
    """All signals for a specific ticker, with outcomes where available."""
    t0 = time.monotonic()
    try:
        async with request.app.state.db.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT
                    sr.id, sr.signal_type, sr.engine_name, sr.direction,
                    sr.composite_score, sr.confidence_level, sr.emitted_at,
                    COALESCE(
                        json_agg(
                            json_build_object(
                                'window_h', so.evaluation_window_h,
                                'correct',  so.direction_correct,
                                'delta',    so.outcome_score_delta
                            )
                        ) FILTER (WHERE so.id IS NOT NULL),
                        '[]'
                    ) AS outcomes
                FROM signals_registry sr
                LEFT JOIN signal_outcomes so ON so.signal_id = sr.id
                WHERE sr.ticker = $1
                  AND sr.emitted_at > now() - ($2 || ' days')::interval
                GROUP BY sr.id
                ORDER BY sr.emitted_at DESC
                LIMIT $3
                """,
                ticker.upper(), str(days), limit,
            )
        _track(f"/signals/ticker", 200, t0)
        return {
            "ticker": ticker.upper(),
            "period_days": days,
            "count": len(rows),
            "signals": [dict(r) for r in rows],
        }
    except Exception as exc:
        _track("/signals/ticker", 500, t0)
        raise HTTPException(status_code=500, detail=str(exc))


# ─── Backtest Runs ────────────────────────────────────────────────────────────

@router.get("/backtest")
async def list_backtest_runs(
    request: Request,
    limit: int = Query(20, ge=1, le=100),
) -> dict:
    """List recent backtest runs with summary accuracy."""
    t0 = time.monotonic()
    try:
        async with request.app.state.db.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT run_id, engine_name, window_days, status,
                       started_at, completed_at,
                       total_signals, evaluated_signals,
                       accuracy_4h, accuracy_24h, accuracy_72h
                FROM backtest_runs
                ORDER BY started_at DESC
                LIMIT $1
                """,
                limit,
            )
        _track("/backtest", 200, t0)
        return {"runs": [dict(r) for r in rows]}
    except Exception as exc:
        _track("/backtest", 500, t0)
        raise HTTPException(status_code=500, detail=str(exc))


@router.get("/backtest/{run_id}")
async def get_backtest_run(
    request: Request,
    run_id: UUID,
    limit: int = Query(100, ge=1, le=1000),
) -> dict:
    """Detailed results for a specific backtest run."""
    t0 = time.monotonic()
    try:
        async with request.app.state.db.acquire() as conn:
            meta = await conn.fetchrow(
                "SELECT * FROM backtest_runs WHERE run_id = $1", str(run_id)
            )
            if not meta:
                raise HTTPException(status_code=404, detail="Run not found")

            results = await conn.fetch(
                """
                SELECT ticker, engine_name, predicted_direction, actual_direction,
                       direction_correct, predicted_score, outcome_score_delta,
                       window_hours, evaluated_at
                FROM backtest_results
                WHERE run_id = $1
                ORDER BY evaluated_at DESC
                LIMIT $2
                """,
                str(run_id), limit,
            )
        _track("/backtest/run_id", 200, t0)
        return {
            "run": dict(meta),
            "results": [dict(r) for r in results],
            "disclaimer": "For informational purposes only. Not financial advice.",
        }
    except HTTPException:
        raise
    except Exception as exc:
        _track("/backtest/run_id", 500, t0)
        raise HTTPException(status_code=500, detail=str(exc))


@router.post("/backtest/trigger")
async def trigger_backtest(
    request: Request,
    engine_name: str | None = Query(None),
    window_days: int = Query(30, ge=1, le=365),
) -> dict:
    """
    Enqueue a backtest run. The backtest-worker picks it up from Redis.
    Returns immediately with a run_id to poll for status.
    """
    t0 = time.monotonic()
    try:
        import uuid as _uuid
        run_id = str(_uuid.uuid4())

        # Enqueue via Redis list — backtest-worker pops from queue
        payload = json.dumps({
            "run_id":      run_id,
            "engine_name": engine_name,
            "window_days": window_days,
        })
        await request.app.state.redis.lpush("backtest:queue", payload)

        _track("/backtest/trigger", 202, t0)
        return {
            "run_id": run_id,
            "status": "queued",
            "message": "Backtest enqueued. Poll GET /validation/backtest/{run_id} for results.",
        }
    except Exception as exc:
        _track("/backtest/trigger", 500, t0)
        raise HTTPException(status_code=500, detail=str(exc))
