"""
Signal Truth Layer — Read-only API routes.

Exposes pre-computed predictive strength scores and half-life data
from the signal_scores table.  No computation at request time.

All endpoints are read-only SELECT queries.
"""
from __future__ import annotations

import time

from fastapi import APIRouter, HTTPException, Query, Request

from src.validation.metrics import validation_requests_total, validation_request_seconds

router = APIRouter(prefix="/scores", tags=["signal-truth"])


def _track(endpoint: str, status: int, t0: float) -> None:
    validation_requests_total.labels(endpoint=endpoint, status_code=str(status)).inc()
    validation_request_seconds.labels(endpoint=endpoint).observe(time.monotonic() - t0)


# ── Predictive Strength Scores ────────────────────────────────────────────────

@router.get("")
async def get_scores(
    request: Request,
    engine: str | None = Query(None, description="Filter by engine name"),
    window_hours: int | None = Query(None, description="Filter by evaluation window"),
) -> dict:
    """
    Latest predictive strength scores per engine / signal_type / window_hours.

    predictive_strength: 0–100
      0   = no better than coin-flip (≤50% accuracy)
      50  = 75% directional accuracy
      100 = perfect directional accuracy

    Requires at least 20 evaluated outcomes per combination.
    """
    t0 = time.monotonic()
    try:
        conditions = ["1=1"]
        params: list = []

        if engine:
            params.append(engine)
            conditions.append(f"engine_name = ${len(params)}")
        if window_hours is not None:
            params.append(window_hours)
            conditions.append(f"window_hours = ${len(params)}")

        where = " AND ".join(conditions)

        async with request.app.state.db.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT DISTINCT ON (engine_name, signal_type, window_hours)
                    engine_name, signal_type, window_hours, period_days,
                    sample_size, accuracy, predictive_strength,
                    half_life_hours, calibration_error, computed_at
                  FROM signal_scores
                 WHERE {where}
                 ORDER BY engine_name, signal_type, window_hours, computed_at DESC
                """,
                *params,
            )

        _track("/scores", 200, t0)
        return {
            "count": len(rows),
            "scores": [dict(r) for r in rows],
            "note": (
                "predictive_strength maps accuracy to 0–100: "
                "50% accuracy → 0, 75% → 50, 100% → 100."
            ),
        }
    except Exception as exc:
        _track("/scores", 500, t0)
        raise HTTPException(status_code=500, detail=str(exc))


@router.get("/half-life")
async def get_half_life(request: Request) -> dict:
    """
    Half-life summary per engine / signal_type.

    half_life_hours: the evaluation window at which directional accuracy
    first drops below 0.55 (5% above coin-flip). null means the signal
    remains predictive beyond the 7-day (168h) window.

    Shorter half-life = signal decays faster. Higher half-life = more durable.
    """
    t0 = time.monotonic()
    try:
        async with request.app.state.db.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT DISTINCT ON (engine_name, signal_type)
                    engine_name, signal_type,
                    half_life_hours, predictive_strength, sample_size, computed_at
                  FROM signal_scores
                 ORDER BY engine_name, signal_type, computed_at DESC
                """
            )

        result = [dict(r) for r in rows]
        result.sort(key=lambda r: (r["half_life_hours"] or 9999.0))

        _track("/scores/half-life", 200, t0)
        return {
            "count": len(result),
            "half_life_threshold": 0.55,
            "engines": result,
            "note": (
                "half_life_hours is null when accuracy stays above 0.55 "
                "at all measured windows (≤168h)."
            ),
        }
    except Exception as exc:
        _track("/scores/half-life", 500, t0)
        raise HTTPException(status_code=500, detail=str(exc))


@router.get("/{engine_name}")
async def get_engine_scores(
    request: Request,
    engine_name: str,
    days: int = Query(30, ge=1, le=90, description="Score history window"),
) -> dict:
    """
    Score history for a specific engine across all windows and dates.
    Returns time-series rows ordered newest-first.
    """
    t0 = time.monotonic()
    try:
        async with request.app.state.db.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT signal_type, window_hours, period_days,
                       sample_size, accuracy, predictive_strength,
                       half_life_hours, calibration_error, computed_at
                  FROM signal_scores
                 WHERE engine_name = $1
                   AND computed_at > now() - ($2 || ' days')::interval
                 ORDER BY computed_at DESC, window_hours ASC
                """,
                engine_name,
                str(days),
            )

        if not rows:
            raise HTTPException(
                status_code=404,
                detail=f"No scores found for engine '{engine_name}'.",
            )

        _track("/scores/engine", 200, t0)
        return {
            "engine_name": engine_name,
            "period_days": days,
            "count": len(rows),
            "history": [dict(r) for r in rows],
        }
    except HTTPException:
        _track("/scores/engine", 404, t0)
        raise
    except Exception as exc:
        _track("/scores/engine", 500, t0)
        raise HTTPException(status_code=500, detail=str(exc))


# ── Registry stats ────────────────────────────────────────────────────────────

@router.get("/registry/stats")
async def get_registry_stats(request: Request) -> dict:
    """
    Aggregate counts from signals_registry:
    total signals, breakdown by engine, type, and direction,
    plus pending outcome counts per evaluation window.
    """
    t0 = time.monotonic()
    try:
        async with request.app.state.db.acquire() as conn:
            total = await conn.fetchval("SELECT COUNT(*) FROM signals_registry") or 0

            by_engine = {
                r["engine_name"]: r["cnt"]
                for r in await conn.fetch(
                    "SELECT engine_name, COUNT(*) AS cnt FROM signals_registry GROUP BY engine_name"
                )
            }
            by_type = {
                r["signal_type"]: r["cnt"]
                for r in await conn.fetch(
                    "SELECT signal_type, COUNT(*) AS cnt FROM signals_registry GROUP BY signal_type"
                )
            }
            by_direction = {
                (r["direction"] or "null"): r["cnt"]
                for r in await conn.fetch(
                    "SELECT COALESCE(direction,'null') AS direction, COUNT(*) AS cnt "
                    "FROM signals_registry GROUP BY direction"
                )
            }

            pending: dict[str, int] = {}
            for wh in (1, 6, 24, 72, 168):
                from datetime import timedelta, timezone
                from datetime import datetime as _dt
                cutoff = _dt.now(timezone.utc) - timedelta(hours=wh)
                n = await conn.fetchval(
                    """
                    SELECT COUNT(*)
                      FROM signals_registry sr
                      LEFT JOIN signal_outcomes so
                        ON so.signal_id = sr.id AND so.evaluation_window_h = $1
                     WHERE sr.emitted_at < $2
                       AND so.id IS NULL
                       AND sr.direction IS NOT NULL
                       AND sr.direction NOT IN ('neutral', 'mixed')
                    """,
                    wh,
                    cutoff,
                )
                pending[f"{wh}h"] = n or 0

        _track("/scores/registry/stats", 200, t0)
        return {
            "total_signals": total,
            "by_engine": by_engine,
            "by_type": by_type,
            "by_direction": by_direction,
            "pending_outcomes": pending,
        }
    except Exception as exc:
        _track("/scores/registry/stats", 500, t0)
        raise HTTPException(status_code=500, detail=str(exc))
