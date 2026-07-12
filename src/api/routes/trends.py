from __future__ import annotations

from fastapi import APIRouter, Query, Request

from src.engines.narrative_engine import NarrativeEngine
from src.engines.trend_detection import TrendDetectionEngine

router = APIRouter()


@router.get("/trends/hot")
async def get_hot_trends(
    request: Request,
    exchange: str | None = Query(None, pattern="^(US|BIST)$"),
    limit: int = Query(20, ge=1, le=100),
) -> dict:
    engine = TrendDetectionEngine(request.app.state.db, request.app.state.redis)
    trends = await engine.get_hot_tickers(exchange=exchange, limit=limit)
    return {"exchange": exchange or "ALL", "count": len(trends), "trends": trends}


@router.get("/trends/signals")
async def get_signals(request: Request) -> dict:
    engine = TrendDetectionEngine(request.app.state.db, request.app.state.redis)
    return await engine.get_signals()


@router.get("/trends/narratives")
async def get_narratives(
    request: Request,
    limit: int = Query(6, ge=1, le=10),
) -> dict:
    """
    Returns the top active market narratives by strength score.
    Narratives explain WHY multiple tickers are moving together.
    """
    engine = NarrativeEngine(request.app.state.db, request.app.state.redis)
    narratives = await engine.get_top_narratives(limit=limit)
    return {
        "count": len(narratives),
        "narratives": narratives,
        "disclaimer": "For informational purposes only. Not financial advice.",
    }
