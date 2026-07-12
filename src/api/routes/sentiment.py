from __future__ import annotations

from fastapi import APIRouter, Query, Request

from src.engines.sentiment_engine import SentimentEngine

router = APIRouter()


@router.get("/sentiment/{ticker}")
async def get_sentiment(
    ticker: str,
    request: Request,
    hours: int = Query(24, ge=1, le=168),
) -> dict:
    engine = SentimentEngine(request.app.state.db)
    summary = await engine.get_sentiment_summary(ticker.upper(), hours=hours)
    return summary.model_dump()


@router.get("/sentiment/hot/signals")
async def get_hot_signals(request: Request) -> dict:
    from src.engines.trend_detection import TrendDetectionEngine
    engine = TrendDetectionEngine(request.app.state.db)
    return await engine.get_signals()
