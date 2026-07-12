from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query, Request

from src.core.cost_control import get_cost_controller
from src.engines.ai_analyst import AIAnalystEngine
from src.engines.trend_detection import TrendDetectionEngine
from src.providers.market.finnhub import FinnhubProvider

router = APIRouter()


@router.get("/market/quote/{symbol}")
async def get_quote(symbol: str, request: Request) -> dict:
    provider = FinnhubProvider()
    quotes = await provider.fetch(symbols=[symbol.upper()])
    if not quotes:
        raise HTTPException(404, f"No quote found for {symbol}")
    return quotes[0].model_dump()


@router.get("/market/us")
async def get_us_market(request: Request) -> dict:
    provider = FinnhubProvider()
    from src.core.config import get_settings
    settings = get_settings()
    quotes = await provider.fetch(symbols=settings.DEFAULT_US_WATCHLIST[:10])
    return {"market": "US", "quotes": [q.model_dump() for q in quotes]}


@router.get("/market/bist")
async def get_bist_market(request: Request) -> dict:
    provider = FinnhubProvider()
    from src.core.config import get_settings
    settings = get_settings()
    quotes = await provider.fetch(symbols=settings.DEFAULT_BIST_WATCHLIST[:10])
    return {"market": "BIST", "quotes": [q.model_dump() for q in quotes]}


@router.get("/market/cost")
async def get_cost_status(request: Request) -> dict:
    return await get_cost_controller().get_cost_summary()


@router.post("/analyst/query")
async def analyst_query(request: Request, body: dict) -> dict:
    question = body.get("question", "").strip()
    if not question:
        raise HTTPException(400, "question is required")

    engine = AIAnalystEngine(
        db_pool=request.app.state.db,
        redis=request.app.state.redis,
        cost_controller=get_cost_controller(),
    )
    answer = await engine.answer_market_question(question)
    return {"question": question, "answer": answer}


@router.get("/market/digest")
async def get_market_digest(
    request: Request,
    exchange: str = Query("US", pattern="^(US|BIST|ALL)$"),
) -> dict:
    db = request.app.state.db
    redis = request.app.state.redis
    cost = get_cost_controller()

    trend_engine = TrendDetectionEngine(db)
    ai_engine = AIAnalystEngine(db, redis, cost)

    trends = await trend_engine.get_hot_tickers(
        exchange=None if exchange == "ALL" else exchange, limit=10
    )

    from src.db.repositories import NewsRepository
    news_repo = NewsRepository(db)
    recent_news = await news_repo.get_recent(hours=24, limit=15)
    headlines = [n["title"] for n in recent_news]

    digest = await ai_engine.generate_market_digest(headlines, trends)
    return {"exchange": exchange, "digest": digest, "hot_tickers": trends[:5]}
