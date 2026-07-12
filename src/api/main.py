from __future__ import annotations

import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST

from src.cache.redis_client import close_redis, get_redis
from src.core.config import get_settings
from src.core.cost_control import init_cost_controller
from src.core.logging import bind_service_context, configure_logging, get_logger
from src.core.metrics import api_requests_total, api_request_seconds, db_pool_free, db_pool_size
from src.db.connection import close_db_pool, get_db_pool

from .routes import dashboard, edge_intelligence, health, intelligence, market, news, sentiment, trends

logger = get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    configure_logging()
    bind_service_context("api")
    settings = get_settings()
    logger.info("startup", app=settings.APP_NAME, version=settings.APP_VERSION)

    db_pool = await get_db_pool()
    redis = await get_redis()
    await init_cost_controller(redis)

    app.state.db = db_pool
    app.state.redis = redis

    logger.info("services_ready")
    yield

    await close_db_pool()
    await close_redis()
    logger.info("shutdown")


def create_app() -> FastAPI:
    settings = get_settings()
    app = FastAPI(
        title=settings.APP_NAME,
        version=settings.APP_VERSION,
        description="Fade — AI market intelligence & trading pipeline — NOT financial advice",
        lifespan=lifespan,
    )

    import os as _os
    _cors_origins = [
        o.strip() for o in
        _os.getenv("CORS_ALLOWED_ORIGINS", "http://localhost:3001").split(",")
        if o.strip()
    ]
    app.add_middleware(
        CORSMiddleware,
        allow_origins=_cors_origins,
        allow_methods=["GET", "POST"],
        allow_headers=["*"],
    )

    @app.get("/metrics", include_in_schema=False)
    async def metrics() -> Response:
        # Refresh pool gauges before scrape
        try:
            pool = app.state.db
            db_pool_size.set(pool.get_size())
            db_pool_free.set(pool.get_idle_size())
        except Exception:
            pass
        return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)

    @app.middleware("http")
    async def prometheus_middleware(request: Request, call_next):
        # Skip metrics endpoint itself to avoid self-instrumentation noise
        if request.url.path == "/metrics":
            return await call_next(request)
        t0 = time.monotonic()
        response = await call_next(request)
        path = request.url.path
        method = request.method
        status = str(response.status_code)
        api_requests_total.labels(method=method, path=path, status_code=status).inc()
        api_request_seconds.labels(method=method, path=path).observe(time.monotonic() - t0)
        return response

    app.include_router(health.router, tags=["health"])
    app.include_router(market.router, prefix="/api/v1", tags=["market"])
    app.include_router(news.router, prefix="/api/v1", tags=["news"])
    app.include_router(sentiment.router, prefix="/api/v1", tags=["sentiment"])
    app.include_router(trends.router, prefix="/api/v1", tags=["trends"])
    app.include_router(intelligence.router, prefix="/api/v1", tags=["intelligence"])
    app.include_router(dashboard.router, prefix="/api/v1", tags=["dashboard"])
    app.include_router(dashboard.page_router, tags=["dashboard"])
    app.include_router(edge_intelligence.router, prefix="/api/v1", tags=["edge"])

    return app


app = create_app()
