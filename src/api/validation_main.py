"""
Validation API — Level 4 Service Entry Point

Separate FastAPI application running on port 8010.
Read-only access to validation tables + Prometheus /metrics endpoint.
"""
from __future__ import annotations

import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST

from src.cache.redis_client import close_redis, get_redis
from src.core.config import get_settings
from src.core.logging import bind_service_context, configure_logging, get_logger
from src.db.connection import close_db_pool, get_db_pool
from src.validation.metrics import (
    signals_in_registry,
    validation_requests_total,
    validation_request_seconds,
)

from .routes.validation import router as validation_router
from .routes.scores import router as scores_router
from .routes.backtest_analytics import router as backtest_router
from .routes.validation_intelligence import router as intelligence_router
from .routes.governance import router as governance_router
from .routes.meta_intelligence import router as meta_router
from .routes.feedback import router as feedback_router

logger = get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    configure_logging()
    bind_service_context("validation-api")
    settings = get_settings()
    logger.info("validation_api_startup", version=settings.APP_VERSION)

    db_pool = await get_db_pool()
    redis   = await get_redis()

    app.state.db    = db_pool
    app.state.redis = redis

    # Initialise gauge from DB
    try:
        async with db_pool.acquire() as conn:
            count = await conn.fetchval("SELECT COUNT(*) FROM signals_registry")
            signals_in_registry.set(count or 0)
    except Exception:
        pass

    logger.info("validation_api_ready")
    yield

    await close_db_pool()
    await close_redis()
    logger.info("validation_api_shutdown")


def create_app() -> FastAPI:
    app = FastAPI(
        title="Market Intelligence — Validation API",
        version="0.9.0",
        description=(
            "Level 4: Signal validation, backtesting, and engine performance analytics. "
            "Read-only. NOT financial advice."
        ),
        lifespan=lifespan,
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["GET", "POST"],
        allow_headers=["*"],
    )

    # ── Prometheus metrics ─────────────────────────────────────────────────────
    @app.get("/metrics", include_in_schema=False)
    async def metrics() -> Response:
        return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)

    # ── Health ─────────────────────────────────────────────────────────────────
    async def _deep_health(req: Request) -> dict:
        try:
            async with req.app.state.db.acquire() as conn:
                await conn.fetchval("SELECT 1")
            db_ok = True
        except Exception:
            db_ok = False

        try:
            await req.app.state.redis.ping()
            redis_ok = True
        except Exception:
            redis_ok = False

        return {
            "status": "ok" if (db_ok and redis_ok) else "degraded",
            "db":     "ok" if db_ok else "error",
            "redis":  "ok" if redis_ok else "error",
            "service": "validation-api",
        }

    @app.get("/health")
    async def health(req: Request) -> dict:
        return await _deep_health(req)

    @app.get("/ready")
    async def ready(req: Request, response: Response) -> dict:
        result = await _deep_health(req)
        if result["status"] != "ok":
            response.status_code = 503
        return result

    # ── Request instrumentation middleware ─────────────────────────────────────
    @app.middleware("http")
    async def instrument(request: Request, call_next):
        t0 = time.monotonic()
        response = await call_next(request)
        path = request.url.path
        validation_requests_total.labels(
            endpoint=path, status_code=str(response.status_code)
        ).inc()
        validation_request_seconds.labels(endpoint=path).observe(
            time.monotonic() - t0
        )
        return response

    app.include_router(validation_router, prefix="/api/v1")
    app.include_router(scores_router, prefix="/api/v1")
    app.include_router(backtest_router, prefix="/api/v1")
    app.include_router(intelligence_router, prefix="/api/v1")
    app.include_router(governance_router, prefix="/api/v1")
    app.include_router(meta_router, prefix="/api/v1")
    app.include_router(feedback_router, prefix="/api/v1")

    return app


app = create_app()
