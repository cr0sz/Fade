from __future__ import annotations

import asyncpg

from src.core.config import get_settings
from src.core.logging import get_logger

logger = get_logger(__name__)
_pool: asyncpg.Pool | None = None


async def get_db_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        settings = get_settings()
        _pool = await asyncpg.create_pool(
            dsn=settings.database_url,
            min_size=settings.DB_POOL_MIN,
            max_size=settings.DB_POOL_MAX,
            command_timeout=30,
        )
        logger.info("db_pool_created", min=settings.DB_POOL_MIN, max=settings.DB_POOL_MAX)
    return _pool


async def close_db_pool() -> None:
    global _pool
    if _pool:
        await _pool.close()
        _pool = None
        logger.info("db_pool_closed")
