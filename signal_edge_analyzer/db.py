"""Database connection management for the Signal Edge Analyzer.

Standalone module — does not import from src.core to keep the service
self-contained and deployable independently of the main application.
"""
from __future__ import annotations

import os
import pathlib

import asyncpg

MIGRATION_FILE = pathlib.Path("/migrations/013_signal_edge_analyzer.sql")

_pool: asyncpg.Pool | None = None


def _build_dsn() -> str:
    host     = os.environ["POSTGRES_HOST"]
    port     = os.getenv("POSTGRES_PORT", "5432")
    db       = os.environ["POSTGRES_DB"]
    user     = os.environ["POSTGRES_USER"]
    password = os.environ["POSTGRES_PASSWORD"]
    return f"postgresql://{user}:{password}@{host}:{port}/{db}"


def _build_redis_url() -> str:
    host     = os.environ["REDIS_HOST"]
    port     = os.getenv("REDIS_PORT", "6379")
    password = os.getenv("REDIS_PASSWORD", "")
    if password:
        return f"redis://:{password}@{host}:{port}/0"
    return f"redis://{host}:{port}/0"


async def create_pool(min_size: int = 2, max_size: int = 5) -> asyncpg.Pool:
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(
            dsn=_build_dsn(),
            min_size=min_size,
            max_size=max_size,
            command_timeout=30,
        )
    return _pool


async def get_pool() -> asyncpg.Pool:
    if _pool is None:
        raise RuntimeError("DB pool not initialised — call create_pool() first")
    return _pool


async def close_pool() -> None:
    global _pool
    if _pool:
        await _pool.close()
        _pool = None


def get_redis_url() -> str:
    return _build_redis_url()


async def run_migration(pool: asyncpg.Pool) -> None:
    """Apply migration 013 if not already applied."""
    if not MIGRATION_FILE.exists():
        return
    sql = MIGRATION_FILE.read_text()
    async with pool.acquire() as conn:
        await conn.execute(sql)


async def health_check(pool: asyncpg.Pool) -> bool:
    """Return True if DB is reachable."""
    try:
        await pool.fetchval("SELECT 1")
        return True
    except Exception:
        return False
