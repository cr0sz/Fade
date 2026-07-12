"""Collector — reads signals + evaluated outcomes from PostgreSQL.

Only performs SELECT queries on immutable/append-only tables.
Never writes to signals_registry or signal_outcomes.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

import asyncpg

logger = logging.getLogger(__name__)

# Evaluation windows to collect (must overlap with OutcomeTrackingEngine)
COLLECTION_WINDOWS: tuple[int, ...] = (1, 6, 24, 72, 168)

_COLLECT_SQL = """
    SELECT
        sr.id                   AS signal_id,
        sr.engine_name,
        sr.signal_type,
        sr.ticker,
        sr.direction,
        sr.composite_score,
        sr.confidence_level,
        sr.emitted_at,
        so.evaluation_window_h  AS window_hours,
        so.outcome_score_delta,
        so.direction_correct,
        so.evaluated_at
    FROM signals_registry sr
    JOIN signal_outcomes so ON so.signal_id = sr.id
    WHERE sr.emitted_at >= $1
      AND so.evaluation_window_h = ANY($2::int[])
      AND so.direction_correct IS NOT NULL
    ORDER BY sr.emitted_at DESC
"""

_WATERMARK_SQL = """
    SELECT MAX(id) FROM signals_registry WHERE emitted_at >= $1
"""


async def collect_outcomes(
    pool: asyncpg.Pool,
    period_days: int = 30,
    windows: tuple[int, ...] = COLLECTION_WINDOWS,
) -> list[dict[str, Any]]:
    """
    Return all evaluated signal outcomes within the rolling period.

    Each row is a flat dict representing one (signal, window) pair.
    Only returns rows where signal_outcomes already has an evaluated record
    (i.e. the evaluation window has elapsed and been processed).
    """
    since = datetime.now(tz=timezone.utc) - timedelta(days=period_days)
    try:
        rows = await pool.fetch(_COLLECT_SQL, since, list(windows))
        result = [dict(r) for r in rows]
        logger.debug("collector: fetched %d outcome rows (period=%dd)", len(result), period_days)
        return result
    except Exception as exc:
        logger.error("collector_failed: %s", exc)
        raise


async def collect_recent_signals(
    pool: asyncpg.Pool,
    since_id: int = 0,
    limit: int = 500,
) -> list[dict[str, Any]]:
    """
    Return signals added since `since_id` (watermark polling).
    Used by the 5-minute outcome cycle to detect new signals.
    """
    sql = """
        SELECT id, engine_name, signal_type, ticker, direction,
               composite_score, confidence_level, emitted_at
        FROM signals_registry
        WHERE id > $1
        ORDER BY id ASC
        LIMIT $2
    """
    try:
        rows = await pool.fetch(sql, since_id, limit)
        return [dict(r) for r in rows]
    except Exception as exc:
        logger.error("collect_recent_signals_failed: %s", exc)
        raise


async def get_max_signal_id(pool: asyncpg.Pool) -> int:
    """Return the current maximum signal ID for watermark initialisation."""
    try:
        val = await pool.fetchval("SELECT COALESCE(MAX(id), 0) FROM signals_registry")
        return int(val)
    except Exception:
        return 0


async def count_pending_outcomes(pool: asyncpg.Pool) -> int:
    """Signals registered but with no outcome records yet."""
    sql = """
        SELECT COUNT(*)
        FROM signals_registry sr
        WHERE NOT EXISTS (
            SELECT 1 FROM signal_outcomes so WHERE so.signal_id = sr.id
        )
        AND sr.emitted_at >= now() - INTERVAL '7 days'
    """
    try:
        return int(await pool.fetchval(sql))
    except Exception:
        return -1
