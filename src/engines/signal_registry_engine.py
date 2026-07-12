"""
Signal Registry Engine — Phase 1: Signal Truth Layer

Programmatic API for the signals_registry append-only table.

Two roles:
  1. Direct registration  — append a signal at the moment of computation,
     complementing SignalArchiver which scrapes engine output tables.
  2. Query API — used by OutcomeTrackingEngine and SignalScoringEngine
     to find pending signals and retrieve metadata.

Immutability contract:
  register() performs INSERT only — never UPDATE, never DELETE.
  All read methods are SELECT-only.
  No existing engine table is ever touched.
"""
from __future__ import annotations

import asyncio
import json
import os
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

import asyncpg

from src.core.logging import configure_logging, get_logger

if TYPE_CHECKING:
    import redis.asyncio as aioredis

logger = get_logger(__name__)

# Canonical evaluation windows (hours) — spec: 1h / 6h / 24h / 72h / 7d
EVAL_WINDOWS_H: tuple[int, ...] = (1, 6, 24, 72, 168)


@dataclass
class RegistryStats:
    total_signals: int = 0
    by_engine: dict[str, int] = field(default_factory=dict)
    by_type: dict[str, int] = field(default_factory=dict)
    by_direction: dict[str, int] = field(default_factory=dict)
    pending_outcomes: dict[int, int] = field(default_factory=dict)  # window_h → count


class SignalRegistryEngine:
    """
    Append-only API for signals_registry.

    The SignalArchiver is a table-scraper that fills signals_registry on a poll
    interval.  SignalRegistryEngine is the direct API for programmatic callers
    and the query layer consumed by the rest of the Signal Truth Layer.
    """

    EVAL_WINDOWS_H: tuple[int, ...] = EVAL_WINDOWS_H

    def __init__(
        self,
        db_pool: asyncpg.Pool,
        redis: "aioredis.Redis | None" = None,
    ) -> None:
        self._pool = db_pool
        self._redis = redis

    # ── Write ─────────────────────────────────────────────────────────────────

    async def register(
        self,
        signal_type: str,
        engine_name: str,
        ticker: str | None,
        direction: str | None,
        composite_score: float | None,
        confidence_level: str | None,
        signal_data: dict,
        source_table: str,
        emitted_at: datetime,
        source_id: int | None = None,
    ) -> int:
        """
        Append a signal to signals_registry.
        Returns the assigned signal_id, or -1 on a conflict (already exists).
        Conflicts are silently ignored — idempotent by design.
        """
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                INSERT INTO signals_registry (
                    signal_type, engine_name, ticker, direction,
                    composite_score, confidence_level, signal_data,
                    source_table, source_id, emitted_at
                ) VALUES ($1,$2,$3,$4,$5,$6,$7::jsonb,$8,$9,$10)
                ON CONFLICT DO NOTHING
                RETURNING id
                """,
                signal_type,
                engine_name,
                ticker,
                direction,
                float(composite_score) if composite_score is not None else None,
                confidence_level,
                json.dumps(signal_data),
                source_table,
                source_id,
                emitted_at,
            )
            if row:
                signal_id: int = row["id"]
                logger.debug(
                    "signal_registered",
                    signal_id=signal_id,
                    engine=engine_name,
                    ticker=ticker,
                    direction=direction,
                )
                return signal_id

            # Conflict — find the existing record
            if source_id is not None:
                existing = await conn.fetchval(
                    """
                    SELECT id FROM signals_registry
                    WHERE source_table = $1 AND source_id = $2
                    LIMIT 1
                    """,
                    source_table,
                    source_id,
                )
                return int(existing) if existing else -1

            return -1

    # ── Reads used by OutcomeTrackingEngine ──────────────────────────────────

    async def get_pending_evaluation(
        self,
        window_hours: int,
        limit: int = 200,
        signal_types: list[str] | None = None,
        include_market_wide: bool = False,
    ) -> list[dict]:
        """
        Signals that have passed the evaluation window but have no outcome
        recorded for that window_hours value.

        Excludes neutral / mixed / NULL directions: direction accuracy cannot
        be computed for non-directional signals.
        """
        cutoff = datetime.now(timezone.utc) - timedelta(hours=window_hours)

        type_clause = ""
        params: list = [window_hours, cutoff, limit]

        if signal_types:
            params.append(signal_types)
            type_clause = f"AND sr.signal_type = ANY(${len(params)})"

        market_clause = (
            ""
            if include_market_wide
            else "AND (sr.ticker IS NOT NULL AND sr.ticker != 'MARKET')"
        )

        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT sr.*
                  FROM signals_registry sr
                  LEFT JOIN signal_outcomes so
                    ON so.signal_id = sr.id AND so.evaluation_window_h = $1
                 WHERE sr.emitted_at < $2
                   AND so.id IS NULL
                   AND sr.direction IS NOT NULL
                   AND sr.direction NOT IN ('neutral', 'mixed')
                   {market_clause}
                   {type_clause}
                 ORDER BY sr.emitted_at ASC
                 LIMIT $3
                """,
                *params,
            )
            return [dict(r) for r in rows]

    async def get_signal(self, signal_id: int) -> dict | None:
        """Fetch a single signal record by primary key."""
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM signals_registry WHERE id = $1",
                signal_id,
            )
            return dict(row) if row else None

    async def get_by_engine(
        self,
        engine_name: str,
        hours: int = 24,
        limit: int = 500,
    ) -> list[dict]:
        """Recent signals from a specific engine, newest first."""
        since = datetime.now(timezone.utc) - timedelta(hours=hours)
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT id, signal_type, ticker, direction, composite_score,
                       confidence_level, emitted_at, archived_at
                  FROM signals_registry
                 WHERE engine_name = $1 AND emitted_at > $2
                 ORDER BY emitted_at DESC
                 LIMIT $3
                """,
                engine_name,
                since,
                limit,
            )
            return [dict(r) for r in rows]

    # ── Monitoring / stats ────────────────────────────────────────────────────

    async def get_registry_stats(self) -> RegistryStats:
        """
        Lightweight aggregation for monitoring dashboards.
        All queries are read-only SELECTs on signals_registry.
        """
        stats = RegistryStats()
        now = datetime.now(timezone.utc)

        async with self._pool.acquire() as conn:
            stats.total_signals = (
                await conn.fetchval("SELECT COUNT(*) FROM signals_registry") or 0
            )

            for row in await conn.fetch(
                "SELECT engine_name, COUNT(*) AS cnt FROM signals_registry GROUP BY engine_name"
            ):
                stats.by_engine[row["engine_name"]] = row["cnt"]

            for row in await conn.fetch(
                "SELECT signal_type, COUNT(*) AS cnt FROM signals_registry GROUP BY signal_type"
            ):
                stats.by_type[row["signal_type"]] = row["cnt"]

            for row in await conn.fetch(
                "SELECT COALESCE(direction, 'null') AS dir, COUNT(*) AS cnt "
                "FROM signals_registry GROUP BY direction"
            ):
                stats.by_direction[row["dir"]] = row["cnt"]

            for wh in EVAL_WINDOWS_H:
                cutoff = now - timedelta(hours=wh)
                count = await conn.fetchval(
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
                stats.pending_outcomes[wh] = count or 0

        return stats


# ── Standalone stats reporter ─────────────────────────────────────────────────

async def _main() -> None:
    configure_logging()
    from src.db.connection import get_db_pool, close_db_pool

    db = await get_db_pool()
    engine = SignalRegistryEngine(db)
    try:
        stats = await engine.get_registry_stats()
        logger.info(
            "registry_stats",
            total=stats.total_signals,
            by_engine=stats.by_engine,
            by_type=stats.by_type,
            pending_outcomes=stats.pending_outcomes,
        )
    finally:
        await close_db_pool()


if __name__ == "__main__":
    asyncio.run(_main())
