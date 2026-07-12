"""
Signal Archiver Service — Level 4

Polls all L2/L3 engine output tables on a configurable interval,
archives new records to signals_registry, and updates watermarks.

Does NOT modify any existing engine tables.
Exposes /health and Prometheus /metrics via embedded HTTP server.
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from datetime import datetime, timezone
from typing import Optional

import asyncpg

from src.core import http_server as _http
from src.core.logging import configure_logging, get_logger, bind_service_context
from src.core.observability import timed_job
from src.db.connection import get_db_pool, close_db_pool
from src.cache.redis_client import get_redis, close_redis
from src.validation.metrics import (
    signals_archived_total,
    archiver_cycle_seconds,
    archiver_lag_seconds,
    archiver_errors_total,
    archiver_watermark_id,
    archiver_jobs_total,
    archiver_jobs_failed_total,
)

logger = get_logger(__name__)

# ─── Table Configurations ─────────────────────────────────────────────────────

_ID_BASED_TABLES: dict[str, dict] = {
    "institutional_flows": {
        "engine_name":   "InstitutionalFlowEngine",
        "signal_type":   "flow",
        "direction_col": "direction",
        "score_col":     "flow_strength",
        "ticker_col":    "ticker",
        "conf_col":      None,
    },
    "etf_rotations": {
        "engine_name":   "ETFRotationEngine",
        "signal_type":   "etf_rotation",
        "direction_col": "rotation_direction",
        "score_col":     "composite_score",
        "ticker_col":    "etf_ticker",
        "conf_col":      None,
    },
    "macro_regimes": {
        "engine_name":   "MacroRegimeEngine",
        "signal_type":   "regime",
        "direction_col": "regime",
        "score_col":     "confidence",
        "ticker_col":    None,
        "conf_col":      None,
    },
    "confluence_signals": {
        "engine_name":   "InstitutionalConfluenceEngine",
        "signal_type":   "confluence",
        "direction_col": "direction",
        "score_col":     "strength",
        "ticker_col":    "ticker",
        "conf_col":      None,
    },
}

_TIME_BASED_TABLES: dict[str, dict] = {
    "market_trends": {
        "engine_name":   "TrendDetectionEngine",
        "signal_type":   "trend",
        "time_col":      "computed_at",
        "ticker_col":    "ticker",
        "conf_col":      "confidence_level",
    },
    "narrative_clusters": {
        "engine_name":   "NarrativeEngine",
        "signal_type":   "narrative",
        "time_col":      "updated_at",
        "ticker_col":    None,
        "conf_col":      None,
    },
}

BATCH_SIZE = 500


def _derive_trend_direction(row: dict) -> str:
    if row.get("trending_up"):
        return "risk_on"
    if row.get("trending_down"):
        return "risk_off"
    if row.get("hype_signal"):
        return "risk_on"
    if row.get("panic_signal"):
        return "risk_off"
    return "neutral"


def _derive_narrative_direction(row: dict) -> str:
    momentum = row.get("momentum", "neutral")
    if momentum == "accelerating":
        return "risk_on"
    if momentum == "decelerating":
        return "risk_off"
    return "neutral"


def _row_to_json(row: asyncpg.Record) -> dict:
    """Convert asyncpg Record to JSON-serialisable dict."""
    result = {}
    for key, value in dict(row).items():
        if isinstance(value, datetime):
            result[key] = value.isoformat()
        elif isinstance(value, (list, dict)):
            result[key] = value
        else:
            result[key] = value
    return result


# ─── Datetime helpers ─────────────────────────────────────────────────────────

def _to_utc(dt: Optional[datetime]) -> datetime:
    """Return a UTC-aware datetime. Treats naive datetimes as UTC (no conversion)."""
    if dt is None:
        return datetime.now(timezone.utc)
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _strip_tz(dt: datetime) -> datetime:
    """Strip timezone info without converting — safe only when value is already UTC."""
    return dt.replace(tzinfo=None)


# ─── Column introspection ─────────────────────────────────────────────────────

# Cache: table → (resolved_column_name, column_is_tz_aware)
# Populated on first use; schemas don't change during a service lifetime.
_col_cache: dict[str, tuple[str, bool]] = {}


async def _resolve_time_col(
    conn: asyncpg.Connection,
    table: str,
    preferred: str,
    fallback: str = "computed_at",
) -> tuple[str, bool]:
    """
    Return (column_name, is_tz_aware) for the best available time column in *table*.

    Checks *preferred* first; if absent, falls back to *fallback*.
    Results are cached so information_schema is queried at most once per table
    per process lifetime.
    """
    cache_key = f"{table}.{preferred}"
    if cache_key in _col_cache:
        return _col_cache[cache_key]

    for col in (preferred, fallback):
        row = await conn.fetchrow(
            """
            SELECT data_type
              FROM information_schema.columns
             WHERE table_schema = 'public'
               AND table_name   = $1
               AND column_name  = $2
            """,
            table, col,
        )
        if row:
            is_tz = "with time zone" in (row["data_type"] or "")
            if col != preferred:
                logger.warning(
                    "time_col_fallback",
                    table=table,
                    preferred=preferred,
                    using=col,
                )
            _col_cache[cache_key] = (col, is_tz)
            return col, is_tz

    raise RuntimeError(
        f"No usable time column ({preferred!r} or {fallback!r}) found in table {table!r}"
    )


class SignalArchiver:
    def __init__(self, db: asyncpg.Pool, redis, poll_interval: int = 30) -> None:
        self._db = db
        self._redis = redis
        self._poll_interval = poll_interval
        self._running = False

    async def run(self) -> None:
        self._running = True
        logger.info("signal_archiver_started", poll_interval=self._poll_interval)
        while self._running:
            cycle_start = time.monotonic()
            rows_archived = 0
            errors = 0
            try:
                rows_archived, errors = await self._poll_cycle()
                archiver_lag_seconds.set(0.0)
            except Exception as exc:
                logger.error("archiver_cycle_error", error=str(exc))
                errors += 1
            finally:
                elapsed = time.monotonic() - cycle_start
                archiver_cycle_seconds.observe(elapsed)
                archiver_lag_seconds.set(elapsed)

            if rows_archived == 0 and errors == 0:
                logger.info("idle_tick", status="alive", cycle_s=round(elapsed, 2))
            else:
                logger.info(
                    "heartbeat",
                    status="alive",
                    rows_archived=rows_archived,
                    errors=errors,
                    cycle_s=round(elapsed, 2),
                )

            await asyncio.sleep(self._poll_interval)

    async def _poll_cycle(self) -> tuple[int, int]:
        """Return (total_rows_archived, total_errors) across all tables."""
        id_rows, id_errors = await self._archive_id_based_tables()
        time_rows, time_errors = await self._archive_time_based_tables()
        return id_rows + time_rows, id_errors + time_errors

    # ── ID-based tables (append-only) ────────────────────────────────────────

    async def _archive_id_based_tables(self) -> tuple[int, int]:
        total_rows, total_errors = 0, 0
        for table, cfg in _ID_BASED_TABLES.items():
            try:
                with timed_job(logger, table=table, job_type="id") as r:
                    count = await self._archive_id_table(table, cfg)
                    r["count"] = count
                    total_rows += count
                archiver_jobs_total.labels(source_table=table).inc()
            except Exception:
                # job_error already emitted by timed_job — only update counters here
                total_errors += 1
                archiver_errors_total.labels(source_table=table).inc()
                archiver_jobs_failed_total.labels(source_table=table).inc()
        return total_rows, total_errors

    async def _archive_id_table(self, table: str, cfg: dict) -> int:
        async with self._db.acquire() as conn:
            wm = await conn.fetchrow(
                "SELECT last_archived_id FROM archiver_watermarks WHERE source_table = $1",
                table,
            )
            last_id = wm["last_archived_id"] if wm else 0

            rows = await conn.fetch(
                f"SELECT * FROM {table} WHERE id > $1 ORDER BY id ASC LIMIT $2",
                last_id,
                BATCH_SIZE,
            )
            if not rows:
                return 0

            archived = 0
            for row in rows:
                d = dict(row)
                ticker = d.get(cfg["ticker_col"]) if cfg["ticker_col"] else None
                direction = d.get(cfg["direction_col"]) if cfg["direction_col"] else None
                score = d.get(cfg["score_col"]) if cfg["score_col"] else None

                # Normalise direction values
                if cfg["signal_type"] == "etf_rotation" and direction == "inflow":
                    direction = "risk_on"
                elif cfg["signal_type"] == "etf_rotation" and direction == "outflow":
                    direction = "risk_off"
                elif cfg["signal_type"] == "regime":
                    pass  # regime IS the direction
                elif cfg["signal_type"] == "flow" and direction == "inflow":
                    direction = "risk_on"
                elif cfg["signal_type"] == "flow" and direction == "outflow":
                    direction = "risk_off"

                await conn.execute(
                    """
                    INSERT INTO signals_registry (
                        signal_type, engine_name, ticker, direction,
                        composite_score, confidence_level, signal_data,
                        source_table, source_id, emitted_at
                    ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
                    ON CONFLICT DO NOTHING
                    """,
                    cfg["signal_type"],
                    cfg["engine_name"],
                    ticker,
                    direction,
                    float(score) if score is not None else None,
                    None,
                    json.dumps(_row_to_json(row)),
                    table,
                    d["id"],
                    d.get("computed_at", datetime.now(timezone.utc)),
                )
                archived += 1

            if rows:
                new_watermark = dict(rows[-1])["id"]
                await conn.execute(
                    """
                    UPDATE archiver_watermarks
                       SET last_archived_id = $1,
                           rows_archived = rows_archived + $2,
                           updated_at = now()
                     WHERE source_table = $3
                    """,
                    new_watermark, archived, table,
                )
                archiver_watermark_id.labels(source_table=table).set(new_watermark)
                signals_archived_total.labels(source_table=table).inc(archived)
        return archived

    # ── Time-based tables (upsert) ────────────────────────────────────────────

    async def _archive_time_based_tables(self) -> tuple[int, int]:
        total_rows, total_errors = 0, 0
        for table, cfg in _TIME_BASED_TABLES.items():
            try:
                with timed_job(logger, table=table, job_type="time") as r:
                    count = await self._archive_time_table(table, cfg)
                    r["count"] = count
                    total_rows += count
                archiver_jobs_total.labels(source_table=table).inc()
            except Exception:
                # job_error already emitted by timed_job — only update counters here
                total_errors += 1
                archiver_errors_total.labels(source_table=table).inc()
                archiver_jobs_failed_total.labels(source_table=table).inc()
        return total_rows, total_errors

    async def _archive_time_table(self, table: str, cfg: dict) -> int:
        async with self._db.acquire() as conn:
            wm = await conn.fetchrow(
                "SELECT last_archived_at FROM archiver_watermarks WHERE source_table = $1",
                table,
            )
            raw_wm = wm["last_archived_at"] if wm and wm["last_archived_at"] else None
            # Always work in UTC-aware internally.
            last_at_utc = _to_utc(raw_wm) if raw_wm else datetime(2000, 1, 1, tzinfo=timezone.utc)

            # Resolve actual time column: fall back to computed_at if preferred is absent.
            time_col, col_is_tz = await _resolve_time_col(conn, table, cfg["time_col"])

            # asyncpg cannot encode a tz-aware datetime against a `timestamp without
            # time zone` column (it internally subtracts a naive epoch, causing a
            # TypeError). Strip the timezone when the target column is naive; the
            # value is already UTC so no information is lost.
            query_ts = last_at_utc if col_is_tz else _strip_tz(last_at_utc)

            rows = await conn.fetch(
                f"SELECT * FROM {table} WHERE {time_col} > $1 ORDER BY {time_col} ASC LIMIT $2",
                query_ts,
                BATCH_SIZE,
            )
            if not rows:
                return 0

            archived = 0
            for row in rows:
                d = dict(row)
                ticker = d.get(cfg["ticker_col"]) if cfg["ticker_col"] else None
                conf = d.get(cfg["conf_col"]) if cfg.get("conf_col") else None

                if table == "market_trends":
                    direction = _derive_trend_direction(d)
                    score = d.get("composite_score")
                elif table == "narrative_clusters":
                    direction = _derive_narrative_direction(d)
                    score = d.get("strength_score")
                else:
                    direction = None
                    score = None

                # Normalise to UTC-aware so all downstream comparisons and
                # archiver_watermarks (TIMESTAMPTZ) writes are type-safe.
                raw_ts = d.get(time_col)
                emitted_at = _to_utc(raw_ts) if raw_ts is not None else datetime.now(timezone.utc)

                # Only archive if not already archived for this ticker+emitted_at combo
                existing = await conn.fetchval(
                    """
                    SELECT id FROM signals_registry
                    WHERE source_table = $1 AND ticker IS NOT DISTINCT FROM $2
                      AND emitted_at = $3
                    LIMIT 1
                    """,
                    table, ticker, emitted_at,
                )
                if existing:
                    archived += 1  # count as processed even if skipped
                    continue

                await conn.execute(
                    """
                    INSERT INTO signals_registry (
                        signal_type, engine_name, ticker, direction,
                        composite_score, confidence_level, signal_data,
                        source_table, source_id, emitted_at
                    ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
                    ON CONFLICT DO NOTHING
                    """,
                    cfg["signal_type"],
                    cfg["engine_name"],
                    ticker,
                    direction,
                    float(score) if score is not None else None,
                    conf,
                    json.dumps(_row_to_json(row)),
                    table,
                    None,
                    emitted_at,
                )
                archived += 1

            if rows:
                raw_new = dict(rows[-1])[time_col]
                new_at = _to_utc(raw_new) if raw_new is not None else datetime.now(timezone.utc)
                await conn.execute(
                    """
                    UPDATE archiver_watermarks
                       SET last_archived_at = $1,
                           rows_archived = rows_archived + $2,
                           updated_at = now()
                     WHERE source_table = $3
                    """,
                    new_at, archived, table,
                )
                signals_archived_total.labels(source_table=table).inc(archived)
        return archived

    async def stop(self) -> None:
        self._running = False


# ─── Entrypoint ──────────────────────────────────────────────────────────────

async def _main() -> None:
    import os
    configure_logging()
    bind_service_context("signal-archiver")
    logger.info("signal_archiver_init")

    port = int(os.getenv("ARCHIVER_HTTP_PORT", "8012"))
    _http.start(port, "signal-archiver")

    poll_interval = int(os.getenv("ARCHIVER_POLL_INTERVAL", "30"))
    batch_size    = int(os.getenv("ARCHIVER_BATCH_SIZE", "500"))
    global BATCH_SIZE
    BATCH_SIZE = batch_size

    db    = await get_db_pool()
    redis = await get_redis()
    _http.set_ready("signal-archiver")

    archiver = SignalArchiver(db, redis, poll_interval=poll_interval)
    try:
        await archiver.run()
    finally:
        _http.set_not_ready()
        await close_db_pool()
        await close_redis()


if __name__ == "__main__":
    asyncio.run(_main())
