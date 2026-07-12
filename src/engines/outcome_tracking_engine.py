"""
Outcome Tracking Engine — Phase 1: Signal Truth Layer

Measures actual market outcomes for registered signals at fixed windows.

Windows: 1h / 6h / 24h / 72h / 7d (168h)

Ground truth hierarchy:
  Ticker signals:        REAL price return from ticker_quotes at t+window
  Market-wide signals:   average per-symbol price return across all quoted symbols

Units: outcome_score_delta is a decimal price return (0.005 = +0.5%), matching
the technical_replay bootstrap data. A return within ±RETURN_THRESHOLD (0.2%)
is classified neutral (direction_correct = NULL — not a tradeable move).

History: before 2026-06-11 this engine measured composite_score deltas — i.e.
whether the engines' own opinion later agreed with itself. That circular ground
truth inflated/corrupted every downstream metric (engine exclusions, quality
boosts, Kelly, calibration, governance). Never reintroduce score-based truth.

Contract:
  - NO prediction logic — only measurement of what actually happened
  - Reads signals_registry (read-only) and ticker_quotes
  - Writes to signal_outcomes (INSERT … ON CONFLICT DO NOTHING)
  - Never touches any existing engine table
"""
from __future__ import annotations

import asyncio
import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

import asyncpg

from src.core import http_server as _http
from src.core.logging import configure_logging, get_logger

if TYPE_CHECKING:
    import redis.asyncio as aioredis

logger = get_logger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────────

EVAL_WINDOWS_H: tuple[int, ...] = (1, 6, 24, 72, 168)   # 1h / 6h / 24h / 72h / 7d

_POSITIVE: frozenset[str] = frozenset({"risk_on", "inflow", "liquidity_expansion", "long", "buy"})
_NEGATIVE: frozenset[str] = frozenset({"risk_off", "outflow", "tightening", "volatility_regime", "short", "sell"})

# Price return must exceed ±RETURN_THRESHOLD to be called directional.
# 0.2% matches scripts/replay_signals.py — below that, fees eat the move.
_RETURN_THRESHOLD: float = 0.002


# ── Helpers ───────────────────────────────────────────────────────────────────

def _polarity(direction: str | None) -> int:
    """Predicted direction → +1 / -1 / 0."""
    if direction in _POSITIVE:
        return 1
    if direction in _NEGATIVE:
        return -1
    return 0


def _delta_polarity(delta: float) -> int:
    """Price return → +1 / -1 / 0 (neutral when within ±RETURN_THRESHOLD)."""
    if delta > _RETURN_THRESHOLD:
        return 1
    if delta < -_RETURN_THRESHOLD:
        return -1
    return 0


def _pol_to_direction(pol: int) -> str:
    return "risk_on" if pol > 0 else ("risk_off" if pol < 0 else "neutral")


def _search_bounds(window_h: int) -> tuple[timedelta, timedelta]:
    """
    Returns (before_buffer, after_buffer) for the snapshot search around target.
    Short windows use tighter bounds; longer windows allow generous catch-up.
    """
    if window_h <= 1:
        return timedelta(minutes=30), timedelta(hours=1)
    if window_h <= 6:
        return timedelta(hours=1), timedelta(hours=6)
    return timedelta(hours=2), timedelta(hours=window_h)


# ── Result type ───────────────────────────────────────────────────────────────

@dataclass
class OutcomeCycleResult:
    windows_processed: int = 0
    outcomes_written: int = 0
    signals_skipped: int = 0
    errors: int = 0


# ── Engine ────────────────────────────────────────────────────────────────────

class OutcomeTrackingEngine:
    """
    Evaluates signals in signals_registry against subsequent market data.

    Runs as a standalone service (see __main__) or embedded in a scheduler.
    Writes outcome records to signal_outcomes; reads are from signals_registry.
    """

    def __init__(
        self,
        db_pool: asyncpg.Pool,
        redis: "aioredis.Redis | None" = None,
    ) -> None:
        self._pool = db_pool
        self._redis = redis

    async def run_cycle(self) -> OutcomeCycleResult:
        """Evaluate all pending signals across every evaluation window."""
        result = OutcomeCycleResult()
        now = datetime.now(timezone.utc)

        for window_h in EVAL_WINDOWS_H:
            cutoff = now - timedelta(hours=window_h)
            try:
                written, skipped = await self._evaluate_window(window_h, cutoff)
                result.windows_processed += 1
                result.outcomes_written += written
                result.signals_skipped += skipped
            except Exception as exc:
                logger.error("outcome_window_error", window_h=window_h, error=str(exc))
                result.errors += 1

        logger.info(
            "outcome_cycle_complete",
            windows=result.windows_processed,
            written=result.outcomes_written,
            skipped=result.signals_skipped,
            errors=result.errors,
        )
        return result

    async def _evaluate_window(self, window_h: int, cutoff: datetime) -> tuple[int, int]:
        """
        Process up to 500 unevaluated signals per window per cycle.
        (500 × 5 windows / 5 min ≈ 30k evaluations/hour — sized to drain the
        Sprint 35 re-evaluation backlog in hours, not days; steady-state load
        is ~10k signals/day so the limit is rarely reached.)
        Returns (outcomes_written, signals_skipped_no_ground_truth).
        """
        async with self._pool.acquire() as conn:
            signals = await conn.fetch(
                """
                SELECT sr.*
                  FROM signals_registry sr
                  LEFT JOIN signal_outcomes so
                    ON so.signal_id = sr.id AND so.evaluation_window_h = $1
                 WHERE sr.emitted_at < $2
                   AND so.id IS NULL
                   AND sr.direction IS NOT NULL
                   AND sr.direction NOT IN ('neutral', 'mixed')
                 ORDER BY sr.emitted_at ASC
                 LIMIT 500
                """,
                window_h,
                cutoff,
            )

            if not signals:
                return 0, 0

            written = 0
            skipped = 0

            for sig in signals:
                try:
                    outcome = await self._compute_outcome(conn, sig, window_h)
                    if outcome is None:
                        skipped += 1
                        continue

                    await conn.execute(
                        """
                        INSERT INTO signal_outcomes (
                            signal_id, ticker, evaluation_window_h,
                            predicted_direction, actual_direction,
                            direction_correct, predicted_score, outcome_score_delta
                        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
                        ON CONFLICT (signal_id, evaluation_window_h) DO NOTHING
                        """,
                        sig["id"],
                        sig["ticker"],
                        window_h,
                        sig["direction"],
                        outcome["actual_direction"],
                        outcome["direction_correct"],
                        sig["composite_score"],
                        outcome["score_delta"],
                    )
                    written += 1

                except Exception as exc:
                    logger.error(
                        "outcome_signal_error",
                        signal_id=sig["id"],
                        window_h=window_h,
                        error=str(exc),
                    )

            if written:
                logger.info("outcomes_written", window_h=window_h, count=written)

            return written, skipped

    async def _compute_outcome(
        self,
        conn: asyncpg.Connection,
        signal: asyncpg.Record,
        window_h: int,
    ) -> dict | None:
        """
        Dispatch to ticker-level or market-wide outcome computation.
        Returns None when ground truth data is not yet available.
        """
        ticker = signal["ticker"]
        emitted_at = signal["emitted_at"]
        target = emitted_at + timedelta(hours=window_h)
        before_buf, after_buf = _search_bounds(window_h)

        if ticker and ticker not in ("MARKET", "market"):
            return await self._ticker_outcome(
                conn, signal, ticker, emitted_at, target, before_buf, after_buf
            )
        return await self._market_outcome(conn, signal, emitted_at, target, before_buf, after_buf)

    @staticmethod
    def _unevaluable_or_retry(target: datetime, after_buf: timedelta) -> dict | None:
        """When ground-truth data is missing: retry later if it may still arrive,
        or write a permanent neutral row once the grace period has passed.

        Without the permanent row, the 200 oldest unevaluable signals (delisted
        tickers, gaps in quote history) jam the queue head forever and block
        evaluation of everything newer.
        """
        if datetime.now(timezone.utc) <= target + after_buf:
            return None  # data may still arrive — retry next cycle
        return {
            "actual_direction": "neutral",
            "direction_correct": None,
            "score_delta": None,
        }

    async def _price_at(
        self,
        conn: asyncpg.Connection,
        ticker: str,
        around: datetime,
        before_buf: timedelta,
        after_buf: timedelta,
    ) -> float | None:
        """Closest real price to `around` within [around-before_buf, around+after_buf]."""
        row = await conn.fetchrow(
            """
            SELECT price FROM ticker_quotes
             WHERE symbol = $1 AND price > 0
               AND ts BETWEEN $2 AND $3
             ORDER BY ABS(EXTRACT(EPOCH FROM (ts - $4::timestamptz))) ASC
             LIMIT 1
            """,
            ticker,
            around - before_buf,
            around + after_buf,
            around,
        )
        return float(row["price"]) if row else None

    async def _ticker_outcome(
        self,
        conn: asyncpg.Connection,
        signal: asyncpg.Record,
        ticker: str,
        emitted_at: datetime,
        target: datetime,
        before_buf: timedelta,
        after_buf: timedelta,
    ) -> dict | None:
        """
        Ground truth: real price return from ticker_quotes between the signal's
        emission time and t+window. outcome_score_delta = decimal return.
        """
        # Entry price: quote closest to emission (tight ±30min bound — a signal
        # acted on hours later would be measured against the wrong baseline).
        base_price = await self._price_at(
            conn, ticker, emitted_at, timedelta(minutes=30), timedelta(minutes=30)
        )
        future_price = await self._price_at(
            conn, ticker, target, before_buf, after_buf
        )

        if base_price is None or future_price is None or base_price <= 0:
            return self._unevaluable_or_retry(target, after_buf)

        price_return = (future_price - base_price) / base_price

        predicted_pol = _polarity(signal["direction"])
        actual_pol = _delta_polarity(price_return)
        correct: bool | None = (
            None
            if predicted_pol == 0 or actual_pol == 0
            else predicted_pol == actual_pol
        )

        return {
            "actual_direction": _pol_to_direction(actual_pol),
            "direction_correct": correct,
            "score_delta": round(price_return, 6),
        }

    async def _market_outcome(
        self,
        conn: asyncpg.Connection,
        signal: asyncpg.Record,
        emitted_at: datetime,
        target: datetime,
        before_buf: timedelta,
        after_buf: timedelta,
    ) -> dict | None:
        """
        Ground truth for market-wide signals: average per-symbol price return
        between emission and t+window. Baseline and future prices are paired
        per symbol (JOIN) so the average is a true return, not the difference
        between two different baskets. VIX is excluded (inverse instrument).
        """
        row = await conn.fetchrow(
            """
            WITH base AS (
                SELECT DISTINCT ON (symbol) symbol, price
                  FROM ticker_quotes
                 WHERE price > 0 AND symbol != 'VIX'
                   AND ts BETWEEN $1 AND $2
                 ORDER BY symbol, ts DESC
            ), fut AS (
                SELECT DISTINCT ON (symbol) symbol, price
                  FROM ticker_quotes
                 WHERE price > 0 AND symbol != 'VIX'
                   AND ts BETWEEN $3 AND $4
                 ORDER BY symbol, ts DESC
            )
            SELECT AVG((f.price - b.price) / b.price) AS avg_return,
                   COUNT(*) AS n
              FROM base b
              JOIN fut f USING (symbol)
            """,
            emitted_at - timedelta(minutes=30),
            emitted_at + timedelta(minutes=30),
            target - before_buf,
            target + after_buf,
        )

        if not row or row["avg_return"] is None or int(row["n"] or 0) < 5:
            return self._unevaluable_or_retry(target, after_buf)

        delta = float(row["avg_return"])
        predicted_pol = _polarity(signal["direction"])
        actual_pol = _delta_polarity(delta)
        correct = (
            None
            if predicted_pol == 0 or actual_pol == 0
            else predicted_pol == actual_pol
        )

        return {
            "actual_direction": _pol_to_direction(actual_pol),
            "direction_correct": correct,
            "score_delta": round(delta, 6),
        }

    async def get_pending_counts(self) -> dict[int, int]:
        """Return count of signals awaiting evaluation per window (monitoring use)."""
        now = datetime.now(timezone.utc)
        counts: dict[int, int] = {}

        async with self._pool.acquire() as conn:
            for wh in EVAL_WINDOWS_H:
                cutoff = now - timedelta(hours=wh)
                n = await conn.fetchval(
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
                counts[wh] = n or 0

        return counts


# ── Standalone entrypoint ─────────────────────────────────────────────────────

async def _main() -> None:
    configure_logging()

    from src.db.connection import get_db_pool, close_db_pool
    from src.cache.redis_client import get_redis, close_redis

    http_port     = int(os.getenv("OUTCOME_HTTP_PORT",    "8013"))
    eval_interval = int(os.getenv("OUTCOME_EVAL_INTERVAL", "300"))

    _http.start(http_port, "outcome-tracker")

    db    = await get_db_pool()
    redis = await get_redis()
    _http.set_ready("outcome-tracker")

    engine = OutcomeTrackingEngine(db, redis)
    logger.info("outcome_tracker_started", eval_interval=eval_interval)

    try:
        while True:
            t0 = time.monotonic()
            try:
                result = await engine.run_cycle()
                logger.info(
                    "outcome_tracker_cycle",
                    written=result.outcomes_written,
                    skipped=result.signals_skipped,
                    errors=result.errors,
                    elapsed_s=round(time.monotonic() - t0, 2),
                )
            except Exception as exc:
                logger.error("outcome_tracker_error", error=str(exc))
            await asyncio.sleep(eval_interval)
    finally:
        _http.set_not_ready()
        await close_db_pool()
        await close_redis()


if __name__ == "__main__":
    asyncio.run(_main())
