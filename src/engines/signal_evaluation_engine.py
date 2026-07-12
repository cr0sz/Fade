"""
Signal Evaluation Engine — Feedback Loop Layer

Reads signal_trace entries that have elapsed their evaluation window and
writes a signal_feedback outcome row for each one.

Ground-truth sources:
  trend     → signals_registry composite_score delta at T + window_h
  macro     → macro_regimes.regime at T + window_h (persistence = win)
  confluence→ confluence_signals.direction at T + window_h
  scoring   → always neutral (meta-signal, not directional)

Evaluation windows: 1h / 4h / 24h

Idempotency: UNIQUE (signal_trace_id, evaluation_window_h) — safe to re-run.
Never modifies signal_trace, signals_registry, or any ingestion table.
"""
from __future__ import annotations

import asyncio
import json
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

EVAL_WINDOWS_H: tuple[int, ...] = (1, 6, 24, 72, 168)
BATCH_SIZE = 200
_DELTA_THRESHOLD = 0.02

# Regime-to-polarity: does signal_direction predict positive market movement?
_REGIME_POSITIVE = frozenset({"risk_on", "liquidity_expansion", "bullish"})
_REGIME_NEGATIVE = frozenset({"risk_off", "tightening", "volatility_regime", "bearish"})


# ── Result ────────────────────────────────────────────────────────────────────

@dataclass
class EvalCycleResult:
    windows_processed: int = 0
    outcomes_written: int = 0
    outcomes_skipped: int = 0
    errors: int = 0


# ── Engine ────────────────────────────────────────────────────────────────────

class SignalEvaluationEngine:
    """
    Evaluates signal_trace entries against subsequent market data and writes
    signal_feedback rows.  Safe to run concurrently with ingestion pipelines.
    """

    def __init__(
        self,
        db_pool: asyncpg.Pool,
        redis: "aioredis.Redis | None" = None,
    ) -> None:
        self._pool = db_pool
        self._redis = redis

    async def run_cycle(self, cycle_id: str | None = None) -> EvalCycleResult:
        result = EvalCycleResult()
        now = datetime.now(timezone.utc)

        for window_h in EVAL_WINDOWS_H:
            cutoff = now - timedelta(hours=window_h)
            try:
                written, skipped = await self._evaluate_window(window_h, cutoff)
                result.windows_processed += 1
                result.outcomes_written += written
                result.outcomes_skipped += skipped
            except Exception as exc:
                logger.error("eval_window_error", window_h=window_h, error=str(exc))
                result.errors += 1

        logger.info(
            "eval_cycle_complete",
            windows=result.windows_processed,
            written=result.outcomes_written,
            skipped=result.outcomes_skipped,
            errors=result.errors,
        )
        return result

    # ── Per-window driver ─────────────────────────────────────────────────────

    async def _evaluate_window(self, window_h: int, cutoff: datetime) -> tuple[int, int]:
        """Find and evaluate all pending traces for this window. Returns (written, skipped)."""
        async with self._pool.acquire() as conn:
            traces = await conn.fetch(
                """
                SELECT st.id, st.cycle_id, st.engine_name, st.provider,
                       st.signal_type, st.signal_direction, st.signal_strength,
                       st.created_at, st.signal_id, st.ticker
                  FROM signal_trace st
                  LEFT JOIN signal_feedback sf
                    ON sf.signal_trace_id = st.id
                   AND sf.evaluation_window_h = $1
                 WHERE st.created_at < $2
                   AND sf.id IS NULL
                   AND st.signal_type != 'scoring'
                 ORDER BY st.created_at ASC
                 LIMIT $3
                """,
                window_h,
                cutoff,
                BATCH_SIZE,
            )

        if not traces:
            return 0, 0

        written = 0
        skipped = 0

        for trace in traces:
            try:
                outcome = await self._compute_outcome(trace, window_h)
                if outcome is None:
                    skipped += 1
                    continue
                await self._persist_feedback(trace, window_h, outcome)
                written += 1
            except Exception as exc:
                logger.error(
                    "eval_trace_error",
                    trace_id=str(trace["id"]),
                    window_h=window_h,
                    error=str(exc),
                )

        if written:
            logger.info("feedback_written", window_h=window_h, count=written)
        return written, skipped

    # ── Outcome dispatch ─────────────────────────────────────────────────────

    async def _compute_outcome(self, trace: asyncpg.Record, window_h: int) -> dict | None:
        sig_type = trace["signal_type"]
        if sig_type == "trend":
            return await self._evaluate_trend(trace, window_h)
        if sig_type == "macro":
            return await self._evaluate_macro(trace, window_h)
        if sig_type == "confluence":
            return await self._evaluate_confluence(trace, window_h)
        return {"outcome_type": "neutral", "direction_correct": None, "pnl_score": 0.0}

    # ── Trend evaluation ──────────────────────────────────────────────────────

    async def _evaluate_trend(self, trace: asyncpg.Record, window_h: int) -> dict | None:
        """
        Compare signal_direction against composite_score delta in signals_registry.
        signal_direction: "bullish" → expects positive delta, "bearish" → negative.
        signal_trace.signal_id is always NULL (engines write trace before signals_registry
        assigns an ID), so look up by ticker + time proximity instead.
        """
        ticker = trace["ticker"]
        if not ticker:
            return None

        emitted_at = trace["created_at"]
        reg_start = emitted_at - timedelta(minutes=30)
        reg_end = emitted_at + timedelta(minutes=30)

        async with self._pool.acquire() as conn:
            reg_row = await conn.fetchrow(
                """
                SELECT composite_score
                  FROM signals_registry
                 WHERE ticker = $1
                   AND signal_type = 'trend'
                   AND emitted_at BETWEEN $2 AND $3
                 ORDER BY ABS(EXTRACT(EPOCH FROM (emitted_at - $4)))
                 LIMIT 1
                """,
                ticker,
                reg_start,
                reg_end,
                emitted_at,
            )

        if not reg_row:
            return None

        recorded_score = reg_row["composite_score"]
        if recorded_score is None:
            return None

        target = emitted_at + timedelta(hours=window_h)
        buf = _search_buffer(window_h)

        async with self._pool.acquire() as conn:
            future_snap = await conn.fetchrow(
                """
                SELECT composite_score
                  FROM signals_registry
                 WHERE ticker = $1
                   AND signal_type = 'trend'
                   AND emitted_at BETWEEN $2 AND $3
                 ORDER BY ABS(EXTRACT(EPOCH FROM (emitted_at - $4)))
                 LIMIT 1
                """,
                ticker,
                target - buf,
                target + buf,
                target,
            )

        if not future_snap:
            return None

        delta = float(future_snap["composite_score"] or 0) - float(recorded_score)
        signal_dir = trace["signal_direction"] or ""
        predicted_positive = signal_dir in _REGIME_POSITIVE
        actual_positive = delta > _DELTA_THRESHOLD
        actual_negative = delta < -_DELTA_THRESHOLD

        if not predicted_positive and signal_dir not in _REGIME_NEGATIVE:
            direction_correct = None
        elif actual_positive and predicted_positive:
            direction_correct = True
        elif actual_negative and signal_dir in _REGIME_NEGATIVE:
            direction_correct = True
        elif actual_positive or actual_negative:
            direction_correct = False
        else:
            direction_correct = None

        return _build_outcome(
            direction_correct=direction_correct,
            signal_strength=trace["signal_strength"],
            market_context={"ticker": ticker, "delta": round(delta, 6)},
        )

    # ── Macro evaluation ──────────────────────────────────────────────────────

    async def _evaluate_macro(self, trace: asyncpg.Record, window_h: int) -> dict | None:
        """
        A macro signal is a win if the same regime persists at T + window_h.
        """
        predicted_regime = trace["signal_direction"]
        if not predicted_regime:
            return None

        emitted_at = trace["created_at"]
        target = emitted_at + timedelta(hours=window_h)
        buf = _search_buffer(window_h)

        async with self._pool.acquire() as conn:
            future_regime = await conn.fetchrow(
                """
                SELECT regime, confidence
                  FROM macro_regimes
                 WHERE computed_at BETWEEN $1 AND $2
                 ORDER BY ABS(EXTRACT(EPOCH FROM (computed_at - $3)))
                 LIMIT 1
                """,
                target - buf,
                target + buf,
                target,
            )

        if not future_regime:
            return None

        direction_correct: bool | None = future_regime["regime"] == predicted_regime
        return _build_outcome(
            direction_correct=direction_correct,
            signal_strength=trace["signal_strength"],
            market_context={
                "predicted_regime": predicted_regime,
                "actual_regime": future_regime["regime"],
                "actual_confidence": float(future_regime["confidence"] or 0),
            },
        )

    # ── Confluence evaluation ─────────────────────────────────────────────────

    async def _evaluate_confluence(self, trace: asyncpg.Record, window_h: int) -> dict | None:
        """
        A confluence signal is a win if the market-wide confluence direction
        matches at T + window_h.
        """
        predicted_dir = trace["signal_direction"]
        if not predicted_dir or predicted_dir == "mixed":
            return None

        emitted_at = trace["created_at"]
        target = emitted_at + timedelta(hours=window_h)
        buf = _search_buffer(window_h)

        async with self._pool.acquire() as conn:
            future_conf = await conn.fetchrow(
                """
                SELECT direction, strength
                  FROM confluence_signals
                 WHERE ticker IS NULL
                   AND computed_at BETWEEN $1 AND $2
                 ORDER BY ABS(EXTRACT(EPOCH FROM (computed_at - $3)))
                 LIMIT 1
                """,
                target - buf,
                target + buf,
                target,
            )

        if not future_conf:
            return None

        direction_correct: bool | None = future_conf["direction"] == predicted_dir
        return _build_outcome(
            direction_correct=direction_correct,
            signal_strength=trace["signal_strength"],
            market_context={
                "predicted_dir": predicted_dir,
                "actual_dir": future_conf["direction"],
                "actual_strength": float(future_conf["strength"] or 0),
            },
        )

    # ── Persist ───────────────────────────────────────────────────────────────

    async def _persist_feedback(
        self,
        trace: asyncpg.Record,
        window_h: int,
        outcome: dict,
    ) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO signal_feedback (
                    signal_trace_id, cycle_id, engine_name, provider,
                    signal_type, outcome_type, pnl_score, direction_correct,
                    confidence_score, evaluation_window_h, market_context, evaluated_at
                ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
                ON CONFLICT (signal_trace_id, evaluation_window_h) DO NOTHING
                """,
                trace["id"],
                trace["cycle_id"],
                trace["engine_name"],
                trace["provider"],
                trace["signal_type"],
                outcome["outcome_type"],
                outcome["pnl_score"],
                outcome["direction_correct"],
                trace["signal_strength"],
                window_h,
                json.dumps(outcome.get("market_context", {})),
                datetime.now(timezone.utc),
            )


# ── Helpers ───────────────────────────────────────────────────────────────────

def _search_buffer(window_h: int) -> timedelta:
    if window_h <= 1:
        return timedelta(minutes=45)
    if window_h <= 6:
        return timedelta(hours=2)
    if window_h <= 24:
        return timedelta(hours=6)
    if window_h <= 72:
        return timedelta(hours=12)
    return timedelta(hours=24)  # 7d window


def _build_outcome(
    *,
    direction_correct: bool | None,
    signal_strength: float | None,
    market_context: dict,
) -> dict:
    strength = float(signal_strength or 0) / 100.0

    if direction_correct is True:
        outcome_type = "win"
        pnl_score = round(strength, 4)
    elif direction_correct is False:
        outcome_type = "loss"
        pnl_score = round(-strength * 0.5, 4)
    else:
        outcome_type = "neutral"
        pnl_score = 0.0

    return {
        "outcome_type": outcome_type,
        "direction_correct": direction_correct,
        "pnl_score": pnl_score,
        "market_context": market_context,
    }


# ── Standalone entrypoint ─────────────────────────────────────────────────────

async def _main() -> None:
    configure_logging()

    from src.db.connection import get_db_pool, close_db_pool
    from src.cache.redis_client import get_redis, close_redis
    import uuid as _uuid

    http_port     = int(os.getenv("EVAL_HTTP_PORT",    "8019"))
    eval_interval = int(os.getenv("EVAL_INTERVAL",    "900"))

    _http.start(http_port, "signal-evaluator")

    db    = await get_db_pool()
    redis = await get_redis()
    _http.set_ready("signal-evaluator")

    engine = SignalEvaluationEngine(db, redis)
    logger.info("signal_evaluator_started", interval_s=eval_interval)

    try:
        while True:
            t0 = time.monotonic()
            cycle_id = str(_uuid.uuid4())
            try:
                result = await engine.run_cycle(cycle_id=cycle_id)
                logger.info(
                    "signal_evaluator_cycle",
                    cycle_id=cycle_id,
                    written=result.outcomes_written,
                    skipped=result.outcomes_skipped,
                    errors=result.errors,
                    elapsed_s=round(time.monotonic() - t0, 2),
                )
            except Exception as exc:
                logger.error("signal_evaluator_error", cycle_id=cycle_id, error=str(exc))
            await asyncio.sleep(eval_interval)
    finally:
        _http.set_not_ready()
        from src.db.connection import close_db_pool
        from src.cache.redis_client import close_redis
        await close_db_pool()
        await close_redis()


if __name__ == "__main__":
    asyncio.run(_main())
