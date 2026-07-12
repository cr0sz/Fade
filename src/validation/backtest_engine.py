"""
Backtest Worker Service — Level 4

Evaluates signals in signals_registry that have passed their evaluation window,
writes outcomes to signal_outcomes and aggregates daily metrics into
engine_performance_metrics.

Also runs full backtest_runs on a schedule (configurable window).

Exposes /health and Prometheus /metrics via embedded HTTP server.
"""
from __future__ import annotations

import asyncio
import logging
import os
import time
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import asyncpg
from src.core import http_server as _http

from src.core.logging import configure_logging, get_logger
from src.db.connection import get_db_pool, close_db_pool
from src.cache.redis_client import get_redis, close_redis
from src.validation.metrics import (
    backtest_cycles_total,
    signals_evaluated_total,
    engine_accuracy_gauge,
    backtest_cycle_seconds,
    backtest_errors_total,
)

logger = get_logger(__name__)

# Evaluation windows in hours. Signal must have emitted_at + window < now to evaluate.
EVAL_WINDOWS = [4, 24, 72]

# Positive directions: signal predicted upside/risk-on
_POSITIVE_DIRECTIONS = {"risk_on", "inflow", "liquidity_expansion"}
# Negative directions: signal predicted downside/risk-off
_NEGATIVE_DIRECTIONS = {"risk_off", "outflow", "tightening", "volatility_regime"}


def _direction_polarity(direction: str | None) -> int:
    """Return +1 for positive, -1 for negative, 0 for neutral/unknown."""
    if direction in _POSITIVE_DIRECTIONS:
        return 1
    if direction in _NEGATIVE_DIRECTIONS:
        return -1
    return 0


def _score_delta_polarity(delta: float) -> int:
    """Derive actual direction from composite_score change."""
    if delta > 0.02:
        return 1   # risk_on / inflow
    if delta < -0.02:
        return -1  # risk_off / outflow
    return 0


def _directions_correct(predicted_pol: int, actual_pol: int) -> bool | None:
    """True if both are same non-neutral polarity. None if can't determine."""
    if predicted_pol == 0 or actual_pol == 0:
        return None  # can't evaluate neutral
    return predicted_pol == actual_pol


class BacktestWorker:
    def __init__(
        self,
        db: asyncpg.Pool,
        redis,
        eval_interval: int = 900,
        backtest_window_days: int = 30,
    ) -> None:
        self._db = db
        self._redis = redis
        self._eval_interval = eval_interval
        self._backtest_window_days = backtest_window_days
        self._running = False

    async def run(self) -> None:
        self._running = True
        logger.info("backtest_worker_started")
        while self._running:
            cycle_start = time.monotonic()
            try:
                await self._evaluation_cycle()
                await self._update_engine_performance_metrics()
                backtest_cycles_total.inc()
            except Exception as exc:
                logger.error("backtest_cycle_error", error=str(exc))
                backtest_errors_total.inc()
            finally:
                elapsed = time.monotonic() - cycle_start
                backtest_cycle_seconds.observe(elapsed)

            await asyncio.sleep(self._eval_interval)

    async def stop(self) -> None:
        self._running = False

    # ─── Outcome Evaluation ──────────────────────────────────────────────────

    async def _evaluation_cycle(self) -> None:
        """
        Find signals past their evaluation windows and compute outcomes.
        Only evaluates signals that have a subsequent trend snapshot to compare against.
        """
        now = datetime.now(timezone.utc)

        # Evaluate each window independently
        for window_h in EVAL_WINDOWS:
            cutoff = now - timedelta(hours=window_h)
            await self._evaluate_window(window_h, cutoff)

    async def _evaluate_window(self, window_h: int, cutoff: datetime) -> None:
        """Evaluate all unevaluated signals whose emitted_at + window_h < now."""
        async with self._db.acquire() as conn:
            # Find signals emitted before cutoff that don't yet have an outcome for this window
            signals = await conn.fetch(
                """
                SELECT sr.*
                  FROM signals_registry sr
                  LEFT JOIN signal_outcomes so
                    ON so.signal_id = sr.id AND so.evaluation_window_h = $1
                 WHERE sr.emitted_at < $2
                   AND so.id IS NULL
                   AND sr.ticker IS NOT NULL
                   AND sr.ticker != 'MARKET'
                   AND sr.direction IS NOT NULL
                 ORDER BY sr.emitted_at ASC
                 LIMIT 200
                """,
                window_h,
                cutoff,
            )

            if not signals:
                return

            evaluated = 0
            for sig in signals:
                outcome = await self._compute_outcome(conn, sig, window_h)
                if outcome is None:
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
                evaluated += 1
                signals_evaluated_total.labels(
                    engine=sig["engine_name"],
                    window_hours=str(window_h),
                ).inc()

            if evaluated:
                logger.info(
                    "outcomes_written",
                    window_h=window_h,
                    count=evaluated,
                )

    async def _compute_outcome(
        self, conn: asyncpg.Connection, signal: asyncpg.Record, window_h: int
    ) -> dict | None:
        """
        Find the closest subsequent trend snapshot and compare directions.
        Uses signals_registry (market_trends snapshots) as ground truth.
        """
        ticker = signal["ticker"]
        emitted_at = signal["emitted_at"]
        target_after = emitted_at + timedelta(hours=window_h)

        # Find the closest trend snapshot for this ticker after the evaluation window
        future_snap = await conn.fetchrow(
            """
            SELECT composite_score, direction, emitted_at
              FROM signals_registry
             WHERE ticker = $1
               AND signal_type = 'trend'
               AND emitted_at BETWEEN $2 AND $3
             ORDER BY emitted_at ASC
             LIMIT 1
            """,
            ticker,
            target_after - timedelta(hours=2),
            target_after + timedelta(hours=window_h),  # allow generous window
        )

        if not future_snap:
            return None  # no data yet — skip

        current_score = signal["composite_score"] or 0.0
        future_score = future_snap["composite_score"] or 0.0
        delta = future_score - current_score

        predicted_pol = _direction_polarity(signal["direction"])
        actual_pol = _score_delta_polarity(delta)

        # Derive actual direction label
        if actual_pol > 0:
            actual_dir = "risk_on"
        elif actual_pol < 0:
            actual_dir = "risk_off"
        else:
            actual_dir = "neutral"

        correct = _directions_correct(predicted_pol, actual_pol)

        return {
            "actual_direction": actual_dir,
            "direction_correct": correct,
            "score_delta": delta,
        }

    # ─── Engine Performance Aggregation ──────────────────────────────────────

    async def _update_engine_performance_metrics(self) -> None:
        """Recompute daily accuracy per engine and upsert into engine_performance_metrics."""
        async with self._db.acquire() as conn:
            engines = await conn.fetch(
                "SELECT DISTINCT engine_name FROM signals_registry WHERE emitted_at > now() - INTERVAL '60 days'"
            )
            for row in engines:
                engine = row["engine_name"]
                await self._upsert_daily_metrics(conn, engine)
                # Update Prometheus gauges
                for window_h in EVAL_WINDOWS:
                    acc = await self._compute_accuracy(conn, engine, window_h, days=7)
                    if acc is not None:
                        engine_accuracy_gauge.labels(
                            engine=engine, window_hours=str(window_h)
                        ).set(acc)

    async def _upsert_daily_metrics(self, conn: asyncpg.Connection, engine: str) -> None:
        today = datetime.now(timezone.utc).date()

        total = await conn.fetchval(
            """
            SELECT COUNT(*) FROM signals_registry
            WHERE engine_name = $1 AND emitted_at::date = $2
            """,
            engine, today,
        )

        acc_4h  = await self._compute_accuracy(conn, engine, 4,  days=1)
        acc_24h = await self._compute_accuracy(conn, engine, 24, days=1)
        acc_72h = await self._compute_accuracy(conn, engine, 72, days=1)

        avg_conf = await conn.fetchval(
            """
            SELECT AVG(
                CASE confidence_level
                    WHEN 'high'   THEN 1.0
                    WHEN 'medium' THEN 0.5
                    WHEN 'low'    THEN 0.0
                    ELSE NULL
                END
            )
            FROM signals_registry
            WHERE engine_name = $1 AND emitted_at::date = $2
            """,
            engine, today,
        )

        high_conf_acc = await self._compute_accuracy(
            conn, engine, 24, days=1, confidence_level="high"
        )

        # Signal count by type
        type_rows = await conn.fetch(
            """
            SELECT signal_type, COUNT(*) as cnt
              FROM signals_registry
             WHERE engine_name = $1 AND emitted_at::date = $2
             GROUP BY signal_type
            """,
            engine, today,
        )
        count_by_type = {r["signal_type"]: r["cnt"] for r in type_rows}

        import json as _json
        await conn.execute(
            """
            INSERT INTO engine_performance_metrics (
                engine_name, metric_date, total_signals,
                accuracy_4h, accuracy_24h, accuracy_72h,
                avg_confidence, high_conf_accuracy, signal_count_by_type
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
            ON CONFLICT (engine_name, metric_date) DO UPDATE SET
                total_signals    = EXCLUDED.total_signals,
                accuracy_4h      = EXCLUDED.accuracy_4h,
                accuracy_24h     = EXCLUDED.accuracy_24h,
                accuracy_72h     = EXCLUDED.accuracy_72h,
                avg_confidence   = EXCLUDED.avg_confidence,
                high_conf_accuracy = EXCLUDED.high_conf_accuracy,
                signal_count_by_type = EXCLUDED.signal_count_by_type,
                computed_at      = now()
            """,
            engine, today, total or 0,
            acc_4h, acc_24h, acc_72h,
            avg_conf, high_conf_acc,
            _json.dumps(count_by_type),
        )

    async def _compute_accuracy(
        self,
        conn: asyncpg.Connection,
        engine: str,
        window_h: int,
        days: int = 7,
        confidence_level: str | None = None,
    ) -> float | None:
        conf_filter = ""
        # str() required: ($3 || ' days') makes asyncpg infer $3 as text
        params: list = [engine, window_h, str(days)]
        if confidence_level:
            params.append(confidence_level)
            conf_filter = f" AND sr.confidence_level = ${len(params)}"

        row = await conn.fetchrow(
            f"""
            SELECT
                COUNT(*) FILTER (WHERE so.direction_correct = true)::float /
                NULLIF(COUNT(*) FILTER (WHERE so.direction_correct IS NOT NULL), 0)
                    AS accuracy
              FROM signal_outcomes so
              JOIN signals_registry sr ON sr.id = so.signal_id
             WHERE sr.engine_name = $1
               AND so.evaluation_window_h = $2
               AND so.evaluated_at > now() - ($3 || ' days')::interval
               {conf_filter}
            """,
            *params,
        )
        return float(row["accuracy"]) if row and row["accuracy"] is not None else None

    # ─── On-demand Backtest Run ───────────────────────────────────────────────

    async def run_backtest(
        self,
        engine_name: str | None = None,
        window_days: int | None = None,
    ) -> str:
        """Create and execute a full backtest run. Returns run_id."""
        window = window_days or self._backtest_window_days
        run_id = str(uuid4())

        async with self._db.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO backtest_runs (run_id, engine_name, window_days, parameters)
                VALUES ($1, $2, $3, $4)
                """,
                run_id, engine_name, window,
                f'{{"window_days": {window}}}',
            )

        try:
            await self._execute_backtest(run_id, engine_name, window)
        except Exception as exc:
            async with self._db.acquire() as conn:
                await conn.execute(
                    "UPDATE backtest_runs SET status = 'failed', completed_at = now() WHERE run_id = $1",
                    run_id,
                )
            raise

        return run_id

    async def _execute_backtest(
        self, run_id: str, engine_name: str | None, window_days: int
    ) -> None:
        cutoff = datetime.now(timezone.utc) - timedelta(hours=72)
        since = datetime.now(timezone.utc) - timedelta(days=window_days)

        engine_filter = "AND sr.engine_name = $3" if engine_name else ""
        params: list = [since, cutoff]
        if engine_name:
            params.append(engine_name)

        async with self._db.acquire() as conn:
            signals = await conn.fetch(
                f"""
                SELECT * FROM signals_registry
                WHERE emitted_at BETWEEN $1 AND $2
                  AND ticker IS NOT NULL
                  AND ticker != 'MARKET'
                  AND direction IS NOT NULL
                  {engine_filter}
                ORDER BY emitted_at ASC
                """,
                *params,
            )

            total = len(signals)
            evaluated = 0

            for sig in signals:
                for window_h in [24, 72]:
                    outcome = await self._compute_outcome(conn, sig, window_h)
                    if not outcome:
                        continue
                    await conn.execute(
                        """
                        INSERT INTO backtest_results (
                            run_id, signal_id, ticker, engine_name,
                            predicted_direction, actual_direction,
                            direction_correct, predicted_score,
                            outcome_score_delta, window_hours
                        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
                        """,
                        run_id,
                        sig["id"],
                        sig["ticker"],
                        sig["engine_name"],
                        sig["direction"],
                        outcome["actual_direction"],
                        outcome["direction_correct"],
                        sig["composite_score"],
                        outcome["score_delta"],
                        window_h,
                    )
                    evaluated += 1

            # Compute summary accuracies
            def _acc(wh: int) -> float | None:
                correct = sum(
                    1 for _ in [None]  # placeholder — computed from DB
                )
                return None

            acc_4h = await self._compute_bt_accuracy(conn, run_id, 4)
            acc_24h = await self._compute_bt_accuracy(conn, run_id, 24)
            acc_72h = await self._compute_bt_accuracy(conn, run_id, 72)

            await conn.execute(
                """
                UPDATE backtest_runs SET
                    status = 'completed',
                    completed_at = now(),
                    total_signals = $1,
                    evaluated_signals = $2,
                    accuracy_4h = $3,
                    accuracy_24h = $4,
                    accuracy_72h = $5
                WHERE run_id = $6
                """,
                total, evaluated, acc_4h, acc_24h, acc_72h, run_id,
            )
            logger.info("backtest_run_complete", run_id=run_id, total=total, evaluated=evaluated)

    async def _compute_bt_accuracy(
        self, conn: asyncpg.Connection, run_id: str, window_h: int
    ) -> float | None:
        row = await conn.fetchrow(
            """
            SELECT
                COUNT(*) FILTER (WHERE direction_correct = true)::float /
                NULLIF(COUNT(*) FILTER (WHERE direction_correct IS NOT NULL), 0) AS acc
            FROM backtest_results
            WHERE run_id = $1 AND window_hours = $2
            """,
            run_id, window_h,
        )
        return float(row["acc"]) if row and row["acc"] is not None else None


# ─── Entrypoint ──────────────────────────────────────────────────────────────

async def _main() -> None:
    configure_logging()
    logger.info("backtest_worker_init")

    port = int(os.getenv("BACKTEST_HTTP_PORT", "8011"))
    _http.start(port, "backtest-worker")

    eval_interval = int(os.getenv("BACKTEST_EVAL_INTERVAL", "900"))
    window_days   = int(os.getenv("BACKTEST_WINDOW_DAYS",   "30"))

    db    = await get_db_pool()
    redis = await get_redis()
    _http.set_ready("backtest-worker")

    worker = BacktestWorker(db, redis, eval_interval=eval_interval, backtest_window_days=window_days)
    try:
        await worker.run()
    finally:
        _http.set_not_ready()
        await close_db_pool()
        await close_redis()


if __name__ == "__main__":
    asyncio.run(_main())
