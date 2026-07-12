"""
Replay Scheduler — Phase 2: Orchestration + Replay System

Redis-backed job queue for BacktestingEngine replay runs.

Design:
  Stateless: all durable state is in Redis (queue) and PostgreSQL (replay_runs).
  Process restart is safe — jobs remain in Redis queue or DB.

Redis keys:
  replay:queue           — LPUSH/BRPOP list of pending job payloads (JSON)
  replay:processing:{id} — String guard while a job is executing (TTL = 3600s)

Auto-scheduling:
  Weekly:  enqueued every Monday 02:00 UTC for the prior 7-day period
  Monthly: enqueued every 1st of month 03:00 UTC for the prior month
  Can be disabled via env: REPLAY_AUTO_SCHEDULE=false

Retry behaviour:
  Failed jobs are re-enqueued up to MAX_RETRIES=3 times with RETRY_DELAY_S=60s gap.
  After max retries the replay_run is marked 'failed' and must be manually re-triggered.

Exposes /health + Prometheus /metrics on REPLAY_HTTP_PORT (default 8015).
"""
from __future__ import annotations

import asyncio
import json
import math
import os
import time
from datetime import date, datetime, timedelta, timezone
from typing import TYPE_CHECKING

import asyncpg
from prometheus_client import Counter, Gauge, Histogram

from src.core import http_server as _http
from src.core.logging import configure_logging, get_logger
from src.engines.backtesting_engine import BacktestingEngine

if TYPE_CHECKING:
    import redis.asyncio as aioredis

logger = get_logger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────────

QUEUE_KEY = "replay:queue"
PROCESSING_KEY_TPL = "replay:processing:{run_id}"
MAX_RETRIES = 3
RETRY_DELAY_S = 60
PROCESSING_LOCK_TTL = 3600   # 1 hour — expires if worker dies mid-job

# ── Prometheus metrics ────────────────────────────────────────────────────────

replay_jobs_enqueued = Counter(
    "replay_jobs_enqueued_total",
    "Total replay jobs pushed to Redis queue",
    ["window_type"],
)
replay_jobs_completed = Counter(
    "replay_jobs_completed_total",
    "Total replay jobs completed successfully",
)
replay_jobs_failed = Counter(
    "replay_jobs_failed_total",
    "Total replay jobs that exhausted retries",
)
replay_queue_depth = Gauge(
    "replay_queue_depth",
    "Current number of jobs in the replay queue",
)
replay_job_duration = Histogram(
    "replay_job_duration_seconds",
    "Duration of individual replay job execution",
    buckets=[1, 5, 15, 30, 60, 120, 300, 600],
)


# ── Scheduler ─────────────────────────────────────────────────────────────────

class ReplayScheduler:
    """
    Stateless Redis-backed job queue for BacktestingEngine.
    Runs as a standalone service or is embedded in a test harness.
    """

    def __init__(
        self,
        db_pool: asyncpg.Pool,
        redis: "aioredis.Redis",
        auto_schedule: bool = True,
        poll_interval: int = 5,
    ) -> None:
        self._db = db_pool
        self._redis = redis
        self._auto_schedule = auto_schedule
        self._poll_interval = poll_interval
        self._engine = BacktestingEngine(db_pool, redis)
        self._running = False

    # ── Enqueue ───────────────────────────────────────────────────────────────

    async def enqueue(
        self,
        window_type: str,
        period_start: datetime,
        period_end: datetime,
        engine_name: str | None = None,
        parameters: dict | None = None,
        retry_count: int = 0,
    ) -> str:
        """
        Create a replay_run DB record and push the job to Redis queue.
        Returns run_id.
        """
        run_id = await self._engine.create_run(
            window_type=window_type,
            period_start=period_start,
            period_end=period_end,
            engine_name=engine_name,
            parameters=parameters or {},
        )

        payload = json.dumps({
            "run_id":       run_id,
            "window_type":  window_type,
            "period_start": period_start.isoformat(),
            "period_end":   period_end.isoformat(),
            "engine_name":  engine_name,
            "retry_count":  retry_count,
        })
        await self._redis.lpush(QUEUE_KEY, payload)
        await self._update_queue_depth_gauge()

        replay_jobs_enqueued.labels(window_type=window_type).inc()
        logger.info(
            "replay_enqueued",
            run_id=run_id,
            window_type=window_type,
            period=f"{period_start.date()} → {period_end.date()}",
        )
        return run_id

    # ── Main loop ─────────────────────────────────────────────────────────────

    async def run(self) -> None:
        """
        Blocking worker loop: pop jobs from Redis and execute them via BacktestingEngine.
        Runs until stop() is called.
        """
        self._running = True
        logger.info("replay_scheduler_started", auto_schedule=self._auto_schedule)

        if self._auto_schedule:
            # Auto-enqueue any missed windows on startup
            await self._schedule_due_windows()

        _last_schedule_check = time.monotonic()
        _SCHEDULE_CHECK_INTERVAL = 3600  # re-check for new due windows every 1h

        while self._running:
            await self._update_queue_depth_gauge()

            # Periodically re-check for newly-due windows (new week / new month / daily)
            elapsed = time.monotonic() - _last_schedule_check
            if self._auto_schedule and elapsed >= _SCHEDULE_CHECK_INTERVAL:
                logger.info("schedule_check_firing", elapsed_s=round(elapsed))
                try:
                    await self._schedule_due_windows()
                except Exception as exc:
                    logger.error("schedule_check_error", error=str(exc))
                _last_schedule_check = time.monotonic()

            payload = await self._pop_job()

            if payload is None:
                await asyncio.sleep(self._poll_interval)
                continue

            await self._execute_job(payload)

    async def stop(self) -> None:
        self._running = False

    # ── Auto-scheduling ───────────────────────────────────────────────────────

    async def _schedule_due_windows(self) -> None:
        """
        Enqueue windows that are due and have no completed replay run yet.
        Called once on startup and periodically (every 1h).
        """
        now = datetime.now(timezone.utc)

        # Daily: yesterday UTC
        yesterday_start = (now - timedelta(days=1)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        yesterday_end = yesterday_start.replace(
            hour=23, minute=59, second=59, microsecond=0
        )
        if not await self._run_exists("daily", yesterday_start, yesterday_end):
            await self.enqueue("daily", yesterday_start, yesterday_end)

        # Weekly: last Monday → this Monday
        last_monday = _last_monday(now)
        prev_monday = last_monday - timedelta(days=7)
        if not await self._run_exists("weekly", prev_monday, last_monday - timedelta(seconds=1)):
            await self.enqueue("weekly", prev_monday, last_monday - timedelta(seconds=1))

        # Monthly: last complete month
        first_this_month = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        last_month_end = first_this_month - timedelta(seconds=1)
        last_month_start = last_month_end.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        if not await self._run_exists("monthly", last_month_start, last_month_end):
            await self.enqueue("monthly", last_month_start, last_month_end)

    async def _run_exists(
        self, window_type: str, period_start: datetime, period_end: datetime
    ) -> bool:
        """True if a completed replay run with actual signal data covers this period.
        Runs with total_signals=0 are treated as missing — they had no outcome data
        when they ran and should be retried once data is available."""
        async with self._db.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT 1 FROM replay_runs
                 WHERE window_type  = $1
                   AND period_start = $2
                   AND status = 'completed'
                   AND COALESCE(total_signals, 0) > 0
                 LIMIT 1
                """,
                window_type,
                period_start,
            )
            return row is not None

    # ── Job execution ─────────────────────────────────────────────────────────

    async def _pop_job(self) -> dict | None:
        """Non-blocking RPOP from the queue. Returns parsed payload or None."""
        raw = await self._redis.rpop(QUEUE_KEY)
        if not raw:
            return None
        try:
            payload = json.loads(raw)
            payload["period_start"] = datetime.fromisoformat(payload["period_start"])
            payload["period_end"]   = datetime.fromisoformat(payload["period_end"])
            return payload
        except Exception as exc:
            logger.error("replay_pop_parse_error", error=str(exc), raw=raw[:200])
            return None

    async def _execute_job(self, payload: dict) -> None:
        """Execute a single replay job. Retries on failure up to MAX_RETRIES."""
        run_id      = payload["run_id"]
        retry_count = payload.get("retry_count", 0)

        # Idempotency guard: skip if already being processed
        lock_key = PROCESSING_KEY_TPL.format(run_id=run_id)
        acquired = await self._redis.set(lock_key, "1", ex=PROCESSING_LOCK_TTL, nx=True)
        if not acquired:
            logger.warning("replay_already_processing", run_id=run_id)
            return

        t0 = time.monotonic()
        try:
            await self._engine.execute_run(run_id)
            replay_jobs_completed.inc()
            elapsed = time.monotonic() - t0
            replay_job_duration.observe(elapsed)
            logger.info("replay_job_done", run_id=run_id, elapsed_s=round(elapsed, 2))

        except Exception as exc:
            elapsed = time.monotonic() - t0
            logger.error(
                "replay_job_error",
                run_id=run_id,
                retry_count=retry_count,
                elapsed_s=round(elapsed, 2),
                error=str(exc),
            )
            if retry_count < MAX_RETRIES:
                await asyncio.sleep(RETRY_DELAY_S)
                new_payload = {**payload, "retry_count": retry_count + 1}
                await self._redis.lpush(QUEUE_KEY, json.dumps({
                    **new_payload,
                    "period_start": payload["period_start"].isoformat(),
                    "period_end":   payload["period_end"].isoformat(),
                }))
                # Reset DB status to queued for retry
                async with self._db.acquire() as conn:
                    await conn.execute(
                        "UPDATE replay_runs SET status='queued', retry_count=$1 WHERE run_id=$2",
                        retry_count + 1, run_id,
                    )
                logger.info("replay_job_requeued", run_id=run_id, attempt=retry_count + 1)
            else:
                replay_jobs_failed.inc()
                logger.error("replay_job_exhausted", run_id=run_id, max_retries=MAX_RETRIES)

        finally:
            await self._redis.delete(lock_key)

    async def _update_queue_depth_gauge(self) -> None:
        try:
            depth = await self._redis.llen(QUEUE_KEY)
            replay_queue_depth.set(depth or 0)
        except Exception:
            pass


# ── Helpers ───────────────────────────────────────────────────────────────────

def _last_monday(ref: datetime) -> datetime:
    """Return the most recent Monday at 00:00 UTC relative to ref."""
    days_since = ref.weekday()  # Monday=0
    return ref.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=days_since)


# ── Standalone entrypoint ─────────────────────────────────────────────────────

async def _main() -> None:
    configure_logging()

    from src.db.connection import get_db_pool, close_db_pool
    from src.cache.redis_client import get_redis, close_redis

    http_port     = int(os.getenv("REPLAY_HTTP_PORT",     "8015"))
    poll_interval = int(os.getenv("REPLAY_POLL_INTERVAL", "5"))
    auto_schedule = os.getenv("REPLAY_AUTO_SCHEDULE", "true").lower() == "true"

    _http.start(http_port, "replay-scheduler")

    db    = await get_db_pool()
    redis = await get_redis()
    _http.set_ready("replay-scheduler")

    scheduler = ReplayScheduler(
        db_pool=db,
        redis=redis,
        auto_schedule=auto_schedule,
        poll_interval=poll_interval,
    )
    try:
        await scheduler.run()
    finally:
        _http.set_not_ready()
        await close_db_pool()
        await close_redis()


if __name__ == "__main__":
    asyncio.run(_main())
