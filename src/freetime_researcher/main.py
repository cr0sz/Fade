"""Free-time fade-sweep researcher — Sprint 48.

Continuously backtests fade-strategy parameter variants in spare compute, but
YIELDS to Ollama: before every work unit it checks /api/ps and the host load.
If a model is loaded or load is high, it pauses (publishing freetime:status to
Redis for the dashboard) and retries. Otherwise it runs ONE param combo, writes
the result to research_runs, and emits a Telegram recommendation if the OOS
result is promising. RECOMMEND-ONLY — never touches live config.

Run:  python -m src.freetime_researcher.main
"""
from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone

from src.cache.redis_client import get_redis
from src.core.logging import configure_logging, get_logger
from src.db.connection import get_db_pool
from src.freetime_researcher import sweep
from src.freetime_researcher.arbitration import load_high, ollama_busy
from src.freetime_researcher.notify import format_find, send_telegram

logger = get_logger(__name__)

STATUS_KEY = "freetime:status"
STATUS_TTL = 120          # seconds; refreshed each tick so a dead worker expires
PAUSE_SLEEP = 30          # seconds to wait while paused
IDLE_SLEEP = 600          # seconds between grid-exhaustion rechecks (10 min)
FIND_COOLDOWN_KEY = "freetime:find_cooldown"
FIND_COOLDOWN_SEC = 1800  # max 1 find message / 30 min
# A combo claimed 'running' but never finalized (worker crashed mid-unit) would
# otherwise be skipped forever by ON CONFLICT DO NOTHING. Reap rows stuck in
# 'running' past this age so the grid can reclaim them on the next pass.
STUCK_RUNNING_MINUTES = 30

# Promotion criteria for a Telegram find.
MIN_OOS_N = 30
MIN_OOS_AVG = 1.0         # percent
MIN_IS_N = 30


async def _set_status(redis, value: str) -> None:
    await redis.setex(STATUS_KEY, STATUS_TTL, value)


async def _wait_until_clear(redis) -> None:
    """Block (publishing paused status) until both Ollama and host load are clear."""
    while True:
        if await ollama_busy():
            await _set_status(redis, "paused_ollama")
            logger.info("paused", reason="ollama")
            await asyncio.sleep(PAUSE_SLEEP)
            continue
        if load_high():
            await _set_status(redis, "paused_load")
            logger.info("paused", reason="load")
            await asyncio.sleep(PAUSE_SLEEP)
            continue
        return


async def _load_quotes(pool):
    async with pool.acquire() as conn:
        rows = await conn.fetch(sweep.QUOTES_SQL)
    prices, volumes, hours = sweep.build_series(rows)
    logger.info(
        "quotes_loaded",
        symbols=len(prices),
        hours=len(hours),
        first=str(hours[0]) if hours else None,
        last=str(hours[-1]) if hours else None,
    )
    return prices, volumes, hours


async def _reap_stuck_running(pool) -> int:
    """Delete combos stuck in 'running' (a crashed prior pass). They are blocked
    by the unique index from ever being re-claimed, so delete rather than flip
    status — the next _claim_combo INSERT recreates the row cleanly."""
    async with pool.acquire() as conn:
        n = await conn.fetchval(
            f"""
            WITH d AS (
                DELETE FROM research_runs
                 WHERE sweep_name = $1 AND status = 'running'
                   AND started_at < now() - interval '{STUCK_RUNNING_MINUTES} minutes'
                RETURNING 1
            )
            SELECT count(*) FROM d
            """,
            sweep.SWEEP_NAME,
        )
    if n:
        logger.warning("reaped_stuck_running", count=n,
                       older_than_min=STUCK_RUNNING_MINUTES)
    return n or 0


async def _claim_combo(pool, params: dict) -> int | None:
    """INSERT status='running' for this combo. Returns the new row id, or None
    if it already exists (unique index on sweep_name + md5(params::text)).

    NOTE: we finalize by this returned id, NOT by re-deriving md5(params::text)
    in Python — Postgres stores jsonb with its own key order/spacing, so a
    Python json.dumps would hash differently and the UPDATE would match 0 rows.
    """
    pjson = json.dumps(params, sort_keys=True, separators=(",", ":"))
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            INSERT INTO research_runs (sweep_name, params, status, started_at)
            VALUES ($1, $2::jsonb, 'running', now())
            ON CONFLICT (sweep_name, md5(params::text)) DO NOTHING
            RETURNING id
            """,
            sweep.SWEEP_NAME,
            pjson,
        )
    return row["id"] if row is not None else None


async def _finalize_combo(pool, run_id: int, m: dict) -> None:
    async with pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE research_runs
               SET n_trades = $2, win_rate = $3, avg_ret_pct = $4,
                   total_ret_pct = $5, sharpe = $6, max_dd_pct = $7,
                   oos_avg_ret_pct = $8, oos_n = $9,
                   status = 'done', finished_at = now()
             WHERE id = $1
            """,
            run_id,
            m["n"], m["win_rate"], m["avg_ret_pct"], m["total_ret_pct"],
            m["sharpe"], m["max_dd_pct"], m["oos_avg_ret_pct"], m["oos_n"],
        )


def _is_promising(params: dict, m: dict) -> bool:
    if m["oos_n"] is None or m["oos_n"] < MIN_OOS_N:
        return False
    if m["oos_avg_ret_pct"] is None or m["oos_avg_ret_pct"] < MIN_OOS_AVG:
        return False
    if m["n"] is None or m["n"] < MIN_IS_N or m["avg_ret_pct"] is None:
        return False
    # IS and OOS must agree in sign.
    return (m["avg_ret_pct"] > 0) == (m["oos_avg_ret_pct"] > 0) and m["avg_ret_pct"] > 0


async def _maybe_notify(redis, params: dict, m: dict) -> None:
    if not _is_promising(params, m):
        return
    # Cooldown: max 1 find / 30 min. SET NX EX is atomic.
    won = await redis.set(FIND_COOLDOWN_KEY, "1", nx=True, ex=FIND_COOLDOWN_SEC)
    if not won:
        logger.info("find_suppressed_cooldown", params=params)
        return
    await send_telegram(format_find(params, m))
    logger.info("find_notified", params=params,
                oos_avg=m["oos_avg_ret_pct"], oos_n=m["oos_n"])


async def run() -> None:
    configure_logging()
    logger.info("freetime_researcher_start", sweep=sweep.SWEEP_NAME,
                grid_size=sweep.grid_size())
    pool = await get_db_pool()
    redis = await get_redis()

    # Quotes are loaded once per pass over the grid; reloaded when we loop back
    # after exhausting the grid (so new data is picked up).
    prices = volumes = hours = None
    quotes_loaded_for_pass = False

    while True:
        if not quotes_loaded_for_pass:
            await _wait_until_clear(redis)
            prices, volumes, hours = await _load_quotes(pool)
            quotes_loaded_for_pass = True

        # Reclaim any combos stranded 'running' by a crashed prior pass.
        await _reap_stuck_running(pool)

        did_work = False
        for params in sweep.enumerate_grid():
            # Arbitration before EVERY work unit.
            await _wait_until_clear(redis)

            run_id = await _claim_combo(pool, params)
            if run_id is None:
                continue  # already done or running — resume past it

            await _set_status(redis, f"running:{sweep.SWEEP_NAME}")
            t0 = datetime.now(timezone.utc)
            try:
                m = sweep.run_combo(prices, volumes, hours, params)
            except Exception as e:
                logger.error("combo_failed", params=params, error=str(e))
                # Mark this row as error (by id) so we don't retry it forever.
                async with pool.acquire() as conn:
                    await conn.execute(
                        "UPDATE research_runs SET status='error', finished_at=now() "
                        "WHERE id=$1",
                        run_id,
                    )
                continue

            await _finalize_combo(pool, run_id, m)
            did_work = True
            dur = (datetime.now(timezone.utc) - t0).total_seconds()
            logger.info("combo_done", params=params, n=m["n"],
                        oos_n=m["oos_n"], oos_avg=m["oos_avg_ret_pct"],
                        is_avg=m["avg_ret_pct"], dur_s=round(dur, 1))
            await _maybe_notify(redis, params, m)

        # Grid pass complete.
        if not did_work:
            await _set_status(redis, "idle_complete")
            logger.info("grid_exhausted_idle", sleep_s=IDLE_SLEEP)
            await asyncio.sleep(IDLE_SLEEP)
            # On next wake, reload quotes in case new data / new sweep version.
            quotes_loaded_for_pass = False
        else:
            # We did work this pass but reached the end — loop again to catch
            # any combos that were 'running' (claimed by a prior crashed pass).
            quotes_loaded_for_pass = False


def main() -> None:
    asyncio.run(run())


if __name__ == "__main__":
    main()
