"""Infrastructure watchdog + daily digest service (Sprint 49).

Every 120s:
  1. containers (docker socket, read-only)
  2. funding freshness (max(ts) < 20min)
  3. quote freshness (CRYPTO max(ts) < 3x observed cadence, floor 10min)
  4. api /health
  5. heartbeat → Redis watchdog:last_run
  + daily digest at 07:00 UTC

Alerts go to Telegram (60-min per-check cooldown + RECOVERY) and into
validation_alerts (alert_type='infra_<check>') for the dashboard.

Run:  python -m src.watchdog.main
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone

from src.cache.redis_client import get_redis
from src.core.logging import configure_logging, get_logger
from src.db.connection import get_db_pool
from src.watchdog import alerts, checks
from src.watchdog.digest import maybe_send_digest

logger = get_logger(__name__)

POLL_INTERVAL_SEC = 120
HEARTBEAT_KEY = "watchdog:last_run"


async def _run_check(redis, db, name: str, coro) -> None:
    """Run a check coroutine, fire/clear alerts, swallow exceptions."""
    try:
        ok, detail = await coro
    except Exception as e:
        ok, detail = False, f"check raised: {e}"
        logger.error("check_exception", check=name, error=str(e))
    if ok:
        await alerts.clear(redis, db, name)
    else:
        await alerts.fire(redis, db, name, detail)
    logger.info("check_result", check=name, ok=ok, detail=detail)


async def _cycle(redis, db, cadence_sec: float) -> None:
    await _run_check(redis, db, "containers", checks.check_containers())
    await _run_check(redis, db, "funding_freshness", checks.check_funding_freshness(db))
    await _run_check(redis, db, "quote_freshness", checks.check_quote_freshness(db, cadence_sec))
    await _run_check(redis, db, "liq_stream", checks.check_liq_stream(redis))
    await _run_check(redis, db, "api_health", checks.check_api_health())

    # Heartbeat for the dashboard /system endpoint.
    try:
        await redis.set(HEARTBEAT_KEY, datetime.now(timezone.utc).isoformat())
    except Exception as e:
        logger.error("heartbeat_write_failed", error=str(e))

    # Daily digest (no-op until 07:00 UTC + not-yet-sent).
    try:
        await maybe_send_digest(redis, db)
    except Exception as e:
        logger.error("digest_cycle_failed", error=str(e))


async def _self_test(redis, db, cadence_sec: float) -> None:
    """Log one line per check with current status, without firing/clearing."""
    logger.info("self_test_start", quote_cadence_sec=round(cadence_sec, 1))
    for name, coro in [
        ("containers", checks.check_containers()),
        ("funding_freshness", checks.check_funding_freshness(db)),
        ("quote_freshness", checks.check_quote_freshness(db, cadence_sec)),
        ("liq_stream", checks.check_liq_stream(redis)),
        ("api_health", checks.check_api_health()),
    ]:
        try:
            ok, detail = await coro
        except Exception as e:
            ok, detail = False, f"raised: {e}"
        logger.info("self_test_check", check=name, ok=ok, detail=detail)


async def main() -> None:
    configure_logging()
    logger.info("watchdog_starting", interval_sec=POLL_INTERVAL_SEC)

    db = await get_db_pool()
    redis = await get_redis()

    cadence_sec = await checks.measure_quote_cadence_sec(db)
    logger.info("quote_cadence_measured", cadence_sec=round(cadence_sec, 1),
                alert_threshold_sec=round(3 * cadence_sec, 1))

    await _self_test(redis, db, cadence_sec)

    # Send the deploy-test digest once, then mark today sent so 07:00 won't
    # double-send (07:00 UTC has already passed at deploy time).
    try:
        if await redis.get("watchdog:digest_sent") is None:
            sent = await maybe_send_digest(redis, db, force=True)
            logger.info("deploy_digest_test", sent=sent)
    except Exception as e:
        logger.error("deploy_digest_failed", error=str(e))

    while True:
        try:
            await _cycle(redis, db, cadence_sec)
        except Exception as e:
            logger.error("cycle_failed", error=str(e))
        await asyncio.sleep(POLL_INTERVAL_SEC)


if __name__ == "__main__":
    asyncio.run(main())
