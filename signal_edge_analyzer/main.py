"""Signal Edge Analyzer — main entry point.

Responsibilities:
  - Start embedded HTTP server (/health, /ready, /metrics)
  - Connect to PostgreSQL and Redis
  - Run DB migration 013
  - Start EdgeScheduler (outcome polling, metrics, aggregation, Claude.md awareness)
  - Monitor system health; trigger controlled restart on repeated failures

Self-healing contract:
  - DB or Redis failure is retried up to MAX_FAILURES consecutive times
  - On breach: Telegram alert sent, then container exits with code 1
  - Docker Compose restart: on-failure policy brings it back automatically
  - After restart the service re-verifies connections and sends "recovered" alert
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from threading import Thread

import asyncpg
import redis.asyncio as aioredis
import structlog
from prometheus_client import (
    CONTENT_TYPE_LATEST,
    Counter,
    Gauge,
    generate_latest,
)

import db as _db
from scheduler import EdgeScheduler
from discord_client import DiscordClient as TelegramClient

# ── Prometheus metrics ─────────────────────────────────────────────────────────
health_check_failures = Counter(
    "edge_health_failures_total", "Consecutive DB/Redis health check failures"
)
service_restarts = Counter(
    "edge_service_restarts_total", "Self-healing restarts triggered"
)
db_connected = Gauge("edge_db_connected", "1 if DB pool is healthy")
redis_connected_g = Gauge("edge_redis_connected", "1 if Redis is reachable")

# ── Logging ────────────────────────────────────────────────────────────────────

def _configure_logging() -> None:
    debug = os.getenv("DEBUG", "").lower() in ("1", "true")
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.stdlib.add_log_level,
            structlog.stdlib.add_logger_name,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.UnicodeDecoder(),
            structlog.processors.JSONRenderer() if not debug
            else structlog.dev.ConsoleRenderer(colors=False),
        ],
        wrapper_class=structlog.stdlib.BoundLogger,
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )
    logging.basicConfig(format="%(message)s", stream=sys.stdout, level=logging.INFO)


logger = structlog.get_logger(__name__)

# ── HTTP server ────────────────────────────────────────────────────────────────

_ready = False


class _Handler(BaseHTTPRequestHandler):
    def log_message(self, *args) -> None:
        pass

    def do_GET(self) -> None:
        if self.path == "/health":
            body = json.dumps({"status": "ok", "service": "signal-edge-analyzer"}).encode()
            self._send(200, "application/json", body)
        elif self.path == "/ready":
            body = json.dumps({"status": "ready" if _ready else "not_ready"}).encode()
            self._send(200 if _ready else 503, "application/json", body)
        elif self.path == "/metrics":
            self._send(200, CONTENT_TYPE_LATEST, generate_latest())
        else:
            self._send(404, "text/plain", b"not found")

    def _send(self, code: int, ct: str, body: bytes) -> None:
        self.send_response(code)
        self.send_header("Content-Type", ct)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def _start_http(port: int) -> None:
    server = HTTPServer(("0.0.0.0", port), _Handler)
    Thread(target=server.serve_forever, daemon=True).start()
    logger.info("http_server_started", port=port)


# ── Self-healing ───────────────────────────────────────────────────────────────

MAX_FAILURES       = int(os.getenv("EDGE_HEALTH_MAX_FAILURES", "3"))
RESTART_COOLDOWN_S = int(os.getenv("EDGE_HEALTH_RESTART_COOLDOWN", "3600"))  # 1 hour
HEALTH_CHECK_INTERVAL_S = int(os.getenv("EDGE_ANALYZER_HEALTH_INTERVAL", "30"))

_consecutive_failures  = 0
_last_degraded_at: float = 0.0
_started_at: float = time.time()
_shutdown_event: asyncio.Event | None = None


async def _health_monitor(
    pool: asyncpg.Pool,
    redis: aioredis.Redis,
    telegram: TelegramClient,
    shutdown: asyncio.Event,
) -> None:
    """Background health monitor — triggers controlled restart on repeated failures."""
    global _consecutive_failures, _last_degraded_at

    while True:
        await asyncio.sleep(HEALTH_CHECK_INTERVAL_S)

        db_ok    = await _db.health_check(pool)
        redis_ok = False
        try:
            await redis.ping()
            redis_ok = True
        except Exception:
            pass

        db_connected.set(1 if db_ok else 0)
        redis_connected_g.set(1 if redis_ok else 0)

        if db_ok and redis_ok:
            if _consecutive_failures > 0:
                downtime = time.time() - _last_degraded_at
                logger.info("system_recovered", downtime_s=round(downtime, 1))
                await telegram.send_recovered(downtime)
            _consecutive_failures = 0
            continue

        reason = []
        if not db_ok:
            reason.append("DB connection failed")
        if not redis_ok:
            reason.append("Redis unavailable")
        reason_str = "; ".join(reason)

        _consecutive_failures += 1
        if _consecutive_failures == 1:
            _last_degraded_at = time.time()

        health_check_failures.inc()
        logger.warning(
            "system_health_degraded",
            event_type="system_health_degraded",
            reason=reason_str,
            consecutive_failures=_consecutive_failures,
        )

        if _consecutive_failures >= MAX_FAILURES:
            logger.error(
                "self_healing_triggered",
                reason=reason_str,
                consecutive_failures=_consecutive_failures,
            )
            await telegram.send_health_degraded(reason_str, restart=True)
            service_restarts.inc()
            # Signal orderly shutdown instead of sys.exit(1). sys.exit skips the
            # finally block in _main(), leaving the DB pool and Redis unclosed.
            shutdown.set()
            return
        else:
            await telegram.send_health_degraded(reason_str, restart=False)


# ── Main ───────────────────────────────────────────────────────────────────────

async def _main() -> None:
    global _ready

    _configure_logging()
    structlog.contextvars.bind_contextvars(service="signal-edge-analyzer")
    logger.info("edge_analyzer_init", event_type="edge_analysis_started")

    http_port = int(os.getenv("EDGE_ANALYZER_HTTP_PORT", "8022"))
    _start_http(http_port)

    # Connect DB
    pool = await _db.create_pool(min_size=2, max_size=5)
    logger.info("db_pool_created")

    # Run migration
    try:
        await _db.run_migration(pool)
        logger.info("migration_applied", migration="013_signal_edge_analyzer")
    except Exception as exc:
        logger.warning("migration_warning: %s", exc)

    # Connect Redis
    redis = aioredis.from_url(
        _db.get_redis_url(),
        encoding="utf-8",
        decode_responses=True,
    )
    await redis.ping()
    logger.info("redis_connected")

    telegram = TelegramClient()
    await telegram.send_startup("signal-edge-analyzer", http_port)

    scheduler = EdgeScheduler(pool, redis, telegram)
    _ready = True
    logger.info("edge_analyzer_ready", port=http_port)

    shutdown = asyncio.Event()
    _crashed = False
    try:
        await asyncio.gather(
            scheduler.start(),
            _health_monitor(pool, redis, telegram, shutdown),
            shutdown.wait(),   # unblocks gather when health monitor signals exit
        )
    except Exception:
        _crashed = True
        raise
    finally:
        _ready = False
        scheduler.stop()   # stop the scheduler loop cleanly
        await _db.close_pool()
        await redis.aclose()
        logger.info("edge_analyzer_stopped")
        if _crashed:
            raise SystemExit(1)


if __name__ == "__main__":
    asyncio.run(_main())
