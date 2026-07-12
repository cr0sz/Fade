"""
Paper Trading Engine — entrypoint.

Exposes /health and /metrics via embedded HTTP server on PAPER_TRADING_HTTP_PORT (default 8021).
Polls signals_registry every PAPER_TRADING_POLL_INTERVAL seconds (default 30).
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
from http.server import BaseHTTPRequestHandler, HTTPServer
from threading import Thread

import asyncpg
import redis.asyncio as aioredis
import structlog
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, generate_latest

# ── Prometheus metrics ────────────────────────────────────────────────────────

trades_opened_total = Counter("pt_trades_opened_total", "Paper trades opened")
trades_closed_total = Counter("pt_trades_closed_total", "Paper trades closed", ["reason"])
trades_rejected_total = Counter("pt_trades_rejected_total", "Paper trades rejected")
portfolio_balance = Gauge("pt_portfolio_balance", "Current paper trading balance")
portfolio_equity = Gauge("pt_portfolio_equity", "Current paper trading equity")
portfolio_open_positions = Gauge("pt_open_positions", "Number of open paper positions")
realized_pnl_gauge = Gauge("pt_realized_pnl", "Total realized PnL")

# ── Logging ───────────────────────────────────────────────────────────────────


def configure_logging() -> None:
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

# ── Embedded HTTP server ──────────────────────────────────────────────────────

_ready = False


class _Handler(BaseHTTPRequestHandler):
    def log_message(self, *args) -> None:
        pass

    def do_GET(self) -> None:
        if self.path == "/health":
            body = json.dumps({"status": "ok", "service": "paper-trading-engine"}).encode()
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


# ── DB / Redis connection ─────────────────────────────────────────────────────


def _build_dsn() -> str:
    host = os.environ["POSTGRES_HOST"]
    port = os.getenv("POSTGRES_PORT", "5432")
    db = os.environ["POSTGRES_DB"]
    user = os.environ["POSTGRES_USER"]
    password = os.environ["POSTGRES_PASSWORD"]
    if not password:
        raise RuntimeError(
            "POSTGRES_PASSWORD is empty — refusing to start. "
            "Set a strong password in .env before running."
        )
    return f"postgresql://{user}:{password}@{host}:{port}/{db}"


def _build_redis_url() -> str:
    host = os.environ["REDIS_HOST"]
    port = os.getenv("REDIS_PORT", "6379")
    password = os.getenv("REDIS_PASSWORD", "")
    if password:
        return f"redis://:{password}@{host}:{port}/0"
    return f"redis://{host}:{port}/0"


# ── Metrics bridge (hook engine events to Prometheus counters) ────────────────


def _attach_metrics(engine_instance) -> None:
    """Monkey-patch the engine's _process_signal to update Prometheus gauges."""
    original_process = engine_instance._process_signal

    async def _instrumented_process(signal):
        before_open = len(engine_instance._portfolio.open_positions)
        before_closed = len(engine_instance._portfolio.closed_trades)
        await original_process(signal)
        after_open = len(engine_instance._portfolio.open_positions)
        after_closed = len(engine_instance._portfolio.closed_trades)

        if after_open > before_open:
            trades_opened_total.inc()
        delta = after_closed - before_closed
        if delta > 0:
            # Read close_reason from the trade objects added in this tick.
            # Multiple closes can happen (partial T1, opposing-signal, etc.).
            for t in engine_instance._portfolio.closed_trades[-delta:]:
                reason = getattr(t, "close_reason", None) or "unknown"
                trades_closed_total.labels(reason=reason).inc()

        snap = engine_instance._portfolio.snapshot()
        portfolio_balance.set(snap["balance"])
        portfolio_equity.set(snap["equity"])
        portfolio_open_positions.set(snap["open_positions"])
        realized_pnl_gauge.set(snap["realized_pnl"])

    engine_instance._process_signal = _instrumented_process


# ── Main ──────────────────────────────────────────────────────────────────────


async def _main() -> None:
    global _ready

    configure_logging()
    structlog.contextvars.bind_contextvars(service="paper-trading-engine")
    logger.info("paper_trading_init")

    http_port = int(os.getenv("PAPER_TRADING_HTTP_PORT", "8021"))
    poll_interval = int(os.getenv("PAPER_TRADING_POLL_INTERVAL", "30"))

    _start_http(http_port)

    # Connect to DB and Redis
    db = await asyncpg.create_pool(
        dsn=_build_dsn(),
        min_size=2,
        max_size=5,
        command_timeout=30,
    )
    logger.info("db_pool_created")

    redis = aioredis.from_url(
        _build_redis_url(),
        encoding="utf-8",
        decode_responses=True,
    )
    await redis.ping()
    logger.info("redis_connected")

    # Import here so the module path works from the container's WORKDIR
    from engine import PaperTradingEngine
    from portfolio import Portfolio

    portfolio = Portfolio()
    engine = PaperTradingEngine(db, redis, portfolio, poll_interval=poll_interval)
    _attach_metrics(engine)

    _ready = True
    logger.info("paper_trading_ready", poll_interval=poll_interval)

    try:
        await engine.run()
    finally:
        _ready = False
        await db.close()
        await redis.aclose()
        logger.info("paper_trading_stopped")


if __name__ == "__main__":
    asyncio.run(_main())
