from __future__ import annotations

import logging
import sys
from datetime import datetime, timezone

import structlog

from .config import get_settings


def utc_now() -> datetime:
    """Return the current time as a timezone-aware UTC datetime."""
    return datetime.now(timezone.utc)


def configure_logging() -> None:
    settings = get_settings()
    log_level = getattr(logging, settings.LOG_LEVEL.upper(), logging.INFO)

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.stdlib.add_log_level,
            structlog.stdlib.add_logger_name,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.UnicodeDecoder(),
            structlog.processors.JSONRenderer() if not settings.DEBUG
            else structlog.dev.ConsoleRenderer(colors=True),
        ],
        wrapper_class=structlog.stdlib.BoundLogger,
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )

    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=log_level,
    )

    # Silence noisy libraries
    for noisy in ("httpx", "httpcore", "asyncio", "aiohttp"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


def bind_service_context(service: str) -> None:
    """Bind a service name into every structlog event for this process lifetime."""
    structlog.contextvars.bind_contextvars(service=service)


def bind_cycle_context(cycle_id: str, job: str) -> None:
    """Bind cycle_id and job to structlog contextvars for the current async task."""
    structlog.contextvars.bind_contextvars(cycle_id=cycle_id, job=job)


def clear_cycle_context() -> None:
    """Clear cycle-scoped context vars (call in finally after each job cycle)."""
    structlog.contextvars.unbind_contextvars("cycle_id", "job")


def get_logger(name: str) -> structlog.stdlib.BoundLogger:
    return structlog.get_logger(name)
