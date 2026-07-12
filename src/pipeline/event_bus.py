from __future__ import annotations

"""Lightweight in-memory async event dispatcher.

Events are published to a module-level asyncio.Queue and consumed by
dispatch_loop(). Handlers are registered per event name via subscribe().

No external dependencies — purely asyncio. Safe for multi-task environments.
"""

import asyncio
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable

from src.core.logging import get_logger

logger = get_logger(__name__)

Handler = Callable[["Event"], Awaitable[None]]


@dataclass
class Event:
    name: str
    data: dict[str, Any] = field(default_factory=dict)


# Module-level shared state — one queue and handler registry per process
_queue: asyncio.Queue[Event] = asyncio.Queue()
_handlers: dict[str, list[Handler]] = {}


def subscribe(event_name: str, handler: Handler) -> None:
    """Register an async handler for a named event."""
    _handlers.setdefault(event_name, []).append(handler)
    logger.debug("event_subscribed", event=event_name, handler=handler.__qualname__)


async def publish(event_name: str, data: dict[str, Any] | None = None) -> None:
    """Publish an event to the bus. Non-blocking — returns immediately."""
    await _queue.put(Event(name=event_name, data=data or {}))
    logger.debug("event_published", event=event_name)


async def dispatch_loop() -> None:
    """Consume events from the queue and dispatch to registered handlers.

    Runs indefinitely. Each handler is awaited sequentially per event.
    Handler exceptions are caught and logged — they never stop the loop.
    """
    logger.info("event_bus_started")
    while True:
        event = await _queue.get()
        handlers = _handlers.get(event.name, [])
        if not handlers:
            logger.debug("event_no_handlers", event=event.name)
            _queue.task_done()
            continue

        for handler in handlers:
            try:
                await handler(event)
            except Exception as exc:
                logger.error(
                    "event_handler_error",
                    event=event.name,
                    handler=handler.__qualname__,
                    error=str(exc),
                )
        _queue.task_done()


def queue_size() -> int:
    return _queue.qsize()
