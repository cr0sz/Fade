from __future__ import annotations

"""Async pipeline worker — subscribes to events and dispatches task handlers.

Runs as a background asyncio task alongside the main scheduler worker.
Uses only asyncio + in-memory queue. No Kafka, Celery, or Redis Streams.

Lifecycle:
  1. Call start_pipeline_worker() once at application startup.
  2. Publish events with event_bus.publish("news.ingest", {...}).
  3. The dispatch_loop() runs indefinitely until the process exits.

Supported events:
  news.ingest      → tasks.fetch_news
  signal.generate  → tasks.generate_signals
  risk.evaluate    → tasks.score_assets
"""

import asyncio

from src.core.logging import get_logger
from src.pipeline import tasks
from src.pipeline.event_bus import dispatch_loop, publish, subscribe

logger = get_logger(__name__)

_EVENT_HANDLERS = {
    "news.ingest": tasks.fetch_news,
    "signal.generate": tasks.generate_signals,
    "risk.evaluate": tasks.score_assets,
}


async def start_pipeline_worker() -> asyncio.Task:
    """Register all handlers and start the event dispatch loop as a background task."""
    for event_name, handler in _EVENT_HANDLERS.items():
        subscribe(event_name, handler)

    logger.info("pipeline_worker_starting", events=list(_EVENT_HANDLERS.keys()))
    task = asyncio.create_task(dispatch_loop(), name="pipeline_dispatch_loop")
    return task


async def trigger(event_name: str, data: dict | None = None) -> None:
    """Convenience: publish an event to the pipeline bus."""
    await publish(event_name, data or {})


async def _demo() -> None:
    """Smoke-test: publish one event of each type and let the loop drain."""
    worker_task = await start_pipeline_worker()

    await trigger("news.ingest", {"tickers": ["AAPL", "MSFT"]})
    await trigger("signal.generate", {"ticker": "AAPL", "context": "Strong earnings beat"})
    await trigger("risk.evaluate", {"tickers": ["NVDA"]})

    await asyncio.sleep(1)
    worker_task.cancel()


if __name__ == "__main__":
    asyncio.run(_demo())
