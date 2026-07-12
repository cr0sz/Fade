from __future__ import annotations

"""Pipeline task handlers consumed by the event worker.

Each handler is an async function that accepts an Event and performs a
single unit of work. Handlers must be resilient — they must NEVER crash
the pipeline. All exceptions are caught internally.

Integration points:
  fetch_news()      → delegates to NewsIntelligenceEngine when db is available
  generate_signals()→ uses core/llm.py for AI signal summarisation
  score_assets()    → placeholder for risk scoring engine hook
"""

from src.core.logging import get_logger
from src.pipeline.event_bus import Event

logger = get_logger(__name__)


async def fetch_news(event: Event) -> None:
    """Handle news.ingest events — trigger a lightweight news fetch."""
    logger.info("task_start", task="fetch_news", data=event.data)
    try:
        # Integration point: instantiate NewsIntelligenceEngine here when
        # db_pool is passed via event.data or injected at startup.
        tickers = event.data.get("tickers", [])
        logger.info("task_complete", task="fetch_news", tickers=len(tickers))
    except Exception as exc:
        logger.error("task_error", task="fetch_news", error=str(exc))


async def generate_signals(event: Event) -> None:
    """Handle signal.generate events — produce LLM-based signal summaries."""
    logger.info("task_start", task="generate_signals", data=event.data)
    try:
        from src.core.llm import ask_llm

        context = event.data.get("context", "No context provided.")
        ticker = event.data.get("ticker", "market")
        prompt = (
            f"Briefly assess the current signal strength for {ticker} "
            f"based on: {context}. One concise sentence, no price targets."
        )
        summary = await ask_llm("signal", prompt, ttl=300)
        logger.info("task_complete", task="generate_signals", ticker=ticker, summary=summary[:80])
    except Exception as exc:
        logger.error("task_error", task="generate_signals", error=str(exc))


async def score_assets(event: Event) -> None:
    """Handle risk.evaluate events — compute asset risk scores."""
    logger.info("task_start", task="score_assets", data=event.data)
    try:
        tickers = event.data.get("tickers", [])
        # Integration point: hook into confidence_engine or signal_scoring_engine
        logger.info("task_complete", task="score_assets", scored=len(tickers))
    except Exception as exc:
        logger.error("task_error", task="score_assets", error=str(exc))
