from __future__ import annotations

"""API fallback strategy: primary → secondary → RSS / static fallback.

Usage:
    result = await with_fallback(
        primary_call,
        secondary_call,
        rss_fallback,
        label="news_fetch",
    )

Never raises. Returns None if all strategies are exhausted.
"""

import asyncio
from typing import Any, Awaitable, Callable

from src.core.logging import get_logger

logger = get_logger(__name__)

_Strategy = Callable[[], Awaitable[Any]]


async def with_fallback(
    *strategies: _Strategy,
    label: str = "operation",
    timeout_sec: float | None = None,
) -> Any:
    """Try each async strategy in order; return first non-None result.

    Each strategy is a zero-argument coroutine factory (lambda or partial).
    Exceptions and None returns are treated as failures — move to next strategy.
    Never raises; returns None only when all strategies fail.
    """
    for i, strategy in enumerate(strategies):
        try:
            if timeout_sec:
                result = await asyncio.wait_for(strategy(), timeout=timeout_sec)
            else:
                result = await strategy()
            if result is not None:
                if i > 0:
                    logger.info("fallback_success", label=label, strategy_index=i)
                return result
        except asyncio.TimeoutError:
            logger.warning("fallback_timeout", label=label, strategy_index=i)
        except Exception as exc:
            logger.warning(
                "fallback_strategy_failed",
                label=label,
                strategy_index=i,
                error=str(exc),
            )

    logger.error("fallback_all_exhausted", label=label, strategies=len(strategies))
    return None


async def news_with_fallback(
    api_fetch: _Strategy,
    rss_fetch: _Strategy,
    label: str = "news",
) -> Any:
    """Convenience wrapper: API first, RSS as fallback."""
    return await with_fallback(api_fetch, rss_fetch, label=label)
