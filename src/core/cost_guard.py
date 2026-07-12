from __future__ import annotations

"""Thin module-level interface over CostController.

Callers import these coroutines instead of wiring up CostController manually.
Redis is obtained lazily — safe to import at module load time.
"""

from src.cache.redis_client import get_redis
from src.core.cost_control import CostController


async def _ctrl() -> CostController:
    redis = await get_redis()
    return CostController(redis)


async def is_allowed() -> bool:
    """Return True if AI calls are within today's budget."""
    return await (await _ctrl()).is_ai_allowed()


async def record(provider: str, cost_usd: float) -> None:
    """Record an AI API call cost."""
    await (await _ctrl()).record_cost(provider, cost_usd)


async def get_status() -> dict:
    """Return the current budget usage summary."""
    return await (await _ctrl()).get_cost_summary()


async def preferred_provider() -> str:
    """Return 'openai', 'anthropic', or 'none' based on budget status."""
    return await (await _ctrl()).get_preferred_ai_provider()
