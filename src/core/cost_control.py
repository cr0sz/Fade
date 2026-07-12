from __future__ import annotations

from datetime import date
from enum import Enum

import redis.asyncio as aioredis

from .config import get_settings
from .logging import get_logger

logger = get_logger(__name__)


class ProviderTier(str, Enum):
    PRIMARY = "primary"
    FALLBACK = "fallback"
    TERTIARY = "tertiary"


class BudgetStatus(str, Enum):
    OK = "ok"
    WARNING = "warning"
    FALLBACK = "fallback"
    HARD_LIMIT = "hard_limit"


class CostController:
    def __init__(self, redis_client: aioredis.Redis) -> None:
        self._redis = redis_client
        self._settings = get_settings()

    def _daily_key(self) -> str:
        return f"cost:daily:{date.today().isoformat()}"

    def _provider_key(self, provider: str) -> str:
        return f"cost:provider:{provider}:{date.today().isoformat()}"

    async def record_cost(self, provider: str, cost_usd: float) -> None:
        daily_key = self._daily_key()
        provider_key = self._provider_key(provider)

        cost_micros = int(cost_usd * 1_000_000)
        pipe = self._redis.pipeline()
        pipe.incrby(daily_key, cost_micros)
        pipe.expire(daily_key, 86400 * 2)
        pipe.incrby(provider_key, cost_micros)
        pipe.expire(provider_key, 86400 * 2)
        await pipe.execute()

        logger.debug("cost_recorded", provider=provider, cost_usd=cost_usd)

    async def get_daily_total(self) -> float:
        raw = await self._redis.get(self._daily_key())
        return int(raw) / 1_000_000 if raw else 0.0

    async def get_budget_status(self) -> BudgetStatus:
        total = await self.get_daily_total()
        budget = self._settings.DAILY_BUDGET_USD
        ratio = total / budget if budget > 0 else 0.0

        if ratio >= self._settings.BUDGET_HARD_LIMIT_THRESHOLD:
            return BudgetStatus.HARD_LIMIT
        if ratio >= self._settings.BUDGET_FALLBACK_THRESHOLD:
            return BudgetStatus.FALLBACK
        if ratio >= self._settings.BUDGET_WARNING_THRESHOLD:
            return BudgetStatus.WARNING
        return BudgetStatus.OK

    async def get_preferred_ai_provider(self) -> str:
        # Ollama is local/free — always preferred over paid APIs for news summarization
        return "ollama"

    async def is_ai_allowed(self) -> bool:
        status = await self.get_budget_status()
        allowed = status != BudgetStatus.HARD_LIMIT
        if not allowed:
            logger.warning(
                "ai_blocked_budget",
                daily_total=await self.get_daily_total(),
                budget=self._settings.DAILY_BUDGET_USD,
            )
        return allowed

    async def get_cost_summary(self) -> dict:
        total = await self.get_daily_total()
        budget = self._settings.DAILY_BUDGET_USD
        return {
            "daily_total_usd": round(total, 4),
            "daily_budget_usd": budget,
            "used_pct": round(total / budget * 100, 1) if budget else 0,
            "remaining_usd": round(max(0.0, budget - total), 4),
            "status": (await self.get_budget_status()).value,
        }


_cost_controller: CostController | None = None


def get_cost_controller() -> CostController:
    if _cost_controller is None:
        raise RuntimeError("CostController not initialized — call init_cost_controller() first")
    return _cost_controller


async def init_cost_controller(redis_client: aioredis.Redis) -> CostController:
    global _cost_controller
    _cost_controller = CostController(redis_client)
    return _cost_controller
