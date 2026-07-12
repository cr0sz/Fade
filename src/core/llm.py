from __future__ import annotations

"""Single OpenAI interface for all internal callers.

ask_llm(task_type, prompt) — model is selected automatically via model_router,
result is cached, budget is guarded. Never raises; returns a degradation message
if AI is unavailable.

Task types:
  news / rss / summary / ingestion → gpt-4o-mini  (cheap, fast)
  signal / trading / risk / decision → gpt-4o      (capable, expensive)
"""

import hashlib
import os
from typing import Any

from src.core.cache import get_cache, set_cache
from src.core.cost_guard import is_allowed, record
from src.core.logging import get_logger
from src.core.model_router import route_model
from src.providers.ai.openai_provider import OpenAIProvider

logger = get_logger(__name__)

_BUDGET_MSG = "⚠️ AI analysis temporarily unavailable (daily budget reached). Try again tomorrow."
_ERROR_MSG = "⚠️ AI analysis unavailable."


def _default_ttl(task_type: str) -> int:
    critical_types = {"signal", "trading", "risk", "decision"}
    if task_type.lower() in critical_types:
        return int(os.getenv("CACHE_TTL_SECONDS", "300"))
    return 3600  # digest-type tasks cache for 1 hour


async def ask_llm(
    task_type: str,
    prompt: str,
    ttl: int | None = None,
    max_tokens: int | None = None,
    system: str | None = None,
) -> str:
    """Route prompt to correct model, check budget, cache and return response.

    Args:
        task_type: Routing key — determines model selection (see module docstring).
        prompt: The user prompt to send.
        ttl: Cache TTL in seconds. Defaults based on task criticality.
        max_tokens: Override max tokens (defaults from env MAX_OPENAI_TOKENS).
        system: Optional system prompt override.

    Returns:
        LLM response string, or a graceful degradation message on failure.
    """
    if not await is_allowed():
        return _BUDGET_MSG

    cache_key = f"llm:{task_type}:{hashlib.sha256(prompt.encode()).hexdigest()[:20]}"
    cached = await get_cache(cache_key)
    if cached:
        logger.debug("llm_cache_hit", task_type=task_type, key=cache_key)
        return cached if isinstance(cached, str) else str(cached)

    model = route_model(task_type)
    max_tokens = max_tokens or int(os.getenv("MAX_OPENAI_TOKENS", "1200"))
    effective_ttl = ttl if ttl is not None else _default_ttl(task_type)

    provider = OpenAIProvider()
    provider._model = model

    kwargs: dict[str, Any] = {"prompt": prompt, "max_tokens": max_tokens}
    if system:
        kwargs["system"] = system

    try:
        resp = await provider.complete(**kwargs)
        await record("openai", resp.cost_usd)
        await set_cache(cache_key, resp.content, ttl=effective_ttl)
        logger.info(
            "llm_response",
            task_type=task_type,
            model=model,
            tokens_in=resp.input_tokens,
            tokens_out=resp.output_tokens,
            cost_usd=resp.cost_usd,
        )
        return resp.content
    except Exception as exc:
        logger.error("llm_error", task_type=task_type, model=model, error=str(exc))
        return _ERROR_MSG
