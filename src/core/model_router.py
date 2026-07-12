from __future__ import annotations

import os

# Tasks that warrant the more capable reasoning model
_CRITICAL_TASKS = {"signal", "trading", "risk", "decision"}

# Tasks that use the cheaper default model
_DEFAULT_TASKS = {"news", "rss", "summary", "ingestion"}


def route_model(task_type: str) -> str:
    """Return the OpenAI model name for a given task type.

    Critical tasks (signal generation, risk, trading) use MODEL_ROUTER_CRITICAL.
    All others use MODEL_ROUTER_DEFAULT.
    """
    enabled = os.getenv("MODEL_ROUTER_ENABLED", "true").lower() == "true"
    default = os.getenv("MODEL_ROUTER_DEFAULT", "gpt-4o-mini")
    critical = os.getenv("MODEL_ROUTER_CRITICAL", "gpt-4o")

    if not enabled:
        return default

    return critical if task_type.lower() in _CRITICAL_TASKS else default


def is_critical_task(task_type: str) -> bool:
    return task_type.lower() in _CRITICAL_TASKS
