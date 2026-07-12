"""Compute arbitration + notifications for the free-time researcher.

The researcher must YIELD to Ollama: the weekly LLM governance pipeline
(qwen2.5 7b+3b) is heavy on this 7.6GB host. Before every work unit we check
whether any model is loaded (GET /api/ps) and whether the host is overloaded
(1-min loadavg). If either, we pause.
"""
from __future__ import annotations

import os

import aiohttp

from src.core.logging import get_logger

logger = get_logger(__name__)

OLLAMA_URL = os.getenv("OLLAMA_BASE_URL", "http://ollama:11434")
LOAD_THRESHOLD = float(os.getenv("FREETIME_LOAD_THRESHOLD", "3.0"))


async def ollama_busy() -> bool:
    """True if any model is currently loaded in Ollama.

    On connection error we treat Ollama as IDLE (return False) so we never
    deadlock when the Ollama container is down — but we log it.
    """
    url = f"{OLLAMA_URL}/api/ps"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                url, timeout=aiohttp.ClientTimeout(total=2)
            ) as resp:
                if resp.status != 200:
                    logger.warning("ollama_ps_non200", status=resp.status)
                    return False
                data = await resp.json()
        models = data.get("models", []) if isinstance(data, dict) else []
        if models:
            logger.info(
                "ollama_loaded",
                names=[m.get("name") for m in models],
            )
            return True
        return False
    except Exception as e:
        # Treat unreachable Ollama as idle so we don't deadlock.
        logger.warning("ollama_ps_unreachable_treating_idle", error=str(e))
        return False


def load_high() -> bool:
    """True if the 1-min load average exceeds the threshold."""
    try:
        one_min = os.getloadavg()[0]
    except (OSError, AttributeError):
        return False
    if one_min > LOAD_THRESHOLD:
        logger.info("load_high", one_min=round(one_min, 2), threshold=LOAD_THRESHOLD)
        return True
    return False
