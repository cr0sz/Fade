"""Shared Discord webhook client for all src/ services."""
from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any

import aiohttp

from src.core.logging import get_logger

logger = get_logger(__name__)

WEBHOOK_ALERTS      = os.getenv("DISCORD_WEBHOOK_ALERTS", "")
WEBHOOK_DAILY_BRIEF = os.getenv("DISCORD_WEBHOOK_DAILY_BRIEF", "")
WEBHOOK_JOURNAL     = os.getenv("DISCORD_WEBHOOK_JOURNAL", "")
WEBHOOK_WATCHDOG    = os.getenv("DISCORD_WEBHOOK_WATCHDOG", "")

# Discord embed colors
COLOR_GREEN    = 3066993   # #2ECC71 — open long / recovery
COLOR_RED      = 15158332  # #E74C3C — open short / critical
COLOR_PROFIT   = 5763719   # #57F287 — closed profit
COLOR_LOSS     = 15548997  # #ED4245 — closed loss
COLOR_WARN     = 15105570  # #E67E22 — warning / high
COLOR_INFO     = 3447003   # #3498DB — info / digest
COLOR_PURPLE   = 10181046  # #9B59B6 — journal


async def _post(url: str, payload: dict) -> bool:
    if not url:
        logger.warning("discord_webhook_not_configured")
        return False
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status not in (200, 204):
                    body = await resp.text()
                    logger.error("discord_post_failed", status=resp.status, body=body[:200])
                    return False
                return True
    except Exception as exc:
        logger.error("discord_post_error", error=str(exc))
        return False


def _ts() -> str:
    return datetime.now(timezone.utc).isoformat()


async def send_embed(
    url: str,
    title: str,
    *,
    description: str = "",
    color: int = COLOR_INFO,
    fields: list[dict[str, Any]] | None = None,
) -> bool:
    embed: dict[str, Any] = {"title": title, "color": color, "timestamp": _ts()}
    if description:
        embed["description"] = description
    if fields:
        embed["fields"] = fields
    return await _post(url, {"embeds": [embed]})


async def send_text(url: str, content: str) -> bool:
    return await _post(url, {"content": content[:2000]})
