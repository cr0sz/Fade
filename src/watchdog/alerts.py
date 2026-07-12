"""Alert delivery for the infra watchdog — posts to Discord #watchdog channel.

Each *check* (a stable string key, e.g. "containers", "funding_freshness") has:
  - a Discord embed with a per-check Redis cooldown (60 min),
  - a RECOVERY message when a previously-alerting check goes green,
  - a row in validation_alerts (alert_type='infra_'+check) so the dashboard
    surfaces it, matching that table's schema and chk_alert_type constraint
    (migration 025 adds the infra_* types).

Redis keys:
  watchdog:alert:cooldown:<check>   TTL 3600s — suppresses re-alert
  watchdog:alert:active:<check>     set while a check is failing — drives RECOVERY
"""
from __future__ import annotations

import json
from datetime import datetime, timezone

from src.core.discord import COLOR_INFO, COLOR_LOSS, COLOR_GREEN, COLOR_WARN, send_embed
from src.core.discord import WEBHOOK_WATCHDOG
from src.core.logging import get_logger

logger = get_logger(__name__)

COOLDOWN_SEC = 3600
_COOLDOWN_KEY = "watchdog:alert:cooldown:{check}"
_ACTIVE_KEY   = "watchdog:alert:active:{check}"

_SEVERITY_EMOJI = {"critical": "🚨", "high": "⚠️", "medium": "🟡", "recovery": "✅"}
_SEVERITY_COLOR = {
    "critical": COLOR_LOSS,
    "high":     COLOR_WARN,
    "medium":   COLOR_INFO,
    "recovery": COLOR_GREEN,
}

_SEVERITY = {
    "containers":          "critical",
    "api_health":          "critical",
    "funding_freshness":   "high",
    "quote_freshness":     "high",
    "liq_stream":          "high",
}


def severity_for(check: str) -> str:
    return _SEVERITY.get(check, "high")


async def send_discord(message: str, severity: str = "high") -> bool:
    """Send a plain-text Discord message to #watchdog. Returns True on success."""
    color = _SEVERITY_COLOR.get(severity, COLOR_WARN)
    emoji = _SEVERITY_EMOJI.get(severity, "⚠️")
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    return await send_embed(
        WEBHOOK_WATCHDOG,
        f"{emoji} Watchdog — {severity.upper()}",
        description=f"{message}\n`{ts}`",
        color=color,
    )


# Backward-compat alias used by digest.py
send_telegram = send_discord


async def _insert_validation_alert(db, check: str, severity: str, detail: str) -> None:
    alert_type = f"infra_{check}"
    try:
        async with db.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO validation_alerts
                    (alert_type, severity, engine_name, details, fired_at, is_active)
                VALUES ($1, $2, $3, $4, NOW(), TRUE)
                """,
                alert_type,
                severity,
                None,
                json.dumps({"check": check, "detail": detail}),
            )
    except Exception as e:
        logger.error("validation_alert_insert_failed", check=check, error=str(e))


async def fire(redis, db, check: str, detail: str) -> None:
    severity = severity_for(check)
    await redis.set(_ACTIVE_KEY.format(check=check), detail)

    cooldown_key = _COOLDOWN_KEY.format(check=check)
    if await redis.exists(cooldown_key):
        logger.info("alert_suppressed_cooldown", check=check)
        return

    await _insert_validation_alert(db, check, severity, detail)
    emoji = _SEVERITY_EMOJI.get(severity, "⚠️")
    color = _SEVERITY_COLOR.get(severity, COLOR_WARN)
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    sent = await send_embed(
        WEBHOOK_WATCHDOG,
        f"{emoji} {severity.upper()} | infra: {check}",
        description=f"{detail}\n`{ts}`\n*infra watchdog — system health only*",
        color=color,
    )
    await redis.set(cooldown_key, "1", ex=COOLDOWN_SEC)
    logger.warning("infra_alert_fired", check=check, detail=detail, discord_sent=sent)


async def _resolve_validation_alerts(db, check: str) -> None:
    alert_type = f"infra_{check}"
    try:
        async with db.acquire() as conn:
            await conn.execute(
                """
                UPDATE validation_alerts
                   SET is_active = FALSE, resolved_at = NOW()
                 WHERE alert_type = $1 AND is_active = TRUE
                """,
                alert_type,
            )
    except Exception as e:
        logger.error("validation_alert_resolve_failed", check=check, error=str(e))


async def clear(redis, db, check: str) -> None:
    active_key = _ACTIVE_KEY.format(check=check)
    was_active = await redis.get(active_key)
    if was_active is None:
        return
    await redis.delete(active_key)
    await redis.delete(_COOLDOWN_KEY.format(check=check))
    await _resolve_validation_alerts(db, check)
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    sent = await send_embed(
        WEBHOOK_WATCHDOG,
        f"✅ RECOVERED | infra: {check}",
        description=f"was: {was_active}\n`{ts}`",
        color=COLOR_GREEN,
    )
    logger.info("infra_alert_recovered", check=check, discord_sent=sent)
