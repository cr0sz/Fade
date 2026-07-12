"""Discord status push — DB-only, standalone aiohttp (no src/ imports needed)."""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone

import aiohttp

logger = logging.getLogger(__name__)

WEBHOOK_ALERTS = os.getenv("DISCORD_WEBHOOK_ALERTS", "")
COLOR_INFO = 3447003
COLOR_WARN = 15105570


async def _send_embed(url: str, title: str, fields: list[dict], color: int = COLOR_INFO) -> bool:
    if not url:
        return False
    payload = {"embeds": [{"title": title, "color": color, "fields": fields,
                            "timestamp": datetime.now(timezone.utc).isoformat()}]}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                return resp.status in (200, 204)
    except Exception as exc:
        logger.warning("discord_status_post_failed: %s", exc)
        return False


async def build_status_fields(db_pool) -> list[dict]:
    """Query DB for current status. Returns list of Discord embed fields."""
    fields = []
    async with db_pool.acquire() as conn:
        open_rows = await conn.fetch(
            "SELECT asset, direction, notional, opened_at, mode "
            "FROM paper_trades WHERE closed_at IS NULL AND mode != 'sim' ORDER BY opened_at DESC LIMIT 10"
        )
        if open_rows:
            pos_lines = []
            for r in open_rows:
                age_h = (datetime.now(timezone.utc) - r["opened_at"].replace(tzinfo=timezone.utc)).total_seconds() / 3600
                tag = "[L]" if r["mode"] == "live" else ""
                pos_lines.append(f"{r['asset']} {r['direction'].upper()} ${r['notional']:.0f} ({age_h:.1f}h){tag}")
            fields.append({"name": f"📈 Open ({len(open_rows)})", "value": "\n".join(pos_lines), "inline": False})
        else:
            fields.append({"name": "📈 Open Positions", "value": "None", "inline": True})

        today_pnl = await conn.fetchval(
            "SELECT COALESCE(SUM(pnl), 0) FROM paper_trades WHERE closed_at > NOW() - INTERVAL '24 hours' AND mode != 'sim'"
        )
        week_pnl = await conn.fetchval(
            "SELECT COALESCE(SUM(pnl), 0) FROM paper_trades WHERE closed_at > NOW() - INTERVAL '7 days' AND mode != 'sim'"
        )
        week_trades = await conn.fetchval(
            "SELECT COUNT(*) FROM paper_trades WHERE closed_at > NOW() - INTERVAL '7 days' AND mode != 'sim'"
        )
        week_wins = await conn.fetchval(
            "SELECT COUNT(*) FROM paper_trades WHERE closed_at > NOW() - INTERVAL '7 days' AND pnl > 0 AND mode != 'sim'"
        )
        wr = f"{week_wins/week_trades*100:.0f}%" if week_trades else "—"
        fields.append({
            "name": "💰 P&L",
            "value": f"24h: ${float(today_pnl):.2f}\n7d: ${float(week_pnl):.2f}\n7d WR: {wr} ({week_trades} trades)",
            "inline": True,
        })

        snap = await conn.fetchrow(
            "SELECT balance, drawdown_pct, governor_state, regime "
            "FROM equity_snapshots ORDER BY captured_at DESC LIMIT 1"
        )
        if snap:
            fields.append({
                "name": "🏦 Account",
                "value": (
                    f"Balance: ${float(snap['balance']):.2f}\n"
                    f"Drawdown: {float(snap['drawdown_pct'])*100:.1f}%\n"
                    f"Governor: {snap['governor_state']}\n"
                    f"Regime: {snap['regime'] or 'unknown'}"
                ),
                "inline": True,
            })

    return fields


async def push_status(db_pool) -> bool:
    """Build and push status embed to Discord #alerts. Returns True on success."""
    if not WEBHOOK_ALERTS:
        logger.warning("discord_status: DISCORD_WEBHOOK_ALERTS not set")
        return False
    try:
        fields = await build_status_fields(db_pool)
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        return await _send_embed(WEBHOOK_ALERTS, f"📊 Status — {now}", fields, COLOR_INFO)
    except Exception as exc:
        logger.warning("discord_status_push_failed: %s", exc)
        return False
