"""Discord push notifier for the paper trading engine (replaces Telegram)."""
from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

import aiohttp
import structlog

if TYPE_CHECKING:
    from models import PaperTrade

logger = structlog.get_logger(__name__)

# DISCORD_WEBHOOK_ENV lets a second sim container point at a different webhook
# without touching the live engine. Set DISCORD_WEBHOOK_ENV=DISCORD_WEBHOOK_PAPER_TRADES
# in the sim container; live container leaves it unset (defaults to DISCORD_WEBHOOK_TRADES).
_WEBHOOK_ENV = os.getenv("DISCORD_WEBHOOK_ENV", "DISCORD_WEBHOOK_TRADES")
_WEBHOOK = os.getenv(_WEBHOOK_ENV, "")
_WEBHOOK_PAPER = os.getenv("DISCORD_WEBHOOK_PAPER_TRADES", "")

# embed colors
_GREEN  = 3066993   # long open / recovery
_RED    = 15158332  # short open / critical
_PROFIT = 5763719   # closed profit
_LOSS   = 15548997  # closed loss
_WARN   = 15105570  # stop / halt


async def _post(payload: dict, webhook: str = "") -> None:
    url = webhook or _WEBHOOK
    if not url:
        logger.warning("discord_trades_webhook_not_set")
        return
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=8)) as resp:
                if resp.status not in (200, 204):
                    body = await resp.text()
                    logger.warning("discord_post_failed", status=resp.status, body=body[:200])
    except Exception as exc:
        logger.warning("discord_post_error", error=str(exc))


def _embed(title: str, color: int, fields: list[dict[str, Any]], description: str = "") -> dict:
    e: dict[str, Any] = {
        "title": title,
        "color": color,
        "fields": fields,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    if description:
        e["description"] = description
    return {"embeds": [e]}


def _field(name: str, value: str, inline: bool = True) -> dict:
    return {"name": name, "value": value, "inline": inline}


class DiscordNotifier:
    def __init__(self) -> None:
        self._enabled = bool(_WEBHOOK)
        if not self._enabled:
            logger.warning("discord_notifier_disabled", hint="set DISCORD_WEBHOOK_TRADES")
        self._webhook_live = _WEBHOOK
        self._webhook_paper = _WEBHOOK_PAPER or _WEBHOOK
        if _WEBHOOK_PAPER:
            logger.info("discord_notifier_dual_webhook", paper="DISCORD_WEBHOOK_PAPER_TRADES")

    def _trade_webhook(self, trade: "PaperTrade") -> str:
        return self._webhook_live if getattr(trade, "is_live", False) else self._webhook_paper

    async def trade_opened(self, trade: "PaperTrade") -> None:
        if not self._enabled:
            return
        is_live = getattr(trade, "is_live", False)
        is_long = trade.direction == "long"
        color = _GREEN if is_long else _RED
        mode_tag = "⚡ LIVE" if is_live else "paper"
        direction_tag = "🟢 LONG" if is_long else "🔴 SHORT"

        fields = [
            _field("Asset", trade.asset),
            _field("Direction", direction_tag),
            _field("Entry", f"${trade.entry_price:.4f}"),
            _field("Notional", f"${trade.notional:.2f}"),
            _field("Exchange", trade.exchange or "unknown"),
            _field("Mode", mode_tag),
        ]
        router_action = getattr(trade, "router_action", "")
        if router_action:
            edge = getattr(trade, "router_edge", 0.0)
            tag = "FADE (inverted)" if router_action == "invert" else router_action
            fields.append(_field("Router", f"{tag} · edge {edge:+.3f}%", inline=False))

        await _post(_embed(f"Trade Opened — {trade.asset}", color, fields), self._trade_webhook(trade))

    async def trade_closed(self, trade: "PaperTrade", reason: str) -> None:
        if not self._enabled:
            return
        pnl = trade.pnl or 0.0
        color = _PROFIT if pnl >= 0 else _LOSS
        emoji = "✅" if pnl >= 0 else "❌"
        held_h = 0.0
        if trade.opened_at and trade.closed_at:
            held_h = round((trade.closed_at - trade.opened_at).total_seconds() / 3600, 1)

        ret_pct = 0.0
        if trade.notional and trade.notional > 0:
            ret_pct = (pnl / trade.notional) * 100

        fields = [
            _field("Asset", trade.asset),
            _field("Direction", trade.direction.upper()),
            _field("PnL", f"{pnl:+.2f} USD ({ret_pct:+.1f}%)"),
            _field("Entry → Exit", f"${trade.entry_price:.4f} → ${(trade.exit_price or 0):.4f}"),
            _field("Hold", f"{held_h}h"),
            _field("Closed", trade.closed_at.strftime("%m-%d %H:%M UTC") if trade.closed_at else "—"),
            _field("Reason", reason),
        ]
        await _post(_embed(f"{emoji} Trade Closed — {trade.asset}", color, fields), self._trade_webhook(trade))

    async def stop_loss_fired(self, trade: "PaperTrade", current_price: float, loss_pct: float) -> None:
        if not self._enabled:
            return
        fields = [
            _field("Asset", trade.asset),
            _field("Direction", trade.direction.upper()),
            _field("Entry", f"${trade.entry_price:.4f}"),
            _field("Current", f"${current_price:.4f}"),
            _field("Loss", f"{loss_pct*100:.2f}%"),
        ]
        await _post(_embed("🛑 Stop-Loss Fired", _LOSS, fields), self._trade_webhook(trade))

    async def kill_switch_changed(self, active: bool) -> None:
        if not self._enabled:
            return
        if active:
            await _post(_embed("🛑 Kill Switch ACTIVATED", _LOSS, [], "All new orders halted."))
        else:
            await _post(_embed("✅ Kill Switch DEACTIVATED", _GREEN, [], "Trading resumed."))

    async def daily_loss_halt(self, amount: float, limit: float) -> None:
        if not self._enabled:
            return
        fields = [
            _field("Loss Today", f"${amount:.2f}"),
            _field("Limit", f"${limit:.2f}"),
        ]
        await _post(_embed("⚠️ Daily Loss Limit Hit — Orders Halted", _WARN, fields,
                            "All new orders halted until tomorrow UTC."))

    async def live_trade_opened(self, asset: str, direction: str, notional: float, fill_price: float) -> None:
        if not self._enabled:
            return
        color = _GREEN if direction == "long" else _RED
        arrow = "🟢 LONG" if direction == "long" else "🔴 SHORT"
        fields = [
            _field("Asset", asset),
            _field("Direction", arrow),
            _field("Notional", f"${notional:.2f}"),
            _field("Fill", f"${fill_price:.4f}"),
        ]
        await _post(_embed("⚡ LIVE Trade Opened", color, fields))

    async def send_message(self, text: str) -> None:
        if self._enabled:
            await _post({"content": text[:2000]})
