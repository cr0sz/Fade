"""Discord webhook client for signal_edge_analyzer (replaces Telegram client).

All sends are best-effort: failures are logged but never propagate exceptions.
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any, Optional

import aiohttp

logger = logging.getLogger(__name__)

_WEBHOOK = os.getenv("DISCORD_WEBHOOK_ALERTS", "")


class DiscordClient:
    """Async Discord webhook notification client."""

    async def _post(self, message: str) -> bool:
        if not _WEBHOOK:
            logger.debug("discord_skipped: DISCORD_WEBHOOK_ALERTS not set")
            return False
        try:
            async with aiohttp.ClientSession() as s:
                async with s.post(
                    _WEBHOOK,
                    json={"content": message[:2000]},
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as r:
                    return r.status in (200, 204)
        except Exception as exc:
            logger.warning("discord_send_failed: %s", exc)
            return False

    async def send_message(self, text: str) -> bool:
        """Send a plain-text message. Returns True on success."""
        return await self._post(text)

    async def send_json(self, event: dict[str, Any]) -> bool:
        """Format a structured event dict and send it as a readable Discord message."""
        text = self._format_event(event)
        return await self.send_message(text)

    async def send_startup(self, service: str, port: int) -> None:
        await self._post(
            f"🟢 **{service}** started on port {port}\n"
            f"Time: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}"
        )

    async def send_signal_processed(
        self,
        signal_id: int,
        asset: Optional[str],
        window_h: int,
        return_val: Optional[float],
        expectancy: Optional[float],
    ) -> None:
        ret_str = f"+{return_val:.4f}" if return_val and return_val >= 0 else f"{return_val:.4f}"
        if expectancy is not None:
            await self._post(
                f"📡 **Edge Analyzer Update**\n"
                f"- Event: `signal_processed`\n"
                f"- Asset: `{asset or 'market-wide'}`\n"
                f"- Signal ID: `{signal_id}`\n"
                f"- Return ({window_h}h): `{ret_str}`\n"
                f"- Expectancy: `{expectancy:.4f}`"
            )
        else:
            await self._post(
                f"📡 **Edge Analyzer Update**\n"
                f"- Event: `signal_processed`\n"
                f"- Asset: `{asset or 'market-wide'}`\n"
                f"- Signal ID: `{signal_id}`"
            )

    async def send_metrics_updated(self, engine: str, sample_size: int, quality: float) -> None:
        await self._post(
            f"📊 **Metrics Updated**\n"
            f"- Engine: `{engine}`\n"
            f"- Samples: `{sample_size}`\n"
            f"- Quality Score: `{quality:.2f}`"
        )

    async def send_top_strategies(self, strategies: list[dict]) -> None:
        if not strategies:
            return
        lines = ["🏆 **Top 5 Strategies (Daily Report)**\n"]
        for i, s in enumerate(strategies[:5], 1):
            ticker = s.get("ticker") or "market"
            direction = s.get("direction") or "mixed"
            lines.append(
                f"{i}. `{s['engine_name']}` | {ticker} | {direction} | "
                f"{s['window_hours']}h — Q: `{s['quality_score']:.2f}` "
                f"WR: `{(s.get('win_rate') or 0)*100:.0f}%`"
            )
        await self._post("\n".join(lines)[:2000])

    async def send_health_degraded(self, reason: str, restart: bool = False) -> None:
        action = "Restarting service..." if restart else "Monitoring..."
        await self._post(
            f"⚠️ **System Health Degraded**\n"
            f"- Service: `signal-edge-analyzer`\n"
            f"- Reason: `{reason}`\n"
            f"- Action: {action}"
        )

    async def send_recovered(self, downtime_s: float) -> None:
        await self._post(
            f"✅ **Service Recovered**\n"
            f"- Service: `signal-edge-analyzer`\n"
            f"- Downtime: `{downtime_s:.0f}s`\n"
            f"- DB/Redis connections verified."
        )

    async def send_architecture_changed(self, old_hash: str, new_hash: str) -> None:
        await self._post(
            f"🔄 **Architecture Doc Changed**\n"
            f"- File: `architecture.md`\n"
            f"- Old hash: `{old_hash[:8]}`\n"
            f"- New hash: `{new_hash[:8]}`\n"
            f"Review for new services or schema changes."
        )

    async def send_error(self, context: str, error: str) -> None:
        await self._post(
            f"❌ **Error in Edge Analyzer**\n"
            f"- Context: `{context}`\n"
            f"- Error: `{error[:200]}`"
        )

    # ── Internal ──────────────────────────────────────────────────────────────

    @staticmethod
    def _format_event(event: dict[str, Any]) -> str:
        ts = event.get("timestamp", datetime.now(timezone.utc).isoformat())
        etype = event.get("event_type", "unknown")
        lines = [f"📡 Edge Analyzer | {etype} | {ts}"]
        skip = {"event_type", "timestamp"}
        for k, v in event.items():
            if k in skip or v is None:
                continue
            lines.append(f"- {k}: {v}")
        return "\n".join(lines)


# Backward-compat alias so existing callers using TelegramClient keep working
TelegramClient = DiscordClient
