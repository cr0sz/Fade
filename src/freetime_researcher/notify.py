"""Discord notification for promising fade-sweep finds. RECOMMEND-ONLY."""
from __future__ import annotations

from src.core.discord import COLOR_INFO, WEBHOOK_ALERTS, send_embed
from src.core.logging import get_logger

logger = get_logger(__name__)


async def send_telegram(message: str) -> None:
    """Send a researcher find to Discord #alerts. Name kept for call-site compat."""
    await send_embed(
        WEBHOOK_ALERTS,
        "🔬 Researcher — Promising Fade Combo",
        description=message[:4000],
        color=COLOR_INFO,
    )


def format_find(params: dict, row: dict) -> str:
    return (
        "**RECOMMEND-ONLY — live config untouched.**\n\n"
        f"window={params['drop_window_h']}h  "
        f"drop≤{params['drop_threshold']:.0%}  "
        f"cond={params['panic_cond']}  hold={params['hold_h']}h\n\n"
        f"OOS: n={row['oos_n']}  avg={row['oos_avg_ret_pct']:+.2f}%\n"
        f"IS:  n={row['n']}  WR={row['win_rate']:.0%}  "
        f"avg={row['avg_ret_pct']:+.2f}%  sharpe={row['sharpe']:.2f}  "
        f"maxDD={row['max_dd_pct']:.1f}%"
    )
