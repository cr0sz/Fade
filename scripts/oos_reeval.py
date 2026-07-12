#!/usr/bin/env python3
"""OOS guardrail re-evaluation script — intended for manual run on 2026-06-19.

Reads router:oos_stats from Redis, evaluates each segment against the Sprint 53
pass/fail criteria, prints a table, and (unless --dry-run) sends a Discord embed
to DISCORD_WEBHOOK_ALERTS.

Usage:
    python3 scripts/oos_reeval.py            # evaluate + send Discord
    python3 scripts/oos_reeval.py --dry-run  # print only, no Discord
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import redis.asyncio as aioredis

from src.core.discord import COLOR_GREEN, COLOR_RED, COLOR_WARN, send_embed

OOS_STATS_KEY = "router:oos_stats"
OOS_MIN_N_14D = 30
OOS_MIN_N_7D = 10


def _verdict(s14: dict, s7: dict | None) -> str:
    """Return ENABLE / DISABLE / HOLD for one segment."""
    n14 = s14.get("n", 0)
    avg14 = s14.get("avg_net", 0.0)

    if n14 < OOS_MIN_N_14D:
        # Not enough data to decide
        return "HOLD"

    if avg14 <= 0.0:
        return "DISABLE"

    # 14d passes; check 7d gate
    if s7 is not None and s7.get("n", 0) >= OOS_MIN_N_7D:
        if s7.get("avg_net", 0.0) <= 0.0:
            return "DISABLE"

    return "ENABLE"


def _color_for(v: str) -> int:
    return {
        "ENABLE": COLOR_GREEN,
        "DISABLE": COLOR_RED,
        "HOLD": COLOR_WARN,
    }.get(v, COLOR_WARN)


async def main(dry_run: bool = False) -> None:
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379")
    redis_password = os.getenv("REDIS_PASSWORD") or None

    client = aioredis.from_url(redis_url, password=redis_password, decode_responses=True)

    try:
        raw = await client.get(OOS_STATS_KEY)
    finally:
        await client.aclose()

    if not raw:
        print("oos_reeval: ERROR — router:oos_stats not found in Redis")
        sys.exit(1)

    data = json.loads(raw)
    windows = data.get("windows", {})
    w14 = windows.get("14d", {}) or {}
    w7 = windows.get("7d", {}) or {}
    computed_at = data.get("computed_at", "unknown")

    print(f"\nOOS Re-evaluation  |  stats computed_at: {computed_at}")
    print(f"{'Segment':<50} {'14d n':>6} {'14d avg':>9} {'7d n':>6} {'7d avg':>9} {'Verdict':>8}")
    print("-" * 100)

    results: list[tuple[str, str, dict, dict | None]] = []
    all_seg_keys = sorted(set(w14.keys()) | set(w7.keys()))

    for seg_key in all_seg_keys:
        s14 = w14.get(seg_key, {})
        s7 = w7.get(seg_key)
        v = _verdict(s14, s7)
        results.append((seg_key, v, s14, s7))

        n14 = s14.get("n", 0)
        avg14 = s14.get("avg_net", 0.0)
        n7 = s7.get("n", 0) if s7 else "-"
        avg7 = round(s7.get("avg_net", 0.0), 4) if s7 else "-"
        print(
            f"{seg_key:<50} {n14:>6} {round(avg14, 4):>9} {str(n7):>6} {str(avg7):>9} {v:>8}"
        )

    enable_count = sum(1 for _, v, _, _ in results if v == "ENABLE")
    disable_count = sum(1 for _, v, _, _ in results if v == "DISABLE")
    hold_count = sum(1 for _, v, _, _ in results if v == "HOLD")

    print(
        f"\nSummary: {enable_count} ENABLE  {disable_count} DISABLE  {hold_count} HOLD"
        f"  (total segments: {len(results)})"
    )

    overall_verdict = "ENABLE" if enable_count > 0 else ("HOLD" if hold_count > 0 else "DISABLE")
    overall_color = _color_for(overall_verdict)

    if dry_run:
        print("\n[dry-run] Discord notification skipped.")
        return

    webhook_url = os.getenv("DISCORD_WEBHOOK_ALERTS", "")
    if not webhook_url:
        print("oos_reeval: DISCORD_WEBHOOK_ALERTS not set — skipping Discord")
        return

    # Build Discord fields — one per segment (cap at 25 Discord embed field limit)
    fields = []
    for seg_key, v, s14, s7 in results[:25]:
        n14 = s14.get("n", 0)
        avg14 = round(s14.get("avg_net", 0.0), 4)
        n7_str = f"{s7.get('n', 0)}" if s7 else "—"
        avg7_str = f"{round(s7.get('avg_net', 0.0), 4)}" if s7 else "—"
        fields.append({
            "name": seg_key,
            "value": (
                f"14d: n={n14} avg={avg14}  |  7d: n={n7_str} avg={avg7_str}  →  **{v}**"
            ),
            "inline": False,
        })

    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    description = (
        f"**{enable_count} ENABLE  |  {disable_count} DISABLE  |  {hold_count} HOLD**\n"
        f"Stats computed at: {computed_at}\n"
        f"Re-eval run: {now_utc}"
    )

    await send_embed(
        webhook_url,
        title="OOS Guardrail Re-evaluation",
        description=description,
        color=overall_color,
        fields=fields or None,
    )
    print("oos_reeval: Discord embed sent.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="OOS guardrail re-evaluation (run 2026-06-19)")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print results only — do not send Discord notification",
    )
    args = parser.parse_args()
    asyncio.run(main(dry_run=args.dry_run))
