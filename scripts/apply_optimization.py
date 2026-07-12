"""Apply a winning parameter set from optimization_runs to .env.

Usage:
    python scripts/apply_optimization.py <run_id>
    python scripts/apply_optimization.py <run_id> --dry-run

Reads the optimization_runs row by run_id, writes the winning params to .env
(only whitelisted keys), sends a Telegram confirmation, and prints the diff.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import sys

import asyncpg

DB_DSN = os.getenv(
    "DATABASE_URL",
    f"postgresql://{os.getenv('POSTGRES_USER','postgres')}:{os.getenv('POSTGRES_PASSWORD','postgres')}"
    f"@{os.getenv('POSTGRES_HOST','localhost')}:{os.getenv('POSTGRES_PORT','5432')}"
    f"/{os.getenv('POSTGRES_DB','market_intelligence')}",
)

DISCORD_WEBHOOK = os.getenv("DISCORD_WEBHOOK_ALERTS", "")

ENV_FILE = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env")

# Mapping: optimization_runs.params key → .env key
PARAM_TO_ENV: dict[str, str] = {
    "short_threshold":    "SCORE_SHORT_THRESHOLD",
    "trail_stop_pct":     "TRAIL_STOP_PCT",
    "trail_activate_pct": "TRAIL_ACTIVATE_PCT",
}


def _read_env(path: str) -> list[str]:
    try:
        with open(path) as f:
            return f.readlines()
    except FileNotFoundError:
        return []


def _set_env_key(lines: list[str], key: str, value: str) -> list[str]:
    pattern = re.compile(rf"^{re.escape(key)}\s*=")
    new_line = f"{key}={value}\n"
    for i, line in enumerate(lines):
        if pattern.match(line):
            lines[i] = new_line
            return lines
    lines.append(new_line)
    return lines


async def _send_discord(message: str) -> None:
    if not DISCORD_WEBHOOK:
        return
    try:
        import aiohttp
        async with aiohttp.ClientSession() as s:
            await s.post(DISCORD_WEBHOOK, json={"content": message[:2000]}, timeout=aiohttp.ClientTimeout(total=10))
    except Exception as exc:
        print(f"discord_send_error: {exc}")


async def apply(run_id: int, dry_run: bool = False) -> None:
    pool = await asyncpg.create_pool(DB_DSN, min_size=1, max_size=2)
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT id, params, score, win_rate, sharpe, notes, created_at FROM optimization_runs WHERE id = $1",
            run_id,
        )
    await pool.close()

    if not row:
        print(f"Error: optimization_runs run_id={run_id} not found.")
        sys.exit(1)

    params = json.loads(row["params"])
    print(f"Run ID {run_id} | Score: {row['score']:.4f} | Created: {row['created_at']}")
    print(f"Notes: {row['notes']}")
    print(f"Params: {params}\n")

    env_lines = _read_env(ENV_FILE)
    diff: list[str] = []

    for param_key, env_key in PARAM_TO_ENV.items():
        if param_key not in params:
            continue
        new_val = str(params[param_key])
        # Find current value
        pattern = re.compile(rf"^{re.escape(env_key)}\s*=\s*(.*)$")
        current_val = None
        for line in env_lines:
            m = pattern.match(line.strip())
            if m:
                current_val = m.group(1).strip()
                break
        if current_val != new_val:
            diff.append(f"  {env_key}: {current_val or '(not set)'} → {new_val}")
        env_lines = _set_env_key(env_lines, env_key, new_val)

    if not diff:
        print("No changes to apply (params already match .env).")
        return

    print("Changes to apply:")
    for d in diff:
        print(d)

    if dry_run:
        print("\n[dry-run] Skipping .env write and Telegram notification.")
        return

    with open(ENV_FILE, "w") as f:
        f.writelines(env_lines)
    print(f"\nWritten to {ENV_FILE}")

    diff_str = "\n".join(diff)
    message = (
        f"✅ *Optimization Applied*\n\n"
        f"Run ID: `{run_id}` | Score: `{row['score']:.4f}`\n"
        f"WR: `{(row['win_rate'] or 0):.1%}` | Sharpe: `{(row['sharpe'] or 0):.2f}`\n\n"
        f"*Changes:*\n{diff_str}\n\n"
        f"Restart the trading engine to pick up the new params."
    )
    await _send_discord(message)
    print("Discord confirmation sent.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Apply winning optimization run to .env")
    parser.add_argument("run_id", type=int, help="Row ID from optimization_runs table")
    parser.add_argument("--dry-run", action="store_true", help="Show diff without writing")
    args = parser.parse_args()
    asyncio.run(apply(args.run_id, dry_run=args.dry_run))


if __name__ == "__main__":
    main()
