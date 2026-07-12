"""Weekly parameter sweep — Pillar 5 of the self-optimizing system.

Grid-searches key trading parameters against the last 30 days of closed trades,
scores each combination by win_rate × sharpe × expectancy, and writes the best
result to the optimization_runs table. If the best run improves by > 10% over
the current baseline, a Telegram recommendation is sent.

Usage:
    python param_sweep.py              # full sweep
    python param_sweep.py --dry-run    # run sweep but don't write to DB or send Telegram
    python param_sweep.py --days 14    # use last 14 days instead of 30
"""
from __future__ import annotations

import argparse
import asyncio
import json
import math
import os
import sys
from itertools import product
from typing import Any

import asyncpg

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

DB_DSN = os.getenv(
    "DATABASE_URL",
    f"postgresql://{os.getenv('POSTGRES_USER','postgres')}:{os.getenv('POSTGRES_PASSWORD','postgres')}"
    f"@{os.getenv('POSTGRES_HOST','localhost')}:{os.getenv('POSTGRES_PORT','5432')}"
    f"/{os.getenv('POSTGRES_DB','market_intelligence')}",
)

DISCORD_WEBHOOK = os.getenv("DISCORD_WEBHOOK_ALERTS", "")

# Parameter grid to search
SWEEP_GRID: dict[str, list[Any]] = {
    "short_threshold":    [-0.10, -0.13, -0.15, -0.18, -0.22],
    "trail_stop_pct":     [1.0, 1.5, 2.0],
    "trail_activate_pct": [0.3, 0.5, 0.7],
}

# Minimum trades required for a valid evaluation
MIN_TRADES = 10
# Improvement threshold before recommendation is sent
IMPROVEMENT_THRESHOLD = 0.10  # 10%


def _score(win_rate: float, sharpe: float, expectancy: float) -> float:
    """Composite score: balanced blend of win rate, sharpe, and expectancy."""
    norm_exp = max(-1.0, min(1.0, expectancy / 0.10))  # normalise to [-1, 1]
    return win_rate * 0.40 + max(0.0, sharpe) * 0.40 + norm_exp * 0.20


async def _evaluate_params(
    conn: asyncpg.Connection,
    short_threshold: float,
    trail_stop_pct: float,
    trail_activate_pct: float,
    days: int,
) -> dict[str, Any] | None:
    """Simulate the parameter set against closed trades and return metrics.

    Uses actual closed trades from paper_trades. For each trade, checks whether
    the entry composite_score would have passed the given short_threshold, and
    re-prices the exit using the given trail_stop_pct (simplified: mark the
    trail-stop exits as re-evaluated). This is a lightweight proxy simulation
    — not a full backtest.
    """
    rows = await conn.fetch(
        """
        SELECT t.pnl, t.close_reason, t.direction,
               s.composite_score
          FROM paper_trades t
          LEFT JOIN signals_registry s ON s.id = t.signal_id::bigint
         WHERE t.closed_at > NOW() - make_interval(days => $1)
           AND t.closed_at IS NOT NULL
           AND t.pnl IS NOT NULL
           AND t.close_reason != 'dedup_cleanup'
        """,
        days,
    )
    if not rows:
        return None

    filtered_pnls: list[float] = []
    for r in rows:
        pnl   = float(r["pnl"])
        score = float(r["composite_score"] or 0.0)
        direction = r["direction"] or ""

        # Apply threshold filter: would this signal have been admitted?
        if direction == "short" and score > short_threshold:
            continue  # score not negative enough — would have been filtered out
        if direction == "long" and score < abs(short_threshold):
            continue  # longs also tightened proportionally

        # For trail_stop exits, apply simplified trail adjustment:
        # if trail_stop_pct is wider than 1.5% baseline, assume some losses
        # that would have been saves by a tighter trail become small wins.
        if r["close_reason"] == "trail_stop" and pnl < 0:
            # Wider trail = exit later = more negative pnl on bad trail exits
            adjustment = (trail_stop_pct - 1.5) * 0.01 * abs(pnl)
            pnl += adjustment

        filtered_pnls.append(pnl)

    n = len(filtered_pnls)
    if n < MIN_TRADES:
        return None

    wins = sum(1 for p in filtered_pnls if p > 0)
    win_rate = wins / n
    avg_pnl  = sum(filtered_pnls) / n
    std_pnl  = math.sqrt(sum((p - avg_pnl) ** 2 for p in filtered_pnls) / n)
    sharpe   = avg_pnl / std_pnl if std_pnl > 0.001 else 0.0
    avg_win  = sum(p for p in filtered_pnls if p > 0) / max(1, wins)
    avg_loss = sum(p for p in filtered_pnls if p <= 0) / max(1, n - wins)
    expectancy = win_rate * avg_win + (1 - win_rate) * avg_loss

    return {
        "win_rate":    round(win_rate, 4),
        "sharpe":      round(sharpe, 4),
        "expectancy":  round(expectancy, 4),
        "trade_count": n,
        "score":       round(_score(win_rate, sharpe, expectancy), 4),
    }


async def _send_discord(message: str) -> None:
    if not DISCORD_WEBHOOK:
        return
    try:
        import aiohttp
        async with aiohttp.ClientSession() as s:
            await s.post(DISCORD_WEBHOOK, json={"content": message[:2000]}, timeout=aiohttp.ClientTimeout(total=10))
    except Exception as exc:
        print(f"discord_send_error: {exc}")


async def run_sweep(days: int = 30, dry_run: bool = False) -> None:
    pool = await asyncpg.create_pool(DB_DSN, min_size=1, max_size=3)

    # Current baseline: read from .env or use defaults
    current_short_threshold    = float(os.getenv("SCORE_SHORT_THRESHOLD", "-0.15"))
    current_trail_stop_pct     = float(os.getenv("TRAIL_STOP_PCT", "1.5"))
    current_trail_activate_pct = float(os.getenv("TRAIL_ACTIVATE_PCT", "0.5"))

    baseline_params = {
        "short_threshold":    current_short_threshold,
        "trail_stop_pct":     current_trail_stop_pct,
        "trail_activate_pct": current_trail_activate_pct,
    }

    print(f"Running parameter sweep over last {days} days...")
    print(f"Grid size: {len(SWEEP_GRID['short_threshold'])} × "
          f"{len(SWEEP_GRID['trail_stop_pct'])} × "
          f"{len(SWEEP_GRID['trail_activate_pct'])} = "
          f"{len(SWEEP_GRID['short_threshold']) * len(SWEEP_GRID['trail_stop_pct']) * len(SWEEP_GRID['trail_activate_pct'])} combinations\n")

    best_result: dict[str, Any] | None = None
    best_score = -999.0
    baseline_score = 0.0
    total_combos = 0

    async with pool.acquire() as conn:
        # Score the current baseline first
        baseline_metrics = await _evaluate_params(
            conn,
            current_short_threshold,
            current_trail_stop_pct,
            current_trail_activate_pct,
            days,
        )
        if baseline_metrics:
            baseline_score = baseline_metrics["score"]
            print(f"Baseline ({baseline_params}): score={baseline_score:.4f} "
                  f"WR={baseline_metrics['win_rate']:.1%} "
                  f"sharpe={baseline_metrics['sharpe']:.2f} "
                  f"n={baseline_metrics['trade_count']}")
        else:
            print("Baseline: insufficient data (< MIN_TRADES)")

        # Grid search
        keys = list(SWEEP_GRID.keys())
        combos = list(product(*[SWEEP_GRID[k] for k in keys]))
        for combo in combos:
            params = dict(zip(keys, combo))
            metrics = await _evaluate_params(
                conn,
                params["short_threshold"],
                params["trail_stop_pct"],
                params["trail_activate_pct"],
                days,
            )
            if metrics is None:
                continue
            total_combos += 1
            if metrics["score"] > best_score:
                best_score  = metrics["score"]
                best_result = {**params, **metrics}

    await pool.close()

    if best_result is None:
        print("No valid combinations found — insufficient trade data.")
        return

    print(f"\nBest combination found ({total_combos} evaluated):")
    for k, v in best_result.items():
        print(f"  {k}: {v}")

    improvement = (best_score - baseline_score) / max(abs(baseline_score), 0.001)
    print(f"\nImprovement over baseline: {improvement:+.1%}")

    if dry_run:
        print("\n[dry-run] Skipping DB write and Telegram notification.")
        return

    # Write to optimization_runs table
    pool2 = await asyncpg.create_pool(DB_DSN, min_size=1, max_size=2)
    async with pool2.acquire() as conn:
        run_id = await conn.fetchval(
            """
            INSERT INTO optimization_runs (params, score, win_rate, sharpe, expectancy, trade_count, notes)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            RETURNING id
            """,
            json.dumps({k: best_result[k] for k in SWEEP_GRID}),
            best_result["score"],
            best_result.get("win_rate"),
            best_result.get("sharpe"),
            best_result.get("expectancy"),
            best_result.get("trade_count"),
            f"sweep over {days}d; improvement {improvement:+.1%} vs baseline",
        )
    await pool2.close()
    print(f"Written to optimization_runs, run_id={run_id}")

    # Send Telegram recommendation if improvement > threshold
    if improvement > IMPROVEMENT_THRESHOLD:
        param_diff = []
        for k in SWEEP_GRID:
            old_v = baseline_params[k]
            new_v = best_result[k]
            if old_v != new_v:
                param_diff.append(f"  {k}: {old_v} → {new_v}")
        diff_str = "\n".join(param_diff) if param_diff else "  (no change)"

        message = (
            f"🔧 *Parameter Sweep Recommendation*\n\n"
            f"Tested {total_combos} combinations over last {days} days.\n"
            f"Best score: `{best_score:.4f}` ({improvement:+.1%} vs current)\n\n"
            f"*Suggested changes:*\n{diff_str}\n\n"
            f"Metrics: WR={best_result['win_rate']:.1%} | "
            f"Sharpe={best_result['sharpe']:.2f} | "
            f"Expectancy={best_result['expectancy']:.4f}\n\n"
            f"To apply: `python scripts/apply_optimization.py {run_id}`"
        )
        await _send_discord(message)
        print(f"Discord recommendation sent (run_id={run_id})")
    else:
        print(f"Improvement {improvement:+.1%} below {IMPROVEMENT_THRESHOLD:.0%} threshold — no recommendation sent.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Weekly parameter sweep for self-optimizing trading system")
    parser.add_argument("--dry-run", action="store_true", help="Run sweep without writing to DB or Telegram")
    parser.add_argument("--days", type=int, default=30, help="Lookback window in days (default: 30)")
    args = parser.parse_args()
    asyncio.run(run_sweep(days=args.days, dry_run=args.dry_run))


if __name__ == "__main__":
    main()
