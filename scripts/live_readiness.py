#!/usr/bin/env python3
"""
Live trading readiness check.

Criteria to graduate from paper to live:
  1. ≥ 30 closed trades
  2. Win rate ≥ 45%
  3. Max portfolio drawdown ≤ 8%
  4. Avg hold time within 1–8 h range (signals working as intended)

Usage:
    python scripts/live_readiness.py [--mode paper|live]
"""
from __future__ import annotations

import asyncio
import os
import sys

import asyncpg


def _load_env_db_url() -> str:
    env_path = os.path.join(os.path.dirname(__file__), "..", ".env")
    cfg: dict[str, str] = {}
    try:
        with open(env_path) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, _, v = line.partition("=")
                    cfg[k.strip()] = v.strip().strip('"').strip("'")
    except FileNotFoundError:
        pass
    host = cfg.get("POSTGRES_HOST", "localhost")
    if host not in ("localhost", "127.0.0.1"):
        host = "localhost"
    port = cfg.get("POSTGRES_PORT", "5432")
    db   = cfg.get("POSTGRES_DB",   "mktintel")
    user = cfg.get("POSTGRES_USER", "mktintel")
    pw   = cfg.get("POSTGRES_PASSWORD", "mktintel")
    return f"postgresql://{user}:{pw}@{host}:{port}/{db}"


_MIN_TRADES    = 30
_MIN_WIN_RATE  = 0.45
_MAX_DRAWDOWN  = 0.08
_MIN_HOLD_H    = 0.5
_MAX_HOLD_H    = 9.0


async def main() -> None:
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", default="paper", choices=["paper", "live", "all"])
    args = parser.parse_args()

    db_url = os.getenv("DATABASE_URL", _load_env_db_url())
    conn = await asyncpg.connect(db_url)

    try:
        mode_filter = "" if args.mode == "all" else f"AND mode = '{args.mode}'"

        # ── Closed trade stats ─────────────────────────────────────────────────
        stats = await conn.fetchrow(
            f"""
            SELECT
                COUNT(*)                                         AS total_closed,
                COUNT(*) FILTER (WHERE pnl > 0)                 AS wins,
                COUNT(*) FILTER (WHERE pnl <= 0)                AS losses,
                COALESCE(SUM(pnl), 0)                           AS total_pnl,
                COALESCE(AVG(pnl), 0)                           AS avg_pnl,
                COALESCE(STDDEV(pnl), 0)                        AS stddev_pnl,
                COALESCE(AVG(
                    EXTRACT(EPOCH FROM (closed_at - opened_at)) / 3600
                ), 0)                                           AS avg_hold_h,
                COALESCE(MAX(pnl), 0)                           AS best_trade,
                COALESCE(MIN(pnl), 0)                           AS worst_trade
              FROM paper_trades
             WHERE closed_at IS NOT NULL
               AND pnl IS NOT NULL
               AND close_reason != 'dedup_cleanup'
               {mode_filter}
            """
        )

        # ── Max drawdown (peak-to-trough on cumulative PnL) ──────────────────
        pnl_rows = await conn.fetch(
            f"""
            SELECT pnl FROM paper_trades
             WHERE closed_at IS NOT NULL AND pnl IS NOT NULL
               AND close_reason != 'dedup_cleanup'
               {mode_filter}
             ORDER BY closed_at ASC
            """
        )
        pnl_series = [float(r["pnl"]) for r in pnl_rows]
        max_dd = _compute_max_drawdown(pnl_series)

        # ── Open position summary ──────────────────────────────────────────────
        open_row = await conn.fetchrow(
            f"""
            SELECT COUNT(*) AS open_count,
                   COALESCE(SUM(notional), 0) AS open_notional
              FROM paper_trades
             WHERE closed_at IS NULL
               {mode_filter}
            """
        )

        # ── Close reason breakdown ─────────────────────────────────────────────
        reason_rows = await conn.fetch(
            f"""
            SELECT close_reason, COUNT(*) AS cnt
              FROM paper_trades
             WHERE closed_at IS NOT NULL {mode_filter}
             GROUP BY close_reason ORDER BY cnt DESC
            """
        )

        # ── Direction breakdown ────────────────────────────────────────────────
        dir_rows = await conn.fetch(
            f"""
            SELECT direction,
                   COUNT(*) AS total,
                   COUNT(*) FILTER (WHERE pnl > 0) AS wins
              FROM paper_trades
             WHERE closed_at IS NOT NULL
               AND close_reason != 'dedup_cleanup'
               {mode_filter}
             GROUP BY direction
            """
        )

    finally:
        await conn.close()

    # ── Compute metrics ────────────────────────────────────────────────────────
    total   = int(stats["total_closed"])
    wins    = int(stats["wins"])
    win_rate = wins / total if total else 0.0
    avg_hold = float(stats["avg_hold_h"])
    total_pnl = float(stats["total_pnl"])

    # ── Readiness checks ───────────────────────────────────────────────────────
    checks = [
        ("Closed trades ≥ 30",       total >= _MIN_TRADES,     f"{total} trades"),
        ("Win rate ≥ 45%",            win_rate >= _MIN_WIN_RATE,  f"{win_rate*100:.1f}%"),
        ("Max drawdown ≤ 8%",         max_dd <= _MAX_DRAWDOWN,   f"{max_dd*100:.2f}%"),
        ("Avg hold time 0.5–9 h",     _MIN_HOLD_H <= avg_hold <= _MAX_HOLD_H,
                                                                  f"{avg_hold:.2f}h"),
    ]

    all_pass = all(ok for _, ok, _ in checks)
    verdict = "✅  READY FOR LIVE" if all_pass else "❌  NOT YET READY"

    # ── Print ──────────────────────────────────────────────────────────────────
    print(f"\n{'═'*55}")
    print(f"  Live Trading Readiness  ({args.mode} mode)")
    print(f"{'═'*55}")

    for label, ok, detail in checks:
        sym = "✓" if ok else "✗"
        print(f"  [{sym}] {label:<30}  {detail}")

    print(f"\n  {verdict}")
    print(f"{'─'*55}")
    print(f"  Total PnL : ${total_pnl:+.2f}")
    print(f"  Avg trade : ${float(stats['avg_pnl']):+.2f}  (σ={float(stats['stddev_pnl']):.2f})")
    print(f"  Best/Worst: ${float(stats['best_trade']):+.2f} / ${float(stats['worst_trade']):+.2f}")
    print(f"  Open now  : {int(open_row['open_count'])} positions  (${float(open_row['open_notional']):.0f} notional)")

    print(f"\n  Close reasons:")
    for r in reason_rows:
        print(f"    {r['close_reason']:<25} {r['cnt']:>4}")

    print(f"\n  Direction breakdown:")
    for r in dir_rows:
        total_dir = int(r["total"])
        wr_dir = int(r["wins"]) / total_dir if total_dir else 0.0
        print(f"    {r['direction']:<8}  {total_dir:>4} trades  win={wr_dir*100:.1f}%")

    print(f"{'═'*55}\n")
    sys.exit(0 if all_pass else 1)


def _compute_max_drawdown(pnl_series: list[float]) -> float:
    """Max peak-to-trough drawdown on cumulative PnL curve, as fraction of initial $10k equity."""
    if not pnl_series:
        return 0.0
    initial_equity = 10_000.0
    equity = initial_equity
    peak = initial_equity
    max_dd = 0.0
    for pnl in pnl_series:
        equity += pnl
        if equity > peak:
            peak = equity
        dd = (peak - equity) / peak
        if dd > max_dd:
            max_dd = dd
    return round(max_dd, 6)


if __name__ == "__main__":
    asyncio.run(main())
