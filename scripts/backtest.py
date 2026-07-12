#!/usr/bin/env python3
"""
Backtest framework — historical signal win-rate analysis.

Reads signals_registry + ticker_quotes to compute per-engine win rates
across configurable forward-return windows.

Usage:
    python scripts/backtest.py [--window 4] [--min-samples 20] [--engine all]

Output: table of engine × window × direction → win_rate, avg_return, sample_count
"""
from __future__ import annotations

import argparse
import asyncio
import os
import sys
from collections import defaultdict
from dataclasses import dataclass, field

import asyncpg


def _load_env_db_url() -> str:
    """Build DB URL from .env file or environment variables."""
    if url := os.getenv("DATABASE_URL"):
        return url
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
    # When running outside Docker, postgres resolves to the container name which won't work
    if host not in ("localhost", "127.0.0.1"):
        host = "localhost"
    port = cfg.get("POSTGRES_PORT", "5432")
    db   = cfg.get("POSTGRES_DB",   "mktintel")
    user = cfg.get("POSTGRES_USER", "mktintel")
    pw   = cfg.get("POSTGRES_PASSWORD", "mktintel")
    return f"postgresql://{user}:{pw}@{host}:{port}/{db}"


# ── Config ────────────────────────────────────────────────────────────────────

DB_URL = _load_env_db_url()

FORWARD_WINDOWS = [1, 2, 4, 8, 12]   # hours
_SCORE_LONG_THRESHOLD  =  0.15
_SCORE_SHORT_THRESHOLD = -0.15


# ── Data structures ───────────────────────────────────────────────────────────

@dataclass
class SignalRow:
    signal_id: int
    ticker: str
    engine_name: str
    direction: str          # "long" | "short"
    composite_score: float
    emitted_at: object      # datetime


@dataclass
class WindowResult:
    engine: str
    window_h: int
    direction: str
    wins: int = 0
    losses: int = 0
    total_return: float = 0.0

    @property
    def samples(self) -> int:
        return self.wins + self.losses

    @property
    def win_rate(self) -> float:
        return round(self.wins / self.samples, 4) if self.samples else 0.0

    @property
    def avg_return_pct(self) -> float:
        return round(self.total_return / self.samples * 100, 3) if self.samples else 0.0


# ── Price lookup ──────────────────────────────────────────────────────────────

async def _build_price_index(
    conn: asyncpg.Connection, tickers: set[str]
) -> dict[str, list[tuple]]:
    """Return {ticker: [(ts, price), ...]} sorted ASC for binary-search."""
    if not tickers:
        return {}
    rows = await conn.fetch(
        """
        SELECT symbol, ts, price FROM ticker_quotes
         WHERE symbol = ANY($1::text[])
           AND price > 0
         ORDER BY symbol, ts ASC
        """,
        list(tickers),
    )
    index: dict[str, list[tuple]] = defaultdict(list)
    for r in rows:
        index[r["symbol"]].append((r["ts"], float(r["price"])))
    return dict(index)


def _price_at(series: list[tuple], target_ts) -> float | None:
    """Binary search for closest price at or after target_ts."""
    lo, hi = 0, len(series) - 1
    result = None
    while lo <= hi:
        mid = (lo + hi) // 2
        if series[mid][0] >= target_ts:
            result = series[mid][1]
            hi = mid - 1
        else:
            lo = mid + 1
    return result


# ── Signal fetch ──────────────────────────────────────────────────────────────

async def _fetch_signals(
    conn: asyncpg.Connection,
    engine_filter: str | None,
    lookback_days: int,
) -> list[SignalRow]:
    where = "WHERE s.emitted_at > NOW() - INTERVAL '%d days' AND s.composite_score IS NOT NULL" % lookback_days
    if engine_filter and engine_filter != "all":
        where += f" AND s.engine_name = '{engine_filter}'"

    rows = await conn.fetch(
        f"""
        SELECT s.id, s.ticker, s.engine_name, s.direction, s.composite_score, s.emitted_at
          FROM signals_registry s
          {where}
         ORDER BY s.emitted_at ASC
        """
    )

    signals = []
    for r in rows:
        raw_dir = r["direction"] or "neutral"
        score = float(r["composite_score"] or 0.0)
        if raw_dir == "risk_on":
            direction = "long"
        elif raw_dir == "risk_off":
            direction = "short"
        elif score >= _SCORE_LONG_THRESHOLD:
            direction = "long"
        elif score <= _SCORE_SHORT_THRESHOLD:
            direction = "short"
        else:
            continue  # truly neutral — skip

        signals.append(SignalRow(
            signal_id=r["id"],
            ticker=r["ticker"],
            engine_name=r["engine_name"] or "unknown",
            direction=direction,
            composite_score=score,
            emitted_at=r["emitted_at"],
        ))
    return signals


# ── Core backtest loop ────────────────────────────────────────────────────────

def _run_backtest(
    signals: list[SignalRow],
    price_index: dict[str, list[tuple]],
    windows: list[int],
) -> dict[tuple, WindowResult]:
    """Compute per-(engine, window, direction) win rates."""
    import datetime as _dt
    results: dict[tuple, WindowResult] = {}

    for sig in signals:
        series = price_index.get(sig.ticker)
        if not series:
            continue

        # Entry price = first quote at or after signal time
        entry_price = _price_at(series, sig.emitted_at)
        if not entry_price:
            continue

        for w in windows:
            target_ts = sig.emitted_at + _dt.timedelta(hours=w)
            exit_price = _price_at(series, target_ts)
            if not exit_price:
                continue

            if sig.direction == "long":
                ret = (exit_price - entry_price) / entry_price
            else:
                ret = (entry_price - exit_price) / entry_price

            key = (sig.engine_name, w, sig.direction)
            if key not in results:
                results[key] = WindowResult(
                    engine=sig.engine_name, window_h=w, direction=sig.direction
                )
            r = results[key]
            if ret > 0:
                r.wins += 1
            else:
                r.losses += 1
            r.total_return += ret

    return results


# ── Report ────────────────────────────────────────────────────────────────────

def _print_report(results: dict[tuple, WindowResult], min_samples: int) -> None:
    rows = [r for r in results.values() if r.samples >= min_samples]
    if not rows:
        print("No results with enough samples (min_samples=%d)." % min_samples)
        return

    rows.sort(key=lambda r: (-r.win_rate, r.engine, r.window_h))

    header = f"{'Engine':<35} {'Dir':<6} {'Win(h)':<8} {'Win%':<8} {'AvgRet%':<10} {'N':>6}"
    sep = "─" * len(header)
    print(sep)
    print(header)
    print(sep)
    for r in rows:
        flag = "★" if r.win_rate >= 0.55 else (" " if r.win_rate >= 0.45 else "▼")
        print(
            f"{r.engine:<35} {r.direction:<6} {r.window_h:<8} "
            f"{r.win_rate*100:>6.1f}%  {r.avg_return_pct:>+8.3f}%  {r.samples:>6}  {flag}"
        )
    print(sep)

    # Summary by engine (best window)
    by_engine: dict[str, list[WindowResult]] = defaultdict(list)
    for r in rows:
        by_engine[r.engine].append(r)

    print("\n── Best window per engine ────────────────────────────────────────────────────")
    print(f"{'Engine':<35} {'BestWin%':<10} {'@Win(h)':<9} {'Dir':<6} {'N':>6}")
    for eng, eng_rows in sorted(by_engine.items()):
        best = max(eng_rows, key=lambda r: r.win_rate)
        print(
            f"{eng:<35} {best.win_rate*100:>7.1f}%   {best.window_h:<9} "
            f"{best.direction:<6} {best.samples:>6}"
        )


# ── Entry point ───────────────────────────────────────────────────────────────

async def main() -> None:
    parser = argparse.ArgumentParser(description="Signal backtest report")
    parser.add_argument("--window", type=int, nargs="+", default=FORWARD_WINDOWS,
                        help="Forward windows in hours (default: 1 2 4 8 12)")
    parser.add_argument("--min-samples", type=int, default=10,
                        help="Minimum sample count to include a row (default: 10)")
    parser.add_argument("--engine", type=str, default="all",
                        help="Filter to a single engine name (default: all)")
    parser.add_argument("--lookback", type=int, default=30,
                        help="Lookback period in days (default: 30)")
    args = parser.parse_args()

    print(f"Connecting to {DB_URL[:40]}...")
    conn = await asyncpg.connect(DB_URL)

    try:
        signals = await _fetch_signals(conn, args.engine, args.lookback)
        print(f"Loaded {len(signals)} signals over last {args.lookback} days.")

        if not signals:
            print("No signals found.")
            return

        tickers = {s.ticker for s in signals}
        print(f"Building price index for {len(tickers)} tickers...")
        price_index = await _build_price_index(conn, tickers)
        priced = sum(1 for t in tickers if t in price_index)
        print(f"Price data available for {priced}/{len(tickers)} tickers.")

        results = _run_backtest(signals, price_index, args.window)
        _print_report(results, args.min_samples)

    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
