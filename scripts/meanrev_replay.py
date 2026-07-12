"""Offline historical replay of MeanReversionEngine — measure the edge first.

Sprint 39d. The live engine (src/engines/mean_reversion_engine.py) emits a
handful of signals per day, so the Truth Router needs 2-3 weeks to accumulate
min_n=30 per bucket. Before deciding whether to bootstrap router stats (or
add a per-engine confirm bar — see the NOTE in the engine docstring), this
script answers the prior question: what edge would the engine's exact rule
have had over the full quote history?

Mirrors the engine faithfully:
  - hourly steps over ticker_quotes (CRYPTO tickers only)
  - 24h return per symbol; past price = nearest quote in [T-26h, T-22h]
  - cross-sectional z-score vs that hour's universe (MIN_UNIVERSE=6)
  - |z| >= 1.25 AND |ret| >= 4%  ->  dump=risk_on (bounce long),
                                     pump=risk_off (fade short)
  - 4h cooldown per (symbol, direction)

Scoring (signal-level, like router_walkforward.py — no portfolio sim):
  - forward 24h price return, nearest quote in [T+22h, T+26h]
  - long  trade ret = +fwd - cost ; short trade ret = -fwd - cost
  - win = trade ret > 0 after the haircut

No DB writes. Usage (inside the worker container):
    python /app/scripts/meanrev_replay.py [--cost 0.003] [--z 1.25] [--abs 0.04]
"""
from __future__ import annotations

import argparse
import asyncio
import math
import os
from collections import defaultdict
from datetime import datetime, timedelta, timezone

import asyncpg

MIN_UNIVERSE = 6
COOLDOWN_H = 4

_QUOTES_SQL = """
SELECT q.symbol, date_trunc('hour', q.ts) AS hr, MAX(q.ts) AS ts,
       (ARRAY_AGG(q.price ORDER BY q.ts DESC))[1] AS price
  FROM ticker_quotes q
  JOIN tickers t ON t.symbol = q.symbol
 WHERE UPPER(t.exchange) = 'CRYPTO' AND q.price > 0
 GROUP BY q.symbol, date_trunc('hour', q.ts)
 ORDER BY hr
"""


def _nearest(series: dict[datetime, float], center: datetime, tol_h: int = 2) -> float | None:
    """Price at the hour nearest `center` within ±tol_h hours."""
    for off in range(tol_h + 1):
        for delta in ((0,) if off == 0 else (-off, off)):
            p = series.get(center + timedelta(hours=delta))
            if p is not None:
                return p
    return None


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--cost", type=float, default=0.003, help="round-trip cost haircut")
    ap.add_argument("--z", type=float, default=1.25, help="cross-sectional z threshold")
    ap.add_argument("--abs", dest="abs_min", type=float, default=0.04, help="abs 24h move floor")
    args = ap.parse_args()

    conn = await asyncpg.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"), port=5432,
        user=os.getenv("POSTGRES_USER", "mktintel"),
        password=os.getenv("POSTGRES_PASSWORD", ""),
        database=os.getenv("POSTGRES_DB", "mktintel"),
    )
    rows = await conn.fetch(_QUOTES_SQL)
    await conn.close()

    prices: dict[str, dict[datetime, float]] = defaultdict(dict)
    hours: set[datetime] = set()
    for r in rows:
        hr = r["hr"].replace(tzinfo=timezone.utc) if r["hr"].tzinfo is None else r["hr"]
        prices[r["symbol"]][hr] = float(r["price"])
        hours.add(hr)
    print(f"loaded {sum(len(v) for v in prices.values())} hourly prices, "
          f"{len(prices)} symbols, {min(hours):%Y-%m-%d} .. {max(hours):%Y-%m-%d}")

    cooldown_until: dict[tuple[str, str], datetime] = {}
    trades: list[dict] = []

    for T in sorted(hours):
        rets: dict[str, float] = {}
        for sym, series in prices.items():
            now_p = series.get(T)
            if now_p is None:
                continue
            past_p = _nearest(series, T - timedelta(hours=24))
            if past_p is None or past_p <= 0:
                continue
            rets[sym] = (now_p - past_p) / past_p
        if len(rets) < MIN_UNIVERSE:
            continue

        mean = sum(rets.values()) / len(rets)
        var = sum((x - mean) ** 2 for x in rets.values()) / len(rets)
        std = math.sqrt(var)
        if std <= 1e-6:
            continue

        for sym, ret in rets.items():
            z = (ret - mean) / std
            if abs(z) < args.z or abs(ret) < args.abs_min:
                continue
            direction = "risk_on" if ret < 0 else "risk_off"
            key = (sym, direction)
            if cooldown_until.get(key, datetime.min.replace(tzinfo=timezone.utc)) > T:
                continue
            cooldown_until[key] = T + timedelta(hours=COOLDOWN_H)

            entry = prices[sym][T]
            exit_p = _nearest(prices[sym], T + timedelta(hours=24))
            if exit_p is None:
                continue
            fwd = (exit_p - entry) / entry
            trade_ret = (fwd if direction == "risk_on" else -fwd) - args.cost
            trades.append({"t": T, "sym": sym, "dir": direction, "z": z,
                           "ret24": ret, "trade_ret": trade_ret})

    def summary(label: str, xs: list[dict]) -> None:
        if not xs:
            print(f"  {label:34s} n=0")
            return
        rs = sorted(t["trade_ret"] for t in xs)
        wins = sum(1 for r in rs if r > 0)
        avg = sum(rs) / len(rs)
        med = rs[len(rs) // 2]
        print(f"  {label:34s} n={len(rs):4d}  WR={wins / len(rs):5.1%}  "
              f"avg={avg:+.3%}  med={med:+.3%}  total={sum(rs):+.1%}")

    print(f"\n=== MeanReversionEngine replay  (z>={args.z}, |ret|>={args.abs_min:.0%}, "
          f"cost={args.cost:.1%}) ===")
    longs = [t for t in trades if t["dir"] == "risk_on"]
    shorts = [t for t in trades if t["dir"] == "risk_off"]
    summary("ALL", trades)
    summary("risk_on  (bounce long, fade dump)", longs)
    summary("risk_off (fade short, fade pump)", shorts)

    print("\n--- by month ---")
    by_month: dict[str, list[dict]] = defaultdict(list)
    for t in trades:
        by_month[t["t"].strftime("%Y-%m")].append(t)
    for m in sorted(by_month):
        ml = [t for t in by_month[m] if t["dir"] == "risk_on"]
        ms = [t for t in by_month[m] if t["dir"] == "risk_off"]
        summary(f"{m}  long", ml)
        summary(f"{m}  short", ms)

    print("\n--- by symbol (n>=10) ---")
    by_sym: dict[str, list[dict]] = defaultdict(list)
    for t in trades:
        by_sym[t["sym"]].append(t)
    for s in sorted(by_sym, key=lambda s: -len(by_sym[s])):
        if len(by_sym[s]) >= 10:
            summary(s, by_sym[s])

    # The last 30 days resemble the period the parameters were eyeballed on;
    # everything before is the closer-to-OOS read.
    cut = max(hours) - timedelta(days=30)
    print("\n--- in-sample-ish (last 30d) vs prior (older = closer to OOS) ---")
    for lbl, sel in (("last 30d", lambda t: t["t"] >= cut),
                     ("prior", lambda t: t["t"] < cut)):
        summary(f"{lbl}  long", [t for t in longs if sel(t)])
        summary(f"{lbl}  short", [t for t in shorts if sel(t)])


if __name__ == "__main__":
    asyncio.run(main())
