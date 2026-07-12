"""Seed the Truth Router with backdated MeanReversionEngine signals.

Sprint 39d. The offline replay (scripts/meanrev_replay.py) measured the
engine's raw rule over 7 months: bounce longs are negative-EV (43% WR,
-0.67%/trade), fade shorts are the real edge (56.5% WR, +1.5%/trade in the
last 30d). The router would handle both correctly — reject/invert the longs,
confirm the shorts — but only once each bucket has min_n=30 scored outcomes,
which live emission needs 2-3 weeks to reach. Until then every signal is
passthrough and the negative-EV longs face only the legacy gates.

This script closes the gap the same way the Sprint-? calibration bootstrap
did: re-run the exact engine rule over the router's 30-day lookback window
and insert the would-have-been signals backdated into signals_registry.
The outcome tracker scores them against price truth within minutes (500/window
per 5-min cycle), and the next hourly router refresh picks them up. Seeded
rows age out of the router window naturally as live emission fills in.

Safety:
  - emitted_at is always older than 25h → the paper trader's freshness gate
    (emitted_at > NOW()-25min) can never trade them.
  - source_table='meanrev_replay' marks provenance; purge with
      DELETE FROM signals_registry WHERE source_table='meanrev_replay';
  - idempotent: skips (ticker, direction, hour) rows already in the registry
    for this engine, so re-runs and live-engine overlap insert nothing.

Usage (inside the signal-edge-analyzer container, which mounts /app/scripts):
    python /app/scripts/meanrev_seed_router.py [--days 36] [--dry-run]
"""
from __future__ import annotations

import argparse
import asyncio
import json
import math
import os
from collections import defaultdict
from datetime import datetime, timedelta, timezone

import asyncpg

# Mirror src/engines/mean_reversion_engine.py exactly
Z_MIN = 1.25
ABS_MIN = 0.04
COOLDOWN_H = 4
MIN_UNIVERSE = 6
ENGINE_NAME = "MeanReversionEngine"

_QUOTES_SQL = """
SELECT q.symbol, date_trunc('hour', q.ts) AS hr,
       (ARRAY_AGG(q.price ORDER BY q.ts DESC))[1] AS price
  FROM ticker_quotes q
  JOIN tickers t ON t.symbol = q.symbol
 WHERE UPPER(t.exchange) = 'CRYPTO' AND q.price > 0
   AND q.ts > NOW() - ($1 || ' days')::interval - INTERVAL '26 hours'
 GROUP BY q.symbol, date_trunc('hour', q.ts)
"""

_EXISTING_SQL = """
SELECT ticker, direction, date_trunc('hour', emitted_at) AS hr
  FROM signals_registry
 WHERE engine_name = $1 AND emitted_at > NOW() - ($2 || ' days')::interval
"""

_INSERT_SQL = """
INSERT INTO signals_registry (
    signal_type, engine_name, ticker, direction,
    composite_score, confidence_level, signal_data,
    source_table, source_id, emitted_at
) VALUES ('mean_reversion', $1, $2, $3, $4, 'medium', $5, 'meanrev_replay', NULL, $6)
"""


def _nearest(series: dict[datetime, float], center: datetime, tol_h: int = 2) -> float | None:
    for off in range(tol_h + 1):
        for delta in ((0,) if off == 0 else (-off, off)):
            p = series.get(center + timedelta(hours=delta))
            if p is not None:
                return p
    return None


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=36)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    conn = await asyncpg.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"), port=5432,
        user=os.getenv("POSTGRES_USER", "mktintel"),
        password=os.getenv("POSTGRES_PASSWORD", ""),
        database=os.getenv("POSTGRES_DB", "mktintel"),
    )
    rows = await conn.fetch(_QUOTES_SQL, str(args.days))
    existing = {
        (r["ticker"], r["direction"],
         r["hr"].replace(tzinfo=timezone.utc) if r["hr"].tzinfo is None else r["hr"])
        for r in await conn.fetch(_EXISTING_SQL, ENGINE_NAME, str(args.days))
    }

    prices: dict[str, dict[datetime, float]] = defaultdict(dict)
    hours: set[datetime] = set()
    for r in rows:
        hr = r["hr"].replace(tzinfo=timezone.utc) if r["hr"].tzinfo is None else r["hr"]
        prices[r["symbol"]][hr] = float(r["price"])
        hours.add(hr)

    now = datetime.now(timezone.utc)
    # Latest seedable hour: 25h ago, so a 24h outcome already exists and the
    # trader freshness gate is structurally unreachable.
    latest = now - timedelta(hours=25)
    print(f"replaying {min(hours):%Y-%m-%d %H} .. {latest:%Y-%m-%d %H} UTC "
          f"({len(prices)} symbols, {len(existing)} existing rows to skip)")

    cooldown_until: dict[tuple[str, str], datetime] = {}
    inserted = skipped = 0
    per_dir: dict[str, int] = defaultdict(int)

    for T in sorted(h for h in hours if h <= latest):
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
        std = math.sqrt(sum((x - mean) ** 2 for x in rets.values()) / len(rets))
        if std <= 1e-6:
            continue

        for sym, ret in rets.items():
            z = (ret - mean) / std
            if abs(z) < Z_MIN or abs(ret) < ABS_MIN:
                continue
            direction = "risk_on" if ret < 0 else "risk_off"
            key = (sym, direction)
            if cooldown_until.get(key, datetime.min.replace(tzinfo=timezone.utc)) > T:
                continue
            cooldown_until[key] = T + timedelta(hours=COOLDOWN_H)

            if (sym, direction, T) in existing:
                skipped += 1
                continue

            score = max(-0.9, min(0.9, math.copysign(abs(z) * 0.15,
                                                     1 if direction == "risk_on" else -1)))
            confidence = min(0.85, 0.50 + abs(z) * 0.10)
            signal_data = json.dumps({
                "confidence_score": round(confidence, 4),
                "ret_24h": round(ret, 5),
                "zscore": round(z, 3),
                "universe_n": len(rets),
                "universe_mean_ret": round(mean, 5),
                "replayed": True,
            })
            if not args.dry_run:
                await conn.execute(_INSERT_SQL, ENGINE_NAME, sym, direction,
                                   round(score, 4), signal_data, T)
            inserted += 1
            per_dir[direction] += 1

    await conn.close()
    mode = "DRY RUN — would insert" if args.dry_run else "inserted"
    print(f"{mode} {inserted} signals ({dict(per_dir)}), skipped {skipped} already present")


if __name__ == "__main__":
    asyncio.run(main())
