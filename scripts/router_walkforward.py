"""Walk-forward (out-of-sample) validation of the Truth Router policy.

Sprint 38e. The router's lift table is computed in-sample; this script answers
the only question that matters: would the policy have made money on data it
had NOT yet seen?

For each evaluation day D in the test range:
  1. Build the lift table exactly like TruthRouter.maybe_refresh, but using
     only signals emitted in [D - lookback, D).
  2. Take day D's deduped (ticker, hour, engine, bucket) candidates that have
     a known 24h price outcome.
  3. Apply the router rule (same thresholds/shrinkage as production).
  4. Score each resulting verdict against the realized 24h return, net of a
     round-trip cost haircut.

Also scores the BASELINE policy (trust the engine's nominal direction on the
same candidates) so the router's added value is explicit.

This measures signal-level edge, not portfolio PnL — no position limits,
sizing, trails, or gates are simulated. It validates the routing rule itself.

Usage (inside any container with asyncpg + DB env):
    python scripts/router_walkforward.py [--days 14] [--cost 0.003]
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone

import asyncpg

# Production parameters (mirror truth_router.py defaults)
WINDOW_H = 24
LOOKBACK_DAYS = 30
MIN_N = 30
MIN_EDGE = 0.05
INVERT_MIN_EDGE = 0.08
INVERT_MIN_N = 100
SHRINK_K = 50.0
FLAT_BAND = 0.002

_BUCKET_SQL = """
CASE WHEN sr.direction IN ('risk_on','inflow','liquidity_expansion') THEN 'emit_long'
     WHEN sr.direction IN ('risk_off','outflow','tightening','volatility_regime') THEN 'emit_short'
     WHEN sr.composite_score >= 0.10 THEN 'score_long'
     WHEN sr.composite_score <= -0.10 THEN 'score_short'
     ELSE NULL END
"""

_STATS_SQL = f"""
WITH src AS (
  SELECT sr.engine_name,
         {_BUCKET_SQL} AS bucket,
         CASE WHEN UPPER(COALESCE(t.exchange,'')) = 'CRYPTO' THEN 'CRYPTO' ELSE 'OTHER' END AS exch,
         sr.ticker, date_trunc('hour', sr.emitted_at) AS hr,
         so.outcome_score_delta AS ret
    FROM signal_outcomes so
    JOIN signals_registry sr ON sr.id = so.signal_id
    LEFT JOIN tickers t ON t.symbol = sr.ticker
   WHERE so.evaluation_window_h = {WINDOW_H}
     AND so.outcome_score_delta IS NOT NULL
     AND sr.emitted_at >= $1 AND sr.emitted_at < $2
     AND sr.engine_name <> 'technical_replay'
), dedup AS (
  SELECT DISTINCT ON (engine_name, bucket, exch, ticker, hr)
         engine_name, bucket, exch, ret
    FROM src WHERE bucket IS NOT NULL
)
SELECT engine_name, bucket, exch,
       COUNT(*) FILTER (WHERE ret > {FLAT_BAND})  AS ups,
       COUNT(*) FILTER (WHERE ret < -{FLAT_BAND}) AS downs
  FROM dedup GROUP BY 1, 2, 3
"""

_CANDIDATES_SQL = f"""
WITH src AS (
  SELECT sr.engine_name,
         {_BUCKET_SQL} AS bucket,
         CASE WHEN UPPER(COALESCE(t.exchange,'')) = 'CRYPTO' THEN 'CRYPTO' ELSE 'OTHER' END AS exch,
         sr.ticker, date_trunc('hour', sr.emitted_at) AS hr,
         so.outcome_score_delta AS ret
    FROM signal_outcomes so
    JOIN signals_registry sr ON sr.id = so.signal_id
    LEFT JOIN tickers t ON t.symbol = sr.ticker
   WHERE so.evaluation_window_h = {WINDOW_H}
     AND so.outcome_score_delta IS NOT NULL
     AND sr.emitted_at >= $1 AND sr.emitted_at < $2
     AND sr.engine_name <> 'technical_replay'
)
SELECT DISTINCT ON (engine_name, bucket, exch, ticker, hr)
       engine_name, bucket, exch, ticker, ret
  FROM src WHERE bucket IS NOT NULL
"""


def build_lift(rows) -> dict[tuple[str, str, str], tuple[float, int]]:
    base: dict[str, list[int]] = defaultdict(lambda: [0, 0])
    per_key: dict[tuple[str, str, str], tuple[int, int]] = {}
    for r in rows:
        ups, downs = int(r["ups"]), int(r["downs"])
        per_key[(r["engine_name"], r["bucket"], r["exch"])] = (ups, downs)
        base[r["exch"]][0] += ups
        base[r["exch"]][1] += downs
    lift = {}
    for key, (ups, downs) in per_key.items():
        n = ups + downs
        bu, bd = base[key[2]]
        if n < MIN_N or (bu + bd) <= 0:
            continue
        lift[key] = ((ups / n - bu / (bu + bd)) * (n / (n + SHRINK_K)), n)
    return lift


CONFIRM_LONG_EDGE = 0.15  # production default (ROUTER_CONFIRM_LONG_EDGE)


def route(lift, engine, bucket, exch) -> tuple[str, str] | None:
    """Return (action, traded_direction) or None for passthrough/no-stats."""
    nominal = "long" if bucket.endswith("long") else "short"
    stats = lift.get((engine, bucket, exch))
    if stats is None:
        return None
    l, n = stats
    edge_nominal = l if nominal == "long" else -l
    confirm_bar = CONFIRM_LONG_EDGE if nominal == "long" else MIN_EDGE
    if edge_nominal >= confirm_bar:
        return ("confirm", nominal)
    if -edge_nominal >= INVERT_MIN_EDGE and n >= INVERT_MIN_N:
        return ("invert", "short" if nominal == "long" else "long")
    return ("reject", nominal)


async def main() -> None:
    global MIN_EDGE, INVERT_MIN_EDGE, INVERT_MIN_N, CONFIRM_LONG_EDGE
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=14, help="test days (ending 2 days ago)")
    ap.add_argument("--start", type=str, default=None,
                    help="test window start date YYYY-MM-DD (inclusive); overrides --days")
    ap.add_argument("--end", type=str, default=None,
                    help="test window end date YYYY-MM-DD (inclusive); default 2 days ago")
    ap.add_argument("--json", type=str, default=None,
                    help="dump per-segment stats as JSON to this path")
    ap.add_argument("--cost", type=float, default=0.003,
                    help="round-trip cost haircut per trade (default 0.3%%)")
    ap.add_argument("--min-edge", type=float, default=MIN_EDGE)
    ap.add_argument("--confirm-long-edge", type=float, default=None,
                    help="separate (higher) confirm bar for long trades")
    ap.add_argument("--invert-edge", type=float, default=INVERT_MIN_EDGE)
    ap.add_argument("--invert-n", type=int, default=INVERT_MIN_N)
    ap.add_argument("--breakdown", action="store_true", help="per-bucket result table")
    args = ap.parse_args()

    MIN_EDGE = args.min_edge
    INVERT_MIN_EDGE = args.invert_edge
    INVERT_MIN_N = args.invert_n
    CONFIRM_LONG_EDGE = args.confirm_long_edge if args.confirm_long_edge is not None else MIN_EDGE

    pool = await asyncpg.create_pool(
        host=os.getenv("POSTGRES_HOST", "postgres"), port=5432,
        user=os.getenv("POSTGRES_USER", "mktintel"),
        password=os.environ["POSTGRES_PASSWORD"],
        database=os.getenv("POSTGRES_DB", "mktintel"), min_size=1, max_size=3,
    )

    # End 2 days ago so every candidate's 24h outcome is final.
    default_end = datetime.now(timezone.utc).date() - timedelta(days=2)
    end_day = (datetime.strptime(args.end, "%Y-%m-%d").date()
               if args.end else default_end)
    if args.start:
        start_day = datetime.strptime(args.start, "%Y-%m-%d").date()
        n_days = (end_day - start_day).days + 1
        if n_days < 1:
            raise SystemExit("--start must be on or before --end")
        days = [start_day + timedelta(days=i) for i in range(n_days)]
    else:
        days = [end_day - timedelta(days=i) for i in range(args.days)][::-1]

    agg: dict[str, list[float]] = defaultdict(list)       # action -> signed net returns
    by_bucket: dict[tuple, list[float]] = defaultdict(list)
    # Finer per-segment key: (engine, bucket, exch, action, final_direction).
    # Each entry holds (net_return, gross_return) tuples.
    by_segment: dict[tuple, list[tuple[float, float]]] = defaultdict(list)
    by_rollup: dict[tuple, list[tuple[float, float]]] = defaultdict(list)  # (action, direction)
    base_returns: list[float] = []                          # nominal-direction policy
    daily_lines: list[str] = []

    async with pool.acquire() as conn:
        for d in days:
            d0 = datetime(d.year, d.month, d.day, tzinfo=timezone.utc)
            d1 = d0 + timedelta(days=1)
            stats_rows = await conn.fetch(_STATS_SQL, d0 - timedelta(days=LOOKBACK_DAYS), d0)
            lift = build_lift(stats_rows)
            cands = await conn.fetch(_CANDIDATES_SQL, d0, d1)

            day_net = {"confirm": [], "invert": []}
            for c in cands:
                ret = float(c["ret"])
                bucket = c["bucket"]
                nominal = "long" if bucket.endswith("long") else "short"
                base_returns.append((ret if nominal == "long" else -ret) - args.cost)
                verdict = route(lift, c["engine_name"], bucket, c["exch"])
                if verdict is None or verdict[0] == "reject":
                    continue
                action, traded = verdict
                gross = ret if traded == "long" else -ret
                signed = gross - args.cost
                agg[action].append(signed)
                day_net[action].append(signed)
                by_bucket[(c["engine_name"], bucket, c["exch"], action)].append(signed)
                seg_key = (c["engine_name"], bucket, c["exch"], action, traded)
                by_segment[seg_key].append((signed, gross))
                by_rollup[(action, traded)].append((signed, gross))

            nc, ni = len(day_net["confirm"]), len(day_net["invert"])
            if nc or ni:
                sc = sum(day_net["confirm"]) * 100
                si = sum(day_net["invert"]) * 100
                daily_lines.append(
                    f"  {d}  confirm: n={nc:3d} net={sc:+7.2f}%   invert: n={ni:3d} net={si:+7.2f}%"
                )

    await pool.close()

    def summary(name: str, xs: list[float]) -> str:
        if not xs:
            return f"{name:>18}: no trades"
        n = len(xs)
        wins = sum(1 for x in xs if x > 0)
        return (f"{name:>18}: n={n:4d}  WR={100*wins/n:5.1f}%  "
                f"avg={100*sum(xs)/n:+.3f}%  total={100*sum(xs):+.1f}%")

    print(f"Walk-forward {days[0]} → {days[-1]}  (cost {args.cost*100:.2f}%/round-trip, "
          f"signal-level, no position limits)\n")
    print("\n".join(daily_lines) or "  (no routed trades on any day)")
    print()
    print(summary("ROUTER confirm", agg["confirm"]))
    print(summary("ROUTER invert", agg["invert"]))
    print(summary("ROUTER combined", agg["confirm"] + agg["invert"]))
    print(summary("BASELINE nominal", base_returns))

    if args.breakdown and by_bucket:
        print("\nPer-bucket results:")
        for key in sorted(by_bucket, key=lambda k: sum(by_bucket[k])):
            xs = by_bucket[key]
            wins = sum(1 for x in xs if x > 0)
            print(f"  {key[3]:>8} {key[0]}/{key[1]}/{key[2]}: "
                  f"n={len(xs):4d} WR={100*wins/len(xs):5.1f}% "
                  f"avg={100*sum(xs)/len(xs):+.3f}% total={100*sum(xs):+.1f}%")

    # ── Coarse rollup: (action, final_direction) ───────────────────────────────
    def _stats(vals: list[tuple[float, float]]) -> dict:
        n = len(vals)
        nets = [v[0] for v in vals]
        grosses = [v[1] for v in vals]
        wins = sum(1 for x in nets if x > 0)
        return {
            "n": n,
            "wr": 100 * wins / n,
            "avg_net": 100 * sum(nets) / n,
            "total_net": 100 * sum(nets),
            "avg_gross": 100 * sum(grosses) / n,
        }

    print("\nRollup by (action, final_direction):")
    rollup_rows = sorted(by_rollup.items(), key=lambda kv: _stats(kv[1])["avg_net"])
    for (action, direction), vals in rollup_rows:
        s = _stats(vals)
        print(f"  {action:>8} {direction:>5}: n={s['n']:4d} WR={s['wr']:5.1f}% "
              f"avg_net={s['avg_net']:+.3f}% avg_gross={s['avg_gross']:+.3f}% "
              f"total={s['total_net']:+.1f}%")

    # ── Fine per-segment summary (n>=10), sorted by avg net ────────────────────
    seg_export = []
    seg_rows = []
    for key, vals in by_segment.items():
        s = _stats(vals)
        engine, bucket, exch, action, direction = key
        rec = {
            "engine": engine, "bucket": bucket, "exch": exch,
            "action": action, "direction": direction,
            "n": s["n"], "wr": round(s["wr"], 2),
            "avg_net": round(s["avg_net"], 4),
            "total_net": round(s["total_net"], 4),
            "avg_gross": round(s["avg_gross"], 4),
        }
        seg_export.append(rec)
        if s["n"] >= 10:
            seg_rows.append((key, s))

    print("\nPer-segment results (n>=10, engine/bucket/exch/action/direction), sorted by avg net:")
    if not seg_rows:
        print("  (no segment reached n>=10)")
    for key, s in sorted(seg_rows, key=lambda ks: ks[1]["avg_net"]):
        engine, bucket, exch, action, direction = key
        print(f"  {action:>8} {direction:>5}  {engine}/{bucket}/{exch}: "
              f"n={s['n']:4d} WR={s['wr']:5.1f}% avg_net={s['avg_net']:+.3f}% "
              f"avg_gross={s['avg_gross']:+.3f}% total={s['total_net']:+.1f}%")

    if args.json:
        seg_export.sort(key=lambda r: r["avg_net"])
        with open(args.json, "w") as f:
            json.dump({
                "window_start": str(days[0]),
                "window_end": str(days[-1]),
                "cost": args.cost,
                "segments": seg_export,
            }, f, indent=2)
        print(f"\nWrote {len(seg_export)} segments to {args.json}")


if __name__ == "__main__":
    asyncio.run(main())
