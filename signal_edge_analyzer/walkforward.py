"""Daily walk-forward heartbeat for the Truth Router policy.

Sprint 38g. Replays the production routing rule over the last N fully-evaluated
days using only data available before each day (true out-of-sample), and
summarises realized 24h returns net of costs. Pushed to Telegram daily so edge
decay is caught in days, not after a drawdown.

Mirrors scripts/router_walkforward.py with the production policy:
long confirms gated by CONFIRM_LONG_EDGE (walk-forward proved them toxic).
"""
from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta, timezone

import asyncpg

WINDOW_H = 24
LOOKBACK_DAYS = 30
MIN_N = 30
MIN_EDGE = 0.05
CONFIRM_LONG_EDGE = 0.15
INVERT_MIN_EDGE = 0.08
INVERT_MIN_N = 100
SHRINK_K = 50.0
FLAT_BAND = 0.002
COST = 0.003  # round-trip haircut

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
       engine_name, bucket, exch, ret
  FROM src WHERE bucket IS NOT NULL
"""


def _build_lift(rows):
    base = defaultdict(lambda: [0, 0])
    per_key = {}
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


def _route(lift, engine, bucket, exch):
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


async def run_walkforward(pool: asyncpg.Pool, days: int = 7) -> str:
    """Run the OOS replay and return a Telegram-ready summary string."""
    end_day = datetime.now(timezone.utc).date() - timedelta(days=2)
    test_days = [end_day - timedelta(days=i) for i in range(days)][::-1]

    agg: dict[str, list[float]] = defaultdict(list)
    base_returns: list[float] = []

    async with pool.acquire() as conn:
        for d in test_days:
            d0 = datetime(d.year, d.month, d.day, tzinfo=timezone.utc)
            d1 = d0 + timedelta(days=1)
            lift = _build_lift(
                await conn.fetch(_STATS_SQL, d0 - timedelta(days=LOOKBACK_DAYS), d0)
            )
            for c in await conn.fetch(_CANDIDATES_SQL, d0, d1):
                ret = float(c["ret"])
                nominal = "long" if c["bucket"].endswith("long") else "short"
                base_returns.append((ret if nominal == "long" else -ret) - COST)
                verdict = _route(lift, c["engine_name"], c["bucket"], c["exch"])
                if verdict is None or verdict[0] == "reject":
                    continue
                action, traded = verdict
                agg[action].append((ret if traded == "long" else -ret) - COST)

    def line(name, xs):
        if not xs:
            return f"{name}: no trades"
        n = len(xs)
        wins = sum(1 for x in xs if x > 0)
        return (f"{name}: n={n} WR={100*wins/n:.0f}% "
                f"avg={100*sum(xs)/n:+.2f}% total={100*sum(xs):+.1f}%")

    routed = agg["confirm"] + agg["invert"]
    avg = sum(routed) / len(routed) if routed else 0.0
    health = "✅ edge intact" if routed and avg > 0 else (
        "⚠️ EDGE DECAY — routed avg ≤ 0, review thresholds" if routed
        else "ℹ️ no routed candidates in window"
    )
    base_avg = (sum(base_returns) / len(base_returns) * 100) if base_returns else 0.0
    return (
        f"🧭 *Router walk-forward* ({test_days[0]} → {test_days[-1]}, OOS, net of {COST*100:.1f}%)\n"
        f"{line('confirm', agg['confirm'])}\n"
        f"{line('invert', agg['invert'])}\n"
        f"{line('combined', routed)}\n"
        f"baseline (trust engines): avg={base_avg:+.2f}%\n"
        f"{health}"
    )
