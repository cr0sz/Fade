"""In-engine out-of-sample (walk-forward) validator for the Truth Router.

Sprint 53. The router's lift table is computed in-sample; static thresholds
cannot notice when a segment's edge dies. This module ports the walk-forward
core of scripts/router_walkforward.py into the engine image (the engine's Docker
build context is ./paper_trading_engine only — scripts/ is NOT in the image, so
the logic is duplicated here intentionally, not imported).

For each evaluation day D in a trailing window:
  1. Build the lift table exactly like TruthRouter.maybe_refresh, using only
     signals emitted in [D - lookback, D).
  2. Take day D's deduped (ticker, hour, engine, bucket) candidates that have a
     known 24h price outcome.
  3. Apply the router rule (same thresholds/shrinkage as production).
  4. Score each resulting confirm/invert verdict against the realized 24h return,
     net of a 0.30% round-trip cost haircut.

Per-segment stats are keyed by (engine, bucket, exch, action) and computed for
two trailing windows (7d and 14d). The result is written to Redis so the router
can gate live verdicts on continuously-measured OOS evidence ("no proof, no
trade"). This validates the routing rule itself at signal level — no position
limits, sizing, trails, or gates are simulated.
"""
from __future__ import annotations

import json
import os
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone

import asyncpg
import structlog

logger = structlog.get_logger(__name__)

# Production parameters (mirror truth_router.py / router_walkforward.py defaults)
WINDOW_H = int(os.getenv("ROUTER_WINDOW_H", "24"))
LOOKBACK_DAYS = int(os.getenv("ROUTER_LOOKBACK_DAYS", "30"))
MIN_N = int(os.getenv("ROUTER_MIN_N", "30"))
MIN_EDGE = float(os.getenv("ROUTER_MIN_EDGE", "0.05"))
CONFIRM_LONG_EDGE = float(os.getenv("ROUTER_CONFIRM_LONG_EDGE", "0.15"))
# Bar for *measuring* confirm-long candidates (matches the router's OOS
# carve-out, OOS_LONG_MIN_EDGE) — see _route for why this differs from
# CONFIRM_LONG_EDGE.
LONG_CANDIDATE_EDGE = float(os.getenv("OOS_LONG_MIN_EDGE", str(MIN_EDGE)))
INVERT_MIN_EDGE = float(os.getenv("ROUTER_INVERT_MIN_EDGE", "0.08"))
INVERT_MIN_N = int(os.getenv("ROUTER_INVERT_MIN_N", "100"))
SHRINK_K = float(os.getenv("ROUTER_SHRINK_K", "50"))
FLAT_BAND = 0.002
COST = float(os.getenv("OOS_COST", "0.003"))  # 0.30% round-trip haircut

OOS_STATS_KEY = "router:oos_stats"

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


def _build_lift(rows) -> dict[tuple[str, str, str], tuple[float, int]]:
    """Mirror TruthRouter.maybe_refresh / router_walkforward.build_lift."""
    base: dict[str, list[int]] = defaultdict(lambda: [0, 0])
    per_key: dict[tuple[str, str, str], tuple[int, int]] = {}
    for r in rows:
        ups, downs = int(r["ups"]), int(r["downs"])
        per_key[(r["engine_name"], r["bucket"], r["exch"])] = (ups, downs)
        base[r["exch"]][0] += ups
        base[r["exch"]][1] += downs
    lift: dict[tuple[str, str, str], tuple[float, int]] = {}
    for key, (ups, downs) in per_key.items():
        n = ups + downs
        bu, bd = base[key[2]]
        if n < MIN_N or (bu + bd) <= 0:
            continue
        lift[key] = ((ups / n - bu / (bu + bd)) * (n / (n + SHRINK_K)), n)
    return lift


def _route(lift, engine, bucket, exch) -> tuple[str, str] | None:
    """Return (action, traded_direction) or None for passthrough/no-stats.

    Mirrors router_walkforward.route, which mirrors TruthRouter._route.
    """
    nominal = "long" if bucket.endswith("long") else "short"
    stats = lift.get((engine, bucket, exch))
    if stats is None:
        return None
    l, n = stats
    edge_nominal = l if nominal == "long" else -l
    # Measure confirm-longs at the carve-out bar, not the static suppression
    # bar: the gate can only un-bench a long segment it has evidence for. The
    # production router still applies CONFIRM_LONG_EDGE unless the segment is
    # OOS-proven — this simulates exactly that candidate policy.
    confirm_bar = LONG_CANDIDATE_EDGE if nominal == "long" else MIN_EDGE
    if edge_nominal >= confirm_bar:
        return ("confirm", nominal)
    if -edge_nominal >= INVERT_MIN_EDGE and n >= INVERT_MIN_N:
        return ("invert", "short" if nominal == "long" else "long")
    return ("reject", nominal)


async def _walk_forward(conn, days: list, cost: float) -> dict[str, dict]:
    """Run the walk-forward over `days`, return {segment_key: stats}.

    segment_key = "engine|bucket|exch|action".
    """
    # per segment: list of net returns
    by_segment: dict[tuple[str, str, str, str], list[float]] = defaultdict(list)
    for d in days:
        d0 = datetime(d.year, d.month, d.day, tzinfo=timezone.utc)
        d1 = d0 + timedelta(days=1)
        stats_rows = await conn.fetch(
            _STATS_SQL, d0 - timedelta(days=LOOKBACK_DAYS), d0
        )
        lift = _build_lift(stats_rows)
        cands = await conn.fetch(_CANDIDATES_SQL, d0, d1)
        for c in cands:
            ret = float(c["ret"])
            bucket = c["bucket"]
            verdict = _route(lift, c["engine_name"], bucket, c["exch"])
            if verdict is None or verdict[0] == "reject":
                continue
            action, traded = verdict
            gross = ret if traded == "long" else -ret
            net = gross - cost
            key = (c["engine_name"], bucket, c["exch"], action)
            by_segment[key].append(net)

    out: dict[str, dict] = {}
    for (engine, bucket, exch, action), nets in by_segment.items():
        n = len(nets)
        if n == 0:
            continue
        wins = sum(1 for x in nets if x > 0)
        seg_key = f"{engine}|{bucket}|{exch}|{action}"
        out[seg_key] = {
            "engine": engine,
            "bucket": bucket,
            "exch": exch,
            "action": action,
            "n": n,
            "win_rate": round(100 * wins / n, 2),
            "avg_net": round(100 * sum(nets) / n, 4),
            "total_net": round(100 * sum(nets), 4),
        }
    return out


async def compute_oos_stats(pool: asyncpg.Pool) -> dict:
    """Compute trailing-7d and trailing-14d OOS per-segment stats.

    Returns {"computed_at": iso_ts, "windows": {"7d": {...}, "14d": {...}}}.
    Each window maps segment_key ("engine|bucket|exch|action") to per-segment
    stats: n, win_rate, avg_net, total_net.
    """
    cost = COST
    # End 2 days ago so every candidate's 24h outcome is final.
    end_day = datetime.now(timezone.utc).date() - timedelta(days=2)
    days_14 = [end_day - timedelta(days=i) for i in range(14)][::-1]
    days_7 = [end_day - timedelta(days=i) for i in range(7)][::-1]

    async with pool.acquire() as conn:
        win_14 = await _walk_forward(conn, days_14, cost)
        win_7 = await _walk_forward(conn, days_7, cost)

    return {
        "computed_at": datetime.now(timezone.utc).isoformat(),
        "windows": {"7d": win_7, "14d": win_14},
    }


def tradeable_segments(stats: dict, min_n: int) -> list[str]:
    """List segment keys that pass the OOS-tradeable bar (for logging).

    14d avg_net > 0 with n >= min_n, AND (7d n < 10 OR 7d avg_net > 0).
    """
    windows = (stats or {}).get("windows", {})
    w14 = windows.get("14d", {})
    w7 = windows.get("7d", {})
    out: list[str] = []
    for seg_key, s14 in w14.items():
        if s14.get("n", 0) < min_n or s14.get("avg_net", 0.0) <= 0.0:
            continue
        s7 = w7.get(seg_key)
        if s7 is not None and s7.get("n", 0) >= 10 and s7.get("avg_net", 0.0) <= 0.0:
            continue
        out.append(seg_key)
    return out


async def run_refresh_loop(pool: asyncpg.Pool, redis, refresh_s: int,
                           min_n: int, start_delay_s: float = 120.0) -> None:
    """Background task: compute OOS stats every `refresh_s`, write to Redis.

    Delayed `start_delay_s` so it never slows engine startup. Failures are logged
    (`oos_refresh_failed`) and never propagate — the engine must keep trading.
    """
    import asyncio

    await asyncio.sleep(start_delay_s)
    while True:
        try:
            stats = await compute_oos_stats(pool)
            payload = json.dumps(stats)
            try:
                await redis.set(OOS_STATS_KEY, payload)
            except Exception as exc:
                logger.warning("oos_refresh_failed", stage="redis_set", error=str(exc))
            else:
                tradeable = tradeable_segments(stats, min_n)
                logger.info(
                    "oos_stats_refreshed",
                    tradeable=tradeable,
                    n_tradeable=len(tradeable),
                    segments_14d=len(stats["windows"]["14d"]),
                    segments_7d=len(stats["windows"]["7d"]),
                )
        except Exception as exc:
            logger.warning("oos_refresh_failed", stage="compute", error=str(exc))
        await asyncio.sleep(refresh_s)
