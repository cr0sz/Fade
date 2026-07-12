"""Fade-entry parameter sweep over the hourly crypto quotes.

Sprint 48. Defines the deterministic grid and the per-combo backtest. The edge
we are hunting (Sprint 39): TDE panic-shorts on crypto, INVERTED to longs, earn
~+1.2%/trade — but the edge lives in *panic-burst conditioning*, not raw 24h
price outliers (raw dumps don't bounce on average). So the grid explores
conditioning (accelerating drop, volume spike) on top of plain drop thresholds,
all as fade *longs*.

Reuses the quote-loading SQL and the ±2h nearest-quote helper from
scripts/meanrev_replay.py (the established replay shape in this repo).
"""
from __future__ import annotations

import hashlib
import json
import math
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from itertools import product
from typing import Iterator

# ── Grid axes ────────────────────────────────────────────────────────────────
SWEEP_NAME = "fade_grid_v1"

DROP_WINDOW_H = [4, 8, 12, 24]
DROP_THRESHOLD = [-0.03, -0.05, -0.08, -0.12]   # window return must be <= this
PANIC_COND = ["none", "accelerating", "volume_spike"]
HOLD_H = [2, 6, 12, 24]

COST = 0.003          # round-trip haircut, same as meanrev_replay
COOLDOWN_NONE = True  # dedup is one-position-per-symbol, no extra cooldown
IS_FRACTION = 0.70    # walk-forward: first 70% of date range fit, last 30% OOS

# Per-symbol/per-hour aggregated quote pull (also carries volume for the spike
# condition). Mirrors meanrev_replay._QUOTES_SQL with volume added.
QUOTES_SQL = """
SELECT q.symbol,
       date_trunc('hour', q.ts) AS hr,
       (ARRAY_AGG(q.price  ORDER BY q.ts DESC))[1] AS price,
       (ARRAY_AGG(q.volume ORDER BY q.ts DESC))[1] AS volume
  FROM ticker_quotes q
 WHERE UPPER(q.exchange) = 'CRYPTO' AND q.price > 0
 GROUP BY q.symbol, date_trunc('hour', q.ts)
 ORDER BY hr
"""


def params_hash(params: dict) -> str:
    """Stable md5 of canonical JSON — matches md5(params::text) in Postgres only
    loosely; we use it as our own dedup guard. The DB unique index is the real
    authority (md5 of the stored jsonb text)."""
    return hashlib.md5(
        json.dumps(params, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def enumerate_grid() -> Iterator[dict]:
    """Deterministic enumeration of every param combo."""
    for win, thr, cond, hold in product(
        DROP_WINDOW_H, DROP_THRESHOLD, PANIC_COND, HOLD_H
    ):
        yield {
            "drop_window_h": win,
            "drop_threshold": thr,
            "panic_cond": cond,
            "hold_h": hold,
            "direction": "long",
        }


def grid_size() -> int:
    return len(DROP_WINDOW_H) * len(DROP_THRESHOLD) * len(PANIC_COND) * len(HOLD_H)


# ── Quote loading ────────────────────────────────────────────────────────────
def _nearest(series: dict, center: datetime, tol_h: int = 2):
    """Price at the hour nearest `center` within ±tol_h hours (from meanrev_replay)."""
    for off in range(tol_h + 1):
        for delta in ((0,) if off == 0 else (-off, off)):
            p = series.get(center + timedelta(hours=delta))
            if p is not None:
                return p
    return None


def build_series(rows) -> tuple[dict, dict, list]:
    """rows -> (prices[sym][hr], volumes[sym][hr], sorted_hours)."""
    prices: dict[str, dict] = defaultdict(dict)
    volumes: dict[str, dict] = defaultdict(dict)
    hours: set = set()
    for r in rows:
        hr = r["hr"]
        if hr.tzinfo is None:
            hr = hr.replace(tzinfo=timezone.utc)
        prices[r["symbol"]][hr] = float(r["price"])
        if r["volume"] is not None:
            volumes[r["symbol"]][hr] = float(r["volume"])
        hours.add(hr)
    return prices, volumes, sorted(hours)


# ── Backtest one combo ───────────────────────────────────────────────────────
def _vol_avg_24h(vseries: dict, T: datetime) -> float | None:
    vals = [v for h, v in vseries.items() if T - timedelta(hours=24) <= h < T]
    if len(vals) < 6:
        return None
    return sum(vals) / len(vals)


def _trades_for_combo(prices, volumes, hours, params) -> list[dict]:
    """Generate fade-long trades for one param combo, dedup overlapping entries
    per symbol (one open position at a time)."""
    win = params["drop_window_h"]
    thr = params["drop_threshold"]
    cond = params["panic_cond"]
    hold = params["hold_h"]
    trades: list[dict] = []

    for sym, series in prices.items():
        vseries = volumes.get(sym, {})
        busy_until: datetime | None = None  # one position at a time per symbol
        for T in hours:
            entry = series.get(T)
            if entry is None or entry <= 0:
                continue
            if busy_until is not None and T < busy_until:
                continue

            past = _nearest(series, T - timedelta(hours=win))
            if past is None or past <= 0:
                continue
            window_ret = (entry - past) / past
            if window_ret > thr:        # not a deep enough drop (thr is negative)
                continue

            # ── panic conditioning ──
            if cond == "accelerating":
                last_hr = _nearest(series, T - timedelta(hours=1), tol_h=1)
                if last_hr is None or last_hr <= 0:
                    continue
                last_hr_ret = (entry - last_hr) / last_hr
                # last hour's drop must be > 1/3 of the whole-window drop
                if last_hr_ret > window_ret / 3.0:
                    continue
            elif cond == "volume_spike":
                v_now = vseries.get(T)
                v_avg = _vol_avg_24h(vseries, T)
                if v_now is None or v_avg is None or v_avg <= 0:
                    continue
                if v_now < 2.0 * v_avg:
                    continue

            exit_p = _nearest(series, T + timedelta(hours=hold))
            if exit_p is None or exit_p <= 0:
                continue
            fwd = (exit_p - entry) / entry
            trade_ret = fwd - COST  # fade long: profit on bounce
            trades.append({"t": T, "sym": sym, "trade_ret": trade_ret})
            busy_until = T + timedelta(hours=hold)

    trades.sort(key=lambda x: x["t"])
    return trades


def _metrics(trades: list[dict]) -> dict:
    if not trades:
        return {"n": 0, "win_rate": None, "avg_ret_pct": None,
                "total_ret_pct": None, "sharpe": None, "max_dd_pct": None}
    rs = [t["trade_ret"] for t in trades]
    n = len(rs)
    wins = sum(1 for r in rs if r > 0)
    avg = sum(rs) / n
    total = sum(rs)
    if n > 1:
        var = sum((r - avg) ** 2 for r in rs) / (n - 1)
        std = math.sqrt(var)
        sharpe = (avg / std) * math.sqrt(n) if std > 1e-12 else None
    else:
        sharpe = None
    # max drawdown of cumulative per-trade equity (trades in time order)
    cum = 0.0
    peak = 0.0
    max_dd = 0.0
    for t in sorted(trades, key=lambda x: x["t"]):
        cum += t["trade_ret"]
        peak = max(peak, cum)
        max_dd = max(max_dd, peak - cum)
    return {
        "n": n,
        "win_rate": wins / n,
        "avg_ret_pct": avg * 100.0,
        "total_ret_pct": total * 100.0,
        "sharpe": sharpe,
        "max_dd_pct": max_dd * 100.0,
    }


def run_combo(prices, volumes, hours, params) -> dict:
    """Walk-forward backtest of one combo. IS = first 70% of date range,
    OOS = last 30%. Returns the columns research_runs expects."""
    if not hours:
        return {**_metrics([]), "oos_avg_ret_pct": None, "oos_n": 0}

    t0, t1 = hours[0], hours[-1]
    span = (t1 - t0).total_seconds()
    cut = t0 + timedelta(seconds=span * IS_FRACTION)

    trades = _trades_for_combo(prices, volumes, hours, params)
    is_trades = [t for t in trades if t["t"] < cut]
    oos_trades = [t for t in trades if t["t"] >= cut]

    m = _metrics(is_trades)            # IS metrics fill the main columns
    om = _metrics(oos_trades)
    m["oos_avg_ret_pct"] = om["avg_ret_pct"]
    m["oos_n"] = om["n"]
    return m
