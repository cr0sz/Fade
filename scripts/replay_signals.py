#!/usr/bin/env python3
"""
Historical Signal Replay — Option 2 of the Calibration Speedup Plan.

Walks through the historical ticker_quotes data already loaded by
ingest_binance_history.py, computes technical signals at 4-hour intervals
for each CRYPTO ticker, writes them to signals_registry, then cross-references
actual price outcomes and writes to signal_outcomes.

Effect: simulates months of live running overnight. The BacktestWorker and
ConfidenceEngine immediately have calibration data instead of waiting weeks.

Usage:
    python scripts/replay_signals.py [--days 180] [--interval 4] [--dry-run] [--symbols BTC ETH]

Phases:
    Phase 1 — Signal generation: walk ticker_quotes, emit a signal every
              INTERVAL hours, bulk-insert into signals_registry.
    Phase 2 — Outcome tagging: for each signal find the price N hours later
              in ticker_quotes, mark direction_correct, insert signal_outcomes.

The script is resumable: it skips timestamps already processed for each ticker.
"""
from __future__ import annotations

import argparse
import asyncio
import bisect
import json
import math
import os
from datetime import datetime, timedelta, timezone
from typing import Any

import asyncpg

# ── Symbols ───────────────────────────────────────────────────────────────────

CRYPTO_SYMBOLS = [
    "BTC", "ETH", "SOL", "BNB", "XRP", "AVAX", "ADA", "DOGE",
    "DOT", "LINK", "MATIC", "ARB", "OP", "UNI", "AAVE", "SUI", "TON",
]

# ── Config ────────────────────────────────────────────────────────────────────

ENGINE_NAME   = "technical_replay"
SIGNAL_TYPE   = "trend"
SOURCE_TABLE  = "ticker_quotes"

# Emit a signal every N hours (sliding-window step)
DEFAULT_INTERVAL_H = 4

# Minimum price rows before the first signal — ensures RSI(14) + 144-tick
# momentum lookback both have enough history.
MIN_HISTORY = 200

# Only emit signals when |composite| exceeds this threshold (noise filter)
SIGNAL_THRESHOLD = 0.10

# Windows to evaluate (must include all EVAL_WINDOWS from backtest_engine.py)
EVAL_WINDOWS = [4, 24, 72]

# Price return thresholds for direction_correct determination
# 0.2% move = meaningful directional signal; below = neutral / noise
RETURN_THRESHOLD = 0.002

# ── Indicator functions (copied from technical_engine.py — pure functions) ───

def _rsi(prices: list[float], period: int = 14) -> float:
    n = min(period, len(prices) - 1)
    if n < 2:
        return 50.0
    deltas = [prices[i] - prices[i - 1] for i in range(1, len(prices))]
    gains  = [max(0.0, d) for d in deltas[-n:]]
    losses = [abs(min(0.0, d)) for d in deltas[-n:]]
    avg_gain = sum(gains)  / n
    avg_loss = sum(losses) / n
    if avg_loss == 0:
        return 100.0
    return 100.0 - 100.0 / (1.0 + avg_gain / avg_loss)


def _rsi_signal(rsi_val: float) -> float:
    if rsi_val <= 30:
        return min(1.0, (30 - rsi_val) / 30)
    if rsi_val >= 70:
        return max(-1.0, -(rsi_val - 70) / 30)
    return (50 - rsi_val) / 50 * 0.25


def _momentum(prices: list[float], lookbacks: list[int]) -> float:
    if len(prices) < 2:
        return 0.0
    current = prices[-1]
    sigs = []
    for lb in lookbacks:
        if len(prices) > lb:
            ret = (current - prices[-lb - 1]) / max(abs(prices[-lb - 1]), 1e-9)
            sigs.append(math.tanh(ret / 0.01))
    return sum(sigs) / len(sigs) if sigs else 0.0


def _bollinger(prices: list[float], period: int = 20) -> float:
    if len(prices) < period:
        return 0.0
    window = prices[-period:]
    mean   = sum(window) / period
    std    = math.sqrt(sum((p - mean) ** 2 for p in window) / period)
    if std == 0:
        return 0.0
    z = (prices[-1] - mean) / std
    return max(-1.0, min(1.0, -z / 2.0))


def _vol_signal(volumes: list[float]) -> float:
    if len(volumes) < 2:
        return 0.0
    current = volumes[-1]
    avg     = sum(volumes[:-1]) / (len(volumes) - 1)
    if avg == 0:
        return 0.0
    ratio = max(current / avg, 1e-6)
    return round(math.tanh(math.log(ratio)), 4)


def _compute_tech_score(prices: list[float], volumes: list[float]) -> dict[str, float]:
    """Compute all technical indicators and combine into a composite score.

    Uses CRYPTO weights renormalized to exclude news/social (which are unavailable
    historically): w_vol = 0.25/0.80 ≈ 0.3125, w_tech = 0.55/0.80 ≈ 0.6875.
    """
    rsi_s  = _rsi_signal(_rsi(prices))
    mom_s  = _momentum(prices, [12, 48, 144])   # 1h / 4h / 12h at 1-hr candles
    bb_s   = _bollinger(prices)
    vol_s  = _vol_signal(volumes)

    # Combine with CRYPTO weights (no funding rate — no historical data available)
    tech_score = round(max(-1.0, min(1.0,
        0.30 * rsi_s + 0.30 * mom_s + 0.15 * bb_s + 0.10 * vol_s
    )), 4)

    # Volume score: latest vs rolling avg (from TrendDetection formula)
    if len(volumes) >= 2:
        latest_vol = volumes[-1]
        avg_vol    = sum(volumes[1:]) / (len(volumes) - 1)
        volume_score = round(math.tanh(math.log(max(latest_vol / max(avg_vol, 1e-9), 1e-6))), 4)
    else:
        volume_score = 0.0

    # Full composite: CRYPTO weights renormalized to sum to 1.0.
    # Live system uses vol=0.25, tech=0.55, news=0.10, social=0.10 (sum=1.0).
    # Without news/social history, renormalize: vol=0.25/0.80=0.3125, tech=0.55/0.80=0.6875.
    # The prior weights (0.25+0.55=0.80) systematically understated composite magnitude,
    # biasing replay signals toward the neutral band and degrading calibration quality.
    composite = round(max(-1.0, min(1.0,
        0.3125 * volume_score + 0.6875 * tech_score
    )), 4)

    rsi_val = _rsi(prices)
    return {
        "tech_score":    tech_score,
        "volume_score":  volume_score,
        "composite":     composite,
        "rsi":           round(rsi_val, 2),
        "rsi_signal":    round(rsi_s, 4),
        "momentum":      round(mom_s, 4),
        "bollinger":     round(bb_s, 4),
        "vol_signal":    round(vol_s, 4),
    }


# ── DB helpers ────────────────────────────────────────────────────────────────

def _load_db_url() -> str:
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
    if host not in ("localhost", "127.0.0.1"):
        host = "localhost"
    port = cfg.get("POSTGRES_PORT", "5432")
    db   = cfg.get("POSTGRES_DB",   "mktintel")
    user = cfg.get("POSTGRES_USER", "mktintel")
    pw   = cfg.get("POSTGRES_PASSWORD", "mktintel")
    return f"postgresql://{user}:{pw}@{host}:{port}/{db}"


async def _load_quotes(conn: asyncpg.Connection, symbol: str) -> list[dict]:
    rows = await conn.fetch(
        """
        SELECT price, volume, ts
          FROM ticker_quotes
         WHERE symbol = $1 AND price > 0
         ORDER BY ts ASC
        """,
        symbol,
    )
    return [dict(r) for r in rows]


async def _last_replayed_ts(conn: asyncpg.Connection, symbol: str) -> datetime | None:
    row = await conn.fetchrow(
        """
        SELECT MAX(emitted_at) AS last_ts
          FROM signals_registry
         WHERE ticker = $1 AND engine_name = $2
        """,
        symbol, ENGINE_NAME,
    )
    return row["last_ts"] if row and row["last_ts"] else None


# ── Phase 1 — Signal generation ───────────────────────────────────────────────

def _confidence_level(score: float) -> str:
    abs_s = abs(score)
    if abs_s >= 0.50:
        return "high"
    if abs_s >= 0.30:
        return "medium"
    return "low"


def _generate_signals(
    symbol: str,
    quotes: list[dict],
    interval_h: int,
    resume_after: datetime | None,
    days_back: int,
) -> list[dict]:
    """Walk the quotes list and emit a signal every interval_h hours."""
    if len(quotes) < MIN_HISTORY:
        return []

    cutoff_start = datetime.now(timezone.utc) - timedelta(days=days_back)
    signals: list[dict] = []
    last_signal_ts: datetime | None = resume_after

    for i in range(MIN_HISTORY, len(quotes)):
        ts = quotes[i]["ts"]
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)

        # Skip rows before our cutoff window
        if ts < cutoff_start:
            continue

        # Skip already-replayed timestamps
        if resume_after and ts <= resume_after:
            continue

        # Emit every interval_h hours
        if last_signal_ts is not None:
            elapsed_h = (ts - last_signal_ts).total_seconds() / 3600
            if elapsed_h < interval_h:
                continue

        window_quotes = quotes[max(0, i - 300):i + 1]
        prices  = [float(q["price"])  for q in window_quotes]
        volumes = [float(q["volume"]) for q in window_quotes]

        indicators = _compute_tech_score(prices, volumes)
        composite  = indicators["composite"]

        # Skip neutral signals — no edge to measure
        if abs(composite) < SIGNAL_THRESHOLD:
            last_signal_ts = ts
            continue

        direction = "risk_on" if composite > 0 else "risk_off"

        signals.append({
            "ticker":           symbol,
            "direction":        direction,
            "composite_score":  composite,
            "confidence_level": _confidence_level(composite),
            "signal_data":      {
                **indicators,
                "prices_count": len(prices),
                "replay":       True,
            },
            "emitted_at":       ts,
        })
        last_signal_ts = ts

    return signals


async def _insert_signals(
    conn: asyncpg.Connection, signals: list[dict]
) -> int:
    if not signals:
        return 0
    rows = [
        (
            SIGNAL_TYPE,
            ENGINE_NAME,
            s["ticker"],
            s["direction"],
            s["composite_score"],
            s["confidence_level"],
            json.dumps(s["signal_data"]),
            SOURCE_TABLE,
            s["emitted_at"],
        )
        for s in signals
    ]
    result = await conn.executemany(
        """
        INSERT INTO signals_registry
            (signal_type, engine_name, ticker, direction,
             composite_score, confidence_level, signal_data,
             source_table, emitted_at)
        VALUES ($1,$2,$3,$4,$5,$6,$7::jsonb,$8,$9)
        """,
        rows,
    )
    return len(rows)


# ── Phase 2 — Outcome tagging ─────────────────────────────────────────────────

def _find_price_at(
    quotes: list[dict],
    target_ts: datetime,
    tolerance_h: float = 1.5,
) -> float | None:
    """Binary-search the sorted quotes list for the price closest to target_ts."""
    if not quotes:
        return None
    tol = timedelta(hours=tolerance_h)
    # Find insertion point
    lo, hi = 0, len(quotes) - 1
    while lo < hi:
        mid = (lo + hi) // 2
        ts  = quotes[mid]["ts"]
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        if ts < target_ts:
            lo = mid + 1
        else:
            hi = mid

    # Check lo and lo-1
    best_idx, best_diff = None, timedelta(days=999)
    for idx in (lo - 1, lo):
        if 0 <= idx < len(quotes):
            ts = quotes[idx]["ts"]
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            diff = abs(ts - target_ts)
            if diff < best_diff and diff <= tol:
                best_diff = diff
                best_idx  = idx

    return float(quotes[best_idx]["price"]) if best_idx is not None else None


async def _fetch_inserted_signals(
    conn: asyncpg.Connection, symbol: str
) -> list[dict]:
    rows = await conn.fetch(
        """
        SELECT id, composite_score, direction, emitted_at
          FROM signals_registry
         WHERE ticker = $1 AND engine_name = $2
         ORDER BY emitted_at ASC
        """,
        symbol, ENGINE_NAME,
    )
    return [dict(r) for r in rows]


async def _insert_outcomes(
    conn: asyncpg.Connection,
    signals: list[dict],
    quotes: list[dict],
) -> int:
    outcome_rows: list[tuple] = []

    for sig in signals:
        emitted_at  = sig["emitted_at"]
        signal_price_row = _find_price_at(quotes, emitted_at, tolerance_h=1.5)
        if signal_price_row is None:
            continue

        # We stored composite_score as the price here — extract signal_price
        # from quotes instead
        signal_price = signal_price_row  # this IS a price float from _find_price_at

        predicted_pol = 1 if sig["direction"] == "risk_on" else -1

        for window_h in EVAL_WINDOWS:
            target_ts    = emitted_at + timedelta(hours=window_h)
            future_price = _find_price_at(quotes, target_ts, tolerance_h=1.5)
            if future_price is None:
                continue  # data not yet available — skip

            price_return = (future_price - signal_price) / signal_price

            if price_return > RETURN_THRESHOLD:
                actual_pol     = 1
                actual_dir     = "risk_on"
            elif price_return < -RETURN_THRESHOLD:
                actual_pol     = -1
                actual_dir     = "risk_off"
            else:
                actual_pol     = 0
                actual_dir     = "neutral"

            if predicted_pol == 0 or actual_pol == 0:
                direction_correct = None
            else:
                direction_correct = (predicted_pol == actual_pol)

            outcome_rows.append((
                sig["id"],
                sig["ticker"] if "ticker" in sig else None,
                window_h,
                sig["direction"],
                actual_dir,
                direction_correct,
                sig["composite_score"],
                round(price_return, 6),
            ))

    if not outcome_rows:
        return 0

    await conn.executemany(
        """
        INSERT INTO signal_outcomes (
            signal_id, ticker, evaluation_window_h,
            predicted_direction, actual_direction,
            direction_correct, predicted_score, outcome_score_delta
        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
        ON CONFLICT (signal_id, evaluation_window_h) DO NOTHING
        """,
        outcome_rows,
    )
    return len(outcome_rows)


# ── Main ──────────────────────────────────────────────────────────────────────

async def main(
    symbols: list[str],
    days_back: int,
    interval_h: int,
    dry_run: bool,
) -> None:
    db_url = _load_db_url()
    pool   = await asyncpg.create_pool(db_url, min_size=2, max_size=5, command_timeout=60)

    print(f"\nHistorical Signal Replay")
    print(f"  Engine    : {ENGINE_NAME}")
    print(f"  Symbols   : {', '.join(symbols)}")
    print(f"  Days back : {days_back}")
    print(f"  Interval  : every {interval_h}h")
    print(f"  Dry run   : {dry_run}")
    print(f"  Windows   : {EVAL_WINDOWS}\n")

    total_signals  = 0
    total_outcomes = 0

    for symbol in symbols:
        async with pool.acquire() as conn:
            quotes       = await _load_quotes(conn, symbol)
            resume_after = await _last_replayed_ts(conn, symbol)

        if not quotes:
            print(f"  [{symbol}] no quotes — skipped")
            continue

        print(f"  [{symbol}] {len(quotes):,} quotes  "
              f"({quotes[0]['ts'].date()} → {quotes[-1]['ts'].date()})  "
              f"resume_after={resume_after.isoformat() if resume_after else 'start'}")

        # ── Phase 1: generate signals ────────────────────────────────────────
        signals = _generate_signals(symbol, quotes, interval_h, resume_after, days_back)
        print(f"  [{symbol}]   phase1: {len(signals):,} signals generated")

        if not dry_run and signals:
            async with pool.acquire() as conn:
                n = await _insert_signals(conn, signals)
            print(f"  [{symbol}]   phase1: {n:,} rows inserted → signals_registry")
        total_signals += len(signals)

        if dry_run:
            print(f"  [{symbol}]   phase2: skipped (dry run)")
            continue

        # ── Phase 2: compute outcomes ────────────────────────────────────────
        async with pool.acquire() as conn:
            db_signals = await _fetch_inserted_signals(conn, symbol)

        # Attach ticker to each signal row (asyncpg records don't always include it)
        for s in db_signals:
            s.setdefault("ticker", symbol)

        async with pool.acquire() as conn:
            n_out = await _insert_outcomes(conn, db_signals, quotes)
        print(f"  [{symbol}]   phase2: {n_out:,} outcome rows inserted → signal_outcomes")
        total_outcomes += n_out

    # ── Summary ──────────────────────────────────────────────────────────────
    if not dry_run:
        async with pool.acquire() as conn:
            sig_count = await conn.fetchval(
                "SELECT COUNT(*) FROM signals_registry WHERE engine_name = $1", ENGINE_NAME
            )
            out_count = await conn.fetchval(
                """
                SELECT COUNT(*) FROM signal_outcomes so
                  JOIN signals_registry sr ON sr.id = so.signal_id
                 WHERE sr.engine_name = $1
                """,
                ENGINE_NAME,
            )
            acc_rows = await conn.fetch(
                """
                SELECT so.evaluation_window_h,
                       COUNT(*) FILTER (WHERE so.direction_correct = true)::float
                         / NULLIF(COUNT(*) FILTER (WHERE so.direction_correct IS NOT NULL), 0)
                           AS accuracy,
                       COUNT(*) FILTER (WHERE so.direction_correct IS NOT NULL) AS evaluated
                  FROM signal_outcomes so
                  JOIN signals_registry sr ON sr.id = so.signal_id
                 WHERE sr.engine_name = $1
                 GROUP BY so.evaluation_window_h
                 ORDER BY so.evaluation_window_h
                """,
                ENGINE_NAME,
            )

        print(f"\n  Total signals in DB  : {sig_count:,}")
        print(f"  Total outcomes in DB : {out_count:,}")
        print(f"\n  Accuracy by window:")
        for r in acc_rows:
            acc = r["accuracy"]
            acc_str = f"{acc*100:.1f}%" if acc is not None else "n/a"
            print(f"    {r['evaluation_window_h']:>3}h : {acc_str}  (n={r['evaluated']:,})")
    else:
        print(f"\n  Would have emitted {total_signals:,} signals (dry run).")

    await pool.close()
    print("\nDone.")


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Replay historical signals from ticker_quotes")
    p.add_argument("--days",     type=int, default=180,
                   help="Days of history to replay (default: 180)")
    p.add_argument("--interval", type=int, default=DEFAULT_INTERVAL_H,
                   help=f"Signal emission interval in hours (default: {DEFAULT_INTERVAL_H})")
    p.add_argument("--symbols",  nargs="+", default=CRYPTO_SYMBOLS,
                   help="Symbols to replay (default: all CRYPTO)")
    p.add_argument("--dry-run",  action="store_true",
                   help="Generate signals but do not write to DB")
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    asyncio.run(main(
        symbols=args.symbols,
        days_back=args.days,
        interval_h=args.interval,
        dry_run=args.dry_run,
    ))
