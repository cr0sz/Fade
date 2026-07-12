#!/usr/bin/env python3
"""
Binance Historical Price Ingest — Option 1 of the Calibration Speedup Plan.

Pulls up to DAYS_BACK days of hourly OHLCV candles from the Binance public
klines endpoint (no API key required) and bulk-inserts them into ticker_quotes.

Usage:
    python scripts/ingest_binance_history.py [--days 200] [--interval 1h] [--dry-run]

After this script completes:
  - BTC 200-SMA regime gate works from day 1 instead of being blind for weeks.
  - The backtest script has real historical prices to cross-reference against
    signals_registry entries.
  - TechnicalEngine RSI / momentum / Bollinger Band have rich price history.
"""
from __future__ import annotations

import argparse
import asyncio
import os
import sys
import time
from datetime import datetime, timezone
from typing import Any

import asyncpg
import httpx

# ── Symbols ──────────────────────────────────────────────────────────────────
# All CRYPTO-exchange tickers tracked in the system (symbol → Binance pair).
# Binance lists MATIC as MATICUSDT (now also POLUSDT); keep MATIC for DB symbol.
SYMBOL_MAP: dict[str, str] = {
    "BTC":  "BTCUSDT",
    "ETH":  "ETHUSDT",
    "SOL":  "SOLUSDT",
    "BNB":  "BNBUSDT",
    "XRP":  "XRPUSDT",
    "AVAX": "AVAXUSDT",
    "ADA":  "ADAUSDT",
    "DOGE": "DOGEUSDT",
    "DOT":  "DOTUSDT",
    "LINK": "LINKUSDT",
    "MATIC":"POLUSDT",    # Binance renamed MATIC → POL; store as MATIC in DB
    "ARB":  "ARBUSDT",
    "OP":   "OPUSDT",
    "UNI":  "UNIUSDT",
    "AAVE": "AAVEUSDT",
    "SUI":  "SUIUSDT",
    "TON":  "TONUSDT",
}

BINANCE_KLINES_URL = "https://api.binance.com/api/v3/klines"
CANDLES_PER_REQUEST = 1000   # Binance max per call
RATE_LIMIT_SLEEP    = 0.15   # seconds between requests (~6 req/s, well under limit)
BATCH_INSERT_SIZE   = 500    # rows per INSERT statement


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


async def _existing_range(conn: asyncpg.Connection, symbol: str) -> tuple[datetime | None, datetime | None]:
    row = await conn.fetchrow(
        "SELECT MIN(ts), MAX(ts) FROM ticker_quotes WHERE symbol = $1", symbol
    )
    return row[0], row[1]


async def _bulk_insert(conn: asyncpg.Connection, rows: list[tuple]) -> int:
    """Insert a batch of (symbol,price,change_pct,volume,market_cap,exchange,currency,ts) rows."""
    if not rows:
        return 0
    await conn.executemany(
        """
        INSERT INTO ticker_quotes (symbol, price, change_pct, volume, market_cap, exchange, currency, ts)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        ON CONFLICT DO NOTHING
        """,
        rows,
    )
    return len(rows)


# ── Binance fetch ─────────────────────────────────────────────────────────────

def _parse_candles(symbol: str, raw: list[list[Any]]) -> list[tuple]:
    """Convert raw Binance kline arrays into ticker_quotes row tuples."""
    rows: list[tuple] = []
    for candle in raw:
        # Binance kline: [open_time, open, high, low, close, volume, close_time,
        #                  quote_asset_volume, num_trades, ...]
        open_time_ms  = int(candle[0])
        close_time_ms = int(candle[6])
        open_price    = float(candle[1])
        close_price   = float(candle[4])
        quote_volume  = float(candle[7])   # USD volume (quote asset = USDT)

        change_pct = ((close_price - open_price) / open_price * 100.0) if open_price else 0.0

        # Use candle close time as the timestamp so it aligns with when the
        # price data becomes observable.
        ts = datetime.fromtimestamp(close_time_ms / 1000, tz=timezone.utc)

        rows.append((
            symbol,
            close_price,
            change_pct,
            quote_volume,
            None,          # market_cap — not in klines
            "CRYPTO",
            "USD",
            ts,
        ))
    return rows


async def fetch_symbol_history(
    client: httpx.AsyncClient,
    db_symbol: str,
    binance_pair: str,
    interval: str,
    start_ms: int,
    end_ms: int,
    dry_run: bool,
    pool: asyncpg.Pool,
) -> int:
    """Fetch all candles for one symbol and insert into DB. Returns total rows inserted."""
    total_inserted = 0
    current_start  = start_ms

    print(f"  [{db_symbol}] fetching {binance_pair} from "
          f"{datetime.fromtimestamp(start_ms/1000, tz=timezone.utc).date()} "
          f"to {datetime.fromtimestamp(end_ms/1000, tz=timezone.utc).date()} ...")

    while current_start < end_ms:
        params = {
            "symbol":    binance_pair,
            "interval":  interval,
            "startTime": current_start,
            "endTime":   end_ms,
            "limit":     CANDLES_PER_REQUEST,
        }
        try:
            resp = await client.get(BINANCE_KLINES_URL, params=params, timeout=30)
            resp.raise_for_status()
            candles = resp.json()
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 400:
                # Symbol not listed or invalid pair — skip silently
                print(f"  [{db_symbol}] skipped — pair {binance_pair} not found on Binance")
                return 0
            raise

        if not candles:
            break

        rows = _parse_candles(db_symbol, candles)

        if not dry_run:
            async with pool.acquire() as conn:
                inserted = await _bulk_insert(conn, rows)
            total_inserted += inserted
        else:
            total_inserted += len(rows)

        # Advance window past the last candle's close time
        last_close_ms = int(candles[-1][6])
        current_start = last_close_ms + 1

        if len(candles) < CANDLES_PER_REQUEST:
            break

        await asyncio.sleep(RATE_LIMIT_SLEEP)

    return total_inserted


# ── Main ──────────────────────────────────────────────────────────────────────

async def main(days_back: int, interval: str, dry_run: bool, symbols: list[str]) -> None:
    db_url = _load_db_url()

    now_ms    = int(time.time() * 1000)
    start_ms  = now_ms - days_back * 24 * 3600 * 1000

    print(f"\nBinance Historical Ingest")
    print(f"  Interval  : {interval}")
    print(f"  Days back : {days_back}")
    print(f"  Symbols   : {', '.join(symbols)}")
    print(f"  Dry run   : {dry_run}")
    print()

    pool = await asyncpg.create_pool(db_url, min_size=2, max_size=5, command_timeout=60)

    # Show existing data summary before ingest
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT COUNT(*), MIN(ts), MAX(ts) FROM ticker_quotes")
        print(f"  ticker_quotes before: {row[0]:,} rows  "
              f"({row[1]} → {row[2]})\n")

    grand_total = 0
    async with httpx.AsyncClient() as client:
        for db_symbol in symbols:
            binance_pair = SYMBOL_MAP.get(db_symbol)
            if not binance_pair:
                print(f"  [{db_symbol}] no Binance pair mapping — skipped")
                continue

            try:
                n = await fetch_symbol_history(
                    client, db_symbol, binance_pair, interval,
                    start_ms, now_ms, dry_run, pool,
                )
                print(f"  [{db_symbol}] {'would insert' if dry_run else 'inserted'} {n:,} rows")
                grand_total += n
            except Exception as exc:
                print(f"  [{db_symbol}] ERROR: {exc}")

    # Show result summary
    if not dry_run:
        async with pool.acquire() as conn:
            row = await conn.fetchrow("SELECT COUNT(*), MIN(ts), MAX(ts) FROM ticker_quotes")
            print(f"\n  ticker_quotes after : {row[0]:,} rows  "
                  f"({row[1]} → {row[2]})")

    await pool.close()
    print(f"\nDone — {'would have inserted' if dry_run else 'inserted'} {grand_total:,} total rows.")


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Bulk-ingest Binance historical OHLCV into ticker_quotes")
    p.add_argument("--days",     type=int, default=200,
                   help="How many days of history to pull (default: 200)")
    p.add_argument("--interval", default="1h",
                   choices=["1m","5m","15m","30m","1h","4h","1d"],
                   help="Candle interval (default: 1h)")
    p.add_argument("--symbols",  nargs="+", default=list(SYMBOL_MAP.keys()),
                   help="Subset of symbols to ingest (default: all)")
    p.add_argument("--dry-run",  action="store_true",
                   help="Fetch candles but do not write to DB")
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    asyncio.run(main(
        days_back=args.days,
        interval=args.interval,
        dry_run=args.dry_run,
        symbols=args.symbols,
    ))
