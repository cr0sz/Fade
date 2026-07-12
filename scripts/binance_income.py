#!/usr/bin/env python3
"""
Binance Futures TRUE P&L — reads the exchange's own ledger, not our estimate.

GET /fapi/v1/income returns every cash event on the USDⓈ-M futures wallet:
REALIZED_PNL, COMMISSION (fees), FUNDING_FEE, and transfers. This is the only
honest number — recorded paper_trades.pnl is a local estimate. Use this to
answer: am I actually net positive after fees + funding?

Read-only. Signed GET with the live keys from .env. No writes, no orders.

Usage:
    python scripts/binance_income.py [--days 30] [--all-days]

  --days N    lookback window (default 30; also prints 90d)
  --all-days  pull everything available (paged), ignore --days
"""
from __future__ import annotations

import argparse
import hashlib
import hmac
import os
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
from urllib.parse import urlencode

import httpx

_LIVE_URL = "https://fapi.binance.com"
_TESTNET_URL = "https://testnet.binancefuture.com"


def _load_env() -> dict[str, str]:
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
    return cfg


def _signed_get(client: httpx.Client, base: str, secret: str, key: str,
                path: str, params: dict) -> list[dict]:
    params = dict(params)
    params["timestamp"] = int(time.time() * 1000)
    params["recvWindow"] = 5000
    query = urlencode(params)
    params["signature"] = hmac.new(secret.encode(), query.encode(), hashlib.sha256).hexdigest()
    r = client.get(f"{base}{path}", params=params, headers={"X-MBX-APIKEY": key},
                   timeout=20.0)
    r.raise_for_status()
    data = r.json()
    return data if isinstance(data, list) else []


def fetch_income(client, base, secret, key, start_ms: int) -> list[dict]:
    """All income records since start_ms, paged forward (endpoint caps at 1000/call)."""
    out: list[dict] = []
    cursor = start_ms
    while True:
        page = _signed_get(client, base, secret, key, "/fapi/v1/income",
                           {"startTime": cursor, "limit": 1000})
        if not page:
            break
        out.extend(page)
        if len(page) < 1000:
            break
        # advance past the last record's timestamp (+1ms) to avoid re-fetching it
        cursor = max(int(rec["time"]) for rec in page) + 1
        time.sleep(0.2)  # ponytail: gentle rate-limit; bump if you ever hit -1003
    return out


def summarize(records: list[dict], since_ms: int) -> dict:
    """Aggregate income (>= since_ms) by type. USDT summed as the headline;
    non-USDT assets (e.g. BNB fee deduction) reported separately so the net
    isn't silently wrong when fees are paid in BNB."""
    by_type_usdt: dict[str, float] = defaultdict(float)
    by_type_other: dict[tuple[str, str], float] = defaultdict(float)
    n = 0
    for rec in records:
        if int(rec["time"]) < since_ms:
            continue
        n += 1
        amt = float(rec["income"])
        itype = rec.get("incomeType", "?")
        asset = rec.get("asset", "?")
        if asset == "USDT":
            by_type_usdt[itype] += amt
        else:
            by_type_other[(itype, asset)] += amt
    return {"usdt": dict(by_type_usdt), "other": dict(by_type_other), "count": n}


def _print_window(label: str, s: dict) -> None:
    usdt = s["usdt"]
    realized = usdt.get("REALIZED_PNL", 0.0)
    commission = usdt.get("COMMISSION", 0.0)
    funding = usdt.get("FUNDING_FEE", 0.0)
    net = sum(v for k, v in usdt.items() if k != "TRANSFER")
    print(f"\n=== {label}  ({s['count']} ledger events) ===")
    print(f"  Realized PnL (gross): {realized:+.4f} USDT")
    print(f"  Commission (fees):    {commission:+.4f} USDT")
    print(f"  Funding fee:          {funding:+.4f} USDT")
    for k, v in sorted(usdt.items()):
        if k not in ("REALIZED_PNL", "COMMISSION", "FUNDING_FEE", "TRANSFER"):
            print(f"  {k}:{' ' * max(1, 20 - len(k))}{v:+.4f} USDT")
    print(f"  {'-' * 34}")
    print(f"  NET (excl. transfers): {net:+.4f} USDT")
    if s["other"]:
        print("  non-USDT (e.g. BNB fee deduction — convert manually for exact net):")
        for (itype, asset), v in sorted(s["other"].items()):
            print(f"    {itype} [{asset}]: {v:+.8f}")


def _selfcheck() -> None:
    recs = [
        {"time": 2000, "income": "10.0", "incomeType": "REALIZED_PNL", "asset": "USDT"},
        {"time": 2000, "income": "-0.05", "incomeType": "COMMISSION", "asset": "USDT"},
        {"time": 2000, "income": "0.30", "incomeType": "FUNDING_FEE", "asset": "USDT"},
        {"time": 2000, "income": "-100.0", "incomeType": "TRANSFER", "asset": "USDT"},
        {"time": 2000, "income": "-0.001", "incomeType": "COMMISSION", "asset": "BNB"},
        {"time": 500, "income": "999.0", "incomeType": "REALIZED_PNL", "asset": "USDT"},  # too old
    ]
    s = summarize(recs, since_ms=1000)
    net = sum(v for k, v in s["usdt"].items() if k != "TRANSFER")
    assert abs(net - 10.25) < 1e-9, net            # 10 - 0.05 + 0.30, transfer & old excluded
    assert s["count"] == 5, s["count"]             # the time=500 record dropped
    assert s["other"][("COMMISSION", "BNB")] == -0.001
    print("selfcheck OK")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=30)
    ap.add_argument("--all-days", action="store_true")
    ap.add_argument("--selfcheck", action="store_true")
    args = ap.parse_args()
    if args.selfcheck:
        _selfcheck()
        return 0

    cfg = _load_env()
    key = cfg.get("BINANCE_API_KEY", "")
    secret = cfg.get("BINANCE_API_SECRET", "")
    if not key or not secret:
        print("BINANCE_API_KEY / SECRET missing in .env", file=sys.stderr)
        return 1
    base = _TESTNET_URL if cfg.get("BINANCE_TESTNET", "true").lower() == "true" else _LIVE_URL

    now_ms = int(time.time() * 1000)
    # pull 90d once (or everything), slice locally for each window
    pull_days = 3650 if args.all_days else max(args.days, 90)
    start_ms = now_ms - pull_days * 86_400_000

    with httpx.Client() as client:
        records = fetch_income(client, base, secret, key, start_ms)

    print(f"Pulled {len(records)} income events from {base}")
    if not records:
        print("No income records — account may be new, or keys lack futures read perms.")
        return 0
    earliest = min(int(r["time"]) for r in records)
    print(f"Earliest event: {datetime.fromtimestamp(earliest/1000, tz=timezone.utc):%Y-%m-%d}")

    if args.all_days:
        _print_window("ALL TIME", summarize(records, earliest))
    else:
        _print_window(f"Last {args.days}d", summarize(records, now_ms - args.days * 86_400_000))
        _print_window("Last 90d", summarize(records, now_ms - 90 * 86_400_000))
    return 0


if __name__ == "__main__":
    sys.exit(main())
