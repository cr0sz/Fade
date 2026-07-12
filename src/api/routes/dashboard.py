"""
Phone-friendly trading dashboard — read-only JSON endpoints + static HTML.

All endpoints are read-only SELECT queries against paper_trades, ticker_quotes,
and alerts.  No computation happens at request time beyond simple aggregation.

The single validated edge of this system is "fades": trades whose router_action
is 'invert' (a TrendDetectionEngine panic-short flipped to a long).  The /fades
endpoint surfaces those specifically.

Live ($100 pilot) trades carry mode='live' and are real money — they are always
split out from paper trades.
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path

import httpx
from fastapi import APIRouter, Depends, Header, HTTPException, Query, Request
from fastapi.responses import FileResponse
from pydantic import BaseModel

from src.core.logging import get_logger

logger = get_logger(__name__)

# ── Token auth ───────────────────────────────────────────────────────────────
# All /api/v1/dashboard/* data endpoints require a token, supplied either as the
# header `X-Dashboard-Token` or the query param `?token=`. The token is read
# from the DASHBOARD_TOKEN env var. If that is unset/empty we FAIL CLOSED (401),
# never open. The static /dashboard HTML page is intentionally served WITHOUT a
# token (it is just markup + JS); every data fetch it makes is authenticated.
# This keeps the token out of the page URL (and browser history) and avoids
# query-string leakage, while still gating all real data behind the token.

def require_dashboard_token(
    x_dashboard_token: str | None = Header(default=None, alias="X-Dashboard-Token"),
    token: str | None = Query(default=None),
) -> None:
    expected = os.getenv("DASHBOARD_TOKEN", "").strip()
    if not expected:
        # Fail closed: a missing/empty server-side token must never grant access.
        raise HTTPException(
            status_code=401,
            detail="Dashboard auth not configured (DASHBOARD_TOKEN unset on server).",
        )
    supplied = (x_dashboard_token or token or "").strip()
    if supplied != expected:
        raise HTTPException(status_code=401, detail="Invalid or missing dashboard token.")


# All data endpoints inherit the token dependency from this router.
router = APIRouter(dependencies=[Depends(require_dashboard_token)])

# Quotes older than this are flagged stale — exchange-aware (see _is_stale_quote).
_STALE_CRYPTO_MIN = 90        # crypto: 90 min (24/7 market)
_STALE_STOCK_MKTHR_MIN = 90   # US stocks during market hours: 90 min
_STALE_STOCK_CLOSED_MIN = 18 * 60  # US stocks outside market hours: 18h (next open)
# US market hours: 13:30–20:00 UTC (9:30–16:00 ET)
_US_MKT_OPEN_UTC  = (13, 30)
_US_MKT_CLOSE_UTC = (20,  0)


def _us_market_open() -> bool:
    """True if UTC clock is inside regular US market hours Mon–Fri."""
    now = datetime.now(timezone.utc)
    if now.weekday() >= 5:  # Sat=5, Sun=6
        return False
    t = (now.hour, now.minute)
    return _US_MKT_OPEN_UTC <= t < _US_MKT_CLOSE_UTC


def _is_stale_quote(exchange: str, age_min: float) -> bool:
    exc = (exchange or "").upper()
    if exc in ("NASDAQ", "NYSE", "CBOE"):
        limit = _STALE_STOCK_MKTHR_MIN if _us_market_open() else _STALE_STOCK_CLOSED_MIN
    else:
        limit = _STALE_CRYPTO_MIN
    return age_min > limit


STALE_QUOTE_MINUTES = _STALE_CRYPTO_MIN  # kept for any other callers

# Funding-rate freshness: warn if the market-data-logger has not written a
# funding_rates row in this many seconds.
DATA_STALE_SECONDS = 900

# |funding_rate| at/above this is flagged "extreme" (per-interval, 8h).
EXTREME_FUNDING = 0.0005

OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://ollama:11434")

VALIDATION_API_BASE = os.getenv("VALIDATION_API_BASE", "http://validation-api:8010")


async def _proxy_validation(path: str) -> dict | list:
    """GET from the internal validation service. Never forwards auth headers. 3s timeout, graceful degradation."""
    try:
        async with httpx.AsyncClient(timeout=3.0) as client:
            resp = await client.get(f"{VALIDATION_API_BASE}/api/v1{path}")
            resp.raise_for_status()
            return resp.json()
    except Exception as exc:
        logger.warning("validation_proxy_failed path=%s error=%s", path, exc)
        return {"error": "validation service unavailable"}


_STATIC_DIR = Path(__file__).resolve().parent.parent / "static"


# ── Helpers ──────────────────────────────────────────────────────────────────

async def _latest_quotes(conn, assets: list[str]) -> dict[str, dict]:
    """Latest quote per symbol for the given assets (price + ts age)."""
    if not assets:
        return {}
    rows = await conn.fetch(
        """
        SELECT DISTINCT ON (symbol)
            symbol, price, ts,
            EXTRACT(EPOCH FROM (now() - ts)) / 60.0 AS age_min
          FROM ticker_quotes
         WHERE symbol = ANY($1::text[])
         ORDER BY symbol, ts DESC
        """,
        assets,
    )
    return {r["symbol"]: dict(r) for r in rows}


# ── Summary ──────────────────────────────────────────────────────────────────

@router.get("/dashboard/summary")
async def get_summary(request: Request) -> dict:
    """Realized PnL / win-rate / trade-count stats, split by mode (paper|live)."""
    try:
        async with request.app.state.db.acquire() as conn:
            mode_rows = await conn.fetch(
                """
                SELECT mode,
                       COUNT(*)                               AS total_trades,
                       COUNT(*) FILTER (WHERE closed_at IS NOT NULL) AS closed,
                       COUNT(*) FILTER (WHERE closed_at IS NULL)     AS open,
                       COALESCE(SUM(pnl) FILTER (WHERE closed_at IS NOT NULL), 0) AS realized_pnl,
                       COUNT(*) FILTER (WHERE closed_at IS NOT NULL AND pnl > 0)  AS wins,
                       COUNT(*) FILTER (WHERE closed_at IS NOT NULL AND pnl < 0)  AS losses,
                       -- "today" = the user's local day (UTC+3), not the UTC day
                       COALESCE(SUM(pnl) FILTER (
                           WHERE closed_at >= date_trunc('day', now() AT TIME ZONE 'Europe/Istanbul')
                                             AT TIME ZONE 'Europe/Istanbul'), 0)  AS today_pnl
                  FROM paper_trades
                 GROUP BY mode
                """
            )

            # Funding fees (futures only — live mode adjustment)
            funding_row = await conn.fetchrow(
                """
                SELECT
                    COALESCE(SUM(income), 0) AS funding_total,
                    COALESCE(SUM(income) FILTER (
                        WHERE paid_at >= date_trunc('day', now() AT TIME ZONE 'Europe/Istanbul')
                                          AT TIME ZONE 'Europe/Istanbul'), 0) AS funding_today
                FROM funding_payments
                """
            )

        by_mode: dict[str, dict] = {}
        for r in mode_rows:
            closed = r["closed"] or 0
            wins = r["wins"] or 0
            by_mode[r["mode"]] = {
                "total_trades": r["total_trades"],
                "closed": closed,
                "open": r["open"],
                "realized_pnl": round(r["realized_pnl"], 4),
                "wins": wins,
                "losses": r["losses"],
                "win_rate": round(wins / closed, 4) if closed else None,
                "today_pnl": round(r["today_pnl"], 4),
            }

        # Apply funding fees to live mode (income is signed: negative = cost)
        funding_total = float(funding_row["funding_total"])
        funding_today = float(funding_row["funding_today"])
        if "live" in by_mode:
            by_mode["live"]["realized_pnl"] = round(by_mode["live"]["realized_pnl"] + funding_total, 4)
            by_mode["live"]["today_pnl"] = round(by_mode["live"]["today_pnl"] + funding_today, 4)
            by_mode["live"]["funding_today"] = round(funding_today, 4)

        return {
            "paper": by_mode.get("paper", _empty_mode()),
            "live": by_mode.get("live", _empty_mode()),
            "sim": by_mode.get("sim", _empty_mode()),
            "has_live": "live" in by_mode,
            "has_sim": "sim" in by_mode,
        }
    except Exception as exc:
        logger.error("dashboard_summary_failed", error=str(exc))
        raise HTTPException(status_code=500, detail=str(exc))


def _empty_mode() -> dict:
    return {
        "total_trades": 0, "closed": 0, "open": 0, "realized_pnl": 0.0,
        "wins": 0, "losses": 0, "win_rate": None, "today_pnl": 0.0, "funding_today": 0.0,
    }


# ── Open positions ───────────────────────────────────────────────────────────

@router.get("/dashboard/positions")
async def get_positions(request: Request) -> dict:
    """Open positions with current price, unrealized PnL, age, stale-quote flag."""
    try:
        async with request.app.state.db.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT id, asset, exchange, direction, entry_price, size,
                       notional, fee, mode, opened_at, router_action, router_edge,
                       EXTRACT(EPOCH FROM (now() - opened_at)) / 3600.0 AS age_hours
                  FROM paper_trades
                 WHERE closed_at IS NULL
                 ORDER BY opened_at DESC
                """
            )
            quotes = await _latest_quotes(conn, [r["asset"] for r in rows])

        positions = []
        for r in rows:
            q = quotes.get(r["asset"])
            cur = q["price"] if q else None
            stale = bool(
                q and q["age_min"] is not None
                and _is_stale_quote(r["exchange"], q["age_min"])
            )
            unrealized = None
            if cur is not None:
                if r["direction"] == "long":
                    unrealized = (cur - r["entry_price"]) * r["size"]
                else:  # short
                    unrealized = (r["entry_price"] - cur) * r["size"]
                unrealized -= (r["fee"] or 0.0)
            positions.append({
                "id": r["id"],
                "asset": r["asset"],
                "exchange": r["exchange"],
                "direction": r["direction"],
                "mode": r["mode"],
                "entry_price": r["entry_price"],
                "current_price": cur,
                "size": r["size"],
                "notional": round(r["notional"], 2),
                "unrealized_pnl": round(unrealized, 4) if unrealized is not None else None,
                "age_hours": round(r["age_hours"], 2),
                "router_action": r["router_action"] or None,
                "router_edge": r["router_edge"],
                "stale_quote": stale,
                "no_quote": q is None,
                "opened_at": r["opened_at"].isoformat(),
            })

        return {"count": len(positions), "positions": positions}
    except Exception as exc:
        logger.error("dashboard_positions_failed", error=str(exc))
        raise HTTPException(status_code=500, detail=str(exc))


# ── Recent closed trades ─────────────────────────────────────────────────────

@router.get("/dashboard/trades")
async def get_trades(
    request: Request,
    limit: int = Query(50, ge=1, le=500),
    mode: str = Query(None, description="Filter by mode: paper, live, sim"),
) -> dict:
    """Recent trades (open + closed), open positions first. Optionally filter by mode."""
    try:
        async with request.app.state.db.acquire() as conn:
            if mode:
                rows = await conn.fetch(
                    """
                    SELECT id, asset, exchange, direction, entry_price, exit_price,
                           size, notional, fee, pnl, close_reason, mode,
                           router_action, router_edge, opened_at, closed_at
                      FROM paper_trades
                     WHERE mode = $2
                     ORDER BY closed_at IS NULL DESC,
                              COALESCE(closed_at, opened_at) DESC
                     LIMIT $1
                    """,
                    limit, mode,
                )
            else:
                rows = await conn.fetch(
                    """
                    SELECT id, asset, exchange, direction, entry_price, exit_price,
                           size, notional, fee, pnl, close_reason, mode,
                           router_action, router_edge, opened_at, closed_at
                      FROM paper_trades
                     ORDER BY closed_at IS NULL DESC,
                              COALESCE(closed_at, opened_at) DESC
                     LIMIT $1
                    """,
                    limit,
                )
        trades = []
        for r in rows:
            pnl_pct = None
            if r["notional"] and r["pnl"] is not None:
                pnl_pct = r["pnl"] / r["notional"] * 100.0
            trades.append({
                "id": r["id"],
                "asset": r["asset"],
                "exchange": r["exchange"],
                "direction": r["direction"],
                "mode": r["mode"],
                "entry_price": r["entry_price"],
                "exit_price": r["exit_price"],
                "pnl": round(r["pnl"], 4) if r["pnl"] is not None else None,
                "pnl_pct": round(pnl_pct, 3) if pnl_pct is not None else None,
                "close_reason": r["close_reason"],
                "router_action": r["router_action"] or None,
                "router_edge": r["router_edge"],
                "opened_at": r["opened_at"].isoformat() if r["opened_at"] else None,
                "closed_at": r["closed_at"].isoformat() if r["closed_at"] else None,
            })
        return {"count": len(trades), "trades": trades}
    except Exception as exc:
        logger.error("dashboard_trades_failed", error=str(exc))
        raise HTTPException(status_code=500, detail=str(exc))


# ── Fades (the one validated edge) ───────────────────────────────────────────

@router.get("/dashboard/fades")
async def get_fades(
    request: Request,
    limit: int = Query(50, ge=1, le=500),
) -> dict:
    """
    Trades where router_action='invert' (TDE panic-short flipped to a long) —
    the system's one validated edge. Returns history + aggregate stats.
    """
    try:
        async with request.app.state.db.acquire() as conn:
            agg = await conn.fetchrow(
                """
                SELECT COUNT(*)                                          AS total,
                       COUNT(*) FILTER (WHERE closed_at IS NOT NULL)     AS closed,
                       COUNT(*) FILTER (WHERE closed_at IS NOT NULL AND pnl > 0) AS wins,
                       COALESCE(SUM(pnl) FILTER (WHERE closed_at IS NOT NULL), 0)  AS total_pnl,
                       COALESCE(AVG(CASE WHEN closed_at IS NOT NULL AND notional > 0
                                         THEN pnl / notional * 100.0 END), 0)      AS avg_pnl_pct
                  FROM paper_trades
                 WHERE router_action = 'invert'
                """
            )
            rows = await conn.fetch(
                """
                SELECT id, asset, exchange, direction, entry_price, exit_price,
                       notional, pnl, close_reason, mode, router_edge,
                       opened_at, closed_at
                  FROM paper_trades
                 WHERE router_action = 'invert'
                 ORDER BY opened_at DESC
                 LIMIT $1
                """,
                limit,
            )

        closed = agg["closed"] or 0
        wins = agg["wins"] or 0
        history = []
        for r in rows:
            pnl_pct = (r["pnl"] / r["notional"] * 100.0) if (r["pnl"] is not None and r["notional"]) else None
            history.append({
                "id": r["id"],
                "asset": r["asset"],
                "exchange": r["exchange"],
                "direction": r["direction"],
                "mode": r["mode"],
                "entry_price": r["entry_price"],
                "exit_price": r["exit_price"],
                "pnl": round(r["pnl"], 4) if r["pnl"] is not None else None,
                "pnl_pct": round(pnl_pct, 3) if pnl_pct is not None else None,
                "close_reason": r["close_reason"],
                "router_edge": r["router_edge"],
                "open": r["closed_at"] is None,
                "opened_at": r["opened_at"].isoformat(),
                "closed_at": r["closed_at"].isoformat() if r["closed_at"] else None,
            })

        return {
            "stats": {
                "total": agg["total"],
                "closed": closed,
                "wins": wins,
                "win_rate": round(wins / closed, 4) if closed else None,
                "avg_pnl_pct": round(agg["avg_pnl_pct"], 3),
                "total_pnl": round(agg["total_pnl"], 4),
            },
            "history": history,
        }
    except Exception as exc:
        logger.error("dashboard_fades_failed", error=str(exc))
        raise HTTPException(status_code=500, detail=str(exc))


# ── Router verdict distribution ──────────────────────────────────────────────

@router.get("/dashboard/router")
async def get_router(request: Request) -> dict:
    """Router verdict distribution over the last 30 days (closed trades)."""
    try:
        async with request.app.state.db.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT COALESCE(NULLIF(router_action, ''), 'none') AS action,
                       COUNT(*)                                     AS total,
                       COUNT(*) FILTER (WHERE closed_at IS NOT NULL) AS closed,
                       COUNT(*) FILTER (WHERE closed_at IS NOT NULL AND pnl > 0) AS wins,
                       COALESCE(SUM(pnl) FILTER (WHERE closed_at IS NOT NULL), 0) AS total_pnl,
                       COALESCE(AVG(pnl) FILTER (WHERE closed_at IS NOT NULL), 0) AS avg_pnl
                  FROM paper_trades
                 WHERE opened_at > now() - interval '30 days'
                   AND mode = 'live'
                 GROUP BY action
                 ORDER BY total DESC
                """
            )
        verdicts = []
        for r in rows:
            closed = r["closed"] or 0
            wins = r["wins"] or 0
            verdicts.append({
                "action": r["action"],
                "total": r["total"],
                "closed": closed,
                "win_rate": round(wins / closed, 4) if closed else None,
                "total_pnl": round(r["total_pnl"], 4),
                "avg_pnl": round(r["avg_pnl"], 4),
            })
        return {"period_days": 30, "verdicts": verdicts}
    except Exception as exc:
        logger.error("dashboard_router_failed", error=str(exc))
        raise HTTPException(status_code=500, detail=str(exc))


# ── Daily realized PnL ───────────────────────────────────────────────────────

@router.get("/dashboard/daily")
async def get_daily(request: Request) -> dict:
    """Daily realized PnL for the last 30 days, split paper/live (bar-chart data)."""
    try:
        async with request.app.state.db.acquire() as conn:
            rows = await conn.fetch(
                """
                -- bucket by the user's local day (UTC+3) to match today_pnl
                SELECT (closed_at AT TIME ZONE 'Europe/Istanbul')::date AS day,
                       mode,
                       COALESCE(SUM(pnl), 0) AS pnl,
                       COUNT(*)              AS trades
                  FROM paper_trades
                 WHERE closed_at IS NOT NULL
                   AND closed_at > now() - interval '30 days'
                 GROUP BY day, mode
                 ORDER BY day ASC
                """
            )
        days: dict[str, dict] = {}
        for r in rows:
            d = r["day"].isoformat()
            entry = days.setdefault(d, {"day": d, "paper": 0.0, "live": 0.0, "trades": 0})
            entry[r["mode"]] = round(r["pnl"], 4)
            entry["trades"] += r["trades"]
        return {"period_days": 30, "daily": list(days.values())}
    except Exception as exc:
        logger.error("dashboard_daily_failed", error=str(exc))
        raise HTTPException(status_code=500, detail=str(exc))


# ── Alerts ───────────────────────────────────────────────────────────────────

def _alert_summary(details: dict | None) -> str | None:
    """
    Derive a short human-readable string from the validation_alerts details JSONB.
    Returns None (cheaply) when details is empty or unhelpful.
    Tries common keys written by the alert engines; falls back to a truncated dump.
    """
    if not details:
        return None
    for key in ("message", "reason", "description", "summary", "msg"):
        if key in details and details[key]:
            return str(details[key])[:120]
    # Assemble a short key=value string from the first 2 scalar values
    parts = []
    for k, v in details.items():
        if isinstance(v, (str, int, float, bool)):
            parts.append(f"{k}={v}")
        if len(parts) == 2:
            break
    return ", ".join(parts) or None


@router.get("/dashboard/alerts")
async def get_alerts(
    request: Request,
    limit: int = Query(20, ge=1, le=100),
) -> dict:
    """Most recent validation alerts, newest first (uses idx_validation_alerts_fired_at)."""
    try:
        async with request.app.state.db.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT id, alert_type, severity, engine_name,
                       details, threshold_value, observed_value,
                       fired_at, resolved_at, is_active
                  FROM validation_alerts
                 ORDER BY fired_at DESC
                 LIMIT $1
                """,
                limit,
            )
        alerts = []
        for r in rows:
            raw = r["details"]
            if isinstance(raw, str):
                try:
                    details = json.loads(raw)
                except Exception:
                    details = {}
            elif isinstance(raw, dict):
                details = raw
            else:
                details = {}
            alerts.append({
                "id": r["id"],
                "alert_type": r["alert_type"],
                "severity": r["severity"],
                "engine_name": r["engine_name"],
                "fired_at": r["fired_at"].isoformat(),
                "is_active": r["is_active"],
                "summary": _alert_summary(details),
            })
        return {"count": len(alerts), "alerts": alerts}
    except Exception as exc:
        logger.error("dashboard_alerts_failed", error=str(exc))
        raise HTTPException(status_code=500, detail=str(exc))


# ── Funding rates ────────────────────────────────────────────────────────────

@router.get("/dashboard/funding")
async def get_funding(request: Request) -> dict:
    """
    Latest funding rate per universe symbol (one row each, newest ts).
    Annualized % = rate * 3 * 365 (3 funding intervals/day). |rate| >= 0.0005
    is flagged extreme.
    """
    try:
        async with request.app.state.db.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT DISTINCT ON (symbol)
                    symbol, funding_rate, mark_price, next_funding_time, ts,
                    EXTRACT(EPOCH FROM (now() - ts)) AS age_sec
                  FROM funding_rates
                 ORDER BY symbol, ts DESC
                """
            )
        items = []
        for r in rows:
            rate = r["funding_rate"]
            items.append({
                "symbol": r["symbol"],
                "funding_rate": rate,
                "annualized_pct": round(rate * 3 * 365 * 100, 3) if rate is not None else None,
                "mark_price": r["mark_price"],
                "extreme": bool(rate is not None and abs(rate) >= EXTREME_FUNDING),
                "next_funding_time": r["next_funding_time"].isoformat() if r["next_funding_time"] else None,
                "age_sec": int(r["age_sec"]) if r["age_sec"] is not None else None,
                "ts": r["ts"].isoformat(),
            })
        # Most extreme (by abs annualized) first for at-a-glance scanning.
        items.sort(key=lambda x: abs(x["annualized_pct"] or 0), reverse=True)
        return {"count": len(items), "funding": items}
    except Exception as exc:
        logger.error("dashboard_funding_failed", error=str(exc))
        raise HTTPException(status_code=500, detail=str(exc))


# ── Liquidations ─────────────────────────────────────────────────────────────

@router.get("/dashboard/liquidations")
async def get_liquidations(request: Request) -> dict:
    """Last-24h liquidation summary: count, total notional, biggest event,
    and a 24-bucket by-hour notional histogram. Empty table handled gracefully."""
    try:
        async with request.app.state.db.acquire() as conn:
            agg = await conn.fetchrow(
                """
                SELECT COUNT(*)                       AS cnt,
                       COALESCE(SUM(notional), 0)     AS total_notional
                  FROM liquidation_events
                 WHERE event_time > now() - interval '24 hours'
                """
            )
            biggest = await conn.fetchrow(
                """
                SELECT symbol, side, price, qty, notional, event_time
                  FROM liquidation_events
                 WHERE event_time > now() - interval '24 hours'
                   AND notional IS NOT NULL
                 ORDER BY notional DESC
                 LIMIT 1
                """
            )
            hist_rows = await conn.fetch(
                """
                SELECT date_trunc('hour', event_time) AS hour,
                       COALESCE(SUM(notional), 0)      AS notional
                  FROM liquidation_events
                 WHERE event_time > now() - interval '24 hours'
                 GROUP BY hour
                """
            )

        count = agg["cnt"] or 0
        if not count:
            return {
                "count": 0,
                "total_notional": 0.0,
                "biggest": None,
                "histogram": [0.0] * 24,
                "empty": True,
            }

        # Build a 24-slot histogram aligned to whole hours ending now.
        from datetime import datetime, timedelta, timezone
        now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
        buckets = {now - timedelta(hours=23 - i): i for i in range(24)}
        histogram = [0.0] * 24
        for r in hist_rows:
            h = r["hour"]
            if h in buckets:
                histogram[buckets[h]] = round(r["notional"], 2)

        big = None
        if biggest:
            big = {
                "symbol": biggest["symbol"],
                "side": biggest["side"],
                "notional": round(biggest["notional"], 2) if biggest["notional"] is not None else None,
                "price": biggest["price"],
                "qty": biggest["qty"],
                "event_time": biggest["event_time"].isoformat() if biggest["event_time"] else None,
            }

        return {
            "count": count,
            "total_notional": round(agg["total_notional"], 2),
            "biggest": big,
            "histogram": histogram,
            "empty": False,
        }
    except Exception as exc:
        logger.error("dashboard_liquidations_failed", error=str(exc))
        raise HTTPException(status_code=500, detail=str(exc))


# ── System status ────────────────────────────────────────────────────────────

@router.get("/dashboard/system")
async def get_system(request: Request) -> dict:
    """
    Service health badges: Ollama busy state, researcher status (Redis key
    set by a future researcher service), data-logger funding freshness, and DB.
    Failures degrade gracefully (null) rather than 500'ing the tile.
    """
    # ── Ollama: busy if any model is currently loaded (GET /api/ps) ──────────
    ollama_busy: bool | None
    try:
        async with httpx.AsyncClient(timeout=2.0) as client:
            resp = await client.get(f"{OLLAMA_BASE_URL}/api/ps")
            resp.raise_for_status()
            models = resp.json().get("models", []) or []
            ollama_busy = len(models) > 0
    except Exception:
        # Connection error / timeout: state unknown, render as grey badge.
        ollama_busy = None

    # ── Researcher status (Redis string; a service sets it later) ────────────
    researcher: str | None
    try:
        researcher = await request.app.state.redis.get("freetime:status")
    except Exception:
        researcher = None

    # ── Data-logger freshness + DB ok (seconds since newest funding row) ─────
    data_age_sec: int | None = None
    db_ok = True
    try:
        async with request.app.state.db.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT EXTRACT(EPOCH FROM (now() - max(ts))) AS age FROM funding_rates"
            )
        if row and row["age"] is not None:
            data_age_sec = int(row["age"])
    except Exception as exc:
        logger.error("dashboard_system_db_failed", error=str(exc))
        db_ok = False

    return {
        "ollama_busy": ollama_busy,
        "researcher": researcher,
        "data_logger": {
            "age_sec": data_age_sec,
            "warn": bool(data_age_sec is not None and data_age_sec > DATA_STALE_SECONDS),
            "stale_threshold_sec": DATA_STALE_SECONDS,
        },
        "db_ok": db_ok,
    }


# ── Research runs ────────────────────────────────────────────────────────────

@router.get("/dashboard/research")
async def get_research(request: Request) -> dict:
    """
    Top-10 completed research_runs by OOS avg return (oos_n >= 30).
    Also returns total combo count and last-activity timestamp.
    """
    try:
        async with request.app.state.db.acquire() as conn:
            totals = await conn.fetchrow(
                """
                SELECT COUNT(*)                                        AS total,
                       COUNT(*) FILTER (WHERE status = 'done')         AS done,
                       MAX(COALESCE(finished_at, started_at))           AS last_activity
                  FROM research_runs
                """
            )
            rows = await conn.fetch(
                """
                SELECT sweep_name, params, n_trades, win_rate,
                       avg_ret_pct, total_ret_pct, sharpe, max_dd_pct,
                       oos_avg_ret_pct, oos_n, status, created_at
                  FROM research_runs
                 WHERE status = 'done'
                   AND oos_n >= 30
                 ORDER BY oos_avg_ret_pct DESC NULLS LAST
                 LIMIT 10
                """
            )

        results = []
        for r in rows:
            raw_params = r["params"]
            if isinstance(raw_params, str):
                import json as _json
                params = _json.loads(raw_params)
            else:
                params = dict(raw_params) if raw_params else {}

            is_avg = r["avg_ret_pct"]
            oos_avg = r["oos_avg_ret_pct"]
            # Unstable: IS and OOS have opposite signs (both non-None)
            unstable = (
                is_avg is not None and oos_avg is not None
                and ((is_avg > 0) != (oos_avg > 0))
            )

            results.append({
                "sweep_name": r["sweep_name"],
                "params": params,
                "params_compact": (
                    f"{params.get('drop_window_h','?')}h/"
                    f"{params.get('drop_threshold','?')}/"
                    f"{params.get('panic_cond','?')}/"
                    f"{params.get('hold_h','?')}h"
                ),
                "n_trades": r["n_trades"],
                "win_rate": round(r["win_rate"], 4) if r["win_rate"] is not None else None,
                "avg_ret_pct": round(is_avg, 3) if is_avg is not None else None,
                "oos_avg_ret_pct": round(oos_avg, 3) if oos_avg is not None else None,
                "oos_n": r["oos_n"],
                "sharpe": round(r["sharpe"], 3) if r["sharpe"] is not None else None,
                "max_dd_pct": round(r["max_dd_pct"], 3) if r["max_dd_pct"] is not None else None,
                "unstable": unstable,
                "created_at": r["created_at"].isoformat(),
            })

        return {
            "total_combos": totals["total"],
            "done_combos": totals["done"],
            "last_activity": totals["last_activity"].isoformat() if totals["last_activity"] else None,
            "top": results,
        }
    except Exception as exc:
        logger.error("dashboard_research_failed", error=str(exc))
        raise HTTPException(status_code=500, detail=str(exc))


# ── Engine intel (proxied from the internal validation service) ──────────────

@router.get("/dashboard/engine-trust")
async def get_engine_trust(request: Request) -> dict:
    """Engine trust scores and health status from the governance service."""
    trust = await _proxy_validation("/governance/trust?window_hours=24")
    return {"trust": trust if isinstance(trust, list) else []}


@router.get("/dashboard/engine-accuracy")
async def get_engine_accuracy(request: Request) -> dict:
    """Per-engine accuracy by evaluation window plus win rates."""
    accuracy = await _proxy_validation("/validation/accuracy?days=7")
    win_rates = await _proxy_validation("/feedback/engines/ranking?window_h=24")
    return {
        "accuracy": accuracy.get("engines", []) if isinstance(accuracy, dict) else [],
        "win_rates": win_rates if isinstance(win_rates, list) else [],
    }


@router.get("/dashboard/meta-risk")
async def get_meta_risk(request: Request) -> dict:
    """Composite meta-risk from the analytics service."""
    risk = await _proxy_validation("/meta/risk")
    return {
        "meta_risk": risk if isinstance(risk, dict) else {},
        "regime": None,
        "confluence": {},
    }


# ── Trade autopsy (single-trade deep dive) ───────────────────────────────────

@router.get("/dashboard/autopsy/{trade_id}")
async def get_trade_autopsy(request: Request, trade_id: str) -> dict:
    """
    Full breakdown of a single trade: mechanics, LLM audit, signal origin,
    outcome evaluation windows, trade reflections, and engine trace.
    Gracefully handles trades with no signal_id (pre-signal-era trades).
    """
    try:
        async with request.app.state.db.acquire() as conn:
            # Query 1: base trade + optional LLM audit
            row = await conn.fetchrow(
                """
                SELECT
                    pt.id, pt.asset, pt.exchange, pt.direction, pt.mode,
                    pt.entry_price, pt.exit_price, pt.size, pt.notional, pt.fee,
                    pt.pnl, pt.close_reason, pt.router_action, pt.router_edge,
                    pt.signal_id, pt.opened_at, pt.closed_at,
                    lta.verdict, lta.confidence AS audit_confidence,
                    lta.bull_case, lta.bear_case, lta.model AS audit_model,
                    lta.latency_ms AS audit_latency_ms
                  FROM paper_trades pt
                  LEFT JOIN llm_trade_audits lta ON lta.trade_id = pt.id
                 WHERE pt.id = $1
                """,
                trade_id,
            )
            if row is None:
                raise HTTPException(status_code=404, detail="Trade not found")

            signal_id = row["signal_id"]
            has_signal = bool(signal_id)

            # Query 2: signal registry + outcomes (skip when no signal_id)
            signal_row = None
            outcome_rows = []
            if has_signal:
                sig_rows = await conn.fetch(
                    """
                    SELECT
                        sr.engine_name, sr.signal_type,
                        sr.direction AS signal_direction,
                        sr.composite_score, sr.confidence_level, sr.emitted_at,
                        sr.signal_data,
                        so.evaluation_window_h, so.direction_correct,
                        so.predicted_direction, so.actual_direction,
                        so.outcome_score_delta, so.evaluated_at AS outcome_evaluated_at
                      FROM signals_registry sr
                      LEFT JOIN signal_outcomes so ON so.signal_id = sr.id
                     WHERE sr.id::text = $1
                     ORDER BY so.evaluation_window_h ASC NULLS LAST
                    """,
                    signal_id,
                )
                if sig_rows:
                    first = sig_rows[0]
                    signal_row = {
                        "engine_name": first["engine_name"],
                        "signal_type": first["signal_type"],
                        "direction": first["signal_direction"],
                        "composite_score": float(first["composite_score"]) if first["composite_score"] is not None else None,
                        "confidence_level": first["confidence_level"],
                        "emitted_at": first["emitted_at"].isoformat() if first["emitted_at"] else None,
                        "signal_data": (
                            json.loads(first["signal_data"])
                            if isinstance(first["signal_data"], str)
                            else dict(first["signal_data"])
                            if first["signal_data"]
                            else None
                        ),
                    }
                    for r in sig_rows:
                        if r["evaluation_window_h"] is not None:
                            outcome_rows.append({
                                "window_h": r["evaluation_window_h"],
                                "direction_correct": r["direction_correct"],
                                "predicted_direction": r["predicted_direction"],
                                "actual_direction": r["actual_direction"],
                                "outcome_score_delta": float(r["outcome_score_delta"]) if r["outcome_score_delta"] is not None else None,
                                "evaluated_at": r["outcome_evaluated_at"].isoformat() if r["outcome_evaluated_at"] else None,
                            })

            # Query 3: trade reflections
            reflection_rows = await conn.fetch(
                """
                SELECT lesson, hold_hours, model, created_at
                  FROM trade_reflections
                 WHERE trade_id = $1
                 ORDER BY created_at ASC
                """,
                trade_id,
            )

            # Query 4: signal trace (skip when no signal_id)
            trace_rows = []
            if has_signal:
                trace_rows = await conn.fetch(
                    """
                    SELECT engine_name, signal_type, signal_strength,
                           signal_direction, latency_ms, created_at
                      FROM signal_trace
                     WHERE signal_id = $1
                     ORDER BY created_at ASC
                     LIMIT 20
                    """,
                    signal_id,
                )

        # Build response
        notional = row["notional"]
        pnl = row["pnl"]
        pnl_pct = (pnl / notional * 100) if (notional and notional != 0) else None

        opened_at = row["opened_at"]
        closed_at = row["closed_at"]
        hold_hours = None
        if opened_at and closed_at:
            hold_hours = round((closed_at - opened_at).total_seconds() / 3600, 2)

        trade = {
            "id": row["id"],
            "asset": row["asset"],
            "exchange": row["exchange"],
            "direction": row["direction"],
            "mode": row["mode"],
            "entry_price": float(row["entry_price"]) if row["entry_price"] is not None else None,
            "exit_price": float(row["exit_price"]) if row["exit_price"] is not None else None,
            "size": float(row["size"]) if row["size"] is not None else None,
            "notional": float(notional) if notional is not None else None,
            "fee": float(row["fee"]) if row["fee"] is not None else None,
            "pnl": float(pnl) if pnl is not None else None,
            "pnl_pct": round(pnl_pct, 4) if pnl_pct is not None else None,
            "close_reason": row["close_reason"],
            "router_action": row["router_action"],
            "router_edge": float(row["router_edge"]) if row["router_edge"] is not None else None,
            "signal_id": row["signal_id"],
            "opened_at": opened_at.isoformat() if opened_at else None,
            "closed_at": closed_at.isoformat() if closed_at else None,
            "hold_hours": hold_hours,
        }

        audit = None
        if row["verdict"] is not None:
            audit = {
                "verdict": row["verdict"],
                "confidence": float(row["audit_confidence"]) if row["audit_confidence"] is not None else None,
                "bull_case": row["bull_case"],
                "bear_case": row["bear_case"],
                "model": row["audit_model"],
                "latency_ms": float(row["audit_latency_ms"]) if row["audit_latency_ms"] is not None else None,
            }

        return {
            "trade": trade,
            "audit": audit,
            "signal": signal_row,
            "outcomes": outcome_rows,
            "reflections": [
                {
                    "lesson": r["lesson"],
                    "hold_hours": float(r["hold_hours"]) if r["hold_hours"] is not None else None,
                    "model": r["model"],
                    "created_at": r["created_at"].isoformat() if r["created_at"] else None,
                }
                for r in reflection_rows
            ],
            "trace": [
                {
                    "engine_name": r["engine_name"],
                    "signal_type": r["signal_type"],
                    "signal_strength": float(r["signal_strength"]) if r["signal_strength"] is not None else None,
                    "signal_direction": r["signal_direction"],
                    "latency_ms": float(r["latency_ms"]) if r["latency_ms"] is not None else None,
                    "created_at": r["created_at"].isoformat() if r["created_at"] else None,
                }
                for r in trace_rows
            ],
        }

    except HTTPException:
        raise
    except Exception as exc:
        logger.error("dashboard_autopsy_failed", trade_id=trade_id, error=str(exc))
        raise HTTPException(status_code=500, detail=str(exc))


# ── Strategy Graveyard ────────────────────────────────────────────────────────

@router.get("/dashboard/graveyard")
async def get_graveyard(request: Request) -> dict:
    """Retired strategies and engines with reason, category, and reactivation status."""
    try:
        async with request.app.state.db.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT
                    id, strategy_name, engine_key, retired_at, sprint,
                    retirement_reason, retirement_category,
                    win_rate_at_retirement, notes,
                    reactivated_at, reactivation_notes
                  FROM retired_strategies
                 ORDER BY reactivated_at NULLS FIRST, retired_at DESC
                """
            )
        entries = []
        for r in rows:
            entries.append({
                "id": r["id"],
                "strategy_name": r["strategy_name"],
                "engine_key": r["engine_key"],
                "retired_at": r["retired_at"].isoformat(),
                "sprint": r["sprint"],
                "retirement_reason": r["retirement_reason"],
                "retirement_category": r["retirement_category"],
                "win_rate_at_retirement": (
                    float(r["win_rate_at_retirement"])
                    if r["win_rate_at_retirement"] is not None
                    else None
                ),
                "notes": r["notes"],
                "reactivated_at": r["reactivated_at"].isoformat() if r["reactivated_at"] else None,
                "reactivation_notes": r["reactivation_notes"],
                "is_reactivated": r["reactivated_at"] is not None,
            })
        still_retired = sum(1 for e in entries if not e["is_reactivated"])
        return {"count": len(entries), "still_retired": still_retired, "entries": entries}
    except Exception as exc:
        logger.error("dashboard_graveyard_failed", error=str(exc))
        raise HTTPException(status_code=500, detail=str(exc))


# ── Services health endpoint ──────────────────────────────────────────────────

_SERVICES = [
    {
        "name": "Trading Dashboard",
        "probe": "http://localhost:8000/health",
        "url": "http://localhost:8000/dashboard",
        "category": "internal",
    },
    {
        "name": "Open WebUI",
        "probe": "http://open-webui:8080/health",
        "url": "http://localhost:3003",
        "category": "ai",
    },
    {
        "name": "Uptime Kuma",
        "probe": "http://host.docker.internal:3002/api/info",
        "url": "http://localhost:3002",
        "category": "monitoring",
    },
    {
        "name": "Miniflux RSS",
        "probe": "http://market-intelligence-miniflux-1:8080/healthcheck",
        "url": "http://localhost:4000",
        "category": "monitoring",
    },
]


async def _probe_service(client: httpx.AsyncClient, svc: dict) -> dict:
    import time
    t0 = time.monotonic()
    try:
        r = await client.get(svc["probe"], timeout=3.0)
        latency_ms = round((time.monotonic() - t0) * 1000)
        ok = r.status_code < 400
    except Exception:
        latency_ms = None
        ok = False
    return {
        "name": svc["name"],
        "status": "ok" if ok else "down",
        "url": svc["url"],
        "category": svc["category"],
        "latency_ms": latency_ms,
    }


@router.get("/dashboard/services")
async def get_services(request: Request) -> dict:
    async with httpx.AsyncClient() as client:
        import asyncio as _aio
        results = await _aio.gather(*[_probe_service(client, s) for s in _SERVICES])
    up = sum(1 for r in results if r["status"] == "ok")
    return {"up": up, "total": len(results), "services": list(results)}


# ── Personal Finance ──────────────────────────────────────────────────────────

class TransactionIn(BaseModel):
    date: str  # YYYY-MM-DD
    amount_usd: float
    category: str
    description: str = ""
    source: str = ""


@router.get("/dashboard/finance")
async def get_finance(request: Request) -> dict:
    async with request.app.state.db.acquire() as conn:
        monthly = await conn.fetch("""
            SELECT category,
                   SUM(amount_usd) AS total,
                   COUNT(*) AS count
            FROM personal_transactions
            WHERE date >= date_trunc('month', CURRENT_DATE)
            GROUP BY category
            ORDER BY total DESC
        """)
        recent = await conn.fetch("""
            SELECT id, date, amount_usd, category, description, source, created_at
            FROM personal_transactions
            ORDER BY date DESC, created_at DESC
            LIMIT 20
        """)
    return {
        "monthly_summary": [dict(r) for r in monthly],
        "recent": [
            {**dict(r), "date": r["date"].isoformat(), "created_at": r["created_at"].isoformat()}
            for r in recent
        ],
    }


@router.post("/dashboard/finance/transaction")
async def add_transaction(request: Request, body: TransactionIn) -> dict:
    from datetime import date as date_type
    try:
        parsed_date = date_type.fromisoformat(body.date)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date format, use YYYY-MM-DD")
    valid_categories = {"income","housing","food","transport","subscriptions","trading_deposit","other"}
    if body.category not in valid_categories:
        raise HTTPException(status_code=400, detail=f"category must be one of {sorted(valid_categories)}")
    async with request.app.state.db.acquire() as conn:
        row = await conn.fetchrow("""
            INSERT INTO personal_transactions (date, amount_usd, category, description, source)
            VALUES ($1, $2, $3, $4, $5)
            RETURNING id
        """, parsed_date, body.amount_usd, body.category, body.description, body.source)
    return {"id": row["id"], "ok": True}


# ── Trading Journal ───────────────────────────────────────────────────────────

@router.get("/dashboard/journal")
async def get_journal(request: Request) -> dict:
    async with request.app.state.db.acquire() as conn:
        rows = await conn.fetch("""
            SELECT id, week_start, generated_at, content, model_used, trade_count, total_pnl
            FROM trading_journal
            ORDER BY week_start DESC
            LIMIT 12
        """)
    return {
        "entries": [
            {**dict(r), "week_start": r["week_start"].isoformat(),
             "generated_at": r["generated_at"].isoformat(),
             "total_pnl": float(r["total_pnl"])}
            for r in rows
        ]
    }


# ── Signal flow ──────────────────────────────────────────────────────────────

@router.get("/dashboard/signals")
async def get_signals(request: Request) -> dict:
    """Signal trace activity + engine predictive strength from signal_scores."""
    try:
        async with request.app.state.db.acquire() as conn:
            totals = await conn.fetchrow("""
                SELECT
                    COUNT(*) FILTER (WHERE created_at > now() - interval '24 hours') AS cnt_24h,
                    COUNT(*) FILTER (WHERE created_at > now() - interval '7 days')   AS cnt_7d
                FROM signal_trace
            """)
            engine_counts = await conn.fetch("""
                SELECT engine_name,
                    COUNT(*) FILTER (WHERE created_at > now() - interval '24 hours') AS cnt_24h,
                    COUNT(*) FILTER (WHERE created_at > now() - interval '7 days')   AS cnt_7d,
                    COUNT(*) FILTER (WHERE created_at > now() - interval '24 hours'
                                          AND signal_direction = 'long')  AS longs_24h,
                    COUNT(*) FILTER (WHERE created_at > now() - interval '24 hours'
                                          AND signal_direction = 'short') AS shorts_24h,
                    ROUND(AVG(signal_strength)
                          FILTER (WHERE created_at > now() - interval '24 hours')::numeric, 3)
                          AS avg_strength_24h
                FROM signal_trace
                WHERE created_at > now() - interval '7 days'
                GROUP BY engine_name
                ORDER BY cnt_24h DESC NULLS LAST
            """)
            scores = await conn.fetch("""
                SELECT DISTINCT ON (engine_name, signal_type)
                    engine_name, signal_type, window_hours,
                    accuracy, predictive_strength, half_life_hours, sample_size, computed_at
                FROM signal_scores
                WHERE computed_at > now() - interval '7 days'
                ORDER BY engine_name, signal_type, computed_at DESC
                LIMIT 60
            """)
            recent = await conn.fetch("""
                SELECT ticker, engine_name, signal_type, signal_direction,
                       signal_strength, latency_ms, created_at
                FROM signal_trace
                ORDER BY created_at DESC
                LIMIT 50
            """)
        return {
            "totals": {"cnt_24h": totals["cnt_24h"] or 0, "cnt_7d": totals["cnt_7d"] or 0},
            "engines": [
                {k: (round(float(v), 3) if isinstance(v, float) else v)
                 for k, v in dict(r).items()}
                for r in engine_counts
            ],
            "scores": [
                {**{k: (float(v) if isinstance(v, float) else v) for k, v in dict(r).items()},
                 "computed_at": r["computed_at"].isoformat()}
                for r in scores
            ],
            "recent": [
                {**dict(r), "created_at": r["created_at"].isoformat()}
                for r in recent
            ],
        }
    except Exception as exc:
        logger.error("dashboard_signals_failed", error=str(exc))
        raise HTTPException(status_code=500, detail=str(exc))


# ── Daily briefs ──────────────────────────────────────────────────────────────

@router.get("/dashboard/brief")
async def get_brief(request: Request) -> dict:
    """Last 7 daily briefs generated by the scheduler."""
    try:
        async with request.app.state.db.acquire() as conn:
            rows = await conn.fetch("""
                SELECT id, generated_at, content, model_used, trade_count, total_pnl, btc_price
                FROM daily_briefs
                ORDER BY generated_at DESC
                LIMIT 7
            """)
        return {
            "entries": [
                {**dict(r),
                 "generated_at": r["generated_at"].isoformat(),
                 "total_pnl": float(r["total_pnl"]) if r["total_pnl"] is not None else None,
                 "btc_price": float(r["btc_price"]) if r["btc_price"] is not None else None}
                for r in rows
            ]
        }
    except Exception as exc:
        logger.error("dashboard_brief_failed", error=str(exc))
        raise HTTPException(status_code=500, detail=str(exc))


# ── Backtest ──────────────────────────────────────────────────────────────────

@router.get("/dashboard/backtest/summary")
async def get_backtest_summary(request: Request) -> dict:
    data = await _proxy_validation("/backtest/summary")
    return data if isinstance(data, dict) else {}


@router.get("/dashboard/backtest/engines")
async def get_backtest_engines(request: Request) -> dict:
    data = await _proxy_validation("/backtest/engines")
    return data if isinstance(data, dict) else {"engines": []}


@router.get("/dashboard/backtest/runs")
async def get_backtest_runs(request: Request) -> dict:
    data = await _proxy_validation("/backtest/replay/runs")
    return data if isinstance(data, dict) else {"runs": []}


# ── Static HTML page (registered at root level in main.py, not /api/v1) ──────
# The page itself is served UNAUTHENTICATED (it is only HTML/JS). All data it
# fetches goes through the token-gated /api/v1/dashboard/* router above, so the
# token never appears in the page URL or browser history.

page_router = APIRouter()


@page_router.get("/dashboard", include_in_schema=False)
async def dashboard_page() -> FileResponse:
    """Serve the single-page phone-friendly dashboard (no token required)."""
    return FileResponse(_STATIC_DIR / "dashboard.html", media_type="text/html")


_GRAPHIFY_DIR = Path("/app/graphify-out")


@page_router.get("/graphify", include_in_schema=False)
async def graphify_page() -> FileResponse:
    """Serve the graphify knowledge-graph viewer (self-contained HTML)."""
    return FileResponse(_GRAPHIFY_DIR / "graph.html", media_type="text/html")
