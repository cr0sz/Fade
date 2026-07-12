"""Daily phone-glanceable digest sent to Discord #watchdog (Sprint 49).

Sent at 07:00 UTC daily (the watchdog loop calls maybe_send_digest each cycle;
the Redis marker watchdog:digest_sent=<YYYY-MM-DD> prevents double-sends).
Kept under ~25 lines, monospace-friendly.
"""
from __future__ import annotations

from datetime import datetime, timezone

from src.core.logging import get_logger
from src.watchdog.alerts import send_telegram

logger = get_logger(__name__)

DIGEST_SENT_KEY = "watchdog:digest_sent"
DIGEST_HOUR_UTC = 7


def _fmt_money(v) -> str:
    v = v or 0.0
    return f"{v:+.2f}"


async def build_digest(db) -> str:
    async with db.acquire() as conn:
        # ── Paper ──────────────────────────────────────────────────────────
        paper_24h = await conn.fetchval(
            """SELECT COALESCE(SUM(pnl),0) FROM paper_trades
               WHERE mode='paper' AND closed_at > now()-interval '24 hours'"""
        )
        paper_total = await conn.fetchval(
            "SELECT COALESCE(SUM(pnl),0) FROM paper_trades WHERE mode='paper' AND closed_at IS NOT NULL"
        )
        paper_open = await conn.fetchval(
            "SELECT COUNT(*) FROM paper_trades WHERE mode='paper' AND closed_at IS NULL"
        )
        wr_row = await conn.fetchrow(
            """SELECT COUNT(*) FILTER (WHERE pnl>0) AS wins, COUNT(*) AS n
               FROM paper_trades
               WHERE mode='paper' AND closed_at > now()-interval '7 days'"""
        )

        # ── Live (only if any live trade exists) ───────────────────────────
        live_exists = await conn.fetchval("SELECT EXISTS(SELECT 1 FROM paper_trades WHERE mode='live')")
        live_block = None
        if live_exists:
            l24 = await conn.fetchval(
                """SELECT COALESCE(SUM(pnl),0) FROM paper_trades
                   WHERE mode='live' AND closed_at > now()-interval '24 hours'"""
            )
            ltot = await conn.fetchval(
                "SELECT COALESCE(SUM(pnl),0) FROM paper_trades WHERE mode='live' AND closed_at IS NOT NULL"
            )
            lopen = await conn.fetchval(
                "SELECT COUNT(*) FROM paper_trades WHERE mode='live' AND closed_at IS NULL"
            )
            live_block = (l24, ltot, lopen)

        # ── Research ───────────────────────────────────────────────────────
        best = await conn.fetchrow(
            """SELECT params, oos_avg_ret_pct, oos_n FROM research_runs
               WHERE created_at > now()-interval '24 hours' AND oos_avg_ret_pct IS NOT NULL
                 AND oos_n >= 30
               ORDER BY oos_avg_ret_pct DESC NULLS LAST LIMIT 1"""
        )
        combos = await conn.fetchval(
            "SELECT COUNT(*) FROM research_runs WHERE created_at > now()-interval '24 hours'"
        )

        # ── Data ───────────────────────────────────────────────────────────
        liq = await conn.fetchrow(
            """SELECT COUNT(*) AS n, COALESCE(SUM(notional),0) AS notional
               FROM liquidation_events WHERE ts > now()-interval '24 hours'"""
        )
        funding = await conn.fetchrow(
            """SELECT symbol, funding_rate FROM funding_rates
               WHERE ts > now()-interval '1 hour'
               ORDER BY abs(funding_rate) DESC NULLS LAST LIMIT 1"""
        )

        # ── Infra alerts ───────────────────────────────────────────────────
        alert_rows = await conn.fetch(
            """SELECT severity, COUNT(*) AS n FROM validation_alerts
               WHERE fired_at > now()-interval '24 hours'
               GROUP BY severity"""
        )

    # ── Format ─────────────────────────────────────────────────────────────
    wins = (wr_row["wins"] or 0) if wr_row else 0
    n7 = (wr_row["n"] or 0) if wr_row else 0
    wr = f"{(wins / n7 * 100):.0f}%" if n7 else "n/a"

    lines = [
        f"**📊 Daily digest** {datetime.now(timezone.utc).strftime('%Y-%m-%d')}",
        "```",
        f"PAPER 24h {_fmt_money(paper_24h)}  tot {_fmt_money(paper_total)}",
        f"      open {paper_open}   WR7d {wr}",
    ]
    if live_block:
        l24, ltot, lopen = live_block
        lines.append(f"LIVE  24h {_fmt_money(l24)}  tot {_fmt_money(ltot)}  open {lopen}")

    if best:
        import json as _json
        p = best["params"]
        if isinstance(p, str):
            try:
                p = _json.loads(p)
            except Exception:
                p = {}
        if isinstance(p, dict):
            # stable key order, fade-grid keys first, so truncation drops the least important
            order = ["drop_window_h", "drop_threshold", "panic_cond", "hold_h", "direction"]
            keys = [k for k in order if k in p] + [k for k in sorted(p) if k not in order]
            pstr = " ".join(f"{k.replace('drop_', '').replace('_pct', '').replace('_h', 'h')}={p[k]}" for k in keys)
        else:
            pstr = str(p)[:48]
        lines.append(f"RSCH best OOS {best['oos_avg_ret_pct']:+.2f}% n{best['oos_n']}")
        lines.append(f"      {pstr[:46]}")
        lines.append(f"      combos24h {combos}")
    else:
        lines.append(f"RSCH no OOS results 24h  combos {combos}")

    liq_n = liq["n"] if liq else 0
    liq_notional = (liq["notional"] or 0) if liq else 0
    lines.append(f"LIQ   {liq_n} events  ${liq_notional/1e6:.1f}M (24h)")
    if funding:
        lines.append(f"FUND  |max| {funding['symbol']} {funding['funding_rate']*100:+.3f}%")

    sev_map = {r["severity"]: r["n"] for r in alert_rows}
    if sev_map:
        parts = " ".join(f"{k[:4]}:{v}" for k, v in sev_map.items())
        lines.append(f"ALRT  {parts} (24h)")
    else:
        lines.append("ALRT  none (24h)")

    lines.append("```")
    return "\n".join(lines)


async def maybe_send_digest(redis, db, force: bool = False) -> bool:
    """Send the digest if it's >=07:00 UTC and not yet sent today (or force).

    Returns True if a digest was sent this call.
    """
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    already = await redis.get(DIGEST_SENT_KEY)

    if not force:
        if already == today:
            return False
        if datetime.now(timezone.utc).hour < DIGEST_HOUR_UTC:
            return False

    try:
        msg = await build_digest(db)
    except Exception as e:
        logger.error("digest_build_failed", error=str(e))
        return False

    sent = await send_telegram(msg)
    # Mark sent regardless of telegram success on the scheduled path to avoid spam
    # loops; on force (deploy test) only mark if delivered so a failed test retries.
    if sent or not force:
        await redis.set(DIGEST_SENT_KEY, today)
    logger.info("digest_sent", date=today, discord_sent=sent, force=force)
    return sent
