"""Sunday 20:00 UTC: Weekly Executive Report — 6 Discord embeds to #journal."""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx

logger = logging.getLogger(__name__)

OLLAMA_URL = "http://ollama:11434"
MODEL = "qwen2.5:3b"
COLOR_PROFIT  = 5763719
COLOR_LOSS    = 15548997
COLOR_INFO    = 3447003
COLOR_PURPLE  = 10181046
COLOR_WARN    = 15105570
COLOR_GREEN   = 3066993

_REDIS_DEDUP_KEY = "exec_report:{week}"
_REDIS_DEDUP_TTL = 7 * 86400  # 7 days


async def run_executive_report(pool, settings, redis=None) -> None:
    try:
        webhook = os.getenv("DISCORD_WEBHOOK_JOURNAL", "")
        if not webhook:
            logger.warning("exec_report_no_webhook")
            return

        week_start = _week_start()

        # Redis dedup: skip if already sent this week
        key = _REDIS_DEDUP_KEY.format(week=week_start)
        if redis:
            try:
                if await redis.get(key):
                    logger.info("exec_report_already_sent", week=str(week_start))
                    return
            except Exception as _dedup_exc:
                logger.warning("exec_report_dedup_check_failed", error=str(_dedup_exc),
                               action="sending_anyway")

        async with pool.acquire() as conn:
            perf   = await _gather_performance(conn)
            regime = await _gather_regime(conn)
            engines = await _gather_engines(conn)
            growth = await _gather_growth(conn)
            lessons = await _gather_lessons(conn)

        outlook = await _call_ollama(perf, regime, lessons)

        embeds = [
            _embed_performance(perf),
            _embed_regime(regime, perf),
            _embed_engines(engines),
            _embed_growth(growth),
            _embed_lessons(lessons),
            _embed_outlook(outlook, perf),
        ]

        await _post_discord(webhook, embeds)
        logger.info("exec_report_sent", week=str(week_start),
                    trades=perf["trade_count"], pnl=perf["total_pnl"])

        if redis:
            try:
                await redis.set(key, "1", ex=_REDIS_DEDUP_TTL)
            except Exception:
                pass

    except Exception as exc:
        logger.error("exec_report_failed", error=str(exc))


def _week_start() -> "datetime.date":
    today = datetime.now(timezone.utc).date()
    return today - timedelta(days=today.weekday())  # Monday of this week


# ── Data gathering ─────────────────────────────────────────────────────────────

async def _gather_performance(conn) -> dict:
    rows = await conn.fetch("""
        SELECT asset, direction, pnl, entry_price, exit_price, opened_at, closed_at
        FROM paper_trades
        WHERE closed_at > NOW() - INTERVAL '7 days'
          AND closed_at IS NOT NULL
          AND close_reason <> 'dedup_cleanup'
        ORDER BY pnl DESC
    """)
    rows = [dict(r) for r in rows]
    n = len(rows)
    if n == 0:
        return {"trade_count": 0, "total_pnl": 0.0, "win_rate": 0.0,
                "best": None, "worst": None, "max_drawdown": 0.0, "sharpe": 0.0}

    pnls = [float(r["pnl"] or 0) for r in rows]
    total_pnl = sum(pnls)
    wins = sum(1 for p in pnls if p > 0)
    win_rate = round(100 * wins / n, 1)

    # Sharpe estimate (per-trade PnL series)
    sharpe = 0.0
    if n >= 3:
        import statistics
        mean_p = statistics.mean(pnls)
        stdev_p = statistics.stdev(pnls)
        sharpe = round(mean_p / stdev_p, 2) if stdev_p > 0 else 0.0

    # Max drawdown from equity curve of PnL
    running = 0.0
    peak = 0.0
    max_dd = 0.0
    for p in sorted(rows, key=lambda r: r["closed_at"] or datetime.min):
        running += float(p["pnl"] or 0)
        peak = max(peak, running)
        max_dd = min(max_dd, running - peak)

    return {
        "trade_count": n,
        "total_pnl": round(total_pnl, 2),
        "win_rate": win_rate,
        "sharpe": sharpe,
        "max_drawdown": round(max_dd, 2),
        "best": rows[0] if rows else None,
        "worst": rows[-1] if rows else None,
    }


async def _gather_regime(conn) -> dict:
    rows = await conn.fetch("""
        SELECT regime, COUNT(*) AS cnt
        FROM macro_regimes
        WHERE computed_at > NOW() - INTERVAL '7 days'
        GROUP BY regime ORDER BY cnt DESC
    """)
    dominant = rows[0]["regime"] if rows else "unknown"

    # Win rate split by regime (join paper_trades with nearest macro_regime)
    regime_wr = await conn.fetch("""
        SELECT mr.regime,
               COUNT(*) FILTER (WHERE pt.pnl > 0) AS wins,
               COUNT(*) AS total
        FROM paper_trades pt
        JOIN LATERAL (
            SELECT regime FROM macro_regimes
            WHERE computed_at <= pt.opened_at
            ORDER BY computed_at DESC LIMIT 1
        ) mr ON true
        WHERE pt.closed_at > NOW() - INTERVAL '7 days'
          AND pt.closed_at IS NOT NULL
          AND pt.close_reason <> 'dedup_cleanup'
        GROUP BY mr.regime
    """)
    wr_by_regime = {
        r["regime"]: {"wins": r["wins"], "total": r["total"],
                      "wr": round(100 * r["wins"] / r["total"], 1) if r["total"] else 0}
        for r in regime_wr
    }
    return {"dominant": dominant, "wr_by_regime": wr_by_regime}


async def _gather_engines(conn) -> dict:
    rows = await conn.fetch("""
        SELECT engine_name,
               SUM(accuracy * sample_size) / NULLIF(SUM(sample_size), 0) AS accuracy,
               SUM(predictive_strength * sample_size) / NULLIF(SUM(sample_size), 0) AS predictive_strength,
               SUM(sample_size) AS sample_size
          FROM signal_scores
         WHERE window_hours = 24
           AND computed_at > NOW() - INTERVAL '7 days'
         GROUP BY engine_name
        HAVING SUM(sample_size) > 0
         ORDER BY accuracy DESC
    """)
    rows = [dict(r) for r in rows]
    return {"top3": rows[:3], "bottom3": list(reversed(rows))[:3]}


async def _gather_growth(conn) -> dict:
    row = await conn.fetchrow("""
        SELECT phase, equity, max_positions, leverage_cap, size_multiplier, captured_at
        FROM capital_growth_log
        ORDER BY captured_at DESC LIMIT 1
    """)
    if not row:
        return {"phase": None}
    phases = {1: 200.0, 2: 500.0, 3: 1000.0, 4: None}
    phase = row["phase"]
    next_target = phases.get(phase)
    equity = float(row["equity"])
    distance = round(next_target - equity, 2) if next_target else None
    return {
        "phase": phase,
        "equity": equity,
        "max_positions": row["max_positions"],
        "leverage_cap": row["leverage_cap"],
        "size_multiplier": float(row["size_multiplier"]),
        "next_target": next_target,
        "distance_to_next": distance,
    }


async def _gather_lessons(conn) -> list[dict]:
    rows = await conn.fetch("""
        SELECT asset, direction, pnl, lesson, verdict, regime_fit
        FROM trade_post_mortems
        WHERE created_at > NOW() - INTERVAL '7 days'
          AND lesson IS NOT NULL
        ORDER BY ABS(pnl) DESC NULLS LAST
        LIMIT 3
    """)
    return [dict(r) for r in rows]


# ── Ollama ─────────────────────────────────────────────────────────────────────

async def _call_ollama(perf: dict, regime: dict, lessons: list) -> str:
    lessons_text = "\n".join(
        f"- [{l.get('asset','?')} {l.get('direction','?')}] {l.get('lesson','')}"
        for l in lessons
    ) or "No post-mortem data yet."

    prompt = f"""You are a trading analyst writing a 100-word outlook for next week.
Be specific and actionable. Plain text only.

Current regime: {regime.get('dominant', 'unknown')}
Last 7-day performance: {perf['trade_count']} trades, ${perf['total_pnl']:+.2f} PnL, {perf['win_rate']}% WR
Key lessons this week:
{lessons_text}

Next week outlook (100 words max):"""

    try:
        async with httpx.AsyncClient(timeout=90.0) as client:
            resp = await client.post(
                f"{OLLAMA_URL}/api/generate",
                json={"model": MODEL, "prompt": prompt, "stream": False},
            )
            resp.raise_for_status()
            return resp.json().get("response", "").strip()[:600]
    except Exception as exc:
        logger.warning("exec_report_ollama_failed", error=str(exc))
        return "Outlook unavailable (Ollama offline)."


# ── Embed builders ─────────────────────────────────────────────────────────────

def _embed_performance(p: dict) -> dict:
    color = COLOR_PROFIT if p["total_pnl"] >= 0 else COLOR_LOSS
    fields = [
        {"name": "Trades", "value": str(p["trade_count"]), "inline": True},
        {"name": "Total PnL", "value": f"${p['total_pnl']:+.2f}", "inline": True},
        {"name": "Win Rate", "value": f"{p['win_rate']}%", "inline": True},
        {"name": "Sharpe (est.)", "value": str(p["sharpe"]), "inline": True},
        {"name": "Max Drawdown", "value": f"${p['max_drawdown']:.2f}", "inline": True},
    ]
    if p.get("best"):
        b = p["best"]
        fields.append({"name": "🏆 Best Trade",
                        "value": f"{b.get('asset')} {b.get('direction')} ${float(b.get('pnl') or 0):+.2f}",
                        "inline": True})
    if p.get("worst"):
        w = p["worst"]
        fields.append({"name": "💔 Worst Trade",
                        "value": f"{w.get('asset')} {w.get('direction')} ${float(w.get('pnl') or 0):+.2f}",
                        "inline": True})
    return {"title": "📈 Weekly Performance", "color": color, "fields": fields}


def _embed_regime(regime: dict, perf: dict) -> dict:
    dominant = regime.get("dominant", "unknown")
    wr_lines = [
        f"**{r}**: {d['wr']}% WR ({d['total']} trades)"
        for r, d in regime.get("wr_by_regime", {}).items()
    ] or ["No regime data."]
    return {
        "title": "🌐 Regime Intelligence",
        "color": COLOR_INFO,
        "description": f"Dominant regime this week: **{dominant}**\n\n" + "\n".join(wr_lines),
    }


def _embed_engines(e: dict) -> dict:
    def _fmt(rows: list) -> str:
        if not rows:
            return "No data."
        return "\n".join(
            f"**{r['engine_name']}**: {round(float(r['accuracy'] or 0)*100,1)}% acc "
            f"(n={r['sample_size']})"
            for r in rows
        )
    fields = [
        {"name": "🏆 Top 3", "value": _fmt(e.get("top3", [])), "inline": False},
        {"name": "📉 Bottom 3", "value": _fmt(e.get("bottom3", [])), "inline": False},
    ]
    return {"title": "🏆 Engine Leaderboard", "color": COLOR_PURPLE, "fields": fields}


def _embed_growth(g: dict) -> dict:
    if not g.get("phase"):
        return {"title": "💰 Capital Growth", "color": COLOR_GREEN,
                "description": "No growth data yet. Will populate after first tick."}
    dist = f"${g['distance_to_next']:.2f} to Phase {g['phase']+1}" if g.get("distance_to_next") else "Phase 4 reached"
    fields = [
        {"name": "Phase", "value": f"P{g['phase']}", "inline": True},
        {"name": "Equity", "value": f"${g['equity']:.2f}", "inline": True},
        {"name": "Max Positions", "value": str(g["max_positions"]), "inline": True},
        {"name": "Leverage Cap", "value": f"{g['leverage_cap']}x", "inline": True},
        {"name": "Size Mult", "value": f"×{g['size_multiplier']:.2f}", "inline": True},
        {"name": "Next Target", "value": dist, "inline": True},
    ]
    return {"title": "💰 Capital Growth", "color": COLOR_GREEN, "fields": fields}


def _embed_lessons(lessons: list) -> dict:
    if not lessons:
        desc = "No post-mortem data yet (Feature 1 active from now on)."
    else:
        lines = []
        for l in lessons:
            icon = "✅" if (l.get("pnl") or 0) > 0 else "❌"
            lines.append(
                f"{icon} **{l.get('asset','?')} {l.get('direction','?')}** "
                f"(${float(l.get('pnl') or 0):+.2f})\n{l.get('lesson','')}"
            )
        desc = "\n\n".join(lines)
    return {"title": "🧠 Key Lessons", "color": COLOR_WARN, "description": desc[:2000]}


def _embed_outlook(outlook: str, perf: dict) -> dict:
    return {
        "title": "🔭 Next Week Outlook",
        "color": COLOR_INFO,
        "description": outlook,
        "footer": {"text": f"Generated by {MODEL}"},
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


# ── Discord ────────────────────────────────────────────────────────────────────

async def _post_discord(webhook: str, embeds: list) -> None:
    # Discord allows max 10 embeds per message; send in one batch (6 < 10)
    payload = {"embeds": embeds}
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.post(webhook, json=payload)
            if resp.status_code not in (200, 204):
                logger.error("exec_report_discord_failed", status=resp.status_code,
                             body=resp.text[:200])
    except Exception as exc:
        logger.error("exec_report_discord_error", error=str(exc))
