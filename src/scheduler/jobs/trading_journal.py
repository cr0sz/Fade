"""Weekly Monday 08:00 UTC: Ollama-generated trading journal from last 7 days of trades."""
from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone

import httpx

logger = logging.getLogger(__name__)

OLLAMA_URL = "http://ollama:11434"
MODEL = "qwen2.5:3b"


async def run_trading_journal(pool, settings) -> None:
    try:
        week_start = _this_week_start()
        # Skip if already generated for this week
        async with pool.acquire() as conn:
            existing = await conn.fetchval(
                "SELECT id FROM trading_journal WHERE week_start = $1", week_start
            )
            if existing:
                logger.info("trading_journal_already_exists", week_start=str(week_start))
                return

            data = await _gather_week_data(conn)

        content = await _call_ollama(data)

        async with pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO trading_journal (week_start, content, model_used, trade_count, total_pnl)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (week_start) DO NOTHING
            """, week_start, content, MODEL, data["trade_count"], data["total_pnl"])

        logger.info("trading_journal_generated", week_start=str(week_start),
                    trade_count=data["trade_count"], total_pnl=data["total_pnl"])
        await _send_discord(content, data)
    except Exception as exc:
        logger.error("trading_journal_failed", error=str(exc))


def _this_week_start() -> "datetime.date":
    today = datetime.now(timezone.utc).date()
    return today - timedelta(days=today.weekday())  # Monday


async def _gather_week_data(conn) -> dict:
    trades = await conn.fetch("""
        SELECT asset, direction, exchange, pnl, close_reason
        FROM paper_trades
        WHERE closed_at > NOW() - INTERVAL '7 days'
          AND closed_at IS NOT NULL
        ORDER BY pnl DESC
    """)

    rows = [dict(r) for r in trades]
    total_pnl = sum(float(r["pnl"] or 0) for r in rows)
    wins = sum(1 for r in rows if float(r.get("pnl") or 0) > 0)
    win_rate = round(100 * wins / len(rows), 1) if rows else 0

    # Close reason breakdown
    reasons: dict[str, int] = {}
    for r in rows:
        reasons[r["close_reason"] or "unknown"] = reasons.get(r["close_reason"] or "unknown", 0) + 1

    # Exchange breakdown
    by_exchange: dict[str, dict] = {}
    for r in rows:
        ex = r["exchange"] or "unknown"
        if ex not in by_exchange:
            by_exchange[ex] = {"count": 0, "pnl": 0.0}
        by_exchange[ex]["count"] += 1
        by_exchange[ex]["pnl"] += float(r["pnl"] or 0)

    return {
        "week_start": str(_this_week_start()),
        "trade_count": len(rows),
        "total_pnl": round(total_pnl, 2),
        "win_rate": win_rate,
        "close_reasons": reasons,
        "by_exchange": by_exchange,
        "top_winners": rows[:3],
        "top_losers": list(reversed(rows[-3:])) if len(rows) >= 3 else [],
    }


async def _call_ollama(data: dict) -> str:
    prompt = f"""You are a trading journal assistant. Write a weekly reflection for a crypto/stock trader.
Be analytical, honest, and concise (under 300 words). Plain text only, no markdown.
Focus on what worked, what didn't, patterns in losses, and one actionable lesson.

Week of {data['week_start']}:
- Trades: {data['trade_count']}, Total PnL: ${data['total_pnl']:+.2f}, Win rate: {data['win_rate']}%
- Exit reasons: {json.dumps(data['close_reasons'])}
- By exchange: {json.dumps(data['by_exchange'])}
- Top winners: {json.dumps(data['top_winners'], default=str)}
- Top losers: {json.dumps(data['top_losers'], default=str)}

Weekly trading journal entry:"""

    async with httpx.AsyncClient(timeout=90.0) as client:
        resp = await client.post(
            f"{OLLAMA_URL}/api/generate",
            json={"model": MODEL, "prompt": prompt, "stream": False},
        )
        resp.raise_for_status()
        return resp.json().get("response", "").strip()


async def _send_discord(content: str, data: dict) -> None:
    import os
    webhook = os.getenv("DISCORD_WEBHOOK_JOURNAL", "")
    if not webhook:
        return
    pnl_sign = "+" if data["total_pnl"] >= 0 else ""
    payload = {"embeds": [{
        "title": f"📓 Weekly Journal — {data['week_start']}",
        "description": content[:4000],
        "color": 10181046,  # purple
        "fields": [
            {"name": "Trades", "value": str(data["trade_count"]), "inline": True},
            {"name": "PnL", "value": f"${pnl_sign}{data['total_pnl']:.2f}", "inline": True},
            {"name": "Win Rate", "value": f"{data['win_rate']}%", "inline": True},
        ],
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }]}
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.post(webhook, json=payload)
            if resp.status_code not in (200, 204):
                logger.error("journal_discord_failed", status=resp.status_code)
    except Exception as exc:
        logger.error("journal_discord_error", error=str(exc))
