"""Daily 07:00 UTC brief: overnight trades, positions, signals, BTC price, funding. Delivered via Discord."""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

import httpx

logger = logging.getLogger(__name__)

OLLAMA_URL = "http://ollama:11434"
MODEL = "qwen2.5:3b"  # deployed model (LLM_AUDIT_MODEL env var default); migration default is 7b — update here if Ollama model changes


async def run_daily_brief(pool, settings) -> None:
    try:
        data = await _gather_data(pool)
        summary = await _call_ollama(data)
        await _send_discord(summary, settings)
        await _store_brief(pool, summary, data)
        logger.info("daily_brief_sent", extra={"trades": data["trade_count"], "pnl": data["total_pnl"]})
    except Exception as exc:
        logger.error("daily_brief_failed", extra={"error": str(exc)})


async def _gather_data(pool) -> dict:
    async with pool.acquire() as conn:
        # Last 24h trades
        trades = await conn.fetch("""
            SELECT asset, direction, pnl, close_reason
            FROM paper_trades
            WHERE closed_at > NOW() - INTERVAL '24 hours'
            ORDER BY pnl DESC
        """)

        # Open positions
        open_pos = await conn.fetch("""
            SELECT asset, direction, notional,
                   ROUND(EXTRACT(EPOCH FROM (NOW()-opened_at))/3600, 1) AS age_h
            FROM paper_trades
            WHERE closed_at IS NULL
            ORDER BY opened_at
        """)

        # Top 3 signals last 24h by abs composite_score
        signals = await conn.fetch("""
            SELECT engine_name, ticker, direction, composite_score
            FROM signals_registry
            WHERE emitted_at > NOW() - INTERVAL '24 hours'
            ORDER BY ABS(composite_score) DESC NULLS LAST
            LIMIT 3
        """)

        # BTC price
        btc = await conn.fetchrow("""
            SELECT price FROM ticker_quotes
            WHERE symbol = 'BTC'
            ORDER BY ts DESC LIMIT 1
        """)

        # Top 3 funding rates by abs value
        funding = await conn.fetch("""
            SELECT symbol, funding_rate
            FROM funding_rates
            WHERE ts > NOW() - INTERVAL '2 hours'
            ORDER BY ABS(funding_rate) DESC NULLS LAST
            LIMIT 3
        """)

    closed = [dict(r) for r in trades]
    total_pnl = sum(float(r["pnl"] or 0) for r in closed)
    wins = sum(1 for r in closed if float(r.get("pnl") or 0) > 0)
    win_rate = round(100 * wins / len(closed), 1) if closed else 0

    return {
        "date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "trade_count": len(closed),
        "total_pnl": round(total_pnl, 2),
        "win_rate": win_rate,
        "trades": closed[:5],  # top 5 for context
        "open_positions": [dict(r) for r in open_pos],
        "top_signals": [dict(r) for r in signals],
        "btc_price": float(btc["price"]) if btc else None,
        "top_funding": [{"symbol": r["symbol"], "rate": float(r["funding_rate"])} for r in funding],
    }


async def _call_ollama(data: dict) -> str:
    btc_display = f"${data['btc_price']:,.0f}" if data["btc_price"] else "unknown"
    prompt = f"""You are a concise trading assistant. Write a brief daily summary for a crypto trader.
Keep it under 250 words. Use plain text, no markdown. Be direct and factual.

Data for {data['date']}:
- Trades closed (24h): {data['trade_count']}, PnL: ${data['total_pnl']:+.2f}, Win rate: {data['win_rate']}%
- Top trades: {json.dumps(data['trades'], default=str)}
- Open positions: {json.dumps(data['open_positions'], default=str)}
- BTC price: {btc_display}
- Top signals: {json.dumps(data['top_signals'], default=str)}
- Notable funding rates: {json.dumps(data['top_funding'], default=str)}

Write the daily brief:"""

    async with httpx.AsyncClient(timeout=60.0) as client:
        resp = await client.post(
            f"{OLLAMA_URL}/api/generate",
            json={"model": MODEL, "prompt": prompt, "stream": False},
        )
        resp.raise_for_status()
        return resp.json().get("response", "").strip()


async def _store_brief(pool, content: str, data: dict) -> None:
    try:
        async with pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO daily_briefs (content, model_used, trade_count, total_pnl, btc_price)
                VALUES ($1, $2, $3, $4, $5)
            """, content, MODEL, data["trade_count"],
                data["total_pnl"], data.get("btc_price"))
    except Exception as exc:
        logger.error("daily_brief_store_failed", extra={"error": str(exc)})


async def _send_discord(message: str, settings) -> None:
    """Post daily brief to Discord #daily-brief channel."""
    import os
    webhook = os.getenv("DISCORD_WEBHOOK_DAILY_BRIEF", "")
    if not webhook:
        logger.warning("daily_brief_discord_webhook_not_configured")
        return
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    payload = {"embeds": [{
        "title": f"📊 Daily Brief — {date_str}",
        "description": message[:4000],
        "color": 15105570,  # orange
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }]}
    async with httpx.AsyncClient(timeout=15.0) as client:
        resp = await client.post(webhook, json=payload)
        if resp.status_code not in (200, 204):
            logger.error("daily_brief_discord_failed", extra={"status": resp.status_code})
