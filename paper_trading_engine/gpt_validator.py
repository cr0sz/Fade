"""GPT-4o-mini signal validation layer.

Called before opening a position when GPT_SIGNAL_VALIDATION=true.
Fetches recent headlines from DB, asks GPT if the direction makes sense.
Reduces confidence by 30% on disagreement; adds 5% on agreement.
"""
from __future__ import annotations

import json
import os
import asyncio
import asyncpg

try:
    import aiohttp
    _AIOHTTP_OK = True
except ImportError:
    _AIOHTTP_OK = False

_OPENAI_URL = "https://api.openai.com/v1/chat/completions"
_MODEL = "gpt-4o-mini"
_CACHE: dict[str, tuple[str, float]] = {}  # ticker+dir → (direction, adj)
_CACHE_TTL = 1800  # 30 min


async def _fetch_headlines(pool: asyncpg.Pool, ticker: str, limit: int = 6) -> list[str]:
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT title FROM news
                WHERE $1 = ANY(ticker_symbols) AND published_at > NOW() - INTERVAL '24 hours'
                ORDER BY published_at DESC LIMIT $2
                """,
                ticker, limit,
            )
        return [r["title"] for r in rows if r["title"]]
    except Exception:
        return []


async def validate_signal(
    pool: asyncpg.Pool,
    ticker: str,
    direction: str,
    composite_score: float,
    confidence: float,
) -> float:
    """Return adjusted confidence. On error, returns original confidence unchanged."""
    if not _AIOHTTP_OK:
        return confidence

    api_key = os.getenv("OPENAI_API_KEY", "")
    if not api_key:
        return confidence

    cache_key = f"{ticker}:{direction}:{int(composite_score * 100)}"
    import time as _time
    cached = _CACHE.get(cache_key)
    if cached:
        gpt_dir, adj, cached_at = cached
        if _time.monotonic() - cached_at < _CACHE_TTL:
            return round(confidence * adj, 4)
        # Expired — evict and re-fetch
        _CACHE.pop(cache_key, None)

    headlines = await _fetch_headlines(pool, ticker)
    headlines_text = "\n".join(f"- {h}" for h in headlines) if headlines else "(no recent headlines)"

    prompt = (
        f"Asset: {ticker}\n"
        f"Proposed trade direction: {direction.upper()}\n"
        f"Composite signal score: {composite_score:.3f} (range -1 to +1, positive = bullish)\n"
        f"Recent headlines:\n{headlines_text}\n\n"
        f"In one word, should we go {direction.upper()} on {ticker} right now based on this information? "
        f"Answer only: AGREE or DISAGREE."
    )

    try:
        async with aiohttp.ClientSession() as session:
            payload = {
                "model": _MODEL,
                "messages": [{"role": "user", "content": prompt}],
                "max_tokens": 10,
                "temperature": 0.0,
            }
            async with session.post(
                _OPENAI_URL,
                headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
                json=payload,
                timeout=aiohttp.ClientTimeout(total=8),
            ) as resp:
                if resp.status != 200:
                    return confidence
                data = await resp.json()
                answer = data["choices"][0]["message"]["content"].strip().upper()
    except Exception:
        return confidence

    if "AGREE" in answer:
        adj = 1.05
    elif "DISAGREE" in answer:
        adj = 0.70
    else:
        adj = 1.0

    _CACHE[cache_key] = (answer, adj, _time.monotonic())
    # Evict oldest entry when cache exceeds 200 keys
    if len(_CACHE) > 200:
        _CACHE.pop(next(iter(_CACHE)), None)

    return round(confidence * adj, 4)
