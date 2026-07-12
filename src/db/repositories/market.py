from __future__ import annotations

import json

import asyncpg

from src.models.market import MarketTrend, SmartMoneyEvent, TickerQuote


class MarketTrendRepository:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def upsert_trend(self, trend: MarketTrend) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO market_trends (
                    ticker, exchange, news_score, social_score, smart_money_score,
                    volume_score, technical_score, composite_score, hype_signal, panic_signal,
                    trending_up, trending_down, computed_at,
                    smoothed_composite, confidence_score, confidence_level,
                    narrative_cluster, narrative_persistence, signal_quality
                ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19)
                ON CONFLICT (ticker) DO UPDATE SET
                    exchange = EXCLUDED.exchange,
                    news_score = EXCLUDED.news_score,
                    social_score = EXCLUDED.social_score,
                    smart_money_score = EXCLUDED.smart_money_score,
                    volume_score = EXCLUDED.volume_score,
                    technical_score = EXCLUDED.technical_score,
                    composite_score = EXCLUDED.composite_score,
                    hype_signal = EXCLUDED.hype_signal,
                    panic_signal = EXCLUDED.panic_signal,
                    trending_up = EXCLUDED.trending_up,
                    trending_down = EXCLUDED.trending_down,
                    computed_at = EXCLUDED.computed_at,
                    smoothed_composite = EXCLUDED.smoothed_composite,
                    confidence_score = EXCLUDED.confidence_score,
                    confidence_level = EXCLUDED.confidence_level,
                    narrative_cluster = EXCLUDED.narrative_cluster,
                    narrative_persistence = EXCLUDED.narrative_persistence,
                    signal_quality = EXCLUDED.signal_quality
                """,
                trend.ticker, trend.exchange, trend.news_score, trend.social_score,
                trend.smart_money_score, trend.volume_score,
                getattr(trend, "technical_score", 0.0), trend.composite_score,
                trend.hype_signal, trend.panic_signal, trend.trending_up,
                trend.trending_down, trend.computed_at,
                trend.smoothed_composite, trend.confidence_score, trend.confidence_level,
                trend.narrative_cluster, trend.narrative_persistence, trend.signal_quality,
            )

    async def get_top_trending(self, exchange: str | None = None, limit: int = 20) -> list[dict]:
        params: list = []
        where = ""
        if exchange:
            params.append(exchange)
            where = "WHERE exchange = $1"
        params.append(limit)
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT * FROM market_trends
                {where}
                ORDER BY composite_score DESC
                LIMIT ${len(params)}
                """,
                *params,
            )
            return [dict(r) for r in rows]

    async def get_signals(self, hype: bool = True) -> list[dict]:
        col = "hype_signal" if hype else "panic_signal"
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                f"SELECT * FROM market_trends WHERE {col} = true ORDER BY composite_score DESC"
            )
            return [dict(r) for r in rows]


class SmartMoneyRepository:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def insert(self, event: SmartMoneyEvent) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO smart_money_events (
                    event_type, ticker, actor, amount_usd, shares,
                    direction, source, event_at
                ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
                ON CONFLICT DO NOTHING
                """,
                event.event_type, event.ticker, event.actor, event.amount_usd,
                event.shares, event.direction, event.source, event.event_at,
            )

    async def get_recent(self, hours: int = 24, ticker: str | None = None) -> list[dict]:
        params: list = [str(hours)]
        where = "WHERE event_at > now() - ($1 || ' hours')::interval"
        if ticker:
            params.append(ticker)
            where += f" AND ticker = ${len(params)}"
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                f"SELECT * FROM smart_money_events {where} ORDER BY event_at DESC",
                *params,
            )
            return [dict(r) for r in rows]


class ProviderUsageRepository:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def log(
        self,
        provider: str,
        operation: str,
        input_tokens: int = 0,
        output_tokens: int = 0,
        cost_usd: float = 0.0,
        success: bool = True,
    ) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO provider_usage_logs (
                    provider, operation, input_tokens, output_tokens, cost_usd, success
                ) VALUES ($1,$2,$3,$4,$5,$6)
                """,
                provider, operation, input_tokens, output_tokens, cost_usd, success,
            )


class TickerRepository:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def get_all_symbols(self, exchange: str | None = None) -> list[str]:
        params: list = []
        where = "WHERE is_active = true"
        if exchange:
            params.append(exchange)
            where += f" AND exchange = ${len(params)}"
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                f"SELECT symbol FROM tickers {where}", *params
            )
            return [r["symbol"] for r in rows]

    async def upsert(self, data: dict) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO tickers (symbol, name, exchange, sector, industry, is_active)
                VALUES ($1,$2,$3,$4,$5,$6)
                ON CONFLICT (symbol) DO UPDATE SET
                    name = EXCLUDED.name,
                    exchange = EXCLUDED.exchange,
                    sector = EXCLUDED.sector,
                    industry = EXCLUDED.industry,
                    updated_at = now()
                """,
                data["symbol"], data.get("name"), data.get("exchange", ""),
                data.get("sector"), data.get("industry"), data.get("is_active", True),
            )

    async def upsert_quote(self, quote: TickerQuote) -> None:
        """Persist a live market quote.

        Two writes:
        1. Update market_cap_usd on the tickers row (latest snapshot).
        2. Insert a row into ticker_quotes (time-series; created by migration 015).
           Silently skips the time-series insert if the table doesn't exist yet
           so the worker doesn't crash before that migration runs.
        """
        async with self._pool.acquire() as conn:
            # Update market cap on parent ticker row
            if quote.market_cap:
                await conn.execute(
                    """
                    UPDATE tickers
                       SET market_cap_usd = $2, updated_at = now()
                     WHERE symbol = $1
                    """,
                    quote.symbol, quote.market_cap,
                )

            # Append to time-series table (best-effort)
            try:
                await conn.execute(
                    """
                    INSERT INTO ticker_quotes
                        (symbol, price, change_pct, volume, market_cap, exchange, currency, ts)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                    """,
                    quote.symbol,
                    quote.price,
                    quote.change_pct,
                    quote.volume,
                    quote.market_cap,
                    quote.exchange,
                    quote.currency,
                    quote.timestamp,
                )
            except asyncpg.UndefinedTableError:
                pass  # migration 015 not yet applied — non-fatal


class AIOutputRepository:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def store(
        self,
        output_type: str,
        ticker: str | None,
        prompt_hash: str,
        content: str,
        model_used: str,
        cost_usd: float,
    ) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO ai_outputs (
                    output_type, ticker, prompt_hash, content, model_used, cost_usd
                ) VALUES ($1,$2,$3,$4,$5,$6)
                ON CONFLICT (prompt_hash) DO UPDATE SET
                    content = EXCLUDED.content,
                    model_used = EXCLUDED.model_used,
                    updated_at = now()
                """,
                output_type, ticker, prompt_hash, content, model_used, cost_usd,
            )

    async def get_by_hash(self, prompt_hash: str) -> dict | None:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM ai_outputs WHERE prompt_hash = $1", prompt_hash
            )
            return dict(row) if row else None
