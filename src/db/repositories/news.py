from __future__ import annotations

from datetime import datetime, timedelta, timezone

import asyncpg

from src.models.news import NewsItem


class NewsRepository:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def exists_by_hash(self, content_hash: str) -> bool:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT 1 FROM news WHERE content_hash = $1", content_hash
            )
            return row is not None

    async def insert(self, item: NewsItem) -> str:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                INSERT INTO news (
                    title, content, summary, url, source, source_domain,
                    author, published_at, ticker_symbols, sector,
                    sentiment_score, content_hash, ai_summary, image_url
                ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
                ON CONFLICT (content_hash) DO NOTHING
                RETURNING id
                """,
                item.title, item.content, item.summary, item.url,
                item.source, item.source_domain, item.author, item.published_at,
                item.ticker_symbols, item.sector, item.sentiment_score,
                item.content_hash, item.ai_summary, item.image_url,
            )
            return str(row["id"]) if row else ""

    async def update_ai_summary(self, news_id: str, ai_summary: str) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                "UPDATE news SET ai_summary = $1 WHERE id = $2",
                ai_summary, int(news_id),
            )

    async def update_impact_analysis(
        self,
        news_id: str,
        ai_summary: str,
        impact_score: float,
        urgency: int,
        source_authority: float,
    ) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE news
                   SET ai_summary = $1, impact_score = $2,
                       urgency = $3, source_authority = $4
                 WHERE id = $5
                """,
                ai_summary, impact_score, urgency, source_authority, int(news_id),
            )

    async def get_without_impact_score(self, limit: int = 50) -> list[dict]:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT * FROM news
                WHERE impact_score IS NULL
                  AND published_at > now() - interval '48 hours'
                ORDER BY published_at DESC
                LIMIT $1
                """,
                limit,
            )
            return [dict(r) for r in rows]

    async def get_recent(
        self,
        hours: int = 24,
        ticker: str | None = None,
        sector: str | None = None,
        limit: int = 50,
    ) -> list[dict]:
        since = datetime.now(timezone.utc) - timedelta(hours=hours)
        conditions = ["published_at > $1"]
        params: list = [since]

        if ticker:
            params.append(ticker)
            conditions.append(f"${len(params)} = ANY(ticker_symbols)")
        if sector:
            params.append(sector)
            conditions.append(f"sector = ${len(params)}")

        params.append(limit)
        where = " AND ".join(conditions)
        query = f"""
            SELECT * FROM news
            WHERE {where}
            ORDER BY published_at DESC
            LIMIT ${len(params)}
        """
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(query, *params)
            return [dict(r) for r in rows]

    async def get_without_ai_summary(self, limit: int = 20) -> list[dict]:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT * FROM news
                WHERE ai_summary IS NULL
                  AND published_at > now() - interval '24 hours'
                ORDER BY published_at DESC
                LIMIT $1
                """,
                limit,
            )
            return [dict(r) for r in rows]

    async def delete_old(self, retention_days: int) -> int:
        async with self._pool.acquire() as conn:
            result = await conn.execute(
                "DELETE FROM news WHERE published_at < now() - interval '$1 days'",
                retention_days,
            )
            return int(result.split()[-1])
