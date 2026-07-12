from __future__ import annotations

from datetime import datetime, timedelta, timezone

import asyncpg

from src.models.social import SocialPost


class SocialRepository:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def insert(self, post: SocialPost) -> bool:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                INSERT INTO social_posts (
                    external_id, platform, content, author, ticker_mentions,
                    sentiment_score, engagement_score, upvotes, comments,
                    url, subreddit, created_at
                ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
                ON CONFLICT (platform, external_id) DO NOTHING
                RETURNING id
                """,
                post.id, post.platform, post.content, post.author,
                post.ticker_mentions, post.sentiment_score, post.engagement_score,
                post.upvotes, post.comments, post.url, post.subreddit, post.created_at,
            )
            return row is not None

    async def get_ticker_mentions(
        self,
        ticker: str,
        hours: int = 24,
    ) -> list[dict]:
        since = datetime.now(timezone.utc) - timedelta(hours=hours)
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT * FROM social_posts
                WHERE $1 = ANY(ticker_mentions)
                  AND created_at > $2
                ORDER BY created_at DESC
                """,
                ticker, since,
            )
            return [dict(r) for r in rows]

    async def get_mention_counts(self, hours: int = 1) -> list[dict]:
        since = datetime.now(timezone.utc) - timedelta(hours=hours)
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT
                    unnest(ticker_mentions) AS ticker,
                    count(*) AS mention_count,
                    avg(sentiment_score) AS avg_sentiment,
                    sum(engagement_score) AS total_engagement
                FROM social_posts
                WHERE created_at > $1
                GROUP BY unnest(ticker_mentions)
                ORDER BY count(*) DESC
                """,
                since,
            )
            return [dict(r) for r in rows]

    async def get_historical_mention_avg(self, ticker: str, days: int = 7) -> float:
        since = datetime.now(timezone.utc) - timedelta(days=days)
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT
                    count(*) / ($1 * 24.0) AS avg_per_hour
                FROM social_posts
                WHERE $2 = ANY(ticker_mentions)
                  AND created_at > $3
                """,
                days, ticker, since,
            )
            return float(row["avg_per_hour"]) if row and row["avg_per_hour"] else 0.0
