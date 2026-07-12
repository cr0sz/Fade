from __future__ import annotations

import asyncio
import re
from datetime import datetime, timezone

import aiohttp

from src.core.config import get_settings
from src.core.logging import get_logger
from src.models.social import SocialPost
from src.providers.base import BaseProvider

logger = get_logger(__name__)

TICKER_PATTERN = re.compile(r'\$([A-Z]{1,5}(?:\.IS)?)\b')
MARKET_QUERIES = [
    "$SPY OR $QQQ OR $AAPL OR $MSFT OR $NVDA lang:en",
    "BIST OR Borsa Istanbul OR $THYAO OR $GARAN lang:en",
    "stock market bull OR bear OR earnings lang:en -is:retweet",
]


class TwitterProvider(BaseProvider):
    provider_name = "twitter"

    def __init__(self) -> None:
        self._settings = get_settings()
        self._bearer = self._settings.TWITTER_BEARER_TOKEN
        self._base = "https://api.twitter.com/2"

    async def fetch(self, limit: int = 100) -> list[SocialPost]:
        if not self._bearer:
            logger.warning("twitter_bearer_missing")
            return []

        posts: list[SocialPost] = []
        per_query = max(10, limit // len(MARKET_QUERIES))
        headers = {"Authorization": f"Bearer {self._bearer}"}

        async with aiohttp.ClientSession() as session:
            for query in MARKET_QUERIES:
                await self.rate_limit_handler()
                try:
                    raw_tweets = await self._search(session, headers, query, per_query)
                    for raw in raw_tweets:
                        post = await self.normalize(raw)
                        if post:
                            posts.append(post)
                except Exception as exc:
                    logger.error("twitter_fetch_error", query=query[:50], error=str(exc))

        logger.info("twitter_fetched", count=len(posts))
        return posts

    async def _search(
        self,
        session: aiohttp.ClientSession,
        headers: dict,
        query: str,
        limit: int,
    ) -> list[dict]:
        params = {
            "query": query,
            "max_results": min(limit, 100),
            "tweet.fields": "created_at,public_metrics,author_id",
            "expansions": "author_id",
            "user.fields": "username",
        }
        async with session.get(
            f"{self._base}/tweets/search/recent", headers=headers, params=params
        ) as resp:
            if resp.status == 429:
                logger.warning("twitter_rate_limited")
                await asyncio.sleep(900)  # 15min window reset
                return []
            if resp.status != 200:
                logger.error("twitter_api_error", status=resp.status)
                return []
            data = await resp.json()
            tweets = data.get("data", [])
            users = {u["id"]: u["username"] for u in data.get("includes", {}).get("users", [])}
            for tweet in tweets:
                tweet["_username"] = users.get(tweet.get("author_id", ""), "unknown")
            return tweets

    async def normalize(self, raw: dict) -> SocialPost | None:
        text = raw.get("text", "")
        if not text:
            return None

        ticker_mentions = list(set(TICKER_PATTERN.findall(text)))
        metrics = raw.get("public_metrics", {})
        engagement = (
            metrics.get("like_count", 0)
            + metrics.get("retweet_count", 0) * 3
            + metrics.get("reply_count", 0)
        )

        created_at = datetime.now(timezone.utc)
        if raw.get("created_at"):
            try:
                created_at = datetime.fromisoformat(
                    raw["created_at"].replace("Z", "+00:00")
                ).astimezone(timezone.utc)
            except (ValueError, AttributeError):
                pass

        return SocialPost(
            id=raw.get("id", ""),
            platform="twitter",
            content=text[:1000],
            author=raw.get("_username", ""),
            ticker_mentions=ticker_mentions,
            engagement_score=float(engagement),
            upvotes=metrics.get("like_count", 0),
            comments=metrics.get("reply_count", 0),
            url=f"https://twitter.com/i/web/status/{raw.get('id', '')}",
            created_at=created_at,
        )

    async def health_check(self) -> bool:
        if not self._bearer:
            return False
        try:
            async with aiohttp.ClientSession() as session:
                headers = {"Authorization": f"Bearer {self._bearer}"}
                async with session.get(
                    f"{self._base}/tweets/search/recent",
                    headers=headers,
                    params={"query": "test", "max_results": 10},
                ) as resp:
                    return resp.status in (200, 400)  # 400 = bad query, but auth worked
        except Exception:
            return False

    async def cost_estimate(self, request_count: int) -> float:
        return 0.0  # Free tier

    async def rate_limit_handler(self) -> None:
        await asyncio.sleep(1.0)
