from __future__ import annotations

import json
from typing import Any

import redis.asyncio as aioredis

from src.core.config import get_settings
from src.core.logging import get_logger

logger = get_logger(__name__)
_redis: aioredis.Redis | None = None


async def get_redis() -> aioredis.Redis:
    global _redis
    if _redis is None:
        settings = get_settings()
        _redis = aioredis.from_url(
            settings.redis_url,
            encoding="utf-8",
            decode_responses=True,
        )
        await _redis.ping()
        logger.info("redis_connected")
    return _redis


async def close_redis() -> None:
    global _redis
    if _redis:
        await _redis.aclose()
        _redis = None


class CacheClient:
    def __init__(self, redis: aioredis.Redis) -> None:
        self._r = redis

    async def get(self, key: str) -> Any | None:
        raw = await self._r.get(key)
        if raw is None:
            return None
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            return raw

    async def set(self, key: str, value: Any, ttl: int = 300) -> None:
        if isinstance(value, (dict, list)):
            serialized = json.dumps(value, default=str)
        else:
            serialized = str(value)
        await self._r.setex(key, ttl, serialized)

    async def delete(self, key: str) -> None:
        await self._r.delete(key)

    async def exists(self, key: str) -> bool:
        return bool(await self._r.exists(key))

    async def incr(self, key: str, ttl: int = 86400) -> int:
        pipe = self._r.pipeline()
        pipe.incr(key)
        pipe.expire(key, ttl)
        results = await pipe.execute()
        return results[0]


class RateLimiter:
    """Sliding-window rate limiter backed by Redis with atomic Lua increment."""

    # Atomic INCR + conditional EXPIRE: only sets TTL on first creation so the
    # window doesn't reset on every call. Non-atomic pipeline had a race where
    # two concurrent callers could both INCR before either EXPIRE ran, and
    # EXPIRE extending the window on every call would silently extend limits.
    _LUA = """
local current = redis.call('INCR', KEYS[1])
if current == 1 then
    redis.call('EXPIRE', KEYS[1], ARGV[1])
end
return current
"""

    def __init__(self, redis: aioredis.Redis) -> None:
        self._r = redis
        self._script = redis.register_script(self._LUA)

    async def check_and_consume(
        self,
        key: str,
        max_calls: int,
        window_seconds: int,
    ) -> bool:
        """Return True if request allowed, False if rate limited."""
        count = await self._script(keys=[key], args=[window_seconds])
        return int(count) <= max_calls

    async def wait_if_needed(
        self,
        key: str,
        max_calls: int,
        window_seconds: int,
        max_wait: float = 60.0,
    ) -> None:
        """Block until a token is available. max_wait caps total sleep time."""
        import asyncio
        slept = 0.0
        sleep_step = max(0.5, window_seconds / max_calls)
        while not await self.check_and_consume(key, max_calls, window_seconds):
            logger.debug("rate_limited", key=key)
            if slept >= max_wait:
                logger.warning("rate_limit_wait_timeout", key=key, max_wait=max_wait)
                return
            await asyncio.sleep(sleep_step)
            slept += sleep_step
