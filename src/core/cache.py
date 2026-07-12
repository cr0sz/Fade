from __future__ import annotations

"""Module-level cache helpers backed by src/cache/redis_client.py.

Provides get_cache / set_cache without requiring callers to manage a CacheClient
instance. Redis connection is established lazily on first call.
"""

import json
import os
from typing import Any

from src.cache.redis_client import get_redis


def _default_ttl() -> int:
    return int(os.getenv("CACHE_TTL_SECONDS", "300"))


def _enabled() -> bool:
    return os.getenv("ENABLE_CACHE", "true").lower() == "true"


async def get_cache(key: str) -> Any | None:
    if not _enabled():
        return None
    r = await get_redis()
    raw = await r.get(key)
    if raw is None:
        return None
    try:
        return json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return raw


async def set_cache(key: str, value: Any, ttl: int | None = None) -> None:
    if not _enabled():
        return
    r = await get_redis()
    ttl = ttl if ttl is not None else _default_ttl()
    if isinstance(value, (dict, list)):
        serialized = json.dumps(value, default=str)
    else:
        serialized = str(value)
    await r.setex(key, ttl, serialized)


async def delete_cache(key: str) -> None:
    if not _enabled():
        return
    r = await get_redis()
    await r.delete(key)
