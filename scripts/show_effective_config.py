#!/usr/bin/env python3
"""
Show the paper-trading engine's last persisted *effective config*.

The engine writes the parameters actually in effect at startup to Redis
(engine:effective_config / engine:effective_config_hash) via
PaperTradingEngine._publish_effective_config(). This script reads those keys
back and pretty-prints them, so you can confirm what config actually landed at
runtime without grepping container logs.

Usage:
    python scripts/show_effective_config.py
"""
from __future__ import annotations

import asyncio
import json
import os
import sys

import redis.asyncio as aioredis

_CONFIG_KEY = "engine:effective_config"
_HASH_KEY = "engine:effective_config_hash"


def _redis_url() -> str:
    """Resolve the Redis URL.

    Prefer src/core/config.py Settings (the established pattern) when importable;
    otherwise fall back to the same env-var construction the engine/governance
    scripts use.
    """
    repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if repo_root not in sys.path:
        sys.path.insert(0, repo_root)
    try:
        from src.core.config import get_settings  # type: ignore

        url = get_settings().redis_url
        # Inside compose the host is "redis"; from the host machine the port is
        # published on localhost only, so rewrite the hostname when run outside.
        return url.replace("@redis:", "@localhost:").replace("//redis:", "//localhost:")
    except Exception:
        pass
    # Fallback: env vars, hydrated from .env when not already exported (the
    # host shell normally doesn't source .env).
    env_file = os.path.join(repo_root, ".env")
    if os.path.isfile(env_file):
        with open(env_file) as fh:
            for line in fh:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, _, value = line.partition("=")
                os.environ.setdefault(key.strip(), value.strip())
    password = os.getenv("REDIS_PASSWORD", "")
    host = os.getenv("REDIS_HOST", "localhost")
    if host == "redis":  # compose-internal hostname; port is published on localhost
        host = "localhost"
    port = os.getenv("REDIS_PORT", "6379")
    db = os.getenv("REDIS_DB", "0")
    if password:
        return f"redis://:{password}@{host}:{port}/{db}"
    return f"redis://{host}:{port}/{db}"


async def _main() -> int:
    redis = aioredis.from_url(_redis_url(), encoding="utf-8", decode_responses=True)
    try:
        raw = await redis.get(_CONFIG_KEY)
        cfg_hash = await redis.get(_HASH_KEY)
    finally:
        await redis.aclose()

    if not raw:
        print(
            f"No effective config found in Redis (key '{_CONFIG_KEY}').\n"
            "The paper-trading engine writes it once per startup — has it run "
            "since this feature was deployed?"
        )
        return 1

    try:
        cfg = json.loads(raw)
    except json.JSONDecodeError as exc:
        print(f"Stored config is not valid JSON: {exc}")
        print(raw)
        return 1

    print("=" * 64)
    print("Paper-trading engine — last effective config")
    print("=" * 64)
    print(f"config_hash: {cfg_hash}")
    print("-" * 64)
    print(json.dumps(cfg, indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(_main()))
