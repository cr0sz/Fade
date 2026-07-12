"""Minimal read-only Docker Engine API client over the unix socket.

We deliberately avoid the docker SDK (not in requirements.txt) and only ever
issue a single read call — GET /containers/json — over the mounted socket using
aiohttp's UnixConnector. The socket is mounted :ro and this is the only endpoint
we ever touch.
"""
from __future__ import annotations

import json
from typing import Any

import aiohttp

from src.core.logging import get_logger

logger = get_logger(__name__)

_SOCK_PATH = "/var/run/docker.sock"
# Any host works; UnixConnector routes everything over the socket regardless.
_BASE = "http://docker"


async def list_containers(all_states: bool = True, timeout: float = 5.0) -> list[dict[str, Any]]:
    """Return the raw /containers/json list, or [] if the socket is unreachable."""
    connector = aiohttp.UnixConnector(path=_SOCK_PATH)
    url = f"{_BASE}/containers/json"
    params = {"all": "true" if all_states else "false"}
    try:
        async with aiohttp.ClientSession(connector=connector) as session:
            async with session.get(
                url, params=params, timeout=aiohttp.ClientTimeout(total=timeout)
            ) as resp:
                if resp.status != 200:
                    body = await resp.text()
                    logger.error("docker_api_bad_status", status=resp.status, body=body[:200])
                    return []
                return json.loads(await resp.text())
    except Exception as e:  # socket missing, perms, daemon down
        logger.error("docker_api_unreachable", error=str(e))
        return []
