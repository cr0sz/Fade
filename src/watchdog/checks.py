"""The individual infra health checks. Each returns (ok: bool, detail: str).

A check raising is treated as a soft failure by the caller, never crashing the
loop.
"""
from __future__ import annotations

import os
from datetime import datetime, timezone

import aiohttp

from src.core.logging import get_logger
from src.watchdog.docker_client import list_containers

logger = get_logger(__name__)

COMPOSE_PROJECT = "market-intelligence"
API_HEALTH_URL = "http://api:8000/health"

# Services that may legitimately be stopped (compose `profiles:` gated).
_ALLOWED_STOPPED = {"jupyter"} | set(
    s.strip() for s in os.getenv("WATCHDOG_IGNORE_CONTAINERS", "freetime-researcher").split(",") if s.strip()
)
# States that mean a service is down and should alert.
_BAD_STATES = {"exited", "dead", "restarting"}

FUNDING_MAX_AGE_SEC = 20 * 60  # 20 min
QUOTE_CADENCE_FLOOR_SEC = 10 * 60  # 10 min floor for observed median cadence
# Liquidation websocket silence: market-data-logger publishes the wall-clock of
# the last !forceOrder frame to this Redis key. A funding poller can stay healthy
# while the liq stream is silently dead (container still Up), so we track it
# separately. Threshold is generous because the stream is genuinely sparse.
LIQ_WS_HEARTBEAT_KEY = "market_data:last_liq_ws"
LIQ_WS_ALIVE_KEY = "market_data:liq_ws_alive_at"
LIQ_WS_DEAD_SEC = 30 * 60           # 30 min without a connect/heartbeat → stream truly dead
LIQ_WS_MAX_SILENCE_SEC = 4 * 60 * 60  # 4h event silence → warn (market may be quiet)


def _age_seconds(ts) -> float:
    if ts is None:
        return float("inf")
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return (datetime.now(timezone.utc) - ts).total_seconds()


# ── 1. Containers ─────────────────────────────────────────────────────────────
async def check_containers() -> tuple[bool, str]:
    containers = await list_containers(all_states=True)
    if not containers:
        # Socket unreachable — can't assess; report but don't hard-fail containers.
        return False, "docker socket unreachable (cannot list containers)"

    bad: list[str] = []
    for c in containers:
        labels = c.get("Labels") or {}
        if labels.get("com.docker.compose.project") != COMPOSE_PROJECT:
            continue
        service = labels.get("com.docker.compose.service", "")
        # Skip one-off `docker compose run` containers (they carry a slot/oneoff label).
        if labels.get("com.docker.compose.oneoff") == "True":
            continue
        if service in _ALLOWED_STOPPED:
            continue
        state = (c.get("State") or "").lower()
        if state in _BAD_STATES:
            name = (c.get("Names") or ["?"])[0].lstrip("/")
            bad.append(f"{name}={state}")

    if bad:
        return False, "down: " + ", ".join(sorted(bad))
    return True, "all compose containers running"


# ── 2. Funding freshness ──────────────────────────────────────────────────────
async def check_funding_freshness(db) -> tuple[bool, str]:
    async with db.acquire() as conn:
        ts = await conn.fetchval("SELECT max(ts) FROM funding_rates")
    age = _age_seconds(ts)
    if age > FUNDING_MAX_AGE_SEC:
        mins = age / 60 if age != float("inf") else age
        return False, f"funding_rates stale: last={ts} age={mins:.1f}min (>20min) — market-data-logger silent"
    return True, f"funding fresh (age={age/60:.1f}min)"


# ── 3. Quote freshness ────────────────────────────────────────────────────────
async def measure_quote_cadence_sec(db) -> float:
    """Median seconds between distinct CRYPTO quote poll cycles over last 24h.

    Quotes land in batches sharing a timestamp; we collapse to distinct minute
    buckets so the median reflects the real poll interval, then floor at 10 min.
    """
    async with db.acquire() as conn:
        median = await conn.fetchval(
            """
            WITH cycles AS (
                SELECT DISTINCT date_trunc('minute', ts) AS m
                FROM ticker_quotes
                WHERE exchange = 'CRYPTO' AND ts > now() - interval '24 hours'
            ),
            gaps AS (
                SELECT EXTRACT(EPOCH FROM m - lag(m) OVER (ORDER BY m)) AS g
                FROM cycles
            )
            SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY g)
            FROM gaps WHERE g IS NOT NULL AND g > 0
            """
        )
    cadence = float(median) if median else QUOTE_CADENCE_FLOOR_SEC
    return max(cadence, QUOTE_CADENCE_FLOOR_SEC)


async def check_quote_freshness(db, cadence_sec: float) -> tuple[bool, str]:
    threshold = 3 * cadence_sec
    async with db.acquire() as conn:
        ts = await conn.fetchval(
            "SELECT max(ts) FROM ticker_quotes WHERE exchange = 'CRYPTO'"
        )
    age = _age_seconds(ts)
    if age > threshold:
        mins = age / 60 if age != float("inf") else age
        return False, (
            f"CRYPTO quotes stale: last={ts} age={mins:.1f}min "
            f"(>{threshold/60:.1f}min = 3x cadence) — quote worker silent"
        )
    return True, f"quotes fresh (age={age/60:.1f}min, thresh={threshold/60:.1f}min)"


# ── 3b. Liquidation websocket silence ────────────────────────────────────────
async def check_liq_stream(redis) -> tuple[bool, str]:
    """Detect a truly-dead liquidation websocket.

    Two Redis keys:
      liq_ws_alive_at  — updated on every ws connect + 15-min heartbeat; shows
                         whether the logger loop is alive regardless of events.
      last_liq_ws      — updated only when a TEXT forceOrder frame is received;
                         used to detect genuine market silence.

    We alert HIGH only when the loop itself is dead (alive_at stale > 30 min).
    Event staleness is informational — the market can be quiet for hours.
    """
    # ── 1. Is the logger loop alive? ─────────────────────────────────────────
    try:
        raw_alive = await redis.get(LIQ_WS_ALIVE_KEY)
    except Exception as e:
        return False, f"liq-ws alive key unreadable from redis: {e}"

    if raw_alive:
        if isinstance(raw_alive, bytes):
            raw_alive = raw_alive.decode()
        try:
            alive_at = datetime.fromisoformat(raw_alive)
            alive_age = _age_seconds(alive_at)
            if alive_age > LIQ_WS_DEAD_SEC:
                return False, (
                    f"liq-ws loop dead: last heartbeat {alive_age/60:.1f}min ago "
                    f"(>{LIQ_WS_DEAD_SEC/60:.0f}min) — logger crashed or stuck"
                )
        except ValueError:
            return False, f"liq-ws alive key unparseable: {raw_alive!r}"
    # alive_at key missing means logger hasn't written it yet (new key, first deploy)
    # fall through to event-based check for backwards compat

    # ── 2. Event staleness (informational) ───────────────────────────────────
    try:
        raw = await redis.get(LIQ_WS_HEARTBEAT_KEY)
    except Exception as e:
        return False, f"liq-ws heartbeat unreadable from redis: {e}"
    if not raw:
        if not raw_alive:
            return False, "liq-ws keys missing (logger not publishing)"
        return True, "liq-ws alive — no events yet (stream just started)"
    if isinstance(raw, bytes):
        raw = raw.decode()
    try:
        last = datetime.fromisoformat(raw)
    except ValueError:
        return False, f"liq-ws heartbeat unparseable: {raw!r}"
    event_age = _age_seconds(last)
    if event_age > LIQ_WS_MAX_SILENCE_SEC:
        # Stream is alive (reconnecting) but no liq events — market is quiet
        return True, (
            f"liq-ws alive, no events {event_age/60:.0f}min (market quiet or "
            f"Binance !forceOrder silent)"
        )
    return True, f"liq-ws active (last frame {event_age/60:.1f}min ago)"


# ── 4. API health ─────────────────────────────────────────────────────────────
async def check_api_health() -> tuple[bool, str]:
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                API_HEALTH_URL, timeout=aiohttp.ClientTimeout(total=5)
            ) as resp:
                if resp.status != 200:
                    return False, f"api /health HTTP {resp.status}"
                return True, "api /health 200"
    except Exception as e:
        return False, f"api /health unreachable: {e}"
