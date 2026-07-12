"""Binance funding-rate & liquidation data logger.

Two concurrent jobs over public USDT-perp endpoints (no API keys):

  * Liquidations: subscribes to the all-market !forceOrder@arr websocket and
    persists every event (the stream is throttled ~1 msg/sec/symbol, so row
    volume is bounded). Inserts are buffered and flushed in batches.

  * Funding rates: polls REST /premiumIndex every 5 min (one call returns all
    symbols) and stores rows only for the bot's CRYPTO universe to bound volume.

Robustness: websocket auto-reconnect with capped exponential backoff + jitter
(Binance force-closes connections ~every 24h), a bounded in-memory buffer that
drops oldest rows on a DB outage, and a heartbeat log every ~15 min.

Run as: python -m src.market_data_logger.main
"""
from __future__ import annotations

import asyncio
import random
from collections import deque
from datetime import datetime, timezone

import aiohttp
import asyncpg

from src.cache.redis_client import get_redis
from src.core.config import get_settings
from src.core.logging import (
    bind_service_context,
    configure_logging,
    get_logger,
    utc_now,
)

logger = get_logger(__name__)

# ── Endpoints ────────────────────────────────────────────────────────────────
WS_URL = "wss://fstream.binance.com/ws/!forceOrder@arr"
PREMIUM_INDEX_URL = "https://fapi.binance.com/fapi/v1/premiumIndex"

# ── Tunables ─────────────────────────────────────────────────────────────────
FLUSH_INTERVAL_SECONDS = 5.0      # flush liquidation buffer at least this often
FLUSH_MAX_ROWS = 200              # ...or when this many rows accumulate
FUNDING_POLL_SECONDS = 300        # 5 min
UNIVERSE_REFRESH_SECONDS = 3600   # 1 hour
HEARTBEAT_SECONDS = 900           # 15 min
# If the liquidation websocket goes silent this long while still "connected"
# (no TEXT frames at all), treat the stream as dead even though the container is
# Up. Binance !forceOrder@arr is sparse but rarely fully silent for >30 min
# across the whole market; the watchdog alerts off the Redis heartbeat below.
WS_SILENCE_WARN_SECONDS = 1800    # 30 min with zero liq frames -> warn + flag
LIQ_WS_HEARTBEAT_KEY = "market_data:last_liq_ws"  # ISO ts of last TEXT frame
LIQ_WS_ALIVE_KEY = "market_data:liq_ws_alive_at"  # ISO ts of last ws connect/heartbeat
BUFFER_MAX_ROWS = 5000            # bound memory on DB outage; drop oldest beyond
WS_BACKOFF_CAP = 60.0             # max reconnect backoff
HTTP_TIMEOUT = aiohttp.ClientTimeout(total=15)


def _ms_to_dt(ms: int | str | None) -> datetime | None:
    if ms is None:
        return None
    return datetime.fromtimestamp(int(ms) / 1000.0, tz=timezone.utc)


def _to_float(v) -> float | None:
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


class MarketDataLogger:
    def __init__(self) -> None:
        self.settings = get_settings()
        self.pool: asyncpg.Pool | None = None
        self.redis = None
        # Wall-clock of the last liquidation TEXT frame received on the ws.
        self._last_ws_msg_at: datetime = utc_now()
        # Active websocket object; set on connect, cleared on close/reconnect.
        # Used by _heartbeat_loop to force-close a silent-but-open connection.
        self._liq_ws = None
        # Liquidation rows pending insert: tuples ordered for the INSERT below.
        self._liq_buffer: deque[tuple] = deque(maxlen=BUFFER_MAX_ROWS)
        self._dropped_rows = 0
        # Universe symbols mapped to Binance USDT-perp pairs, e.g. {"BTCUSDT", ...}
        self._universe_pairs: set[str] = set()
        # Counters for the heartbeat line.
        self._liqs_stored = 0
        self._funding_stored = 0
        self._liqs_seen = 0
        self._stop = asyncio.Event()

    # ── Universe ─────────────────────────────────────────────────────────────
    async def _refresh_universe(self) -> None:
        """Load CRYPTO symbols from tickers and map to USDT-perp pairs."""
        assert self.pool is not None
        rows = await self.pool.fetch(
            "SELECT symbol FROM tickers WHERE exchange = 'CRYPTO'"
        )
        pairs = {f"{r['symbol'].upper()}USDT" for r in rows}
        if pairs and pairs != self._universe_pairs:
            self._universe_pairs = pairs
            logger.info("universe_refreshed", pairs=len(pairs))
        elif not pairs:
            logger.warning("universe_empty", note="no CRYPTO tickers found")

    async def _universe_loop(self) -> None:
        while not self._stop.is_set():
            try:
                await self._refresh_universe()
            except Exception as exc:  # noqa: BLE001
                logger.warning("universe_refresh_failed", error=str(exc))
            await self._sleep(UNIVERSE_REFRESH_SECONDS)

    # ── Funding rates ────────────────────────────────────────────────────────
    async def _funding_loop(self, session: aiohttp.ClientSession) -> None:
        while not self._stop.is_set():
            try:
                await self._poll_funding(session)
            except Exception as exc:  # noqa: BLE001
                logger.warning("funding_poll_failed", error=str(exc))
            await self._sleep(FUNDING_POLL_SECONDS)

    async def _poll_funding(self, session: aiohttp.ClientSession) -> None:
        if not self._universe_pairs:
            logger.info("funding_poll_skipped", reason="universe_not_loaded")
            return
        async with session.get(PREMIUM_INDEX_URL, timeout=HTTP_TIMEOUT) as resp:
            resp.raise_for_status()
            data = await resp.json()

        now = utc_now()
        rows: list[tuple] = []
        for item in data:
            sym = item.get("symbol", "").upper()
            if sym not in self._universe_pairs:
                continue
            rows.append((
                sym,
                _to_float(item.get("lastFundingRate")),
                _to_float(item.get("markPrice")),
                _ms_to_dt(item.get("nextFundingTime")),
                now,
            ))
        if not rows:
            logger.warning("funding_poll_no_universe_rows", returned=len(data))
            return

        assert self.pool is not None
        await self.pool.executemany(
            """
            INSERT INTO funding_rates
                (symbol, funding_rate, mark_price, next_funding_time, ts)
            VALUES ($1, $2, $3, $4, $5)
            """,
            rows,
        )
        self._funding_stored += len(rows)
        logger.info("funding_stored", rows=len(rows))

    # ── Liquidations ─────────────────────────────────────────────────────────
    def _handle_force_order(self, msg: dict) -> None:
        """Parse a forceOrder payload and buffer a row.

        Payload: {"e":"forceOrder","E":<event ms>,"o":{
            "s":symbol,"S":side,"o":orderType,"f":timeInForce,"q":origQty,
            "p":price,"ap":avgPrice,"X":orderStatus,"l":lastFilledQty,
            "z":cumFilledQty,"T":tradeTime ms}}
        """
        o = msg.get("o")
        if not o:
            return
        symbol = o.get("s")
        if not symbol:
            return
        # Prefer average fill price; fall back to order price.
        price = _to_float(o.get("ap")) or _to_float(o.get("p"))
        # Cumulative filled qty is the realized liquidation size.
        qty = _to_float(o.get("z"))
        if qty is None:
            qty = _to_float(o.get("l")) or _to_float(o.get("q"))
        notional = (price * qty) if (price is not None and qty is not None) else None

        self._liqs_seen += 1
        if len(self._liq_buffer) >= BUFFER_MAX_ROWS:
            # deque(maxlen) would silently evict; count it explicitly.
            self._dropped_rows += 1
            if self._dropped_rows % 500 == 1:
                logger.warning(
                    "liq_buffer_full_dropping_oldest",
                    dropped_total=self._dropped_rows,
                    cap=BUFFER_MAX_ROWS,
                )
        self._liq_buffer.append((
            symbol,
            o.get("S"),
            price,
            qty,
            notional,
            o.get("X"),
            _ms_to_dt(o.get("T")),
        ))

    async def _flush_loop(self) -> None:
        while not self._stop.is_set():
            await self._sleep(FLUSH_INTERVAL_SECONDS)
            await self._flush_liquidations()

    async def _flush_liquidations(self) -> None:
        if not self._liq_buffer:
            return
        batch = list(self._liq_buffer)
        try:
            assert self.pool is not None
            await self.pool.executemany(
                """
                INSERT INTO liquidation_events
                    (symbol, side, price, qty, notional, order_status, event_time)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                ON CONFLICT (symbol, event_time, price, qty) DO NOTHING
                """,
                batch,
            )
        except Exception as exc:  # noqa: BLE001
            # Leave rows buffered for the next flush (bounded by maxlen).
            logger.warning("liq_flush_failed", error=str(exc), buffered=len(batch))
            return
        # Drop exactly what we flushed; new rows may have arrived meanwhile.
        for _ in range(len(batch)):
            if self._liq_buffer:
                self._liq_buffer.popleft()
        self._liqs_stored += len(batch)

    async def _ws_loop(self, session: aiohttp.ClientSession) -> None:
        backoff = 1.0
        while not self._stop.is_set():
            try:
                logger.info("ws_connecting", url=WS_URL)
                async with session.ws_connect(
                    WS_URL, heartbeat=30, timeout=15
                ) as ws:
                    logger.info("ws_connected")
                    self._liq_ws = ws
                    backoff = 1.0  # reset on a clean connect
                    await self._write_liq_ws_alive()
                    async for raw in ws:
                        if raw.type == aiohttp.WSMsgType.TEXT:
                            self._last_ws_msg_at = utc_now()
                            try:
                                self._handle_force_order(raw.json())
                                if len(self._liq_buffer) >= FLUSH_MAX_ROWS:
                                    await self._flush_liquidations()
                            except Exception as exc:  # noqa: BLE001
                                logger.warning("liq_parse_failed", error=str(exc))
                        elif raw.type in (
                            aiohttp.WSMsgType.CLOSED,
                            aiohttp.WSMsgType.ERROR,
                        ):
                            logger.info("ws_closed", msg_type=str(raw.type))
                            break
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                logger.warning("ws_error", error=str(exc))
            finally:
                self._liq_ws = None
            if self._stop.is_set():
                break
            sleep_for = min(backoff, WS_BACKOFF_CAP) + random.uniform(0, 1.0)
            logger.info("ws_reconnect_backoff", seconds=round(sleep_for, 2))
            await self._sleep(sleep_for)
            backoff = min(backoff * 2, WS_BACKOFF_CAP)

    # ── Heartbeat ────────────────────────────────────────────────────────────
    async def _liq_ws_silence_age(self) -> float:
        return (utc_now() - self._last_ws_msg_at).total_seconds()

    async def _write_liq_ws_heartbeat(self) -> None:
        """Publish the last liquidation-frame wall-clock to Redis so the watchdog
        can detect a silent-but-Up stream. Fail-open: never crash the logger."""
        if self.redis is None:
            return
        try:
            await self.redis.set(
                LIQ_WS_HEARTBEAT_KEY, self._last_ws_msg_at.isoformat()
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("liq_ws_heartbeat_write_failed", error=str(exc))

    async def _write_liq_ws_alive(self) -> None:
        """Publish current wall-clock to Redis on each ws connect + heartbeat.
        Lets the watchdog distinguish 'stream alive but no events' from 'stream dead'."""
        if self.redis is None:
            return
        try:
            await self.redis.set(LIQ_WS_ALIVE_KEY, utc_now().isoformat())
        except Exception as exc:  # noqa: BLE001
            logger.warning("liq_ws_alive_write_failed", error=str(exc))

    async def _heartbeat_loop(self) -> None:
        while not self._stop.is_set():
            await self._sleep(HEARTBEAT_SECONDS)
            silence_age = await self._liq_ws_silence_age()
            await self._write_liq_ws_heartbeat()
            await self._write_liq_ws_alive()
            if silence_age > 2700:  # 45 min: force-close the silent socket
                logger.warning(
                    "liq_ws_silent_force_close",
                    silence_age_sec=round(silence_age, 0),
                    note="forcing ws close to trigger reconnect",
                )
                if self._liq_ws is not None:
                    try:
                        await self._liq_ws.close()
                    except Exception:  # noqa: BLE001
                        pass
                    self._liq_ws = None
            elif silence_age > WS_SILENCE_WARN_SECONDS:
                logger.warning(
                    "liq_ws_silent",
                    silence_age_sec=round(silence_age, 0),
                    threshold_sec=WS_SILENCE_WARN_SECONDS,
                    note="no liquidation frames — stream may be dead while Up",
                )
            logger.info(
                "heartbeat",
                liqs_stored=self._liqs_stored,
                liqs_seen=self._liqs_seen,
                funding_stored=self._funding_stored,
                buffer=len(self._liq_buffer),
                dropped=self._dropped_rows,
                universe_pairs=len(self._universe_pairs),
                liq_ws_silence_sec=round(silence_age, 0),
            )

    # ── Lifecycle ────────────────────────────────────────────────────────────
    async def _sleep(self, seconds: float) -> None:
        """Sleep that wakes early on stop."""
        try:
            await asyncio.wait_for(self._stop.wait(), timeout=seconds)
        except asyncio.TimeoutError:
            pass

    async def run(self) -> None:
        self.pool = await asyncpg.create_pool(
            dsn=self.settings.database_url,
            min_size=1,
            max_size=2,
            command_timeout=30,
        )
        logger.info("db_pool_created", min=1, max=2)
        try:
            self.redis = await get_redis()
            # Seed only if key missing (first run). On restart: preserve existing
            # value — it's more accurate than utc_now() and prevents the watchdog
            # getting a false 4h grace window every time the container restarts.
            if not await self.redis.exists(LIQ_WS_HEARTBEAT_KEY):
                await self._write_liq_ws_heartbeat()
        except Exception as exc:  # noqa: BLE001
            logger.warning("redis_init_failed_continuing", error=str(exc))
            self.redis = None
        try:
            await self._refresh_universe()
        except Exception as exc:  # noqa: BLE001
            logger.warning("initial_universe_load_failed", error=str(exc))

        async with aiohttp.ClientSession() as session:
            tasks = [
                asyncio.create_task(self._ws_loop(session)),
                asyncio.create_task(self._flush_loop()),
                asyncio.create_task(self._funding_loop(session)),
                asyncio.create_task(self._universe_loop()),
                asyncio.create_task(self._heartbeat_loop()),
            ]
            logger.info("market_data_logger_started")
            try:
                await asyncio.gather(*tasks)
            finally:
                self._stop.set()
                for t in tasks:
                    t.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
                await self._flush_liquidations()
                if self.pool:
                    await self.pool.close()
                logger.info("market_data_logger_stopped")


async def _amain() -> None:
    configure_logging()
    bind_service_context("market-data-logger")
    await MarketDataLogger().run()


if __name__ == "__main__":
    try:
        asyncio.run(_amain())
    except KeyboardInterrupt:
        pass
