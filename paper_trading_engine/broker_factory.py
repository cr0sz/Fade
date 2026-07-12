"""Broker factory — routes signals to the right broker by exchange.

Routing:
  CRYPTO (short) → BinanceFuturesBroker
  CRYPTO (long)  → BinanceBroker
  Everything else → PaperBroker (no live broker)

TRADING_MODE env var:
  paper (default) — always use PaperBroker, never touch real money
  live            — route CRYPTO to real Binance brokers
"""
from __future__ import annotations

import os

import structlog

from brokers.base import AbstractBroker
from brokers.paper import PaperBroker
from brokers.binance import BinanceBroker
from brokers.binance_futures import BinanceFuturesBroker

logger = structlog.get_logger(__name__)

_TRADING_MODE = os.getenv("TRADING_MODE", "paper").strip().lower()


def futures_enabled() -> bool:
    return os.getenv("BINANCE_FUTURES", "true").strip().lower() in ("1", "true", "yes")


# Singletons — created once, reused across all orders
_binance: BinanceBroker | None = None
_binance_futures: BinanceFuturesBroker | None = None


def get_broker(exchange: str, portfolio, direction: str = "") -> AbstractBroker:
    """Return the appropriate broker for a given exchange string.

    Sprint 42: CRYPTO routing is direction-aware. Longs execute on SPOT
    (where the pilot bankroll lives — routing them to futures sent orders
    at an empty wallet); shorts need futures (spot cannot short-sell).
    Closes pass the position's own direction, so a position always closes
    on the venue that opened it.
    """
    global _binance, _binance_futures

    if _TRADING_MODE != "live":
        return PaperBroker(portfolio)

    exchange_upper = (exchange or "").upper()

    if exchange_upper == "CRYPTO":
        if direction == "short" and futures_enabled():
            if _binance_futures is None:
                _binance_futures = BinanceFuturesBroker()
            if _binance_futures._configured:
                return _binance_futures
            # LIVE MODE: futures not configured — cannot execute CRYPTO short.
            # Raise so the caller blocks the trade rather than silently papering a live short.
            raise RuntimeError(
                "live_futures_broker_not_configured: BINANCE_FUTURES_API_KEY missing or invalid; "
                "CRYPTO short blocked to prevent silent paper fallback"
            )
        if _binance is None:
            _binance = BinanceBroker()
        if _binance._configured:
            return _binance
        raise RuntimeError(
            "live_spot_broker_not_configured: BINANCE_API_KEY missing or invalid; "
            "CRYPTO long blocked to prevent silent paper fallback"
        )

    # Non-CRYPTO: no live broker
    return PaperBroker(portfolio)


def trading_mode() -> str:
    return _TRADING_MODE
