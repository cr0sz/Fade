"""Abstract broker interface.

Every broker (Binance, Alpaca, Paper) implements this contract so the
engine never knows which one it's talking to.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional


@dataclass
class OrderResult:
    success: bool
    order_id: str
    asset: str
    direction: str          # "long" | "short"
    qty: float              # units filled
    fill_price: float       # average fill price (0 if unknown yet)
    notional: float         # qty * fill_price
    fee: float
    error: Optional[str] = None  # set when success=False
    stop_order_id: Optional[str] = None  # Sprint 57: resting STOP_MARKET order ID (live futures only)


class AbstractBroker(ABC):
    """Minimal async broker interface used by the trading engine."""

    broker_name: str = "abstract"

    @abstractmethod
    async def place_order(
        self,
        asset: str,
        direction: str,       # "long" | "short"
        notional_usd: float,  # how much cash to deploy
    ) -> OrderResult:
        """Place a market order. Returns fill details."""
        ...

    @abstractmethod
    async def close_position(
        self,
        asset: str,
        qty: float,
        direction: str,  # direction of the position being CLOSED
        full_exit: bool = False,  # full_exit: futures-only; ignored here
        maker: bool = False,  # maker: futures-only; ignored here
    ) -> OrderResult:
        """Close (fully) an existing position.
        full_exit=True tells the futures broker to use the actual exchange
        position qty instead of the passed qty, preventing dust remnants."""
        ...

    @abstractmethod
    async def get_balance(self) -> float:
        """Return available cash balance in USD (or USDT for crypto)."""
        ...

    @abstractmethod
    async def health_check(self) -> bool:
        """Return True if the broker API is reachable and credentials are valid."""
        ...
