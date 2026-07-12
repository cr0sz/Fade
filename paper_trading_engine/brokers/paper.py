"""Paper broker — wraps the existing executor/portfolio simulation.

Used when TRADING_MODE=paper (default). Delegates to executor.open_trade /
close_trade so paper mode behaviour is identical to Phase 1/2.
"""
from __future__ import annotations

import uuid
from typing import TYPE_CHECKING

from brokers.base import AbstractBroker, OrderResult

if TYPE_CHECKING:
    from models import Signal
    from portfolio import Portfolio


class PaperBroker(AbstractBroker):
    broker_name = "paper"

    def __init__(self, portfolio: "Portfolio") -> None:
        self._portfolio = portfolio

    async def place_order(
        self,
        asset: str,
        direction: str,
        notional_usd: float,
    ) -> OrderResult:
        # Paper mode: the engine calls portfolio.open_position directly.
        # This method exists for interface compliance only.
        return OrderResult(
            success=True,
            order_id=str(uuid.uuid4()),
            asset=asset,
            direction=direction,
            qty=0.0,
            fill_price=0.0,
            notional=notional_usd,
            fee=notional_usd * 0.001,
        )

    async def close_position(
        self,
        asset: str,
        qty: float,
        direction: str,
        full_exit: bool = False,  # full_exit: futures-only; ignored here
        maker: bool = False,  # maker: futures-only; ignored here
    ) -> OrderResult:
        return OrderResult(
            success=True,
            order_id=str(uuid.uuid4()),
            asset=asset,
            direction=direction,
            qty=qty,
            fill_price=0.0,
            notional=0.0,
            fee=0.0,
        )

    async def get_balance(self) -> float:
        return self._portfolio.balance

    async def health_check(self) -> bool:
        return True
