"""Execution simulator: converts a signal into a PaperTrade.

Price model (in priority order):
  1. signal.real_price  — live market price from ticker_quotes (Phase 2+)
  2. composite_score    — normalized 0-100 score used as price proxy (legacy fallback)

Position sizing: per-exchange TRADE_FRACTION from risk module.
Slippage: 0.05% of price, applied in the adverse direction.
Fee:      0.05%/leg; BOTH legs subtracted from PnL. Live trades carry the broker's
          real maker/taker fee on trade.fee (set at open from the actual fill).
"""
from __future__ import annotations

import os
import uuid
from datetime import datetime, timezone
from typing import TYPE_CHECKING

import risk
from models import PaperTrade

if TYPE_CHECKING:
    from models import Signal
    from portfolio import Portfolio

SLIPPAGE_PCT    = 0.0005  # 0.05%
FEE_RATE        = 0.0005  # 0.05% taker — matches broker _TAKER_FEE; live exits are always MARKET (taker)
MIN_NOTIONAL    = 20.0    # skip trades below $20 — Binance minimum is ~$5-10 per pair

# Exchange-specific hold limits — Sprint 38, price-truth signal_outcomes data:
# directional edge is flat at 1-6h (42-54% for every engine) and only develops
# at the 24h window the Truth Router calibrates against. The old 2h/6h limits
# (from the circular-truth era) closed positions before the edge could play out.
_HOLD_CRYPTO_LONG  = 8    # hours: cut from 12; tighten exits on underperforming longs
_HOLD_CRYPTO_SHORT = int(os.getenv("MAX_HOLD_CRYPTO_SHORT", "24"))
_HOLD_CRYPTO_SHORT_RISK_ON = int(os.getenv("MAX_HOLD_CRYPTO_SHORT_RISK_ON", "6"))
_HOLD_STOCK        = 48   # hours: daily candles need 2 trading days to beat fees
_HOLD_BIST         = 6    # hours: ~1 BIST trading session


def _max_hold_hours(confidence: float, exchange: str = "", direction: str = "", regime: str = "") -> int:
    """Return exchange + direction appropriate hold limit."""
    exc = (exchange or "").upper()
    if exc in ("NASDAQ", "NYSE", "CBOE"):
        return _HOLD_STOCK
    if exc == "BIST":
        return _HOLD_BIST
    # CRYPTO or unknown exchange: direction decides hold
    if direction == "short":
        if regime == "risk_on":
            return _HOLD_CRYPTO_SHORT_RISK_ON
        return _HOLD_CRYPTO_SHORT
    return _HOLD_CRYPTO_LONG


def _effective_price(signal: "Signal") -> float:
    """Return the best available price for this signal. Returns 0.0 when unavailable."""
    if signal.real_price and signal.real_price > 0:
        return signal.real_price
    return 0.0


def open_trade(signal: "Signal", portfolio: "Portfolio") -> "PaperTrade | None":
    """Simulate a market-order fill and return a PaperTrade, or None if no real price."""
    base_price = _effective_price(signal)
    if base_price <= 0:
        return None
    slippage = base_price * SLIPPAGE_PCT
    entry_price = base_price + slippage if signal.direction == "long" else base_price - slippage

    fraction = risk.trade_fraction(signal.exchange or "", signal.confidence)
    fraction *= min(1.5, getattr(signal, "vol_scale", 1.0))
    notional = portfolio.balance * fraction
    # Liquidity cap: never commit more than the pre-computed volume_cap_usd
    if signal.volume_cap_usd > 0:
        notional = min(notional, signal.volume_cap_usd)
    # Skip microscopic positions — not worth occupying a trade slot
    if notional < MIN_NOTIONAL:
        return None
    size = notional / entry_price
    fee = notional * FEE_RATE

    trade = PaperTrade(
        trade_id=str(uuid.uuid4()),
        signal_id=signal.signal_id,
        asset=signal.asset,
        direction=signal.direction,
        size=size,
        entry_price=entry_price,
        notional=notional,
        fee=fee,
        max_hold_hours=_max_hold_hours(signal.confidence, signal.exchange, signal.direction, regime=getattr(signal, "macro_regime", "")),
        opened_at=datetime.now(timezone.utc),
    )
    # Store exchange on trade so portfolio sub-limits can count correctly
    trade.exchange = signal.exchange
    # Sprint 38: carry router verdict onto the trade for persistence/analysis
    trade.router_action = getattr(signal, "router_action", "")
    trade.router_edge = getattr(signal, "router_edge", 0.0)
    return trade


def close_trade(trade: PaperTrade, exit_price: float) -> float:
    """
    Compute net PnL for closing a trade at exit_price.

    Mutates trade in place (sets exit_price, closed_at, fee, pnl).
    Returns net PnL (positive = win).
    """
    # trade.fee here = entry-leg fee: broker's real fill fee (live) or notional*FEE_RATE
    # (paper). ponytail: assumes close_trade runs once per trade — if partial/repeat
    # closes are added, snapshot the entry fee at open instead of reading trade.fee.
    entry_fee = trade.fee
    exit_fee = trade.size * exit_price * FEE_RATE

    if trade.direction == "long":
        gross_pnl = (exit_price - trade.entry_price) * trade.size
    else:
        gross_pnl = (trade.entry_price - exit_price) * trade.size

    net_pnl = gross_pnl - entry_fee - exit_fee

    trade.exit_price = exit_price
    trade.closed_at = datetime.now(timezone.utc)
    trade.fee += exit_fee
    trade.pnl = net_pnl

    return net_pnl
