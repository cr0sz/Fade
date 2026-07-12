"""Shared data models for the paper trading engine."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional


@dataclass
class Signal:
    signal_id: str
    asset: str
    direction: str          # "long" | "short"
    confidence: float       # 0.0–1.0
    timestamp: str
    source: str
    composite_score: float  # raw 0–100 score
    exchange: str = ""      # CRYPTO | BIST | NASDAQ | NYSE | CBOE | ""
    real_price: float = 0.0  # live market price from ticker_quotes (0 = unavailable)
    volume_cap_usd: float = 0.0  # max notional based on liquidity (0 = uncapped)
    # Component scores extracted from signal_data — used for conviction checks
    news_score: float = 0.0
    social_score: float = 0.0
    volume_score: float = 0.0
    # Meta-engines (InstitutionalConfluenceEngine etc.) signal conviction via
    # confidence_weighted rather than individual component scores
    has_meta_conviction: bool = False
    # Volatility-scaling factor applied in executor (1.0 = no adjustment)
    vol_scale: float = 1.0
    # Technical score from signal_data (avoids extra DB round-trip in _process_signal)
    tech_score: float = 0.0
    # Sprint 38 — Truth Router verdict: confirm | invert | passthrough ("" = pre-router)
    router_action: str = ""
    router_edge: float = 0.0
    macro_regime: str = ""
    # Set by equity-based sizing path (EDGE_SCALE_ENABLED=true) to override fraction sizing
    target_notional: Optional[float] = None


@dataclass
class PaperTrade:
    trade_id: str
    signal_id: str
    asset: str
    direction: str          # "long" | "short"
    size: float             # units purchased
    entry_price: float      # actual market price at entry (slippage-adjusted)
    notional: float         # cash allocated = size * entry_price
    fee: float              # cumulative fees paid
    exchange: str = ""      # CRYPTO | BIST | NASDAQ | NYSE | ""
    exit_price: Optional[float] = None
    pnl: Optional[float] = None
    close_reason: Optional[str] = None   # set by engine at close time for Prometheus labelling
    max_hold_hours: int = 8  # auto-close threshold, scaled by confidence at entry
    opened_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    closed_at: Optional[datetime] = None
    # Sprint 38 — copied from the originating Signal for persistence/analysis
    router_action: str = ""
    router_edge: float = 0.0
    # Sprint 41 — True only when a real broker order was filled at open.
    # Close paths key off this, NOT the global TRADING_MODE: in live-pilot
    # mode most positions are paper-tracked, and a real close for a position
    # that was never really opened would sell assets we don't hold.
    is_live: bool = False
    # Sprint 57 — exchange-side resting STOP_MARKET order ID (live futures only).
    # Cancelled before any software-side close to prevent double-stop fills.
    stop_order_id: Optional[str] = None
