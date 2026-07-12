from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from pydantic import BaseModel, Field


class TickerQuote(BaseModel):
    symbol: str
    name: str | None = None
    price: float
    change: float
    change_pct: float
    volume: float
    market_cap: float | None = None
    pe_ratio: float | None = None
    high_52w: float | None = None
    low_52w: float | None = None
    exchange: str = ""
    currency: str = "USD"
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class MarketTrend(BaseModel):
    ticker: str
    exchange: str
    news_score: float = 0.0
    social_score: float = 0.0
    smart_money_score: float = 0.0
    volume_score: float = 0.0
    technical_score: float = 0.0
    composite_score: float = 0.0
    hype_signal: bool = False
    panic_signal: bool = False
    trending_up: bool = False
    trending_down: bool = False
    computed_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    # ── Level 2 overlay fields (all optional — backward compatible) ────────────
    smoothed_composite: float | None = None     # time-decay smoothed score
    confidence_score: float | None = None       # 0.0–1.0
    confidence_level: str | None = None         # low | medium | high
    narrative_cluster: str | None = None        # ai_semiconductors | macro_liquidity | …
    narrative_persistence: float | None = None  # 0.0–1.0: cluster strength × momentum factor
    signal_quality: str | None = None           # low | medium | high


class NarrativeCluster(BaseModel):
    name: str                                   # canonical narrative ID
    label: str                                  # human-readable label
    strength_score: float = 0.0                 # 0.0–1.0
    momentum: str = "neutral"                   # accelerating | decelerating | neutral
    momentum_delta: float = 0.0                 # change vs previous cycle
    top_tickers: list[str] = Field(default_factory=list)
    contributing_sources: list[str] = Field(default_factory=list)
    sample_headlines: list[str] = Field(default_factory=list)
    computed_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class SmartMoneyEvent(BaseModel):
    id: str | None = None
    event_type: str           # insider_buy | insider_sell | etf_flow | whale_tx
    ticker: str | None = None
    actor: str | None = None  # person name, fund name, wallet address
    amount_usd: float | None = None
    shares: int | None = None
    direction: str | None = None  # buy | sell | in | out
    source: str = ""
    event_at: datetime
    raw: dict[str, Any] = Field(default_factory=dict)


class MacroIndicator(BaseModel):
    name: str
    value: float
    previous: float | None = None
    unit: str = ""
    country: str = ""
    release_date: datetime
    source: str = ""


# ── Level 3: Institutional Capital Flow Intelligence ──────────────────────────

class InstitutionalFlow(BaseModel):
    ticker: str                              # 'MARKET' for market-wide
    direction: str = "neutral"               # inflow | outflow | neutral
    flow_strength: float = 0.0              # 0–100
    persistence_score: float = 0.0          # 0–1
    acceleration: str = "stable"            # building | fading | stable
    insider_cluster: str = "neutral"        # bullish_cluster | neutral | bearish_distribution
    whale_buy_usd: float = 0.0
    whale_sell_usd: float = 0.0
    insider_buy_count: int = 0
    insider_sell_count: int = 0
    net_flow_ratio: float = 0.0             # -1 to 1
    computed_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class ETFRotation(BaseModel):
    sector: str
    etf_ticker: str
    composite_score: float = 0.0
    rotation_velocity: float = 0.0         # delta vs previous cycle
    rotation_direction: str = "neutral"    # inflow | outflow | neutral
    rank_position: int | None = None       # 1 = leading sector
    is_leading: bool = False
    is_lagging: bool = False
    computed_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class MacroRegime(BaseModel):
    regime: str                             # risk_on | risk_off | tightening | liquidity_expansion | volatility_regime
    confidence: float = 0.0               # 0–1
    previous_regime: str | None = None
    regime_shift: bool = False
    dominant_narrative: str | None = None
    narrative_score: float = 0.0
    proxy_score: float = 0.0
    keyword_score: float = 0.0
    regime_scores: dict[str, float] = Field(default_factory=dict)
    computed_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class ConfluenceSignal(BaseModel):
    ticker: str | None = None              # None = market-wide
    strength: float = 0.0                  # 0–100
    direction: str = "mixed"              # risk_on | risk_off | mixed
    narrative_alignment: float = 0.0
    confidence_weighted: float = 0.0
    flow_alignment: float = 0.0
    regime_coherence: float = 0.0
    contributing_sources: dict[str, Any] = Field(default_factory=dict)
    computed_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
