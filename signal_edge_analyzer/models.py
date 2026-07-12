"""Data models for the Signal Edge Analyzer."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional


@dataclass
class SignalOutcome:
    """One evaluated signal — raw join of signals_registry + signal_outcomes."""
    signal_id: int
    engine_name: str
    signal_type: str
    ticker: Optional[str]
    direction: Optional[str]
    composite_score: Optional[float]
    confidence_level: Optional[str]
    emitted_at: datetime
    window_hours: int
    outcome_score_delta: Optional[float]   # composite_score Δ — return proxy
    direction_correct: Optional[bool]
    evaluated_at: Optional[datetime]


@dataclass
class EdgeMetrics:
    """Computed quality metrics for a strategy group."""
    engine_name: str
    signal_type: str
    ticker: Optional[str]
    direction: Optional[str]
    window_hours: int
    period_days: int
    sample_size: int
    win_rate: float
    avg_return: float
    avg_win: float
    avg_loss: float
    expectancy: float
    max_drawdown: float
    volatility: float
    sharpe_score: float
    quality_score: float


@dataclass
class StrategyRank:
    """A ranked strategy from the aggregation step."""
    rank: int
    group_key: str
    engine_name: str
    ticker: Optional[str]
    direction: Optional[str]
    window_hours: int
    period_days: int
    sample_size: int
    quality_score: float
    win_rate: float
    avg_return: float
    expectancy: float
    sharpe_score: float


@dataclass
class SystemEvent:
    """Structured event for Telegram + JSON logging."""
    event_type: str
    timestamp: datetime = field(default_factory=lambda: datetime.now())
    signal_id: Optional[int] = None
    asset: Optional[str] = None
    engine: Optional[str] = None
    window_hours: Optional[int] = None
    return_value: Optional[float] = None
    expectancy: Optional[float] = None
    quality_score: Optional[float] = None
    message: Optional[str] = None
    error: Optional[str] = None

    def to_dict(self) -> dict:
        return {k: v for k, v in self.__dict__.items() if v is not None}
