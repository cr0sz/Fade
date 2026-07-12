"""Pydantic models for Level 4 signal validation and backtesting."""
from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel


class SignalRecord(BaseModel):
    id: int
    signal_type: str
    engine_name: str
    ticker: str | None = None
    direction: str | None = None
    composite_score: float | None = None
    confidence_level: str | None = None
    signal_data: dict[str, Any] = {}
    source_table: str
    source_id: int | None = None
    emitted_at: datetime
    archived_at: datetime


class SignalOutcome(BaseModel):
    id: int
    signal_id: int
    ticker: str | None = None
    evaluation_window_h: int
    predicted_direction: str | None = None
    actual_direction: str | None = None
    direction_correct: bool | None = None
    predicted_score: float | None = None
    outcome_score_delta: float | None = None
    evaluated_at: datetime


class BacktestRun(BaseModel):
    id: int
    run_id: UUID
    engine_name: str | None = None
    window_days: int
    started_at: datetime
    completed_at: datetime | None = None
    status: str
    total_signals: int
    evaluated_signals: int
    accuracy_4h: float | None = None
    accuracy_24h: float | None = None
    accuracy_72h: float | None = None
    parameters: dict[str, Any] = {}


class EnginePerformance(BaseModel):
    engine_name: str
    metric_date: str
    total_signals: int
    accuracy_4h: float | None = None
    accuracy_24h: float | None = None
    accuracy_72h: float | None = None
    avg_confidence: float | None = None
    high_conf_accuracy: float | None = None
    signal_count_by_type: dict[str, int] = {}


class AccuracySummary(BaseModel):
    engine_name: str
    accuracy_4h: float | None = None
    accuracy_24h: float | None = None
    accuracy_72h: float | None = None
    total_evaluated: int
    high_conf_accuracy: float | None = None
    period_days: int
