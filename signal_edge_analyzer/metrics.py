"""Metrics engine — computes statistical quality metrics from signal outcomes.

All computations are pure Python with no external dependencies.

Formulas:
    win_rate    = correct_calls / total_evaluated
    avg_return  = mean(outcome_score_delta)
    avg_win     = mean(outcome_score_delta where direction_correct=True)
    avg_loss    = mean(outcome_score_delta where direction_correct=False)
    expectancy  = win_rate * avg_win + (1 - win_rate) * avg_loss
    max_drawdown= max peak-to-trough loss in cumulative-return time series
    volatility  = std_dev(outcome_score_delta)
    sharpe_score= avg_return / volatility  (0 if volatility == 0)
    quality_score = (expectancy * 0.5) + (win_rate * 0.3) + (sharpe_score * 0.2)
"""
from __future__ import annotations

import logging
import math
from typing import Optional

from models import EdgeMetrics, SignalOutcome

logger = logging.getLogger(__name__)

MIN_SAMPLES = 10    # below this we don't emit metrics


def compute_metrics(
    outcomes: list[SignalOutcome],
    engine_name: str,
    signal_type: str,
    ticker: Optional[str],
    direction: Optional[str],
    window_hours: int,
    period_days: int = 30,
) -> Optional[EdgeMetrics]:
    """
    Compute EdgeMetrics for a single strategy group.
    Returns None if sample_size < MIN_SAMPLES or all returns are None.
    """
    evaluated = [o for o in outcomes if o.direction_correct is not None]
    if len(evaluated) < MIN_SAMPLES:
        return None

    returns = [o.outcome_score_delta for o in evaluated if o.outcome_score_delta is not None]
    if not returns:
        return None

    n = len(evaluated)
    correct = [o for o in evaluated if o.direction_correct is True]
    incorrect = [o for o in evaluated if o.direction_correct is False]

    win_rate = len(correct) / n if n > 0 else 0.0

    avg_return   = _mean(returns)
    avg_win      = _mean([o.outcome_score_delta for o in correct if o.outcome_score_delta is not None]) or 0.0
    avg_loss     = _mean([o.outcome_score_delta for o in incorrect if o.outcome_score_delta is not None]) or 0.0
    expectancy   = win_rate * avg_win + (1.0 - win_rate) * avg_loss
    max_drawdown = _max_drawdown(returns)
    volatility   = _std_dev(returns)
    sharpe_score = avg_return / volatility if volatility > 1e-9 else 0.0

    # Normalise expectancy from percentage-point units to [-1, +1] before blending.
    # Raw expectancy lives in the 0.001–0.02 range (pnl_pct per trade); win_rate is
    # 0–1. Without normalisation expectancy contributed <1% of the blended score,
    # making quality_score a near-linear transformation of win_rate.
    norm_exp = max(-1.0, min(1.0, expectancy / 0.10))
    quality_score = (norm_exp * 0.4) + (win_rate * 0.4) + (sharpe_score * 0.2)

    return EdgeMetrics(
        engine_name=engine_name,
        signal_type=signal_type,
        ticker=ticker,
        direction=direction,
        window_hours=window_hours,
        period_days=period_days,
        sample_size=n,
        win_rate=round(win_rate, 6),
        avg_return=round(avg_return, 6),
        avg_win=round(avg_win, 6),
        avg_loss=round(avg_loss, 6),
        expectancy=round(expectancy, 6),
        max_drawdown=round(max_drawdown, 6),
        volatility=round(volatility, 6),
        sharpe_score=round(sharpe_score, 6),
        quality_score=round(quality_score, 6),
    )


# ── Statistical helpers ────────────────────────────────────────────────────────

def _mean(values: list[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def _std_dev(values: list[float]) -> float:
    if len(values) < 2:
        return 0.0
    m = _mean(values)
    variance = sum((v - m) ** 2 for v in values) / (len(values) - 1)
    return math.sqrt(variance)


def _max_drawdown(returns: list[float]) -> float:
    """
    Max peak-to-trough loss in the cumulative return series.
    Returns a positive number representing the magnitude of the worst drawdown.
    Returns 0.0 if the series never draws down.
    """
    if not returns:
        return 0.0

    cumulative = []
    running = 0.0
    for r in returns:
        running += r
        cumulative.append(running)

    peak = cumulative[0]
    max_dd = 0.0
    for val in cumulative:
        if val > peak:
            peak = val
        drawdown = peak - val
        if drawdown > max_dd:
            max_dd = drawdown
    return max_dd
