"""Shared mathematical utilities for intelligence engines."""
from __future__ import annotations

from statistics import mean
from typing import List, Tuple

_DECAY_WEEKS = 12  # weeks of history for OLS slope


def ols_slope(values: List[float], max_n: int = _DECAY_WEEKS) -> Tuple[float, bool]:
    """OLS slope over last max_n values.  Returns (slope_per_week, is_decaying)."""
    vals = values[-max_n:]
    n = len(vals)
    if n < 2:
        return 0.0, False
    x_mean = (n - 1) / 2
    y_mean = mean(vals)
    num = sum((i - x_mean) * (v - y_mean) for i, v in enumerate(vals))
    den = sum((i - x_mean) ** 2 for i in range(n))
    slope = num / den if den != 0 else 0.0
    return slope, slope < -0.02
