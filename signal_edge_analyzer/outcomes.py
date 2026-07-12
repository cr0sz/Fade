"""Outcomes — enriches raw DB rows into typed SignalOutcome objects."""
from __future__ import annotations

import logging
from typing import Any

from models import SignalOutcome

logger = logging.getLogger(__name__)


def build_outcomes(rows: list[dict[str, Any]]) -> list[SignalOutcome]:
    """Convert raw DB rows from collector into SignalOutcome instances."""
    outcomes: list[SignalOutcome] = []
    for row in rows:
        try:
            outcomes.append(SignalOutcome(
                signal_id=int(row["signal_id"]),
                engine_name=str(row["engine_name"]),
                signal_type=str(row["signal_type"]),
                ticker=row.get("ticker"),
                direction=row.get("direction"),
                composite_score=_float_or_none(row.get("composite_score")),
                confidence_level=row.get("confidence_level"),
                emitted_at=row["emitted_at"],
                window_hours=int(row["window_hours"]),
                outcome_score_delta=_float_or_none(row.get("outcome_score_delta")),
                direction_correct=row.get("direction_correct"),
                evaluated_at=row.get("evaluated_at"),
            ))
        except (KeyError, TypeError, ValueError) as exc:
            logger.warning("outcome_parse_error: %s | row=%s", exc, row)
    return outcomes


def group_by_strategy(
    outcomes: list[SignalOutcome],
) -> dict[tuple, list[SignalOutcome]]:
    """
    Group outcomes by (engine_name, signal_type, ticker, direction, window_hours).
    Returns dict keyed by that 5-tuple.
    """
    groups: dict[tuple, list[SignalOutcome]] = {}
    for o in outcomes:
        key = (o.engine_name, o.signal_type, o.ticker, o.direction, o.window_hours)
        groups.setdefault(key, []).append(o)
    return groups


def filter_evaluated(outcomes: list[SignalOutcome]) -> list[SignalOutcome]:
    """Keep only outcomes where direction_correct is known (evaluation complete)."""
    return [o for o in outcomes if o.direction_correct is not None]


def _float_or_none(value: object) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
