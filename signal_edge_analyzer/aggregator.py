"""Aggregator — ranks strategies and persists results to PostgreSQL.

Groups outcomes by (engine × ticker × direction × window_hours), computes
metrics for each group, ranks by quality_score, and writes results to
signal_edge_metrics and signal_edge_aggregations tables.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

import asyncpg
import structlog

from collector import collect_outcomes
from metrics import compute_metrics
from models import EdgeMetrics, StrategyRank
from outcomes import build_outcomes, filter_evaluated, group_by_strategy

logger = structlog.get_logger(__name__)

_UPSERT_METRIC_SQL = """
    INSERT INTO signal_edge_metrics (
        engine_name, signal_type, ticker, direction, window_hours,
        period_days, sample_size,
        win_rate, avg_return, avg_win, avg_loss,
        expectancy, max_drawdown, volatility, sharpe_score, quality_score,
        computed_at
    ) VALUES (
        $1, $2, $3, $4, $5,
        $6, $7,
        $8, $9, $10, $11,
        $12, $13, $14, $15, $16,
        $17
    )
    ON CONFLICT (engine_name, signal_type,
                 COALESCE(ticker, ''), COALESCE(direction, ''),
                 window_hours, (_utc_date(computed_at)))
    DO UPDATE SET
        period_days    = EXCLUDED.period_days,
        sample_size    = EXCLUDED.sample_size,
        win_rate       = EXCLUDED.win_rate,
        avg_return     = EXCLUDED.avg_return,
        avg_win        = EXCLUDED.avg_win,
        avg_loss       = EXCLUDED.avg_loss,
        expectancy     = EXCLUDED.expectancy,
        max_drawdown   = EXCLUDED.max_drawdown,
        volatility     = EXCLUDED.volatility,
        sharpe_score   = EXCLUDED.sharpe_score,
        quality_score  = EXCLUDED.quality_score,
        computed_at    = EXCLUDED.computed_at
"""

_INSERT_AGG_SQL = """
    INSERT INTO signal_edge_aggregations (
        rank_position, group_key, engine_name, ticker, direction,
        window_hours, period_days, sample_size,
        quality_score, win_rate, avg_return, expectancy, sharpe_score,
        computed_at
    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
    ON CONFLICT (group_key, (_utc_date(computed_at))) DO UPDATE SET
        rank_position = EXCLUDED.rank_position,
        period_days   = EXCLUDED.period_days,
        sample_size   = EXCLUDED.sample_size,
        quality_score = EXCLUDED.quality_score,
        win_rate      = EXCLUDED.win_rate,
        avg_return    = EXCLUDED.avg_return,
        expectancy    = EXCLUDED.expectancy,
        sharpe_score  = EXCLUDED.sharpe_score,
        computed_at   = EXCLUDED.computed_at
"""


async def run_metrics_cycle(
    pool: asyncpg.Pool,
    period_days: int = 30,
) -> list[EdgeMetrics]:
    """
    Full metrics computation cycle:
    1. Collect all evaluated outcomes for the period
    2. Group by strategy
    3. Compute EdgeMetrics for each group
    4. Upsert to signal_edge_metrics
    Returns list of computed metrics.
    """
    logger.info("metrics_cycle_start", period_days=period_days)
    rows = await collect_outcomes(pool, period_days=period_days)
    outcomes = filter_evaluated(build_outcomes(rows))

    if not outcomes:
        logger.info("metrics_cycle_no_data")
        return []

    groups = group_by_strategy(outcomes)
    all_metrics: list[EdgeMetrics] = []
    now = datetime.now(tz=timezone.utc)

    for (engine, sig_type, ticker, direction, window_h), group_outcomes in groups.items():
        m = compute_metrics(
            outcomes=group_outcomes,
            engine_name=engine,
            signal_type=sig_type,
            ticker=ticker,
            direction=direction,
            window_hours=window_h,
            period_days=period_days,
        )
        if m is not None:
            all_metrics.append(m)

    if all_metrics:
        await _upsert_metrics(pool, all_metrics, now)
        logger.info("metrics_cycle_complete", computed=len(all_metrics), period_days=period_days)
    else:
        logger.info("metrics_cycle_no_metrics: all groups below MIN_SAMPLES threshold")

    return all_metrics


async def run_aggregation_cycle(
    pool: asyncpg.Pool,
    top_n: int = 20,
    period_days: int = 30,
) -> list[StrategyRank]:
    """
    Aggregation cycle:
    1. Run metrics cycle
    2. Rank all strategies by quality_score
    3. Persist rankings to signal_edge_aggregations
    Returns top_n ranked strategies.
    """
    metrics = await run_metrics_cycle(pool, period_days=period_days)
    if not metrics:
        return []

    ranked = _rank_strategies(metrics, top_n=top_n)
    await _save_aggregations(pool, ranked)

    logger.info("aggregation_cycle_complete", strategies_ranked=len(ranked))
    return ranked


def _rank_strategies(
    metrics: list[EdgeMetrics],
    top_n: int = 20,
) -> list[StrategyRank]:
    """Sort metrics by quality_score descending, return top_n."""
    sorted_metrics = sorted(metrics, key=lambda m: m.quality_score, reverse=True)

    ranked: list[StrategyRank] = []
    for i, m in enumerate(sorted_metrics[:top_n], 1):
        ticker_part = m.ticker or "all"
        direction_part = m.direction or "mixed"
        group_key = f"{m.engine_name}|{ticker_part}|{direction_part}|{m.window_hours}h"
        ranked.append(StrategyRank(
            rank=i,
            group_key=group_key,
            engine_name=m.engine_name,
            ticker=m.ticker,
            direction=m.direction,
            window_hours=m.window_hours,
            period_days=m.period_days,
            sample_size=m.sample_size,
            quality_score=m.quality_score,
            win_rate=m.win_rate,
            avg_return=m.avg_return,
            expectancy=m.expectancy,
            sharpe_score=m.sharpe_score,
        ))
    return ranked


async def _upsert_metrics(
    pool: asyncpg.Pool,
    metrics: list[EdgeMetrics],
    now: datetime,
) -> None:
    rows = [
        (
            m.engine_name, m.signal_type, m.ticker, m.direction, m.window_hours,
            m.period_days, m.sample_size,
            m.win_rate, m.avg_return, m.avg_win, m.avg_loss,
            m.expectancy, m.max_drawdown, m.volatility, m.sharpe_score, m.quality_score,
            now,
        )
        for m in metrics
    ]
    async with pool.acquire() as conn:
        await conn.executemany(_UPSERT_METRIC_SQL, rows)
    logger.debug("upserted %d metric rows", len(rows))


async def _save_aggregations(
    pool: asyncpg.Pool,
    rankings: list[StrategyRank],
) -> None:
    if not rankings:
        return
    now = datetime.now(tz=timezone.utc)
    rows = [
        (
            r.rank, r.group_key, r.engine_name, r.ticker, r.direction,
            r.window_hours, r.period_days, r.sample_size,
            r.quality_score, r.win_rate, r.avg_return, r.expectancy, r.sharpe_score,
            now,
        )
        for r in rankings
    ]
    async with pool.acquire() as conn:
        await conn.executemany(_INSERT_AGG_SQL, rows)
    logger.debug("saved %d aggregation rows", len(rows))
