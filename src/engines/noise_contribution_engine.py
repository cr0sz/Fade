"""
Noise Contribution Engine — Phase 4

Quantifies false-positive contribution per engine, detects amplification chains
(correlated FPR between engine pairs), and identifies reliability degradation
hotspots. Read-only; computes on-demand from replay_metrics.

Amplification chain detection: Pearson correlation of weekly FPR time series.
Pairs with r > 0.70 and both FPR > 0.30 are flagged as amplification partners.
(See DEC-035 for rationale on correlation-based approximation.)
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import datetime, timezone
from statistics import mean, stdev
from typing import Any, Dict, List, Optional, Tuple

import asyncpg

from src.core.logging import get_logger

logger = get_logger(__name__)

_AMPLIFICATION_CORRELATION_THRESHOLD = 0.70
_AMPLIFICATION_FPR_FLOOR             = 0.30
_HOTSPOT_STDEV_MULTIPLIER            = 1.0   # mean + 1σ = hotspot boundary
_MIN_WEEKLY_POINTS                   = 4     # minimum weeks for correlation


@dataclass
class NoiseContribution:
    engine_name: str
    total_fp: int
    fp_contribution_pct: float      # fraction of all FP across engines (0–1)
    noise_score: float              # 0–100 relative noise metric
    avg_fpr: float                  # average false positive rate
    is_hotspot: bool
    amplification_partners: List[str]


@dataclass
class AmplificationChain:
    engine_a: str
    engine_b: str
    correlation: float
    avg_fpr_a: float
    avg_fpr_b: float
    joint_fp_weeks: int             # weeks where both had FPR > threshold


@dataclass
class NoiseReport:
    total_fp_all_engines: int
    period_days: int
    contributions: List[NoiseContribution]
    amplification_chains: List[AmplificationChain]
    hotspot_engines: List[str]
    system_avg_fpr: float
    hotspot_threshold_fpr: float
    computed_at: datetime


class NoiseContributionEngine:
    """
    Reads replay_metrics to compute noise contribution per engine.
    All methods are read-only. compute_report() is the main entry point.
    """

    def __init__(self, db: asyncpg.Pool, window_hours: int = 24):
        self._db = db
        self._window = window_hours

    # ── Main Report ────────────────────────────────────────────────────────────

    async def compute_report(self, period_days: int = 30) -> NoiseReport:
        fp_data      = await self._fp_by_engine(period_days)
        weekly_fpr   = await self._weekly_fpr(period_days)
        chains       = _detect_amplification(weekly_fpr)
        contributions = _build_contributions(fp_data, chains)

        # Hotspot detection: FPR > mean + stdev
        fprs = [c.avg_fpr for c in contributions if c.avg_fpr is not None]
        if len(fprs) >= 2:
            sys_mean = mean(fprs)
            sys_sd   = stdev(fprs)
            threshold = sys_mean + _HOTSPOT_STDEV_MULTIPLIER * sys_sd
        elif fprs:
            sys_mean  = fprs[0]
            sys_sd    = 0.0
            threshold = sys_mean
        else:
            sys_mean  = 0.0
            sys_sd    = 0.0
            threshold = 0.0

        for c in contributions:
            c.is_hotspot = (c.avg_fpr > threshold and c.avg_fpr > _AMPLIFICATION_FPR_FLOOR)

        hotspots = [c.engine_name for c in contributions if c.is_hotspot]
        total_fp = sum(c.total_fp for c in contributions)

        return NoiseReport(
            total_fp_all_engines=total_fp,
            period_days=period_days,
            contributions=sorted(contributions, key=lambda x: x.noise_score, reverse=True),
            amplification_chains=chains,
            hotspot_engines=hotspots,
            system_avg_fpr=round(sys_mean, 4),
            hotspot_threshold_fpr=round(threshold, 4),
            computed_at=datetime.now(timezone.utc),
        )

    # ── DB Queries ─────────────────────────────────────────────────────────────

    async def _fp_by_engine(self, period_days: int) -> List[Any]:
        async with self._db.acquire() as conn:
            return await conn.fetch("""
                SELECT
                    engine_name,
                    SUM(false_positives)::int              AS total_fp,
                    SUM(true_positives + false_positives
                        + true_negatives + false_negatives)::int AS total_signals,
                    AVG(false_positive_rate)               AS avg_fpr
                FROM replay_metrics
                WHERE window_hours = $1
                  AND computed_at >= NOW() - ($2 || ' days')::INTERVAL
                GROUP BY engine_name
            """, self._window, str(period_days))

    async def _weekly_fpr(self, period_days: int) -> Dict[str, List[float]]:
        async with self._db.acquire() as conn:
            rows = await conn.fetch("""
                SELECT
                    engine_name,
                    date_trunc('week', computed_at)  AS wk,
                    AVG(false_positive_rate)          AS fpr
                FROM replay_metrics
                WHERE window_hours = $1
                  AND computed_at >= NOW() - ($2 || ' days')::INTERVAL
                  AND false_positive_rate IS NOT NULL
                GROUP BY engine_name, wk
                ORDER BY engine_name, wk
            """, self._window, str(period_days))

        weekly: Dict[str, List[float]] = {}
        for r in rows:
            weekly.setdefault(r["engine_name"], []).append(float(r["fpr"]))
        return weekly


# ─── Computation Helpers ──────────────────────────────────────────────────────

def _build_contributions(
    fp_data: List[Any],
    chains: List[AmplificationChain],
) -> List[NoiseContribution]:
    total = sum(int(r["total_fp"] or 0) for r in fp_data)
    partner_map: Dict[str, List[str]] = {}
    for c in chains:
        partner_map.setdefault(c.engine_a, []).append(c.engine_b)
        partner_map.setdefault(c.engine_b, []).append(c.engine_a)

    n = max(1, len(fp_data))
    result: List[NoiseContribution] = []
    for r in fp_data:
        fp = int(r["total_fp"] or 0)
        pct = fp / total if total > 0 else 0.0
        # Normalize noise score: fraction × number of engines × 100 → relative
        noise = min(100.0, pct * n * 100.0)
        result.append(NoiseContribution(
            engine_name=r["engine_name"],
            total_fp=fp,
            fp_contribution_pct=round(pct, 4),
            noise_score=round(noise, 2),
            avg_fpr=round(float(r["avg_fpr"] or 0), 4),
            is_hotspot=False,           # set by caller after computing threshold
            amplification_partners=partner_map.get(r["engine_name"], []),
        ))
    return result


def _pearson_correlation(xs: List[float], ys: List[float]) -> Optional[float]:
    """Pearson r for two equal-length lists. Returns None if undefined."""
    n = len(xs)
    if n < _MIN_WEEKLY_POINTS or len(ys) != n:
        return None
    mx, my = mean(xs), mean(ys)
    num = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    dx  = math.sqrt(sum((x - mx) ** 2 for x in xs))
    dy  = math.sqrt(sum((y - my) ** 2 for y in ys))
    if dx == 0 or dy == 0:
        return None
    return num / (dx * dy)


def _detect_amplification(
    weekly_fpr: Dict[str, List[float]],
) -> List[AmplificationChain]:
    """Detect pairs of engines with correlated high FPR patterns."""
    engines = list(weekly_fpr.keys())
    chains: List[AmplificationChain] = []

    for i in range(len(engines)):
        for j in range(i + 1, len(engines)):
            a, b = engines[i], engines[j]
            a_fpr = weekly_fpr[a]
            b_fpr = weekly_fpr[b]

            # Align to same length (take overlap at end)
            min_len = min(len(a_fpr), len(b_fpr))
            if min_len < _MIN_WEEKLY_POINTS:
                continue
            a_aligned = a_fpr[-min_len:]
            b_aligned = b_fpr[-min_len:]

            r = _pearson_correlation(a_aligned, b_aligned)
            if r is None:
                continue

            avg_a = mean(a_aligned)
            avg_b = mean(b_aligned)

            # Amplification: high correlation AND both have elevated FPR
            if r >= _AMPLIFICATION_CORRELATION_THRESHOLD and \
               avg_a >= _AMPLIFICATION_FPR_FLOOR and \
               avg_b >= _AMPLIFICATION_FPR_FLOOR:

                joint_weeks = sum(
                    1 for x, y in zip(a_aligned, b_aligned)
                    if x >= _AMPLIFICATION_FPR_FLOOR and y >= _AMPLIFICATION_FPR_FLOOR
                )
                chains.append(AmplificationChain(
                    engine_a=a,
                    engine_b=b,
                    correlation=round(r, 4),
                    avg_fpr_a=round(avg_a, 4),
                    avg_fpr_b=round(avg_b, 4),
                    joint_fp_weeks=joint_weeks,
                ))

    return sorted(chains, key=lambda c: c.correlation, reverse=True)


def _report_to_dict(report: NoiseReport) -> Dict[str, Any]:
    """Convert NoiseReport to a JSON-serialisable dict."""
    return {
        "total_fp_all_engines": report.total_fp_all_engines,
        "period_days": report.period_days,
        "system_avg_fpr": report.system_avg_fpr,
        "hotspot_threshold_fpr": report.hotspot_threshold_fpr,
        "hotspot_engines": report.hotspot_engines,
        "computed_at": report.computed_at.isoformat(),
        "contributions": [
            {
                "engine_name": c.engine_name,
                "total_fp": c.total_fp,
                "fp_contribution_pct": c.fp_contribution_pct,
                "noise_score": c.noise_score,
                "avg_fpr": c.avg_fpr,
                "is_hotspot": c.is_hotspot,
                "amplification_partners": c.amplification_partners,
            }
            for c in report.contributions
        ],
        "amplification_chains": [
            {
                "engine_a": c.engine_a,
                "engine_b": c.engine_b,
                "correlation": c.correlation,
                "avg_fpr_a": c.avg_fpr_a,
                "avg_fpr_b": c.avg_fpr_b,
                "joint_fp_weeks": c.joint_fp_weeks,
            }
            for c in report.amplification_chains
        ],
    }
