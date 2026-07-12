"""
Confidence Engine — Level 2 overlay.

Computes a confidence score (0.0–1.0) and level (low/medium/high) for each
ticker's trend signal. This runs AFTER the existing trend computation and
does NOT modify composite_score or any existing trend field.

Confidence dimensions:
  1. cross_source_agreement   — news, social, smart_money all pointing same direction?
  2. data_consistency         — low variance across multiple score windows?
  3. velocity_stability       — is the signal building steadily vs. spiking once?
  4. provider_reliability     — are the data sources healthy (circuit breakers)?
  5. sample_size_adequacy     — do we have enough data points to be confident?

Formula:
  raw = weighted average of 5 dimensions (see WEIGHTS below)
  confidence_score = sigmoid-like normalization of raw → [0.0, 1.0]
  confidence_level: < 0.35 → low, < 0.65 → medium, ≥ 0.65 → high
"""
from __future__ import annotations

import math
from dataclasses import dataclass

from src.core.logging import get_logger

logger = get_logger(__name__)

CONFIDENCE_WEIGHTS = {
    "cross_source_agreement": 0.30,
    "data_consistency": 0.25,
    "velocity_stability": 0.20,
    "provider_reliability": 0.15,
    "sample_size": 0.10,
}

LEVEL_THRESHOLDS = {
    "low": 0.35,
    "medium": 0.65,
}


@dataclass
class ConfidenceResult:
    score: float                    # 0.0 – 1.0
    level: str                      # low | medium | high
    cross_source_agreement: float
    data_consistency: float
    velocity_stability: float
    provider_reliability: float
    sample_size_score: float

    def to_dict(self) -> dict:
        return {
            "score": round(self.score, 3),
            "level": self.level,
            "dimensions": {
                "cross_source_agreement": round(self.cross_source_agreement, 3),
                "data_consistency": round(self.data_consistency, 3),
                "velocity_stability": round(self.velocity_stability, 3),
                "provider_reliability": round(self.provider_reliability, 3),
                "sample_size": round(self.sample_size_score, 3),
            },
        }


class ConfidenceEngine:
    """
    Stateless confidence scorer — all inputs must be passed explicitly.
    No database queries; designed to be called from TrendDetectionEngine
    after trend scores are computed.
    """

    # ── Dimension 1: Cross-source agreement ───────────────────────────────────

    def _cross_source_agreement(
        self,
        news_score: float,
        social_score: float,
        smart_money_score: float,
    ) -> float:
        """
        Returns 1.0 if all available sources agree on direction,
        scales down with disagreement.
        Scores that are 0.0 are treated as absent (not as neutral disagreement).
        """
        available = [
            s for s in [news_score, social_score, smart_money_score]
            if abs(s) > 0.01  # treat near-zero as "no signal"
        ]
        if len(available) < 2:
            return 0.5  # only one source — moderate confidence (CRYPTO often has no news/SM)

        positive = sum(1 for s in available if s > 0)
        negative = sum(1 for s in available if s < 0)
        n = len(available)

        majority = max(positive, negative)
        agreement_ratio = majority / n  # 1.0 = all agree, 0.5 = split

        # Unanimous = 1.0, 2/3 = 0.75, split = 0.2
        if agreement_ratio == 1.0:
            return 1.0
        elif agreement_ratio >= 0.67:
            return 0.75
        else:
            return 0.2

    # ── Dimension 2: Data consistency ─────────────────────────────────────────

    def _data_consistency(
        self,
        composite_score: float,
        smoothed_composite: float | None,
    ) -> float:
        """
        Measures agreement between instantaneous composite and smoothed composite.
        High agreement = high consistency = high confidence.
        """
        if smoothed_composite is None:
            return 0.5  # no history available — neutral

        diff = abs(composite_score - smoothed_composite)
        # Cap at 0.7: EMA converging to composite is baseline behaviour, not a
        # quality signal. diff=0 → 0.7, diff=0.7 → 0.0, diff>0.7 → 0.0
        return max(0.0, 0.7 - diff)

    # ── Dimension 3: Velocity stability ───────────────────────────────────────

    def _velocity_stability(self, velocity_ratio: float) -> float:
        """
        Extreme velocity (very high or very low) suggests unstable signal.
        Moderate velocity = higher confidence.

        velocity_ratio: current mentions / 7d avg
          0.5 – 2.0 = stable zone → high confidence
          2.0 – 5.0 = elevated = some confidence
          > 5.0 = spike = potentially transient = lower confidence
          < 0.3 = very quiet = no signal = low confidence
        """
        if velocity_ratio < 0.3:
            return 0.2  # almost no activity
        elif velocity_ratio <= 2.0:
            # Normal activity: baseline score, not a quality differentiator.
            # Was 1.0 — that inflated confidence for every "quiet" ticker.
            return 0.65
        elif velocity_ratio <= 5.0:
            # linear decay from 0.65 at 2.0 to 0.30 at 5.0
            return 0.65 - 0.35 * (velocity_ratio - 2.0) / 3.0
        else:
            # extreme spike — potentially one big viral post
            return max(0.1, 0.30 - 0.1 * math.log(velocity_ratio / 5.0))

    # ── Dimension 4: Provider reliability ─────────────────────────────────────

    def _provider_reliability(self, circuit_statuses: list[dict]) -> float:
        """
        1.0 if all circuits CLOSED, lower if any are OPEN or HALF_OPEN.
        """
        if not circuit_statuses:
            return 0.5  # unknown state — neutral, not optimistic

        closed = sum(1 for s in circuit_statuses if s.get("state") == "closed")
        total = len(circuit_statuses)
        ratio = closed / total

        # Smooth: all closed = 1.0, half open/failing = 0.5, all open = 0.2
        return max(0.2, ratio)

    # ── Dimension 5: Sample size adequacy ─────────────────────────────────────

    def _sample_size_score(
        self,
        news_count: int,
        social_mention_count: int,
    ) -> float:
        """
        More data points = higher confidence. Diminishing returns after thresholds.
        """
        news_score = min(1.0, news_count / 5.0)          # 5 articles = full news confidence
        social_score = min(1.0, social_mention_count / 20.0)  # 20 mentions = full social confidence
        return (news_score * 0.5 + social_score * 0.5)

    # ── Combined scoring ──────────────────────────────────────────────────────

    def compute(
        self,
        news_score: float,
        social_score: float,
        smart_money_score: float,
        composite_score: float,
        smoothed_composite: float | None = None,
        velocity_ratio: float = 1.0,
        circuit_statuses: list[dict] | None = None,
        news_count: int = 0,
        social_mention_count: int = 0,
        technical_score: float = 0.0,
    ) -> ConfidenceResult:
        d1 = self._cross_source_agreement(news_score, social_score, smart_money_score)
        d2 = self._data_consistency(composite_score, smoothed_composite)
        d3 = self._velocity_stability(velocity_ratio)
        d4 = self._provider_reliability(circuit_statuses or [])
        d5 = self._sample_size_score(news_count, social_mention_count)

        raw = (
            CONFIDENCE_WEIGHTS["cross_source_agreement"] * d1
            + CONFIDENCE_WEIGHTS["data_consistency"] * d2
            + CONFIDENCE_WEIGHTS["velocity_stability"] * d3
            + CONFIDENCE_WEIGHTS["provider_reliability"] * d4
            + CONFIDENCE_WEIGHTS["sample_size"] * d5
        )

        # Reward signals confirmed by 2+ independent sources (news + social, etc.)
        active_sources = sum(
            1 for s in [news_score, social_score, smart_money_score] if abs(s) > 0.01
        )
        diversity_bonus = 0.05 if active_sources >= 2 else 0.0

        # Technical strength bonus — RSI/momentum/Bollinger have actual predictive
        # value. Strong technical signal deserves higher confidence score.
        abs_tech = abs(technical_score)
        if abs_tech >= 0.30:
            tech_bonus = 0.07
        elif abs_tech >= 0.15:
            tech_bonus = 0.04
        else:
            tech_bonus = 0.0

        score = min(1.0, max(0.0, raw + diversity_bonus + tech_bonus))
        if score < LEVEL_THRESHOLDS["low"]:
            level = "low"
        elif score < LEVEL_THRESHOLDS["medium"]:
            level = "medium"
        else:
            level = "high"

        logger.debug(
            "confidence_computed",
            agreement=round(d1, 3),
            consistency=round(d2, 3),
            velocity=round(d3, 3),
            reliability=round(d4, 3),
            sample=round(d5, 3),
            score=round(score, 3),
            level=level,
        )

        return ConfidenceResult(
            score=score,
            level=level,
            cross_source_agreement=d1,
            data_consistency=d2,
            velocity_stability=d3,
            provider_reliability=d4,
            sample_size_score=d5,
        )
