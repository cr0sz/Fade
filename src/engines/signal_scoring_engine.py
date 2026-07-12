"""
Signal Scoring Engine — Phase 1: Signal Truth Layer

Evaluates recorded signal outcomes to compute predictive quality metrics.

Reads from:
  signals_registry     — signal metadata (engine, type, confidence)
  signal_outcomes      — truth records written by OutcomeTrackingEngine

Writes to:
  signal_scores        — one row per engine / signal_type / window_hours per day

Scoring formulas:
  predictive_strength = max(0, (accuracy - 0.5) × 200)
    Maps directional accuracy to a 0–100 quality score.
    50% accuracy (coin-flip) → 0 · 75% → 50 · 100% → 100.

  half_life_hours = first window_h where accuracy < HALF_LIFE_THRESHOLD (0.55)
    Linear interpolation between consecutive windows for precision.
    None means accuracy stays above threshold past the 7d window.

  calibration_error = |mean_confidence_numeric − accuracy|
    Weighted average across confidence tiers (high=1, medium=0.5, low=0).
    A well-calibrated engine: high confidence → high accuracy.

Minimum sample size (MIN_SAMPLES = 20) is required before any score is written.
No existing engine table is ever modified.
"""
from __future__ import annotations

import asyncio
import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING

import asyncpg

from src.core import http_server as _http
from src.core.logging import configure_logging, get_logger
from src.core.signal_tracer import SignalTracer

if TYPE_CHECKING:
    import redis.asyncio as aioredis

logger = get_logger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────────

SCORE_WINDOWS_H: tuple[int, ...] = (1, 6, 24, 72, 168)   # must match OutcomeTrackingEngine

# Accuracy threshold for half-life: signal loses meaningful predictive edge here
HALF_LIFE_THRESHOLD: float = 0.55

# Minimum evaluated outcomes required before producing a score
MIN_SAMPLES: int = 20

DEFAULT_PERIOD_DAYS: int = 30

# Numeric value assigned to each confidence tier for calibration error
_CONF_NUMERIC: dict[str, float] = {"high": 1.0, "medium": 0.5, "low": 0.0}


# ── Data types ────────────────────────────────────────────────────────────────

@dataclass
class SignalScore:
    engine_name: str
    signal_type: str
    window_hours: int
    period_days: int
    sample_size: int
    accuracy: float | None               # 0.0–1.0 directional accuracy
    predictive_strength: float | None    # 0–100
    half_life_hours: float | None        # None = stays predictive past 7d
    calibration_error: float | None      # |mean_conf_numeric – accuracy|
    computed_at: datetime


@dataclass
class ScoringCycleResult:
    engines_scored: int = 0
    scores_written: int = 0
    errors: int = 0


# ── Engine ────────────────────────────────────────────────────────────────────

class SignalScoringEngine:
    """
    Reads signal_outcomes to score every active engine/signal_type combination.
    Writes daily snapshots of predictive strength and half-life to signal_scores.
    Never modifies signals_registry, signal_outcomes, or any engine table.
    """

    def __init__(
        self,
        db_pool: asyncpg.Pool,
        redis: "aioredis.Redis | None" = None,
        period_days: int = DEFAULT_PERIOD_DAYS,
        min_samples: int = MIN_SAMPLES,
    ) -> None:
        self._pool = db_pool
        self._redis = redis
        self._period_days = period_days
        self._min_samples = min_samples

    async def run_cycle(self, cycle_id: str | None = None) -> ScoringCycleResult:
        """Score all engine/signal_type pairs that have enough outcome data."""
        import time as _time
        result = ScoringCycleResult()
        tracer = SignalTracer(self._pool) if cycle_id else None

        async with self._pool.acquire() as conn:
            engine_types = await self._fetch_active_engine_types(conn)

        for engine_name, signal_type in engine_types:
            _t0 = _time.monotonic()
            try:
                async with self._pool.acquire() as conn:
                    half_life = await self._compute_half_life(conn, engine_name, signal_type)

                    for window_h in SCORE_WINDOWS_H:
                        score = await self._compute_score(
                            conn, engine_name, signal_type, window_h, half_life
                        )
                        if score is None:
                            continue
                        await self._persist_score(conn, score)
                        result.scores_written += 1

                result.engines_scored += 1

                if tracer:
                    await tracer.record(
                        cycle_id=cycle_id,
                        engine_name="signal_scoring",
                        signal_type="scoring",
                        signal_strength=None,
                        signal_direction=None,
                        output_data={
                            "scored_engine": engine_name,
                            "signal_type": signal_type,
                            "scores_written": result.scores_written,
                        },
                        latency_ms=round((_time.monotonic() - _t0) * 1000, 1),
                    )

            except Exception as exc:
                logger.error(
                    "scoring_error",
                    engine=engine_name,
                    signal_type=signal_type,
                    error=str(exc),
                )
                result.errors += 1

        logger.info(
            "scoring_cycle_complete",
            engines_scored=result.engines_scored,
            scores_written=result.scores_written,
            errors=result.errors,
        )
        return result

    # ── Internal computation ─────────────────────────────────────────────────

    async def _fetch_active_engine_types(
        self, conn: asyncpg.Connection
    ) -> list[tuple[str, str]]:
        """Return distinct (engine_name, signal_type) pairs with recent outcomes."""
        rows = await conn.fetch(
            """
            SELECT DISTINCT sr.engine_name, sr.signal_type
              FROM signal_outcomes so
              JOIN signals_registry sr ON sr.id = so.signal_id
             WHERE so.evaluated_at > now() - $1 * interval '1 day'
             ORDER BY sr.engine_name, sr.signal_type
            """,
            self._period_days,
        )
        return [(r["engine_name"], r["signal_type"]) for r in rows]

    async def _compute_score(
        self,
        conn: asyncpg.Connection,
        engine_name: str,
        signal_type: str,
        window_h: int,
        half_life: float | None,
    ) -> SignalScore | None:
        """
        Compute accuracy, predictive_strength, and calibration_error
        for one engine/signal_type/window combination.

        Returns None when sample_size < MIN_SAMPLES.
        """
        row = await conn.fetchrow(
            """
            SELECT
                COUNT(*) FILTER (WHERE so.direction_correct IS NOT NULL)
                    AS sample_size,
                COUNT(*) FILTER (WHERE so.direction_correct = true)::float /
                    NULLIF(COUNT(*) FILTER (WHERE so.direction_correct IS NOT NULL), 0)
                    AS accuracy
              FROM signal_outcomes so
              JOIN signals_registry sr ON sr.id = so.signal_id
             WHERE sr.engine_name = $1
               AND sr.signal_type = $2
               AND so.evaluation_window_h = $3
               AND so.evaluated_at > now() - $4 * interval '1 day'
            """,
            engine_name,
            signal_type,
            window_h,
            self._period_days,
        )

        if not row or (row["sample_size"] or 0) < self._min_samples:
            return None

        accuracy = float(row["accuracy"]) if row["accuracy"] is not None else None
        strength = self._predictive_strength(accuracy) if accuracy is not None else None
        calib = await self._calibration_error(conn, engine_name, window_h)

        return SignalScore(
            engine_name=engine_name,
            signal_type=signal_type,
            window_hours=window_h,
            period_days=self._period_days,
            sample_size=int(row["sample_size"]),
            accuracy=round(accuracy, 4) if accuracy is not None else None,
            predictive_strength=round(strength, 2) if strength is not None else None,
            half_life_hours=round(half_life, 1) if half_life is not None else None,
            calibration_error=round(calib, 4) if calib is not None else None,
            computed_at=datetime.now(timezone.utc),
        )

    def _predictive_strength(self, accuracy: float) -> float:
        """
        Rescale directional accuracy to a 0–100 predictive strength score.
          50% (coin-flip) → 0.0
          75%             → 50.0
          100%            → 100.0
        """
        return max(0.0, min(100.0, (accuracy - 0.5) * 200.0))

    async def _compute_half_life(
        self,
        conn: asyncpg.Connection,
        engine_name: str,
        signal_type: str,
    ) -> float | None:
        """
        Find the window_hours at which accuracy first drops below
        HALF_LIFE_THRESHOLD (0.55).

        Uses linear interpolation between consecutive evaluation windows
        for sub-window precision. Returns None when:
          - accuracy stays above threshold past the 168h window, or
          - insufficient samples at any window to establish the crossing.
        """
        data: list[tuple[int, float]] = []

        for wh in SCORE_WINDOWS_H:
            row = await conn.fetchrow(
                """
                SELECT
                    COUNT(*) FILTER (WHERE so.direction_correct IS NOT NULL) AS n,
                    COUNT(*) FILTER (WHERE so.direction_correct = true)::float /
                        NULLIF(COUNT(*) FILTER (WHERE so.direction_correct IS NOT NULL), 0)
                        AS accuracy
                  FROM signal_outcomes so
                  JOIN signals_registry sr ON sr.id = so.signal_id
                 WHERE sr.engine_name = $1
                   AND sr.signal_type = $2
                   AND so.evaluation_window_h = $3
                   AND so.evaluated_at > now() - $4 * interval '1 day'
                """,
                engine_name,
                signal_type,
                wh,
                self._period_days,
            )
            if (
                row
                and (row["n"] or 0) >= self._min_samples
                and row["accuracy"] is not None
            ):
                data.append((wh, float(row["accuracy"])))

        if len(data) < 2:
            return None

        # Scan for first crossing below threshold
        for i in range(len(data) - 1):
            wh_a, acc_a = data[i]
            wh_b, acc_b = data[i + 1]

            if acc_a < HALF_LIFE_THRESHOLD:
                # Already below threshold at the shortest measured window
                return float(wh_a)

            if acc_b < HALF_LIFE_THRESHOLD:
                # Crossing between wh_a and wh_b — linear interpolation
                frac = (acc_a - HALF_LIFE_THRESHOLD) / (acc_a - acc_b)
                return wh_a + frac * (wh_b - wh_a)

        # Accuracy stays above threshold across all measured windows
        return None

    async def _calibration_error(
        self,
        conn: asyncpg.Connection,
        engine_name: str,
        window_h: int,
    ) -> float | None:
        """
        Weighted-average absolute error between confidence numeric value and accuracy.
        Requires ≥5 outcomes per confidence tier to include that tier.
        """
        rows = await conn.fetch(
            """
            SELECT
                sr.confidence_level,
                COUNT(*) FILTER (WHERE so.direction_correct IS NOT NULL) AS n,
                COUNT(*) FILTER (WHERE so.direction_correct = true)::float /
                    NULLIF(COUNT(*) FILTER (WHERE so.direction_correct IS NOT NULL), 0)
                    AS accuracy
              FROM signal_outcomes so
              JOIN signals_registry sr ON sr.id = so.signal_id
             WHERE sr.engine_name = $1
               AND so.evaluation_window_h = $2
               AND so.evaluated_at > now() - $3 * interval '1 day'
               AND sr.confidence_level IS NOT NULL
             GROUP BY sr.confidence_level
            """,
            engine_name,
            window_h,
            self._period_days,
        )

        total_w = 0.0
        weighted_err = 0.0

        for row in rows:
            if row["accuracy"] is None or (row["n"] or 0) < 5:
                continue
            conf_num = _CONF_NUMERIC.get(row["confidence_level"], 0.5)
            err = abs(conf_num - float(row["accuracy"]))
            n = float(row["n"])
            weighted_err += err * n
            total_w += n

        return (weighted_err / total_w) if total_w > 0 else None

    async def _persist_score(self, conn: asyncpg.Connection, score: SignalScore) -> None:
        await conn.execute(
            """
            INSERT INTO signal_scores (
                engine_name, signal_type, window_hours, period_days,
                sample_size, accuracy, predictive_strength,
                half_life_hours, calibration_error, computed_at
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
            ON CONFLICT (engine_name, signal_type, window_hours, (_utc_date(computed_at)))
            DO UPDATE SET
                sample_size         = EXCLUDED.sample_size,
                accuracy            = EXCLUDED.accuracy,
                predictive_strength = EXCLUDED.predictive_strength,
                half_life_hours     = EXCLUDED.half_life_hours,
                calibration_error   = EXCLUDED.calibration_error,
                computed_at         = EXCLUDED.computed_at
            """,
            score.engine_name,
            score.signal_type,
            score.window_hours,
            score.period_days,
            score.sample_size,
            score.accuracy,
            score.predictive_strength,
            score.half_life_hours,
            score.calibration_error,
            score.computed_at,
        )
        logger.debug(
            "score_persisted",
            engine=score.engine_name,
            window_h=score.window_hours,
            strength=score.predictive_strength,
            half_life_h=score.half_life_hours,
        )

    # ── Read API (used by routes/scores.py) ──────────────────────────────────

    async def get_latest_scores(
        self,
        engine_name: str | None = None,
        window_hours: int | None = None,
    ) -> list[dict]:
        """Latest score row per engine/type/window combination."""
        conditions = ["1=1"]
        params: list = []

        if engine_name:
            params.append(engine_name)
            conditions.append(f"engine_name = ${len(params)}")
        if window_hours is not None:
            params.append(window_hours)
            conditions.append(f"window_hours = ${len(params)}")

        where = " AND ".join(conditions)

        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT DISTINCT ON (engine_name, signal_type, window_hours)
                    engine_name, signal_type, window_hours, period_days,
                    sample_size, accuracy, predictive_strength,
                    half_life_hours, calibration_error, computed_at
                  FROM signal_scores
                 WHERE {where}
                 ORDER BY engine_name, signal_type, window_hours, computed_at DESC
                """,
                *params,
            )
            return [dict(r) for r in rows]

    async def get_half_life_summary(self) -> list[dict]:
        """Latest half-life per engine/type, ordered by shortest half-life first."""
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT DISTINCT ON (engine_name, signal_type)
                    engine_name, signal_type,
                    half_life_hours, predictive_strength, sample_size, computed_at
                  FROM signal_scores
                 ORDER BY engine_name, signal_type, computed_at DESC
                """
            )
            result = [dict(r) for r in rows]
            result.sort(key=lambda r: (r["half_life_hours"] or 9999))
            return result




# ── Standalone entrypoint ─────────────────────────────────────────────────────

async def _main() -> None:
    configure_logging()

    from src.db.connection import get_db_pool, close_db_pool
    from src.cache.redis_client import get_redis, close_redis

    http_port      = int(os.getenv("SCORER_HTTP_PORT",   "8014"))
    score_interval = int(os.getenv("SCORER_INTERVAL",    "3600"))
    period_days    = int(os.getenv("SCORER_PERIOD_DAYS", "30"))
    min_samples    = int(os.getenv("SCORER_MIN_SAMPLES", "20"))

    _http.start(http_port, "signal-scorer")

    db    = await get_db_pool()
    redis = await get_redis()
    _http.set_ready("signal-scorer")

    engine = SignalScoringEngine(db, redis, period_days=period_days, min_samples=min_samples)
    logger.info("signal_scorer_started", interval_s=score_interval)

    import uuid as _uuid
    try:
        while True:
            t0 = time.monotonic()
            cycle_id = str(_uuid.uuid4())
            try:
                result = await engine.run_cycle(cycle_id=cycle_id)
                logger.info(
                    "signal_scorer_cycle",
                    cycle_id=cycle_id,
                    engines_scored=result.engines_scored,
                    scores_written=result.scores_written,
                    errors=result.errors,
                    elapsed_s=round(time.monotonic() - t0, 2),
                )
            except Exception as exc:
                logger.error("signal_scorer_error", cycle_id=cycle_id, error=str(exc))
            await asyncio.sleep(score_interval)
    finally:
        _http.set_not_ready()
        await close_db_pool()
        await close_redis()


if __name__ == "__main__":
    asyncio.run(_main())
