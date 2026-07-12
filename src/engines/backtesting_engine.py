"""
Backtesting Engine — Phase 2: Orchestration + Replay System

Deterministic retrospective replay over archived signal data.

Reads exclusively from:
  signals_registry   — archived signal metadata (immutable)
  signal_outcomes    — pre-computed ground truth (immutable)
  signal_scores      — predictive strength from SignalScoringEngine
  macro_regimes      — regime context at emission time (LATERAL join)
  narrative_clusters — narrative context (for narrative breakdown)

Writes to:
  replay_runs    — job metadata (status, timing, signal counts)
  replay_metrics — comprehensive per-engine/type/window metrics

Determinism contract:
  Same period_start + period_end + engine_name → identical replay_metrics.
  No market data is fetched. No outcomes are (re-)computed.
  If re-run, existing metrics are replaced via ON CONFLICT DO UPDATE.

Metrics computed per engine / signal_type / window_hours:
  precision, recall, false_positive_rate, f1_score, directional_accuracy
  regime_breakdown   {regime: accuracy}
  narrative_breakdown {narrative: accuracy}
  predictive_strength, half_life_hours, calibration_error (from signal_scores)
"""
from __future__ import annotations

import json
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING
from uuid import uuid4

import asyncpg

from src.core.logging import get_logger

if TYPE_CHECKING:
    import redis.asyncio as aioredis

logger = get_logger(__name__)

# Directions that represent a "positive" (bullish) prediction
_POSITIVE: frozenset[str] = frozenset({"risk_on", "inflow", "liquidity_expansion"})
# Directions that represent a "negative" (bearish) prediction
_NEGATIVE: frozenset[str] = frozenset({"risk_off", "outflow", "tightening", "volatility_regime"})


@dataclass
class ReplayRunResult:
    run_id: str
    status: str
    total_signals: int
    evaluated_signals: int
    metric_rows_written: int
    errors: int


class BacktestingEngine:
    """
    Retrospective replay engine for historical signal evaluation.

    Reads stored signals and outcomes to compute comprehensive metrics
    over configurable time windows. Never touches source engine tables.
    """

    def __init__(
        self,
        db_pool: asyncpg.Pool,
        redis: "aioredis.Redis | None" = None,
    ) -> None:
        self._pool = db_pool
        self._redis = redis

    # ── Public API ────────────────────────────────────────────────────────────

    async def create_run(
        self,
        window_type: str,
        period_start: datetime,
        period_end: datetime,
        engine_name: str | None = None,
        parameters: dict | None = None,
    ) -> str:
        """
        Insert a replay_runs record with status='queued'.
        Returns the new run_id (UUID string).
        """
        run_id = str(uuid4())
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO replay_runs (
                    run_id, window_type, period_start, period_end,
                    engine_name, status, parameters
                ) VALUES ($1,$2,$3,$4,$5,'queued',$6::jsonb)
                """,
                run_id,
                window_type,
                period_start,
                period_end,
                engine_name,
                json.dumps(parameters or {}),
            )
        logger.info(
            "replay_run_created",
            run_id=run_id,
            window_type=window_type,
            period_start=period_start.isoformat(),
            period_end=period_end.isoformat(),
            engine_name=engine_name,
        )
        return run_id

    async def execute_run(self, run_id: str) -> ReplayRunResult:
        """
        Execute a queued replay run.
        Marks status=running, computes metrics, marks status=completed|failed.
        """
        async with self._pool.acquire() as conn:
            meta = await conn.fetchrow(
                "SELECT * FROM replay_runs WHERE run_id = $1", run_id
            )
            if not meta:
                raise ValueError(f"No replay run found for run_id={run_id}")
            if meta["status"] not in ("queued", "failed"):
                logger.warning("replay_run_skip", run_id=run_id, status=meta["status"])
                return ReplayRunResult(
                    run_id=run_id, status=meta["status"],
                    total_signals=0, evaluated_signals=0, metric_rows_written=0, errors=0,
                )

        period_start: datetime = meta["period_start"]
        period_end: datetime = meta["period_end"]
        engine_filter: str | None = meta["engine_name"]

        await self._set_status(run_id, "running")

        errors = 0
        metric_rows = 0
        total_signals = 0
        evaluated_signals = 0

        try:
            async with self._pool.acquire() as conn:
                # Enumerate distinct (engine_name, signal_type, window_hours) combos
                combos = await self._fetch_combos(
                    conn, period_start, period_end, engine_filter
                )

                for engine_name, signal_type, window_h in combos:
                    try:
                        result = await self._compute_metrics(
                            conn, run_id, engine_name, signal_type, window_h,
                            period_start, period_end,
                        )
                        if result:
                            total_signals += result["signal_count"]
                            evaluated_signals += result["evaluated_count"]
                            metric_rows += 1
                    except Exception as exc:
                        logger.error(
                            "replay_metric_error",
                            run_id=run_id,
                            engine=engine_name,
                            signal_type=signal_type,
                            window_h=window_h,
                            error=str(exc),
                        )
                        errors += 1

            await self._finalize_run(run_id, "completed", total_signals, evaluated_signals)
            logger.info(
                "replay_run_complete",
                run_id=run_id,
                combos=len(combos),
                metric_rows=metric_rows,
                evaluated=evaluated_signals,
                errors=errors,
            )

        except Exception as exc:
            await self._finalize_run(run_id, "failed", total_signals, evaluated_signals, str(exc))
            logger.error("replay_run_failed", run_id=run_id, error=str(exc))
            raise

        return ReplayRunResult(
            run_id=run_id,
            status="completed",
            total_signals=total_signals,
            evaluated_signals=evaluated_signals,
            metric_rows_written=metric_rows,
            errors=errors,
        )

    # ── Read API ──────────────────────────────────────────────────────────────

    async def get_run(self, run_id: str) -> dict | None:
        """Fetch run metadata with its metrics."""
        async with self._pool.acquire() as conn:
            run = await conn.fetchrow(
                "SELECT * FROM replay_runs WHERE run_id = $1", run_id
            )
            if not run:
                return None

            metrics = await conn.fetch(
                """
                SELECT engine_name, signal_type, window_hours,
                       signal_count, evaluated_count,
                       precision_score, recall_score, false_positive_rate,
                       f1_score, directional_accuracy, predictive_strength,
                       half_life_hours, calibration_error,
                       regime_breakdown, narrative_breakdown
                  FROM replay_metrics
                 WHERE run_id = $1
                 ORDER BY engine_name, signal_type, window_hours
                """,
                run_id,
            )

            result = dict(run)
            result["metrics"] = []
            for m in metrics:
                row = dict(m)
                for k in ("regime_breakdown", "narrative_breakdown"):
                    if isinstance(row[k], str):
                        row[k] = json.loads(row[k])
                result["metrics"].append(row)
            return result

    async def list_runs(self, limit: int = 20) -> list[dict]:
        """List recent replay runs, newest first."""
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT run_id, window_type, period_start, period_end,
                       engine_name, status, retry_count,
                       queued_at, started_at, completed_at,
                       total_signals, evaluated_signals, error_message
                  FROM replay_runs
                 ORDER BY queued_at DESC
                 LIMIT $1
                """,
                limit,
            )
            return [dict(r) for r in rows]

    # ── Internal computation ──────────────────────────────────────────────────

    async def _fetch_combos(
        self,
        conn: asyncpg.Connection,
        period_start: datetime,
        period_end: datetime,
        engine_filter: str | None,
    ) -> list[tuple[str, str, int]]:
        """Return distinct (engine_name, signal_type, window_hours) with outcomes in the period."""
        engine_clause = "AND sr.engine_name = $3" if engine_filter else ""
        params: list = [period_start, period_end]
        if engine_filter:
            params.append(engine_filter)

        rows = await conn.fetch(
            f"""
            SELECT DISTINCT sr.engine_name, sr.signal_type, so.evaluation_window_h
              FROM signals_registry sr
              JOIN signal_outcomes so ON so.signal_id = sr.id
             WHERE sr.emitted_at BETWEEN $1 AND $2
               AND so.direction_correct IS NOT NULL
               {engine_clause}
             ORDER BY sr.engine_name, sr.signal_type, so.evaluation_window_h
            """,
            *params,
        )
        return [(r["engine_name"], r["signal_type"], r["evaluation_window_h"]) for r in rows]

    async def _compute_metrics(
        self,
        conn: asyncpg.Connection,
        run_id: str,
        engine_name: str,
        signal_type: str,
        window_h: int,
        period_start: datetime,
        period_end: datetime,
    ) -> dict | None:
        """
        Compute and persist replay_metrics for one (engine, signal_type, window_h).
        Returns summary dict or None if no evaluable signals.
        """
        # ── Core classification counts ─────────────────────────────────────────
        row = await conn.fetchrow(
            """
            SELECT
                COUNT(*)                                                    AS signal_count,
                COUNT(*) FILTER (WHERE so.direction_correct IS NOT NULL)    AS evaluated_count,
                -- Positive = risk_on/inflow direction predicted correctly
                COUNT(*) FILTER (
                    WHERE so.direction_correct = true
                      AND sr.direction = ANY($5::text[])
                )                                                           AS true_positives,
                COUNT(*) FILTER (
                    WHERE so.direction_correct = false
                      AND sr.direction = ANY($5::text[])
                )                                                           AS false_positives,
                COUNT(*) FILTER (
                    WHERE so.direction_correct = true
                      AND sr.direction = ANY($6::text[])
                )                                                           AS true_negatives,
                COUNT(*) FILTER (
                    WHERE so.direction_correct = false
                      AND sr.direction = ANY($6::text[])
                )                                                           AS false_negatives
              FROM signals_registry sr
              JOIN signal_outcomes so
                ON so.signal_id = sr.id AND so.evaluation_window_h = $4
             WHERE sr.engine_name   = $1
               AND sr.signal_type   = $2
               AND sr.emitted_at    BETWEEN $3 AND $7
            """,
            engine_name,
            signal_type,
            period_start,
            window_h,
            list(_POSITIVE),
            list(_NEGATIVE),
            period_end,
        )

        if not row or (row["evaluated_count"] or 0) == 0:
            return None

        tp = row["true_positives"] or 0
        fp = row["false_positives"] or 0
        tn = row["true_negatives"] or 0
        fn = row["false_negatives"] or 0
        evaluated = row["evaluated_count"] or 0
        total = row["signal_count"] or 0

        precision = tp / (tp + fp) if (tp + fp) > 0 else None
        recall    = tp / (tp + fn) if (tp + fn) > 0 else None
        fpr       = fp / (fp + tn) if (fp + tn) > 0 else None
        f1        = (
            2 * precision * recall / (precision + recall)
            if precision and recall
            else None
        )
        dir_acc = (tp + tn) / evaluated if evaluated > 0 else None

        # ── Pull quality scores from signal_scores ─────────────────────────────
        score_row = await conn.fetchrow(
            """
            SELECT predictive_strength, half_life_hours, calibration_error
              FROM signal_scores
             WHERE engine_name  = $1
               AND signal_type  = $2
               AND window_hours = $3
             ORDER BY computed_at DESC
             LIMIT 1
            """,
            engine_name,
            signal_type,
            window_h,
        )
        ps   = float(score_row["predictive_strength"]) if score_row and score_row["predictive_strength"] is not None else None
        hl   = float(score_row["half_life_hours"])     if score_row and score_row["half_life_hours"]     is not None else None
        cal  = float(score_row["calibration_error"])   if score_row and score_row["calibration_error"]   is not None else None

        # ── Regime breakdown ───────────────────────────────────────────────────
        regime_bd = await self._regime_breakdown(
            conn, engine_name, signal_type, window_h, period_start, period_end
        )

        # ── Narrative breakdown ────────────────────────────────────────────────
        narrative_bd = await self._narrative_breakdown(
            conn, engine_name, signal_type, window_h, period_start, period_end
        )

        # ── Persist ───────────────────────────────────────────────────────────
        await conn.execute(
            """
            INSERT INTO replay_metrics (
                run_id, engine_name, signal_type, window_hours,
                period_start, period_end,
                signal_count, evaluated_count,
                true_positives, false_positives, true_negatives, false_negatives,
                precision_score, recall_score, false_positive_rate, f1_score,
                directional_accuracy, predictive_strength, half_life_hours,
                calibration_error, regime_breakdown, narrative_breakdown
            ) VALUES (
                $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,
                $13,$14,$15,$16,$17,$18,$19,$20,$21::jsonb,$22::jsonb
            )
            ON CONFLICT (run_id, engine_name, signal_type, window_hours) DO UPDATE SET
                signal_count        = EXCLUDED.signal_count,
                evaluated_count     = EXCLUDED.evaluated_count,
                true_positives      = EXCLUDED.true_positives,
                false_positives     = EXCLUDED.false_positives,
                true_negatives      = EXCLUDED.true_negatives,
                false_negatives     = EXCLUDED.false_negatives,
                precision_score     = EXCLUDED.precision_score,
                recall_score        = EXCLUDED.recall_score,
                false_positive_rate = EXCLUDED.false_positive_rate,
                f1_score            = EXCLUDED.f1_score,
                directional_accuracy= EXCLUDED.directional_accuracy,
                predictive_strength = EXCLUDED.predictive_strength,
                half_life_hours     = EXCLUDED.half_life_hours,
                calibration_error   = EXCLUDED.calibration_error,
                regime_breakdown    = EXCLUDED.regime_breakdown,
                narrative_breakdown = EXCLUDED.narrative_breakdown,
                computed_at         = now()
            """,
            run_id, engine_name, signal_type, window_h,
            period_start, period_end,
            total, evaluated,
            tp, fp, tn, fn,
            _r(precision), _r(recall), _r(fpr), _r(f1),
            _r(dir_acc), _r(ps), _r(hl),
            _r(cal),
            json.dumps(regime_bd),
            json.dumps(narrative_bd),
        )

        return {"signal_count": total, "evaluated_count": evaluated}

    async def _regime_breakdown(
        self,
        conn: asyncpg.Connection,
        engine_name: str,
        signal_type: str,
        window_h: int,
        period_start: datetime,
        period_end: datetime,
    ) -> dict[str, float]:
        """
        Accuracy per macro regime at signal emission time.
        Uses LATERAL join to find the regime active just before each signal.
        """
        rows = await conn.fetch(
            """
            SELECT
                COALESCE(mr.regime, 'unknown') AS regime,
                COUNT(*) FILTER (WHERE so.direction_correct IS NOT NULL) AS n,
                COUNT(*) FILTER (WHERE so.direction_correct = true)::float /
                    NULLIF(COUNT(*) FILTER (WHERE so.direction_correct IS NOT NULL), 0)
                    AS accuracy
              FROM signals_registry sr
              JOIN signal_outcomes so
                ON so.signal_id = sr.id AND so.evaluation_window_h = $4
              LEFT JOIN LATERAL (
                SELECT regime FROM macro_regimes
                 WHERE computed_at <= sr.emitted_at
                 ORDER BY computed_at DESC
                 LIMIT 1
              ) mr ON true
             WHERE sr.engine_name  = $1
               AND sr.signal_type  = $2
               AND sr.emitted_at   BETWEEN $3 AND $5
               AND so.direction_correct IS NOT NULL
             GROUP BY mr.regime
            """,
            engine_name, signal_type, period_start, window_h, period_end,
        )
        return {
            r["regime"]: round(float(r["accuracy"]), 4)
            for r in rows
            if r["accuracy"] is not None and (r["n"] or 0) >= 5
        }

    async def _narrative_breakdown(
        self,
        conn: asyncpg.Connection,
        engine_name: str,
        signal_type: str,
        window_h: int,
        period_start: datetime,
        period_end: datetime,
    ) -> dict[str, float]:
        """
        Accuracy per narrative cluster.
        Extracts narrative from signal_data JSONB (narrative_cluster field
        or contributing_sources.top_narrative for confluence signals).
        """
        rows = await conn.fetch(
            """
            SELECT
                COALESCE(
                    sr.signal_data->>'narrative_cluster',
                    sr.signal_data->'contributing_sources'->>'top_narrative',
                    'unknown'
                ) AS narrative,
                COUNT(*) FILTER (WHERE so.direction_correct IS NOT NULL) AS n,
                COUNT(*) FILTER (WHERE so.direction_correct = true)::float /
                    NULLIF(COUNT(*) FILTER (WHERE so.direction_correct IS NOT NULL), 0)
                    AS accuracy
              FROM signals_registry sr
              JOIN signal_outcomes so
                ON so.signal_id = sr.id AND so.evaluation_window_h = $4
             WHERE sr.engine_name  = $1
               AND sr.signal_type  = $2
               AND sr.emitted_at   BETWEEN $3 AND $5
               AND so.direction_correct IS NOT NULL
             GROUP BY narrative
            """,
            engine_name, signal_type, period_start, window_h, period_end,
        )
        return {
            r["narrative"]: round(float(r["accuracy"]), 4)
            for r in rows
            if r["accuracy"] is not None and (r["n"] or 0) >= 5
                and r["narrative"] not in ("unknown", "null", "")
        }

    # ── DB helpers ────────────────────────────────────────────────────────────

    async def _set_status(self, run_id: str, status: str) -> None:
        col = "started_at" if status == "running" else "completed_at"
        async with self._pool.acquire() as conn:
            await conn.execute(
                f"UPDATE replay_runs SET status = $1, {col} = now() WHERE run_id = $2",
                status, run_id,
            )

    async def _finalize_run(
        self,
        run_id: str,
        status: str,
        total: int,
        evaluated: int,
        error: str | None = None,
    ) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE replay_runs SET
                    status            = $1,
                    completed_at      = now(),
                    total_signals     = $2,
                    evaluated_signals = $3,
                    error_message     = $4
                WHERE run_id = $5
                """,
                status, total, evaluated, error, run_id,
            )


def _r(v: float | None, decimals: int = 4) -> float | None:
    """Round or pass through None."""
    return round(v, decimals) if v is not None and not math.isnan(v) else None
