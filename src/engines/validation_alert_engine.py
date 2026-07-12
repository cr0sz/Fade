"""
Validation Alert Engine — Phase 3

Monitors system quality metrics and fires alerts when degradation is detected:
  - precision_collapse: engine accuracy drops >20% or >30% week-over-week
  - calibration_drift: calibration error exceeds threshold
  - fp_spike: false positive rate sustained high across recent runs
  - replay_failure: multiple failed replay jobs in 24h
  - narrative_degradation: narrative accuracy consistently below 0.50
  - coverage_gap: outcome coverage below critical thresholds

Alerts are stored in validation_alerts table and delivered via Telegram.
Cooldown period prevents duplicate alerts within a configurable window.

Standalone service with embedded HTTPServer on port 8016.
"""
from __future__ import annotations

import asyncio
import json
import os
import traceback
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple

import asyncpg
from prometheus_client import Counter, Gauge, Histogram

from src.core import http_server as _http

try:
    import aiohttp
    _AIOHTTP_AVAILABLE = True
except ImportError:
    _AIOHTTP_AVAILABLE = False

try:
    from src.core.discord import send_embed as _discord_send_embed, WEBHOOK_ALERTS as _DISCORD_WEBHOOK_ALERTS, COLOR_WARN as _COLOR_WARN
    _DISCORD_AVAILABLE = True
except ImportError:
    _DISCORD_AVAILABLE = False

from src.core.logging import get_logger

logger = get_logger(__name__)

# ─── Prometheus Metrics ───────────────────────────────────────────────────────
alerts_fired_total = Counter(
    "validation_alerts_fired_total",
    "Alerts fired by the validation alert engine",
    ["alert_type", "severity"],
)
alerts_active_gauge = Gauge(
    "validation_alerts_active",
    "Currently active (unresolved) alerts",
    ["alert_type"],
)
alert_check_cycle_seconds = Histogram(
    "alert_check_cycle_seconds",
    "Duration of each alert check cycle",
    buckets=[0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0],
)
alert_cycles_total = Counter(
    "alert_check_cycles_total",
    "Total alert check cycles completed",
)
alert_errors_total = Counter(
    "alert_check_errors_total",
    "Errors in alert check cycles",
)


# ─── Configuration ────────────────────────────────────────────────────────────

@dataclass
class AlertConfig:
    check_interval_s: int      = int(os.getenv("ALERT_CHECK_INTERVAL", "900"))
    cooldown_hours: float      = float(os.getenv("ALERT_COOLDOWN_HOURS", "4"))
    precision_drop_high: float = float(os.getenv("ALERT_PRECISION_DROP_HIGH", "0.20"))
    precision_drop_crit: float = float(os.getenv("ALERT_PRECISION_DROP_CRITICAL", "0.30"))
    calib_drift_thresh: float  = float(os.getenv("ALERT_CALIBRATION_DRIFT_THRESHOLD", "0.20"))
    fp_rate_thresh: float      = float(os.getenv("ALERT_FP_RATE_THRESHOLD", "0.40"))
    replay_fail_high: int      = int(os.getenv("ALERT_REPLAY_FAILURE_HIGH", "2"))
    replay_fail_crit: int      = int(os.getenv("ALERT_REPLAY_FAILURE_CRITICAL", "5"))
    coverage_gap_high: float   = float(os.getenv("ALERT_COVERAGE_GAP_HIGH", "0.40"))
    coverage_gap_crit: float   = float(os.getenv("ALERT_COVERAGE_GAP_CRITICAL", "0.20"))
    http_port: int             = int(os.getenv("ALERT_HTTP_PORT", "8016"))
    # ── Phase 4: Governance alert thresholds ─────────────────────────────────
    instability_drop_thresh: float  = float(os.getenv("ALERT_INSTABILITY_DROP", "20.0"))
    instability_cooldown_hours: float = float(os.getenv("ALERT_INSTABILITY_COOLDOWN_HOURS", "24.0"))
    recovery_fail_weeks: int        = int(os.getenv("ALERT_RECOVERY_FAIL_WEEKS", "4"))
    degradation_accel_thresh: float = float(os.getenv("ALERT_DEGRADATION_ACCEL", "0.03"))
    narrative_collapse_from: float  = float(os.getenv("ALERT_NARRATIVE_COLLAPSE_FROM", "0.60"))
    narrative_collapse_to: float    = float(os.getenv("ALERT_NARRATIVE_COLLAPSE_TO", "0.45"))
    confidence_inconsistency_thresh: float = float(os.getenv("ALERT_CONFIDENCE_INCONSISTENCY", "0.55"))
    # ── Phase 5: Meta-intelligence alert thresholds ──────────────────────────
    recurring_failure_min_recurrence: int   = int(os.getenv("ALERT_RECURRING_FAILURE_MIN", "3"))
    meta_memory_window_days: int            = int(os.getenv("ALERT_META_MEMORY_WINDOW_DAYS", "90"))
    transition_instability_thresh: float    = float(os.getenv("ALERT_TRANSITION_INSTABILITY", "15.0"))
    narrative_decay_velocity_thresh: float  = float(os.getenv("ALERT_NARRATIVE_DECAY_VELOCITY", "0.05"))
    governance_stress_thresh: float         = float(os.getenv("ALERT_GOVERNANCE_STRESS", "65.0"))
    strategic_instability_thresh: float     = float(os.getenv("ALERT_STRATEGIC_INSTABILITY", "60.0"))


# ─── Alert Engine ─────────────────────────────────────────────────────────────

class ValidationAlertEngine:
    """Detects system quality degradation and routes alerts to Telegram."""

    def __init__(self, db: asyncpg.Pool, redis=None, config: Optional[AlertConfig] = None):
        self._db = db
        self._redis = redis
        self._cfg = config or AlertConfig()

    # ── Main cycle ─────────────────────────────────────────────────────────────

    async def run_cycle(self) -> int:
        """Run one full check cycle. Returns number of alerts fired."""
        fired = 0
        with alert_check_cycle_seconds.time():
            try:
                fired += await self._check_precision_collapse()
                fired += await self._check_calibration_drift()
                fired += await self._check_fp_spike()
                fired += await self._check_replay_failures()
                fired += await self._check_narrative_degradation()
                fired += await self._check_coverage_gap()
                fired += await self._check_stale_quotes()
                # Phase 4: Governance alerts
                fired += await self._check_engine_instability_spike()
                fired += await self._check_recovery_failure()
                fired += await self._check_degradation_acceleration()
                fired += await self._check_narrative_collapse()
                fired += await self._check_confidence_inconsistency()
                # Phase 5: Meta-intelligence alerts
                fired += await self._check_recurring_historical_failure()
                fired += await self._check_transition_instability_warning()
                fired += await self._check_narrative_collapse_acceleration()
                fired += await self._check_governance_stress_spike()
                fired += await self._check_strategic_instability_alert()
                fired += await self._check_crypto_engine_inversion()
                await self._resolve_cleared_alerts()
                await self._update_active_gauges()
                alert_cycles_total.inc()
            except Exception:
                alert_errors_total.inc()
                logger.error("alert_cycle_error", exc=traceback.format_exc())
        return fired

    # ── Precision Collapse ─────────────────────────────────────────────────────

    async def _check_precision_collapse(self) -> int:
        fired = 0
        async with self._db.acquire() as conn:
            rows = await conn.fetch("""
                WITH current_week AS (
                    SELECT engine_name, AVG(precision_score) AS prec
                    FROM replay_metrics
                    WHERE window_hours = 24
                      AND computed_at >= NOW() - INTERVAL '7 days'
                    GROUP BY engine_name
                ),
                prior_week AS (
                    SELECT engine_name, AVG(precision_score) AS prec
                    FROM replay_metrics
                    WHERE window_hours = 24
                      AND computed_at >= NOW() - INTERVAL '14 days'
                      AND computed_at <  NOW() - INTERVAL '7 days'
                    GROUP BY engine_name
                )
                SELECT c.engine_name,
                       c.prec  AS current_prec,
                       p.prec  AS prior_prec,
                       (p.prec - c.prec) AS drop
                FROM current_week c
                JOIN prior_week p USING (engine_name)
                WHERE p.prec IS NOT NULL
                  AND c.prec IS NOT NULL
            """)

        _PRECISION_PENALTY_KEY = "trading:engine_weights:precision"
        _PENALTY_COLLAPSE = 0.40   # relative drop threshold to apply 0.5× penalty
        _PENALTY_RECOVERY = 0.20   # relative drop threshold to clear penalty (hysteresis)

        for row in rows:
            drop = float(row["drop"])
            # ── Alert path (unchanged): only fires on absolute drops above config threshold ──
            if drop >= self._cfg.precision_drop_high:
                severity = "critical" if drop >= self._cfg.precision_drop_crit else "high"
                if not await self._in_cooldown("precision_collapse", row["engine_name"]):
                    details = {
                        "current_precision": round(float(row["current_prec"]), 4),
                        "prior_precision": round(float(row["prior_prec"]), 4),
                        "drop_pct": round(drop * 100, 1),
                    }
                    await self._fire(
                        "precision_collapse", severity, row["engine_name"],
                        threshold_value=self._cfg.precision_drop_high,
                        observed_value=round(drop, 4),
                        details=details,
                    )
                    fired += 1

            # ── Precision-collapse confidence penalty: halve confidence via Redis weight ──
            if self._redis is None:
                continue
            prior_prec = float(row["prior_prec"])
            if prior_prec <= 0:
                continue
            rel_drop = drop / prior_prec   # (prior - current) / prior
            engine_name = row["engine_name"]
            try:
                if rel_drop > _PENALTY_COLLAPSE:
                    await self._redis.hset(_PRECISION_PENALTY_KEY, engine_name, "0.5")
                    logger.info(
                        "precision_penalty_applied",
                        engine=engine_name,
                        rel_drop_pct=round(rel_drop * 100, 1),
                        prior_prec=round(prior_prec, 4),
                        current_prec=round(float(row["current_prec"]), 4),
                    )
                elif rel_drop < _PENALTY_RECOVERY:
                    removed = await self._redis.hdel(_PRECISION_PENALTY_KEY, engine_name)
                    if removed:
                        logger.info(
                            "precision_penalty_cleared",
                            engine=engine_name,
                            rel_drop_pct=round(rel_drop * 100, 1),
                        )
            except Exception as _redis_exc:
                logger.warning("precision_penalty_redis_error",
                               engine=engine_name, error=str(_redis_exc))

        return fired

    # ── Calibration Drift ──────────────────────────────────────────────────────

    async def _check_calibration_drift(self) -> int:
        fired = 0
        async with self._db.acquire() as conn:
            rows = await conn.fetch("""
                SELECT DISTINCT ON (engine_name)
                    engine_name, calibration_error
                FROM signal_scores
                WHERE calibration_error IS NOT NULL
                ORDER BY engine_name, computed_at DESC
            """)

        for row in rows:
            err = float(row["calibration_error"])
            if err < self._cfg.calib_drift_thresh:
                continue
            if await self._in_cooldown("calibration_drift", row["engine_name"]):
                continue
            severity = "critical" if err > self._cfg.calib_drift_thresh * 1.5 else "high"
            await self._fire(
                "calibration_drift", severity, row["engine_name"],
                threshold_value=self._cfg.calib_drift_thresh,
                observed_value=round(err, 4),
                details={"calibration_error": round(err, 4)},
            )
            fired += 1
        return fired

    # ── False Positive Spike ───────────────────────────────────────────────────

    async def _check_fp_spike(self) -> int:
        fired = 0
        async with self._db.acquire() as conn:
            rows = await conn.fetch("""
                SELECT engine_name, AVG(false_positive_rate) AS avg_fpr
                FROM replay_metrics
                WHERE window_hours = 24
                  AND computed_at >= NOW() - INTERVAL '21 days'
                GROUP BY engine_name
                HAVING COUNT(*) >= 3
            """)

        for row in rows:
            fpr = float(row["avg_fpr"])
            if fpr < self._cfg.fp_rate_thresh:
                continue
            if await self._in_cooldown("fp_spike", row["engine_name"]):
                continue
            await self._fire(
                "fp_spike", "high", row["engine_name"],
                threshold_value=self._cfg.fp_rate_thresh,
                observed_value=round(fpr, 4),
                details={"avg_false_positive_rate": round(fpr, 4)},
            )
            fired += 1
        return fired

    # ── Replay Failures ────────────────────────────────────────────────────────

    async def _check_replay_failures(self) -> int:
        async with self._db.acquire() as conn:
            fail_24h = await conn.fetchval("""
                SELECT COUNT(*) FROM replay_runs
                WHERE status IN ('failed', 'error')
                  AND COALESCE(completed_at, queued_at) >= NOW() - INTERVAL '24 hours'
            """) or 0

        count = int(fail_24h)
        if count < self._cfg.replay_fail_high:
            return 0
        if await self._in_cooldown("replay_failure", None):
            return 0

        severity = "critical" if count >= self._cfg.replay_fail_crit else "high"
        await self._fire(
            "replay_failure", severity, None,
            threshold_value=float(self._cfg.replay_fail_high),
            observed_value=float(count),
            details={"failed_replays_24h": count},
        )
        return 1

    # ── Narrative Degradation ──────────────────────────────────────────────────

    async def _check_narrative_degradation(self) -> int:
        fired = 0
        async with self._db.acquire() as conn:
            rows = await conn.fetch("""
                SELECT
                    kv.key AS narrative,
                    date_trunc('week', computed_at) AS wk,
                    AVG((kv.value->>'accuracy')::numeric)   AS acc,
                    SUM((kv.value->>'sample_count')::int)   AS samples
                FROM replay_metrics,
                     jsonb_each(narrative_breakdown) AS kv
                WHERE computed_at >= NOW() - INTERVAL '4 weeks'
                  AND jsonb_typeof(narrative_breakdown) = 'object'
                GROUP BY narrative, wk
                HAVING SUM((kv.value->>'sample_count')::int) >= 5
                ORDER BY narrative, wk
            """)

        # Group by narrative, check if last 2 weeks are both < 0.50
        narrative_weeks: Dict[str, List[float]] = {}
        for row in rows:
            narrative_weeks.setdefault(row["narrative"], []).append(float(row["acc"]))

        for narrative, acc_list in narrative_weeks.items():
            if len(acc_list) < 2:
                continue
            if all(a < 0.50 for a in acc_list[-2:]):
                key = f"narrative_degradation::{narrative}"
                if await self._in_cooldown("narrative_degradation", narrative):
                    continue
                await self._fire(
                    "narrative_degradation", "medium", None,
                    threshold_value=0.50,
                    observed_value=round(acc_list[-1], 4),
                    details={
                        "narrative": narrative,
                        "recent_accuracies": [round(a, 4) for a in acc_list[-2:]],
                    },
                )
                fired += 1
        return fired

    # ── Coverage Gap ───────────────────────────────────────────────────────────

    async def _check_coverage_gap(self) -> int:
        async with self._db.acquire() as conn:
            total = await conn.fetchval("SELECT COUNT(*) FROM signals_registry") or 0
            evaluated = await conn.fetchval(
                "SELECT COUNT(DISTINCT signal_id) FROM signal_outcomes"
            ) or 0

        if total < 50:  # skip on cold start — not enough history for meaningful coverage
            return 0
        coverage = evaluated / total
        if coverage >= self._cfg.coverage_gap_high:
            return 0
        if await self._in_cooldown("coverage_gap", None):
            return 0

        severity = "critical" if coverage < self._cfg.coverage_gap_crit else "high"
        await self._fire(
            "coverage_gap", severity, None,
            threshold_value=self._cfg.coverage_gap_high,
            observed_value=round(coverage, 4),
            details={
                "total_signals": int(total),
                "evaluated_signals": int(evaluated),
                "coverage_pct": round(coverage * 100, 1),
            },
        )
        return 1

    # ── Stale Quote Feed ────────────────────────────────────────────────────────

    async def _check_stale_quotes(self) -> int:
        """Alert when an active CRYPTO ticker's quote feed goes silent.

        Crypto feeds run 24/7 every ~5 min, so >90 min of silence means the
        feed is broken (provider id migration, delisting), not a quiet market.
        The MATIC/POL migration killed that feed for 5 days unnoticed — and the
        only fade signals that day were MATIC, silently dropped on stale price.
        Per-ticker cooldown via the engine_name slot.
        """
        fired = 0
        async with self._db.acquire() as conn:
            rows = await conn.fetch("""
                SELECT t.symbol,
                       EXTRACT(EPOCH FROM (NOW() - MAX(q.ts))) / 60 AS age_min
                  FROM tickers t
                  LEFT JOIN ticker_quotes q ON q.symbol = t.symbol
                 WHERE t.is_active AND UPPER(t.exchange) = 'CRYPTO'
                   AND t.metadata->>'coingecko_id' IS NOT NULL
                 GROUP BY t.symbol
                HAVING MAX(q.ts) IS NULL OR MAX(q.ts) < NOW() - INTERVAL '90 minutes'
            """)

        for row in rows:
            age_min = float(row["age_min"]) if row["age_min"] is not None else -1.0
            if await self._in_cooldown("stale_quote_feed", row["symbol"]):
                continue
            severity = "critical" if (age_min < 0 or age_min > 24 * 60) else "high"
            await self._fire(
                "stale_quote_feed", severity, row["symbol"],
                threshold_value=90.0,
                observed_value=round(age_min, 1),
                details={
                    "ticker": row["symbol"],
                    "quote_age_min": round(age_min, 1) if age_min >= 0 else None,
                    "hint": "check coingecko_id in tickers.metadata (token migration?)",
                },
            )
            fired += 1
        return fired

    # ── Phase 5: Meta-Intelligence Alert Checks ───────────────────────────────

    async def _check_recurring_historical_failure(self) -> int:
        """Alert when a failure pattern recurs >= threshold times in memory window."""
        fired = 0
        async with self._db.acquire() as conn:
            rows = await conn.fetch("""
                SELECT cluster_id, event_type, engine_name,
                       MAX(recurrence_index) AS max_recurrence,
                       MAX(occurred_at)      AS last_seen
                FROM strategic_memory_events
                WHERE occurred_at >= NOW() - ($1 || ' days')::INTERVAL
                  AND event_type IN ('degradation_event', 'calibration_failure',
                                     'false_positive_surge')
                GROUP BY cluster_id, event_type, engine_name
                HAVING MAX(recurrence_index) >= $2
                   AND MAX(occurred_at) >= NOW() - INTERVAL '6 hours'
            """, str(self._cfg.meta_memory_window_days),
                self._cfg.recurring_failure_min_recurrence)

        for row in rows:
            if await self._in_cooldown("recurring_historical_failure", row["engine_name"]):
                continue
            recurrence = int(row["max_recurrence"])
            severity = "critical" if recurrence >= 5 else "high"
            await self._fire(
                "recurring_historical_failure", severity, row["engine_name"],
                threshold_value=float(self._cfg.recurring_failure_min_recurrence),
                observed_value=float(recurrence),
                details={
                    "cluster_id": row["cluster_id"],
                    "event_type": row["event_type"],
                    "recurrence_count": recurrence,
                    "last_seen": row["last_seen"].isoformat() if row["last_seen"] else None,
                },
            )
            fired += 1
        return fired

    async def _check_transition_instability_warning(self) -> int:
        """Alert when a historically dangerous regime transition is detected."""
        fired = 0
        async with self._db.acquire() as conn:
            rows = await conn.fetch("""
                SELECT from_regime, to_regime, transition_instability_score,
                       transition_volatility_score, detected_at
                FROM regime_transition_records
                WHERE is_historically_dangerous = TRUE
                  AND detected_at >= NOW() - INTERVAL '48 hours'
                ORDER BY detected_at DESC
                LIMIT 5
            """)

        for row in rows:
            engine_ctx = f"{row['from_regime']}->{row['to_regime']}"
            if await self._in_cooldown("transition_instability_warning", engine_ctx):
                continue
            score = float(row["transition_instability_score"])
            severity = "critical" if score >= 40 else "high"
            await self._fire(
                "transition_instability_warning", severity, None,
                threshold_value=self._cfg.transition_instability_thresh,
                observed_value=round(score, 2),
                details={
                    "from_regime": row["from_regime"],
                    "to_regime": row["to_regime"],
                    "instability_score": round(score, 2),
                    "volatility_score": round(float(row["transition_volatility_score"]), 2),
                    "is_historically_dangerous": True,
                },
            )
            fired += 1
        return fired

    async def _check_narrative_collapse_acceleration(self) -> int:
        """Alert when a narrative's decay velocity exceeds threshold (rapid collapse)."""
        fired = 0
        async with self._db.acquire() as conn:
            rows = await conn.fetch("""
                SELECT DISTINCT ON (narrative_name)
                    narrative_name, decay_velocity, current_accuracy, lifecycle_phase
                FROM narrative_lifecycle_records
                WHERE lifecycle_phase IN ('degradation', 'collapse')
                  AND decay_velocity < $1
                  AND recorded_at >= NOW() - INTERVAL '48 hours'
                ORDER BY narrative_name, recorded_at DESC
            """, -self._cfg.narrative_decay_velocity_thresh)

        for row in rows:
            if await self._in_cooldown("narrative_collapse_acceleration", row["narrative_name"]):
                continue
            vel = float(row["decay_velocity"])
            await self._fire(
                "narrative_collapse_acceleration", "high", row["narrative_name"],
                threshold_value=-self._cfg.narrative_decay_velocity_thresh,
                observed_value=round(vel, 6),
                details={
                    "narrative": row["narrative_name"],
                    "decay_velocity_per_week": round(vel, 6),
                    "current_accuracy": round(float(row["current_accuracy"] or 0), 4),
                    "lifecycle_phase": row["lifecycle_phase"],
                },
            )
            fired += 1
        return fired

    async def _check_crypto_engine_inversion(self) -> int:
        """Alert when any engine is strongly anti-predictive (direction_correct < 40%, n > 100)."""
        fired = 0
        async with self._db.acquire() as conn:
            rows = await conn.fetch("""
                SELECT sr.engine_name, sr.direction,
                       COUNT(*) AS n,
                       AVG(CASE WHEN so.direction_correct THEN 1.0 ELSE 0.0 END) AS accuracy
                FROM signal_outcomes so
                JOIN signals_registry sr ON sr.id = so.signal_id
                LEFT JOIN tickers t ON t.symbol = sr.ticker
                WHERE so.evaluated_at > NOW() - INTERVAL '7 days'
                  AND so.direction_correct IS NOT NULL
                  AND UPPER(COALESCE(t.exchange, '')) = 'CRYPTO'
                GROUP BY sr.engine_name, sr.direction
                HAVING COUNT(*) > 100
                   AND AVG(CASE WHEN so.direction_correct THEN 1.0 ELSE 0.0 END) < 0.40
            """)

        for row in rows:
            engine_name = row["engine_name"]
            direction = row["direction"]
            n = int(row["n"])
            accuracy = float(row["accuracy"])
            if await self._in_cooldown("engine_inversion", engine_name):
                continue
            await self._fire(
                "engine_inversion", "high", engine_name,
                threshold_value=0.40,
                observed_value=round(accuracy, 3),
                details={
                    "engine_name": engine_name,
                    "direction": direction,
                    "n": n,
                    "accuracy_7d": round(accuracy, 3),
                    "source": "engine_inversion_check",
                    "recommendation": f"Consider inverting {engine_name}/{direction} signals in router",
                },
            )
            if _DISCORD_AVAILABLE and _DISCORD_WEBHOOK_ALERTS:
                try:
                    await _discord_send_embed(
                        _DISCORD_WEBHOOK_ALERTS,
                        f"⚠️ Engine Inversion: {engine_name}",
                        color=_COLOR_WARN,
                        fields=[
                            {"name": "Direction", "value": direction, "inline": True},
                            {"name": "Accuracy (7d)", "value": f"{accuracy:.1%} (n={n})", "inline": True},
                            {"name": "Action", "value": f"Add to router invert bucket: {engine_name}/{direction}/CRYPTO", "inline": False},
                        ],
                    )
                except Exception:
                    pass
            fired += 1
        return fired

    async def _check_governance_stress_spike(self) -> int:
        """Alert when governance stress score exceeds critical threshold."""
        async with self._db.acquire() as conn:
            row = await conn.fetchrow("""
                SELECT governance_stress_score, strategic_instability_index, snapshot_at
                FROM meta_intelligence_snapshots
                WHERE snapshot_at >= NOW() - INTERVAL '26 hours'
                ORDER BY snapshot_at DESC
                LIMIT 1
            """)

        if not row:
            return 0
        stress = float(row["governance_stress_score"])
        if stress < self._cfg.governance_stress_thresh:
            return 0
        if await self._in_cooldown("governance_stress_spike", None):
            return 0

        severity = "critical" if stress >= 80 else "high"
        await self._fire(
            "governance_stress_spike", severity, None,
            threshold_value=self._cfg.governance_stress_thresh,
            observed_value=round(stress, 2),
            details={
                "governance_stress_score": round(stress, 2),
                "strategic_instability_index": round(float(row["strategic_instability_index"]), 2),
                "snapshot_at": row["snapshot_at"].isoformat() if row["snapshot_at"] else None,
            },
        )
        return 1

    async def _check_strategic_instability_alert(self) -> int:
        """Alert when strategic instability index exceeds critical threshold."""
        async with self._db.acquire() as conn:
            row = await conn.fetchrow("""
                SELECT strategic_instability_index, recurring_failure_density,
                       active_failure_clusters, snapshot_at
                FROM meta_intelligence_snapshots
                WHERE snapshot_at >= NOW() - INTERVAL '6 hours'
                ORDER BY snapshot_at DESC
                LIMIT 1
            """)

        if not row:
            return 0
        instability = float(row["strategic_instability_index"])
        if instability < self._cfg.strategic_instability_thresh:
            return 0
        density = float(row["recurring_failure_density"]) if row["recurring_failure_density"] else 0.0
        if density >= 100.0:
            # Saturated/degenerate snapshot state — suppress to avoid spam
            return 0
        if await self._in_cooldown("strategic_instability_alert", None):
            return 0

        severity = "critical" if instability >= 75 else "high"
        await self._fire(
            "strategic_instability_alert", severity, None,
            threshold_value=self._cfg.strategic_instability_thresh,
            observed_value=round(instability, 2),
            details={
                "strategic_instability_index": round(instability, 2),
                "recurring_failure_density": round(float(row["recurring_failure_density"]), 2),
                "active_failure_clusters": int(row["active_failure_clusters"]),
            },
        )
        return 1

    # ── Phase 4: Governance Alert Checks ──────────────────────────────────────

    async def _check_engine_instability_spike(self) -> int:
        """Alert when engine stability_score drops > 20 points in 7 days."""
        fired = 0
        async with self._db.acquire() as conn:
            rows = await conn.fetch("""
                WITH recent AS (
                    SELECT DISTINCT ON (engine_name)
                        engine_name, stability_score
                    FROM engine_trust_scores
                    WHERE computed_at >= NOW() - INTERVAL '24 hours'
                    ORDER BY engine_name, computed_at DESC
                ),
                prior AS (
                    SELECT DISTINCT ON (engine_name)
                        engine_name, stability_score
                    FROM engine_trust_scores
                    WHERE computed_at BETWEEN NOW() - INTERVAL '8 days'
                                          AND NOW() - INTERVAL '7 days'
                    ORDER BY engine_name, computed_at DESC
                )
                SELECT r.engine_name,
                       r.stability_score AS current_stability,
                       p.stability_score AS prior_stability,
                       (p.stability_score - r.stability_score) AS drop
                FROM recent r JOIN prior p USING (engine_name)
                WHERE p.stability_score IS NOT NULL
                  AND r.stability_score IS NOT NULL
            """)

        for row in rows:
            drop = float(row["drop"] or 0)
            if drop < self._cfg.instability_drop_thresh:
                continue
            # current_stability == 0 means no scored samples yet (container restart / cold start)
            if float(row["current_stability"] or 0) < 1.0:
                continue
            if await self._in_cooldown("engine_instability_spike", row["engine_name"],
                                       hours=self._cfg.instability_cooldown_hours):
                continue
            severity = "critical" if drop >= self._cfg.instability_drop_thresh * 1.5 else "high"
            await self._fire(
                "engine_instability_spike", severity, row["engine_name"],
                threshold_value=self._cfg.instability_drop_thresh,
                observed_value=round(drop, 2),
                details={
                    "current_stability": round(float(row["current_stability"]), 2),
                    "prior_stability":   round(float(row["prior_stability"]), 2),
                    "stability_drop":    round(drop, 2),
                },
            )
            fired += 1
        return fired

    async def _check_recovery_failure(self) -> int:
        """Alert when an engine has been in 'degraded'/'failing' status for N+ weeks."""
        fired = 0
        threshold_weeks = self._cfg.recovery_fail_weeks
        async with self._db.acquire() as conn:
            rows = await conn.fetch("""
                SELECT engine_name,
                       MIN(computed_at) AS first_degraded,
                       MAX(trust_score) AS max_trust_in_period,
                       COUNT(DISTINCT DATE(computed_at)) AS days_degraded
                FROM engine_trust_scores
                WHERE health_status IN ('degraded', 'failing')
                  AND computed_at >= NOW() - make_interval(weeks => $1)
                GROUP BY engine_name
                HAVING COUNT(DISTINCT DATE_TRUNC('week', computed_at)) >= $1
            """, threshold_weeks)

        for row in rows:
            if await self._in_cooldown("recovery_failure", row["engine_name"]):
                continue
            await self._fire(
                "recovery_failure", "high", row["engine_name"],
                threshold_value=float(threshold_weeks),
                observed_value=float(row["days_degraded"]),
                details={
                    "weeks_degraded": threshold_weeks,
                    "first_degraded_at": row["first_degraded"].isoformat() if row["first_degraded"] else None,
                    "max_trust_in_period": round(float(row["max_trust_in_period"]), 2) if row["max_trust_in_period"] else None,
                },
            )
            fired += 1
        return fired

    async def _check_degradation_acceleration(self) -> int:
        """Alert when degradation velocity worsens by >threshold vs prior week."""
        fired = 0
        async with self._db.acquire() as conn:
            rows = await conn.fetch("""
                WITH recent AS (
                    SELECT DISTINCT ON (engine_name)
                        engine_name, degradation_velocity
                    FROM engine_trust_scores
                    WHERE computed_at >= NOW() - INTERVAL '24 hours'
                    ORDER BY engine_name, computed_at DESC
                ),
                prior AS (
                    SELECT DISTINCT ON (engine_name)
                        engine_name, degradation_velocity
                    FROM engine_trust_scores
                    WHERE computed_at BETWEEN NOW() - INTERVAL '8 days'
                                          AND NOW() - INTERVAL '7 days'
                    ORDER BY engine_name, computed_at DESC
                )
                SELECT r.engine_name,
                       r.degradation_velocity AS current_vel,
                       p.degradation_velocity AS prior_vel,
                       (r.degradation_velocity - p.degradation_velocity) AS accel
                FROM recent r JOIN prior p USING (engine_name)
                WHERE r.degradation_velocity < 0  -- only check decaying engines
            """)

        threshold = -self._cfg.degradation_accel_thresh
        for row in rows:
            accel = float(row["accel"] or 0)
            if accel >= threshold:   # accel is negative = accelerating decay; threshold is -0.03
                continue
            if await self._in_cooldown("degradation_acceleration", row["engine_name"]):
                continue
            severity = "critical" if accel < threshold * 2 else "high"
            await self._fire(
                "degradation_acceleration", severity, row["engine_name"],
                threshold_value=self._cfg.degradation_accel_thresh,
                observed_value=round(abs(accel), 6),
                details={
                    "current_velocity_per_week": round(float(row["current_vel"]), 6),
                    "prior_velocity_per_week":   round(float(row["prior_vel"]), 6),
                    "acceleration_per_week":     round(accel, 6),
                },
            )
            fired += 1
        return fired

    async def _check_narrative_collapse(self) -> int:
        """Alert when a narrative goes from stable (>0.60) to below floor (<0.45) in 2 weeks."""
        fired = 0
        async with self._db.acquire() as conn:
            rows = await conn.fetch("""
                SELECT DISTINCT ON (narrative_name)
                    narrative_name, is_collapsing, persistence_score, degradation_rate
                FROM narrative_stability_scores
                WHERE is_collapsing = TRUE
                  AND computed_at >= NOW() - INTERVAL '48 hours'
                ORDER BY narrative_name, computed_at DESC
            """)

        for row in rows:
            if await self._in_cooldown("narrative_collapse", row["narrative_name"]):
                continue
            await self._fire(
                "narrative_collapse", "critical", row["narrative_name"],
                threshold_value=self._cfg.narrative_collapse_from,
                observed_value=round(float(row["persistence_score"] or 0), 4),
                details={
                    "persistence_score": round(float(row["persistence_score"] or 0), 4),
                    "degradation_rate_per_week": round(float(row["degradation_rate"] or 0), 6),
                    "narrative": row["narrative_name"],
                },
            )
            fired += 1
        return fired

    async def _check_confidence_inconsistency(self) -> int:
        """Alert when high-confidence signals have accuracy < threshold for 3+ weeks."""
        fired = 0
        async with self._db.acquire() as conn:
            rows = await conn.fetch("""
                SELECT
                    sr.engine_name,
                    date_trunc('week', so.evaluated_at) AS wk,
                    AVG(so.direction_correct::int)       AS acc,
                    COUNT(*)                             AS samples
                FROM signal_outcomes so
                JOIN signals_registry sr ON sr.id = so.signal_id
                WHERE sr.confidence_level = 'high'
                  AND so.evaluated_at >= NOW() - INTERVAL '5 weeks'
                  AND so.direction_correct IS NOT NULL
                GROUP BY sr.engine_name, wk
                HAVING COUNT(*) >= 5
                ORDER BY sr.engine_name, wk
            """)

        engine_weeks: Dict[str, List[float]] = {}
        for row in rows:
            engine_weeks.setdefault(row["engine_name"], []).append(float(row["acc"]))

        threshold = self._cfg.confidence_inconsistency_thresh
        for eng, accs in engine_weeks.items():
            if len(accs) < 3:
                continue
            if all(a < threshold for a in accs[-3:]):
                if await self._in_cooldown("confidence_inconsistency", eng):
                    continue
                await self._fire(
                    "confidence_inconsistency", "high", eng,
                    threshold_value=threshold,
                    observed_value=round(accs[-1], 4),
                    details={
                        "recent_high_conf_accuracies": [round(a, 4) for a in accs[-3:]],
                        "threshold": threshold,
                        "consecutive_weeks_below": 3,
                    },
                )
                fired += 1
        return fired

    # ── Alert Resolution ───────────────────────────────────────────────────────

    async def _resolve_cleared_alerts(self) -> None:
        """Mark previously active alerts as resolved if condition has cleared."""
        async with self._db.acquire() as conn:
            # Precision collapse: resolve if precision is no longer collapsed
            await conn.execute("""
                UPDATE validation_alerts
                   SET is_active = FALSE, resolved_at = NOW()
                 WHERE alert_type = 'precision_collapse'
                   AND is_active = TRUE
                   AND NOT EXISTS (
                       WITH cur AS (
                           SELECT engine_name, AVG(precision_score) AS prec
                           FROM replay_metrics
                           WHERE window_hours = 24
                             AND computed_at >= NOW() - INTERVAL '7 days'
                           GROUP BY engine_name
                       ),
                       pri AS (
                           SELECT engine_name, AVG(precision_score) AS prec
                           FROM replay_metrics
                           WHERE window_hours = 24
                             AND computed_at BETWEEN NOW() - INTERVAL '14 days'
                                                 AND NOW() - INTERVAL '7 days'
                           GROUP BY engine_name
                       )
                       SELECT 1 FROM cur c JOIN pri p USING (engine_name)
                       WHERE c.engine_name = validation_alerts.engine_name
                         AND (p.prec - c.prec) >= $1
                   )
            """, self._cfg.precision_drop_high)

            # Stale quote feed: resolve per-ticker once a fresh quote lands
            # (< 90 min old). Without this the alert stays active forever after
            # recovery — it never re-fires, but the active gauge stays stuck.
            await conn.execute("""
                UPDATE validation_alerts a
                   SET is_active = FALSE, resolved_at = NOW()
                 WHERE a.alert_type = 'stale_quote_feed'
                   AND a.is_active = TRUE
                   AND EXISTS (
                       SELECT 1 FROM ticker_quotes q
                        WHERE q.symbol = a.engine_name
                          AND q.ts >= NOW() - INTERVAL '90 minutes'
                   )
            """)

            # Coverage gap: resolve if coverage recovered
            total = await conn.fetchval("SELECT COUNT(*) FROM signals_registry") or 1
            evaluated = await conn.fetchval(
                "SELECT COUNT(DISTINCT signal_id) FROM signal_outcomes"
            ) or 0
            coverage = evaluated / total if total > 0 else 0.0
            if coverage >= self._cfg.coverage_gap_high:
                await conn.execute("""
                    UPDATE validation_alerts
                       SET is_active = FALSE, resolved_at = NOW()
                     WHERE alert_type = 'coverage_gap' AND is_active = TRUE
                """)

            # fp_spike: resolve per-engine when replay_metrics FP rate drops below threshold.
            # Previously used global signal_outcomes FP (different metric than fire condition)
            # which caused alerts to never clear even when the engine recovered.
            try:
                await conn.execute("""
                    UPDATE validation_alerts a
                       SET is_active = FALSE, resolved_at = NOW()
                     WHERE a.alert_type = 'fp_spike'
                       AND a.is_active = TRUE
                       AND NOT EXISTS (
                           SELECT 1 FROM replay_metrics rm
                            WHERE rm.engine_name = a.engine_name
                              AND rm.window_hours = 24
                              AND rm.computed_at >= NOW() - INTERVAL '21 days'
                            GROUP BY rm.engine_name
                           HAVING COUNT(*) >= 3
                              AND AVG(rm.false_positive_rate) >= $1
                       )
                """, self._cfg.fp_rate_thresh)
            except Exception as _e:
                logger.warning("resolve_fp_spike_failed", error=str(_e))

            # engine_inversion: resolve when 7d direction_correct rises above 0.40 again
            try:
                await conn.execute("""
                    UPDATE validation_alerts a
                       SET is_active = FALSE, resolved_at = NOW()
                     WHERE a.alert_type = 'engine_inversion'
                       AND a.is_active = TRUE
                       AND NOT EXISTS (
                           SELECT 1
                             FROM signal_outcomes so
                             JOIN signals_registry sr ON sr.id = so.signal_id
                             LEFT JOIN tickers t ON t.symbol = sr.ticker
                            WHERE sr.engine_name = a.engine_name
                              AND so.direction_correct IS NOT NULL
                              AND so.evaluated_at >= NOW() - INTERVAL '7 days'
                              AND UPPER(COALESCE(t.exchange, '')) = 'CRYPTO'
                           HAVING AVG(so.direction_correct::int) < 0.40
                       )
                """)
            except Exception as _e:
                logger.warning("resolve_engine_inversion_failed", error=str(_e))

            # strategic_instability_alert: resolve when index drops below threshold
            try:
                await conn.execute("""
                    UPDATE validation_alerts
                       SET is_active = FALSE, resolved_at = NOW()
                     WHERE alert_type = 'strategic_instability_alert'
                       AND is_active = TRUE
                       AND NOT EXISTS (
                           SELECT 1 FROM meta_intelligence_snapshots
                            WHERE strategic_instability_index >= $1
                              AND snapshot_at > NOW() - INTERVAL '1 day'
                       )
                """, float(self._cfg.strategic_instability_thresh))
            except Exception as _e:
                logger.warning("resolve_strategic_instability_failed", error=str(_e))

            # recurring_historical_failure / calib_failure: auto-resolve if stale (no re-fire in 14d)
            try:
                await conn.execute("""
                    UPDATE validation_alerts
                       SET is_active = FALSE, resolved_at = NOW()
                     WHERE alert_type IN ('recurring_historical_failure', 'calib_failure', 'calibration_drift')
                       AND is_active = TRUE
                       AND fired_at < NOW() - INTERVAL '14 days'
                       AND NOT EXISTS (
                           SELECT 1 FROM validation_alerts v2
                            WHERE v2.alert_type = validation_alerts.alert_type
                              AND (v2.engine_name IS NOT DISTINCT FROM validation_alerts.engine_name)
                              AND v2.fired_at > NOW() - INTERVAL '14 days'
                       )
                """)
            except Exception as _e:
                logger.warning("resolve_recurring_failure_stale_failed", error=str(_e))

            # engine_instability_spike: resolve when stability drop is no longer detected
            try:
                await conn.execute("""
                    UPDATE validation_alerts
                       SET is_active = FALSE, resolved_at = NOW()
                     WHERE alert_type = 'engine_instability_spike'
                       AND is_active = TRUE
                       AND NOT EXISTS (
                           WITH recent AS (
                               SELECT DISTINCT ON (engine_name)
                                   engine_name, stability_score
                               FROM engine_trust_scores
                               WHERE computed_at >= NOW() - INTERVAL '24 hours'
                               ORDER BY engine_name, computed_at DESC
                           ),
                           prior AS (
                               SELECT DISTINCT ON (engine_name)
                                   engine_name, stability_score
                               FROM engine_trust_scores
                               WHERE computed_at BETWEEN NOW() - INTERVAL '8 days'
                                                     AND NOW() - INTERVAL '7 days'
                               ORDER BY engine_name, computed_at DESC
                           )
                           SELECT 1 FROM recent r
                           JOIN prior p USING (engine_name)
                           WHERE r.engine_name = validation_alerts.engine_name
                             AND (p.stability_score - r.stability_score) >= $1
                       )
                """, float(self._cfg.instability_drop_thresh))
            except Exception as _e:
                logger.warning("resolve_instability_spike_failed", error=str(_e))

            # engine_instability_spike: time-based fallback (48h no re-fire).
            # Instability cooldown is 24h, so 48h silence means condition cleared.
            # infra alerts: longer 7d fallback (less predictable cadence).
            try:
                await conn.execute("""
                    UPDATE validation_alerts
                       SET is_active = FALSE, resolved_at = NOW()
                     WHERE alert_type = 'engine_instability_spike'
                       AND is_active = TRUE
                       AND fired_at < NOW() - INTERVAL '48 hours'
                       AND NOT EXISTS (
                           SELECT 1 FROM validation_alerts v2
                            WHERE v2.alert_type = 'engine_instability_spike'
                              AND v2.fired_at > NOW() - INTERVAL '48 hours'
                       )
                """)
                await conn.execute("""
                    UPDATE validation_alerts
                       SET is_active = FALSE, resolved_at = NOW()
                     WHERE alert_type IN ('infra_api_health', 'infra_containers')
                       AND is_active = TRUE
                       AND fired_at < NOW() - INTERVAL '7 days'
                       AND NOT EXISTS (
                           SELECT 1 FROM validation_alerts v2
                            WHERE v2.alert_type = validation_alerts.alert_type
                              AND v2.fired_at > NOW() - INTERVAL '7 days'
                       )
                """)
            except Exception as _e:
                logger.warning("resolve_infra_stale_failed", error=str(_e))

    # ── Active Gauge Update ────────────────────────────────────────────────────

    async def _update_active_gauges(self) -> None:
        async with self._db.acquire() as conn:
            rows = await conn.fetch("""
                SELECT alert_type, COUNT(*) AS cnt
                FROM validation_alerts
                WHERE is_active = TRUE
                GROUP BY alert_type
            """)
        all_types = [
            "precision_collapse", "calibration_drift", "fp_spike",
            "replay_failure", "narrative_degradation", "coverage_gap",
        ]
        counts = {r["alert_type"]: int(r["cnt"]) for r in rows}
        for at in all_types:
            alerts_active_gauge.labels(alert_type=at).set(counts.get(at, 0))

    # ── Cooldown Check ─────────────────────────────────────────────────────────

    async def _in_cooldown(
        self,
        alert_type: str,
        engine_name: Optional[str],
        hours: Optional[float] = None,
    ) -> bool:
        """Return True if a matching alert fired within the cooldown window."""
        # Check Redis suppress key first
        if self._redis:
            try:
                suppress_key = f"alert:suppress:{alert_type}:{engine_name or 'system'}"
                if await self._redis.exists(suppress_key):
                    return True
            except Exception:
                pass

        cooldown_cutoff = datetime.now(timezone.utc) - timedelta(
            hours=hours if hours is not None else self._cfg.cooldown_hours
        )
        async with self._db.acquire() as conn:
            if engine_name is None:
                existing = await conn.fetchval("""
                    SELECT id FROM validation_alerts
                    WHERE alert_type = $1
                      AND engine_name IS NULL
                      AND fired_at >= $2
                    LIMIT 1
                """, alert_type, cooldown_cutoff)
            else:
                existing = await conn.fetchval("""
                    SELECT id FROM validation_alerts
                    WHERE alert_type = $1
                      AND engine_name = $2
                      AND fired_at >= $3
                    LIMIT 1
                """, alert_type, engine_name, cooldown_cutoff)
        return existing is not None

    # ── Fire Alert ─────────────────────────────────────────────────────────────

    async def _fire(
        self,
        alert_type: str,
        severity: str,
        engine_name: Optional[str],
        threshold_value: Optional[float],
        observed_value: Optional[float],
        details: dict,
    ) -> None:
        async with self._db.acquire() as conn:
            # Dedup: resolve all previous active alerts for same type+engine before
            # inserting the new one. Prevents unbounded accumulation of stale rows
            # in the dashboard — only the latest firing is ever active.
            if engine_name is not None:
                await conn.execute("""
                    UPDATE validation_alerts
                       SET is_active = FALSE, resolved_at = NOW()
                     WHERE alert_type = $1 AND engine_name = $2 AND is_active = TRUE
                """, alert_type, engine_name)
            else:
                await conn.execute("""
                    UPDATE validation_alerts
                       SET is_active = FALSE, resolved_at = NOW()
                     WHERE alert_type = $1 AND engine_name IS NULL AND is_active = TRUE
                """, alert_type)
            await conn.execute("""
                INSERT INTO validation_alerts
                    (alert_type, severity, engine_name, details,
                     threshold_value, observed_value, fired_at, is_active)
                VALUES ($1, $2, $3, $4, $5, $6, NOW(), TRUE)
            """, alert_type, severity, engine_name,
                json.dumps(details), threshold_value, observed_value)

        alerts_fired_total.labels(alert_type=alert_type, severity=severity).inc()

        await self._send_discord(alert_type, severity, engine_name, details,
                                  threshold_value, observed_value)
        logger.warning(
            "alert_fired",
            alert_type=alert_type,
            severity=severity,
            engine=engine_name,
            observed=observed_value,
        )

    # ── Discord Delivery ───────────────────────────────────────────────────────

    async def _send_discord(
        self,
        alert_type: str,
        severity: str,
        engine_name: Optional[str],
        details: dict,
        threshold_value: Optional[float],
        observed_value: Optional[float],
    ) -> None:
        import os as _os
        webhook = _os.getenv("DISCORD_WEBHOOK_ALERTS", "")
        if not webhook or not _AIOHTTP_AVAILABLE:
            return
        from src.core.discord import COLOR_LOSS, COLOR_WARN, COLOR_INFO
        _SCOLORS = {"critical": COLOR_LOSS, "high": COLOR_WARN, "medium": COLOR_INFO}
        _SEMOJI  = {"critical": "🚨", "high": "⚠️", "medium": "🟡"}
        color = _SCOLORS.get(severity, COLOR_WARN)
        emoji = _SEMOJI.get(severity, "ℹ️")
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

        lines: list[str] = []
        if engine_name:
            lines.append(f"**Engine:** `{engine_name}`")
        if observed_value is not None and threshold_value is not None:
            lines.append(f"**Observed:** {observed_value:.4f} | **Threshold:** {threshold_value:.4f}")
        for k, v in details.items():
            if k not in ("current_precision", "prior_precision"):
                lines.append(f"**{k.replace('_', ' ').title()}:** {v}")
        lines.append(f"`{ts}`")
        lines.append("*System health alert — not financial advice*")

        payload = {"embeds": [{
            "title": f"{emoji} {severity.upper()} | {alert_type}",
            "description": "\n".join(lines),
            "color": color,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }]}
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(webhook, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    if resp.status not in (200, 204):
                        body = await resp.text()
                        logger.error("discord_send_failed", status=resp.status, body=body[:200])
        except Exception as e:
            logger.error("discord_send_error", error=str(e))


# ─── Formatting ───────────────────────────────────────────────────────────────

_SEVERITY_EMOJI = {"critical": "🚨", "high": "⚠️", "medium": "🟡"}


def _format_telegram_message(
    alert_type: str,
    severity: str,
    engine_name: Optional[str],
    details: dict,
    threshold_value: Optional[float],
    observed_value: Optional[float],
) -> str:
    emoji = _SEVERITY_EMOJI.get(severity, "ℹ️")
    lines = [
        f"{emoji} <b>{severity.upper()} | {alert_type}</b>",
    ]
    if engine_name:
        lines.append(f"Engine: <code>{engine_name}</code>")
    if observed_value is not None and threshold_value is not None:
        lines.append(f"Observed: {observed_value:.4f} | Threshold: {threshold_value:.4f}")
    for k, v in details.items():
        if k not in ("current_precision", "prior_precision"):
            lines.append(f"{k.replace('_', ' ').title()}: {v}")
    lines.append(f"Time: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
    lines.append("<i>Not financial advice — system health alert only</i>")
    return "\n".join(lines)


# ─── Standalone Entry Point ───────────────────────────────────────────────────

async def _main() -> None:
    from src.core.logging import configure_logging
    from src.db.connection import get_db_pool
    from src.cache.redis_client import get_redis

    configure_logging()
    cfg = AlertConfig()
    _http.start(cfg.http_port, "validation-alert")
    logger.info("validation_alert_engine_starting", port=cfg.http_port,
                interval=cfg.check_interval_s)

    db    = await get_db_pool()
    redis = await get_redis()
    _http.set_ready("validation-alert")
    engine = ValidationAlertEngine(db=db, redis=redis, config=cfg)

    while True:
        fired = await engine.run_cycle()
        if fired:
            logger.info("alert_cycle_done", fired=fired)
        await asyncio.sleep(cfg.check_interval_s)


if __name__ == "__main__":
    asyncio.run(_main())
