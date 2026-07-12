"""Prometheus metrics definitions shared across Level 4 services."""
from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram

# ─── Signal Archiver ──────────────────────────────────────────────────────────
signals_archived_total = Counter(
    "signals_archived_total",
    "Total signals archived to signals_registry",
    ["source_table"],
)
archiver_cycle_seconds = Histogram(
    "archiver_cycle_seconds",
    "Duration of each archiver poll cycle",
    buckets=[0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0],
)
archiver_lag_seconds = Gauge(
    "archiver_lag_seconds",
    "Seconds since last successful archiver cycle",
)
archiver_errors_total = Counter(
    "archiver_errors_total",
    "Errors during archiver poll cycles",
    ["source_table"],
)
archiver_watermark_id = Gauge(
    "archiver_watermark_id",
    "Last archived row id per source table",
    ["source_table"],
)
archiver_jobs_total = Counter(
    "archiver_jobs_total",
    "Total archiver table-poll jobs completed (including idle polls)",
    ["source_table"],
)
archiver_jobs_failed_total = Counter(
    "archiver_jobs_failed_total",
    "Total archiver table-poll jobs that raised an exception",
    ["source_table"],
)

# ─── Backtest Worker ──────────────────────────────────────────────────────────
backtest_cycles_total = Counter(
    "backtest_cycles_total",
    "Total backtest evaluation cycles completed",
)
signals_evaluated_total = Counter(
    "signals_evaluated_total",
    "Signals evaluated for outcome accuracy",
    ["engine", "window_hours"],
)
engine_accuracy_gauge = Gauge(
    "engine_accuracy",
    "Directional accuracy per engine and evaluation window",
    ["engine", "window_hours"],
)
backtest_cycle_seconds = Histogram(
    "backtest_cycle_seconds",
    "Duration of each backtest evaluation cycle",
    buckets=[1.0, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0],
)
backtest_errors_total = Counter(
    "backtest_errors_total",
    "Errors during backtest evaluation cycles",
)

# ─── Validation API ───────────────────────────────────────────────────────────
validation_requests_total = Counter(
    "validation_api_requests_total",
    "Validation API HTTP requests",
    ["endpoint", "status_code"],
)
validation_request_seconds = Histogram(
    "validation_api_request_seconds",
    "Validation API request latency",
    ["endpoint"],
    buckets=[0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0],
)
signals_in_registry = Gauge(
    "signals_registry_total",
    "Total signals currently in signals_registry",
)

# ─── Signal Truth Layer ───────────────────────────────────────────────────────
outcome_tracker_cycles_total = Counter(
    "outcome_tracker_cycles_total",
    "Total outcome tracking cycles completed",
)
outcomes_written_total = Counter(
    "outcomes_written_total",
    "Outcome records written to signal_outcomes",
    ["window_hours"],
)
outcomes_skipped_total = Counter(
    "outcomes_skipped_total",
    "Signals skipped (no ground truth data yet)",
    ["window_hours"],
)
outcome_tracker_errors_total = Counter(
    "outcome_tracker_errors_total",
    "Errors in outcome tracker cycles",
)
scorer_cycles_total = Counter(
    "scorer_cycles_total",
    "Total signal scoring cycles completed",
)
predictive_strength_gauge = Gauge(
    "signal_predictive_strength",
    "Predictive strength score 0–100 per engine and window",
    ["engine", "window_hours"],
)
half_life_gauge = Gauge(
    "signal_half_life_hours",
    "Signal half-life in hours per engine (NaN = beyond 7d)",
    ["engine"],
)
scorer_errors_total = Counter(
    "scorer_errors_total",
    "Errors in signal scoring cycles",
)

# ─── Validation Alert Engine (Phase 3) ───────────────────────────────────────
# Note: alert engine defines its own Counter/Gauge/Histogram at module level
# to avoid import cycles. These are shared metrics exposed via validation-api.
validation_active_alerts_gauge = Gauge(
    "validation_active_alerts_total",
    "Total active (unresolved) validation alerts across all types",
)
validation_system_precision_gauge = Gauge(
    "validation_system_precision",
    "Rolling 30-day average precision across all engines at 24h window",
)
validation_coverage_pct_gauge = Gauge(
    "validation_outcome_coverage_pct",
    "Fraction of archived signals that have at least one outcome record",
)
