"""Prometheus metrics for the main API and scheduler worker."""
from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram

# ── HTTP request instrumentation (main API) ────────────────────────────────────
api_requests_total = Counter(
    "api_requests_total",
    "Total HTTP requests to the main API",
    ["method", "path", "status_code"],
)
api_request_seconds = Histogram(
    "api_request_seconds",
    "Main API request latency",
    ["method", "path"],
    buckets=[0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5],
)

# ── Scheduler job metrics (worker) ────────────────────────────────────────────
job_duration_seconds = Histogram(
    "worker_job_duration_seconds",
    "Duration of each scheduler job cycle",
    ["job"],
    buckets=[0.5, 1.0, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0],
)
job_errors_total = Counter(
    "worker_job_errors_total",
    "Scheduler job failures",
    ["job"],
)
job_runs_total = Counter(
    "worker_job_runs_total",
    "Scheduler job executions",
    ["job"],
)

# ── Ingestion deduplication metrics ───────────────────────────────────────────
ingestion_fetched_total = Counter(
    "ingestion_fetched_total",
    "Articles/posts fetched from providers",
    ["job"],
)
ingestion_new_total = Counter(
    "ingestion_new_total",
    "New items written to the database after dedup",
    ["job"],
)
ingestion_duplicates_total = Counter(
    "ingestion_duplicates_total",
    "Items dropped as duplicates (exact + fuzzy + semantic)",
    ["job", "dedup_type"],
)

# ── Provider latency (Prometheus-tracked, complements structured logs) ─────────
provider_latency_seconds = Histogram(
    "provider_latency_seconds",
    "External provider fetch latency",
    ["provider", "operation", "status"],
    buckets=[0.1, 0.25, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0],
)
provider_errors_total = Counter(
    "provider_errors_total",
    "External provider fetch errors",
    ["provider", "operation"],
)
provider_circuit_open_total = Counter(
    "provider_circuit_open_total",
    "Requests blocked by open circuit breaker",
    ["provider"],
)

# ── DB pool health ─────────────────────────────────────────────────────────────
db_pool_size = Gauge(
    "db_pool_size",
    "Current asyncpg connection pool size",
)
db_pool_free = Gauge(
    "db_pool_free_connections",
    "Free (idle) connections in the asyncpg pool",
)
