# Observability Guide

End-to-end reference for the market intelligence pipeline: schemas, lifecycle,
and a step-by-step debugging workflow for tracing a signal from ingestion to
evaluation.

---

## Pipeline Overview

```
Provider API / RSS
      │
      ▼
provider_ingestion_log    ← Did the fetch happen?  How many items?
      │
      ▼  (NewsIntelligenceEngine, per cycle_id)
signal_trace              ← Did an engine emit a signal?
      │
      ▼  (SignalEvaluationEngine, runs every 15 min)
signal_feedback           ← Was the signal correct?  win / loss / neutral
      │
      ▼  (FeedbackAggregator, runs every hour)
engine_performance_metrics     ← Engine win rate and avg PnL (daily)
provider_performance_metrics   ← Provider quality and noise ratio (daily)
```

Every row in `signal_trace` carries the same `cycle_id` as the
`provider_ingestion_log` row that triggered it.  This single key links the
entire pipeline end-to-end.

---

## Table Schemas

### `provider_ingestion_log`

Written by `NewsIntelligenceEngine` after every provider fetch cycle.

| Column       | Type        | Description |
|--------------|-------------|-------------|
| `id`         | bigserial   | PK |
| `cycle_id`   | text        | UUID linking this row to all downstream signal_trace rows |
| `job`        | text        | Scheduler job name (`news_cycle`) |
| `provider`   | text        | Provider name (`bloomberg`, `ft`, `newsapi`, …) |
| `fetched`    | int         | Total articles returned by the provider |
| `new_count`  | int         | Articles that passed deduplication and were stored |
| `dup_exact`  | int         | SHA-256 exact duplicates dropped |
| `dup_fuzzy`  | int         | Fuzzy-match duplicates dropped |
| `dup_semantic` | int       | Semantic-similarity duplicates dropped |
| `errors`     | int         | Fetch or parse errors for this provider this cycle |
| `latency_ms` | float       | End-to-end fetch latency in milliseconds |
| `status`     | text        | `ok` \| `failed` \| `circuit_open` |
| `ts`         | timestamptz | When this row was written |

**Indexes:** `cycle_id`, `(provider, ts DESC)`, `ts DESC`

---

### `signal_trace`

Append-only audit log — one row per (signal, cycle).  Written by
`SignalTracer.record()` which every engine calls after emitting a signal.
Never modified after insert.

| Column            | Type        | Description |
|-------------------|-------------|-------------|
| `id`              | uuid        | PK |
| `cycle_id`        | text        | Links back to `provider_ingestion_log.cycle_id` |
| `news_id`         | text        | Source news item, if applicable |
| `provider`        | text        | Data provider that fed this signal |
| `engine_name`     | text        | Engine class that produced the signal |
| `signal_id`       | text        | FK-like reference to `signals_registry.id` (integer as text) |
| `signal_type`     | text        | `trend` \| `macro` \| `confluence` \| `scoring` |
| `signal_strength` | float       | Normalised strength 0–100 |
| `signal_direction`| text        | `risk_on` \| `risk_off` \| `bullish` \| `bearish` \| `mixed` |
| `input_data_hash` | text        | SHA-256 of engine input snapshot (for dedup audit) |
| `output_data_hash`| text        | SHA-256 of engine output snapshot |
| `latency_ms`      | float       | Engine compute time for this signal |
| `created_at`      | timestamptz | Signal emission time |

**Indexes:** `cycle_id`, `signal_id`, `(provider, created_at DESC)`,
`(engine_name, created_at DESC)`, `created_at DESC`

---

### `signal_feedback`

One row per `(signal_trace_id, evaluation_window_h)`.  Written by
`SignalEvaluationEngine` after the evaluation window has elapsed.
UNIQUE constraint prevents duplicates — safe to re-run.

| Column                 | Type        | Description |
|------------------------|-------------|-------------|
| `id`                   | uuid        | PK |
| `signal_trace_id`      | uuid        | References `signal_trace.id` |
| `cycle_id`             | text        | Propagated from signal_trace |
| `engine_name`          | text        | Engine that produced the original signal |
| `provider`             | text        | Provider associated with the signal |
| `signal_type`          | text        | `trend` \| `macro` \| `confluence` |
| `outcome_type`         | text        | `win` \| `loss` \| `neutral` \| `pending` |
| `pnl_score`            | float       | Proxy PnL: `+strength/100` (win), `-strength/200` (loss), `0` (neutral) |
| `direction_correct`    | boolean     | Was the predicted direction correct? |
| `confidence_score`     | float       | `signal_strength` at trace time (0–100) |
| `evaluation_window_h`  | int         | `1`, `4`, or `24` hours |
| `market_context`       | jsonb       | Ground-truth data used for evaluation |
| `evaluated_at`         | timestamptz | When this outcome was written |

**Evaluation logic by signal type:**
- `trend` — looks up `signals_registry` via `signal_id` for ticker + composite_score; win if composite_score delta > 0.02 in predicted direction
- `macro` — win if `macro_regimes.regime` still matches predicted regime at T + window_h
- `confluence` — win if `confluence_signals.direction` still matches at T + window_h
- `scoring` — always excluded (meta-signal, not directional)

---

### `engine_performance_metrics`

Daily aggregate per engine.  Upserted by `FeedbackAggregator` every hour.
Extends the base table from migration 004 with feedback-loop columns.

| Column                  | Type   | Description |
|-------------------------|--------|-------------|
| `engine_name`           | text   | Engine identifier |
| `metric_date`           | date   | Aggregation date |
| `total_signals`         | int    | Signals attributed to this engine on this date |
| `accuracy_4h`           | float  | Directional accuracy at 4h window (backtest engine) |
| `accuracy_24h`          | float  | Directional accuracy at 24h window |
| `accuracy_72h`          | float  | Directional accuracy at 72h window |
| `avg_confidence`        | float  | Mean confidence_score for the day |
| `win_rate_1h`           | float  | Feedback win rate at 1h window (0–1) |
| `win_rate_24h`          | float  | Feedback win rate at 24h window |
| `win_rate_7d`           | float  | Rolling 7d average win rate |
| `avg_pnl_score`         | float  | Average proxy PnL score |
| `feedback_signals_total`| int    | Total signals with completed feedback |

---

### `provider_performance_metrics`

Daily quality rollup per provider.  Upserted by `FeedbackAggregator`.

| Column          | Type   | Description |
|-----------------|--------|-------------|
| `provider`      | text   | Provider name |
| `metric_date`   | date   | Aggregation date |
| `total_signals` | int    | Signals attributed to this provider |
| `win_count`     | int    | Signals with `outcome_type = 'win'` |
| `loss_count`    | int    | Signals with `outcome_type = 'loss'` |
| `neutral_count` | int    | Neutral outcomes |
| `win_rate`      | float  | `win_count / (win_count + loss_count)` |
| `avg_pnl_score` | float  | Average proxy PnL |
| `noise_ratio`   | float  | `(dup_exact + dup_fuzzy) / fetched` — lower is better |
| `avg_latency_ms`| float  | Average fetch latency from `provider_ingestion_log` |

---

## Signal Lifecycle (end-to-end)

```
1. Scheduler triggers news_cycle (every 5 min)
   → NewsIntelligenceEngine.run_cycle(cycle_id=<uuid>)
   → Provider fetches articles
   → Writes provider_ingestion_log (cycle_id, provider, fetched, …)

2. TrendDetectionEngine, MacroRegimeEngine, InstitutionalConfluenceEngine run
   → Each engine calls SignalTracer.record(cycle_id=…, signal_id=…, …)
   → Writes signal_trace row

3. SignalEvaluationEngine runs every 15 min (and via signal_evaluation_cycle job)
   → Queries signal_trace LEFT JOIN signal_feedback (finds unevaluated rows)
   → For each trace older than evaluation window:
       trend:      looks up signals_registry.composite_score delta
       macro:      compares macro_regimes.regime persistence
       confluence: compares confluence_signals.direction persistence
   → Writes signal_feedback row (outcome_type, pnl_score, direction_correct)

4. FeedbackAggregator runs every hour
   → Aggregates signal_feedback by engine → upserts engine_performance_metrics
   → Joins signal_feedback + provider_ingestion_log → upserts provider_performance_metrics
```

---

## Debugging a Signal: Step-by-Step

### Step 1 — Find the ingestion event

```sql
-- Was there recent provider activity?
SELECT provider, MAX(ts) AS last_seen, SUM(fetched) AS fetched, SUM(errors) AS errors
FROM provider_ingestion_log
WHERE ts > now() - interval '2 hours'
GROUP BY provider ORDER BY last_seen DESC;
```

If 0 rows: the scheduler is not running or all providers are failing.
Check `docker compose logs worker`.

### Step 2 — Find the cycle_id

```sql
-- Most recent cycle
SELECT cycle_id, job, ts, provider, fetched, status
FROM provider_ingestion_log
ORDER BY ts DESC
LIMIT 5;
```

Copy the `cycle_id` for the cycle you want to trace.

### Step 3 — Find signals emitted in that cycle

```sql
SELECT id, engine_name, signal_type, signal_direction, signal_strength,
       signal_id, created_at
FROM signal_trace
WHERE cycle_id = '<your-cycle-id>'
ORDER BY created_at;
```

If 0 rows: signals were not traced.  Check the engine's call to
`SignalTracer.record()` and look for `signal_trace_write_error` in logs.

### Step 4 — Check evaluation status

```sql
-- Find evaluations for a specific signal_trace row
SELECT sf.outcome_type, sf.pnl_score, sf.direction_correct,
       sf.evaluation_window_h, sf.evaluated_at, sf.market_context
FROM signal_feedback sf
WHERE sf.signal_trace_id = '<signal-trace-uuid>'
ORDER BY sf.evaluation_window_h;
```

Expected: one row per evaluation window (1h, 4h, 24h) once enough time has passed.

If `outcome_type = 'pending'` after 25+ hours: the evaluation engine is stalled.
Check `docker compose logs signal-evaluator` or the `signal_evaluation_cycle` job
in the scheduler worker logs.

If 0 rows at all: the evaluation engine hasn't reached this signal yet, or the
signal has `signal_type = 'scoring'` (excluded by design).

### Step 5 — Check engine scores

```sql
-- Latest scores for all engines
SELECT DISTINCT ON (engine_name)
    engine_name, metric_date,
    ROUND(win_rate_24h * 100, 1) AS win_rate_24h_pct,
    avg_pnl_score,
    feedback_signals_total
FROM engine_performance_metrics
WHERE win_rate_24h IS NOT NULL
ORDER BY engine_name, metric_date DESC;
```

If engine is absent: FeedbackAggregator hasn't run yet, or signal_feedback has
no completed rows for that engine.  Run `make verify` for a full pipeline report.

### Step 6 — Check provider scores

```sql
-- Provider quality ranking
SELECT DISTINCT ON (provider)
    provider, metric_date,
    ROUND(win_rate * 100, 1)     AS win_rate_pct,
    ROUND(noise_ratio * 100, 1)  AS noise_pct,
    avg_latency_ms
FROM provider_performance_metrics
ORDER BY provider, metric_date DESC;
```

High `noise_pct` (> 40%) means a provider is sending many duplicate articles.
The deduplication layer handles this, but it degrades signal quality upstream.

---

## Quick Reference — Verification Queries

Run any of the SQL files in `sql/verification/` directly with psql:

```bash
# Requires PGPASSWORD set or ~/.pgpass configured
psql -h localhost -U mktintel -d mktintel -f sql/verification/01_provider_activity.sql
psql -h localhost -U mktintel -d mktintel -f sql/verification/02_signal_generation.sql
psql -h localhost -U mktintel -d mktintel -f sql/verification/03_signal_evaluation.sql
psql -h localhost -U mktintel -d mktintel -f sql/verification/04_feedback_metrics.sql
psql -h localhost -U mktintel -d mktintel -f sql/verification/05_pipeline_integrity.sql
```

Or run the full pipeline report:

```bash
make verify
```

The report exits 0 (PASS) if providers are active and signals are flowing, or 1
(FAIL) if the pipeline is broken.  Use it in CI after deployment to verify that
the stack is live and end-to-end data flow is working.

---

## Grafana Dashboards

| Dashboard                    | UID                         | What it shows |
|------------------------------|-----------------------------|---------------|
| Signal Feedback Loop         | `mktintel-signal-feedback`  | Outcome distribution, engine win rates, provider quality table, signal type comparison |
| Engine Performance           | `mktintel-engine-performance` | Signal volume, archiver throughput, backtest health |
| Provider (custom panels)     | via PostgreSQL datasource   | See `provider_performance_metrics` table |

Grafana is exposed at `http://localhost:3001`.

---

## Ports Reference

| Service             | Port  | Health endpoint |
|---------------------|-------|-----------------|
| API (main)          | 8000  | `/health` |
| Validation API      | 8010  | `/health` |
| Backtest worker     | 8011  | `/health` |
| Signal archiver     | 8012  | `/health` |
| Outcome tracker     | 8013  | `/health` |
| Signal scorer       | 8014  | `/health` |
| Replay scheduler    | 8015  | `/health` |
| Validation alert    | 8016  | `/health` |
| Governance engine   | 8017  | `/health` |
| Meta-intelligence   | 8018  | `/health` |
| Signal evaluator    | 8019  | `/health` |
| Feedback aggregator | 8020  | `/health` |
| Scheduler worker    | 8009  | `/health` |
| Prometheus          | 9091  | `/-/healthy` |
| Grafana             | 3001  | `/api/health` |
