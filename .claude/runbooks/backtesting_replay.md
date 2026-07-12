# Runbook: Backtesting & Replay System

**Module:** Phase 2 — Orchestration + Replay  
**Version:** v0.6.0  
**Owner:** Quantitative Systems Engineering  

---

## Purpose

The Replay System evaluates historical signal performance **deterministically** over
configurable time windows (weekly / monthly / yearly / custom). It reads exclusively from
pre-computed, immutable data: `signals_registry` (archived signals) and `signal_outcomes`
(ground-truth outcomes). No signal recomputation occurs.

---

## Components

| Component | File | Port | Role |
|-----------|------|------|------|
| BacktestingEngine | `src/engines/backtesting_engine.py` | — | Computes replay metrics from stored data |
| ReplayScheduler | `src/engines/replay_scheduler.py` | 8015 | Redis-backed job queue + worker |
| EnginePerformanceDashboard | `src/engines/engine_performance_dashboard.py` | — | Aggregated analytics read layer |
| Backtest Analytics API | `src/api/routes/backtest_analytics.py` | 8010 | Read-only REST endpoints |

### New Tables (migration 006)
- `replay_runs` — replay job metadata (status, time range, window_type)
- `replay_metrics` — per-engine/type/window metrics with precision, recall, FPR, F1, regime/narrative breakdown

### Redis Keys
- `replay:queue` — LPUSH/BRPOP job queue (list)
- `replay:processing:{run_id}` — in-flight guard (string, TTL = 3600s)

---

## Running a Manual Replay

### Via API
```bash
curl -X POST "http://localhost:8010/api/v1/backtest/replay/trigger" \
  -H "Content-Type: application/json" \
  -d '{
    "window_type": "monthly",
    "period_start": "2026-04-01T00:00:00Z",
    "period_end":   "2026-04-30T23:59:59Z",
    "engine_name": null
  }'
```
Returns: `{ "run_id": "...", "status": "queued" }`

### Via Redis (direct push)
```bash
redis-cli LPUSH replay:queue '{"window_type":"weekly","period_start":"2026-05-20T00:00:00Z","period_end":"2026-05-26T23:59:59Z","engine_name":null,"retry_count":0}'
```

### Via Python (embedded)
```python
from src.engines.backtesting_engine import BacktestingEngine
engine = BacktestingEngine(db_pool)
run_id = await engine.create_run("custom", period_start, period_end)
await engine.execute_run(run_id)
```

---

## Monitoring a Replay Run

```bash
# Poll status
curl "http://localhost:8010/api/v1/backtest/replay/runs/{run_id}"

# List all runs
curl "http://localhost:8010/api/v1/backtest/replay/runs"

# Check queue depth
redis-cli LLEN replay:queue
```

---

## Scheduling Auto-Replays

The `replay-scheduler` service auto-enqueues replays on startup and periodically:
- **Weekly replay**: enqueued every Monday 02:00 UTC for the prior 7 days
- **Monthly replay**: enqueued every 1st of month 03:00 UTC for the prior month

To disable auto-scheduling:
```env
REPLAY_AUTO_SCHEDULE=false
```

---

## Validation Steps

After a successful replay run:

1. **Verify run completed**
   ```bash
   curl "http://localhost:8010/api/v1/backtest/replay/runs/{run_id}" \
     | python3 -m json.tool | grep '"status"'
   # Expected: "completed"
   ```

2. **Verify metrics written**
   ```bash
   psql $DATABASE_URL -c "
     SELECT engine_name, signal_type, window_hours,
            precision_score, recall_score, directional_accuracy
       FROM replay_metrics
      WHERE run_id = '{run_id}'
      ORDER BY engine_name, window_hours;"
   ```

3. **Verify engine ranking responds**
   ```bash
   curl "http://localhost:8010/api/v1/backtest/engines?days=30"
   ```

4. **Verify regime breakdown populated**
   ```bash
   curl "http://localhost:8010/api/v1/backtest/regimes?days=30"
   ```

5. **Verify Prometheus metrics visible**
   ```bash
   curl -s "http://localhost:8015/metrics" | grep replay_
   ```

---

## Rollback Steps

### Rollback a specific run
```sql
-- Delete metrics for the run, reset status
DELETE FROM replay_metrics WHERE run_id = '{run_id}';
UPDATE replay_runs SET status = 'queued', started_at = NULL, completed_at = NULL
 WHERE run_id = '{run_id}';
-- Then re-enqueue
```

### Rollback migration 006
```sql
DROP TABLE IF EXISTS replay_metrics CASCADE;
DROP TABLE IF EXISTS replay_runs CASCADE;
DELETE FROM schema_migrations WHERE version = '006_replay_system';
```

### Rollback Docker services
```bash
docker-compose stop replay-scheduler
docker-compose rm -f replay-scheduler
```

---

## Troubleshooting

| Symptom | Likely Cause | Fix |
|---------|-------------|-----|
| run stays `queued` | replay-scheduler stopped | `docker-compose restart replay-scheduler` |
| run stays `running` | scheduler crashed mid-run | Check Redis `replay:processing:{run_id}`, delete key, reset DB status |
| `evaluated_signals = 0` | No outcomes for the period | OutcomeTrackingEngine may not have run yet; check `signal_outcomes` table |
| metrics all NULL | Sample size < 20 | Normal; wait for more outcome data to accumulate |
| API returns 503 | validation-api unhealthy | `docker-compose restart validation-api` |

---

## Retry Behaviour

Failed jobs are re-queued automatically up to `MAX_RETRIES = 3` times with a 60s delay.
After 3 failures the run is marked `failed` and must be manually re-triggered.

---

## State and Changelog Update Instructions

After any change to this system:

1. Update `.claude/state.md` — bump version, add to Completed Modules
2. Append to `.claude/changelog.md` — list Added/Changed/Architecture Notes
3. Update `.claude/architecture.md` — add service to topology diagram
4. Append to `.claude/decisions.md` — add DEC-0XX for any non-obvious choices

---

## Related Runbooks

- `deploy_production.md` — deploying all services
- `debug_pipeline.md` — debugging signal ingestion
