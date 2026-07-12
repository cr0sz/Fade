# Runbook: Validation Alert Engine

**Module:** Phase 3 — Validation Intelligence Platform  
**Version:** v0.7.0  
**Owner:** Quantitative Systems Engineering  

---

## Purpose

The Validation Alert Engine continuously monitors the quality of the signal validation
system and fires alerts when degradation is detected. It is the last line of defense
against **silent accuracy decay** — situations where engines keep producing signals but
their predictive quality has collapsed.

**Alerts do NOT predict market movements. They predict system health.**

---

## Components

| Component | File | Port | Role |
|-----------|------|------|------|
| ValidationAlertEngine | `src/engines/validation_alert_engine.py` | 8016 | Alert detection + Telegram delivery |
| ValidationIntelligenceAPI | `src/engines/validation_intelligence_api.py` | — | Aggregate analytics data layer |
| ReplayObservabilityDashboard | `src/engines/replay_observability_dashboard.py` | — | Replay health + decay analytics |
| Validation Intelligence Routes | `src/api/routes/validation_intelligence.py` | 8010 | 5 new read-only endpoints |

### New Table (migration 007)
- `validation_alerts` — alert history, severity, observed vs threshold values, resolution state

---

## Alert Types

| Alert | Trigger | Severity | Cooldown |
|-------|---------|----------|---------|
| `precision_collapse` | Engine precision drops >20% week-over-week | HIGH | 4h |
| `precision_collapse` | Engine precision drops >30% week-over-week | CRITICAL | 4h |
| `calibration_drift` | `calibration_error > 0.20` for any engine | HIGH | 4h |
| `fp_spike` | `false_positive_rate > 0.40` over last 3 runs | HIGH | 4h |
| `replay_failure` | >2 failed replay runs in 24h | HIGH | 2h |
| `replay_failure` | >5 failed replay runs in 24h | CRITICAL | 2h |
| `narrative_degradation` | Narrative accuracy <0.50 for 2+ consecutive weeks | MEDIUM | 8h |
| `coverage_gap` | Outcome coverage <40% of archived signals | HIGH | 4h |
| `coverage_gap` | Outcome coverage <20% of archived signals | CRITICAL | 4h |

---

## Configuration

```env
ALERT_CHECK_INTERVAL=900            # Check every 15 minutes
ALERT_COOLDOWN_HOURS=4              # Default per-alert cooldown
ALERT_PRECISION_DROP_HIGH=0.20      # 20% drop triggers HIGH
ALERT_PRECISION_DROP_CRITICAL=0.30  # 30% drop triggers CRITICAL
ALERT_CALIBRATION_DRIFT_THRESHOLD=0.20
ALERT_FP_RATE_THRESHOLD=0.40
ALERT_REPLAY_FAILURE_HIGH=2         # >2 failures/24h = HIGH
ALERT_REPLAY_FAILURE_CRITICAL=5     # >5 failures/24h = CRITICAL
ALERT_COVERAGE_GAP_HIGH=0.40
ALERT_COVERAGE_GAP_CRITICAL=0.20
ALERT_HTTP_PORT=8016
TELEGRAM_BOT_TOKEN=<existing>       # Reuses existing bot token
TELEGRAM_ADMIN_CHAT_ID=<existing>   # Reuses existing admin chat
```

---

## Telegram Alert Format

```
🚨 CRITICAL | precision_collapse
Engine: macro_regime_engine
Observed: 0.38 precision (was 0.71 last week)
Drop: -46.5% | Threshold: -30%
Time: 2026-05-29 14:23:11 UTC

⚠️ HIGH | calibration_drift
Engine: narrative_engine
Calibration Error: 0.24 (threshold: 0.20)
Time: 2026-05-29 14:23:11 UTC
```

---

## Checking Alert State

```bash
# List active alerts
psql $DATABASE_URL -c "
  SELECT alert_type, severity, engine_name, observed_value, fired_at
  FROM validation_alerts
  WHERE is_active = TRUE
  ORDER BY fired_at DESC;"

# List all alerts in last 24h
psql $DATABASE_URL -c "
  SELECT alert_type, severity, engine_name, fired_at
  FROM validation_alerts
  WHERE fired_at > NOW() - INTERVAL '24 hours'
  ORDER BY fired_at DESC;"

# Alert engine health
curl -s "http://localhost:8016/health"

# Alert Prometheus metrics
curl -s "http://localhost:8016/metrics" | grep alert_
```

---

## Resolving Alerts

Alerts auto-resolve when the condition clears on the next check cycle.
Manual resolution (e.g., after investigation):

```sql
UPDATE validation_alerts
   SET is_active = FALSE, resolved_at = NOW()
 WHERE id = <alert_id>;
```

To silence a specific alert type temporarily:
```bash
# Set a Redis key to suppress re-fire
redis-cli SET alert:suppress:precision_collapse:macro_regime_engine 1 EX 14400
```

The alert engine checks for this key before firing a new alert.

---

## Validation Steps (after deploy)

1. **Service is up and healthy**
   ```bash
   curl "http://localhost:8016/health"
   # Expected: {"status": "ok", "db": "ok", "redis": "ok"}
   ```

2. **Prometheus scraping alert metrics**
   ```bash
   curl -s "http://localhost:8016/metrics" | grep alerts_fired_total
   ```

3. **API endpoints responding**
   ```bash
   curl "http://localhost:8010/api/v1/validation/system-health"
   curl "http://localhost:8010/api/v1/validation/engine-decay"
   curl "http://localhost:8010/api/v1/validation/calibration"
   curl "http://localhost:8010/api/v1/validation/failures"
   curl "http://localhost:8010/api/v1/validation/reliability"
   ```

4. **Migration applied**
   ```bash
   psql $DATABASE_URL -c "\d validation_alerts"
   ```

5. **Grafana dashboards loaded**
   - Engine Accuracy Dashboard → http://localhost:3000/d/mktintel-engine-accuracy
   - Signal Decay Dashboard → http://localhost:3000/d/mktintel-signal-decay
   - Replay Health Dashboard → http://localhost:3000/d/mktintel-replay-health
   - System Reliability Dashboard → http://localhost:3000/d/mktintel-system-reliability

---

## Rollback Steps

### Stop alert service
```bash
docker-compose stop validation-alert
docker-compose rm -f validation-alert
```

### Rollback migration 007
```sql
DROP TABLE IF EXISTS validation_alerts CASCADE;
DELETE FROM schema_migrations WHERE version = '007_observability';
```

### Rollback new API endpoints
Remove `validation_intelligence_router` registration from `src/api/validation_main.py`.

---

## Troubleshooting

| Symptom | Likely Cause | Fix |
|---------|-------------|-----|
| No Telegram alerts despite known degradation | Bot token wrong / admin chat ID wrong | Check `TELEGRAM_BOT_TOKEN` + `TELEGRAM_ADMIN_CHAT_ID` env vars |
| Alert firing every cycle | Cooldown not working | Check `validation_alerts` for duplicate rows; verify `is_active` flag |
| `/validation/system-health` returns 500 | No replay data yet | Run at least one replay first via `POST /api/v1/backtest/replay/trigger` |
| Alert engine restarting | DB connection failure | Check postgres connectivity; `docker-compose logs validation-alert` |
| coverage_gap alerts on fresh install | Expected — no outcomes yet | Normal; alerts clear once OutcomeTrackingEngine accumulates data |

---

## State and Changelog Update Instructions

After any change to this system:

1. Update `.claude/state.md` — bump version, add to Completed Modules
2. Append to `.claude/changelog.md` — list Added/Changed/Architecture Notes
3. Update `.claude/architecture.md` — add service to topology
4. Append to `.claude/decisions.md` — add DEC-0XX for any non-obvious choices

---

## Related Runbooks

- `backtesting_replay.md` — replay system that alert engine monitors
- `deploy_production.md` — deploying all services
- `debug_pipeline.md` — debugging signal ingestion
