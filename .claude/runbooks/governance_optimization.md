# Runbook: Governance Optimization Engine

**Module:** Phase 4 — Adaptive Governance + Self-Correction Intelligence  
**Version:** v0.8.0  
**Owner:** Quantitative Systems Governance Engineering  

---

## Purpose

The Governance Optimization Engine continuously measures system reliability against
historical truth and generates evidence-based recommendations for improving signal
quality. It is a **recommendation-only** system — it never mutates live engines,
scoring formulas, or signal generation.

It answers: *"Which engines should we trust more, which less, and why?"*

---

## Components

| Component | File | Port | Role |
|-----------|------|------|------|
| EngineGovernanceEngine | `src/engines/engine_governance_engine.py` | 8017 | Trust scoring + recommendation persistence |
| AdaptiveWeightingEngine | `src/engines/adaptive_weighting_engine.py` | — | Weighting + calibration recommendations |
| NarrativeStabilityEngine | `src/engines/narrative_stability_engine.py` | — | Narrative persistence + degradation scoring |
| NoiseContributionEngine | `src/engines/noise_contribution_engine.py` | — | FP contribution + amplification chain detection |
| Governance API | `src/api/routes/governance.py` | 8010 | 6 read-only endpoints |

### New Tables (migration 008)
- `engine_trust_scores` — per-engine trust scores computed hourly
- `governance_recommendations` — evidence-based recommendations with 7-day TTL
- `narrative_stability_scores` — per-narrative persistence and degradation tracking

---

## Trust Score Formula

```
trust_score (0–100) =
    30%  × predictive_strength_normalized
    25%  × decay_adjusted_precision        (precision × max(0, 1 + decay_velocity×5))
    20%  × calibration_stability           (max(0, 1 – calibration_error×4) × 100)
    15%  × regime_reliability              (avg accuracy across regimes × 100)
    10%  × recovery_capability             (fraction of dips followed by recovery)
```

Engine health thresholds:
- **Healthy:** trust_score ≥ 60
- **Degraded:** 30 ≤ trust_score < 60
- **Failing:** trust_score < 30

---

## Recommendation Types

| Action | Trigger |
|--------|---------|
| `investigate` | trust_score < 30 or noise_contribution_pct > 40% |
| `reduce_weight` | trust_score 30–50, degradation_velocity < -0.03/week |
| `increase_weight` | trust_score > 75 and stable for 4+ weeks |
| `recalibrate` | calibration_error > 0.15 for 2+ consecutive periods |
| `monitor` | trust_score 50–60 with accelerating decay |

Recommendations expire after 7 days unless re-generated. Previous recommendations
are replaced by newer ones for the same target.

---

## Monitoring

```bash
# Trust score API
curl "http://localhost:8010/api/v1/governance/trust"

# All recommendations
curl "http://localhost:8010/api/v1/governance/recommendations"

# Noise report
curl "http://localhost:8010/api/v1/governance/noise"

# Recovery analysis
curl "http://localhost:8010/api/v1/governance/recovery"

# Governance engine health
curl "http://localhost:8017/health"

# Prometheus metrics
curl -s "http://localhost:8017/metrics" | grep governance_
```

---

## Checking Trust Scores

```bash
# Latest trust scores per engine
psql $DATABASE_URL -c "
  SELECT DISTINCT ON (engine_name)
    engine_name, trust_score, degradation_velocity,
    stability_score, noise_contribution, computed_at
  FROM engine_trust_scores
  ORDER BY engine_name, computed_at DESC;"

# Engines below trust threshold
psql $DATABASE_URL -c "
  SELECT DISTINCT ON (engine_name)
    engine_name, trust_score, degradation_velocity
  FROM engine_trust_scores
  WHERE trust_score < 30
  ORDER BY engine_name, computed_at DESC;"

# Active recommendations
psql $DATABASE_URL -c "
  SELECT target_type, target_name, action, priority,
         confidence_score, generated_at
  FROM governance_recommendations
  WHERE is_acknowledged = FALSE
    AND (expires_at IS NULL OR expires_at > NOW())
  ORDER BY priority DESC, confidence_score DESC;"
```

---

## Acknowledging Recommendations

Recommendations must be manually acknowledged after review. Never auto-apply.

```bash
# Acknowledge a specific recommendation
psql $DATABASE_URL -c "
  UPDATE governance_recommendations
     SET is_acknowledged = TRUE, acknowledged_at = NOW()
   WHERE id = <rec_id>;"

# Acknowledge all low-priority recommendations
psql $DATABASE_URL -c "
  UPDATE governance_recommendations
     SET is_acknowledged = TRUE, acknowledged_at = NOW()
   WHERE priority = 'low'
     AND is_acknowledged = FALSE;"
```

---

## Governance Alert Types (Phase 4 additions)

| Alert | Trigger | Severity | Cooldown |
|-------|---------|----------|---------|
| `engine_instability_spike` | stability_score drops > 20 points in 7 days | HIGH | 6h |
| `recovery_failure` | engine degraded for 4+ consecutive weeks, not recovering | HIGH | 12h |
| `degradation_acceleration` | decay velocity worsened by > 0.03/week vs prior week | HIGH | 6h |
| `narrative_collapse` | narrative accuracy drops from > 0.60 to < 0.45 in 2 weeks | CRITICAL | 4h |
| `confidence_inconsistency` | high-confidence accuracy < 0.55 for 3+ consecutive periods | HIGH | 8h |

---

## Rollback Steps

### Stop governance engine
```bash
docker-compose stop governance-engine
docker-compose rm -f governance-engine
```

### Rollback migration 008
```sql
DROP TABLE IF EXISTS engine_trust_scores CASCADE;
DROP TABLE IF EXISTS governance_recommendations CASCADE;
DROP TABLE IF EXISTS narrative_stability_scores CASCADE;
-- Restore old alert check constraint
ALTER TABLE validation_alerts DROP CONSTRAINT IF EXISTS chk_alert_type;
ALTER TABLE validation_alerts ADD CONSTRAINT chk_alert_type CHECK (alert_type IN (
    'precision_collapse', 'calibration_drift', 'fp_spike',
    'replay_failure', 'narrative_degradation', 'coverage_gap'
));
DELETE FROM schema_migrations WHERE version = '008_governance';
```

### Remove governance router from API
Remove `governance_router` import and `include_router` call from `src/api/validation_main.py`.

---

## Troubleshooting

| Symptom | Likely Cause | Fix |
|---------|-------------|-----|
| No trust scores in DB | governance-engine not running | `docker-compose restart governance-engine` |
| `/governance/trust` returns empty list | No replay data yet | Run at least one weekly replay first |
| Recommendations flooding | Cooldown too low | Raise `GOVERNANCE_INTERVAL` or check recommendation deduplication |
| Governance engine OOM | Too many engines with long history | Reduce `GOVERNANCE_PERIOD_DAYS` |
| Amplification chains empty | Not enough weekly replay data | Need 4+ weekly replays for correlation |

---

## State and Changelog Update Instructions

After any change to this system:

1. Update `.claude/state.md` — bump version, add to Completed Modules
2. Append to `.claude/changelog.md` — list Added/Changed/Architecture Notes
3. Update `.claude/architecture.md` — add service to topology
4. Append to `.claude/decisions.md` — add DEC-0XX for any non-obvious choices

---

## Related Runbooks
- `calibration_governance.md` — calibration-specific governance procedures
- `backtesting_replay.md` — replay system that generates the metrics governance analyzes
- `validation_alert.md` — alert system that governance extends
