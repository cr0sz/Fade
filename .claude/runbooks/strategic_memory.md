# Strategic Memory + Meta-Intelligence Runbook

## Purpose
Phase 5 runbook for the Meta-Intelligence + Strategic Memory Layer.
This layer learns from long-term historical behavior patterns without modifying
live signal generation, engines, or truth data.

## Service Overview
| Service | Port | Description |
|---------|------|-------------|
| `meta-intelligence` | 8018 | Strategic memory orchestrator |

## Components Built
1. `strategic_memory_engine.py` — orchestrator, event detection, meta snapshots
2. `regime_transition_intelligence.py` — transition analysis, fragility scoring
3. `narrative_lifecycle_engine.py` — full lifecycle tracking per narrative
4. `strategic_failure_analysis_engine.py` — failure cluster detection
5. `src/api/routes/meta_intelligence.py` — 6 read-only API endpoints
6. `migrations/009_meta_intelligence.sql` — 5 new tables

## Tables Created
| Table | Purpose |
|-------|---------|
| `strategic_memory_events` | Append-only log of significant system events |
| `regime_transition_records` | Per-transition fragility and instability analysis |
| `narrative_lifecycle_records` | Full lifecycle state per narrative |
| `strategic_failure_clusters` | Upserted recurring failure patterns |
| `meta_intelligence_snapshots` | Daily system-wide meta snapshot |

## API Endpoints
```
GET /api/v1/meta/failures   — Historical Failure Atlas + systemic risk
GET /api/v1/meta/transitions — Regime Transition Risk Report
GET /api/v1/meta/narratives  — Narrative Lifecycle Intelligence Report
GET /api/v1/meta/stability   — Strategic Stability Ranking + stress timeline
GET /api/v1/meta/similarity  — Historical Similarity Detection
GET /api/v1/meta/risk        — Composite risk dashboard (primary monitoring endpoint)
```

## Alert Types Added (Phase 5)
| Type | Trigger |
|------|---------|
| `recurring_historical_failure` | Same failure pattern seen >= 3x in memory window |
| `transition_instability_warning` | Historically dangerous regime transition detected |
| `narrative_collapse_acceleration` | Narrative decay velocity > threshold (rapid collapse) |
| `governance_stress_spike` | Governance stress score > 65 |
| `strategic_instability_alert` | Strategic instability index > 60 |

## Monitoring
```bash
# Service health
curl http://localhost:8018/health

# Prometheus metrics
curl http://localhost:8018/metrics

# Key metrics:
#   strategic_instability_index          — 0–100 system instability
#   governance_stress_score              — 0–100 governance pressure
#   recurring_failure_density            — 0–100 failure cluster density
#   strategic_memory_events_30d          — event count for 30-day window
```

## Manual Queries
```sql
-- Recent strategic memory events by type
SELECT event_type, severity, engine_name, cluster_id, recurrence_index, occurred_at
FROM strategic_memory_events
WHERE occurred_at >= NOW() - INTERVAL '7 days'
ORDER BY occurred_at DESC LIMIT 20;

-- Current failure clusters (active)
SELECT cluster_id, cluster_type, severity, recurrence_count, last_seen_at, is_systemic
FROM strategic_failure_clusters
WHERE is_active = TRUE
ORDER BY CASE severity WHEN 'critical' THEN 0 WHEN 'high' THEN 1 ELSE 2 END,
         recurrence_count DESC;

-- Latest narrative lifecycle states
SELECT narrative_name, lifecycle_phase, survivability_score, decay_velocity,
       is_hype, is_structural, is_recurring, collapse_count
FROM (SELECT DISTINCT ON (narrative_name) * FROM narrative_lifecycle_records
      ORDER BY narrative_name, recorded_at DESC) t
ORDER BY survivability_score DESC NULLS LAST;

-- Dangerous regime transitions in last 30 days
SELECT from_regime, to_regime, transition_instability_score,
       is_historically_dangerous, detected_at
FROM regime_transition_records
WHERE detected_at >= NOW() - INTERVAL '30 days'
ORDER BY transition_instability_score DESC;

-- Current strategic instability
SELECT strategic_instability_index, governance_stress_score,
       recurring_failure_density, strategic_stability_rank,
       active_failure_clusters, collapsing_narratives, snapshot_at
FROM meta_intelligence_snapshots
ORDER BY snapshot_at DESC LIMIT 1;

-- Historical similarity: find periods similar to current
SELECT strategic_instability_index, governance_stress_score,
       snapshot_at,
       ABS(strategic_instability_index - 45.0) AS dist_from_current
FROM meta_intelligence_snapshots
WHERE snapshot_at >= NOW() - INTERVAL '180 days'
ORDER BY dist_from_current LIMIT 5;
```

## Grafana Dashboards (Phase 5)
| Dashboard UID | Title |
|---------------|-------|
| `mktintel-strategic-stability` | Strategic Stability Dashboard |
| `mktintel-regime-transition` | Regime Transition Intelligence Dashboard |
| `mktintel-narrative-lifecycle` | Narrative Lifecycle Intelligence Dashboard |
| `mktintel-historical-failure` | Historical Failure Intelligence Dashboard |

## Interpreting Metrics

### Strategic Instability Index (0–100)
- **< 30**: Healthy — engines mostly stable, low failure density
- **30–50**: Elevated — some degradation, monitor closely
- **50–70**: High — multiple engines degraded, investigation needed
- **> 70**: Critical — systemic instability, review all governance recommendations

### Governance Stress Score (0–100)
- Composite of: calibration error levels + decaying engine fraction
- **> 65**: `governance_stress_spike` alert fires
- **> 80**: Critical — confidence is misaligned with actual accuracy system-wide

### Recurring Failure Density (0–100)
- Number of active failure clusters × severity weights
- **> 50**: Persistent failure patterns dominating system behavior
- Focus on clusters with `recurrence_count >= 5` — these are institutional failure modes

### Narrative Lifecycle Phases
- **emergence**: First appearing, accuracy rising above 0.50
- **acceleration**: Accuracy rising toward peak
- **peak**: Accuracy >= 0.70, at or near maximum
- **degradation**: Accuracy falling from peak
- **collapse**: Accuracy < 0.40
- **dormant**: Insufficient data / no recent activity

## Operational Procedures

### After Governance Stress Spike Alert
1. Query `meta_intelligence_snapshots` for current values
2. Check `signal_scores` for engines with high `calibration_error`
3. Check `engine_trust_scores` for engines with negative `degradation_velocity`
4. Review `governance_recommendations` for actionable items
5. If stress > 80: Consider temporarily increasing ALERT_COOLDOWN_HOURS to avoid alert flooding

### After Transition Instability Warning
1. Check `regime_transition_records` for the triggering transition
2. Review `engine_fragility_scores` JSONB column for most fragile engines
3. Cross-reference with `strategic_memory_events` for pre-transition signatures
4. If `is_historically_dangerous = TRUE`: this pattern historically caused precision collapses —
   increase monitoring frequency for the next 7 days

### After Strategic Instability Alert
1. Run `GET /api/v1/meta/risk` for full composite risk dashboard
2. Review strategic stability ranking at `/api/v1/meta/stability`
3. Check active failure clusters at `/api/v1/meta/failures`
4. If instability > 75: consider escalating to engineering review

## Strict Invariants
- Meta-intelligence is READ-ONLY with respect to signal generation
- No engine weights are modified automatically
- No historical truth data is modified
- All insights require manual review before action
- `strategic_failure_clusters` are UPSERTED (not append-only) for operational clarity
