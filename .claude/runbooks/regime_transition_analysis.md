# Regime Transition Analysis Runbook

## Purpose
Operational procedures for the Regime Transition Intelligence module (Phase 5).
Use when a dangerous regime transition is detected or when investigating engine
fragility during macro-regime changes.

## Overview
Regime transitions are the single biggest source of engine performance degradation.
When the macro regime shifts (e.g., risk_on → risk_off), engines calibrated on the
prior regime may produce false positives until they re-stabilize.

The `RegimeTransitionIntelligence` engine:
- Detects regime transitions from `macro_regimes` table (LAG-based detection)
- Computes instability and volatility scores using trust score deltas
- Identifies per-engine fragility (how much each engine dropped during transition)
- Flags historically dangerous transition pairs (those that previously caused collapses)

## Key Metrics
| Metric | Meaning | Threshold |
|--------|---------|-----------|
| `transition_instability_score` | Avg trust score drop across engines (0–100) | > 25 = high risk |
| `transition_volatility_score` | Std-dev of post-transition trust scores | > 20 = unstable |
| `is_historically_dangerous` | This pair caused precision collapses ≥ 2 times before | Any = alert |
| `engine_fragility_score` | Per-engine: 0–100, how much it dropped in this transition | > 50 = fragile |

## Dangerous Transition Pairs (Historical)
The following transition pairs have historically been associated with precision collapses:
- `risk_on → volatility_regime` — sudden volatility surge catches bullish engines off-guard
- `liquidity_expansion → tightening` — rate-change narrative engines often diverge
- `risk_off → risk_on` — recovery false-positives common when data lags

These are identified automatically. Check `GET /api/v1/meta/transitions` for current assessments.

## Investigation Procedures

### When `transition_instability_warning` fires
```sql
-- Get the triggering transition details
SELECT from_regime, to_regime, transition_instability_score,
       transition_volatility_score, is_historically_dangerous,
       engine_fragility_scores, degradation_signatures, detected_at
FROM regime_transition_records
WHERE is_historically_dangerous = TRUE
  AND detected_at >= NOW() - INTERVAL '48 hours'
ORDER BY detected_at DESC LIMIT 5;

-- Which engines degraded BEFORE the transition (early warning signatures)?
SELECT detected_at,
       degradation_signatures->>'engines_with_pre_degradation' AS pre_degraded_engines
FROM regime_transition_records
WHERE detected_at >= NOW() - INTERVAL '7 days'
ORDER BY detected_at DESC;
```

### Historical Danger Assessment for a Specific Transition Pair
```sql
-- History of risk_on → risk_off transitions with outcomes
SELECT transition_at, transition_instability_score,
       post_transition_precision_delta,
       is_historically_dangerous, similarity_score
FROM regime_transition_records
WHERE from_regime = 'risk_on' AND to_regime = 'risk_off'
ORDER BY transition_at DESC;
```

### Engine Fragility Ranking Across All Transitions
```sql
-- Which engines are consistently fragile during regime transitions?
SELECT kv.key AS engine_name,
       ROUND(AVG((kv.value)::numeric)::numeric, 2) AS avg_fragility,
       COUNT(*) AS transition_count
FROM regime_transition_records,
     jsonb_each_text(engine_fragility_scores) AS kv
WHERE detected_at >= NOW() - INTERVAL '180 days'
GROUP BY kv.key
HAVING COUNT(*) >= 3
ORDER BY avg_fragility DESC;
```

## Post-Transition Monitoring Protocol
After any transition with `instability_score > 15`:
1. Monitor engine trust scores for 7 days post-transition
2. Check `replay_metrics` daily for precision drops
3. If precision drops > 20% for any engine: fire manual governance investigation
4. Update `regime_transition_records.post_transition_precision_delta` manually if needed:
```sql
UPDATE regime_transition_records
SET post_transition_precision_delta = <value>
WHERE id = <transition_id>;
```

## Invariants
- `regime_transition_records` is append-only (no deletes, no updates except `post_transition_precision_delta`)
- Transition detection uses LAG() — cannot produce false transitions from noise
- `is_historically_dangerous` requires >= 2 past collapses for the same pair (configurable via `_DANGER_THRESH`)
- Fragility score = 0 means engine was unaffected; 100 = dropped 50+ trust points
