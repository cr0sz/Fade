# Runbook: Calibration Governance

**Module:** Phase 4 — Adaptive Governance  
**Version:** v0.8.0  
**Owner:** Quantitative Systems Governance Engineering  

---

## Purpose

Calibration governance tracks whether the system's confidence levels correctly
predict its accuracy. A well-calibrated system is one where "high confidence"
predictions are correct ~80% of the time, "medium confidence" ~65%, and
"low confidence" ~50%.

Poor calibration — either over-confidence or under-confidence — is a form of
systematic error that governance recommendations can flag and track.

---

## Calibration Targets

| Confidence Level | Target Accuracy | Alert Threshold |
|-----------------|----------------|-----------------|
| `high`          | 0.80           | error > 0.15    |
| `medium`        | 0.65           | error > 0.15    |
| `low`           | 0.50           | error > 0.15    |

Calibration error = weighted mean absolute deviation from target across tiers.
Overall threshold for calibration_drift alert: error > 0.20.

---

## What "Recalibrate" Means Operationally

A governance `recalibrate` recommendation means the responsible team should review:

1. **How confidence levels are assigned** — which rules or thresholds the engine uses
   to classify a signal as high/medium/low confidence
2. **Historical signal data** — run the query below to see actual vs expected accuracy
3. **Decide on a fix** — e.g., tighten the threshold for "high confidence" classification
   so fewer signals qualify, making those that do more reliably accurate

A recalibrate recommendation is **never automatic** — it requires human review.

---

## Diagnosing Calibration Issues

### Step 1: Check which engines are miscalibrated

```bash
curl "http://localhost:8010/api/v1/validation/calibration?days=30" | python3 -m json.tool
```

### Step 2: Drill into the specific confidence tier

```bash
psql $DATABASE_URL -c "
  SELECT sr.engine_name,
         so.confidence_level,
         COUNT(*)                          AS samples,
         ROUND(AVG(so.is_correct::int)::numeric, 4) AS actual_accuracy,
         CASE so.confidence_level
           WHEN 'high'   THEN 0.80
           WHEN 'medium' THEN 0.65
           ELSE           0.50
         END                               AS target_accuracy
  FROM signal_outcomes so
  JOIN signals_registry sr ON sr.signal_id = so.signal_id
  WHERE sr.engine_name = '<engine_name>'
    AND so.evaluated_at >= NOW() - INTERVAL '30 days'
    AND so.confidence_level IS NOT NULL
  GROUP BY sr.engine_name, so.confidence_level
  ORDER BY so.confidence_level;"
```

### Step 3: Review calibration trend

```bash
psql $DATABASE_URL -c "
  SELECT DISTINCT ON (engine_name)
    engine_name, calibration_error, predictive_strength, computed_at
  FROM signal_scores
  WHERE engine_name = '<engine_name>'
  ORDER BY engine_name, computed_at DESC
  LIMIT 10;"
```

### Step 4: Check governance recommendation

```bash
psql $DATABASE_URL -c "
  SELECT action, recommended_adjustment, confidence_score,
         evidence, generated_at, expires_at
  FROM governance_recommendations
  WHERE target_name = '<engine_name>'
    AND target_type = 'engine'
    AND is_acknowledged = FALSE
  ORDER BY generated_at DESC
  LIMIT 5;"
```

---

## Overconfident Engines

An engine is **overconfident** when:
- `confidence_level = 'high'` BUT actual accuracy < 0.60
- Indicated by governance recommendation: `recalibrate` + `overconfident` in evidence

**What to do:**
1. Review the engine's confidence scoring logic
2. Raise the threshold for "high confidence" classification
3. Acknowledge the recommendation after fixing
4. Monitor the next 2–4 weeks for calibration recovery

---

## Underconfident Engines

An engine is **underconfident** when:
- `confidence_level = 'low'` BUT actual accuracy > 0.70
- This means signals are being suppressed that are actually reliable

**What to do:**
1. Review why the engine assigns low confidence to accurate signals
2. Consider loosening the confidence assignment thresholds
3. Check if training data or feature set has changed

---

## Calibration Stability Check (Grafana)

After fixes are applied, monitor the Stability Dashboard:

- **http://localhost:3000/d/mktintel-stability** → Calibration Error Trend panel
- Look for: calibration_error decreasing over 2–4 weeks after adjustment

If calibration_error does not decrease within 2 weeks of a configuration change,
escalate — there may be a data quality issue upstream.

---

## Calibration Recovery SLA

| Severity | Expected Recovery | Action if Not Recovered |
|----------|------------------|------------------------|
| HIGH (error > 0.20) | 14 days | Escalate to engine owner |
| MEDIUM (error 0.15–0.20) | 30 days | Re-examine confidence thresholds |
| LOW (error 0.10–0.15) | Monitor only | No immediate action needed |

---

## Related Runbooks
- `governance_optimization.md` — full governance layer overview
- `validation_alert.md` — calibration_drift alert configuration
