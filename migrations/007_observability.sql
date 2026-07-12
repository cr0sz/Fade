-- Migration 007: Validation Intelligence + Alert tables
-- Phase 3 — Full Validation Intelligence Platform

-- ─── Alert History ────────────────────────────────────────────────────────────
-- Append-only log of all fired alerts. is_active tracks current resolution state.
CREATE TABLE IF NOT EXISTS validation_alerts (
    id               BIGSERIAL    PRIMARY KEY,
    alert_type       TEXT         NOT NULL,   -- precision_collapse | calibration_drift | fp_spike | replay_failure | narrative_degradation | coverage_gap
    severity         TEXT         NOT NULL,   -- critical | high | medium
    engine_name      TEXT,                    -- NULL means system-wide alert
    details          JSONB        NOT NULL DEFAULT '{}',
    threshold_value  NUMERIC(8,4),
    observed_value   NUMERIC(8,4),
    fired_at         TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    resolved_at      TIMESTAMPTZ,
    is_active        BOOLEAN      NOT NULL DEFAULT TRUE,

    CONSTRAINT chk_alert_severity CHECK (severity IN ('critical', 'high', 'medium')),
    CONSTRAINT chk_alert_type     CHECK (alert_type IN (
        'precision_collapse', 'calibration_drift', 'fp_spike',
        'replay_failure', 'narrative_degradation', 'coverage_gap'
    ))
);

CREATE INDEX IF NOT EXISTS idx_validation_alerts_type_engine
    ON validation_alerts (alert_type, engine_name, fired_at DESC);

CREATE INDEX IF NOT EXISTS idx_validation_alerts_active
    ON validation_alerts (is_active, fired_at DESC);

CREATE INDEX IF NOT EXISTS idx_validation_alerts_fired_at
    ON validation_alerts (fired_at DESC);

-- ─── Migration tracking ───────────────────────────────────────────────────────
INSERT INTO schema_migrations (version, applied_at)
VALUES ('007_observability', NOW())
ON CONFLICT DO NOTHING;
