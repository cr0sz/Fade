-- Migration 008: Adaptive Governance + Self-Correction Intelligence
-- Phase 4 — Engine Trust Scoring + Governance Recommendations

-- ─── Expand alert type constraint from migration 007 ─────────────────────────
-- Drop old constraint and re-add with governance alert types included
ALTER TABLE validation_alerts DROP CONSTRAINT IF EXISTS chk_alert_type;
ALTER TABLE validation_alerts ADD CONSTRAINT chk_alert_type CHECK (alert_type IN (
    -- Phase 3 alert types
    'precision_collapse', 'calibration_drift', 'fp_spike',
    'replay_failure', 'narrative_degradation', 'coverage_gap',
    -- Phase 4 governance alert types
    'engine_instability_spike', 'recovery_failure', 'degradation_acceleration',
    'narrative_collapse', 'confidence_inconsistency'
));

-- ─── Engine Trust Scores ──────────────────────────────────────────────────────
-- Append-only; latest score per engine retrieved via DISTINCT ON.
CREATE TABLE IF NOT EXISTS engine_trust_scores (
    id                    BIGSERIAL     PRIMARY KEY,
    engine_name           TEXT          NOT NULL,
    computed_at           TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    window_hours          INT           NOT NULL DEFAULT 24,

    -- Core trust dimensions (all 0–100 or fraction 0–1)
    trust_score           NUMERIC(6,2)  NOT NULL,   -- composite 0–100
    predictive_strength   NUMERIC(6,2),              -- from signal_scores
    decay_adj_precision   NUMERIC(6,4),              -- precision × decay penalty
    calibration_stability NUMERIC(6,2),              -- 0–100, higher = better
    regime_reliability    NUMERIC(6,4),              -- avg accuracy across regimes
    recovery_capability   NUMERIC(6,2),              -- 0–100
    stability_score       NUMERIC(6,2),              -- 0–100, lower variance = higher
    noise_contribution    NUMERIC(6,2),              -- 0–100, higher = more noise
    degradation_velocity  NUMERIC(10,6),             -- OLS slope per week (negative = decaying)

    -- Derived classifications
    health_status         TEXT          NOT NULL DEFAULT 'unknown',  -- healthy | degraded | failing
    is_decaying           BOOLEAN       NOT NULL DEFAULT FALSE,
    is_unstable           BOOLEAN       NOT NULL DEFAULT FALSE,

    -- Raw data snapshot
    avg_precision_7d      NUMERIC(6,4),
    avg_precision_30d     NUMERIC(6,4),
    weekly_accuracy_json  JSONB         NOT NULL DEFAULT '[]',  -- last N weeks
    regime_breakdown_json JSONB         NOT NULL DEFAULT '{}',

    CONSTRAINT chk_health_status CHECK (health_status IN ('healthy', 'degraded', 'failing', 'unknown'))
);

CREATE INDEX IF NOT EXISTS idx_engine_trust_scores_engine
    ON engine_trust_scores (engine_name, computed_at DESC);

CREATE INDEX IF NOT EXISTS idx_engine_trust_scores_computed
    ON engine_trust_scores (computed_at DESC);

CREATE INDEX IF NOT EXISTS idx_engine_trust_scores_trust
    ON engine_trust_scores (trust_score DESC, computed_at DESC);

-- ─── Governance Recommendations ───────────────────────────────────────────────
-- Each recommendation targets one engine, confidence level, or narrative.
-- Expires after 7 days if not superseded.
CREATE TABLE IF NOT EXISTS governance_recommendations (
    id                     BIGSERIAL    PRIMARY KEY,
    generated_at           TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    expires_at             TIMESTAMPTZ  NOT NULL DEFAULT (NOW() + INTERVAL '7 days'),

    target_type            TEXT         NOT NULL,  -- engine | confidence_level | narrative
    target_name            TEXT         NOT NULL,

    action                 TEXT         NOT NULL,  -- investigate | reduce_weight | increase_weight | recalibrate | monitor
    recommended_adjustment NUMERIC(6,4),           -- e.g. -0.20 means reduce by 20%
    priority               TEXT         NOT NULL,  -- high | medium | low
    confidence_score       NUMERIC(6,4) NOT NULL,  -- 0–1: how confident is the recommendation

    evidence               JSONB        NOT NULL DEFAULT '{}',
    rationale              TEXT,

    is_acknowledged        BOOLEAN      NOT NULL DEFAULT FALSE,
    acknowledged_at        TIMESTAMPTZ,

    CONSTRAINT chk_rec_target_type CHECK (target_type IN ('engine', 'confidence_level', 'narrative')),
    CONSTRAINT chk_rec_action      CHECK (action IN ('investigate', 'reduce_weight', 'increase_weight', 'recalibrate', 'monitor')),
    CONSTRAINT chk_rec_priority    CHECK (priority IN ('high', 'medium', 'low'))
);

CREATE INDEX IF NOT EXISTS idx_governance_recs_target
    ON governance_recommendations (target_type, target_name, generated_at DESC);

CREATE INDEX IF NOT EXISTS idx_governance_recs_active
    ON governance_recommendations (is_acknowledged, expires_at DESC, generated_at DESC);

CREATE INDEX IF NOT EXISTS idx_governance_recs_priority
    ON governance_recommendations (priority, is_acknowledged, generated_at DESC);

-- ─── Narrative Stability Scores ───────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS narrative_stability_scores (
    id                      BIGSERIAL    PRIMARY KEY,
    narrative_name          TEXT         NOT NULL,
    computed_at             TIMESTAMPTZ  NOT NULL DEFAULT NOW(),

    persistence_score       NUMERIC(6,4),  -- fraction of windows with accuracy > STABILITY_FLOOR
    degradation_rate        NUMERIC(10,6), -- OLS slope per week (negative = degrading)
    predictive_longevity_w  INT,           -- consecutive stable weeks
    half_life_weeks         NUMERIC(8,4),  -- estimated; NULL if beyond 26w or no data

    is_stable               BOOLEAN,
    is_collapsing           BOOLEAN       NOT NULL DEFAULT FALSE,  -- rapidly degrading

    sample_weeks            INT,
    regime_breakdown        JSONB         NOT NULL DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS idx_narrative_stability_name
    ON narrative_stability_scores (narrative_name, computed_at DESC);

-- ─── Migration tracking ───────────────────────────────────────────────────────
INSERT INTO schema_migrations (version, applied_at)
VALUES ('008_governance', NOW())
ON CONFLICT DO NOTHING;
