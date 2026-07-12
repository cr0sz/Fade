-- Migration 009: Meta-Intelligence + Strategic Memory Layer
-- Phase 5 — Long-term historical pattern learning, regime transition intelligence,
--            narrative lifecycle tracking, and strategic failure analysis.

-- ─── Expand alert type constraint to include Phase 5 meta-intelligence alerts ──
ALTER TABLE validation_alerts DROP CONSTRAINT IF EXISTS chk_alert_type;
ALTER TABLE validation_alerts ADD CONSTRAINT chk_alert_type CHECK (alert_type IN (
    -- Phase 3 alert types
    'precision_collapse', 'calibration_drift', 'fp_spike',
    'replay_failure', 'narrative_degradation', 'coverage_gap',
    -- Phase 4 governance alert types
    'engine_instability_spike', 'recovery_failure', 'degradation_acceleration',
    'narrative_collapse', 'confidence_inconsistency',
    -- Phase 5 meta-intelligence alert types
    'recurring_historical_failure', 'transition_instability_warning',
    'narrative_collapse_acceleration', 'governance_stress_spike',
    'strategic_instability_alert'
));

-- ─── Strategic Memory Events ──────────────────────────────────────────────────
-- Append-only log of significant long-term system behavior events.
-- Enables pattern recognition across months of system history.
CREATE TABLE IF NOT EXISTS strategic_memory_events (
    id                  BIGSERIAL       PRIMARY KEY,
    event_type          TEXT            NOT NULL,   -- regime_transition | degradation_event | calibration_failure | recovery_event | stability_milestone
    severity            TEXT            NOT NULL DEFAULT 'medium',  -- critical | high | medium | low
    occurred_at         TIMESTAMPTZ     NOT NULL DEFAULT NOW(),

    engine_name         TEXT,           -- NULL for system-wide events
    regime_context      TEXT,           -- macro regime active at event time
    narrative_context   TEXT,           -- narrative cluster (if applicable)

    -- Pattern clustering
    cluster_id          TEXT,           -- groups similar recurring events
    recurrence_index    INT             NOT NULL DEFAULT 1,  -- nth occurrence of this pattern

    event_data          JSONB           NOT NULL DEFAULT '{}',   -- full event details
    metrics_snapshot    JSONB           NOT NULL DEFAULT '{}',   -- key metrics at time of event

    CONSTRAINT chk_mem_event_type CHECK (event_type IN (
        'regime_transition', 'degradation_event', 'calibration_failure',
        'recovery_event', 'stability_milestone', 'false_positive_surge',
        'narrative_collapse_event', 'trust_score_drop', 'strategic_instability'
    )),
    CONSTRAINT chk_mem_severity CHECK (severity IN ('critical', 'high', 'medium', 'low'))
);

CREATE INDEX IF NOT EXISTS idx_strategic_memory_type
    ON strategic_memory_events (event_type, occurred_at DESC);

CREATE INDEX IF NOT EXISTS idx_strategic_memory_engine
    ON strategic_memory_events (engine_name, event_type, occurred_at DESC);

CREATE INDEX IF NOT EXISTS idx_strategic_memory_cluster
    ON strategic_memory_events (cluster_id, occurred_at DESC);

CREATE INDEX IF NOT EXISTS idx_strategic_memory_regime
    ON strategic_memory_events (regime_context, event_type, occurred_at DESC);

-- ─── Regime Transition Records ────────────────────────────────────────────────
-- Records macro-regime transitions and engine behavior during each transition.
CREATE TABLE IF NOT EXISTS regime_transition_records (
    id                          BIGSERIAL       PRIMARY KEY,
    detected_at                 TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    transition_at               TIMESTAMPTZ     NOT NULL,

    from_regime                 TEXT            NOT NULL,
    to_regime                   TEXT            NOT NULL,

    -- Instability metrics during transition
    transition_instability_score NUMERIC(6,2)   NOT NULL DEFAULT 0,   -- 0–100
    transition_volatility_score  NUMERIC(6,2)   NOT NULL DEFAULT 0,   -- 0–100, std-dev of trust scores

    -- Per-engine fragility during this transition (JSON: {engine: score})
    engine_fragility_scores      JSONB          NOT NULL DEFAULT '{}',

    -- Historical risk assessment
    is_historically_dangerous    BOOLEAN        NOT NULL DEFAULT FALSE,
    danger_evidence              JSONB          NOT NULL DEFAULT '{}',  -- similar past transitions

    -- Early degradation signatures detected before/during this transition
    degradation_signatures       JSONB          NOT NULL DEFAULT '{}',

    -- Outcome (populated post-hoc by next run)
    post_transition_precision_delta  NUMERIC(8,4),  -- avg precision change across engines after
    post_transition_trust_delta      NUMERIC(8,4),  -- avg trust score change

    pre_window_days             INT             NOT NULL DEFAULT 7,
    post_window_days            INT             NOT NULL DEFAULT 7,

    similarity_score            NUMERIC(6,4),   -- similarity to most-similar historical transition (0–1)
    most_similar_transition_id  BIGINT          REFERENCES regime_transition_records(id)
);

CREATE INDEX IF NOT EXISTS idx_regime_transition_pair
    ON regime_transition_records (from_regime, to_regime, transition_at DESC);

CREATE INDEX IF NOT EXISTS idx_regime_transition_dangerous
    ON regime_transition_records (is_historically_dangerous, detected_at DESC);

CREATE INDEX IF NOT EXISTS idx_regime_transition_at
    ON regime_transition_records (transition_at DESC);

-- ─── Narrative Lifecycle Records ──────────────────────────────────────────────
-- Tracks the full lifecycle of each narrative from emergence to collapse.
CREATE TABLE IF NOT EXISTS narrative_lifecycle_records (
    id                      BIGSERIAL       PRIMARY KEY,
    narrative_name          TEXT            NOT NULL,
    recorded_at             TIMESTAMPTZ     NOT NULL DEFAULT NOW(),

    -- Current lifecycle phase
    lifecycle_phase         TEXT            NOT NULL DEFAULT 'unknown',  -- emergence | acceleration | peak | degradation | collapse | dormant
    phase_started_at        TIMESTAMPTZ,

    -- Lifecycle metrics
    survivability_score     NUMERIC(6,2),   -- 0–100: long-term predictive durability
    recurrence_probability  NUMERIC(6,4),   -- 0–1: likelihood of re-emergence after collapse
    decay_velocity          NUMERIC(10,6),  -- OLS slope (negative = decaying)
    lifecycle_duration_weeks INT,           -- weeks since first emergence

    -- Accuracy profile
    peak_accuracy           NUMERIC(6,4),   -- highest accuracy reached
    current_accuracy        NUMERIC(6,4),   -- latest accuracy
    accuracy_at_emergence   NUMERIC(6,4),   -- accuracy when first detected

    -- Classification flags
    is_hype                 BOOLEAN         NOT NULL DEFAULT FALSE,  -- short-lived hype (<= 4 weeks peak)
    is_structural           BOOLEAN         NOT NULL DEFAULT FALSE,  -- long-term persistent (> 12 weeks stable)
    is_recurring            BOOLEAN         NOT NULL DEFAULT FALSE,  -- has re-emerged at least once

    -- Historical context
    collapse_count          INT             NOT NULL DEFAULT 0,   -- how many times collapsed
    emergence_count         INT             NOT NULL DEFAULT 1,   -- how many times emerged
    avg_lifecycle_weeks     NUMERIC(8,2),   -- average weeks per lifecycle cycle

    regime_performance      JSONB           NOT NULL DEFAULT '{}',  -- accuracy per regime

    CONSTRAINT chk_lifecycle_phase CHECK (lifecycle_phase IN (
        'emergence', 'acceleration', 'peak', 'degradation', 'collapse', 'dormant', 'unknown'
    ))
);

CREATE INDEX IF NOT EXISTS idx_narrative_lifecycle_name
    ON narrative_lifecycle_records (narrative_name, recorded_at DESC);

CREATE INDEX IF NOT EXISTS idx_narrative_lifecycle_phase
    ON narrative_lifecycle_records (lifecycle_phase, recorded_at DESC);

CREATE INDEX IF NOT EXISTS idx_narrative_lifecycle_hype
    ON narrative_lifecycle_records (is_hype, is_structural, recorded_at DESC);

-- ─── Strategic Failure Clusters ───────────────────────────────────────────────
-- Clustered recurring failure patterns with persistence across time.
CREATE TABLE IF NOT EXISTS strategic_failure_clusters (
    id                  BIGSERIAL       PRIMARY KEY,
    cluster_id          TEXT            NOT NULL UNIQUE,   -- stable identifier, e.g. "precision_collapse_risk_off_v1"
    cluster_type        TEXT            NOT NULL,          -- precision_collapse | governance_instability | false_positive_spike | calibration_drift | systemic_degradation
    severity            TEXT            NOT NULL DEFAULT 'medium',

    first_seen_at       TIMESTAMPTZ     NOT NULL,
    last_seen_at        TIMESTAMPTZ     NOT NULL,
    recurrence_count    INT             NOT NULL DEFAULT 1,

    affected_engines    JSONB           NOT NULL DEFAULT '[]',   -- list of engine names
    regime_context      TEXT,           -- most common regime during failures
    narrative_context   TEXT,           -- most common narrative during failures

    -- Pattern signature (key metrics that define this cluster)
    cluster_signature   JSONB           NOT NULL DEFAULT '{}',
    -- Evidence from most recent occurrence
    latest_evidence     JSONB           NOT NULL DEFAULT '{}',

    is_active           BOOLEAN         NOT NULL DEFAULT TRUE,   -- FALSE if pattern hasn't appeared in 30+ days
    is_systemic         BOOLEAN         NOT NULL DEFAULT FALSE,  -- affects 3+ engines simultaneously

    CONSTRAINT chk_cluster_type CHECK (cluster_type IN (
        'precision_collapse', 'governance_instability', 'false_positive_spike',
        'calibration_drift', 'systemic_degradation', 'narrative_collapse_pattern',
        'regime_transition_fragility'
    )),
    CONSTRAINT chk_cluster_severity CHECK (severity IN ('critical', 'high', 'medium', 'low'))
);

CREATE INDEX IF NOT EXISTS idx_failure_cluster_type
    ON strategic_failure_clusters (cluster_type, last_seen_at DESC);

CREATE INDEX IF NOT EXISTS idx_failure_cluster_active
    ON strategic_failure_clusters (is_active, severity, last_seen_at DESC);

-- ─── Meta-Intelligence Snapshots ─────────────────────────────────────────────
-- Daily system-wide meta-intelligence summary for long-term trend tracking.
CREATE TABLE IF NOT EXISTS meta_intelligence_snapshots (
    id                              BIGSERIAL       PRIMARY KEY,
    snapshot_at                     TIMESTAMPTZ     NOT NULL DEFAULT NOW(),

    -- System-level strategic scores (all 0–100)
    strategic_instability_index     NUMERIC(6,2)    NOT NULL DEFAULT 0,
    recurring_failure_density       NUMERIC(6,2)    NOT NULL DEFAULT 0,
    governance_stress_score         NUMERIC(6,2)    NOT NULL DEFAULT 0,
    strategic_stability_rank        NUMERIC(6,2)    NOT NULL DEFAULT 0,  -- inverse of instability

    -- Counts at snapshot time
    active_failure_clusters         INT             NOT NULL DEFAULT 0,
    dangerous_transitions_30d       INT             NOT NULL DEFAULT 0,
    collapsing_narratives           INT             NOT NULL DEFAULT 0,
    degrading_engines               INT             NOT NULL DEFAULT 0,
    total_memory_events_30d         INT             NOT NULL DEFAULT 0,

    -- Engine-level strategic summary
    engine_stability_summary        JSONB           NOT NULL DEFAULT '{}',
    -- Narrative lifecycle summary
    narrative_lifecycle_summary     JSONB           NOT NULL DEFAULT '{}',
    -- Top failure clusters
    top_failure_clusters            JSONB           NOT NULL DEFAULT '[]'
);

CREATE INDEX IF NOT EXISTS idx_meta_snapshot_at
    ON meta_intelligence_snapshots (snapshot_at DESC);

-- ─── Migration tracking ───────────────────────────────────────────────────────
INSERT INTO schema_migrations (version, applied_at)
VALUES ('009_meta_intelligence', NOW())
ON CONFLICT DO NOTHING;
