-- ============================================================
-- Migration: 006_replay_system
-- Phase 2: Orchestration + Replay System
-- Created: 2026-05-29
-- ============================================================

-- ─── Replay Runs ──────────────────────────────────────────────────────────────
-- One row per replay job: metadata, status, timing.
-- Written by BacktestingEngine; status updated by ReplayScheduler.
CREATE TABLE IF NOT EXISTS replay_runs (
    id                BIGSERIAL    PRIMARY KEY,
    run_id            UUID         NOT NULL DEFAULT gen_random_uuid() UNIQUE,
    window_type       VARCHAR(20)  NOT NULL,   -- weekly | monthly | yearly | custom
    period_start      TIMESTAMPTZ  NOT NULL,
    period_end        TIMESTAMPTZ  NOT NULL,
    engine_name       VARCHAR(100),             -- NULL = all engines
    status            VARCHAR(20)  NOT NULL DEFAULT 'queued',  -- queued | running | completed | failed
    retry_count       INT          NOT NULL DEFAULT 0,
    queued_at         TIMESTAMPTZ  NOT NULL DEFAULT now(),
    started_at        TIMESTAMPTZ,
    completed_at      TIMESTAMPTZ,
    total_signals     INT          NOT NULL DEFAULT 0,
    evaluated_signals INT          NOT NULL DEFAULT 0,
    error_message     TEXT,
    parameters        JSONB        NOT NULL DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS idx_replay_runs_status
    ON replay_runs (status, queued_at DESC);
CREATE INDEX IF NOT EXISTS idx_replay_runs_window
    ON replay_runs (window_type, period_start DESC);
CREATE INDEX IF NOT EXISTS idx_replay_runs_completed
    ON replay_runs (completed_at DESC NULLS LAST);

-- ─── Replay Metrics ───────────────────────────────────────────────────────────
-- Per-engine / signal_type / window_hours metrics from each replay run.
-- Written once per run; never updated (idempotent via UNIQUE + DO UPDATE on re-run).
CREATE TABLE IF NOT EXISTS replay_metrics (
    id                    BIGSERIAL    PRIMARY KEY,
    run_id                UUID         NOT NULL REFERENCES replay_runs(run_id) ON DELETE CASCADE,
    engine_name           VARCHAR(100) NOT NULL,
    signal_type           VARCHAR(50)  NOT NULL,
    window_hours          INT          NOT NULL,
    period_start          TIMESTAMPTZ  NOT NULL,
    period_end            TIMESTAMPTZ  NOT NULL,
    signal_count          INT          NOT NULL DEFAULT 0,
    evaluated_count       INT          NOT NULL DEFAULT 0,  -- has direction_correct != NULL
    -- Binary classification (positive = risk_on/inflow predicted)
    true_positives        INT          NOT NULL DEFAULT 0,
    false_positives       INT          NOT NULL DEFAULT 0,
    true_negatives        INT          NOT NULL DEFAULT 0,
    false_negatives       INT          NOT NULL DEFAULT 0,
    precision_score       FLOAT,                            -- TP / (TP + FP)
    recall_score          FLOAT,                            -- TP / (TP + FN)
    false_positive_rate   FLOAT,                            -- FP / (FP + TN)
    f1_score              FLOAT,                            -- harmonic mean of precision + recall
    directional_accuracy  FLOAT,                            -- (TP + TN) / evaluated_count
    -- Quality scores (pulled from signal_scores at run time)
    predictive_strength   FLOAT,                            -- 0–100
    half_life_hours       FLOAT,
    calibration_error     FLOAT,
    -- Regime-specific breakdown: {"risk_on": 0.75, "risk_off": 0.6, ...}
    regime_breakdown      JSONB        NOT NULL DEFAULT '{}',
    -- Narrative-specific breakdown: {"ai_semiconductors": 0.8, ...}
    narrative_breakdown   JSONB        NOT NULL DEFAULT '{}',
    computed_at           TIMESTAMPTZ  NOT NULL DEFAULT now(),
    UNIQUE(run_id, engine_name, signal_type, window_hours)
);

CREATE INDEX IF NOT EXISTS idx_replay_metrics_run_id
    ON replay_metrics (run_id);
CREATE INDEX IF NOT EXISTS idx_replay_metrics_engine_window
    ON replay_metrics (engine_name, window_hours, computed_at DESC);
CREATE INDEX IF NOT EXISTS idx_replay_metrics_strength
    ON replay_metrics (predictive_strength DESC NULLS LAST, computed_at DESC);
CREATE INDEX IF NOT EXISTS idx_replay_metrics_period
    ON replay_metrics (period_start, period_end);

-- ─── Migration Tracker ────────────────────────────────────────────────────────
INSERT INTO schema_migrations (version) VALUES ('006_replay_system')
ON CONFLICT DO NOTHING;
