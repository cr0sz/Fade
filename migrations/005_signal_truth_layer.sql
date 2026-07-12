-- ============================================================
-- Migration: 005_signal_truth_layer
-- Phase 1: Signal Truth Layer — scoring storage
-- Created: 2026-05-29
-- ============================================================

-- IMMUTABLE helper: extract UTC calendar date from a timestamptz without
-- relying on the session timezone (date_trunc/::date on timestamptz is STABLE,
-- which PostgreSQL forbids in index expressions).  Uses epoch arithmetic which
-- is always UTC-based and therefore truly IMMUTABLE.
CREATE OR REPLACE FUNCTION _utc_date(ts TIMESTAMPTZ)
    RETURNS DATE LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE AS
    $$ SELECT ('1970-01-01'::date + (extract(epoch from ts)::int8 / 86400)::int4) $$;

-- ─── Signal Scores ────────────────────────────────────────────────────────────
-- Rolling predictive strength and half-life per engine/type/window.
-- Populated by SignalScoringEngine after outcomes exist.
-- One row per engine + signal_type + window_hours per calendar day (upsert).
CREATE TABLE IF NOT EXISTS signal_scores (
    id                  BIGSERIAL    PRIMARY KEY,
    engine_name         VARCHAR(100) NOT NULL,
    signal_type         VARCHAR(50)  NOT NULL,
    window_hours        INT          NOT NULL,   -- 1 | 6 | 24 | 72 | 168
    period_days         INT          NOT NULL DEFAULT 30,
    sample_size         INT          NOT NULL DEFAULT 0,
    accuracy            FLOAT,                   -- directional accuracy 0.0–1.0
    predictive_strength FLOAT,                   -- 0–100  (50% acc → 0, 100% → 100)
    half_life_hours     FLOAT,                   -- NULL = still predictive past 7d
    calibration_error   FLOAT,                   -- |mean_confidence_numeric - accuracy|
    computed_at         TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- Unique: one row per engine / signal_type / window_hours per UTC calendar day.
-- Uses the IMMUTABLE _utc_date() helper so the expression is safe for indexing.
CREATE UNIQUE INDEX IF NOT EXISTS idx_signal_scores_daily_uq
    ON signal_scores (engine_name, signal_type, window_hours, (_utc_date(computed_at)));

CREATE INDEX IF NOT EXISTS idx_signal_scores_engine_window
    ON signal_scores (engine_name, window_hours, computed_at DESC);
CREATE INDEX IF NOT EXISTS idx_signal_scores_computed_at
    ON signal_scores (computed_at DESC);
CREATE INDEX IF NOT EXISTS idx_signal_scores_strength
    ON signal_scores (predictive_strength DESC NULLS LAST, computed_at DESC);
CREATE INDEX IF NOT EXISTS idx_signal_scores_half_life
    ON signal_scores (half_life_hours NULLS LAST, computed_at DESC);

-- ─── Migration Tracker ────────────────────────────────────────────────────────
INSERT INTO schema_migrations (version) VALUES ('005_signal_truth_layer')
ON CONFLICT DO NOTHING;
