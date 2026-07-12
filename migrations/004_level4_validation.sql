-- ============================================================
-- Migration: 004_level4_validation
-- Level 4 Extension: Signal Validation + Backtesting Infrastructure
-- Created: 2026-05-28
-- ============================================================

-- Widen version column to accommodate longer migration names (was VARCHAR(20))
ALTER TABLE IF EXISTS schema_migrations
    ALTER COLUMN version TYPE VARCHAR(50);

-- ─── Signals Registry ─────────────────────────────────────────────────────────
-- Central archive of every L2/L3 signal before evaluation.
-- Populated by signal-archiver from engine output tables.
-- Immutable once written — no UPSERT, only INSERT.
CREATE TABLE IF NOT EXISTS signals_registry (
    id               BIGSERIAL PRIMARY KEY,
    signal_type      VARCHAR(50)  NOT NULL,   -- trend | flow | regime | confluence | etf_rotation | narrative
    engine_name      VARCHAR(100) NOT NULL,   -- emitting engine class name
    ticker           VARCHAR(20),             -- NULL for market-wide signals
    direction        VARCHAR(30),             -- risk_on | risk_off | inflow | outflow | neutral | mixed
    composite_score  FLOAT,                   -- normalised score at emission time
    confidence_level VARCHAR(20),             -- low | medium | high
    signal_data      JSONB NOT NULL DEFAULT '{}',  -- full signal snapshot
    source_table     VARCHAR(100) NOT NULL,   -- market_trends | institutional_flows | ...
    source_id        BIGINT,                  -- source row id (NULL for upsert-based tables)
    emitted_at       TIMESTAMPTZ NOT NULL,    -- when engine computed this (source computed_at)
    archived_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_signals_type_emitted
    ON signals_registry (signal_type, emitted_at DESC);
CREATE INDEX IF NOT EXISTS idx_signals_ticker_emitted
    ON signals_registry (ticker, emitted_at DESC);
CREATE INDEX IF NOT EXISTS idx_signals_engine_emitted
    ON signals_registry (engine_name, emitted_at DESC);
CREATE INDEX IF NOT EXISTS idx_signals_source
    ON signals_registry (source_table, source_id);
CREATE INDEX IF NOT EXISTS idx_signals_direction
    ON signals_registry (direction, emitted_at DESC);
CREATE INDEX IF NOT EXISTS idx_signals_archived_at
    ON signals_registry (archived_at DESC);
-- Index to find signals by emitted_at (supports pending evaluation queries)
CREATE INDEX IF NOT EXISTS idx_signals_pending_eval
    ON signals_registry (emitted_at DESC);

-- ─── Signal Outcomes ──────────────────────────────────────────────────────────
-- Ground truth: was the signal directionally correct?
-- Populated by backtest-worker once evaluation window elapses.
CREATE TABLE IF NOT EXISTS signal_outcomes (
    id                  BIGSERIAL PRIMARY KEY,
    signal_id           BIGINT NOT NULL REFERENCES signals_registry(id) ON DELETE CASCADE,
    ticker              VARCHAR(20),
    evaluation_window_h INT NOT NULL,          -- 4 | 8 | 24 | 72
    predicted_direction VARCHAR(30),
    actual_direction    VARCHAR(30),            -- derived from subsequent composite_score delta
    direction_correct   BOOLEAN,
    predicted_score     FLOAT,
    outcome_score_delta FLOAT,                  -- composite_score Δ over evaluation window
    evaluated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE(signal_id, evaluation_window_h)
);

CREATE INDEX IF NOT EXISTS idx_outcomes_signal_id
    ON signal_outcomes (signal_id);
CREATE INDEX IF NOT EXISTS idx_outcomes_ticker_window
    ON signal_outcomes (ticker, evaluation_window_h, evaluated_at DESC);
CREATE INDEX IF NOT EXISTS idx_outcomes_correct
    ON signal_outcomes (direction_correct, evaluation_window_h);
CREATE INDEX IF NOT EXISTS idx_outcomes_evaluated_at
    ON signal_outcomes (evaluated_at DESC);

-- ─── Backtest Runs ────────────────────────────────────────────────────────────
-- One row per backtest execution (on-demand or scheduled).
CREATE TABLE IF NOT EXISTS backtest_runs (
    id                BIGSERIAL PRIMARY KEY,
    run_id            UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
    engine_name       VARCHAR(100),            -- NULL = all engines
    window_days       INT NOT NULL DEFAULT 30,
    started_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at      TIMESTAMPTZ,
    status            VARCHAR(20) NOT NULL DEFAULT 'running', -- running | completed | failed
    total_signals     INT NOT NULL DEFAULT 0,
    evaluated_signals INT NOT NULL DEFAULT 0,
    accuracy_4h       FLOAT,
    accuracy_24h      FLOAT,
    accuracy_72h      FLOAT,
    parameters        JSONB NOT NULL DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS idx_bt_runs_status
    ON backtest_runs (status, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_bt_runs_engine
    ON backtest_runs (engine_name, started_at DESC);

-- ─── Backtest Results ─────────────────────────────────────────────────────────
-- Per-signal outcome detail for each backtest run.
CREATE TABLE IF NOT EXISTS backtest_results (
    id                  BIGSERIAL PRIMARY KEY,
    run_id              UUID   NOT NULL REFERENCES backtest_runs(run_id) ON DELETE CASCADE,
    signal_id           BIGINT NOT NULL REFERENCES signals_registry(id),
    ticker              VARCHAR(20),
    engine_name         VARCHAR(100),
    predicted_direction VARCHAR(30),
    actual_direction    VARCHAR(30),
    direction_correct   BOOLEAN,
    predicted_score     FLOAT,
    outcome_score_delta FLOAT,
    window_hours        INT NOT NULL,
    evaluated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_bt_results_run_id
    ON backtest_results (run_id, evaluated_at DESC);
CREATE INDEX IF NOT EXISTS idx_bt_results_signal
    ON backtest_results (signal_id, window_hours);
CREATE INDEX IF NOT EXISTS idx_bt_results_correct
    ON backtest_results (direction_correct, window_hours);
CREATE INDEX IF NOT EXISTS idx_bt_results_engine
    ON backtest_results (engine_name, window_hours, direction_correct);

-- ─── Engine Performance Metrics ───────────────────────────────────────────────
-- Daily accuracy rollup per engine. One row per engine per day.
-- Replaces or upserts — used by Grafana for time-series accuracy charts.
CREATE TABLE IF NOT EXISTS engine_performance_metrics (
    id                  BIGSERIAL PRIMARY KEY,
    engine_name         VARCHAR(100) NOT NULL,
    metric_date         DATE NOT NULL,
    total_signals       INT NOT NULL DEFAULT 0,
    accuracy_4h         FLOAT,          -- directional accuracy at 4h window
    accuracy_24h        FLOAT,          -- directional accuracy at 24h window
    accuracy_72h        FLOAT,          -- directional accuracy at 72h window
    avg_confidence      FLOAT,          -- mean confidence_score for signals this day
    high_conf_accuracy  FLOAT,          -- accuracy for signals with confidence_level='high'
    signal_count_by_type JSONB NOT NULL DEFAULT '{}',  -- {signal_type: count}
    computed_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE(engine_name, metric_date)
);

CREATE INDEX IF NOT EXISTS idx_eng_perf_engine_date
    ON engine_performance_metrics (engine_name, metric_date DESC);
CREATE INDEX IF NOT EXISTS idx_eng_perf_date
    ON engine_performance_metrics (metric_date DESC);

-- ─── Archiver Watermarks ──────────────────────────────────────────────────────
-- Internal state for signal-archiver: tracks per-table progress.
-- Enables crash recovery and deterministic restart.
CREATE TABLE IF NOT EXISTS archiver_watermarks (
    source_table      VARCHAR(100) PRIMARY KEY,
    last_archived_id  BIGINT,           -- for append-only tables (institutional_flows, etc.)
    last_archived_at  TIMESTAMPTZ,      -- for upsert-based tables (market_trends, etc.)
    rows_archived     BIGINT NOT NULL DEFAULT 0,
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Seed watermarks for all source tables
INSERT INTO archiver_watermarks (source_table, last_archived_id, last_archived_at) VALUES
    ('institutional_flows',  0,    NULL),
    ('etf_rotations',        0,    NULL),
    ('macro_regimes',        0,    NULL),
    ('confluence_signals',   0,    NULL),
    ('market_trends',        NULL, now() - INTERVAL '2 hours'),
    ('narrative_clusters',   NULL, now() - INTERVAL '2 hours')
ON CONFLICT (source_table) DO NOTHING;

-- ─── Migration Tracker ────────────────────────────────────────────────────────
INSERT INTO schema_migrations (version) VALUES ('004_level4_validation')
ON CONFLICT DO NOTHING;
