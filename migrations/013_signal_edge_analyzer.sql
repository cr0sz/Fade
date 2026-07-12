-- ============================================================
-- Migration: 013_signal_edge_analyzer
-- Signal Edge Detection Module — analytics tables
-- Created: 2026-05-30
--
-- Depends on: _utc_date() from 005_signal_truth_layer
--             signals_registry from 004_level4_validation
--             signal_outcomes from 004_level4_validation
-- ============================================================

-- ─── Signal Edge Metrics ──────────────────────────────────────────────────────
-- Rolling statistical quality metrics per engine/type/ticker/direction/window.
-- Populated by EdgeAnalyzer on each hourly metrics cycle.
-- One row per group + window per UTC calendar day (upsert).
CREATE TABLE IF NOT EXISTS signal_edge_metrics (
    id              BIGSERIAL    PRIMARY KEY,
    engine_name     VARCHAR(100) NOT NULL,
    signal_type     VARCHAR(50)  NOT NULL,
    ticker          VARCHAR(20),              -- NULL = market-wide / all tickers
    direction       VARCHAR(30),              -- NULL = all directions
    window_hours    INT          NOT NULL,    -- 1 | 6 | 24 | 72 | 168
    period_days     INT          NOT NULL DEFAULT 30,
    sample_size     INT          NOT NULL DEFAULT 0,
    win_rate        FLOAT,                    -- fraction of correct direction calls (0.0–1.0)
    avg_return      FLOAT,                    -- mean outcome_score_delta over period
    avg_win         FLOAT,                    -- mean return of correct calls
    avg_loss        FLOAT,                    -- mean return of incorrect calls (negative)
    expectancy      FLOAT,                    -- win_rate*avg_win + (1-win_rate)*avg_loss
    max_drawdown    FLOAT,                    -- max peak-to-trough loss in cumulative return series
    volatility      FLOAT,                    -- std dev of individual outcome_score_deltas
    sharpe_score    FLOAT,                    -- avg_return / volatility (simplified, annualisation-free)
    quality_score   FLOAT,                    -- 0.5*expectancy + 0.3*win_rate + 0.2*sharpe_score
    computed_at     TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_edge_metrics_daily_uq
    ON signal_edge_metrics (
        engine_name,
        signal_type,
        COALESCE(ticker,    ''),
        COALESCE(direction, ''),
        window_hours,
        (_utc_date(computed_at))
    );

CREATE INDEX IF NOT EXISTS idx_edge_metrics_quality
    ON signal_edge_metrics (quality_score DESC NULLS LAST, computed_at DESC);

CREATE INDEX IF NOT EXISTS idx_edge_metrics_engine_window
    ON signal_edge_metrics (engine_name, window_hours, computed_at DESC);

CREATE INDEX IF NOT EXISTS idx_edge_metrics_ticker
    ON signal_edge_metrics (ticker, window_hours, computed_at DESC);

-- ─── Signal Edge Aggregations ─────────────────────────────────────────────────
-- Ranked strategy combinations (best performing engine × ticker × direction × window).
-- Rebuilt on each 24h aggregation cycle.
-- Used to produce daily Telegram top-5 strategy report.
CREATE TABLE IF NOT EXISTS signal_edge_aggregations (
    id              BIGSERIAL    PRIMARY KEY,
    rank_position   INT          NOT NULL,
    group_key       VARCHAR(300) NOT NULL,  -- "engine|ticker|direction|Xh"
    engine_name     VARCHAR(100) NOT NULL,
    ticker          VARCHAR(20),
    direction       VARCHAR(30),
    window_hours    INT          NOT NULL,
    period_days     INT          NOT NULL DEFAULT 30,
    sample_size     INT          NOT NULL DEFAULT 0,
    quality_score   FLOAT        NOT NULL DEFAULT 0,
    win_rate        FLOAT,
    avg_return      FLOAT,
    expectancy      FLOAT,
    sharpe_score    FLOAT,
    computed_at     TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_edge_agg_rank
    ON signal_edge_aggregations (rank_position, computed_at DESC);

CREATE INDEX IF NOT EXISTS idx_edge_agg_quality
    ON signal_edge_aggregations (quality_score DESC, computed_at DESC);

CREATE INDEX IF NOT EXISTS idx_edge_agg_computed
    ON signal_edge_aggregations (computed_at DESC);

-- ─── Migration Tracker ────────────────────────────────────────────────────────
INSERT INTO schema_migrations (version) VALUES ('013_signal_edge_analyzer')
ON CONFLICT DO NOTHING;
