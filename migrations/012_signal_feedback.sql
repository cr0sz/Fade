-- Migration 012: Signal Feedback Loop
-- Closed-loop learning layer: signal_trace → evaluation → engine/provider scoring
--
-- Creates:
--   signal_feedback            — per-trace outcome rows (win/loss/neutral/pending)
--   provider_performance_metrics — daily quality rollup per provider
-- Extends:
--   engine_performance_metrics  — adds win_rate columns from feedback loop

-- ─── signal_feedback ─────────────────────────────────────────────────────────
-- One row per (signal_trace, evaluation_window_h).
-- Written by SignalEvaluationEngine; never modified after insert.
CREATE TABLE IF NOT EXISTS signal_feedback (
    id                   UUID         NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
    signal_trace_id      UUID         NOT NULL,  -- references signal_trace.id
    cycle_id             TEXT         NOT NULL,
    engine_name          TEXT         NOT NULL,
    provider             TEXT,
    signal_type          TEXT         NOT NULL,  -- trend/macro/confluence/scoring
    outcome_type         TEXT         NOT NULL DEFAULT 'pending',
    pnl_score            DOUBLE PRECISION,       -- proxy: +1→win full, -0.5→loss, 0→neutral
    direction_correct    BOOLEAN,
    confidence_score     DOUBLE PRECISION,       -- signal_strength at time of trace
    evaluation_window_h  INT          NOT NULL,
    market_context       JSONB        NOT NULL DEFAULT '{}',
    evaluated_at         TIMESTAMPTZ  NOT NULL DEFAULT now(),
    UNIQUE (signal_trace_id, evaluation_window_h),
    CONSTRAINT chk_sf_outcome CHECK (outcome_type IN ('win', 'loss', 'neutral', 'pending'))
);

CREATE INDEX IF NOT EXISTS idx_sf_trace_id     ON signal_feedback (signal_trace_id);
CREATE INDEX IF NOT EXISTS idx_sf_cycle_id     ON signal_feedback (cycle_id);
CREATE INDEX IF NOT EXISTS idx_sf_engine_name  ON signal_feedback (engine_name, evaluated_at DESC);
CREATE INDEX IF NOT EXISTS idx_sf_provider     ON signal_feedback (provider, evaluated_at DESC);
CREATE INDEX IF NOT EXISTS idx_sf_signal_type  ON signal_feedback (signal_type, evaluated_at DESC);
CREATE INDEX IF NOT EXISTS idx_sf_outcome_type ON signal_feedback (outcome_type, engine_name);
CREATE INDEX IF NOT EXISTS idx_sf_evaluated_at ON signal_feedback (evaluated_at DESC);

-- ─── provider_performance_metrics ────────────────────────────────────────────
-- Daily quality rollup per provider.  Upserted by FeedbackAggregator.
CREATE TABLE IF NOT EXISTS provider_performance_metrics (
    id               BIGSERIAL    PRIMARY KEY,
    provider         TEXT         NOT NULL,
    metric_date      DATE         NOT NULL,
    total_signals    INT          NOT NULL DEFAULT 0,
    win_count        INT          NOT NULL DEFAULT 0,
    loss_count       INT          NOT NULL DEFAULT 0,
    neutral_count    INT          NOT NULL DEFAULT 0,
    win_rate         DOUBLE PRECISION,
    avg_pnl_score    DOUBLE PRECISION,
    noise_ratio      DOUBLE PRECISION,  -- (dup_exact+dup_fuzzy) / max(fetched,1)
    avg_latency_ms   DOUBLE PRECISION,
    computed_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    UNIQUE (provider, metric_date)
);

CREATE INDEX IF NOT EXISTS idx_ppm_provider_date ON provider_performance_metrics (provider, metric_date DESC);
CREATE INDEX IF NOT EXISTS idx_ppm_win_rate       ON provider_performance_metrics (win_rate DESC, metric_date DESC);
CREATE INDEX IF NOT EXISTS idx_ppm_metric_date    ON provider_performance_metrics (metric_date DESC);

-- ─── Extend engine_performance_metrics ───────────────────────────────────────
ALTER TABLE engine_performance_metrics
    ADD COLUMN IF NOT EXISTS win_rate_1h            DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS win_rate_24h           DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS win_rate_7d            DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS avg_pnl_score          DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS feedback_signals_total INT DEFAULT 0;

-- ─── Migration tracking ───────────────────────────────────────────────────────
INSERT INTO schema_migrations (version, applied_at)
VALUES ('012_signal_feedback', NOW())
ON CONFLICT DO NOTHING;
