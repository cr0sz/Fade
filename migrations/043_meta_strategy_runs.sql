CREATE TABLE IF NOT EXISTS meta_strategy_runs (
    id                BIGSERIAL PRIMARY KEY,
    run_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    regime            VARCHAR(50),
    regime_confidence FLOAT,
    transition_active BOOLEAN DEFAULT FALSE,
    weights           JSONB NOT NULL DEFAULT '{}',
    inputs            JSONB NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_meta_runs_at ON meta_strategy_runs (run_at DESC);
