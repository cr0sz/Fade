-- Migration 020: optimization_runs table for weekly parameter sweep results (Pillar 5)
CREATE TABLE IF NOT EXISTS optimization_runs (
    id          SERIAL PRIMARY KEY,
    params      JSONB        NOT NULL,
    score       FLOAT        NOT NULL,
    win_rate    FLOAT,
    sharpe      FLOAT,
    expectancy  FLOAT,
    trade_count INT,
    notes       TEXT,
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_optimization_runs_score ON optimization_runs (score DESC);
CREATE INDEX IF NOT EXISTS idx_optimization_runs_created ON optimization_runs (created_at DESC);
