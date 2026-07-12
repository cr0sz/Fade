-- 024_research_runs.sql
-- Free-time fade-sweep researcher (Sprint 48). The freetime-researcher service
-- continuously backtests fade-strategy parameter variants in spare compute,
-- yielding to Ollama whenever a model is loaded. Each completed param combo is
-- recorded here once (UNIQUE on sweep_name + params hash) so restarts resume
-- the grid instead of repeating work. RECOMMEND-ONLY: nothing trades off this.

CREATE TABLE IF NOT EXISTS research_runs (
    id               BIGSERIAL PRIMARY KEY,
    sweep_name       TEXT NOT NULL,
    params           JSONB NOT NULL,
    n_trades         INT,
    win_rate         DOUBLE PRECISION,
    avg_ret_pct      DOUBLE PRECISION,
    total_ret_pct    DOUBLE PRECISION,
    sharpe           DOUBLE PRECISION,
    max_dd_pct       DOUBLE PRECISION,
    oos_avg_ret_pct  DOUBLE PRECISION,
    oos_n            INT,
    status           TEXT,
    started_at       TIMESTAMPTZ,
    finished_at      TIMESTAMPTZ,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_research_runs_sweep_created
    ON research_runs (sweep_name, created_at DESC);

-- Each combo runs once per sweep version. md5 of the canonical params text is
-- the natural dedup key; the queue INSERTs status='running' and finalizes.
CREATE UNIQUE INDEX IF NOT EXISTS uq_research_runs_sweep_params
    ON research_runs (sweep_name, md5(params::text));
