-- Migration 021: governance_log table for Lessons Learned Engine audit trail
CREATE TABLE IF NOT EXISTS governance_log (
    id              SERIAL PRIMARY KEY,
    run_id          TEXT NOT NULL,          -- UUID per pipeline invocation
    action          TEXT NOT NULL,          -- governance action type
    target          TEXT NOT NULL,          -- what was changed
    target_type     TEXT,                   -- engine / symbol / threshold / size
    reason          TEXT,
    evidence        TEXT,
    expected_effect TEXT,
    applied         BOOLEAN NOT NULL DEFAULT false,
    applied_at      TIMESTAMPTZ,
    skipped_reason  TEXT,                   -- why action was skipped if not applied
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_governance_log_run ON governance_log (run_id);
CREATE INDEX IF NOT EXISTS idx_governance_log_created ON governance_log (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_governance_log_target ON governance_log (target, target_type);

-- Stores the full lessons_learned.json outputs for audit and replay
CREATE TABLE IF NOT EXISTS lessons_learned_runs (
    id              SERIAL PRIMARY KEY,
    run_id          TEXT NOT NULL UNIQUE,
    data_window_days INT,
    trade_count     INT,
    win_rate        FLOAT,
    total_pnl       FLOAT,
    output_json     JSONB NOT NULL,
    model_used      TEXT,
    input_tokens    INT,
    output_tokens   INT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ll_runs_created ON lessons_learned_runs (created_at DESC);
