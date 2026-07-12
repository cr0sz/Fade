-- 038_equity_snapshots.sql
-- Equity curve tracking for drawdown/Sharpe reporting.
-- Written every 30min by equity_tracker.py (Tier 2D).
CREATE TABLE IF NOT EXISTS equity_snapshots (
    id               BIGSERIAL PRIMARY KEY,
    mode             TEXT          NOT NULL DEFAULT 'paper',
    balance          NUMERIC(18,4) NOT NULL,
    equity           NUMERIC(18,4) NOT NULL,
    drawdown_pct     NUMERIC(8,5)  NOT NULL DEFAULT 0,
    governor_state   TEXT          NOT NULL DEFAULT 'NORMAL',
    regime           TEXT,
    cluster_edges    JSONB         NOT NULL DEFAULT '{}'::jsonb,
    captured_at      TIMESTAMPTZ   NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_equity_snapshots_captured
    ON equity_snapshots (mode, captured_at DESC);
