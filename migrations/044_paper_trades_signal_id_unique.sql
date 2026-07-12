-- Migration 044: partial unique index on paper_trades(signal_id) for open positions
-- Prevents duplicate open trades for the same signal on crash-replay.
-- Partial (WHERE closed_at IS NULL) so the same signal_id can appear in history
-- (closed trades) without conflicting with a newly opened position.
CREATE UNIQUE INDEX IF NOT EXISTS idx_pt_signal_id_open
    ON paper_trades(signal_id)
    WHERE closed_at IS NULL;
