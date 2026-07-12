-- Sprint 61: prevent duplicate open positions on engine restart (dedup_cleanup race)
-- A partial unique index rejects a second INSERT for the same asset while one is open,
-- making the open path idempotent against in-memory/DB desync across restarts.
CREATE UNIQUE INDEX IF NOT EXISTS idx_paper_trades_single_open
    ON paper_trades(asset)
    WHERE closed_at IS NULL;
