-- Sprint 62: allow one open position per (asset, mode) simultaneously.
-- Previously the index was on asset only, blocking the paper-sim container
-- from opening an asset already held live (and vice versa).
DROP INDEX IF EXISTS idx_paper_trades_single_open;
CREATE UNIQUE INDEX IF NOT EXISTS idx_paper_trades_single_open
    ON paper_trades(asset, mode)
    WHERE closed_at IS NULL;
