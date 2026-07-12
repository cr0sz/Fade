-- Migration 016: paper_trades persistent table
--
-- Stores every trade opened/closed by the paper trading engine.
-- Opened trades have closed_at = NULL; closes update the row in place.
-- Also used by Telegram /positions and /pnl commands.

CREATE TABLE IF NOT EXISTS paper_trades (
    id           TEXT        PRIMARY KEY,         -- PaperTrade.trade_id (UUID string)
    signal_id    TEXT        NOT NULL,
    asset        TEXT        NOT NULL,
    exchange     TEXT        NOT NULL DEFAULT '',
    direction    TEXT        NOT NULL,            -- long | short
    entry_price  DOUBLE PRECISION NOT NULL,
    exit_price   DOUBLE PRECISION,
    size         DOUBLE PRECISION NOT NULL,
    notional     DOUBLE PRECISION NOT NULL,
    fee          DOUBLE PRECISION NOT NULL DEFAULT 0,
    pnl          DOUBLE PRECISION,
    close_reason TEXT,                            -- opposing_signal | max_hold_exceeded
    mode         TEXT        NOT NULL DEFAULT 'paper',  -- paper | live
    opened_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    closed_at    TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_pt_asset       ON paper_trades(asset, opened_at DESC);
CREATE INDEX IF NOT EXISTS idx_pt_closed_at   ON paper_trades(closed_at DESC) WHERE closed_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_pt_open        ON paper_trades(opened_at DESC) WHERE closed_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_pt_signal_id   ON paper_trades(signal_id);
