-- Sprint 101: persist Binance stop/fill order IDs so they survive restart.
-- Without these, pos.stop_order_id = None after reload → cancel_stop_order
-- is skipped → ghost resting stop stays live → double-close risk.
ALTER TABLE paper_trades ADD COLUMN IF NOT EXISTS stop_order_id    TEXT;
ALTER TABLE paper_trades ADD COLUMN IF NOT EXISTS exchange_order_id TEXT;
