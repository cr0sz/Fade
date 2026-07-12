-- signal_trace was always written with a ticker value (src/core/signal_tracer.py)
-- but the column was never created in 011 — every INSERT has failed silently.
ALTER TABLE signal_trace ADD COLUMN IF NOT EXISTS ticker TEXT;

CREATE INDEX IF NOT EXISTS idx_st_ticker ON signal_trace(ticker, created_at DESC);
