-- Sprint 61: store daily briefs for dashboard display
CREATE TABLE IF NOT EXISTS daily_briefs (
    id          SERIAL PRIMARY KEY,
    generated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    content     TEXT NOT NULL,
    model_used  TEXT NOT NULL DEFAULT 'qwen2.5:7b',
    trade_count INT,
    total_pnl   NUMERIC(14,2),
    btc_price   NUMERIC(18,2)
);
CREATE INDEX IF NOT EXISTS idx_daily_briefs_generated_at ON daily_briefs(generated_at DESC);
