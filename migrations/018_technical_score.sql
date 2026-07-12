-- Migration 018: Add technical_score to market_trends
-- Idempotent: safe to re-run

ALTER TABLE market_trends
    ADD COLUMN IF NOT EXISTS technical_score double precision NOT NULL DEFAULT 0.0;

CREATE INDEX IF NOT EXISTS idx_trends_technical ON market_trends (technical_score DESC);

COMMENT ON COLUMN market_trends.technical_score IS
    'Price-action technical signal [-1,+1]: RSI + momentum + Bollinger Band + volume anomaly';
