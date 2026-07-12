-- ============================================================
-- Ticker Quotes Time-Series Table
-- Migration: 015_ticker_quotes
-- Created: 2026-05-31
-- ============================================================
-- Stores per-tick OHLCV snapshots from CoinGecko, Finnhub, Polygon.
-- Used by TrendDetectionEngine for volume_score (real trading volume)
-- and by the paper trading engine for realistic exit price lookups.

CREATE TABLE IF NOT EXISTS ticker_quotes (
    id          BIGSERIAL PRIMARY KEY,
    symbol      VARCHAR(20)  NOT NULL,
    price       FLOAT        NOT NULL,
    change_pct  FLOAT        NOT NULL DEFAULT 0.0,
    volume      FLOAT        NOT NULL DEFAULT 0.0,
    market_cap  FLOAT,
    exchange    VARCHAR(50)  NOT NULL DEFAULT '',
    currency    VARCHAR(10)  NOT NULL DEFAULT 'USD',
    ts          TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- Lookup by symbol + recency (most common access pattern)
CREATE INDEX IF NOT EXISTS idx_ticker_quotes_symbol_ts
    ON ticker_quotes (symbol, ts DESC);

-- Prune old quotes (keep 7 days; cleaned by daily cleanup job)
CREATE INDEX IF NOT EXISTS idx_ticker_quotes_ts
    ON ticker_quotes (ts DESC);
