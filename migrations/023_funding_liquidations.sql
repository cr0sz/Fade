-- 023_funding_liquidations.sql
-- Passive market-data accumulation for the fade edge: Binance USDT-perp
-- funding rates and all-market liquidation (forceOrder) cascades.
-- Populated by src/market_data_logger. Read-only dataset for a future
-- signal engine — nothing trades off these tables yet.

-- ── Funding rates ────────────────────────────────────────────────────────────
-- Polled every ~5 min from fapi /premiumIndex, one row per universe symbol.
CREATE TABLE IF NOT EXISTS funding_rates (
    id                 BIGSERIAL PRIMARY KEY,
    symbol             TEXT NOT NULL,
    funding_rate       DOUBLE PRECISION,
    mark_price         DOUBLE PRECISION,
    next_funding_time  TIMESTAMPTZ,
    ts                 TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_funding_rates_symbol_ts ON funding_rates (symbol, ts DESC);
CREATE INDEX IF NOT EXISTS idx_funding_rates_ts        ON funding_rates (ts DESC);

-- ── Liquidation events ───────────────────────────────────────────────────────
-- Streamed from !forceOrder@arr (all markets). Stores every symbol; the stream
-- is throttled by Binance to ~1 msg/sec/symbol so volume is bounded.
CREATE TABLE IF NOT EXISTS liquidation_events (
    id            BIGSERIAL PRIMARY KEY,
    symbol        TEXT NOT NULL,
    side          TEXT,
    price         DOUBLE PRECISION,
    qty           DOUBLE PRECISION,
    notional      DOUBLE PRECISION,
    order_status  TEXT,
    event_time    TIMESTAMPTZ,
    ts            TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Dedup: websocket reconnects can replay recent events. The natural key is
-- (symbol, event_time, price, qty); inserts use ON CONFLICT DO NOTHING.
CREATE UNIQUE INDEX IF NOT EXISTS uq_liquidation_events_natural
    ON liquidation_events (symbol, event_time, price, qty);

CREATE INDEX IF NOT EXISTS idx_liquidation_events_symbol_time ON liquidation_events (symbol, event_time DESC);
CREATE INDEX IF NOT EXISTS idx_liquidation_events_time        ON liquidation_events (event_time DESC);
