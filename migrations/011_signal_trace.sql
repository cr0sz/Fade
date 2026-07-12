-- Signal Traceability Layer
-- Links news → provider → engine → signal → outcome in a single audit table.
-- Append-only, never modified after insert.

CREATE TABLE IF NOT EXISTS signal_trace (
    id               UUID         NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
    cycle_id         TEXT         NOT NULL,
    news_id          TEXT,                       -- source news item (if applicable)
    provider         TEXT,                       -- news/data provider name
    engine_name      TEXT         NOT NULL,      -- producing engine
    signal_id        TEXT,                       -- FK-like ref to signals_registry.id
    signal_type      TEXT         NOT NULL,      -- trend / macro / confluence / scoring
    signal_strength  DOUBLE PRECISION,           -- 0.0–100.0 or engine-specific
    signal_direction TEXT,                       -- risk_on / risk_off / mixed / bullish / bearish
    input_data_hash  TEXT,                       -- SHA-256 of input snapshot (for dedup audit)
    output_data_hash TEXT,                       -- SHA-256 of output snapshot
    latency_ms       DOUBLE PRECISION,           -- engine compute time for this signal
    created_at       TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_st_cycle_id    ON signal_trace(cycle_id);
CREATE INDEX IF NOT EXISTS idx_st_signal_id   ON signal_trace(signal_id);
CREATE INDEX IF NOT EXISTS idx_st_provider    ON signal_trace(provider, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_st_engine_name ON signal_trace(engine_name, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_st_created_at  ON signal_trace(created_at DESC);
