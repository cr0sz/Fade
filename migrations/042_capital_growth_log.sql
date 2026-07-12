CREATE TABLE IF NOT EXISTS capital_growth_log (
    id              BIGSERIAL PRIMARY KEY,
    phase           SMALLINT NOT NULL,
    equity          NUMERIC(18,4) NOT NULL,
    max_positions   SMALLINT NOT NULL,
    leverage_cap    SMALLINT NOT NULL,
    size_multiplier NUMERIC(6,3) NOT NULL,
    captured_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_growth_captured ON capital_growth_log (captured_at DESC);
