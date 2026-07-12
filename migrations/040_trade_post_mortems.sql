CREATE TABLE IF NOT EXISTS trade_post_mortems (
    id              BIGSERIAL PRIMARY KEY,
    trade_id        TEXT NOT NULL,
    asset           TEXT NOT NULL,
    direction       TEXT NOT NULL,
    pnl             DOUBLE PRECISION,
    verdict         TEXT,   -- won_correctly|won_despite_signal|lost_correctly|lost_despite_signal
    key_mistake     TEXT,
    lesson          TEXT,
    regime_fit      TEXT,   -- good|neutral|bad
    should_have_traded BOOLEAN,
    regime          TEXT,
    model           TEXT,
    latency_ms      INTEGER,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_postmortem_created ON trade_post_mortems (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_postmortem_asset   ON trade_post_mortems (asset, created_at DESC);
