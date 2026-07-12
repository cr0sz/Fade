-- Sprint 38f: trade reflection memory (TradingAgents-style learning loop)
--
-- Every closed trade generates a short LLM "lesson" tied to the trade facts.
-- Recent same-asset and global lessons are injected into the LLM trade
-- auditor's dossier, so each new audit is informed by realized outcomes —
-- adaptive learning without retraining (cf. TauricResearch/TradingAgents
-- decision log + reflection mechanism).

CREATE TABLE IF NOT EXISTS trade_reflections (
    id            BIGSERIAL PRIMARY KEY,
    trade_id      TEXT NOT NULL,
    asset         TEXT NOT NULL,
    direction     TEXT NOT NULL,
    router_action TEXT,
    close_reason  TEXT,
    pnl           DOUBLE PRECISION,
    hold_hours    DOUBLE PRECISION,
    lesson        TEXT,
    model         TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_reflections_asset ON trade_reflections (asset, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_reflections_created ON trade_reflections (created_at DESC);
