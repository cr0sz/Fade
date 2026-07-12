-- Sprint 38: Truth Router + LLM trade auditor (shadow mode)
--
-- paper_trades.router_action / router_edge: how the evidence router handled the
-- originating signal (confirm | invert | passthrough) and the measured lift, so
-- routed vs legacy trade performance is directly queryable.
--
-- llm_trade_audits: TradingAgents-style bull/bear/judge verdict per opened trade,
-- produced by the in-network Ollama model. Shadow mode only — verdicts have no
-- authority until their discrimination is proven against price outcomes.

ALTER TABLE paper_trades ADD COLUMN IF NOT EXISTS router_action TEXT;
ALTER TABLE paper_trades ADD COLUMN IF NOT EXISTS router_edge DOUBLE PRECISION;

CREATE TABLE IF NOT EXISTS llm_trade_audits (
    id            BIGSERIAL PRIMARY KEY,
    trade_id      TEXT NOT NULL,
    signal_id     TEXT NOT NULL,
    asset         TEXT NOT NULL,
    direction     TEXT NOT NULL,
    model         TEXT NOT NULL,
    verdict       TEXT,                -- approve | reject | error
    confidence    DOUBLE PRECISION,    -- model's self-reported 0-1
    bull_case     TEXT,
    bear_case     TEXT,
    raw_response  TEXT,                -- unparsed model output for debugging
    latency_ms    INTEGER,
    dossier       JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_llm_audits_trade ON llm_trade_audits (trade_id);
CREATE INDEX IF NOT EXISTS idx_llm_audits_created ON llm_trade_audits (created_at DESC);
