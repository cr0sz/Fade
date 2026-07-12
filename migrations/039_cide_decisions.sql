CREATE TABLE IF NOT EXISTS cide_decisions (
    id               BIGSERIAL PRIMARY KEY,
    requested_capital NUMERIC(12,4) NOT NULL,
    decision         TEXT NOT NULL,
    recommended_capital NUMERIC(12,4) NOT NULL DEFAULT 0,
    edge_stability_score NUMERIC(8,6) NOT NULL,
    metrics          JSONB NOT NULL DEFAULT '{}'::jsonb,
    reasoning        JSONB NOT NULL DEFAULT '[]'::jsonb,
    evaluated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_cide_decisions_evaluated
    ON cide_decisions (evaluated_at DESC);
