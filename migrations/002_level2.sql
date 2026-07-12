-- ============================================================
-- Migration: 002_level2
-- Level 2 Extension: Confidence + Narrative + Smoothing
-- Created: 2026-05-28
-- ============================================================

-- ─── Extend market_trends with Level-2 overlay columns ────────────────────────
ALTER TABLE market_trends
    ADD COLUMN IF NOT EXISTS smoothed_composite  FLOAT,
    ADD COLUMN IF NOT EXISTS confidence_score     FLOAT DEFAULT 0.0,
    ADD COLUMN IF NOT EXISTS confidence_level     VARCHAR(10) DEFAULT 'low',
    ADD COLUMN IF NOT EXISTS narrative_cluster    VARCHAR(100),
    ADD COLUMN IF NOT EXISTS signal_quality       VARCHAR(10) DEFAULT 'low';

CREATE INDEX IF NOT EXISTS idx_trends_confidence ON market_trends(confidence_score DESC);
CREATE INDEX IF NOT EXISTS idx_trends_narrative ON market_trends(narrative_cluster);

-- ─── Narrative clusters table ─────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS narrative_clusters (
    id              BIGSERIAL PRIMARY KEY,
    name            VARCHAR(100) NOT NULL,        -- ai_semiconductors | macro_liquidity | …
    label           VARCHAR(200) NOT NULL,
    strength_score  FLOAT NOT NULL DEFAULT 0.0,
    momentum        VARCHAR(20) NOT NULL DEFAULT 'neutral',
    momentum_delta  FLOAT NOT NULL DEFAULT 0.0,
    top_tickers     TEXT[] NOT NULL DEFAULT '{}',
    contrib_sources TEXT[] NOT NULL DEFAULT '{}',
    sample_headlines TEXT[] NOT NULL DEFAULT '{}',
    computed_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE(name)
);

CREATE INDEX IF NOT EXISTS idx_narrative_strength ON narrative_clusters(strength_score DESC);
CREATE INDEX IF NOT EXISTS idx_narrative_computed_at ON narrative_clusters(computed_at DESC);

-- ─── Noise filter audit log (for bot-detection traceability) ──────────────────
CREATE TABLE IF NOT EXISTS noise_filter_log (
    id          BIGSERIAL PRIMARY KEY,
    platform    VARCHAR(50) NOT NULL,
    external_id VARCHAR(200) NOT NULL,
    reason      VARCHAR(100) NOT NULL,  -- low_engagement | low_quality_sub | bot_burst | …
    ticker      VARCHAR(20),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_noise_log_created_at ON noise_filter_log(created_at DESC);

-- ─── Migrations tracker ────────────────────────────────────────────────────────
INSERT INTO schema_migrations (version) VALUES ('002_level2')
ON CONFLICT DO NOTHING;
