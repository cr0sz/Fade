-- ============================================================
-- Migration: 003_level3
-- Level 3 Extension: Institutional Capital Flow Intelligence
-- Created: 2026-05-28
-- ============================================================

-- ─── Institutional Flows (append-only time-series) ────────────────────────────
-- Records per-ticker capital flow direction, strength, persistence, and
-- insider clustering. Query latest per ticker with DISTINCT ON (ticker).
CREATE TABLE IF NOT EXISTS institutional_flows (
    id                  BIGSERIAL PRIMARY KEY,
    ticker              VARCHAR(20) NOT NULL,        -- NULL-safe: 'MARKET' = market-wide
    direction           VARCHAR(20) NOT NULL,         -- inflow | outflow | neutral
    flow_strength       FLOAT  NOT NULL DEFAULT 0.0,  -- 0–100 institutional intensity index
    persistence_score   FLOAT  NOT NULL DEFAULT 0.0,  -- 0–1 multi-day confirmation
    acceleration        VARCHAR(20) NOT NULL DEFAULT 'stable',  -- building | fading | stable
    insider_cluster     VARCHAR(30) NOT NULL DEFAULT 'neutral', -- bullish_cluster | neutral | bearish_distribution
    whale_buy_usd       FLOAT  NOT NULL DEFAULT 0.0,
    whale_sell_usd      FLOAT  NOT NULL DEFAULT 0.0,
    insider_buy_count   INT    NOT NULL DEFAULT 0,
    insider_sell_count  INT    NOT NULL DEFAULT 0,
    net_flow_ratio      FLOAT  NOT NULL DEFAULT 0.0,  -- -1 to 1
    computed_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_flows_ticker_time
    ON institutional_flows (ticker, computed_at DESC);
CREATE INDEX IF NOT EXISTS idx_flows_direction
    ON institutional_flows (direction, computed_at DESC);
CREATE INDEX IF NOT EXISTS idx_flows_computed_at
    ON institutional_flows (computed_at DESC);

-- ─── ETF Rotations (append-only time-series) ──────────────────────────────────
-- Records per-sector ETF composite scores and rotation velocity each cycle.
CREATE TABLE IF NOT EXISTS etf_rotations (
    id                  BIGSERIAL PRIMARY KEY,
    sector              VARCHAR(50) NOT NULL,
    etf_ticker          VARCHAR(20) NOT NULL,
    composite_score     FLOAT  NOT NULL DEFAULT 0.0,
    rotation_velocity   FLOAT  NOT NULL DEFAULT 0.0,  -- delta vs prev cycle
    rotation_direction  VARCHAR(20) NOT NULL DEFAULT 'neutral', -- inflow | outflow | neutral
    rank_position       INT,                           -- 1 = leading sector this cycle
    is_leading          BOOLEAN NOT NULL DEFAULT false,
    is_lagging          BOOLEAN NOT NULL DEFAULT false,
    computed_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_etf_sector_time
    ON etf_rotations (sector, computed_at DESC);
CREATE INDEX IF NOT EXISTS idx_etf_computed_at
    ON etf_rotations (computed_at DESC);

-- ─── Macro Regimes (append-only time-series) ──────────────────────────────────
-- One row per detection cycle — query latest with ORDER BY computed_at DESC LIMIT 1.
CREATE TABLE IF NOT EXISTS macro_regimes (
    id                  BIGSERIAL PRIMARY KEY,
    regime              VARCHAR(30) NOT NULL,   -- risk_on | risk_off | tightening | liquidity_expansion | volatility_regime
    confidence          FLOAT  NOT NULL DEFAULT 0.0,   -- 0–1
    previous_regime     VARCHAR(30),
    regime_shift        BOOLEAN NOT NULL DEFAULT false,
    dominant_narrative  VARCHAR(100),            -- top narrative driving regime
    narrative_score     FLOAT  NOT NULL DEFAULT 0.0,
    proxy_score         FLOAT  NOT NULL DEFAULT 0.0,   -- from VIX/GLD/TLT/SPY signals
    keyword_score       FLOAT  NOT NULL DEFAULT 0.0,   -- from news keyword matching
    regime_scores       JSONB  NOT NULL DEFAULT '{}',  -- full {regime: score} map
    computed_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_regime_computed_at
    ON macro_regimes (computed_at DESC);
CREATE INDEX IF NOT EXISTS idx_regime_name
    ON macro_regimes (regime, computed_at DESC);

-- ─── Confluence Signals (append-only time-series) ─────────────────────────────
-- Combined institutional intelligence per ticker (or market-wide when ticker IS NULL).
CREATE TABLE IF NOT EXISTS confluence_signals (
    id                      BIGSERIAL PRIMARY KEY,
    ticker                  VARCHAR(20),                -- NULL = market-wide signal
    strength                FLOAT  NOT NULL DEFAULT 0.0, -- 0–100
    direction               VARCHAR(20) NOT NULL DEFAULT 'mixed', -- risk_on | risk_off | mixed
    narrative_alignment     FLOAT  NOT NULL DEFAULT 0.0, -- 0–1 narrative contribution
    confidence_weighted     FLOAT  NOT NULL DEFAULT 0.0, -- 0–1 from ConfidenceEngine
    flow_alignment          FLOAT  NOT NULL DEFAULT 0.0, -- 0–1 institutional flow contribution
    regime_coherence        FLOAT  NOT NULL DEFAULT 0.0, -- 0–1 macro regime alignment
    contributing_sources    JSONB  NOT NULL DEFAULT '{}', -- breakdown by source type
    computed_at             TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_confluence_ticker_time
    ON confluence_signals (ticker, computed_at DESC);
CREATE INDEX IF NOT EXISTS idx_confluence_direction
    ON confluence_signals (direction, computed_at DESC);
CREATE INDEX IF NOT EXISTS idx_confluence_computed_at
    ON confluence_signals (computed_at DESC);
CREATE INDEX IF NOT EXISTS idx_confluence_strength
    ON confluence_signals (strength DESC, computed_at DESC);

-- ─── Migrations tracker ────────────────────────────────────────────────────────
INSERT INTO schema_migrations (version) VALUES ('003_level3')
ON CONFLICT DO NOTHING;
