-- ============================================================
-- AI Market Intelligence Platform — Initial Schema
-- Migration: 001_initial
-- Created: 2026-05-28
-- ============================================================

-- ─── Extensions ───────────────────────────────────────────────────────────────
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";  -- fuzzy text search

-- ─── Tickers ──────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS tickers (
    symbol          VARCHAR(20) PRIMARY KEY,
    name            VARCHAR(500),
    exchange        VARCHAR(50) NOT NULL DEFAULT '',
    sector          VARCHAR(100),
    industry        VARCHAR(200),
    market_cap_usd  FLOAT,
    is_active       BOOLEAN NOT NULL DEFAULT true,
    is_watched      BOOLEAN NOT NULL DEFAULT false,
    metadata        JSONB DEFAULT '{}',
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_tickers_exchange ON tickers(exchange);
CREATE INDEX IF NOT EXISTS idx_tickers_is_watched ON tickers(is_watched) WHERE is_watched = true;
CREATE INDEX IF NOT EXISTS idx_tickers_sector ON tickers(sector);

-- ─── News ─────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS news (
    id              BIGSERIAL PRIMARY KEY,
    title           TEXT NOT NULL,
    content         TEXT,
    summary         TEXT,
    url             TEXT,
    source          VARCHAR(200),
    source_domain   VARCHAR(200),
    author          VARCHAR(200),
    published_at    TIMESTAMPTZ NOT NULL,
    ticker_symbols  TEXT[] NOT NULL DEFAULT '{}',
    sector          VARCHAR(100),
    sentiment_score FLOAT,
    content_hash    VARCHAR(64) UNIQUE NOT NULL,
    ai_summary      TEXT,
    image_url       TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_news_published_at ON news(published_at DESC);
CREATE INDEX IF NOT EXISTS idx_news_ticker_symbols ON news USING GIN(ticker_symbols);
CREATE INDEX IF NOT EXISTS idx_news_sector ON news(sector);
CREATE INDEX IF NOT EXISTS idx_news_source ON news(source);
CREATE INDEX IF NOT EXISTS idx_news_content_hash ON news(content_hash);
CREATE INDEX IF NOT EXISTS idx_news_ai_summary_null ON news(id) WHERE ai_summary IS NULL;

-- Full-text search on title
CREATE INDEX IF NOT EXISTS idx_news_title_trgm ON news USING GIN(title gin_trgm_ops);

-- ─── Social Posts ──────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS social_posts (
    id               BIGSERIAL PRIMARY KEY,
    external_id      VARCHAR(200) NOT NULL,
    platform         VARCHAR(50) NOT NULL,   -- reddit | twitter | stocktwits
    content          TEXT NOT NULL,
    author           VARCHAR(200),
    ticker_mentions  TEXT[] NOT NULL DEFAULT '{}',
    sentiment_score  FLOAT,
    engagement_score FLOAT NOT NULL DEFAULT 0,
    upvotes          INTEGER NOT NULL DEFAULT 0,
    comments         INTEGER NOT NULL DEFAULT 0,
    url              TEXT,
    subreddit        VARCHAR(100),
    created_at       TIMESTAMPTZ NOT NULL,
    ingested_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE(platform, external_id)
);

CREATE INDEX IF NOT EXISTS idx_social_created_at ON social_posts(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_social_ticker_mentions ON social_posts USING GIN(ticker_mentions);
CREATE INDEX IF NOT EXISTS idx_social_platform ON social_posts(platform);
CREATE INDEX IF NOT EXISTS idx_social_engagement ON social_posts(engagement_score DESC);

-- ─── Market Trends ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS market_trends (
    ticker              VARCHAR(20) PRIMARY KEY,
    exchange            VARCHAR(50) NOT NULL DEFAULT 'US',
    news_score          FLOAT NOT NULL DEFAULT 0,
    social_score        FLOAT NOT NULL DEFAULT 0,
    smart_money_score   FLOAT NOT NULL DEFAULT 0,
    volume_score        FLOAT NOT NULL DEFAULT 0,
    composite_score     FLOAT NOT NULL DEFAULT 0,
    hype_signal         BOOLEAN NOT NULL DEFAULT false,
    panic_signal        BOOLEAN NOT NULL DEFAULT false,
    trending_up         BOOLEAN NOT NULL DEFAULT false,
    trending_down       BOOLEAN NOT NULL DEFAULT false,
    computed_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_trends_composite ON market_trends(composite_score DESC);
CREATE INDEX IF NOT EXISTS idx_trends_exchange ON market_trends(exchange);
CREATE INDEX IF NOT EXISTS idx_trends_hype ON market_trends(hype_signal) WHERE hype_signal = true;
CREATE INDEX IF NOT EXISTS idx_trends_panic ON market_trends(panic_signal) WHERE panic_signal = true;

-- ─── Smart Money Events ────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS smart_money_events (
    id          BIGSERIAL PRIMARY KEY,
    event_type  VARCHAR(50) NOT NULL,   -- insider_buy | insider_sell | etf_flow | whale_tx
    ticker      VARCHAR(20),
    actor       VARCHAR(500),           -- person/fund/wallet
    amount_usd  FLOAT,
    shares      BIGINT,
    direction   VARCHAR(10),            -- buy | sell | in | out
    source      VARCHAR(100) NOT NULL,
    metadata    JSONB DEFAULT '{}',
    event_at    TIMESTAMPTZ NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_smart_money_event_at ON smart_money_events(event_at DESC);
CREATE INDEX IF NOT EXISTS idx_smart_money_ticker ON smart_money_events(ticker);
CREATE INDEX IF NOT EXISTS idx_smart_money_event_type ON smart_money_events(event_type);
CREATE INDEX IF NOT EXISTS idx_smart_money_amount ON smart_money_events(amount_usd DESC) WHERE amount_usd IS NOT NULL;

-- ─── AI Outputs ────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS ai_outputs (
    id          BIGSERIAL PRIMARY KEY,
    output_type VARCHAR(100) NOT NULL,  -- market_digest | ticker_analysis | sentiment | user_query
    ticker      VARCHAR(20),
    prompt_hash VARCHAR(16) UNIQUE NOT NULL,
    content     TEXT NOT NULL,
    model_used  VARCHAR(100) NOT NULL,
    cost_usd    FLOAT NOT NULL DEFAULT 0,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_ai_outputs_type ON ai_outputs(output_type);
CREATE INDEX IF NOT EXISTS idx_ai_outputs_ticker ON ai_outputs(ticker);
CREATE INDEX IF NOT EXISTS idx_ai_outputs_created_at ON ai_outputs(created_at DESC);

-- ─── Provider Usage Logs ───────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS provider_usage_logs (
    id            BIGSERIAL PRIMARY KEY,
    provider      VARCHAR(100) NOT NULL,
    operation     VARCHAR(100) NOT NULL,
    input_tokens  INTEGER NOT NULL DEFAULT 0,
    output_tokens INTEGER NOT NULL DEFAULT 0,
    cost_usd      FLOAT NOT NULL DEFAULT 0,
    success       BOOLEAN NOT NULL DEFAULT true,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_provider_logs_created_at ON provider_usage_logs(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_provider_logs_provider ON provider_usage_logs(provider);
CREATE INDEX IF NOT EXISTS idx_provider_logs_daily ON provider_usage_logs(created_at, provider);

-- ─── Alerts ────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS alerts (
    id          BIGSERIAL PRIMARY KEY,
    alert_type  VARCHAR(100) NOT NULL,
    ticker      VARCHAR(20),
    message     TEXT NOT NULL,
    severity    VARCHAR(20) NOT NULL DEFAULT 'info',  -- info | warning | critical
    sent_at     TIMESTAMPTZ,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_alerts_created_at ON alerts(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_alerts_unsent ON alerts(sent_at) WHERE sent_at IS NULL;

-- ─── Seed Default Watchlist ────────────────────────────────────────────────────
INSERT INTO tickers (symbol, name, exchange, is_watched) VALUES
    ('AAPL', 'Apple Inc.', 'NASDAQ', true),
    ('MSFT', 'Microsoft Corp.', 'NASDAQ', true),
    ('NVDA', 'NVIDIA Corp.', 'NASDAQ', true),
    ('GOOGL', 'Alphabet Inc.', 'NASDAQ', true),
    ('AMZN', 'Amazon.com Inc.', 'NASDAQ', true),
    ('META', 'Meta Platforms Inc.', 'NASDAQ', true),
    ('TSLA', 'Tesla Inc.', 'NASDAQ', true),
    ('SPY', 'SPDR S&P 500 ETF', 'NYSE', true),
    ('QQQ', 'Invesco QQQ Trust', 'NASDAQ', true),
    ('GLD', 'SPDR Gold Shares', 'NYSE', true),
    ('THYAO.IS', 'Türk Hava Yolları', 'BIST', true),
    ('GARAN.IS', 'Garanti BBVA', 'BIST', true),
    ('ASELS.IS', 'Aselsan', 'BIST', true),
    ('KCHOL.IS', 'Koç Holding', 'BIST', true),
    ('EREGL.IS', 'Ereğli Demir Çelik', 'BIST', true),
    ('BIMAS.IS', 'BİM Birleşik Mağazalar', 'BIST', true),
    ('TCELL.IS', 'Turkcell', 'BIST', true),
    ('AKBNK.IS', 'Akbank', 'BIST', true)
ON CONFLICT (symbol) DO NOTHING;

-- ─── Migrations Tracker ────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS schema_migrations (
    version     VARCHAR(50) PRIMARY KEY,
    applied_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO schema_migrations (version) VALUES ('001_initial')
ON CONFLICT DO NOTHING;
