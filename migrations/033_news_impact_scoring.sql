-- Sprint 63: Ollama-powered news impact scoring
-- Adds impact_score, urgency, source_authority to news table.
-- impact_score: -1.0 (strongly bearish) to +1.0 (strongly bullish)
-- urgency: 0=noise, 1=low, 2=medium, 3=breaking/market-moving
-- source_authority: 0.5–2.0 multiplier (Reuters=2.0, Yahoo=1.0, unknown=0.8)

ALTER TABLE news
    ADD COLUMN IF NOT EXISTS impact_score     DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS urgency          SMALLINT,
    ADD COLUMN IF NOT EXISTS source_authority DOUBLE PRECISION;

-- Fast lookup: recent high-impact articles for news_score computation
CREATE INDEX IF NOT EXISTS idx_news_impact_recent
    ON news (published_at DESC, impact_score)
    WHERE impact_score IS NOT NULL;

-- Backfill queue: articles needing analysis (no impact score yet)
CREATE INDEX IF NOT EXISTS idx_news_needs_analysis
    ON news (published_at DESC)
    WHERE impact_score IS NULL;
