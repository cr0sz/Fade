-- Add narrative_persistence score to market_trends.
-- 0.0–1.0: strength of the current narrative weighted by its momentum direction.
-- Computed each trend cycle; flows into signals_registry.signal_data via signal archiver.
ALTER TABLE market_trends
    ADD COLUMN IF NOT EXISTS narrative_persistence double precision DEFAULT 0.0;
