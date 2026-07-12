-- ============================================================
-- Migration: 019_signal_edge_agg_uq
-- Add missing unique index on signal_edge_aggregations so that
-- the ON CONFLICT (group_key, (_utc_date(computed_at))) clause
-- in aggregator.py has the constraint it requires.
-- Created: 2026-06-10
--
-- Depends on: _utc_date() from 005_signal_truth_layer
--             signal_edge_aggregations from 013_signal_edge_analyzer
-- ============================================================

-- Remove duplicate rows first (keep latest id per group+day)
DELETE FROM signal_edge_aggregations a
USING signal_edge_aggregations b
WHERE a.id < b.id
  AND a.group_key = b.group_key
  AND _utc_date(a.computed_at) = _utc_date(b.computed_at);

CREATE UNIQUE INDEX IF NOT EXISTS idx_edge_agg_daily_uq
    ON signal_edge_aggregations (group_key, (_utc_date(computed_at)));

INSERT INTO schema_migrations (version) VALUES ('019_signal_edge_agg_uq')
ON CONFLICT DO NOTHING;
