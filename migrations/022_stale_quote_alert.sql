-- Sprint 40: stale_quote_feed alert type (per-ticker dead-feed detection).
-- The MATIC/POL CoinGecko id migration killed that ticker's quotes for 5 days
-- unnoticed; ValidationAlertEngine._check_stale_quotes now fires when an
-- active CRYPTO ticker has no quote for 90+ minutes.

ALTER TABLE validation_alerts DROP CONSTRAINT IF EXISTS chk_alert_type;
ALTER TABLE validation_alerts ADD CONSTRAINT chk_alert_type CHECK (alert_type IN (
    -- Phase 3 alert types
    'precision_collapse', 'calibration_drift', 'fp_spike',
    'replay_failure', 'narrative_degradation', 'coverage_gap',
    -- Phase 4 governance alert types
    'engine_instability_spike', 'recovery_failure', 'degradation_acceleration',
    'narrative_collapse', 'confidence_inconsistency',
    -- Phase 5 meta-intelligence alert types
    'recurring_historical_failure', 'transition_instability_warning',
    'narrative_collapse_acceleration', 'governance_stress_spike',
    'strategic_instability_alert',
    -- Sprint 40: data feed health
    'stale_quote_feed'
));
