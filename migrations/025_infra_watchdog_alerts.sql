-- 025_infra_watchdog_alerts.sql
-- Sprint 49: the infra watchdog (src/watchdog) writes INFRASTRUCTURE health
-- alerts into validation_alerts so the dashboard surfaces them alongside
-- signal-quality alerts. New alert_type values are 'infra_<check>'. This
-- re-declares chk_alert_type to include every previously-allowed type plus
-- the four infra checks.

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
    'stale_quote_feed',
    -- Sprint 49: infra watchdog (dead containers, silent feeds, dead api/ws)
    'infra_containers', 'infra_funding_freshness',
    'infra_quote_freshness', 'infra_api_health',
    -- Sprint 51: silent-but-Up liquidation websocket detection
    'infra_liq_stream'
));
