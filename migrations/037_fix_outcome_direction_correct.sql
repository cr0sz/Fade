-- Fix signal_outcomes rows where direction_correct is NULL because the
-- outcome tracker did not recognise 'long'/'short' as directional labels
-- (only 'risk_on'/'risk_off' were in _POSITIVE/_NEGATIVE before this fix).
--
-- Recomputes direction_correct from outcome_score_delta sign vs predicted direction.
-- Leaves ±0.2% neutral zone rows as NULL (matching _RETURN_THRESHOLD in the engine).

UPDATE signal_outcomes so
   SET direction_correct = CASE
         WHEN sr.direction IN ('long',  'buy')  AND so.outcome_score_delta >  0.002 THEN TRUE
         WHEN sr.direction IN ('long',  'buy')  AND so.outcome_score_delta < -0.002 THEN FALSE
         WHEN sr.direction IN ('short', 'sell') AND so.outcome_score_delta < -0.002 THEN TRUE
         WHEN sr.direction IN ('short', 'sell') AND so.outcome_score_delta >  0.002 THEN FALSE
         ELSE NULL  -- neutral zone (|return| ≤ 0.2%) — not a tradeable move
       END
  FROM signals_registry sr
 WHERE so.signal_id    = sr.id
   AND sr.direction    IN ('long', 'buy', 'short', 'sell')
   AND so.direction_correct IS NULL
   AND so.outcome_score_delta IS NOT NULL;
