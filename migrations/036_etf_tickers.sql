-- Sprint ETF-fix: seed missing sector ETF tickers so they get priced,
-- generate signal_outcomes, and feed the OOS guardrail's ETF long-confirm bucket.
--
-- Exchange mapping:
--   XLK  → NASDAQ (Technology Select Sector SPDR)
--   rest  → NYSE   (NYSE Arca SPDR sector ETFs)
--
-- ON CONFLICT DO NOTHING makes this idempotent.

INSERT INTO tickers (symbol, name, exchange, sector, industry, is_active, is_watched, metadata)
VALUES
    ('XLK',  'Technology Select Sector SPDR Fund',            'NASDAQ', 'technology',              'ETF - Sector',  true, true, '{"tags": ["etf", "sector", "technology"]}'),
    ('XLF',  'Financial Select Sector SPDR Fund',             'NYSE',   'financials',              'ETF - Sector',  true, true, '{"tags": ["etf", "sector", "financials"]}'),
    ('XLE',  'Energy Select Sector SPDR Fund',                'NYSE',   'energy',                  'ETF - Sector',  true, true, '{"tags": ["etf", "sector", "energy"]}'),
    ('XLV',  'Health Care Select Sector SPDR Fund',           'NYSE',   'healthcare',              'ETF - Sector',  true, true, '{"tags": ["etf", "sector", "healthcare"]}'),
    ('XLI',  'Industrial Select Sector SPDR Fund',            'NYSE',   'industrials',             'ETF - Sector',  true, true, '{"tags": ["etf", "sector", "industrials"]}'),
    ('XLP',  'Consumer Staples Select Sector SPDR Fund',      'NYSE',   'consumer_staples',        'ETF - Sector',  true, true, '{"tags": ["etf", "sector", "consumer_staples"]}'),
    ('XLY',  'Consumer Discretionary Select Sector SPDR Fund','NYSE',   'consumer_discretionary',  'ETF - Sector',  true, true, '{"tags": ["etf", "sector", "consumer_discretionary"]}'),
    ('XLU',  'Utilities Select Sector SPDR Fund',             'NYSE',   'utilities',               'ETF - Sector',  true, true, '{"tags": ["etf", "sector", "utilities"]}'),
    ('XLC',  'Communication Services Select Sector SPDR Fund','NYSE',   'communication_services',  'ETF - Sector',  true, true, '{"tags": ["etf", "sector", "communication_services"]}'),
    ('XLRE', 'Real Estate Select Sector SPDR Fund',           'NYSE',   'real_estate',             'ETF - Sector',  true, true, '{"tags": ["etf", "sector", "real_estate"]}'),
    ('XLB',  'Materials Select Sector SPDR Fund',             'NYSE',   'materials',               'ETF - Sector',  true, true, '{"tags": ["etf", "sector", "materials"]}')
ON CONFLICT (symbol) DO NOTHING;

-- Partial index to speed OOS guardrail's pending-outcome scans:
-- filters out neutral/mixed signals which never generate tradeable outcomes.
CREATE INDEX IF NOT EXISTS idx_signals_registry_pending_outcomes
    ON signals_registry(emitted_at ASC)
    WHERE direction NOT IN ('neutral', 'mixed');
