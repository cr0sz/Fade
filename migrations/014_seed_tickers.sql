-- ============================================================
-- Seed Tickers: NASDAQ, Crypto, BIST
-- Migration: 014_seed_tickers
-- Created: 2026-05-31
-- ============================================================
-- INSERT OR UPDATE so re-running is idempotent.
-- is_watched=true means these tickers are actively tracked by all engines.

INSERT INTO tickers (symbol, name, exchange, sector, industry, is_active, is_watched, metadata)
VALUES

-- ── NASDAQ / US Tech (major holdings + institutional targets) ─────────────────
('NVDA',  'NVIDIA Corporation',             'NASDAQ', 'technology',  'Semiconductors',              true, true, '{"tags":["ai","gpu","datacenter"]}'),
('AMD',   'Advanced Micro Devices',         'NASDAQ', 'technology',  'Semiconductors',              true, true, '{"tags":["ai","gpu","cpu"]}'),
('AAPL',  'Apple Inc.',                     'NASDAQ', 'technology',  'Consumer Electronics',        true, true, '{"tags":["consumer","hardware"]}'),
('MSFT',  'Microsoft Corporation',          'NASDAQ', 'technology',  'Software',                    true, true, '{"tags":["cloud","ai","enterprise"]}'),
('GOOGL', 'Alphabet Inc.',                  'NASDAQ', 'technology',  'Internet Services',           true, true, '{"tags":["ai","ads","cloud"]}'),
('META',  'Meta Platforms Inc.',            'NASDAQ', 'technology',  'Social Media',                true, true, '{"tags":["social","vr","ai"]}'),
('TSLA',  'Tesla Inc.',                     'NASDAQ', 'consumer',    'Electric Vehicles',           true, true, '{"tags":["ev","energy","ai"]}'),
('AMZN',  'Amazon.com Inc.',                'NASDAQ', 'consumer',    'E-Commerce / Cloud',          true, true, '{"tags":["cloud","ecommerce","ai"]}'),
('NFLX',  'Netflix Inc.',                   'NASDAQ', 'consumer',    'Streaming',                   true, true, '{"tags":["streaming","content"]}'),
('INTC',  'Intel Corporation',              'NASDAQ', 'technology',  'Semiconductors',              true, true, '{"tags":["cpu","datacenter"]}'),
('QCOM',  'Qualcomm Inc.',                  'NASDAQ', 'technology',  'Semiconductors',              true, true, '{"tags":["mobile","chips","5g"]}'),
('AVGO',  'Broadcom Inc.',                  'NASDAQ', 'technology',  'Semiconductors',              true, true, '{"tags":["networking","chips"]}'),
('CRM',   'Salesforce Inc.',                'NASDAQ', 'technology',  'Enterprise Software',         true, true, '{"tags":["saas","crm"]}'),
('ORCL',  'Oracle Corporation',             'NASDAQ', 'technology',  'Enterprise Software',         true, true, '{"tags":["database","cloud"]}'),
('PYPL',  'PayPal Holdings Inc.',           'NASDAQ', 'finance',     'Fintech / Payments',          true, true, '{"tags":["payments","fintech"]}'),
('COIN',  'Coinbase Global Inc.',           'NASDAQ', 'finance',     'Crypto Exchange',             true, true, '{"tags":["crypto","exchange"]}'),
('MSTR',  'MicroStrategy Inc.',             'NASDAQ', 'technology',  'Business Intelligence / BTC', true, true, '{"tags":["bitcoin","btc-proxy"]}'),

-- ── US Market ETFs + Macro instruments ────────────────────────────────────────
('SPY',   'SPDR S&P 500 ETF Trust',         'NYSE',   'etf',         'Broad Market',                true, true, '{"tags":["index","sp500"]}'),
('QQQ',   'Invesco QQQ Trust',              'NASDAQ', 'etf',         'Tech Index',                  true, true, '{"tags":["index","nasdaq100"]}'),
('IWM',   'iShares Russell 2000 ETF',       'NYSE',   'etf',         'Small Cap',                   true, true, '{"tags":["index","smallcap"]}'),
('GLD',   'SPDR Gold Shares ETF',           'NYSE',   'etf',         'Commodities',                 true, true, '{"tags":["gold","macro"]}'),
('TLT',   'iShares 20+ Year Treasury ETF',  'NASDAQ', 'etf',         'Bonds',                       true, true, '{"tags":["bonds","macro","rates"]}'),
('VIX',   'CBOE Volatility Index',          'CBOE',   'index',       'Volatility',                  true, true, '{"tags":["volatility","macro"]}'),

-- ── Crypto (CoinGecko IDs stored in metadata for price lookups) ───────────────
('BTC',   'Bitcoin',                        'CRYPTO', 'crypto',      'Store of Value',              true, true, '{"coingecko_id":"bitcoin","tags":["layer1","store-of-value"]}'),
('ETH',   'Ethereum',                       'CRYPTO', 'crypto',      'Smart Contract Platform',     true, true, '{"coingecko_id":"ethereum","tags":["layer1","defi","nft"]}'),
('SOL',   'Solana',                         'CRYPTO', 'crypto',      'Smart Contract Platform',     true, true, '{"coingecko_id":"solana","tags":["layer1","defi","nft"]}'),
('BNB',   'BNB (Binance)',                  'CRYPTO', 'crypto',      'Exchange Token',              true, true, '{"coingecko_id":"binancecoin","tags":["exchange","layer1"]}'),
('XRP',   'XRP (Ripple)',                   'CRYPTO', 'crypto',      'Payment Network',             true, true, '{"coingecko_id":"ripple","tags":["payments","cbdc"]}'),
('AVAX',  'Avalanche',                      'CRYPTO', 'crypto',      'Smart Contract Platform',     true, true, '{"coingecko_id":"avalanche-2","tags":["layer1","defi"]}'),
('ADA',   'Cardano',                        'CRYPTO', 'crypto',      'Smart Contract Platform',     true, true, '{"coingecko_id":"cardano","tags":["layer1"]}'),
('DOGE',  'Dogecoin',                       'CRYPTO', 'crypto',      'Meme Coin',                   true, true, '{"coingecko_id":"dogecoin","tags":["meme","payments"]}'),
('DOT',   'Polkadot',                       'CRYPTO', 'crypto',      'Interoperability Protocol',   true, true, '{"coingecko_id":"polkadot","tags":["interop","layer0"]}'),
('LINK',  'Chainlink',                      'CRYPTO', 'crypto',      'Oracle Network',              true, true, '{"coingecko_id":"chainlink","tags":["oracle","defi"]}'),
('MATIC', 'Polygon (POL)',                  'CRYPTO', 'crypto',      'Layer 2 Scaling',             true, true, '{"coingecko_id":"matic-network","tags":["layer2","defi"]}'),
('ARB',   'Arbitrum',                       'CRYPTO', 'crypto',      'Layer 2 Scaling',             true, true, '{"coingecko_id":"arbitrum","tags":["layer2","defi"]}'),
('OP',    'Optimism',                       'CRYPTO', 'crypto',      'Layer 2 Scaling',             true, true, '{"coingecko_id":"optimism","tags":["layer2","defi"]}'),
('UNI',   'Uniswap',                        'CRYPTO', 'crypto',      'Decentralized Exchange',      true, true, '{"coingecko_id":"uniswap","tags":["defi","dex"]}'),
('AAVE',  'Aave',                           'CRYPTO', 'crypto',      'DeFi Lending',                true, true, '{"coingecko_id":"aave","tags":["defi","lending"]}'),
('SUI',   'Sui',                            'CRYPTO', 'crypto',      'Smart Contract Platform',     true, true, '{"coingecko_id":"sui","tags":["layer1"]}'),
('TON',   'Toncoin',                        'CRYPTO', 'crypto',      'Messaging / L1',              true, true, '{"coingecko_id":"the-open-network","tags":["layer1","telegram"]}'),

-- ── BIST (Borsa Istanbul) — Finnhub uses .IS suffix ───────────────────────────
('THYAO.IS', 'Türk Hava Yolları (THY)',     'BIST',   'industrial',  'Airlines',                    true, true, '{"tags":["aviation","turkey"]}'),
('GARAN.IS', 'Garanti BBVA',               'BIST',   'finance',     'Banking',                     true, true, '{"tags":["banking","turkey"]}'),
('ASELS.IS', 'Aselsan',                    'BIST',   'industrial',  'Defense Electronics',         true, true, '{"tags":["defense","turkey"]}'),
('KCHOL.IS', 'Koç Holding',               'BIST',   'industrial',  'Conglomerate',                true, true, '{"tags":["conglomerate","turkey"]}'),
('EREGL.IS', 'Ereğli Demir ve Çelik',     'BIST',   'materials',   'Steel',                       true, true, '{"tags":["steel","materials","turkey"]}'),
('BIMAS.IS', 'BİM Birleşik Mağazalar',    'BIST',   'consumer',    'Retail / Grocery',            true, true, '{"tags":["retail","turkey"]}'),
('SISE.IS',  'Şişe Cam',                  'BIST',   'materials',   'Glass / Chemicals',           true, true, '{"tags":["materials","turkey"]}'),
('TCELL.IS', 'Turkcell',                  'BIST',   'telecom',     'Telecom',                     true, true, '{"tags":["telecom","turkey"]}'),
('AKBNK.IS', 'Akbank',                    'BIST',   'finance',     'Banking',                     true, true, '{"tags":["banking","turkey"]}'),
('YKBNK.IS', 'Yapı Kredi Bankası',        'BIST',   'finance',     'Banking',                     true, true, '{"tags":["banking","turkey"]}'),
('TUPRS.IS', 'Tüpraş',                    'BIST',   'energy',      'Oil Refinery',                true, true, '{"tags":["energy","oil","turkey"]}'),
('SAHOL.IS', 'Sabancı Holding',           'BIST',   'industrial',  'Conglomerate',                true, true, '{"tags":["conglomerate","turkey"]}'),
('FROTO.IS', 'Ford Otosan',               'BIST',   'industrial',  'Automotive',                  true, true, '{"tags":["automotive","turkey"]}'),
('TOASO.IS', 'Tofaş Türk Otomobil',       'BIST',   'industrial',  'Automotive',                  true, true, '{"tags":["automotive","turkey"]}'),
('PGSUS.IS', 'Pegasus Hava Taşımacılığı', 'BIST',   'industrial',  'Airlines',                    true, true, '{"tags":["aviation","turkey"]}')

ON CONFLICT (symbol) DO UPDATE
    SET name        = EXCLUDED.name,
        exchange    = EXCLUDED.exchange,
        sector      = EXCLUDED.sector,
        industry    = EXCLUDED.industry,
        is_watched  = true,
        metadata    = EXCLUDED.metadata,
        updated_at  = now();
