# Runbook: Add Ticker/Symbol Source

## Purpose
Add or update the ticker database — the list of tradable symbols tracked by the system.

## Preconditions
- [ ] Know the exchange: US (NYSE/NASDAQ) or BIST
- [ ] Have access to ticker list source (API or CSV)
- [ ] Read `.claude/state.md`

## Step-by-Step Execution

### Step 1: Understand ticker table schema
```sql
CREATE TABLE tickers (
    symbol VARCHAR(20) PRIMARY KEY,
    name VARCHAR(500),
    exchange VARCHAR(50),       -- NYSE, NASDAQ, BIST
    sector VARCHAR(100),
    industry VARCHAR(200),
    market_cap_usd FLOAT,
    is_active BOOLEAN DEFAULT true,
    is_watched BOOLEAN DEFAULT false,
    metadata JSONB,
    updated_at TIMESTAMPTZ DEFAULT now()
);
```

### Step 2: For US Equities — use Polygon.io ticker list
```bash
docker compose exec worker python -c "
import asyncio
from src.providers.market.polygon import PolygonProvider

async def sync():
    p = PolygonProvider()
    tickers = await p.fetch_all_tickers(exchange='NYSE')
    tickers += await p.fetch_all_tickers(exchange='NASDAQ')
    print(f'Fetched {len(tickers)} tickers')

asyncio.run(sync())
"
```

### Step 3: For BIST — use Finnhub or manual CSV
BIST tickers follow pattern: `SYMBOL.IS` (e.g., `THYAO.IS`, `GARAN.IS`)

If loading from CSV:
```bash
# CSV format: symbol,name,sector
docker compose exec worker python -c "
import asyncio, csv
from src.db.repositories import TickerRepository
from src.db.connection import get_db_pool

async def load_csv(path: str):
    pool = await get_db_pool()
    repo = TickerRepository(pool)
    with open(path) as f:
        reader = csv.DictReader(f)
        for row in reader:
            await repo.upsert({
                'symbol': row['symbol'],
                'name': row['name'],
                'exchange': 'BIST',
                'sector': row.get('sector', 'Unknown'),
                'is_active': True,
            })

asyncio.run(load_csv('/data/bist_tickers.csv'))
"
```

### Step 4: Add sector mapping
In `src/core/config.py`, update `SECTOR_KEYWORDS`:
```python
SECTOR_KEYWORDS = {
    "technology": ["software", "semiconductor", "AI", "cloud"],
    "finance": ["bank", "insurance", "fintech"],
    "energy": ["oil", "gas", "renewable"],
    # Add new sectors here
}
```

### Step 5: Mark tickers as watched
Watched tickers get priority processing in all engines:
```bash
docker compose exec postgres psql -U mktintel -d mktintel -c "
UPDATE tickers SET is_watched = true
WHERE symbol IN ('AAPL', 'MSFT', 'NVDA', 'THYAO.IS', 'GARAN.IS');
"
```

### Step 6: Verify ticker extraction
Run news cycle and verify tickers are being extracted:
```bash
docker compose exec postgres psql -U mktintel -d mktintel -c "
SELECT ticker_symbols, count(*)
FROM news
WHERE ticker_symbols != '{}'
ORDER BY created_at DESC LIMIT 10;
"
```

## Files to Modify
1. `src/core/config.py` — sector keywords (if adding new sectors)
2. `migrations/` — if schema change needed
3. No provider changes needed — ticker sync is data operation

## Validation Steps
1. Count tickers: `SELECT exchange, count(*) FROM tickers GROUP BY exchange;`
2. Check watched: `SELECT count(*) FROM tickers WHERE is_watched = true;`
3. Test ticker extraction: fetch news and check `ticker_symbols` column populated

## Rollback Strategy
- Ticker data is append-only; rollback means setting `is_active = false`
- `UPDATE tickers SET is_active = false WHERE exchange = '<exchange>';`

## Update Instructions
- `.claude/state.md` — note new exchange/ticker count added
- `.claude/changelog.md` — log ticker sync
