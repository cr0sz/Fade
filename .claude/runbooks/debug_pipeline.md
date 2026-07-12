# Runbook: Debug Pipeline Issues

## Purpose
Systematic approach to diagnosing and fixing issues in any data pipeline stage.

## Preconditions
- [ ] Docker Compose stack is running: `docker compose ps`
- [ ] Identify which pipeline is failing: news | social | smart_money | ai | trend

## Diagnosis Decision Tree

```
Pipeline issue reported
        │
        ▼
Are services running?
  NO → Step 1: Service health
  YES →
        │
        ▼
Is data being fetched?
  NO → Step 2: Provider health
  YES →
        │
        ▼
Is data being stored?
  NO → Step 3: Database debug
  YES →
        │
        ▼
Is data quality correct?
  NO → Step 4: Normalization debug
  YES →
        │
        ▼
Is downstream engine consuming data?
  NO → Step 5: Engine debug
  YES → Step 6: Output/API debug
```

## Step 1: Service Health Check
```bash
docker compose ps
docker compose logs --tail=50 worker
docker compose logs --tail=50 api
docker compose exec postgres pg_isready
docker compose exec redis redis-cli ping
```

If service down: `docker compose restart <service>`
If restart loop: `docker compose logs <service> --tail=100` — look for startup error

## Step 2: Provider Health Check
```bash
# Run health checks for all providers
docker compose exec worker python -c "
import asyncio
from src.providers.news import NEWS_PROVIDERS
from src.providers.social import SOCIAL_PROVIDERS

async def check():
    for name, cls in NEWS_PROVIDERS.items():
        p = cls()
        ok = await p.health_check()
        print(f'{name}: {\"OK\" if ok else \"FAIL\"}')

asyncio.run(check())
"
```

Common causes:
- API key missing or expired → check `.env`
- Rate limit hit → check Redis: `redis-cli get ratelimit:<provider>`
- API endpoint changed → check provider's API changelog

## Step 3: Database Debug
```bash
# Check recent inserts
docker compose exec postgres psql -U mktintel -d mktintel -c "
SELECT table_name,
       (SELECT count(*) FROM information_schema.tables) as count
FROM information_schema.tables
WHERE table_schema = 'public';
"

# Check news table
docker compose exec postgres psql -U mktintel -d mktintel -c "
SELECT source, count(*), max(created_at)
FROM news GROUP BY source ORDER BY max(created_at) DESC;
"

# Check for DB errors in logs
docker compose logs worker | grep -i "asyncpg\|database\|postgres\|error"
```

## Step 4: Normalization Debug
```bash
# Test a single normalize() call
docker compose exec worker python -c "
import asyncio, json
from src.providers.news.newsapi import NewsAPIProvider

async def test():
    p = NewsAPIProvider()
    # fetch raw and inspect
    raw = await p._fetch_raw(query='market', limit=1)
    print(json.dumps(raw[0], indent=2))
    normalized = await p.normalize(raw[0])
    print(normalized.model_dump())

asyncio.run(test())
"
```

Look for: missing fields, None values, wrong types, encoding errors.

## Step 5: Engine Debug
```bash
# Run engine in isolation
docker compose exec worker python -c "
import asyncio
from src.engines.news_intelligence import NewsIntelligenceEngine

async def test():
    engine = NewsIntelligenceEngine()
    results = await engine.run_cycle(dry_run=True)
    print(f'Fetched: {results.fetched}')
    print(f'Deduplicated: {results.deduped}')
    print(f'Stored: {results.stored}')
    print(f'Errors: {results.errors}')

asyncio.run(test())
"
```

## Step 6: Cost/Budget Debug
```bash
# Check daily budget state
docker compose exec redis redis-cli get "cost:daily:$(date +%Y-%m-%d)"

# Check provider logs
docker compose exec postgres psql -U mktintel -d mktintel -c "
SELECT provider, sum(cost_usd), count(*)
FROM provider_usage_logs
WHERE created_at > now() - interval '24 hours'
GROUP BY provider;
"
```

## Common Fixes

| Symptom | Fix |
|---------|-----|
| No news ingested | Check API key in `.env`, restart worker |
| Duplicate news | Clear Redis dedup cache: `redis-cli del dedup:news:*` |
| AI calls failing | Check OPENAI_API_KEY, check daily budget in Redis |
| Telegram bot silent | Check TELEGRAM_BOT_TOKEN, check bot logs |
| DB connection refused | `docker compose restart postgres`, wait 10s |
| Worker OOM | Reduce batch sizes in `src/core/config.py` |

## Rollback Strategy
- No rollback needed for read operations
- For config changes: `git diff src/core/config.py` then revert if needed

## Update Instructions
After resolving:
- `.claude/state.md` — update Known Issues section (add or remove issue)
- `.claude/changelog.md` — log if a fix was deployed
