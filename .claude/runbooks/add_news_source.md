# Runbook: Add News Source

## Purpose
Add a new external news data provider to the intelligence pipeline.

## Preconditions
- [ ] API key/credentials obtained and stored in `.env`
- [ ] Rate limits documented
- [ ] Cost per request known
- [ ] Read `.claude/state.md` to check no conflicting work in progress

## Step-by-Step Execution

### Step 1: Create provider file
Create `src/providers/news/<provider_name>.py`

Copy the structure from `src/providers/news/newsapi.py` as template.

Implement all required methods from `BaseProvider`:
```python
async def fetch(self, **kwargs) -> list[NewsItem]
async def normalize(self, raw: dict) -> NewsItem
async def health_check(self) -> bool
async def cost_estimate(self, request_count: int) -> float
async def rate_limit_handler(self) -> None
```

### Step 2: Add provider config
In `src/core/config.py`, add to `NewsConfig`:
```python
<PROVIDER>_API_KEY: str = ""
<PROVIDER>_BASE_URL: str = "https://api.provider.com/v1"
<PROVIDER>_RATE_LIMIT: int = 100  # requests per minute
```

### Step 3: Register provider in registry
In `src/providers/news/__init__.py`:
```python
from .new_provider import NewProviderNewsProvider
NEWS_PROVIDERS["new_provider"] = NewProviderNewsProvider
```

### Step 4: Add .env variables
In `.env.example`:
```
NEW_PROVIDER_API_KEY=your_key_here
NEW_PROVIDER_BASE_URL=https://api.provider.com/v1
```

### Step 5: Update News Intelligence Engine
In `src/engines/news_intelligence.py`, the engine auto-discovers registered providers.
If the provider needs special handling, add a condition in `_fetch_all_sources()`.

### Step 6: Write provider test
Create `tests/providers/news/test_<provider_name>.py`
Test: fetch(), normalize(), health_check(), rate limiting.
Use `pytest-asyncio` and `respx` for HTTP mocking.

## Files to Modify
1. `src/providers/news/<provider_name>.py` — CREATE
2. `src/providers/news/__init__.py` — REGISTER
3. `src/core/config.py` — ADD CONFIG
4. `.env.example` — ADD ENV VARS
5. `tests/providers/news/test_<provider_name>.py` — CREATE

## Validation Steps
1. `docker compose exec worker python -c "from src.providers.news.<name> import *; import asyncio; asyncio.run(p.health_check())"`
2. Check `provider_usage_logs` table — should see new provider entries
3. Check logs: `docker compose logs worker | grep <provider_name>`
4. Verify dedup working: run twice, confirm no duplicate inserts

## Rollback Strategy
1. Remove provider from `NEWS_PROVIDERS` registry in `__init__.py`
2. Restart worker: `docker compose restart worker`
3. Provider file can remain — it won't be called if not registered

## Update Instructions
After completion, update:
- `.claude/state.md` — mark provider as completed, update "Known Issues" if any
- `.claude/changelog.md` — add entry: `[version] — date — Added <name> news provider`
- `.claude/decisions.md` — if any architecture decision was made (e.g., unusual parsing approach)
