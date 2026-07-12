# Runbook: Add Any Provider (Generic)

## Purpose
Generic runbook for adding a new data provider of any category (news/social/market/ai/smart_money).

## Preconditions
- [ ] Identify category: news | social | market | ai | smart_money
- [ ] API credentials available
- [ ] Rate limits and costs documented
- [ ] Read `.claude/state.md`

## Step-by-Step Execution

### Step 1: Choose the correct category runbook
- News source → use `add_news_source.md` instead
- Social source → use `add_social_source.md` instead
- AI model → use `add_ai_model.md` instead

If none apply, continue with this runbook.

### Step 2: Create provider file
Path: `src/providers/<category>/<provider_name>.py`

Required interface (from `src/providers/base.py`):
```python
class BaseProvider(ABC):
    @abstractmethod
    async def fetch(self, **kwargs) -> list[BaseModel]: ...

    @abstractmethod
    async def normalize(self, raw: dict) -> BaseModel: ...

    @abstractmethod
    async def health_check(self) -> bool: ...

    @abstractmethod
    async def cost_estimate(self, request_count: int) -> float: ...

    @abstractmethod
    async def rate_limit_handler(self) -> None: ...
```

### Step 3: Add configuration
In `src/core/config.py`:
```python
class <Category>Config(BaseSettings):
    <PROVIDER>_API_KEY: str = ""
    <PROVIDER>_RATE_LIMIT: int = 60
```

### Step 4: Register provider
In `src/providers/<category>/__init__.py`:
```python
PROVIDERS["<name>"] = ProviderClass
```

### Step 5: Add cost tracking
In `src/core/cost_control.py`, ensure provider is included in cost estimation.
Each `fetch()` call must call `self.log_request_cost(tokens_or_units, cost_usd)`.

### Step 6: Connect to engine
Each category has an engine in `src/engines/`:
- Determine which engine consumes this provider
- Add provider instantiation to the engine's `__init__`
- The engine's `_fetch_all_sources()` should already iterate registered providers

## Files to Modify
1. `src/providers/<category>/<name>.py` — CREATE
2. `src/providers/<category>/__init__.py` — REGISTER
3. `src/core/config.py` — ADD CONFIG
4. `.env.example` — ADD ENV VARS
5. Relevant engine in `src/engines/` — possibly add special handling

## Validation Steps
1. Health check: `docker compose exec worker python -c "...health_check()"`
2. Single fetch test: `docker compose exec worker python -c "...fetch(limit=3)"`
3. Check cost tracking: `SELECT * FROM provider_usage_logs ORDER BY created_at DESC LIMIT 5;`
4. Check error logs: `docker compose logs worker --since 5m`

## Rollback Strategy
1. Unregister from `__init__.py`
2. `docker compose restart worker api`
3. Provider file remains harmless if unregistered

## Update Instructions
- `.claude/state.md` — mark complete
- `.claude/changelog.md` — log addition
- `.claude/decisions.md` — log if non-obvious choice made
