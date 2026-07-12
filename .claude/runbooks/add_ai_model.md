# Runbook: Add AI Model/Provider

## Purpose
Add a new LLM provider or model to the AI Analyst Engine's provider router.

## Preconditions
- [ ] API key obtained
- [ ] Pricing per 1M input/output tokens documented
- [ ] Context window size documented
- [ ] Identify primary use case: summarization | analysis | sentiment | qa

## Step-by-Step Execution

### Step 1: Create AI provider file
Path: `src/providers/ai/<provider_name>.py`

Use `src/providers/ai/openai_provider.py` as template.

Must implement:
```python
class BaseAIProvider(BaseProvider):
    async def complete(
        self,
        prompt: str,
        system: str,
        max_tokens: int = 1000,
        temperature: float = 0.1,
    ) -> AIResponse: ...

    async def complete_structured(
        self,
        prompt: str,
        system: str,
        response_model: type[BaseModel],
    ) -> BaseModel: ...
```

`AIResponse` schema:
```python
class AIResponse(BaseModel):
    content: str
    model: str
    input_tokens: int
    output_tokens: int
    cost_usd: float
    cached: bool
```

### Step 2: Add pricing constants
In provider file:
```python
INPUT_COST_PER_1M = 0.15   # USD
OUTPUT_COST_PER_1M = 0.60  # USD
```

### Step 3: Register in AI provider router
In `src/engines/ai_analyst.py`, add to `PROVIDER_PRIORITY`:
```python
PROVIDER_PRIORITY = [
    ("openai", ProviderTier.PRIMARY),
    ("anthropic", ProviderTier.FALLBACK),
    ("new_provider", ProviderTier.TERTIARY),  # add here
]
```

### Step 4: Add to cost routing thresholds
In `src/core/cost_control.py`, update `get_preferred_provider()`:
```python
# Example: use new_provider when budget is < 30%
if budget_remaining_pct < 0.3:
    return "new_provider"
```

### Step 5: Add config
In `src/core/config.py`:
```python
NEW_PROVIDER_API_KEY: str = ""
NEW_PROVIDER_MODEL: str = "model-name-here"
NEW_PROVIDER_MAX_TOKENS: int = 4096
```

## Files to Modify
1. `src/providers/ai/<provider_name>.py` — CREATE
2. `src/providers/ai/__init__.py` — REGISTER
3. `src/engines/ai_analyst.py` — ADD TO ROUTER
4. `src/core/cost_control.py` — ADD ROUTING RULE
5. `src/core/config.py` — ADD CONFIG
6. `.env.example` — ADD API KEY

## Validation Steps
1. `docker compose exec worker python -c "from src.providers.ai.<name> import *; ..."`
2. Send test prompt: `POST /api/v1/analyst/query {"question": "What is the market doing?"}`
3. Verify cost logged: `SELECT * FROM provider_usage_logs WHERE provider='<name>' LIMIT 5;`
4. Verify cache working: send same prompt twice, second should be cache hit
5. Check model appears in `ai_outputs.model_used` column

## Rollback Strategy
1. Remove from `PROVIDER_PRIORITY` in `ai_analyst.py`
2. Remove routing rule from `cost_control.py`
3. `docker compose restart worker api`
4. Provider file stays — safe if not in routing table

## Update Instructions
- `.claude/state.md` — add to completed, note model pricing
- `.claude/changelog.md` — log addition with model name and cost
- `.claude/decisions.md` — add DEC-0XX explaining why this model was added
