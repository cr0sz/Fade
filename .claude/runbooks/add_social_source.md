# Runbook: Add Social Media Source

## Purpose
Add a new social media platform to the sentiment tracking pipeline.

## Preconditions
- [ ] API credentials obtained (OAuth tokens, API keys)
- [ ] Rate limit tier documented (free/paid)
- [ ] Understand data model: posts, comments, threads, reactions
- [ ] Compliance check: ToS allows data collection for research

## Step-by-Step Execution

### Step 1: Create social provider
Path: `src/providers/social/<platform_name>.py`

Use `src/providers/social/reddit.py` as template.

Must produce `SocialPost` objects:
```python
class SocialPost(BaseModel):
    id: str
    platform: str          # "reddit" | "twitter" | "stocktwits"
    content: str
    author: str
    ticker_mentions: list[str]
    sentiment_score: float | None  # pre-scored or None
    engagement_score: float        # likes + comments + shares
    created_at: datetime
    url: str
    raw: dict
```

### Step 2: Define tracked communities/channels
In `src/core/config.py`:
```python
<PLATFORM>_COMMUNITIES: list[str] = [
    "community1",
    "community2",
]
<PLATFORM>_SEARCH_QUERIES: list[str] = [
    "$AAPL", "$SPY", "BIST", "borsa"
]
```

### Step 3: Register provider
In `src/providers/social/__init__.py`:
```python
SOCIAL_PROVIDERS["<platform>"] = PlatformProvider
```

### Step 4: Connect to Sentiment Engine
In `src/engines/sentiment_engine.py`:
The engine iterates `SOCIAL_PROVIDERS` — no change needed unless platform needs special handling (e.g., media-only platform, thread-based structure).

### Step 5: Configure rate limiting
In provider `rate_limit_handler()`:
- Use Redis token bucket: `src/cache/redis_client.py` has `RateLimiter` helper
- Key pattern: `ratelimit:<platform>:<endpoint>`

### Step 6: Handle auth refresh
If platform uses OAuth2 with refresh tokens:
- Store refresh token in `.env`
- Implement `_refresh_token()` in provider
- Call before each `fetch()` if token near expiry

## Files to Modify
1. `src/providers/social/<platform>.py` — CREATE
2. `src/providers/social/__init__.py` — REGISTER
3. `src/core/config.py` — ADD CONFIG
4. `.env.example` — ADD CREDENTIALS

## Validation Steps
1. Health check via docker exec
2. Fetch 10 posts: verify `SocialPost` objects populated correctly
3. Ticker mentions extracted: check `ticker_mentions` field
4. Check `social_posts` table: `SELECT platform, count(*) FROM social_posts GROUP BY platform;`
5. Sentiment cycle ran: `docker compose logs worker | grep sentiment`

## Rollback Strategy
1. Unregister from `SOCIAL_PROVIDERS`
2. `docker compose restart worker`
3. Existing data in `social_posts` unaffected

## Update Instructions
- `.claude/state.md` — mark complete, note rate tier
- `.claude/changelog.md` — log with platform name
- `.claude/decisions.md` — if new sentiment model needed for this platform
