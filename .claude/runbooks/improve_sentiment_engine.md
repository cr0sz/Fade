# Runbook: Improve Sentiment Engine

## Purpose
Tune, retrain, or improve the sentiment scoring pipeline without breaking production.

## Preconditions
- [ ] Understand current sentiment pipeline: `src/engines/sentiment_engine.py`
- [ ] Have baseline metrics: `SELECT avg(sentiment_score) FROM social_posts WHERE created_at > now() - interval '7 days';`
- [ ] Read `.claude/decisions.md` — DEC-010 explains VADER + LLM hybrid choice

## Current Architecture
```
Social posts → VADER (fast, free) → pre_score
Top 20 mentioned tickers → LLM scoring (slow, $$)
Composite: 0.3 * vader_score + 0.7 * llm_score (if LLM available)
           1.0 * vader_score (if LLM not available / budget exceeded)
```

## Step-by-Step: Improving VADER Component

### Option A: Tune VADER lexicon for financial terms
```python
# In src/engines/sentiment_engine.py, add to __init__:
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
self.vader = SentimentIntensityAnalyzer()
# Add financial domain words
self.vader.lexicon.update({
    "moon": 3.0,
    "rip": -2.0,
    "bagholder": -2.5,
    "diamond hands": 2.0,
    "rug pull": -4.0,
    "squeeze": 1.5,
    "recession": -2.0,
    "crash": -3.5,
    "rally": 2.5,
})
```

### Option B: Replace VADER with FinBERT
Only do this if VADER quality is demonstrably poor.

See `decisions.md` before proceeding — adds CPU/memory requirements.

```python
# Requires: transformers, torch in requirements.txt
from transformers import pipeline
self.finbert = pipeline(
    "text-classification",
    model="ProsusAI/finbert",
    device=-1  # CPU; use 0 for GPU
)
```

Cost: ~200ms per call vs 1ms VADER. Only use for top-N tickers.

## Step-by-Step: Improving LLM Sentiment Component

### Improve the sentiment prompt
Location: `src/engines/sentiment_engine.py` → `_build_sentiment_prompt()`

Current prompt structure:
```
System: You are a financial sentiment analyst. Score -1.0 (bearish) to 1.0 (bullish).
User: Analyze sentiment for {ticker} from these posts: {posts}
```

Improvement: Add structured output schema (Pydantic):
```python
class SentimentOutput(BaseModel):
    score: float = Field(ge=-1.0, le=1.0)
    confidence: float = Field(ge=0.0, le=1.0)
    key_signals: list[str]
    hype_detected: bool
    panic_detected: bool
```

### Add contrarian signal detection
In `_calculate_hype_panic()`:
```python
# Hype: high velocity + extreme positive sentiment
hype = velocity_ratio > 3.0 and sentiment_score > 0.7

# Panic: high velocity + extreme negative sentiment
panic = velocity_ratio > 3.0 and sentiment_score < -0.7

# Contrarian: very high hype often precedes reversal
contrarian_signal = velocity_ratio > 5.0 and sentiment_score > 0.85
```

## Step-by-Step: Improving Velocity Calculation

Current: `velocity = mentions_last_hour / avg_mentions_per_hour_7d`

Improvement: Exponential moving average:
```python
# Smooth velocity with EMA
alpha = 0.3  # smoothing factor
ema_velocity = alpha * current_velocity + (1 - alpha) * previous_ema
```

## Validation After Changes

```bash
# Compare before/after scores
docker compose exec postgres psql -U mktintel -d mktintel -c "
SELECT
    ticker,
    avg(sentiment_score) as avg_sent,
    stddev(sentiment_score) as volatility,
    count(*) as sample_size
FROM social_posts
WHERE created_at > now() - interval '24 hours'
GROUP BY ticker
ORDER BY count(*) DESC LIMIT 20;
"

# Check hype/panic signals
docker compose exec postgres psql -U mktintel -d mktintel -c "
SELECT * FROM market_trends
WHERE hype_signal = true OR panic_signal = true
ORDER BY updated_at DESC LIMIT 10;
"
```

## Rollback Strategy
- VADER changes: revert `self.vader.lexicon.update()` call
- FinBERT: remove import, revert to VADER — restart worker
- Prompt changes: git revert `src/engines/sentiment_engine.py`
- `docker compose restart worker`

## Update Instructions
- `.claude/state.md` — update sentiment engine description
- `.claude/changelog.md` — log improvement with metrics (before/after avg scores)
- `.claude/decisions.md` — add DEC-0XX if architecture changed (e.g., VADER → FinBERT)
