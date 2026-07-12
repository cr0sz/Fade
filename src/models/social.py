from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from pydantic import BaseModel, Field


class SocialPost(BaseModel):
    id: str
    platform: str
    content: str
    author: str
    ticker_mentions: list[str] = Field(default_factory=list)
    sentiment_score: float | None = None
    engagement_score: float = 0.0
    upvotes: int = 0
    comments: int = 0
    url: str = ""
    subreddit: str | None = None
    created_at: datetime
    raw: dict[str, Any] = Field(default_factory=dict)


class SentimentSummary(BaseModel):
    ticker: str
    period_hours: int
    mention_count: int
    avg_sentiment: float
    velocity_ratio: float
    hype_signal: bool
    panic_signal: bool
    top_posts: list[SocialPost] = Field(default_factory=list)
    computed_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
