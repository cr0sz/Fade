from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field, HttpUrl


class NewsItem(BaseModel):
    id: str | None = None
    title: str
    content: str | None = None
    summary: str | None = None
    url: str
    source: str
    source_domain: str
    author: str | None = None
    published_at: datetime
    ticker_symbols: list[str] = Field(default_factory=list)
    sector: str | None = None
    sentiment_score: float | None = None
    content_hash: str | None = None
    ai_summary: str | None = None
    image_url: str | None = None
    raw: dict[str, Any] = Field(default_factory=dict)


class NewsFeedResult(BaseModel):
    provider: str
    fetched: int
    new_items: int
    duplicates: int
    errors: int
    items: list[NewsItem] = Field(default_factory=list)
