from __future__ import annotations

"""
Reuters News Provider — STUB
Follow runbook: .claude/runbooks/add_news_source.md to implement.
"""

from src.core.logging import get_logger
from src.models.news import NewsItem
from src.providers.base import BaseProvider

logger = get_logger(__name__)


class ReutersProvider(BaseProvider):
    provider_name = "reuters"

    async def fetch(self, **kwargs) -> list[NewsItem]:
        logger.warning("reuters_stub_not_implemented")
        return []

    async def normalize(self, raw: dict) -> NewsItem | None:
        return None

    async def health_check(self) -> bool:
        return False

    async def cost_estimate(self, request_count: int) -> float:
        return 0.0

    async def rate_limit_handler(self) -> None:
        pass
