from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from pydantic import BaseModel


class BaseProvider(ABC):
    """
    Base class for all external data providers.
    Every provider must implement all abstract methods.
    """

    provider_name: str = "base"

    @abstractmethod
    async def fetch(self, **kwargs) -> list[BaseModel]:
        """Fetch data from the external source."""
        ...

    @abstractmethod
    async def normalize(self, raw: dict[str, Any]) -> BaseModel:
        """Normalize raw API response into a Pydantic model."""
        ...

    @abstractmethod
    async def health_check(self) -> bool:
        """Return True if provider is reachable and credentials valid."""
        ...

    @abstractmethod
    async def cost_estimate(self, request_count: int) -> float:
        """Return estimated cost in USD for N requests."""
        ...

    @abstractmethod
    async def rate_limit_handler(self) -> None:
        """Enforce rate limiting — sleep if necessary."""
        ...
