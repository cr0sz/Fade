from __future__ import annotations

import asyncio
from typing import Any

import aiohttp

from src.core.config import get_settings
from src.core.logging import get_logger
from src.providers.ai.openai_provider import AIResponse
from src.providers.base import BaseProvider

logger = get_logger(__name__)


class AnthropicProvider(BaseProvider):
    provider_name = "anthropic"

    def __init__(self) -> None:
        self._settings = get_settings()
        self._api_key = self._settings.ANTHROPIC_API_KEY
        self._model = self._settings.ANTHROPIC_MODEL
        self._base = "https://api.anthropic.com/v1"

    async def fetch(self, **kwargs) -> list[Any]:
        raise NotImplementedError("Use complete() instead")

    async def normalize(self, raw: dict) -> Any:
        raise NotImplementedError

    async def complete(
        self,
        prompt: str,
        system: str = "You are a financial market intelligence analyst. Never give financial advice.",
        max_tokens: int | None = None,
        temperature: float | None = None,
    ) -> AIResponse:
        if not self._api_key:
            raise ValueError("ANTHROPIC_API_KEY not configured")

        max_tokens = max_tokens or self._settings.ANTHROPIC_MAX_TOKENS
        temperature = temperature if temperature is not None else 0.1

        payload = {
            "model": self._model,
            "max_tokens": max_tokens,
            "temperature": temperature,
            "system": system,
            "messages": [{"role": "user", "content": prompt}],
        }
        headers = {
            "x-api-key": self._api_key,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json",
        }

        async with aiohttp.ClientSession() as session:
            await self.rate_limit_handler()
            async with session.post(
                f"{self._base}/messages", json=payload, headers=headers
            ) as resp:
                if resp.status == 529:
                    await asyncio.sleep(30)
                    raise RuntimeError("Anthropic overloaded")
                resp.raise_for_status()
                data = await resp.json()

        usage = data.get("usage", {})
        input_tokens = usage.get("input_tokens", 0)
        output_tokens = usage.get("output_tokens", 0)
        cost = (
            input_tokens / 1_000_000 * self._settings.ANTHROPIC_INPUT_COST_PER_1M
            + output_tokens / 1_000_000 * self._settings.ANTHROPIC_OUTPUT_COST_PER_1M
        )
        content = data["content"][0]["text"]

        return AIResponse(
            content=content,
            model=self._model,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            cost_usd=round(cost, 6),
        )

    async def health_check(self) -> bool:
        if not self._api_key:
            return False
        try:
            resp = await self.complete("Hello", max_tokens=5)
            return bool(resp.content)
        except Exception:
            return False

    async def cost_estimate(self, request_count: int) -> float:
        avg_input = 500
        avg_output = 300
        return request_count * (
            avg_input / 1_000_000 * self._settings.ANTHROPIC_INPUT_COST_PER_1M
            + avg_output / 1_000_000 * self._settings.ANTHROPIC_OUTPUT_COST_PER_1M
        )

    async def rate_limit_handler(self) -> None:
        await asyncio.sleep(0.1)
