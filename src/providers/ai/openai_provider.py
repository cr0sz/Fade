from __future__ import annotations

import asyncio
from typing import Any

import aiohttp

from src.core.config import get_settings
from src.core.logging import get_logger
from src.providers.base import BaseProvider

logger = get_logger(__name__)


class AIResponse:
    def __init__(
        self,
        content: str,
        model: str,
        input_tokens: int,
        output_tokens: int,
        cost_usd: float,
    ) -> None:
        self.content = content
        self.model = model
        self.input_tokens = input_tokens
        self.output_tokens = output_tokens
        self.cost_usd = cost_usd


class OpenAIProvider(BaseProvider):
    provider_name = "openai"

    def __init__(self) -> None:
        self._settings = get_settings()
        self._api_key = self._settings.OPENAI_API_KEY
        self._model = self._settings.OPENAI_MODEL
        self._base = "https://api.openai.com/v1"

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
            raise ValueError("OPENAI_API_KEY not configured")

        max_tokens = max_tokens or self._settings.OPENAI_MAX_TOKENS
        temperature = temperature if temperature is not None else self._settings.OPENAI_TEMPERATURE

        payload = {
            "model": self._model,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": prompt},
            ],
            "max_tokens": max_tokens,
            "temperature": temperature,
        }

        headers = {
            "Authorization": f"Bearer {self._api_key}",
            "Content-Type": "application/json",
        }

        async with aiohttp.ClientSession() as session:
            await self.rate_limit_handler()
            async with session.post(
                f"{self._base}/chat/completions", json=payload, headers=headers
            ) as resp:
                if resp.status == 429:
                    await asyncio.sleep(30)
                    raise RuntimeError("OpenAI rate limited")
                resp.raise_for_status()
                data = await resp.json()

        usage = data.get("usage", {})
        input_tokens = usage.get("prompt_tokens", 0)
        output_tokens = usage.get("completion_tokens", 0)
        cost = (
            input_tokens / 1_000_000 * self._settings.OPENAI_INPUT_COST_PER_1M
            + output_tokens / 1_000_000 * self._settings.OPENAI_OUTPUT_COST_PER_1M
        )
        content = data["choices"][0]["message"]["content"]

        logger.debug(
            "openai_completion",
            model=self._model,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            cost_usd=round(cost, 6),
        )

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
            avg_input / 1_000_000 * self._settings.OPENAI_INPUT_COST_PER_1M
            + avg_output / 1_000_000 * self._settings.OPENAI_OUTPUT_COST_PER_1M
        )

    async def rate_limit_handler(self) -> None:
        await asyncio.sleep(0.1)
