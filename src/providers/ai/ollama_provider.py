"""Local Ollama AI provider — zero cost, runs in the compose stack."""
from __future__ import annotations

import os
import time

import httpx

from .openai_provider import AIResponse

OLLAMA_URL = os.getenv("OLLAMA_BASE_URL", "http://ollama:11434")
DEFAULT_MODEL = os.getenv("OLLAMA_NEWS_MODEL", "qwen2.5:3b")
TIMEOUT = 120.0


class OllamaProvider:
    def __init__(self) -> None:
        self._model = DEFAULT_MODEL

    async def complete(self, prompt: str, max_tokens: int = 400) -> AIResponse:
        t0 = time.monotonic()
        async with httpx.AsyncClient(timeout=TIMEOUT) as client:
            resp = await client.post(
                f"{OLLAMA_URL}/api/generate",
                json={"model": self._model, "prompt": prompt, "stream": False},
            )
            resp.raise_for_status()
        content = resp.json().get("response", "").strip()
        latency = time.monotonic() - t0
        return AIResponse(
            content=content,
            model=self._model,
            cost_usd=0.0,
            input_tokens=0,
            output_tokens=0,
        )
