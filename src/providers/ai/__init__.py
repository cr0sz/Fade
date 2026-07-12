from .anthropic_provider import AnthropicProvider
from .ollama_provider import OllamaProvider
from .openai_provider import AIResponse, OpenAIProvider

AI_PROVIDERS: dict[str, type] = {
    "ollama": OllamaProvider,
    "openai": OpenAIProvider,
    "anthropic": AnthropicProvider,
}

__all__ = ["AI_PROVIDERS", "OllamaProvider", "OpenAIProvider", "AnthropicProvider", "AIResponse"]
