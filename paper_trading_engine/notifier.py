"""Push notifier for the paper trading engine — uses Discord webhooks."""
from discord_notifier import DiscordNotifier  # noqa: F401 (relative inside container)

# Alias kept so existing `from notifier import TelegramNotifier` in engine.py resolves
TelegramNotifier = DiscordNotifier

__all__ = ["DiscordNotifier", "TelegramNotifier"]
