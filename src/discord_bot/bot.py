"""Discord slash-command bot."""
from __future__ import annotations

import asyncio
import os

import discord
from discord import app_commands

from src.cache.redis_client import get_redis
from src.core.cost_control import init_cost_controller
from src.core.logging import bind_service_context, configure_logging, get_logger
from src.db.connection import get_db_pool

logger = get_logger(__name__)


class _Bot(discord.Client):
    def __init__(self) -> None:
        intents = discord.Intents.default()
        super().__init__(intents=intents)
        self.tree = app_commands.CommandTree(self)
        self._db = None
        self._redis = None

    async def setup_hook(self) -> None:
        self._db = await get_db_pool()
        self._redis = await get_redis()
        cost = await init_cost_controller(self._redis)

        from src.discord_bot.handlers import register_commands
        register_commands(self.tree, self._db, self._redis, cost)

        guild_id_str = os.getenv("DISCORD_GUILD_ID", "")
        if guild_id_str:
            guild = discord.Object(id=int(guild_id_str))
            self.tree.copy_global_to(guild=guild)
            await self.tree.sync(guild=guild)
            logger.info("discord_commands_synced", scope="guild", guild_id=guild_id_str)
        else:
            await self.tree.sync()
            logger.info("discord_commands_synced", scope="global")

    async def on_ready(self) -> None:
        logger.info("discord_bot_ready", user=str(self.user), user_id=str(self.user.id) if self.user else "?")


async def main() -> None:
    configure_logging()
    bind_service_context("discord-bot")

    token = os.getenv("DISCORD_BOT_TOKEN", "")
    if not token:
        logger.error("discord_token_missing")
        raise SystemExit("DISCORD_BOT_TOKEN not configured")

    bot = _Bot()
    async with bot:
        await bot.start(token)


if __name__ == "__main__":
    asyncio.run(main())
