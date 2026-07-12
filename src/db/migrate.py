"""Run database migrations — idempotent via schema_migrations tracking."""
from __future__ import annotations

import asyncio
from pathlib import Path

import asyncpg

from src.core.config import get_settings
from src.core.logging import configure_logging, get_logger

logger = get_logger(__name__)

_BOOTSTRAP_DDL = """
CREATE TABLE IF NOT EXISTS schema_migrations (
    version    VARCHAR(50) PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
)
"""


async def run_migrations() -> None:
    configure_logging()
    settings = get_settings()

    conn = await asyncpg.connect(dsn=settings.database_url)
    try:
        # Bootstrap tracker before the first migration creates it
        await conn.execute(_BOOTSTRAP_DDL)

        applied: set[str] = {
            row["version"]
            for row in await conn.fetch("SELECT version FROM schema_migrations")
        }

        migrations_dir = Path(__file__).parent.parent.parent / "migrations"
        sql_files = sorted(migrations_dir.glob("*.sql"))

        new_count = 0
        for sql_file in sql_files:
            version = sql_file.stem  # e.g. "001_initial"
            if version in applied:
                logger.info("migration_skip", file=sql_file.name, reason="already_applied")
                continue
            logger.info("migration_applying", file=sql_file.name)
            await conn.execute(sql_file.read_text())
            logger.info("migration_applied", file=sql_file.name)
            new_count += 1

        logger.info(
            "migrations_complete",
            applied=new_count,
            skipped=len(sql_files) - new_count,
        )
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(run_migrations())
