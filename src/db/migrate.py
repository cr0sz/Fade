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

# Cluster-level migrations. Postgres has no "CREATE DATABASE IF NOT EXISTS", so
# these files cannot be made idempotent in plain SQL. The compose stack mounts
# migrations/ into docker-entrypoint-initdb.d, which runs every file once on a
# fresh volume — re-running the raw file here would raise DuplicateDatabaseError
# and take the api/validation-api containers down on first boot. We create them
# guarded instead, then record the version so the file itself is never executed.
_BOOTSTRAP_DATABASES = {"000_create_miniflux_db": "miniflux"}


async def _record(conn: asyncpg.Connection, version: str) -> None:
    """Mark a migration as applied. Safe to call for an already-recorded version."""
    await conn.execute(
        "INSERT INTO schema_migrations (version) VALUES ($1) "
        "ON CONFLICT (version) DO NOTHING",
        version,
    )


async def _apply_bootstrap_databases(
    conn: asyncpg.Connection, applied: set[str]
) -> int:
    """Create cluster-level databases if absent, then record their versions."""
    new_count = 0
    for version, dbname in _BOOTSTRAP_DATABASES.items():
        if version in applied:
            continue
        exists = await conn.fetchval(
            "SELECT 1 FROM pg_database WHERE datname = $1", dbname
        )
        if exists:
            logger.info(
                "migration_skip", file=f"{version}.sql", reason="database_exists"
            )
        else:
            # CREATE DATABASE cannot run inside a transaction block.
            await conn.execute(f'CREATE DATABASE "{dbname}"')
            logger.info("migration_applied", file=f"{version}.sql")
            new_count += 1
        await _record(conn, version)
        applied.add(version)
    return new_count


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

        new_count = await _apply_bootstrap_databases(conn, applied)

        migrations_dir = Path(__file__).parent.parent.parent / "migrations"
        sql_files = sorted(migrations_dir.glob("*.sql"))

        for sql_file in sql_files:
            version = sql_file.stem  # e.g. "001_initial"
            if version in applied:
                logger.info("migration_skip", file=sql_file.name, reason="already_applied")
                continue
            logger.info("migration_applying", file=sql_file.name)
            # One transaction per file: a failure rolls the whole file back
            # instead of leaving the schema half-migrated. None of the
            # migrations use CREATE INDEX CONCURRENTLY, which would forbid this.
            async with conn.transaction():
                await conn.execute(sql_file.read_text(encoding="utf-8"))
            # Record here rather than relying on each file to insert its own
            # row — most of them don't, which used to make them re-run on
            # every single container start.
            await _record(conn, version)
            applied.add(version)
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
