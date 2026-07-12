#!/usr/bin/env python3
"""Standalone backfill script — scores all unscored news articles via Ollama.

Usage:
    python3 scripts/backfill_news.py            # run backfill
    python3 scripts/backfill_news.py --dry-run  # print backlog size and exit
"""
from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

# Allow running from repo root without installing the package
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.core.logging import get_logger
from src.db.connection import close_db_pool, get_db_pool
from src.engines.news_intelligence import NewsIntelligenceEngine
from src.providers.ai.ollama_provider import OllamaProvider

logger = get_logger(__name__)


async def _get_remaining(pool) -> int:
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT COUNT(*) AS cnt FROM news"
            " WHERE impact_score IS NULL"
            " AND published_at > now() - interval '48 hours'"
        )
        return int(row["cnt"]) if row else 0


async def main(dry_run: bool = False) -> None:
    pool = await get_db_pool()

    remaining = await _get_remaining(pool)
    print(f"backfill_news: backlog={remaining} unscored articles (last 48 h)")

    if dry_run:
        await close_db_pool()
        return

    if remaining == 0:
        print("backfill_news: nothing to do")
        await close_db_pool()
        return

    ai_provider = OllamaProvider()
    engine = NewsIntelligenceEngine(pool)

    total_scored = 0
    batch_num = 0
    try:
        while True:
            batch_num += 1
            scored = await engine.batch_analyze(ai_provider)
            if scored == 0:
                break
            total_scored += scored
            remaining = await _get_remaining(pool)
            print(
                f"backfill_batch: batch={batch_num} scored={scored}"
                f" total_scored={total_scored} remaining={remaining}"
            )
            logger.info(
                "backfill_batch",
                batch=batch_num,
                scored=scored,
                total_scored=total_scored,
                remaining=remaining,
            )
            if remaining == 0:
                break
            await asyncio.sleep(2)
    except KeyboardInterrupt:
        print(f"\nbackfill_news: interrupted after {total_scored} articles scored")
    finally:
        await close_db_pool()

    print(f"backfill_news: done total_scored={total_scored}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Backfill impact_score for unscored news articles")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print backlog size and exit without scoring",
    )
    args = parser.parse_args()
    asyncio.run(main(dry_run=args.dry_run))
