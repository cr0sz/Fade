#!/usr/bin/env python3
"""Embed news rows with Ollama (nomic-embed-text) into Qdrant.

Usage:
    python3 scripts/embed_news_qdrant.py
    python3 scripts/embed_news_qdrant.py --limit 500
    python3 scripts/embed_news_qdrant.py --since-days 30

Run inside a container on the compose network (qdrant + ollama hostnames must resolve),
or set QDRANT_URL / OLLAMA_BASE_URL env vars.
"""
from __future__ import annotations

import argparse
import asyncio
import os
import sys
from pathlib import Path

import aiohttp

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.core.logging import configure_logging, get_logger
from src.db.connection import close_db_pool, get_db_pool

configure_logging()
logger = get_logger(__name__)

QDRANT_URL = os.getenv("QDRANT_URL", "http://qdrant:6333")
OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://ollama:11434")
COLLECTION = os.getenv("QDRANT_COLLECTION", "market_news")
EMBED_MODEL = os.getenv("EMBED_MODEL", "nomic-embed-text")
VECTOR_SIZE = 768  # nomic-embed-text output dimensionality
BATCH_SIZE = 32


async def _embed(session: aiohttp.ClientSession, text: str) -> list[float]:
    async with session.post(
        f"{OLLAMA_BASE_URL}/api/embeddings",
        json={"model": EMBED_MODEL, "prompt": text},
        timeout=aiohttp.ClientTimeout(total=120),
    ) as r:
        r.raise_for_status()
        data = await r.json()
        return data["embedding"]


async def _ensure_collection(session: aiohttp.ClientSession) -> None:
    async with session.get(f"{QDRANT_URL}/collections/{COLLECTION}") as r:
        if r.status == 200:
            logger.info("qdrant_collection_exists", collection=COLLECTION)
            return
    async with session.put(
        f"{QDRANT_URL}/collections/{COLLECTION}",
        json={"vectors": {"size": VECTOR_SIZE, "distance": "Cosine"}},
    ) as r:
        r.raise_for_status()
        logger.info("qdrant_collection_created", collection=COLLECTION)


async def _upsert(session: aiohttp.ClientSession, points: list[dict]) -> None:
    async with session.put(
        f"{QDRANT_URL}/collections/{COLLECTION}/points?wait=true",
        json={"points": points},
    ) as r:
        if r.status not in (200, 202):
            body = await r.text()
            raise RuntimeError(f"qdrant upsert {r.status}: {body[:300]}")


async def main(limit: int | None, since_days: int | None) -> None:
    pool = await get_db_pool()

    where_clauses = ["title IS NOT NULL"]
    params: list = []
    if since_days is not None:
        params.append(since_days)
        where_clauses.append(f"published_at > now() - ($1 * interval '1 day')")

    where = " AND ".join(where_clauses)
    sql = (
        "SELECT id, title, content, summary, ticker_symbols, source, published_at, url "
        f"FROM news WHERE {where} ORDER BY id"
    )
    if limit is not None:
        sql += f" LIMIT {int(limit)}"

    async with aiohttp.ClientSession() as session:
        await _ensure_collection(session)

        async with pool.acquire() as conn:
            rows = await conn.fetch(sql, *params)
        logger.info("embed_start", total=len(rows))

        points: list[dict] = []
        done = 0
        failed = 0
        for row in rows:
            body = row["content"] or row["summary"] or ""
            text = f"{row['title']}\n\n{body}".strip()
            try:
                vec = await _embed(session, text)
            except Exception as exc:
                logger.warning("embed_row_failed", id=row["id"], error=str(exc))
                failed += 1
                continue

            points.append({
                "id": int(row["id"]),
                "vector": vec,
                "payload": {
                    "id": int(row["id"]),
                    "ticker": list(row["ticker_symbols"] or []),
                    "source": row["source"],
                    "published_at": row["published_at"].isoformat()
                    if row["published_at"]
                    else None,
                    "url": row["url"],
                },
            })

            if len(points) >= BATCH_SIZE:
                await _upsert(session, points)
                done += len(points)
                points = []
                logger.info("embed_progress", done=done, total=len(rows), failed=failed)

        if points:
            await _upsert(session, points)
            done += len(points)

        logger.info("embed_complete", upserted=done, failed=failed, total=len(rows))

    await close_db_pool()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--since-days", type=int, default=None)
    args = ap.parse_args()
    asyncio.run(main(args.limit, args.since_days))
