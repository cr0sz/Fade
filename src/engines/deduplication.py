"""
Hybrid deduplication for news articles.

Three-level pipeline (each level is a superset of the previous):

  Level 1 — Exact hash (SHA256 of normalized title + domain + date)
             → catches identical articles from multiple providers
             → stored in PostgreSQL news.content_hash (existing)

  Level 2 — Fuzzy title fingerprint (token-sort SHA256)
             → catches reordered / lightly rephrased titles
             → stored in Redis, TTL 24h
             → cost: zero (no API calls)

  Level 3 — Semantic embedding similarity (OpenAI text-embedding-3-small)
             → catches substantively duplicate articles with different wording
             → cost-gated: only runs when budget OK and DEDUP_USE_EMBEDDINGS=true
             → vectors stored in Redis as compact float16 arrays, TTL 6h
             → threshold: cosine similarity >= DEDUP_SEMANTIC_THRESHOLD (default 0.92)

Decision table:
  Match at Level 1 → DUPLICATE (discard)
  Match at Level 2 → DUPLICATE (discard, log as fuzzy_dup)
  Match at Level 3 → DUPLICATE (discard, log as semantic_dup)
  No match        → NEW (store)
"""
from __future__ import annotations

import hashlib
import json
import re
import struct
import time
from typing import TYPE_CHECKING

import redis.asyncio as aioredis

from src.core.config import get_settings
from src.core.logging import get_logger

if TYPE_CHECKING:
    from src.models.news import NewsItem

logger = get_logger(__name__)

_PUNCT_RE = re.compile(r"[^\w\s]")
_SPACE_RE = re.compile(r"\s+")


def _normalize_title(title: str) -> str:
    """Lowercase, strip punctuation, sort tokens — order-invariant fingerprint."""
    clean = _PUNCT_RE.sub(" ", title.lower())
    clean = _SPACE_RE.sub(" ", clean).strip()
    return " ".join(sorted(clean.split()))


def fuzzy_title_hash(title: str) -> str:
    return hashlib.sha256(_normalize_title(title).encode()).hexdigest()[:32]


def _cosine_similarity(a: list[float], b: list[float]) -> float:
    dot = sum(x * y for x, y in zip(a, b))
    norm_a = sum(x * x for x in a) ** 0.5
    norm_b = sum(x * x for x in b) ** 0.5
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return dot / (norm_a * norm_b)


def _pack_vector(vec: list[float]) -> bytes:
    """Pack float32 list to bytes for compact Redis storage."""
    return struct.pack(f"{len(vec)}f", *vec)


def _unpack_vector(data: bytes) -> list[float]:
    n = len(data) // 4
    return list(struct.unpack(f"{n}f", data))


class HybridDeduplicator:
    """
    Stateful hybrid deduplicator. Requires Redis for levels 2 and 3.
    The PostgreSQL check (level 1) is delegated to NewsRepository — this class
    only handles the in-process Redis layers.
    """

    FUZZY_KEY_PREFIX = "dedup:fuzzy"
    EMBED_KEY_PREFIX = "dedup:embed"
    FUZZY_TTL = 86400        # 24 hours
    EMBED_TTL = 21600        # 6 hours
    EMBED_WINDOW_SCAN = 200  # max recent embeddings to compare against

    def __init__(self, redis: aioredis.Redis) -> None:
        self._r = redis
        self._settings = get_settings()

    # ── Level 2: Fuzzy title dedup ────────────────────────────────────────────

    async def is_fuzzy_duplicate(self, title: str) -> bool:
        fhash = fuzzy_title_hash(title)
        key = f"{self.FUZZY_KEY_PREFIX}:{fhash}"
        exists = await self._r.exists(key)
        return bool(exists)

    async def register_fuzzy(self, title: str) -> None:
        fhash = fuzzy_title_hash(title)
        key = f"{self.FUZZY_KEY_PREFIX}:{fhash}"
        await self._r.setex(key, self.FUZZY_TTL, "1")

    # ── Level 3: Semantic embedding dedup ─────────────────────────────────────

    async def _get_embedding(self, text: str, openai_api_key: str) -> list[float] | None:
        """Call OpenAI text-embedding-3-small with 512 dims (cost: ~$0.00002/1k tokens)."""
        try:
            import aiohttp

            payload = {
                "input": text[:2000],
                "model": "text-embedding-3-small",
                "dimensions": 512,
            }
            headers = {
                "Authorization": f"Bearer {openai_api_key}",
                "Content-Type": "application/json",
            }
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    "https://api.openai.com/v1/embeddings",
                    json=payload,
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as resp:
                    if resp.status != 200:
                        return None
                    data = await resp.json()
                    return data["data"][0]["embedding"]
        except Exception as exc:
            logger.warning("embedding_error", error=str(exc))
            return None

    async def is_semantic_duplicate(
        self,
        title: str,
        content_hash: str,
        openai_api_key: str,
        threshold: float | None = None,
    ) -> bool:
        threshold = threshold or getattr(self._settings, "DEDUP_SEMANTIC_THRESHOLD", 0.92)

        vec = await self._get_embedding(title, openai_api_key)
        if vec is None:
            return False  # graceful degradation

        # Load recent embeddings from Redis for comparison
        pattern = f"{self.EMBED_KEY_PREFIX}:*"
        duplicate_found = False
        cursor = 0
        compared = 0

        while compared < self.EMBED_WINDOW_SCAN:
            cursor, keys = await self._r.scan(cursor, match=pattern, count=50)
            for key in keys:
                if compared >= self.EMBED_WINDOW_SCAN:
                    break
                raw = await self._r.get(key)
                if raw is None:
                    continue
                try:
                    stored_vec = _unpack_vector(raw)
                    sim = _cosine_similarity(vec, stored_vec)
                    if sim >= threshold:
                        logger.info(
                            "dedup_semantic_match",
                            similarity=round(sim, 4),
                            threshold=threshold,
                        )
                        duplicate_found = True
                        break
                except Exception:
                    continue
                compared += 1

            if cursor == 0 or duplicate_found:
                break

        if not duplicate_found:
            # Store this embedding for future comparison
            key = f"{self.EMBED_KEY_PREFIX}:{content_hash}"
            await self._r.setex(key, self.EMBED_TTL, _pack_vector(vec))

        return duplicate_found

    # ── Combined check ────────────────────────────────────────────────────────

    async def check(
        self,
        item: "NewsItem",
        db_hash_exists: bool,
        use_embeddings: bool = False,
        openai_api_key: str = "",
    ) -> tuple[bool, str]:
        """
        Returns (is_duplicate, reason).
        reason: "exact" | "fuzzy" | "semantic" | "new"
        """
        # Level 1: DB exact hash (caller already queried — pass result in)
        if db_hash_exists:
            return True, "exact"

        # Level 2: fuzzy title
        if item.title and await self.is_fuzzy_duplicate(item.title):
            logger.debug("dedup_fuzzy", title=item.title[:60])
            return True, "fuzzy"

        # Level 3: semantic (opt-in, cost-gated)
        if use_embeddings and openai_api_key and item.content_hash:
            if await self.is_semantic_duplicate(item.title, item.content_hash, openai_api_key):
                return True, "semantic"

        # Not a duplicate — register at level 2
        if item.title:
            await self.register_fuzzy(item.title)

        return False, "new"
