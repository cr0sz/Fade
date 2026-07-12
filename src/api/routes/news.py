from __future__ import annotations

from fastapi import APIRouter, Query, Request

from src.db.repositories import NewsRepository

router = APIRouter()

_IMPACT_SQL = """
SELECT id, title, source, source_domain, url, published_at, ticker_symbols,
       impact_score, urgency, source_authority, ai_summary
  FROM news
 WHERE impact_score IS NOT NULL
   AND published_at > now() - interval '7 days'
 ORDER BY urgency DESC NULLS LAST, published_at DESC, ABS(impact_score) DESC
 LIMIT 30
"""


@router.get("/news/impact")
async def get_news_impact(request: Request) -> list:
    """Return top 30 scored articles sorted by urgency DESC, ABS(impact_score) DESC."""
    async with request.app.state.db.acquire() as conn:
        rows = await conn.fetch(_IMPACT_SQL)
    return [
        {
            "id": r["id"],
            "title": r["title"],
            "source": r["source"],
            "source_domain": r["source_domain"],
            "url": r["url"],
            "published_at": r["published_at"].isoformat() if r["published_at"] else None,
            "ticker_symbols": r["ticker_symbols"],
            "impact_score": float(r["impact_score"]) if r["impact_score"] is not None else None,
            "urgency": r["urgency"],
            "source_authority": float(r["source_authority"]) if r["source_authority"] is not None else None,
            "ai_summary": r["ai_summary"],
        }
        for r in rows
    ]


@router.get("/news/unscored-count")
async def get_news_unscored_count(request: Request) -> dict:
    """Count recent (last 48h) articles awaiting impact scoring.

    Bounded to the same window the batch scorer processes — older articles
    are trading-irrelevant and intentionally skipped by the scorer, so
    counting them produced a permanent ~18k misleading backlog warning.
    """
    async with request.app.state.db.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT COUNT(*) AS count FROM news "
            "WHERE impact_score IS NULL "
            "AND published_at > now() - interval '48 hours'"
        )
    return {"count": int(row["count"])}


@router.get("/news")
async def get_news(
    request: Request,
    ticker: str | None = Query(None),
    sector: str | None = Query(None),
    hours: int = Query(24, ge=1, le=168),
    limit: int = Query(20, ge=1, le=100),
) -> dict:
    repo = NewsRepository(request.app.state.db)
    items = await repo.get_recent(hours=hours, ticker=ticker, sector=sector, limit=limit)
    return {
        "count": len(items),
        "filters": {"ticker": ticker, "sector": sector, "hours": hours},
        "items": [
            {
                "id": str(n["id"]),
                "title": n["title"],
                "source": n["source"],
                "published_at": str(n["published_at"]),
                "ticker_symbols": n["ticker_symbols"],
                "sector": n["sector"],
                "ai_summary": n["ai_summary"],
                "url": n["url"],
            }
            for n in items
        ],
    }


@router.get("/news/macro")
async def get_macro_news(request: Request) -> dict:
    repo = NewsRepository(request.app.state.db)
    macro_keywords = ["Federal Reserve", "interest rate", "inflation", "CPI", "GDP",
                      "TCMB", "ECB", "recession", "yield", "treasury"]
    items = []
    for kw in macro_keywords[:5]:
        kw_items = await repo.get_recent(hours=48, limit=5)
        items.extend(kw_items)

    seen = set()
    unique = []
    for item in items:
        if item["id"] not in seen:
            seen.add(item["id"])
            unique.append(item)

    return {
        "type": "macro",
        "count": len(unique[:20]),
        "items": [{"title": n["title"], "source": n["source"], "url": n["url"]} for n in unique[:20]],
    }
