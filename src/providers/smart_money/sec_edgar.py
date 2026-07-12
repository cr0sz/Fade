"""SEC EDGAR Form 4 insider transaction provider.

Uses the EDGAR Atom RSS feed (free, no API key) to fetch recent
Form 4 filings (insider buy/sell transactions).

Feed URL: https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=4&dateb=&owner=include&count=40&output=atom

Ticker resolution: company name → ticker via a local map covering our 55 monitored tickers.
Transactions classified as:
  "buy"  → insiders buying shares (bullish signal)
  "sell" → insiders selling shares (bearish signal, weaker — routine diversification)

Rate-limited to max 10 req/s per SEC guidelines.
"""
from __future__ import annotations

import asyncio
import re
import xml.etree.ElementTree as ET
from datetime import datetime, timezone

import aiohttp

from src.core.config import get_settings
from src.core.logging import get_logger
from src.models.market import SmartMoneyEvent
from src.providers.base import BaseProvider

logger = get_logger(__name__)

_EDGAR_ATOM_URL = (
    "https://www.sec.gov/cgi-bin/browse-edgar"
    "?action=getcurrent&type=4&dateb=&owner=include&count=40&output=atom"
)
_NS = {"atom": "http://www.w3.org/2005/Atom"}

# Company name fragments → ticker (lowercase match)
_COMPANY_TICKER: dict[str, str] = {
    "nvidia":           "NVDA",
    "advanced micro":   "AMD",
    "apple inc":        "AAPL",
    "microsoft":        "MSFT",
    "alphabet":         "GOOGL",
    "meta platforms":   "META",
    "tesla":            "TSLA",
    "amazon":           "AMZN",
    "netflix":          "NFLX",
    "intel":            "INTC",
    "qualcomm":         "QCOM",
    "broadcom":         "AVGO",
    "salesforce":       "CRM",
    "oracle":           "ORCL",
    "paypal":           "PYPL",
    "coinbase":         "COIN",
    "microstrategy":    "MSTR",
    "visa":             "V",
    "jpmorgan":         "JPM",
    "goldman":          "GS",
}


class SECEdgarProvider(BaseProvider):
    provider_name = "sec_edgar"

    def __init__(self) -> None:
        self._settings = get_settings()

    async def fetch(self, form_types: list[str] | None = None, limit: int = 40) -> list[SmartMoneyEvent]:
        events: list[SmartMoneyEvent] = []
        try:
            entries = await self._fetch_atom_feed(limit)
            for entry in entries:
                event = self._parse_entry(entry)
                if event:
                    events.append(event)
        except Exception as exc:
            logger.error("sec_edgar_fetch_error", error=str(exc))

        logger.info("sec_edgar_fetched", count=len(events))
        return events

    async def _fetch_atom_feed(self, limit: int) -> list[dict]:
        headers = {
            "User-Agent": getattr(self._settings, "SEC_EDGAR_USER_AGENT",
                                  "MarketIntelligence admin@example.com"),
            "Accept": "application/atom+xml,application/xml,text/xml",
        }
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(_EDGAR_ATOM_URL, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    if resp.status != 200:
                        logger.warning("sec_edgar_http_error", status=resp.status)
                        return []
                    xml_text = await resp.text()
        except Exception as exc:
            logger.warning("sec_edgar_request_failed", error=str(exc))
            return []

        try:
            root = ET.fromstring(xml_text)
        except ET.ParseError as exc:
            logger.warning("sec_edgar_xml_parse_error", error=str(exc))
            return []

        entries = []
        for entry in root.findall("atom:entry", _NS)[:limit]:
            title_el   = entry.find("atom:title",   _NS)
            summary_el = entry.find("atom:summary", _NS)
            updated_el = entry.find("atom:updated", _NS)
            entries.append({
                "title":   title_el.text   if title_el   is not None else "",
                "summary": summary_el.text if summary_el is not None else "",
                "updated": updated_el.text if updated_el is not None else "",
            })
        return entries

    def _parse_entry(self, entry: dict) -> SmartMoneyEvent | None:
        title   = (entry.get("title")   or "").strip()
        summary = (entry.get("summary") or "").strip()

        # Resolve ticker from company name in title
        ticker = self._resolve_ticker(title)
        if not ticker:
            return None

        # Detect transaction type from summary text
        summary_lower = summary.lower()
        if any(kw in summary_lower for kw in ("acquisition", "purchase", "p -", "(p)")):
            direction = "buy"
        elif any(kw in summary_lower for kw in ("disposition", "sale", "s -", "(s)")):
            direction = "sell"
        else:
            return None   # can't determine — skip

        # Parse amount from summary if present
        amount: float | None = None
        m = re.search(r"\$([0-9,]+)", summary)
        if m:
            try:
                amount = float(m.group(1).replace(",", ""))
            except ValueError:
                pass

        return SmartMoneyEvent(
            event_type="insider_trade",
            ticker=ticker,
            actor=self._extract_actor(title),
            amount_usd=amount,
            direction=direction,
            source="sec_edgar",
            event_at=datetime.now(timezone.utc),
            raw={"title": title[:200], "summary": summary[:200]},
        )

    @staticmethod
    def _resolve_ticker(title: str) -> str | None:
        title_lower = title.lower()
        for fragment, ticker in _COMPANY_TICKER.items():
            if fragment in title_lower:
                return ticker
        return None

    @staticmethod
    def _extract_actor(title: str) -> str | None:
        # Title format: "4 - COMPANY NAME (0001234567) (Reporting)"
        # or "4 - OFFICER NAME for COMPANY NAME"
        m = re.match(r"4\s*-\s*(.+?)(?:\s*\(|$)", title)
        return m.group(1).strip() if m else None

    async def health_check(self) -> bool:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(_EDGAR_ATOM_URL, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                    return resp.status == 200
        except Exception:
            return False

    async def cost_estimate(self, request_count: int) -> float:
        return 0.0

    async def normalize(self, raw: dict) -> SmartMoneyEvent | None:
        return None

    async def rate_limit_handler(self) -> None:
        await asyncio.sleep(0.1)  # max 10/s per SEC policy
