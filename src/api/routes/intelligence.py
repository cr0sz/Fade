"""
Level-3 Institutional Intelligence API routes.

All endpoints are read-only — they query the latest computed results
from institutional_flows, macro_regimes, and confluence_signals.

No computation happens at request time; all data is pre-computed by the
Level-3 engine cycle (runs every 15 min via scheduler).
"""
from __future__ import annotations

from fastapi import APIRouter, Query, Request

from src.engines.institutional_confluence_engine import InstitutionalConfluenceEngine
from src.engines.institutional_flow_engine import InstitutionalFlowEngine
from src.engines.macro_regime_engine import MacroRegimeEngine

router = APIRouter()


@router.get("/intelligence/regime")
async def get_macro_regime(request: Request) -> dict:
    """
    Current macro regime (risk_on / risk_off / tightening /
    liquidity_expansion / volatility_regime).
    Disclaimer: For informational purposes only. Not financial advice.
    """
    engine = MacroRegimeEngine(request.app.state.db, request.app.state.redis)
    regime = await engine.get_current_regime()
    return {
        "regime": regime,
        "disclaimer": "For informational purposes only. Not financial advice.",
    }


@router.get("/intelligence/flows")
async def get_institutional_flows(
    request: Request,
    limit: int = Query(20, ge=1, le=100),
) -> dict:
    """
    Latest institutional capital flow signals per ticker,
    ordered by flow strength descending.
    """
    engine = InstitutionalFlowEngine(request.app.state.db, request.app.state.redis)
    flows       = await engine.get_latest_flows(limit=limit)
    market_flow = await engine.get_market_flow()
    return {
        "market_flow": market_flow,
        "ticker_flows": flows,
        "count": len(flows),
        "disclaimer": "For informational purposes only. Not financial advice.",
    }


@router.get("/intelligence/flows/{ticker}")
async def get_ticker_flow(request: Request, ticker: str) -> dict:
    """Latest institutional flow signal for a specific ticker."""
    engine = InstitutionalFlowEngine(request.app.state.db, request.app.state.redis)
    flow = await engine.get_ticker_flow(ticker.upper())
    return {
        "ticker": ticker.upper(),
        "flow": flow,
        "disclaimer": "For informational purposes only. Not financial advice.",
    }



@router.get("/intelligence/confluence")
async def get_confluence(
    request: Request,
    limit: int = Query(10, ge=1, le=50),
) -> dict:
    """
    Institutional confluence signals — market-wide and top tickers.
    Confluence combines narrative, confidence, flow, and regime signals.
    """
    engine          = InstitutionalConfluenceEngine(request.app.state.db, request.app.state.redis)
    market_signal   = await engine.get_market_confluence()
    top_tickers     = await engine.get_strongest_confluences(limit=limit)
    return {
        "market_confluence": market_signal,
        "top_tickers":       top_tickers,
        "count":             len(top_tickers),
        "disclaimer":        "For informational purposes only. Not financial advice.",
    }


@router.get("/intelligence/confluence/{ticker}")
async def get_ticker_confluence(request: Request, ticker: str) -> dict:
    """Confluence signal for a specific ticker."""
    engine = InstitutionalConfluenceEngine(request.app.state.db, request.app.state.redis)
    signal = await engine.get_ticker_confluence(ticker.upper())
    return {
        "ticker":    ticker.upper(),
        "confluence": signal,
        "disclaimer": "For informational purposes only. Not financial advice.",
    }
