"""Risk gate: validates a signal against current portfolio state.

Per-market thresholds reflect volatility and liquidity profile of each exchange:
  CRYPTO  — highest volatility → strictest confidence bar, smallest position size
  BIST    — emerging market / currency risk → tighter than US equities
  NASDAQ/NYSE — standard US equities
  ETF     — broad market instruments → lower bar, larger allocation OK

Macro regime override: if the global regime is risk_off, long signals require
an additional +0.10 confidence margin to compensate for adverse conditions.

Confidence stratification (Phase 4):
  Minimum thresholds and position fractions are 2D tables (exchange × confidence tier).
  "medium" tier values match the previous flat thresholds — no regression.
  High-confidence signals get a lower bar and larger allocation (reward conviction).
  Low-confidence signals face a tighter bar and smaller allocation (reduce exposure).
"""
from __future__ import annotations

import os as _os
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from models import Signal
    from portfolio import Portfolio

# ── Confidence tier boundaries (aligned with ConfidenceEngine) ───────────────
# Raised from 0.70→0.72 after ConfidenceEngine default-score recalibration
# (stable-zone and EMA-convergence dimensions no longer inflate to 1.0).
_CONF_HIGH   = 0.72
_CONF_MEDIUM = 0.42


def _confidence_tier(confidence: float) -> str:
    if confidence >= _CONF_HIGH:
        return "high"
    if confidence >= _CONF_MEDIUM:
        return "medium"
    return "low"


# ── Per-exchange × confidence-tier minimum confidence ─────────────────────────
# IMPORTANT: signal accuracy data (30-day, n=305 CRYPTO risk_on outcomes) shows
# confidence is INVERSELY correlated with accuracy:
#   >= 0.85 conf → 36.2% accuracy (worse than random — pump-chasing)
#   0.65-0.72 conf → 42.5% accuracy
#   < 0.65 conf → 79.1% accuracy (genuinely trending, low hype)
#
# Thresholds lowered to admit moderate-confidence signals (where edge is real).
# A MAX_CONFIDENCE cap (0.82) is enforced separately in the evaluate() function.
_MIN_CONFIDENCE_TIERS: dict[str, dict[str, float]] = {
    "CRYPTO": {"high": 0.55, "medium": 0.52, "low": 0.38},
    "BIST":   {"high": 0.61, "medium": 0.68, "low": 0.80},
    "":       {"high": 0.57, "medium": 0.64, "low": 0.76},
}

# ── Per-exchange × confidence-tier position fraction ──────────────────────────
# medium tier = previous TRADE_FRACTION (zero regression).
# high  tier  = medium × 1.3 (larger allocation for high conviction).
# low   tier  = medium × 0.6 (reduce exposure for weak signals).
TRADE_FRACTION_TIERS: dict[str, dict[str, float]] = {
    # CRYPTO reduced — high volatility + poll lag means effective losses are
    # larger than the stop-loss %; smaller bets limit damage per bad trade.
    "CRYPTO": {"high": 0.045, "medium": 0.035, "low": 0.020},
    "BIST":   {"high": 0.091, "medium": 0.07,  "low": 0.042},
    "":       {"high": 0.110, "medium": 0.085, "low": 0.050},
}

# Backward-compat flat lookup (medium tier) — used by executor fallback
TRADE_FRACTION: dict[str, float] = {
    exch: tiers["medium"] for exch, tiers in TRADE_FRACTION_TIERS.items()
}

# ── Portfolio-level limits ─────────────────────────────────────────────────────
MAX_OPEN_TRADES   = 8     # global cap
MAX_CRYPTO_TRADES = int(_os.getenv("MAX_CRYPTO_TRADES", "4"))
MAX_BIST_TRADES   = 3     # BIST sub-limit
MIN_BALANCE       = 100.0

# ── Signal conviction requirements ────────────────────────────────────────────
# At least one component score must clear its threshold — prevents trades where
# ALL three components are near zero (pure noise signals).
_MIN_NEWS_CONVICTION   = float(_os.getenv("MIN_NEWS_CONVICTION",   "0.10"))
_MIN_SOCIAL_CONVICTION = float(_os.getenv("MIN_SOCIAL_CONVICTION", "0.15"))
_MIN_VOLUME_CONVICTION = float(_os.getenv("MIN_VOLUME_CONVICTION", "0.05"))

# ── Macro regime penalty for longs ────────────────────────────────────────────
_RISK_OFF_LONG_PENALTY = 0.10


def _min_confidence(exchange: str, direction: str, macro_regime: str, confidence: float, vix: float = 0.0,
                    router_confirmed: bool = False, router_inverted: bool = False,
                    oos_override: bool = False) -> float:
    # Router-confirmed/inverted non-CRYPTO signals have measured 24h price edge. Their raw
    # composite_score (used as confidence proxy) is on a different scale than
    # the confidence_score from momentum engines — use a flat 0.05 floor so the
    # ETF/ICE rotation model isn't blocked by the momentum-engine confidence bar.
    if (router_confirmed or router_inverted) and (exchange not in ("CRYPTO",) or oos_override):
        return 0.05  # near-zero floor; router edge measurement is the quality bar
    tier = _confidence_tier(confidence)
    base = _MIN_CONFIDENCE_TIERS.get(exchange, _MIN_CONFIDENCE_TIERS[""])[tier]
    if direction == "long" and macro_regime in ("risk_off", "tightening", "volatility_regime"):
        base += _RISK_OFF_LONG_PENALTY
    # Mirror: lower the bar for shorts in fearful / risk-off conditions
    if direction == "short":
        if macro_regime in ("risk_off", "tightening", "volatility_regime"):
            base -= 0.10
        if vix >= 25.0:
            base -= 0.05
        # Counter-trend penalty: crypto shorts in bull/risk_on regime have higher bleed risk.
        # Data shows low-conf signals are the edge (79.1% acc); penalty kept minimal.
        if exchange == "CRYPTO" and macro_regime == "risk_on" and not oos_override:
            base += 0.05
    return max(0.20, base)  # never drop below 20% confidence floor


def trade_fraction(exchange: str, confidence: float) -> float:
    """Return the position fraction for this exchange × confidence tier."""
    tier = _confidence_tier(confidence)
    return TRADE_FRACTION_TIERS.get(exchange, TRADE_FRACTION_TIERS[""])[tier]


def equity_based_notional(
    equity: float,
    exchange: str,
    confidence: float,
    cluster_edge: float,
    regime_profile: str,
    drawdown_scale: float,
) -> float:
    """Equity-proportional notional. Used when EDGE_SCALE_ENABLED=true.
    Returns 0.0 on HARD_STOP (drawdown_scale==0), else clamps to [floor, ceiling].
    """
    if drawdown_scale == 0.0:
        return 0.0
    risk_pct = float(_os.getenv("RISK_PCT", "0.01"))
    edge_quality_mult = max(0.8, min(1.3, 0.8 + cluster_edge))
    regime_mult = {"bull": 1.0, "bear": 0.7, "neutral": 0.85}.get(regime_profile, 0.85)
    raw = equity * risk_pct * edge_quality_mult * regime_mult * drawdown_scale
    floor = 15.0
    ceiling = min(75.0, equity * 0.30)
    return min(max(raw, floor), ceiling)


_REGIME_FILTER_ENABLED = _os.getenv("REGIME_FILTER_ENABLED", "false").lower() in ("1", "true", "yes")

# Crypto assets that are exempt from tightening long-block (most liquid).
_TIGHTENING_LONG_EXEMPT = {"BTC", "ETH"}

# router_edge threshold that overrides volatility_regime block.
_VOLATILITY_REGIME_EDGE_MIN = 0.5


def regime_filter(
    exchange: str, asset: str, direction: str,
    router_edge: float, macro_regime: str,
) -> tuple[bool, float, str]:
    """Return (allowed, size_multiplier, reason).

    Adds regime-specific entry blocks and size penalties on top of the existing
    confidence-bar penalties in _min_confidence. Regime component lives ONLY here;
    AdaptiveSizer must NOT re-apply regime multipliers (double-count risk).
    """
    if not _REGIME_FILTER_ENABLED or not macro_regime or macro_regime == "unknown":
        return True, 1.0, ""

    r = macro_regime
    is_crypto = exchange == "CRYPTO"

    if r == "risk_off":
        if direction == "long" and is_crypto:
            return False, 0.8, "risk_off: CRYPTO longs blocked"
        return True, 0.8, "risk_off: size ×0.8"

    if r == "tightening":
        if direction == "long" and asset.upper() not in _TIGHTENING_LONG_EXEMPT:
            return False, 0.7, f"tightening: longs blocked (not in {_TIGHTENING_LONG_EXEMPT})"
        return True, 0.7, "tightening: size ×0.7"

    if r == "volatility_regime":
        if router_edge < _VOLATILITY_REGIME_EDGE_MIN:
            return False, 0.5, f"volatility_regime: router_edge {router_edge:.3f} < {_VOLATILITY_REGIME_EDGE_MIN}"
        return True, 0.5, f"volatility_regime: router_edge {router_edge:.3f} >= {_VOLATILITY_REGIME_EDGE_MIN}, size ×0.5"

    if r == "liquidity_expansion":
        mult = 1.2 if direction == "long" else 1.0
        return True, mult, f"liquidity_expansion: longs ×1.2" if mult > 1.0 else ""

    # risk_on or unknown: no restriction
    return True, 1.0, ""


def evaluate(
    signal: "Signal",
    portfolio: "Portfolio",
    macro_regime: str = "unknown",
    vix: float = 0.0,
    oos_override: bool = False,
) -> tuple[bool, str]:
    """Return (accepted, reason). reason is empty string when accepted."""
    exchange = signal.exchange or ""

    # Signals with no exchange value are ambiguous — log and skip rather than
    # silently routing them to the US equities rejection path (which previously
    # happened because "" was in the US equities guard set).
    if not exchange:
        return False, "unknown exchange (ticker missing from tickers table)"

    import os as _os

    # BIST: data-collection only — no trading until edge is measured.
    if exchange == "BIST":
        return False, "BIST trading disabled (data-collection only)"

    # US equities: removed (no Alpaca broker, no tax tracking).
    if str(exchange).upper() in ("NASDAQ", "NYSE", "CBOE"):
        return False, "US equities disabled"

    # Long kill-switch: env var DISABLE_LONGS=true blocks crypto long entries only.
    # LONG_ASSET_OVERRIDE=BTC,ETH bypasses the block for router-confirmed assets only.
    _long_override = {a.strip().upper() for a in _os.getenv("LONG_ASSET_OVERRIDE", "").split(",") if a.strip()}
    _router_confirmed_long = getattr(signal, "router_action", "") == "confirm"
    if (signal.direction == "long"
            and _os.getenv("DISABLE_LONGS", "").lower() == "true"
            and str(exchange).upper() == "CRYPTO"
            and not (signal.asset.upper() in _long_override and _router_confirmed_long)):
        return False, "longs disabled (crypto)"

    if (
        signal.direction == "short"
        and str(exchange).upper() == "CRYPTO"
        and macro_regime == "risk_on"
        and _os.getenv("DISABLE_CRYPTO_SHORTS_RISK_ON", "false").lower() == "true"
    ):
        return False, "crypto shorts disabled in risk_on (regime conflict)"

    # Asset ban: hard-coded floor (survives any .env mishap) + env-configurable list.
    # ARB/DOT: Sprint 30 catastrophic. ORCL: recurrent thrash (-$10.70, 14 trades, 0% WR across sim/paper).
    # XRP/SUI: regime_conflict passthrough losses poisoning calibration (XRP -$9.41, SUI -$5.62).
    _HARD_BANNED = {"ARB", "DOT", "ORCL", "XRP", "SUI"}
    _banned = _HARD_BANNED | {a.strip().upper() for a in _os.getenv("BANNED_ASSETS", "").split(",") if a.strip()}
    if signal.asset.upper() in _banned:
        return False, f"{signal.asset} banned (historical 0%/catastrophic WR)"

    # ETH shorts net -$0.28 live over 14d: max_hold/time_stop exits bleed in risk_on
    # (market trends up against short, never hits T1/T2 in 12h). Works fine in risk_off.
    _RISK_ON_SHORT_BAN = {"ETH"}
    if (signal.asset.upper() in _RISK_ON_SHORT_BAN
            and signal.direction == "short"
            and macro_regime == "risk_on"):
        return False, f"{signal.asset} short banned in risk_on regime (counter-trend bleed)"

    router_confirmed = getattr(signal, "router_action", "") == "confirm"
    router_inverted = getattr(signal, "router_action", "") == "invert"
    min_conf = _min_confidence(exchange, signal.direction, macro_regime, signal.confidence, vix,
                               router_confirmed=router_confirmed, router_inverted=router_inverted,
                               oos_override=oos_override)

    if signal.confidence < min_conf:
        tier = _confidence_tier(signal.confidence)
        regime_note = ""
        if signal.direction == "long" and macro_regime in ("risk_off", "tightening", "volatility_regime"):
            regime_note = f", {macro_regime} long-penalty"
        elif signal.direction == "short" and exchange == "CRYPTO" and macro_regime == "risk_on":
            regime_note = f", risk_on-crypto-short-penalty"
        elif signal.direction == "short" and (macro_regime in ("risk_off", "tightening", "volatility_regime") or vix >= 25.0):
            regime_note = f", short-activated"
        return False, (
            f"confidence {signal.confidence:.2f} < {min_conf:.2f} "
            f"({exchange or 'default'}/{tier} tier{regime_note})"
        )

    # Pump-chasing cap: very high confidence on CRYPTO long signals historically
    # has only 36% accuracy — these are hype-driven entries about to reverse.
    # Cap at 0.82 to avoid entering on pumped assets. Shorts are exempt (fear/panic
    # readings tend to be accurate even at high confidence).
    if exchange == "CRYPTO" and signal.direction == "long" and signal.confidence > 0.82:
        return False, (
            f"confidence {signal.confidence:.2f} > 0.82 cap (pump-chasing guard: "
            f"CRYPTO high-conf longs have 36% historical accuracy)"
        )

    # Require at least one component score to show conviction — blocks pure-noise signals.
    # Meta-engines (ICE etc.) bypass this via has_meta_conviction (confidence_weighted >= 0.5).
    # Router-confirmed/inverted signals bypass this gate: the measured 24h price-truth edge
    # is a stronger quality bar than component scores (which TDE invert signals lack entirely).
    # Set CONVICTION_GATE_ENABLED=false (sim) to trade without social/news signal in quiet markets.
    _conviction_gate_on = _os.getenv("CONVICTION_GATE_ENABLED", "true").lower() not in ("0", "false", "no")
    if _conviction_gate_on:
        _router_vetted = getattr(signal, "router_action", "") in ("confirm", "invert")
        has_conviction = (
            abs(signal.news_score) >= _MIN_NEWS_CONVICTION
            or abs(signal.social_score) >= _MIN_SOCIAL_CONVICTION
            or abs(signal.volume_score) >= _MIN_VOLUME_CONVICTION
            or getattr(signal, "has_meta_conviction", False)
            or _router_vetted
        )
        if not has_conviction:
            return False, (
                f"no conviction: news={signal.news_score:.3f}, "
                f"social={signal.social_score:.3f}, vol={signal.volume_score:.3f}"
            )

    if portfolio.balance < MIN_BALANCE:
        return False, f"balance {portfolio.balance:.2f} < minimum {MIN_BALANCE}"

    if signal.asset in portfolio.open_positions:
        return False, f"position already open for {signal.asset}"

    if len(portfolio.open_positions) >= MAX_OPEN_TRADES:
        return False, f"max open trades ({MAX_OPEN_TRADES}) reached"

    if exchange == "CRYPTO":
        crypto_count = sum(
            1 for t in portfolio.open_positions.values()
            if getattr(t, "exchange", "") == "CRYPTO"
        )
        if crypto_count >= MAX_CRYPTO_TRADES:
            return False, f"max crypto positions ({MAX_CRYPTO_TRADES}) reached"

    if exchange == "BIST":
        bist_count = sum(
            1 for t in portfolio.open_positions.values()
            if getattr(t, "exchange", "") == "BIST"
        )
        if bist_count >= MAX_BIST_TRADES:
            return False, f"max BIST positions ({MAX_BIST_TRADES}) reached"

    return True, ""
