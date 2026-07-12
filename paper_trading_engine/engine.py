"""
Paper Trading Engine — core orchestration loop.

Signal source: polls signals_registry (same DB-polling pattern as signal-archiver).
Watermark:     Redis key "paper_trading:watermark" (last processed signals_registry id).
Closing:       triggered by opposing direction signal OR after MAX_HOLD_HOURS.

Price model (Phase 2):
  Real market prices are fetched from ticker_quotes table (populated by CoinGecko + Finnhub).
  Falls back to composite_score proxy when no real price is available.

Direction mapping from signals_registry:
  risk_on  → long
  risk_off → short
"""
from __future__ import annotations

import asyncio
import time
from datetime import datetime, time as dtime, timezone
from typing import Optional

import asyncpg
import structlog

import os

import risk
import broker_factory
from broker_factory import trading_mode
from gpt_validator import validate_signal as _gpt_validate
from llm_auditor import LLMTradeAuditor
from models import PaperTrade, Signal
from truth_router import TruthRouter
from risk_governor import DrawdownGovernor, ClusterEdgeCalculator
import oos_validator
from notifier import TelegramNotifier
from portfolio import Portfolio
from risk_guard import RiskGuard
from capital_growth import CapitalGrowthFramework, GrowthDecision
from adaptive_sizer import AdaptiveSizer
import equity_tracker as _equity_tracker
import discord_status as _discord_status

logger = structlog.get_logger(__name__)

_DIRECTION_MAP = {
    "risk_on": "long", "inflow": "long", "liquidity_expansion": "long",
    "risk_off": "short", "outflow": "short", "tightening": "short", "volatility_regime": "short",
}
_OPPOSITE = {"long": "short", "short": "long"}

# Composite score thresholds for direction inference on neutral signals (-1 to +1 scale).
# Loaded from env so governance_engine.py writes take effect on next startup.
# Walk-forward tuner applies ±0.02 nudges at runtime from these baselines.
_SCORE_LONG_THRESHOLD  = float(os.getenv("SCORE_LONG_THRESHOLD",  "0.22"))
_SCORE_SHORT_THRESHOLD = float(os.getenv("SCORE_SHORT_THRESHOLD", "-0.15"))

# Governance hot-reload: Redis key prefix for live parameter overrides written by
# governance_engine.py.  Engine reads these every tuning cycle and updates baselines
# without requiring a container restart.
_GOV_OVERRIDE_PREFIX = "trading:gov_override:"
_GOV_RELOAD_TTL      = 86400  # governance override keys expire after 24h if not renewed

# Market session windows (UTC) — BIST only (crypto runs 24/7).
_BIST_SESSION_START = dtime(7, 0)    # 10:00 TRT
_BIST_SESSION_END   = dtime(15, 0)   # 18:00 TRT

# Macro regime cache TTL in seconds — 60s keeps latency low without a pub/sub system
_REGIME_CACHE_TTL = 60

# Altcoin long whitelist — only top-tier liquidity assets allowed as longs.
# ARB/OP/DOT/AVAX/DOGE/TON are chronic losers; BTC/ETH/SOL/BNB have real depth.
_CRYPTO_LONG_WHITELIST = frozenset({"BTC", "ETH", "SOL", "BNB"})
_CRYPTO_SHORT_WHITELIST = frozenset(
    t.strip().upper() for t in os.getenv(
        "CRYPTO_SHORT_WHITELIST", "BTC,ETH,AVAX,SOL,BNB,OP,DOGE"
    ).split(",") if t.strip()
)
_LIVE_CRYPTO_SHORT_WHITELIST = frozenset(
    t.strip().upper() for t in os.getenv("LIVE_CRYPTO_SHORT_WHITELIST", "").split(",") if t.strip()
)

# Engines excluded from signal fetch and consensus checks — produce no actionable signals.
# InstitutionalFlowEngine: always outputs 0.0 composite score (broken normalisation) and
# has no price-truth outcome history, so the Truth Router cannot vouch for it either.
# Sprint 38: ETFRotationEngine and InstitutionalConfluenceEngine REMOVED from this set —
# they were banned here in the circular-truth era, but honest price outcomes show ICE
# risk_off has the strongest measured edge in the system (84.8% WR @24h, n=158 deduped).
# The Truth Router now decides per (engine, bucket) on measured lift; hand bans only
# threw away alpha.
_EXCLUDED_ENGINES = ("InstitutionalFlowEngine",)

# Engines that Pillar 2 may never auto-exclude. TrendDetectionEngine produces ~57% of
# all signals and is the source of nearly every trade — excluding it silently halts the
# bot (zero entries, zero new calibration data). Entry gates filter its weak signals;
# governance can still penalise it via trading:engine_weights multipliers.
# Router governs all these engines via lift stats — dynamic exclusion uses signal_edge_metrics
# which disagrees with router lift for macro/reversion engines (different measurement).
_PROTECTED_ENGINES = frozenset({
    "TrendDetectionEngine",
    "InstitutionalConfluenceEngine",
    "MacroRegimeEngine",
})

# ── Self-optimizing system keys & TTLs ──────────────────────────────────────
_DYNAMIC_EXCLUSIONS_KEY = "trading:excluded_engines"   # Redis Set: dynamically excluded engines
_DYNAMIC_BANS_KEY       = "trading:dynamic_bans"        # Redis Hash: asset → ban reason
_REGIME_PROFILE_KEY     = "trading:regime_profile"      # Redis String: current regime profile name
_ENGINE_EXCLUSION_TTL   = 21600   # 6h — re-evaluate engine performance
_ASSET_LIFECYCLE_TTL    = 21600   # 6h — re-evaluate asset ban/unban
_REGIME_PROFILE_TTL     = 1800    # 30min — refresh bull/bear/neutral profile
_TRAIL_STOP_BASE        = 1.5     # % baseline matching TRAIL_STOP_PCT env default
_TRAIL_ACTIVATE_BASE    = 0.5     # % baseline matching TRAIL_ACTIVATE_PCT env default
_MAX_TRAIL_ACT_PCT      = float(os.getenv("MAX_TRAIL_ACT_PCT", "0.015"))  # cap trail activate at 1.5%

# ── Binance funding rate gate (Pillar: entry quality) ───────────────────────
# When BTC perpetual funding rate is below this threshold, shorts are
# already over-crowded (shorts paying longs). New short entries are blocked
# to avoid joining the wrong side of a potential squeeze.
_FUNDING_RATE_TTL       = 1800    # 30min refresh
_FUNDING_RATE_URL       = "https://fapi.binance.com/fapi/v1/premiumIndex"
_FUNDING_BLOCK_THRESHOLD = float(os.getenv("FUNDING_BLOCK_THRESHOLD", "-0.001"))

# Sprint 41 — live pilot: with a small real account, only the validated edge
# (router-inverted fades, +1.2%/trade OOS) executes real orders; every other
# accepted trade stays paper-tracked for measurement. Cap real notional so a
# $100 pilot can't put more than ~15% of the bankroll in one trade.
_LIVE_PILOT_FADES_ONLY = os.getenv("LIVE_PILOT_FADES_ONLY", "true").strip().lower() in ("1", "true", "yes")
_LIVE_CONFIRM_TICKERS = frozenset(
    t.strip().upper() for t in os.getenv("LIVE_CONFIRM_TICKERS", "").split(",") if t.strip()
)
_LIVE_MAX_NOTIONAL = float(os.getenv("LIVE_MAX_NOTIONAL", "15"))
# Floor keeps real orders above Binance pair minimums (~$5) plus headroom —
# fraction-of-balance sizing on a $100 account lands at $3-4 otherwise.
_LIVE_MIN_NOTIONAL = float(os.getenv("LIVE_MIN_NOTIONAL", "10"))
# Percentage-of-wallet sizing: effective min/max = max(dollar_floor, pct * free_wallet).
# Default 0.0 keeps current dollar-floor behaviour unless env vars are set.
_LIVE_MIN_NOTIONAL_PCT = float(os.getenv("LIVE_MIN_NOTIONAL_PCT", "0.0"))
_LIVE_MAX_NOTIONAL_PCT = float(os.getenv("LIVE_MAX_NOTIONAL_PCT", "0.0"))

# Regime profiles — parameters shift together based on BTC regime + VIX conditions.
# Walk-forward tuner applies ±0.02 nudges on top of these baselines.
_REGIME_PROFILES: dict[str, dict] = {
    "bull":    {"long_thr": 0.20, "short_thr": -0.12, "trail_act": 0.0125},
    "bear":    {"long_thr": 0.28, "short_thr": -0.12, "trail_act": 0.0125},
    "neutral": {"long_thr": 0.22, "short_thr": -0.15, "trail_act": 0.0125},
}

# ENGINE_REDIS_PREFIX isolates Redis keys when running a second sim container.
# Empty (default) = no prefix, preserving live-engine key names exactly.
_REDIS_PREFIX = os.getenv("ENGINE_REDIS_PREFIX", "").strip()
# Sim container writes mode='sim' so dashboard can separate it from historical paper trades.
_DB_PAPER_MODE = "sim" if _REDIS_PREFIX == "paper_sim" else "paper"


def _k(key: str) -> str:
    return f"{_REDIS_PREFIX}:{key}" if _REDIS_PREFIX else key


WATERMARK_KEY      = _k("paper_trading:watermark")
_DRIFT_ALERT_KEY   = _k("paper_trading:drift_alert_sent")  # cooldown: fire once per 24h
_PEAK_BALANCE_KEY  = _k("paper_trading:peak_balance")
_COOLDOWNS_KEY     = _k("paper_trading:cooldowns")       # Redis hash: asset → unix expiry timestamp
_STRIKES_KEY       = _k("paper_trading:stop_strikes")    # Redis hash: asset → strike count
_STOPS_KEY         = _k("paper_trading:position_stops")  # Redis hash: asset → ATR stop pct (float)
_PARTIAL_EXITS_KEY = _k("paper_trading:partial_exits")   # Redis hash: asset → JSON set of taken tranches
BATCH_SIZE = 100
POLL_INTERVAL = 30  # seconds — matches signal-archiver cadence
_QUALITY_CACHE_TTL = 300   # re-read signal_edge_aggregations every 5 min
_TUNING_CACHE_TTL  = 3600  # re-tune thresholds at most once per hour
_READINESS_INTERVAL = 86400  # push readiness summary once per day
_CALIB_CACHE_TTL   = 600   # re-read signal_scores calibration_error every 10 min
_LIVE_BALANCE_SYNC_TTL = 900  # resync Binance USDT balance every 15 min in live mode
_LIVE_RECONCILE_TTL = 3600    # hourly exchange-vs-records reconciliation in live mode


class PaperTradingEngine:
    def __init__(
        self,
        db: asyncpg.Pool,
        redis,
        portfolio: Portfolio,
        poll_interval: int = POLL_INTERVAL,
    ) -> None:
        self._db = db
        self._redis = redis
        self._portfolio = portfolio
        self._poll_interval = poll_interval
        self._watermark: int = 0
        self._live_wallet_usdt: float = 0.0  # real spendable USDT (live mode; sizes real orders)
        self._reconciled_at: float = 0.0
        self._running = False
        self._macro_regime: str = "unknown"
        self._regime_fetched_at: float = 0.0
        self._notifier = TelegramNotifier()
        self._risk_guard = RiskGuard(redis, db, notifier=self._notifier)
        self._dd_governor = DrawdownGovernor(redis, _k, notifier=self._notifier)
        self._cluster_calc = ClusterEdgeCalculator()
        self._live = trading_mode() == "live"
        self._stop_loss_pct = float(os.getenv("STOP_LOSS_PCT", "2.0")) / 100.0
        self._crypto_stop_loss_pct = float(os.getenv("CRYPTO_STOP_LOSS_PCT", "12.0")) / 100.0
        # Blind-flatten: close a LIVE position whose price feed is stale beyond this many seconds.
        # Past the suspend window we can't see flash moves; the broker close fills at the real
        # exchange price even when our local feed is dead. Must exceed 600s (the suspend) so a
        # transient hiccup never triggers it. 0 disables.
        self._stale_flatten_s = int(os.getenv("STALE_FLATTEN_S", "1200"))
        # Assets the user holds manually (e.g. BNB for the fee discount). The
        # hourly reconcile must NEVER auto-close a futures position in one of
        # these — it is the user's, not an engine orphan. ponytail: denylist, not
        # a whitelist; if more manual assets get wrongly closed, gate auto-close
        # on the engine's tradeable set instead.
        self._reconcile_ignore = self._parse_ignore_assets(
            os.getenv("RECONCILE_IGNORE_ASSETS", "BNB"))
        self._trail_stop_pct = float(os.getenv("TRAIL_STOP_PCT", "1.5")) / 100.0
        self._trail_activate_pct = float(os.getenv("TRAIL_ACTIVATE_PCT", "0.5")) / 100.0
        self._no_momentum_hours = float(os.getenv("NO_MOMENTUM_HOURS", "4"))
        self._no_momentum_min_mfe = float(os.getenv("NO_MOMENTUM_MIN_MFE", "-0.001"))  # fire only when short is red ≥0.1% (was +0.2%: dumped flat trades before T1 could hit)
        # Partial-exit triggers, env-tunable (so tuning needs no rebuild).
        # T2 widened 0.018→0.022 (2026-06-27) to let the middle tranche ride further.
        self._partial_t1_pct = float(os.getenv("PARTIAL_T1_PCT", "0.008"))
        self._partial_t2_pct = float(os.getenv("PARTIAL_T2_PCT", "0.022"))
        self._position_peaks: dict[str, float] = {}  # asset → best price seen while held
        # Assets where exchange stop placement failed — skip retrying until next restart
        self._stop_place_tried: set[str] = set()
        # Phase B: correlation guard — reset each poll cycle
        self._cycle_entries: dict[str, int] = {}
        self._vix_price: float = 0.0
        # Phase D: strategy quality cache from signal_edge_aggregations
        self._strategy_quality: dict[str, float] = {}
        self._quality_fetched_at: float = 0.0
        # Three-tranche exits: track which profit levels taken per position
        # Keys: "T1" (+0.5%), "T2" (+1.5%)
        self._partial_exits: dict[str, set] = {}
        # Per-position ATR-based stop: asset → computed stop_pct at entry time
        self._position_stops: dict[str, float] = {}
        # Price timestamp cache populated by _refresh_portfolio_prices — used to detect
        # stale quotes before acting on stop/trail/partial calculations.
        self._live_price_ts: dict[str, datetime] = {}
        # Tracks which assets have had a stale-price Discord alert fired this staleness
        # event; cleared when the price freshens so alerts don't spam on every cycle.
        self._stale_price_warned: set[str] = set()
        # Peak balance watermark for drawdown protection
        self._peak_balance: float = 0.0
        # Phase 7: walk-forward threshold self-tuning
        self._score_long_threshold  = _SCORE_LONG_THRESHOLD
        self._score_short_threshold = _SCORE_SHORT_THRESHOLD
        self._tuning_fetched_at: float = 0.0
        self._long_at_cap_cycles:  int   = 0
        self._short_at_cap_cycles: int   = 0
        self._drift_alerted_at:    float = 0.0
        # Sprint 3C: daily readiness push — track last push time
        self._readiness_pushed_at: float = 0.0
        self._equity_snapshot_at: float = 0.0
        self._discord_status_at: float = 0.0
        self._last_open_at: float = time.monotonic()
        self._silence_alerted: bool = False
        self._trail_activated: set[str] = set()  # assets where trail has activated; stop moved to entry
        self._regime_since: float = time.monotonic()  # monotonic timestamp of last regime change
        self._pool = db  # alias for GPT validator access
        # Calibration dampening: scale-down overconfident engines dynamically
        self._calibration_errors: dict[str, float] = {}
        self._calib_fetched_at: float = 0.0
        # Per-asset stop-loss strike counter — put assets on 24h cooldown after 2 stops
        self._asset_stop_strikes: dict[str, int] = {}
        self._asset_cooldown_until: dict[str, float] = {}  # asset → monotonic timestamp
        # Live balance sync: track last resync time
        self._balance_synced_at: float = 0.0
        # BTC regime hysteresis: persist bull/bear state to avoid flipping at SMA boundary.
        # Updated by _is_btc_bull_regime() — None means uninitialised (fail-closed = bear).
        self._btc_bull_cache: bool | None = None
        # ── Self-optimizing system state ──────────────────────────────────────
        # Pillar 2: dynamic engine exclusions (auto-excluded engines beyond hardcoded set)
        self._dynamic_excluded_engines: set[str] = set()
        self._engine_excl_fetched_at: float = 0.0
        # Pillar 3: dynamic asset bans
        self._asset_lifecycle_fetched_at: float = 0.0
        # Pillar 4: regime-adaptive baselines (walk-forward tuner applies ±0.02 on top)
        self._regime_profile: str = "neutral"
        self._regime_profile_fetched_at: float = 0.0
        # Minimum dwell between profile switches — VIX hovering at a boundary caused
        # 8 flips in 12h, churning baselines and spamming Telegram on every flip.
        self._regime_profile_changed_at: float = 0.0
        self._baseline_long_threshold  = _SCORE_LONG_THRESHOLD
        self._baseline_short_threshold = _SCORE_SHORT_THRESHOLD
        self._baseline_trail_activate  = self._trail_activate_pct
        # Anchors are frozen at env-load time and used as the blending origin for regime
        # profiles so that repeated regime flips cannot drift the baseline away from the
        # governance-configured values.
        self._anchor_long_threshold    = _SCORE_LONG_THRESHOLD
        self._anchor_short_threshold   = _SCORE_SHORT_THRESHOLD
        self._anchor_trail_activate    = self._trail_activate_pct
        # Pillar 1: extended tuner — trail stop and per-asset thresholds
        self._trail_stop_pct_base: float = self._trail_stop_pct  # env baseline for decay
        self._asset_thresholds: dict[str, float] = {}  # asset → threshold offset vs baseline
        # Funding rate gate — blocks crypto shorts when BTC perp funding is deeply negative
        self._funding_rates: dict[str, float] = {}   # symbol → last funding rate
        self._funding_rate_fetched_at: float = 0.0
        self._funding_poll_at: float = 0.0          # last funding payment poll
        # Governance engine weight overrides: engine_name → confidence multiplier
        self._engine_weights: dict[str, float] = {}
        self._weights_fetched_at: float = 0.0
        # Sprint 38: evidence router — measured per-(engine,bucket) edge decides
        # confirm/invert/reject before any other gate sees the signal.
        # Sprint 53: pass the notifier so the router can fire a one-time alert
        # when its OOS guardrail goes stale.
        self._truth_router = TruthRouter(notifier=self._notifier)
        # Sprint 53: passthrough policy — signals with no router stats fell
        # through to legacy gates, which lost money 7 days straight. Default
        # "block" drops them; "legacy" restores the pre-Sprint-53 behaviour.
        self._passthrough_policy = os.getenv("ROUTER_PASSTHROUGH_POLICY", "block").lower()
        self._oos_refresh_started = False
        # Per-cycle reject aggregation: (engine, direction) → count
        self._router_rejects: dict[tuple[str, str], int] = {}
        # Sprint 38: shadow-mode LLM trade auditor (logs verdicts, no authority)
        self._llm_auditor = LLMTradeAuditor(db)
        # Capital Growth Framework — equity-phased position limits
        self._cgf = CapitalGrowthFramework()
        self._growth: GrowthDecision = GrowthDecision(phase=1, max_positions=1, leverage_cap=2, size_multiplier=0.8)
        # Adaptive Position Sizer — per-asset + heat + streak multipliers
        self._adaptive_sizer = AdaptiveSizer(db, redis)

    # ── Async fire-and-track helper ───────────────────────────────────────────

    def _fire(self, coro) -> None:
        """Schedule `coro` as a tracked task. Logs errors; never silently swallows them."""
        task = asyncio.create_task(coro)

        def _on_done(t: asyncio.Task) -> None:
            exc = t.exception()
            if exc:
                logger.error("background_task_failed", error=str(exc), exc_info=exc)

        task.add_done_callback(_on_done)

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def run(self) -> None:
        self._watermark = await self._load_watermark()
        await self._reload_portfolio_from_db()
        await self._load_persistent_state()
        if self._live:
            await self._live_startup_check()
        await self._refresh_macro_regime()
        # Effective-config proof: log + persist the parameters actually in effect at
        # startup, and alert if they drifted from the previous run (catches silent
        # resets like the crypto-short max_hold flipping 6h→2h on restart). Strictly
        # observational — never gates or alters the trading loop below.
        await self._publish_effective_config()
        self._running = True
        logger.info("paper_trading_started", watermark=self._watermark, poll_interval=self._poll_interval)

        while self._running:
            cycle_start = time.monotonic()
            try:
                processed = await self._poll_cycle()
            except Exception as exc:
                logger.error("poll_cycle_error", error=str(exc))
                processed = 0
            finally:
                elapsed = round(time.monotonic() - cycle_start, 2)

            snap = self._portfolio.snapshot()
            if processed:
                logger.info("heartbeat", processed=processed, elapsed_s=elapsed, **snap)
            else:
                logger.info("idle_tick", status="alive", elapsed_s=elapsed, **snap)

            await asyncio.sleep(self._poll_interval)

    async def stop(self) -> None:
        self._running = False

    # ── Effective-config proof ────────────────────────────────────────────────

    def _effective_config(self) -> dict:
        """Collect every tunable trading parameter actually in effect at startup.

        Values are sourced from the *live instance attributes and module/imported
        constants the trading logic actually reads* — never re-read from .env. The
        whole point is to prove what landed at runtime, so that silent resets
        (e.g. crypto-short max_hold flipping 6h→2h on restart, or a trail % that a
        sprint "deployed" but never took effect) become visible in the logs.

        Read-only: builds and returns a dict; touches no engine state.
        """
        # Imported here to avoid reshaping module-level imports; these are the same
        # symbols the trading loop reads (portfolio.py / executor.py / risk.py).
        from portfolio import (
            INITIAL_BALANCE,
            MAX_HOLD_HOURS,
            MAX_HOLD_HOURS_STOCK,
            MAX_HOLD_HOURS_BIST,
        )
        import executor as _executor
        import risk as _risk

        cfg: dict = {
            # ── Trading mode ──────────────────────────────────────────────────
            "trading_mode": "live" if self._live else "paper",
            "poll_interval_s": self._poll_interval,

            # ── Direction-inference thresholds (live, post-tuning baselines) ──
            # _score_* are the values the loop reads now; baseline/anchor show the
            # frozen origins so regime/walk-forward drift is visible against them.
            "score_long_threshold": self._score_long_threshold,
            "score_short_threshold": self._score_short_threshold,
            "baseline_long_threshold": self._baseline_long_threshold,
            "baseline_short_threshold": self._baseline_short_threshold,
            "anchor_long_threshold": self._anchor_long_threshold,
            "anchor_short_threshold": self._anchor_short_threshold,

            # ── Stop / trail / partial-exit levels (fractions, as the loop uses) ─
            "stop_loss_pct": self._stop_loss_pct,
            "crypto_stop_loss_pct": self._crypto_stop_loss_pct,
            "trail_stop_pct": self._trail_stop_pct,
            "trail_activate_pct": self._trail_activate_pct,
            "trail_stop_pct_base": self._trail_stop_pct_base,
            "baseline_trail_activate": self._baseline_trail_activate,
            "anchor_trail_activate": self._anchor_trail_activate,
            "partial_exit_tranches": {"T1_pct": 0.5, "T2_pct": 1.5},  # _check_positions levels

            # ── Max-hold per exchange (executor._max_hold_hours sources) ──────
            "max_hold_hours_default": MAX_HOLD_HOURS,
            "max_hold_hours_stock": MAX_HOLD_HOURS_STOCK,
            "max_hold_hours_bist": MAX_HOLD_HOURS_BIST,
            "max_hold_crypto_long": _executor._HOLD_CRYPTO_LONG,
            "max_hold_crypto_short": _executor._HOLD_CRYPTO_SHORT,
            "max_hold_stock": _executor._HOLD_STOCK,
            "max_hold_bist": _executor._HOLD_BIST,

            # ── Open-trade caps (risk.py module constants) ────────────────────
            "max_open_trades": _risk.MAX_OPEN_TRADES,
            "max_crypto_trades": _risk.MAX_CRYPTO_TRADES,
            "max_bist_trades": _risk.MAX_BIST_TRADES,
            "min_balance": _risk.MIN_BALANCE,

            # ── Conviction / penalty gates (risk.py) ──────────────────────────
            "conf_high": _risk._CONF_HIGH,
            "conf_medium": _risk._CONF_MEDIUM,
            "min_news_conviction": _risk._MIN_NEWS_CONVICTION,
            "min_social_conviction": _risk._MIN_SOCIAL_CONVICTION,
            "min_volume_conviction": _risk._MIN_VOLUME_CONVICTION,
            "risk_off_long_penalty": _risk._RISK_OFF_LONG_PENALTY,

            # ── Sizing / fees / notional (executor.py) ────────────────────────
            "slippage_pct": _executor.SLIPPAGE_PCT,
            "fee_rate": _executor.FEE_RATE,
            "min_notional": _executor.MIN_NOTIONAL,
            "initial_balance": INITIAL_BALANCE,
            # Kelly sizing is computed/clamped in _get_kelly_fraction (half-Kelly,
            # 0.01–0.08); TARGET_VOL is the vol-targeting baseline read per entry.
            "target_vol": float(os.getenv("TARGET_VOL", "0.02")),
            "kelly_fraction_clamp": {"min": 0.01, "max": 0.08, "scale": 0.5},

            # ── Daily-loss / drawdown / heat limits ───────────────────────────
            "max_daily_loss": self._risk_guard._max_daily_loss,
            "drawdown_protection_trigger_pct": 0.05,   # _process_signal dd gate
            "drawdown_protection_scale": 0.5,
            "portfolio_heat_pause_pct": -0.015,        # _portfolio_heat_ok threshold

            # ── Regime params ─────────────────────────────────────────────────
            "regime_profile": self._regime_profile,
            "macro_regime": self._macro_regime,
            "regime_profiles": _REGIME_PROFILES,
            "crypto_noise_gate": float(os.getenv("CRYPTO_NOISE_GATE", "0.10")),
            "funding_block_threshold": _FUNDING_BLOCK_THRESHOLD,

            # ── Whitelists / bans / excluded engines ──────────────────────────
            "crypto_long_whitelist": sorted(_CRYPTO_LONG_WHITELIST),
            "crypto_short_whitelist": sorted(_CRYPTO_SHORT_WHITELIST),
            "excluded_engines": list(_EXCLUDED_ENGINES),
            "protected_engines": sorted(_PROTECTED_ENGINES),
            "banned_assets_env": sorted(
                a.strip().upper()
                for a in os.getenv("BANNED_ASSETS", "").split(",")
                if a.strip()
            ),

            # ── Gate toggles ──────────────────────────────────────────────────
            "disable_longs": os.getenv("DISABLE_LONGS", "").lower() == "true",
            "gpt_signal_validation": os.getenv("GPT_SIGNAL_VALIDATION", "false").lower() == "true",

            # ── Live-pilot params ─────────────────────────────────────────────
            "live_pilot_fades_only": _LIVE_PILOT_FADES_ONLY,
            "live_max_notional": _LIVE_MAX_NOTIONAL,
            "live_min_notional": _LIVE_MIN_NOTIONAL,
            "live_max_notional_pct": _LIVE_MAX_NOTIONAL_PCT,
            "live_min_notional_pct": _LIVE_MIN_NOTIONAL_PCT,

            # ── Evidence router window ────────────────────────────────────────
            "router_window_h": getattr(self._truth_router, "window_h", None),

            # ── Sprint 53: OOS guardrail + passthrough policy ─────────────────
            "oos_gate_enabled": getattr(self._truth_router, "oos_gate_enabled", None),
            "oos_min_n": getattr(self._truth_router, "oos_min_n", None),
            "oos_refresh_s": int(os.getenv("OOS_REFRESH_S", "21600")),
            "oos_max_staleness_s": getattr(self._truth_router, "oos_max_staleness_s", None),
            "oos_long_min_edge": getattr(self._truth_router, "oos_long_min_edge", None),
            "router_passthrough_policy": self._passthrough_policy,
        }
        return cfg

    def _canonical_config_json(self, cfg: dict) -> str:
        """Deterministic JSON (sorted keys) for hashing + diffing."""
        import json as _json
        return _json.dumps(cfg, sort_keys=True, default=str)

    async def _publish_portfolio_edge(self) -> None:
        """Publish router_edge-weighted portfolio score to Redis for sizing gate."""
        try:
            pos = list(self._portfolio.open_positions.values())
            if not pos:
                await self._redis.set("paper_trading:portfolio_edge_score", "0.0")
                return
            scores = [(t.notional or 0.0, t.router_edge or 0.0) for t in pos]
            total_n = sum(n for n, _ in scores) or 1.0
            pe = sum(n * e for n, e in scores) / total_n
            await self._redis.set("paper_trading:portfolio_edge_score", str(round(pe, 4)))
        except Exception:
            pass

    async def _publish_effective_config(self) -> None:
        """Log, hash, persist, and diff the effective config. Never raises.

        - Logs `effective_config` (full dict) + `config_hash` (sha256 of canonical JSON).
        - Persists dict + hash to Redis (engine:effective_config / _hash).
        - If a prior hash exists and differs, logs `effective_config_changed` with the
          per-key old→new diff and fires a (best-effort) Telegram alert.

        Strictly observational: any failure here is logged and swallowed so the
        trading loop always starts.
        """
        import hashlib
        import json as _json

        try:
            cfg = self._effective_config()
            canonical = self._canonical_config_json(cfg)
            cfg_hash = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
        except Exception as exc:
            logger.error("effective_config_build_failed", error=str(exc), exc_info=exc)
            return

        logger.info("effective_config", config_hash=cfg_hash, **cfg)

        # Load the previously-persisted snapshot to diff against.
        prev_hash = None
        prev_cfg: dict | None = None
        try:
            prev_hash = await self._redis.get(_k("engine:effective_config_hash"))
            prev_raw = await self._redis.get(_k("engine:effective_config"))
            if prev_raw:
                prev_cfg = _json.loads(prev_raw)
        except Exception as exc:
            logger.warning("effective_config_load_prev_failed", error=str(exc))

        if prev_hash and prev_hash != cfg_hash and prev_cfg is not None:
            # Diff against the JSON round-trip of the current dict, not the raw
            # dict — default=str stringifies non-JSON types in the persisted
            # snapshot, so comparing raw values would flag phantom changes.
            changes = self._diff_config(prev_cfg, _json.loads(canonical))
            if changes:
                logger.info(
                    "effective_config_changed",
                    prev_hash=prev_hash,
                    new_hash=cfg_hash,
                    changed_keys=sorted(changes.keys()),
                    changes=changes,
                )
                await self._notify_config_changed(changes)

        # Persist current snapshot (best-effort).
        try:
            await self._redis.set(_k("engine:effective_config"), canonical)
            await self._redis.set(_k("engine:effective_config_hash"), cfg_hash)
        except Exception as exc:
            logger.warning("effective_config_persist_failed", error=str(exc))

    @staticmethod
    def _diff_config(old: dict, new: dict) -> dict:
        """Return {key: {"old": ..., "new": ...}} for added/removed/changed keys."""
        changes: dict = {}
        for key in set(old) | set(new):
            ov = old.get(key, "<absent>")
            nv = new.get(key, "<absent>")
            if ov != nv:
                changes[key] = {"old": ov, "new": nv}
        return changes

    async def _notify_config_changed(self, changes: dict) -> None:
        """Best-effort Telegram alert that the effective config drifted across restarts.

        Failures are logged and swallowed — config drift detection must never block
        engine startup.
        """
        try:
            lines = ["⚙️ effective_config changed across restart:"]
            for key in sorted(changes)[:15]:
                old = str(changes[key]["old"])[:40]
                new = str(changes[key]["new"])[:40]
                lines.append(f"• {key}: {old} → {new}")
            if len(changes) > 15:
                lines.append(f"…and {len(changes) - 15} more")
            await self._notifier.send_message("\n".join(lines))
        except Exception as exc:
            logger.warning("effective_config_notify_failed", error=str(exc))

    # ── Live startup validation ───────────────────────────────────────────────

    async def _live_startup_check(self) -> None:
        """Validate live trading prerequisites on startup.

        1. Verify Binance API key is reachable and can trade.
        2. Fetch real USDT balance and sync portfolio.balance.
        3. Warn if balance is zero (needs funding before any trade fires).
        """
        # Fades OR confirm-ticker shorts use futures wallet; spot otherwise.
        if broker_factory.futures_enabled() and (not _LIVE_PILOT_FADES_ONLY or bool(_LIVE_CONFIRM_TICKERS)):
            from brokers.binance_futures import BinanceFuturesBroker
            broker = BinanceFuturesBroker()
        else:
            from brokers.binance import BinanceBroker
            broker = BinanceBroker()

        healthy = await broker.health_check()
        if not healthy:
            logger.error(
                "live_startup_binance_unreachable",
                hint="Check BINANCE_API_KEY / BINANCE_API_SECRET and network connectivity",
            )
            await self._notifier.send_message(
                "⚠️ LIVE MODE: Binance API unreachable on startup — check API keys"
            )
            return

        usdt_balance = await broker.get_balance()
        logger.info("live_startup_binance_ok", usdt_balance=round(usdt_balance, 2))

        if usdt_balance <= 0:
            logger.warning(
                "live_startup_zero_balance",
                hint="Deposit USDT to your Binance account before live trading starts",
            )
            await self._notifier.send_message(
                "⚠️ LIVE MODE: Binance USDT balance is $0 — deposit funds before trades fire"
            )
        else:
            # Seed the live wallet figure (sizes real orders); the paper
            # ledger (portfolio.balance) is deliberately left untouched.
            # F10: size off AVAILABLE (free) USDT, not total — open-position margin
            # isn't spendable. Total (usdt_balance) stays the daily-loss/wallet_start source.
            _avail = (await broker.get_available_balance()
                      if hasattr(broker, "get_available_balance") else usdt_balance)
            self._live_wallet_usdt = _avail if _avail > 0 else usdt_balance
            logger.info("live_balance_synced", usdt_balance=round(usdt_balance, 2),
                        available=round(_avail, 2))
            await self._notifier.send_message(
                f"✅ LIVE MODE active — Binance USDT balance: ${usdt_balance:.2f}"
            )
            # Store start-of-day wallet balance for RiskGuard's wallet-delta loss check.
            # Key expires in 26h so it resets each calendar day.
            from datetime import date as _date
            _day_key = f"trading:wallet_start:{_date.today().isoformat()}"
            if not await self._redis.exists(_day_key):
                await self._redis.set(_day_key, str(usdt_balance), ex=93600)  # 26h TTL

    async def _sync_live_balance(self) -> None:
        """Refresh the real spendable USDT figure used to size live orders.

        Sprint 42: the real wallet balance lives in _live_wallet_usdt and
        NEVER overwrites portfolio.balance. Stomping the paper ledger with a
        small real balance ($107 pilot) pushed paper sizing below executor
        MIN_NOTIONAL and silently halted ALL paper-tracked trading — starving
        the measurement loop the router learns from. The two ledgers serve
        different purposes: portfolio.balance = simulation; _live_wallet_usdt
        = what we may actually spend. Free USDT is valid to read even with
        positions open (funds in positions aren't 'free').
        """
        try:
            # Fades OR confirm-ticker shorts use futures wallet; spot otherwise.
            if broker_factory.futures_enabled() and (not _LIVE_PILOT_FADES_ONLY or bool(_LIVE_CONFIRM_TICKERS)):
                from brokers.binance_futures import BinanceFuturesBroker
                broker = BinanceFuturesBroker()
            else:
                from brokers.binance import BinanceBroker
                broker = BinanceBroker()
            usdt = await broker.get_balance()  # total wallet — daily-loss source
            # F10: size off AVAILABLE (free) USDT so open-position margin isn't double-spent.
            _avail = (await broker.get_available_balance()
                      if hasattr(broker, "get_available_balance") else usdt)
            _size_bal = _avail if _avail > 0 else usdt
            if _size_bal > 0 and abs(_size_bal - self._live_wallet_usdt) > 0.5:
                logger.info(
                    "live_wallet_resynced",
                    old=round(self._live_wallet_usdt, 2),
                    new=round(_size_bal, 2),
                    total=round(usdt, 2),
                )
            if _size_bal > 0:
                self._live_wallet_usdt = _size_bal
            if usdt > 0:
                # wallet_current = TOTAL (matches wallet_start) for RiskGuard's delta loss check.
                await self._redis.set("trading:wallet_current", str(usdt), ex=3600)
            self._balance_synced_at = time.monotonic()
        except Exception as exc:
            logger.warning("live_balance_sync_failed", error=str(exc))
            self._balance_synced_at = time.monotonic()  # don't retry immediately

    async def _reconcile_live_positions(self) -> None:
        """Compare exchange holdings against our live position records (hourly).

        Three drift classes:
          1. Recorded live LONG whose base asset is missing from the spot wallet.
          2. Meaningful spot holdings (>$2) with NO live position recorded.
          3. Binance Futures position with NO matching open live record (orphaned
             short — the most common failure mode when a close order fills 0).
        Alerts only; never auto-trades on drift (a wrong auto-fix is worse
        than a loud warning).
        """
        self._reconciled_at = time.monotonic()
        try:
            from brokers.binance import BinanceBroker, _SYMBOL_MAP
            broker = BinanceBroker()
            balances = await broker.get_all_balances()
            # Don't return early on empty spot balance — futures-only accounts always
            # have empty spot wallets, which previously caused the futures orphan check
            # to be silently skipped on every reconcile cycle.

            problems: list[str] = []
            live_assets = set()
            for asset, pos in self._portfolio.open_positions.items():
                if not getattr(pos, "is_live", False):
                    continue
                live_assets.add(asset)
                if pos.direction != "long":
                    continue  # spot check only for longs; futures checked separately below
                if not balances:
                    continue
                held = balances.get(asset, 0.0)
                if held < pos.size * 0.95:
                    problems.append(
                        f"{asset}: recorded {pos.size:.6f}, wallet holds {held:.6f}")

            orphan_candidates = [a for a, free in (balances or {}).items()
                                 if a in _SYMBOL_MAP and a not in live_assets
                                 and a not in self._reconcile_ignore and free > 0]
            if orphan_candidates:
                async with self._db.acquire() as conn:
                    rows = await conn.fetch(
                        """
                        SELECT DISTINCT ON (symbol) symbol, price FROM ticker_quotes
                         WHERE symbol = ANY($1::text[]) AND price > 0
                         ORDER BY symbol, ts DESC
                        """, orphan_candidates)
                prices = {r["symbol"]: float(r["price"]) for r in rows}
                for a in orphan_candidates:
                    value = balances[a] * prices.get(a, 0.0)
                    if value > 2.0:
                        problems.append(
                            f"{a}: untracked wallet holding worth ${value:.2f}")

            # Check Binance Futures for two drift classes:
            # (A) orphaned: exchange has position, we have no record
            # (B) ghost:    we have live record, exchange already closed it (e.g. exchange stop fired)
            try:
                from brokers.binance_futures import BinanceFuturesBroker
                from brokers.binance_futures import _SYMBOL_MAP as _FUT_MAP
                fut_broker = BinanceFuturesBroker()
                if fut_broker._configured:
                    fut_resp = await fut_broker._signed_get("/fapi/v2/positionRisk")
                    if isinstance(fut_resp, list) and len(fut_resp) > 0:
                        exchange_futures: dict[str, float] = {}
                        for p in fut_resp:
                            amt = float(p.get("positionAmt", 0.0))
                            sym = p.get("symbol", "")
                            asset_key = next((a for a, s in _FUT_MAP.items() if s == sym), None)
                            if asset_key:
                                exchange_futures[asset_key] = amt

                        for p in fut_resp:
                            amt = float(p.get("positionAmt", 0.0))
                            if amt == 0.0:
                                continue
                            sym = p.get("symbol", "")
                            asset = next((a for a, s in _FUT_MAP.items() if s == sym), None)
                            if asset is None:
                                continue
                            if asset not in live_assets:  # (A) orphaned
                                notional = abs(float(p.get("notional", 0.0)))
                                if notional < 0.10:  # dust remnant — skip alert spam
                                    continue
                                if asset in self._reconcile_ignore:
                                    # User-held position (e.g. BNB for fee discount).
                                    # Not an engine orphan — leave it alone, no alert spam.
                                    logger.info("live_reconcile_ignored_asset",
                                                asset=asset, notional=round(notional, 2))
                                    continue
                                direction = "short" if amt < 0 else "long"
                                _alert_str = (
                                    f"{asset}: orphaned {direction} futures position "
                                    f"qty={abs(amt):.4f} notional=${notional:.2f} — "
                                    f"close order likely filled 0, close manually on Binance"
                                )
                                if 0.10 <= notional <= 50.0:
                                    # Small orphan dust — attempt auto-close via full_exit.
                                    try:
                                        res = await fut_broker.close_position(
                                            asset, abs(amt), direction, full_exit=True)
                                        if res.success:
                                            logger.info("live_orphan_auto_closed",
                                                        asset=asset,
                                                        notional=round(notional, 2),
                                                        qty=abs(amt))
                                            problems.append(
                                                f"{asset}: orphaned {direction} ${notional:.2f} — AUTO-CLOSED")
                                        else:
                                            problems.append(_alert_str)
                                    except Exception as _oe:
                                        logger.warning("live_orphan_auto_close_failed",
                                                       asset=asset, error=str(_oe))
                                        problems.append(_alert_str)
                                else:
                                    # Large drift (>$50) — alert only, do not auto-trade.
                                    problems.append(_alert_str)

                        # (B) ghost: we have a live short record but exchange shows 0.
                        # Only runs when exchange_futures is non-empty (successfully fetched)
                        # so an API error doesn't falsely trigger ghost-close of all positions.
                        if exchange_futures:
                            for asset, pos in list(self._portfolio.open_positions.items()):
                                if not getattr(pos, "is_live", False):
                                    continue
                                # Asset must be in _FUT_MAP to be a futures position
                                if asset not in _FUT_MAP:
                                    continue
                                if exchange_futures.get(asset, 0.0) == 0.0:
                                    exit_px = self._portfolio._live_prices.get(asset)
                                    if exit_px is None:
                                        exit_px = await fut_broker.get_mark_price(asset)
                                    if exit_px is None:
                                        logger.warning("ghost_close_no_price",
                                                       asset=asset, fallback="entry_price")
                                        exit_px = pos.entry_price
                                    logger.warning(
                                        "live_ghost_position_detected",
                                        asset=asset,
                                        reason="exchange_closed_externally",
                                        exit_px=round(exit_px, 4),
                                    )
                                    closed = self._portfolio.close_position(asset, exit_px)
                                    if closed:
                                        self._position_peaks.pop(asset, None)
                                        self._position_stops.pop(asset, None)
                                        self._fire(self._redis.hdel(_STOPS_KEY, asset))
                                        self._partial_exits.pop(asset, None)
                                        self._fire(self._persist_partial_exits(asset, set()))
                                        await self._persist_close_trade(closed, "exchange_stop")
                                        await self._notifier.trade_closed(closed, "exchange_stop")
                                        await self._notifier.send_message(
                                            f"⚠️ {asset} closed by exchange stop — position synced."
                                        )
            except Exception as _fe:
                logger.warning("live_reconcile_futures_failed", error=str(_fe))

            if problems:
                logger.error("live_reconcile_drift", problems=problems)
                await self._notifier.send_message(
                    "🚨 RECONCILIATION DRIFT (exchange vs records):\n"
                    + "\n".join(f"• {p}" for p in problems)
                    + "\nNo auto-action taken — check manually."
                )
            else:
                logger.info("live_reconcile_ok",
                            live_positions=len(live_assets),
                            wallet_assets=len(balances))
        except Exception as exc:
            logger.warning("live_reconcile_failed", error=str(exc))

    # ── Portfolio reload ──────────────────────────────────────────────────────

    async def _reload_portfolio_from_db(self) -> None:
        """Deduplicate stale open positions and restore portfolio state from DB on startup."""
        from portfolio import INITIAL_BALANCE, MAX_HOLD_HOURS
        from executor import _max_hold_hours as _compute_hold

        # Sim container only manages its own mode='sim' rows.
        # Live/paper engine excludes sim rows so the two don't cross-manage positions.
        _mode_clause = "mode = $1" if _REDIS_PREFIX == "paper_sim" else "mode != 'sim'"
        _mode_param: list = [_DB_PAPER_MODE] if _REDIS_PREFIX == "paper_sim" else []

        async with self._db.acquire() as conn:
            # Close duplicates — keep only the highest id per asset (scoped to this engine's mode)
            result = await conn.execute(
                f"""
                UPDATE paper_trades
                   SET closed_at    = NOW(),
                       exit_price   = entry_price,
                       pnl          = 0.0,
                       close_reason = 'dedup_cleanup'
                 WHERE closed_at IS NULL
                   AND {_mode_clause}
                   AND id NOT IN (
                       SELECT MAX(id)
                         FROM paper_trades
                        WHERE closed_at IS NULL
                          AND {_mode_clause}
                        GROUP BY asset
                   )
                """,
                *_mode_param,
            )
            logger.info("startup_dedup", result=result)

            rows = await conn.fetch(
                f"""
                SELECT id, signal_id, asset, exchange, direction,
                       entry_price, size, notional, fee, opened_at, mode,
                       stop_order_id, exchange_order_id
                  FROM paper_trades
                 WHERE closed_at IS NULL AND {_mode_clause}
                 ORDER BY opened_at ASC
                """,
                *_mode_param,
            )

            bal_row = await conn.fetchrow(
                f"""
                SELECT
                    COALESCE(SUM(CASE WHEN closed_at IS NOT NULL
                                      AND close_reason != 'dedup_cleanup'
                                     THEN pnl ELSE 0 END), 0) AS closed_pnl,
                    COALESCE(SUM(CASE WHEN closed_at IS NOT NULL
                                      AND close_reason != 'dedup_cleanup'
                                     THEN notional * 0.001 ELSE 0 END), 0) AS closed_entry_fees,
                    COALESCE(SUM(CASE WHEN closed_at IS NULL THEN notional+fee ELSE 0 END), 0) AS open_committed
                  FROM paper_trades
                 WHERE {_mode_clause}
                """,
                *_mode_param,
            )

        for row in rows:
            opened = row["opened_at"]
            if opened is not None and opened.tzinfo is None:
                opened = opened.replace(tzinfo=timezone.utc)
            trade = PaperTrade(
                trade_id=row["id"],
                signal_id=row["signal_id"] or "",
                asset=row["asset"],
                exchange=row["exchange"] or "",
                direction=row["direction"],
                size=float(row["size"]),
                entry_price=float(row["entry_price"]),
                notional=float(row["notional"]),
                fee=float(row["fee"]),
                max_hold_hours=_compute_hold(0.0, row["exchange"] or "", row["direction"] or ""),
                opened_at=opened or datetime.now(timezone.utc),
                is_live=(row["mode"] == "live"),
            )
            trade.stop_order_id    = row["stop_order_id"]
            trade.exchange_order_id = row["exchange_order_id"]
            if trade.stop_order_id:
                logger.info("reload_stop_order_id_restored",
                            asset=trade.asset, stop_order_id=trade.stop_order_id)
            self._portfolio.open_positions[trade.asset] = trade

        closed_pnl       = float(bal_row["closed_pnl"])
        closed_entry_fees = float(bal_row["closed_entry_fees"])
        open_committed   = float(bal_row["open_committed"])
        self._portfolio.balance = max(
            0.0,
            INITIAL_BALANCE + closed_pnl - closed_entry_fees - open_committed,
        )

        # Restore trailing-stop peaks from actual price history since entry.
        # Without this, any restart resets peaks to entry_price and trail stops
        # lose all accumulated gain — positions that moved 3% in favour revert
        # to needing 3% + trail_stop_pct before trail fires again.
        for trade in self._portfolio.open_positions.values():
            try:
                async with self._db.acquire() as _conn:
                    if trade.direction == "long":
                        row = await _conn.fetchrow(
                            "SELECT MAX(price) AS peak FROM ticker_quotes "
                            "WHERE symbol=$1 AND ts>=($2 AT TIME ZONE 'UTC') AND price>0",
                            trade.asset, trade.opened_at,
                        )
                    else:
                        row = await _conn.fetchrow(
                            "SELECT MIN(price) AS peak FROM ticker_quotes "
                            "WHERE symbol=$1 AND ts>=($2 AT TIME ZONE 'UTC') AND price>0",
                            trade.asset, trade.opened_at,
                        )
                    best = float(row["peak"]) if row and row["peak"] else trade.entry_price
                    self._position_peaks[trade.asset] = best
            except Exception as _e:
                self._position_peaks[trade.asset] = trade.entry_price
                logger.warning("peak_restore_failed", asset=trade.asset, error=str(_e))

        logger.info(
            "portfolio_reloaded",
            open_positions=len(self._portfolio.open_positions),
            balance=round(self._portfolio.balance, 4),
            closed_pnl=round(closed_pnl, 4),
            open_committed=round(open_committed, 4),
        )

    # ── Poll cycle ────────────────────────────────────────────────────────────

    async def _refresh_macro_regime(self) -> None:
        """TTL-gated wrapper so _fetch_macro_regime fits the asyncio.gather pattern."""
        if time.monotonic() - self._regime_fetched_at > _REGIME_CACHE_TTL:
            self._macro_regime = await self._fetch_macro_regime()
            self._regime_fetched_at = time.monotonic()

    async def _poll_cycle(self) -> int:
        # All refreshes are TTL-gated internally and touch independent DB tables —
        # run them concurrently to cut cycle overhead.
        await asyncio.gather(
            self._refresh_macro_regime(),
            self._refresh_strategy_quality(),
            self._refresh_thresholds(),
            self._refresh_calibration_errors(),
            self._refresh_portfolio_prices(),
            self._refresh_engine_exclusions(),
            self._refresh_regime_profile(),
            self._refresh_asset_bans(),
            self._refresh_funding_rates(),
            self._refresh_engine_weights(),
            self._truth_router.maybe_refresh(self._db, self._redis),
            self._poll_funding_payments(),
        )

        # Sprint 53: launch the OOS guardrail refresher once (delayed start so
        # it never slows startup). It writes router:oos_stats every OOS_REFRESH_S.
        if not self._oos_refresh_started:
            self._oos_refresh_started = True
            refresh_s = int(os.getenv("OOS_REFRESH_S", "21600"))
            min_n = int(os.getenv("OOS_MIN_N", "30"))
            self._fire(oos_validator.run_refresh_loop(
                self._db, self._redis, refresh_s, min_n))

        # Periodically sync real Binance USDT balance so deposits/withdrawals
        # are reflected without requiring a container restart.
        if self._live and time.monotonic() - self._balance_synced_at > _LIVE_BALANCE_SYNC_TTL:
            await self._sync_live_balance()

        # Hourly reconciliation: exchange truth vs our records. Catches the
        # drift class no code audit can — manual sells, dust, failed fills
        # the API reported oddly, anything that desyncs reality from records.
        if self._live and time.monotonic() - self._reconciled_at > _LIVE_RECONCILE_TTL:
            await self._reconcile_live_positions()

        # Reset per-exchange entry counter for correlation guard
        self._cycle_entries = {}

        signals = await self._fetch_signals()
        if not signals:
            await self._check_positions()
            return 0

        # Dedup: at most one signal per asset per cycle.
        # Opposing directions → keep higher abs(router_edge); same direction → keep first.
        _dedup: dict[str, Signal] = {}
        for sig in signals:
            if sig.asset not in _dedup:
                _dedup[sig.asset] = sig
            else:
                existing = _dedup[sig.asset]
                if existing.direction != sig.direction:
                    # Opposing signals — keep the one with larger absolute edge
                    if abs(sig.router_edge) > abs(existing.router_edge):
                        keep, drop = sig, existing
                    else:
                        keep, drop = existing, sig
                    _dedup[sig.asset] = keep
                    logger.info(
                        "opposing_signal_suppressed",
                        asset=sig.asset,
                        kept_dir=keep.direction,
                        kept_edge=round(keep.router_edge, 3),
                        dropped_dir=drop.direction,
                        dropped_edge=round(drop.router_edge, 3),
                    )
                # Same direction: keep first (already in _dedup), ignore rest
        deduped_signals = list(_dedup.values())

        for signal in deduped_signals:
            try:
                await self._process_signal(signal)
            except Exception as exc:
                logger.error(
                    "process_signal_error",
                    signal_id=signal.signal_id,
                    asset=signal.asset,
                    error=str(exc),
                )

        if self._router_rejects:
            logger.info(
                "router_reject_summary",
                rejects={f"{k[0]}/{k[1]}": v for k, v in self._router_rejects.items()},
            )
            self._router_rejects = {}
        self._truth_router.log_oos_cycle_summary()

        await self._check_positions()

        new_wm = max(int(s.signal_id) for s in signals)
        if new_wm > self._watermark:
            self._watermark = new_wm
            await self._save_watermark()

        # Daily readiness push
        if time.monotonic() - self._readiness_pushed_at > _READINESS_INTERVAL:
            await self._push_readiness_summary()
            self._readiness_pushed_at = time.monotonic()

        # 30-min equity snapshot
        if time.monotonic() - self._equity_snapshot_at > 1800:
            try:
                gov_state = getattr(self, "_gov_decision", None)
                gov_state_str = gov_state.state if gov_state else "NORMAL"
                await _equity_tracker.write_snapshot(
                    self._db,
                    balance=self._portfolio.balance,
                    equity=self._portfolio.equity,
                    peak_balance=self._peak_balance,
                    governor_state=gov_state_str,
                    regime=self._macro_regime,
                    cluster_edges={},
                    mode="live" if self._live else "paper",
                )
            except Exception as _es_exc:
                logger.warning("equity_snapshot_failed: %s", _es_exc)
            self._equity_snapshot_at = time.monotonic()

        # Capital Growth Framework: evaluate phase every tick (pure/cheap), tick() for snapshots
        self._growth = self._cgf.evaluate(self._portfolio.equity)
        self._fire(self._cgf.tick(self._db, self._portfolio.equity, self._notifier, self._growth))

        # 4h Discord status push
        if time.monotonic() - self._discord_status_at > 14400:
            self._fire(_discord_status.push_status(self._db))
            self._discord_status_at = time.monotonic()

        # 12h trade-silence alert
        _silence_h = (time.monotonic() - self._last_open_at) / 3600
        if _silence_h >= 12.0 and not self._silence_alerted:
            self._fire(self._notifier.send_message(
                f"⚠️ No trade opened in {_silence_h:.0f}h "
                f"(regime={self._macro_regime}, mode={'live' if self._live else 'paper'})."))
            self._silence_alerted = True

        return len(signals)

    # ── Signal fetch ──────────────────────────────────────────────────────────

    async def _fetch_signals(self) -> list[Signal]:
        # Sprint 38: dynamic (Pillar 2) exclusions are overridden for engines the
        # router has measured stats on — the router governs those per-bucket, and
        # excluding a reliably-wrong engine would throw away its contrarian value.
        # Engines without router stats remain subject to dynamic exclusion.
        dynamic_excluded = [
            e for e in self._dynamic_excluded_engines
            if not self._truth_router.has_stats(e)
        ]
        overridden = self._dynamic_excluded_engines - set(dynamic_excluded)
        if overridden:
            logger.debug("router_overrides_exclusion", engines=sorted(overridden))
        async with self._db.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT s.id, s.ticker, s.direction, s.composite_score,
                       s.confidence_level, s.engine_name, s.emitted_at,
                       s.signal_data,
                       t.exchange,
                       q.price AS real_price,
                       q.price_ts
                  FROM signals_registry s
                  LEFT JOIN tickers t ON t.symbol = s.ticker
                  LEFT JOIN LATERAL (
                      SELECT price, ts AS price_ts FROM ticker_quotes
                       WHERE symbol = s.ticker
                       ORDER BY ts DESC
                       LIMIT 1
                  ) q ON true
                 WHERE s.id > $1
                   AND s.ticker IS NOT NULL
                   AND s.composite_score IS NOT NULL
                   AND ABS(s.composite_score) > 0.01
                   AND s.emitted_at > NOW() - INTERVAL '25 minutes'
                   AND s.engine_name != ALL($3)
                 ORDER BY s.id ASC
                 LIMIT $2
                """,
                self._watermark,
                BATCH_SIZE,
                list(_EXCLUDED_ENGINES) + dynamic_excluded,
            )
        signals = []
        for row in rows:
            sig = self._row_to_signal(row)
            if sig is not None:
                signals.append(sig)
        return signals

    def _row_to_signal(self, row: asyncpg.Record) -> Signal | None:
        import json as _json
        raw_score = float(row["composite_score"] or 0.0)
        raw_dir = row["direction"] or "neutral"

        # CRYPTO noise gate: lowered from 0.12 → 0.10 (Sprint 33) to admit NarrativeEngine
        # signals whose avg composite sits at 0.105. Original 0.20 threshold was too tight;
        # the entry gates (confidence, conviction, consensus, trend) handle further filtering.
        if str(row["exchange"] or "").upper() == "CRYPTO" and abs(raw_score) < float(os.getenv("CRYPTO_NOISE_GATE", "0.10")):
            return None

        if raw_dir in _DIRECTION_MAP:
            direction = _DIRECTION_MAP[raw_dir]
        elif raw_dir in ("neutral", "mixed"):
            # Per-asset threshold offset: relax for proven winners, tighten for losers (Pillar 1)
            asset_offset = self._asset_thresholds.get(str(row["ticker"] or ""), 0.0)
            long_thr  = self._score_long_threshold  + asset_offset
            short_thr = self._score_short_threshold - asset_offset  # tighter = more negative
            if raw_score >= long_thr:
                direction = "long"
            elif raw_score <= short_thr:
                direction = "short"
            else:
                return None  # truly neutral — not actionable
        else:
            return None

        # Sprint 38 — Truth Router: measured edge decides confirm/invert/reject
        # before any other gate. Engines that are reliably wrong at the trade
        # horizon get FADED (direction flipped); engines with no measured edge
        # in this bucket get rejected; unknown buckets pass through unchanged.
        verdict = self._truth_router.route(
            row["engine_name"] or "unknown",
            str(row["exchange"] or ""),
            raw_dir,
            raw_score,
            direction,
            ticker=str(row["ticker"] or ""),
        )
        if verdict.action == "reject":
            # Aggregate rejects per poll cycle — identical batches every 15min
            # would otherwise emit ~15 info lines each (see _poll_cycle summary).
            key = (str(row["engine_name"] or "unknown"), direction)
            self._router_rejects[key] = self._router_rejects.get(key, 0) + 1
            logger.debug(
                "router_rejected", asset=row["ticker"], engine=row["engine_name"],
                direction=direction, edge=round(verdict.edge, 3), n=verdict.n,
            )
            return None
        router_action, router_edge = verdict.action, verdict.edge
        if verdict.action == "invert":
            logger.info(
                "router_inverted", asset=row["ticker"], engine=row["engine_name"],
                nominal=direction, traded=verdict.direction,
                edge=round(verdict.edge, 3), n=verdict.n,
            )
            direction = verdict.direction

        # Use confidence_score from signal_data JSON (ConfidenceEngine output, 0.0–1.0).
        # Falls back to abs(raw_score) for signal types that don't carry confidence_score.
        try:
            sig_data = _json.loads(row["signal_data"] or "{}")
            # confidence_score: TrendDetectionEngine; confidence_weighted: ICE/flow engines.
            # Use explicit None checks — `or` would treat a genuine 0.0 confidence as falsy
            # and silently promote it to abs(raw_score), masking zero-confidence signals.
            _cs = sig_data.get("confidence_score")
            _cw = sig_data.get("confidence_weighted")
            if _cs is not None:
                confidence = float(_cs)
            elif _cw is not None:
                confidence = float(_cw)
            else:
                confidence = abs(raw_score)
            news_score    = float(sig_data.get("news_score")       or 0.0)
            social_score  = float(sig_data.get("social_score")     or 0.0)
            volume_score  = float(sig_data.get("volume_score")     or 0.0)
            # technical_score is already computed by TrendDetectionEngine — reuse it
            # directly to avoid an extra DB round-trip in _process_signal.
            tech_score_sd = float(sig_data.get("technical_score")  or 0.0)
            # Meta-engines (ICE etc.) express conviction via confidence_weighted >= 0.5
            has_meta_conviction = float(sig_data.get("confidence_weighted") or 0.0) >= 0.5
        except Exception:
            confidence = abs(raw_score)
            news_score = social_score = volume_score = tech_score_sd = 0.0
            has_meta_conviction = False

        # Price staleness check: if the latest price quote is older than 10 minutes
        # AND the market is open, don't trade — stale prices cause incorrect stop-loss
        # and entry calculations.  After-hours quotes are expected to be old.
        real_price = float(row["real_price"]) if row["real_price"] else 0.0
        price_ts = row["price_ts"] if "price_ts" in row.keys() else None
        if real_price > 0 and price_ts is not None:
            from datetime import datetime as _dt
            now_utc = _dt.now(timezone.utc)
            if price_ts.tzinfo is None:
                price_ts = price_ts.replace(tzinfo=timezone.utc)
            age_min = (now_utc - price_ts).total_seconds() / 60
            _sig_exchange = str(row.get("exchange") or "CRYPTO").upper()
            if self._quote_stale(age_min * 60, _sig_exchange):
                # A dropped FADE is a dropped occurrence of the only validated
                # edge (+1.2%/trade, rare) — losing one silently is how the
                # MATIC dead-feed went unnoticed for 5 days. Warn + Telegram.
                if router_action == "invert":
                    logger.warning("fade_dropped_stale_price", asset=row["ticker"],
                                   age_min=round(age_min, 1), edge=round(router_edge, 3))
                    self._fire(self._notifier.send_message(
                        f"⚠️ FADE DROPPED: {row['ticker']} inverted signal skipped — "
                        f"price quote is {age_min:.0f} min stale (edge {router_edge:+.3f}). "
                        f"Check the ticker's feed."
                    ))
                else:
                    logger.debug("stale_price_skipped", asset=row["ticker"],
                                 age_min=round(age_min, 1))
                real_price = 0.0  # executor will skip if price=0

        # Router confidence scaling: confirmed/inverted signals with a wide
        # measured edge get up to +30% confidence; passthrough is unchanged.
        if verdict.conf_scale != 1.0:
            confidence = min(1.0, confidence * verdict.conf_scale)

        return Signal(
            signal_id=str(row["id"]),
            asset=row["ticker"],
            direction=direction,
            confidence=confidence,
            timestamp=row["emitted_at"].isoformat() if row["emitted_at"] else "",
            source=row["engine_name"] or "unknown",
            composite_score=raw_score,
            exchange=row["exchange"] or "CRYPTO",  # blank→CRYPTO: consistent with stale-price coercion at line 1168
            real_price=real_price,
            news_score=news_score,
            social_score=social_score,
            volume_score=volume_score,
            tech_score=tech_score_sd,
            has_meta_conviction=has_meta_conviction,
            router_action=router_action,
            router_edge=router_edge,
        )

    # ── Market hours ─────────────────────────────────────────────────────────

    @staticmethod
    def _quote_stale(age_s: float, exchange: str) -> bool:
        """Return True only when the price quote is stale AND the market is open.

        CRYPTO trades 24/7 so any quote >600s is a real feed problem.
        Non-CRYPTO: stale only matters when the market is open.
        """
        if age_s <= 600:
            return False
        if exchange == "CRYPTO":
            return True
        # For exchange-traded assets: stale only matters when the market is open.
        return PaperTradingEngine._is_market_open(exchange)

    @staticmethod
    def _blind_flatten_due(is_live: bool, price_stale: bool, age_s: float, flatten_s: int) -> bool:
        """True when a LIVE position's feed has been stale longer than flatten_s — too blind to
        hold safely. flatten_s must exceed the 600s suspend window (hysteresis); 0 disables."""
        return bool(is_live and price_stale and flatten_s > 0 and age_s > flatten_s)

    @staticmethod
    def _parse_ignore_assets(env_val: str) -> set[str]:
        """Comma-separated asset list → upper-cased set. These assets are held by
        the user (e.g. BNB for the fee discount) and must never be auto-closed by
        the hourly reconcile orphan sweep."""
        return {a.strip().upper() for a in (env_val or "").split(",") if a.strip()}

    @staticmethod
    def _is_market_open(exchange: str) -> bool:
        """Return True if the exchange is in its regular cash session (UTC)."""
        now = datetime.now(timezone.utc)
        if now.weekday() >= 5:
            return exchange == "CRYPTO"
        t = now.time().replace(tzinfo=None)
        if exchange == "BIST":
            return _BIST_SESSION_START <= t <= _BIST_SESSION_END
        return True

    @staticmethod
    def _can_enter(exchange: str) -> bool:
        """Return True if we're allowed to OPEN a new position on this exchange.

        CRYPTO dead zone: 04:00–07:00 UTC is the lowest-liquidity window globally.
        """
        now = datetime.now(timezone.utc)
        if now.weekday() >= 5:
            if exchange != "CRYPTO":
                return False
            t = now.time().replace(tzinfo=None)
            return not (dtime(4, 0) <= t < dtime(7, 0))

        t = now.time().replace(tzinfo=None)
        if exchange == "BIST":
            return _BIST_SESSION_START <= t <= _BIST_SESSION_END
        if exchange == "CRYPTO":
            return not (dtime(4, 0) <= t < dtime(7, 0))
        return True

    # ── Liquidity cap ─────────────────────────────────────────────────────────

    async def _volume_cap_usd(self, asset: str) -> float:
        """Return 1% of estimated 24h dollar volume, or 0 (uncapped) on data absence."""
        try:
            async with self._db.acquire() as conn:
                rows = await conn.fetch(
                    """
                    SELECT price, volume FROM ticker_quotes
                     WHERE symbol = $1 AND volume > 0
                     ORDER BY ts DESC LIMIT 48
                    """,
                    asset,
                )
            if len(rows) < 2:
                return 0.0
            avg_price = sum(float(r["price"]) for r in rows) / len(rows)
            avg_vol   = sum(float(r["volume"]) for r in rows) / len(rows)
            daily_dollar_vol = avg_price * avg_vol * 24  # rows are ~hourly
            return max(50.0, daily_dollar_vol * 0.01)
        except Exception as exc:
            logger.warning("volume_cap_fetch_failed", asset=asset, error=str(exc))
            return 0.0

    # ── Signal processing ─────────────────────────────────────────────────────

    async def _process_signal(self, signal: Signal) -> None:
        asset = signal.asset
        existing = self._portfolio.open_positions.get(asset)
        exchange_key = signal.exchange or ""

        # Non-CRYPTO dropped entirely — US stocks removed, BIST disabled.
        # Skip paper recording so calibration tables stay clean.
        if (signal.exchange or "").upper() not in ("CRYPTO", ""):
            return

        # Block new entries when outside regular session OR past entry cutoff.
        # Existing positions (opposite-direction close) still processed regardless.
        is_fade_signal = signal.router_action == "invert"
        # Router-confirmed signals bypass consensus/trend/tech/conviction gates when:
        # (a) non-CRYPTO: single-engine US tickers (ETF, ICE) can't achieve 60%
        #     multi-engine consensus, but 24h lift proves edge.
        # (b) CRYPTO in risk_off/tightening/volatility_regime: OOS-proven TDE shorts
        #     are counter-trend gates mismatch the walk-forward measurement which didn't
        #     apply them. Confidence floor stays at normal CRYPTO level (not the 0.05
        #     floor for US stocks) — CRYPTO volatility warrants stricter sizing bar.
        #     In risk_on, CRYPTO confirms keep full gate chain (counter-trend protection).
        # OOS ticker overrides are explicit user approvals of measured edge — bypass
        # the full downstream gate chain (consensus/trend/tech) for both confirm and
        # invert. Stored as (TICKER_UPPER, action_lower); `asset` is already uppercased.
        _oos_overrides = getattr(self._truth_router, "oos_ticker_overrides", frozenset())
        _is_oos_override = (
            signal.router_action in ("confirm", "invert")
            and (asset, signal.router_action) in _oos_overrides
        )
        _router_confirm_bypass = (
            _is_oos_override
            or signal.router_action == "invert"
            or (
                signal.router_action == "confirm" and (
                    (signal.exchange or "").upper() not in ("CRYPTO",)
                    or self._macro_regime in ("risk_off", "tightening", "volatility_regime")
                )
            )
        )

        # Sprint 53: passthrough policy. Signals with no router stats
        # (router_action == "passthrough", or absent) used to fall through to
        # the legacy gates — that path lost money 7 days straight. Default
        # "block" drops them here (counted in the router reject summary);
        # "legacy" restores the old fall-through behaviour.
        #
        # Sprint 62: in live mode, passthrough signals still paper-simulate
        # (for data collection / win-rate tracking). The live execution path
        # downstream gates on LIVE_CONFIRM_TICKERS, so unproven signals never
        # place real orders — they just write a paper_trades row with mode='paper'.
        if self._passthrough_policy != "legacy":
            action = signal.router_action or "passthrough"
            if action == "passthrough":
                if (signal.exchange or "").upper() == "CRYPTO":
                    logger.info(
                        "trade_rejected",
                        signal_id=signal.signal_id,
                        asset=asset,
                        exchange=signal.exchange,
                        direction=signal.direction,
                        reason="unrouted_crypto_blocked",
                    )
                    return
                if self._live:
                    # Live mode: paper-simulate unproven signals so the paper ledger
                    # keeps collecting win-rate data. Real orders are still gated by
                    # LIVE_CONFIRM_TICKERS further below — nothing fires on Binance.
                    logger.debug(
                        "oos_paper_sim",
                        signal_id=signal.signal_id,
                        asset=asset,
                        engine=signal.source,
                        reason="passthrough_allowed_for_paper_data_collection",
                    )
                else:
                    key = (signal.source or "unknown", signal.direction)
                    self._router_rejects[key] = self._router_rejects.get(key, 0) + 1
                    logger.info(
                        "trade_rejected",
                        signal_id=signal.signal_id,
                        asset=asset,
                        exchange=signal.exchange,
                        direction=signal.direction,
                        reason="router_passthrough_blocked",
                    )
                    return

        # Crypto fades bypass the 04-07 UTC dead-zone: the zone's "near-zero
        # win rate" was measured on momentum trades, while the fade edge
        # (+1.2%/trade OOS) was validated across ALL hours — and panic dumps
        # cluster in exactly these thin windows. Blocking them would make live
        # behaviour diverge from the validated backtest.
        _crypto_confirm_skip_dz = (
            os.getenv("CRYPTO_CONFIRM_SKIPS_DEADZONE", "false").lower() == "true"
            and signal.exchange == "CRYPTO"
            and getattr(signal, "router_action", "") == "confirm"
        )
        fade_skips_session_gate = (is_fade_signal and signal.exchange == "CRYPTO") or _crypto_confirm_skip_dz
        if not existing and not fade_skips_session_gate and not self._can_enter(signal.exchange):
            # Fades are too rare to drop silently — surface every loss of one.
            (logger.info if is_fade_signal else logger.debug)(
                "market_closed_skip", asset=asset, exchange=signal.exchange)
            return

        # Correlation guard: max 2 new entries per exchange per poll cycle.
        # Flip-closes (existing + opposite signal) bypass this — they're deliberate.
        if existing is None and self._cycle_entries.get(exchange_key, 0) >= 2:
            (logger.info if is_fade_signal else logger.debug)(
                "correlation_guard", asset=asset, exchange=signal.exchange,
                cycle_entries=self._cycle_entries.get(exchange_key))
            return

        # Phase D: apply strategy quality confidence adjustment
        quality = self._strategy_quality.get(signal.source, 0.0)
        if quality >= 0.5:
            signal.confidence = min(1.0, signal.confidence + min(0.10, (quality - 0.5) * 0.20))
        elif signal.source in self._strategy_quality and quality < 0.40:
            signal.confidence = max(0.0, signal.confidence - min(0.10, (0.40 - quality) * 0.20))

        # Calibration dampening: the calib_error metric uses tier-level accuracy
        # (high=1.0/medium=0.5/low=0.0) which overstates the error vs float confidence.
        # TDE calib_error=0.848 is misleading — TDE has 57% actual accuracy and signals
        # in the 0.50-0.70 confidence range. Only dampen extreme outliers (conf>0.80)
        # to avoid suppressing all signals below the confidence minimum gate.
        calib_err = self._calibration_errors.get(signal.source, 0.0)
        if calib_err > 0.3 and signal.confidence > 0.80:
            # Gentle penalty for very high claimed confidence from miscalibrated engines
            dampen = max(0.88, 1.0 - (calib_err - 0.3) * 0.15)
            signal.confidence = round(signal.confidence * dampen, 4)
            logger.debug(
                "calibration_dampened",
                asset=asset,
                source=signal.source,
                calib_err=round(calib_err, 4),
                dampen=round(dampen, 3),
                confidence_after=signal.confidence,
            )

        # If we hold the opposite direction, close first using real price.
        # Guards: (1) confidence >= 0.5 so weak signals don't force-exit winning positions,
        # (2) position must be open >= 5 min to avoid immediate round-trips from simultaneous signals.
        _min_hold_before_opposing_s = 300
        _held_seconds = (
            (datetime.now(timezone.utc) - existing.opened_at).total_seconds()
            if existing else 0
        )
        # Hard floor: ORCL sim closed in 0.24s despite 300s guard — async race
        # with stale DB-loaded opened_at. Never act on a position < 60s old.
        if existing and existing.direction == _OPPOSITE.get(signal.direction) and _held_seconds < 60:
            logger.warning(
                "sub_second_opposing_signal_skipped",
                asset=asset,
                held_seconds=round(_held_seconds, 3),
                signal_id=signal.signal_id,
            )
        if (existing and existing.direction == _OPPOSITE.get(signal.direction)
                and signal.confidence >= 0.5
                and _held_seconds >= 60
                and _held_seconds >= _min_hold_before_opposing_s):
            exit_price, _ = await self._get_real_price(asset, existing.entry_price)
            # P&L gate: don't cut a winning position on a weak counter-signal.
            # Only exit if currently losing OR opposing signal is high-conviction.
            if existing.entry_price > 0:
                _opp_gain = (
                    (existing.entry_price - exit_price) / existing.entry_price
                    if existing.direction == "short"
                    else (exit_price - existing.entry_price) / existing.entry_price
                )
            else:
                _opp_gain = 0.0
            if _opp_gain > 0.003 and signal.confidence < 0.65:
                logger.info(
                    "opposing_signal_suppressed_winning",
                    asset=asset,
                    gain_pct=round(_opp_gain * 100, 3),
                    signal_confidence=round(signal.confidence, 3),
                )
                return  # keep position open; skip opposing exit AND new entry
            # Use existing.exchange (the held position's exchange), not signal.exchange.
            # signal.exchange could differ from the position's exchange on data inconsistency,
            # and sending a real broker close on a paper-only position would be harmful.
            existing_exchange = getattr(existing, "exchange", "")
            # Sprint 41: close for real only when THIS position was opened for
            # real (covers pilot paper-tracked trades and paper-only shorts).
            if getattr(existing, "is_live", False):
                # Sprint 57: cancel resting stop before software-side close to prevent double-fill.
                if getattr(existing, 'stop_order_id', None):
                    try:
                        broker_for_cancel = broker_factory.get_broker(existing_exchange, self._portfolio, existing.direction)
                        await broker_for_cancel.cancel_stop_order(asset, existing.stop_order_id)
                        existing.stop_order_id = None
                        self._fire(self._persist_stop_order_id(existing))
                    except Exception as _e:
                        logger.warning(f"Failed to cancel resting stop for {asset}: {_e}")
                broker = broker_factory.get_broker(existing_exchange, self._portfolio, existing.direction)
                result = await broker.close_position(asset, existing.size, existing.direction, full_exit=True)
                if result.success and result.fill_price:
                    exit_price = result.fill_price
                elif not result.success:
                    # Sprint 42: closing the paper record while the exchange
                    # close failed would leave a dangling REAL position with
                    # no tracking. Keep it tracked, retry on a later signal.
                    logger.error("live_close_failed_keeping_position",
                                 asset=asset, error=result.error)
                    self._fire(self._notifier.send_message(
                        f"🚨 LIVE CLOSE FAILED: {asset} {existing.direction} — exchange "
                        f"close rejected ({result.error}); position kept open for retry."))
                    return
            closed = self._portfolio.close_position(asset, exit_price)
            if closed:
                self._position_peaks.pop(asset, None)
                self._position_stops.pop(asset, None)
                self._fire(self._redis.hdel(_STOPS_KEY, asset))
                self._partial_exits.pop(asset, None)
                self._fire(self._persist_partial_exits(asset, set()))
                await self._persist_close_trade(closed, "opposing_signal")
                await self._notifier.trade_closed(closed, "opposing_signal")
                logger.info(
                    "trade_closed",
                    signal_id=signal.signal_id,
                    asset=asset,
                    exchange=signal.exchange,
                    confidence=round(signal.confidence, 4),
                    direction=closed.direction,
                    entry_price=round(closed.entry_price, 4),
                    exit_price=round(closed.exit_price, 4),
                    pnl=round(closed.pnl, 4),
                    fee=round(closed.fee, 4),
                    price_source="real" if signal.real_price else "proxy",
                    reason="opposing_signal",
                )
                self._log_portfolio_update(signal.signal_id, asset, signal.confidence)
                # Opposing-signal flip cooldown: prevents immediate re-entry on the
                # same asset that would cause same-cycle or next-cycle thrash.
                _opp_cd = int(os.getenv("OPP_SIGNAL_COOLDOWN_MIN", "45")) * 60
                self._asset_cooldown_until[asset] = time.monotonic() + _opp_cd
                # Count the close-then-reopen as one entry so the re-entry below
                # respects the correlation guard (max 2 per exchange per cycle).
                self._cycle_entries[exchange_key] = self._cycle_entries.get(exchange_key, 0) + 1

        # Dynamic asset ban check (auto-banned by lifecycle engine — separate from .env BANNED_ASSETS)
        try:
            _dban = await self._redis.hget(_DYNAMIC_BANS_KEY, asset.upper())
            if _dban:
                logger.info("dynamic_ban_blocked", asset=asset,
                            reason=_dban if isinstance(_dban, str) else _dban.decode())
                return
        except Exception as _dban_exc:
            # Fail-closed in live mode: Redis outage must not allow banned assets to trade.
            if self._live:
                logger.error("dynamic_ban_redis_error_fail_closed", asset=asset, error=str(_dban_exc))
                return
            logger.warning("dynamic_ban_redis_error_paper_passthrough", asset=asset, error=str(_dban_exc))

        # Governance engine weight override: scale confidence by per-engine multiplier.
        # Written to Redis trading:engine_weights by governance_engine.py.
        engine_weight = self._engine_weights.get(signal.source, 1.0)
        if engine_weight != 1.0:
            signal.confidence = max(0.0, min(1.0, signal.confidence * engine_weight))
            logger.debug("engine_weight_applied", asset=asset, source=signal.source,
                         weight=engine_weight, confidence=round(signal.confidence, 4))

        # Crypto short whitelist: only assets with proven short edge are allowed.
        # SUI/LINK/XRP removed — 0% WR on max_hold in 30d analysis.
        if signal.exchange == "CRYPTO" and signal.direction == "short":
            if asset not in _CRYPTO_SHORT_WHITELIST:
                logger.info(
                    "trade_rejected",
                    signal_id=signal.signal_id,
                    asset=asset,
                    direction=signal.direction,
                    reason="crypto_short_not_whitelisted",
                )
                return

        # Funding rate gate: block CRYPTO shorts when BTC perp funding is deeply negative.
        # Negative funding = shorts already over-crowded and paying longs = squeeze risk.
        if signal.exchange == "CRYPTO" and signal.direction == "short":
            btc_fr = self._funding_rates.get("BTCUSDT", 0.0)
            if btc_fr < _FUNDING_BLOCK_THRESHOLD:
                logger.info("funding_rate_blocked", asset=asset,
                            btc_funding=round(btc_fr * 100, 4))
                return

        # Router-confirmed non-CRYPTO signals have proven 24h edge — treat as having
        # meta-conviction so the conviction gate doesn't reject them for missing
        # news/social/volume scores (ETFRotationEngine signals have none of those).
        if _router_confirm_bypass and not signal.has_meta_conviction:
            signal.has_meta_conviction = True

        # Evaluate risk — pass macro regime and VIX for threshold adjustments
        accepted, reason = risk.evaluate(signal, self._portfolio, self._macro_regime, self._vix_price,
                                         oos_override=_is_oos_override)

        if not accepted:
            logger.info(
                "trade_rejected",
                signal_id=signal.signal_id,
                asset=asset,
                exchange=signal.exchange,
                confidence=round(signal.confidence, 4),
                direction=signal.direction,
                macro_regime=self._macro_regime,
                reason=reason,
            )
            return

        # Capital Growth Framework: enforce phase max_positions limit
        _open_count = len(self._portfolio.open_positions)
        if _open_count >= self._growth.max_positions:
            logger.info("capital_growth_max_positions",
                        asset=asset, open=_open_count,
                        max=self._growth.max_positions, phase=self._growth.phase)
            return

        # Risk guard: kill switch + daily loss limit (also deduplicates)
        guard_ok, guard_reason = await self._risk_guard.check(asset, signal.direction, self._portfolio)
        if not guard_ok:
            logger.info(
                "trade_blocked_by_risk_guard",
                signal_id=signal.signal_id,
                asset=asset,
                reason=guard_reason,
            )
            return

        # Phase 4: multi-timeframe consensus — require 60% directional agreement (last 6h).
        # Bypass for strong technical setups (tech > 0.25): RSI/momentum/BB giving a
        # clear signal should not be blocked by stale sentiment history.
        # Use signal.tech_score (extracted from signal_data in _row_to_signal) — avoids
        # a DB round-trip here and fixes the UnboundLocalError that existed when
        # tech_score was fetched later (line ~623) but used here.
        strong_tech = (
            (signal.direction == "long"  and signal.tech_score >= 0.20) or
            (signal.direction == "short" and signal.tech_score <= -0.20)
        )
        # Sprint 38: router-inverted signals skip the momentum-confirmation gates
        # (consensus / SMA trend / tech score). Those gates demand agreement with
        # recent momentum, but an inverted trade is a FADE — its evidence is that
        # the momentum signal reliably mean-reverts at the 24h horizon. All risk
        # gates (regime, RSI-opposed, funding, anti-chase, heat, drawdown, bans)
        # still apply.
        is_fade = is_fade_signal
        if not is_fade and not _router_confirm_bypass and not strong_tech and not await self._check_signal_consensus(asset, signal.direction):
            logger.info(
                "consensus_rejected",
                signal_id=signal.signal_id,
                asset=asset,
                direction=signal.direction,
                tech=round(signal.tech_score, 3),
            )
            return

        # Asset stop-loss cooldown — skip if burned twice recently
        if time.monotonic() < self._asset_cooldown_until.get(asset, 0):
            logger.info("asset_cooldown_blocked", asset=asset, direction=signal.direction)
            return

        # Drawdown protection: multi-tier governor (NORMAL/SOFT/HARD/STOP/RECOVERY).
        # DD_GOVERNOR_ENABLED=false reverts to legacy 5%→0.5x single-tier behaviour.
        _current_equity = self._portfolio.equity
        if self._peak_balance < _current_equity:
            self._peak_balance = _current_equity
            self._fire(self._persist_peak_balance())
        _gov_decision = self._dd_governor.evaluate(_current_equity, self._peak_balance)
        if _gov_decision.transitioned:
            self._fire(self._dd_governor.apply(_gov_decision))
            if _gov_decision.rebase_peak:
                self._peak_balance = _current_equity
                self._fire(self._persist_peak_balance())
                logger.info("risk_governor_peak_rebased", new_peak=_current_equity)
        if _gov_decision.block_new:
            logger.info("risk_governor_hard_stop", asset=asset, dd_state=_gov_decision.state)
            return
        drawdown_scale = _gov_decision.size_scale

        # Portfolio heat — pause new entries when open positions already losing > 1.5%
        if not await self._portfolio_heat_ok():
            logger.info("portfolio_heat_blocked", asset=asset, direction=signal.direction)
            return

        # SMA trend filter — don't trade against the local price trend
        # (skipped for router-inverted fades and router-confirmed non-CRYPTO signals;
        # the OOS walk-forward proved edge without this gate, so it should match)
        if not is_fade and not _router_confirm_bypass and not await self._is_trend_aligned(asset, signal.direction):
            logger.info("trend_filter_rejected", asset=asset, direction=signal.direction)
            return

        # BTC dominance gate — for crypto, BTC must trend with us (50-SMA).
        # Fades exempt: requiring BTC momentum agreement negates a mean-reversion
        # entry by construction (same family as the SMA-trend gate above).
        if not is_fade and signal.exchange == "CRYPTO" and asset != "BTC":
            if not await self._btc_trend_allows(signal.direction):
                logger.info("btc_gate_rejected", asset=asset, direction=signal.direction)
                return

        # BTC 200-SMA regime gates — symmetric for both directions:
        #   Bear regime: block altcoin longs (42.1% accuracy — negative EV)
        #   Bull regime: block altcoin shorts (47.8% accuracy — below 50%)
        # Data source: 180-day technical replay, 12k+ outcomes per window.
        # Fades exempt (Sprint 39b): 14d OOS walk-forward — ALL 66 fade wins
        # (+86.9% on non-whitelist alts) occurred in bear regime; this gate
        # would have blocked every one of them. The fade IS the bear-bounce.
        if not is_fade and signal.exchange == "CRYPTO" and asset != "BTC":
            if signal.direction == "long" and not await self._is_btc_bull_regime():
                logger.info("btc_bear_regime_blocked", asset=asset, direction="long")
                return
            if signal.direction == "short" and await self._is_btc_bull_regime():
                logger.info("btc_bull_blocks_short", asset=asset,
                            reason="altcoin shorts are 47.8% accurate in BTC bull regime")
                return

        # BTC longs in bear regime: require extreme RSI oversold (< 35) as confirmation.
        # BTC is exempt from the altcoin gate above, but bear-market longs on BTC still
        # need a strong technical reason — counter-trend longs without oversold RSI are
        # just as likely to fail as altcoin longs in bear.
        if signal.exchange == "CRYPTO" and asset == "BTC" and signal.direction == "long":
            if not await self._is_btc_bull_regime():
                rsi_btc = await self._get_rsi(asset)
                if rsi_btc >= 35:
                    logger.info("btc_bear_long_not_oversold", asset=asset, rsi=round(rsi_btc, 1))
                    return

        # Altcoin long whitelist: ARB/OP/DOT/AVAX/DOGE/TON are chronic losers as longs.
        # Only BTC, ETH, SOL, BNB allowed for longs — top-tier liquidity and less dump risk.
        # Fades exempt (Sprint 39b): the whitelist is measured BACKWARDS for fades —
        # OOS, whitelist fade-longs lost (-6.9%, n=2) while non-whitelist fade-longs
        # made +86.9% (n=64; ADA 82% WR). The whitelist was built against momentum
        # chases; fades long the dump, not the pump.
        if (not is_fade and signal.exchange == "CRYPTO" and signal.direction == "long"
                and asset not in _CRYPTO_LONG_WHITELIST):
            logger.info("altcoin_long_blocked", asset=asset,
                        reason="not in long whitelist (BTC/ETH/SOL/BNB only)")
            return

        # Live-only execution gate is handled below (_LIVE_CRYPTO_SHORT_WHITELIST).
        # Paper simulation is allowed for any crypto short to build OOS data.

        # Technical score gate — tech indicators MUST confirm direction.
        # signal.tech_score was extracted from signal_data in _row_to_signal. If it's
        # zero (engine didn't include it), fall back to a fresh DB fetch.
        if signal.tech_score == 0.0:
            signal.tech_score = await self._get_tech_score(asset)
        tech_score = signal.tech_score  # local alias for readability
        if not is_fade and not _router_confirm_bypass and not await self._tech_score_allows(asset, signal.direction, signal.exchange,
                                              cached_tech=tech_score,
                                              social_score=signal.social_score):
            logger.info("tech_score_rejected", asset=asset, direction=signal.direction,
                        tech=round(tech_score, 3), social=round(signal.social_score, 3))
            return

        # RSI-tiered gate + vol sizing: single DB fetch feeds both computations.
        rsi, realized_vol = await self._get_rsi_and_vol(asset)
        is_rsi_extreme = (signal.direction == "long" and rsi < 35) or \
                         (signal.direction == "short" and rsi > 65)
        is_rsi_opposed = (signal.direction == "long" and rsi > 65) or \
                         (signal.direction == "short" and rsi < 35)
        if is_rsi_opposed and not _router_confirm_bypass:
            logger.info("rsi_opposed", asset=asset, direction=signal.direction, rsi=round(rsi, 1))
            return
        if is_rsi_opposed and _router_confirm_bypass:
            logger.debug("rsi_opposed_oos_bypass", asset=asset, rsi=round(rsi, 1),
                         router_action=signal.router_action)
        if is_rsi_extreme:
            # Oversold/overbought: boost confidence (these are highest-probability setups)
            boost = 1.15 if (rsi < 28 or rsi > 72) else 1.08
            signal.confidence = min(1.0, signal.confidence * boost)
            logger.debug("rsi_extreme_boost", asset=asset, rsi=round(rsi, 1), boost=boost)
        elif not is_fade and not _router_confirm_bypass and signal.exchange == "CRYPTO" and abs(tech_score) < 0.20:
            # Neutral RSI + weak tech: no directional edge. Raised from 0.12 → 0.20
            # based on 180-day replay data: short accuracy only crosses 55% at |tech| >= 0.20.
            # Fades exempt (router measured edge). OOS-confirmed signals exempt.
            logger.debug("neutral_rsi_weak_tech", asset=asset, rsi=round(rsi, 1),
                         tech=round(tech_score, 3))
            return

        # Anti-momentum filter — don't chase recent pumps or dumps (CRYPTO only)
        if not await self._entry_momentum_ok(asset, signal.direction, signal.exchange):
            logger.info("momentum_filter_rejected", asset=asset, direction=signal.direction)
            return

        # GPT signal validation — adjusts confidence before sizing (opt-in via env var)
        if os.getenv("GPT_SIGNAL_VALIDATION", "false").lower() == "true":
            signal.confidence = await _gpt_validate(
                self._pool, asset, signal.direction,
                signal.composite_score, signal.confidence,
            )
            # Re-check confidence threshold after GPT adjustment
            accepted, reason = risk.evaluate(signal, self._portfolio, self._macro_regime, self._vix_price,
                                             oos_override=False)
            if not accepted:
                logger.info("gpt_adjusted_below_threshold", asset=asset, reason=reason)
                return

        # Regime intelligence filter: blocks/penalizes signals by macro regime.
        # Pure function in risk.py; gate behind REGIME_FILTER_ENABLED env (default false).
        _rf_allowed, _rf_mult, _rf_reason = risk.regime_filter(
            signal.exchange or "", asset, signal.direction,
            getattr(signal, "router_edge", 0.0), self._macro_regime,
        )
        if not _rf_allowed:
            logger.info("regime_filter_block", asset=asset, direction=signal.direction,
                        regime=self._macro_regime, reason=_rf_reason)
            return

        # Phase C: volatility-scaled position sizing (target 2% daily vol per position)
        # CRYPTO is capped at 1.0x — low realized vol boosting crypto positions to
        # 9.75% of balance was causing oversized bets relative to the stop-loss amount.
        # realized_vol already computed above by _get_rsi_and_vol.
        if realized_vol > 0:
            target_vol = float(os.getenv("TARGET_VOL", "0.02"))
            max_scale = 1.0 if signal.exchange == "CRYPTO" else 1.3
            signal.vol_scale = min(max_scale, max(0.3, target_vol / realized_vol))

        # Regime size multiplier (from regime_filter above; 1.0 when filter disabled)
        if _rf_mult != 1.0:
            signal.vol_scale *= _rf_mult
            logger.debug("regime_size_mult", asset=asset, regime=self._macro_regime,
                         mult=_rf_mult, scale=round(signal.vol_scale, 3))

        # Invert signals carry less proven edge — size down to limit exposure on reversals
        if signal.router_action == "invert":
            signal.vol_scale *= 0.5
            logger.debug("invert_size_penalty", asset=asset, scale=round(signal.vol_scale, 3))

        # Apply Kelly fraction (overrides default fraction when data is sufficient)
        kelly = await self._get_kelly_fraction(signal.exchange or "", signal.direction)
        if kelly is not None:
            from risk import trade_fraction as _base_fraction
            base = _base_fraction((signal.exchange or "").upper(), signal.confidence)
            if kelly < base:
                # Kelly says bet less than our default — respect it
                signal.vol_scale *= (kelly / base)
                logger.debug("kelly_sizing", asset=asset, kelly=round(kelly, 4),
                             base=round(base, 4), scale=round(signal.vol_scale, 3))

        # Exchange performance: scale down in losing streaks
        perf_scale = await self._get_exchange_performance_scale(signal.exchange or "")
        if perf_scale != 1.0:
            signal.vol_scale *= perf_scale
            logger.debug("perf_scale", asset=asset, exchange=signal.exchange,
                         scale=round(perf_scale, 2))

        # Adaptive sizing: per-asset WR + portfolio heat + loss streak (no regime — owned by regime_filter)
        _adapt_mult, _adapt_bd = await self._adaptive_sizer.multiplier(
            asset, self._portfolio.open_positions, self._portfolio._live_prices)
        if _adapt_mult != 1.0:
            signal.vol_scale *= _adapt_mult
            logger.debug("adaptive_size_mult", asset=asset, mult=round(_adapt_mult, 3),
                         scale=round(signal.vol_scale, 3), breakdown=_adapt_bd)

        # Capital Growth Framework: phase-based size multiplier (P1=×0.8, P2-P4=×1.0)
        if self._growth.size_multiplier != 1.0:
            signal.vol_scale *= self._growth.size_multiplier
            logger.debug("growth_size_mult", asset=asset, phase=self._growth.phase,
                         mult=self._growth.size_multiplier, scale=round(signal.vol_scale, 3))

        # Drawdown protection: size down when in drawdown
        signal.vol_scale *= drawdown_scale

        # Hard cap: no single paper trade > 10% of balance (defense-in-depth vs Kelly mis-fire).
        # Passthrough signals (no proven OOS edge) use a tighter 4% cap.
        # Notional = balance * trade_fraction * vol_scale; cap vol_scale so product ≤ cap.
        # Equity-based sizing: enabled via EDGE_SCALE_ENABLED=true.
        # Replaces fixed notional with equity% × cluster_edge × regime multiplier.
        # Default false = zero regression on existing behaviour.
        if os.getenv("EDGE_SCALE_ENABLED", "false").lower() in ("1", "true", "yes"):
            _bucket = self._truth_router.bucket_for(signal.direction, signal.composite_score)
            _exch_c = "CRYPTO" if (signal.exchange or "").upper() == "CRYPTO" else "OTHER"
            _cluster_edge = self._cluster_calc.compute_cluster_edge(
                self._truth_router.lift_lookup, _bucket, _exch_c,
                signal.direction, self._regime_profile,
            )
            from risk import equity_based_notional as _ebn
            _eq_notional = _ebn(
                float(self._portfolio.balance),
                (signal.exchange or "").upper(),
                signal.confidence,
                _cluster_edge,
                self._regime_profile,
                drawdown_scale,
            )
            if _eq_notional <= 0.0:
                logger.info("equity_sizing_zero", asset=asset, dd_state=_gov_decision.state)
                return
            signal.target_notional = _eq_notional
            logger.info("equity_sizing", asset=asset,
                        notional=round(_eq_notional, 2),
                        cluster_edge=round(_cluster_edge, 3),
                        regime=self._regime_profile,
                        dd_state=_gov_decision.state)

        _router_act = signal.router_action or "passthrough"
        _MAX_SINGLE_TRADE_PCT = 0.04 if _router_act == "passthrough" else 0.10
        _paper_bal = float(self._portfolio.balance)
        if _paper_bal > 0:
            from risk import trade_fraction as _cap_fraction
            _base_frac = _cap_fraction((signal.exchange or "").upper(), signal.confidence)
            _effective_frac = _base_frac * signal.vol_scale
            if _effective_frac > _MAX_SINGLE_TRADE_PCT:
                # Cap vol_scale so balance * _base_frac * vol_scale ≤ cap% of balance
                signal.vol_scale = (_MAX_SINGLE_TRADE_PCT / _base_frac) if _base_frac > 0 else signal.vol_scale
                logger.debug("notional_cap_applied", asset=asset,
                             cap_pct=_MAX_SINGLE_TRADE_PCT, router_action=_router_act,
                             scale=round(signal.vol_scale, 3))

        # Live mode: execute via real broker
        live_filled = False
        if self._live:
            # Sprint 39: with BINANCE_FUTURES=true (default), crypto shorts execute
            # on USDT-M futures. Only when futures are disabled do shorts fall back
            # to paper-only tracking (Binance Spot cannot short-sell).
            if (signal.exchange == "CRYPTO" and signal.direction == "short"
                    and not broker_factory.futures_enabled()):
                logger.info(
                    "live_short_paper_only",
                    asset=asset,
                    reason="futures disabled and Binance Spot cannot short; tracked in paper mode",
                )
                # Fall through to paper-record the trade (no real broker call)
            elif (_LIVE_PILOT_FADES_ONLY and not is_fade_signal
                    and asset not in _LIVE_CONFIRM_TICKERS
                    and asset not in _LIVE_CRYPTO_SHORT_WHITELIST):
                # Sprint 41 pilot: real money only behind the measured edge.
                # _LIVE_CRYPTO_SHORT_WHITELIST assets also bypass the pilot gate —
                # both LIVE_CONFIRM_TICKERS and LIVE_CRYPTO_SHORT_WHITELIST are
                # explicit user approvals; the downstream whitelist check re-verifies.
                logger.info("live_pilot_paper_only", asset=asset,
                            direction=signal.direction,
                            router_action=signal.router_action or "passthrough")
                # Fall through to paper-record the trade (no real broker call)
            elif not is_fade_signal and (signal.router_action or "passthrough") == "passthrough":
                # Passthrough = unproven OOS edge. Paper-simulate for data collection only.
                # Only router_action="confirm" (or fade/invert) signals place real live orders.
                logger.info("live_passthrough_paper_only", asset=asset,
                            direction=signal.direction,
                            router_action="passthrough")
                # Fall through to paper-record the trade (no real broker call)
            elif (signal.exchange == "CRYPTO" and signal.direction == "short"
                    and not is_fade_signal
                    and asset not in _LIVE_CRYPTO_SHORT_WHITELIST):
                # Sprint 59: confirm-ticker shorts restricted to the live whitelist.
                logger.info("live_short_not_in_live_whitelist", asset=asset, direction=signal.direction)
                # Fall through to paper-record the trade (no real broker call)
            else:
                # Real orders are sized from the REAL wallet, never the paper
                # ledger (paper $9.6k would size a $100 wallet's trades 60x).
                if self._live_wallet_usdt < _LIVE_MIN_NOTIONAL:
                    logger.warning("live_wallet_insufficient", asset=asset,
                                   wallet=round(self._live_wallet_usdt, 2),
                                   needed=_LIVE_MIN_NOTIONAL)
                    return
                broker = broker_factory.get_broker(signal.exchange, self._portfolio, signal.direction)
                from risk import trade_fraction as _trade_fraction
                fraction = _trade_fraction((signal.exchange or "").upper(), signal.confidence)
                fraction *= min(1.5, signal.vol_scale)  # apply vol scaling
                if os.getenv("PORTFOLIO_EDGE_SIZING", "false").lower() in ("1", "true", "yes"):
                    _pe_raw = await self._redis.get("paper_trading:portfolio_edge_score")
                    if _pe_raw and float(_pe_raw) < 0.1:
                        fraction *= 0.8
                _free = self._live_wallet_usdt
                _eff_min = max(_LIVE_MIN_NOTIONAL, _LIVE_MIN_NOTIONAL_PCT * _free)
                _eff_max = max(_LIVE_MAX_NOTIONAL, _LIVE_MAX_NOTIONAL_PCT * _free)
                _eff_min = min(_eff_min, _eff_max)
                notional = min(max(_free * fraction, _eff_min),
                               _eff_max,
                               _free * 0.95)
                result = await broker.place_order(asset, signal.direction, notional)
                if not result.success:
                    logger.warning("live_order_failed", asset=asset, error=result.error)
                    return
                live_filled = True
                # Decrement the local wallet figure now — back-to-back fades
                # inside one 15-min sync window must not size off stale funds.
                # (Conservative: closes re-credit only at the next sync.)
                self._live_wallet_usdt = max(
                    0.0, self._live_wallet_usdt - result.notional - result.fee)
                # Override signal price with real fill so portfolio tracks correctly
                signal.real_price = result.fill_price if result.fill_price else signal.real_price
                logger.info(
                    "live_order_filled",
                    asset=asset, direction=signal.direction,
                    order_id=result.order_id, broker=broker.broker_name,
                    fill_price=round(result.fill_price, 4),
                    notional=round(result.notional, 2),
                    fee=round(result.fee, 6),
                )
                try:
                    await self._notifier.live_trade_opened(
                        asset, signal.direction, result.notional, result.fill_price or 0.0
                    )
                except Exception:
                    pass

        signal.volume_cap_usd = await self._volume_cap_usd(asset)
        signal.macro_regime = self._macro_regime
        # Sprint 42: a real fill is recorded 1:1 from the OrderResult — paper
        # sizing (balance fraction, MIN_NOTIONAL floor) must never reshape or
        # reject a position the exchange already holds.
        if live_filled:
            trade = self._portfolio.register_live_fill(signal, result)
            if trade is None:
                # Exchange reported a fill we can't represent (qty/price 0) —
                # an untracked live position. Unwind immediately and scream.
                logger.error("live_orphan_unwind", asset=asset, direction=signal.direction)
                broker = broker_factory.get_broker(signal.exchange, self._portfolio, signal.direction)
                res = await broker.close_position(asset, result.qty, signal.direction)
                await self._notifier.send_message(
                    f"🚨 LIVE ORPHAN: {asset} {signal.direction} order filled but paper "
                    f"tracking failed — auto-unwind {'OK' if res.success else 'FAILED: ' + str(res.error)}"
                )
                return
            # Sprint 57: store resting stop order ID on the trade object for later cancellation.
            if result.stop_order_id:
                trade.stop_order_id = result.stop_order_id
                logger.info("live_resting_stop_stored",
                            asset=asset, stop_order_id=result.stop_order_id)
        else:
            trade = self._portfolio.open_position(signal)
            if trade is None:
                # None = no usable price OR notional below MIN_NOTIONAL (executor.open_trade).
                (logger.info if is_fade_signal else logger.debug)(
                    "trade_skipped_no_price", signal_id=signal.signal_id, asset=asset)
                return
        self._position_peaks[asset] = trade.entry_price
        # Reuse realized_vol already fetched by _get_rsi_and_vol — no extra DB call.
        _atr_stop = await self._compute_atr_stop(asset, vol=realized_vol)
        self._position_stops[asset] = _atr_stop
        self._fire(self._persist_atr_stop(asset, _atr_stop))
        # Clear tranche tracking for this position
        self._partial_exits[asset] = set()
        self._cycle_entries[exchange_key] = self._cycle_entries.get(exchange_key, 0) + 1
        await self._persist_open_trade(trade)
        await self._notifier.trade_opened(trade)
        self._last_open_at = time.monotonic()
        self._silence_alerted = False
        logger.info(
            "trade_opened",
            signal_id=signal.signal_id,
            asset=asset,
            exchange=signal.exchange,
            confidence=round(signal.confidence, 4),
            direction=trade.direction,
            entry_price=round(trade.entry_price, 4),
            size=round(trade.size, 6),
            notional=round(trade.notional, 4),
            fee=round(trade.fee, 4),
            price_source="real",
            macro_regime=self._macro_regime,
            max_hold_hours=trade.max_hold_hours,
            volume_cap_usd=round(signal.volume_cap_usd, 2) if signal.volume_cap_usd else None,
            vol_scale=round(signal.vol_scale, 3),
            router_action=signal.router_action or None,
            router_edge=round(signal.router_edge, 3) if signal.router_action else None,
            mode="live" if trade.is_live else _DB_PAPER_MODE,
        )
        self._log_portfolio_update(signal.signal_id, asset, signal.confidence)

        # Sprint 38: shadow-mode LLM audit — fire-and-forget, no authority.
        self._fire(self._llm_auditor.audit(trade, signal, {
            "rsi": rsi,
            "regime": self._macro_regime,
            "funding": self._funding_rates.get("BTCUSDT", 0.0),
        }))
        self._fire(self._publish_portfolio_edge())

    # ── Position monitor — single pass (partial → trail → stop → age-out) ────

    async def _check_positions(self) -> None:
        """Iterate open positions once, applying all exit checks in priority order.

        Priority (highest first):
          1. Partial profit targets (T1/T2) — position may remain after
          2. Trailing stop — closes position if activated and retreated
          3. Hard stop loss (non-CRYPTO only) — closes on ATR-threshold breach
          4. Age-out — closes when max_hold_hours exceeded

        Trail stop intentionally runs before age-out: a position that has both
        aged out AND has an active trail stop should exit via the trail stop path
        (which correctly captures partial profit), not just expire neutrally.

        Price is resolved once per position from the cached _live_prices batch
        (populated by _refresh_portfolio_prices); DB fallback only fires on cache miss.
        """
        aged_assets = set(self._portfolio.aged_positions())

        for asset in list(self._portfolio.open_positions):
            pos = self._portfolio.open_positions.get(asset)
            if pos is None:
                continue
            try:
                current_price = self._portfolio._live_prices.get(asset, 0.0)
                price_ts: datetime | None = self._live_price_ts.get(asset)
                if current_price <= 0:
                    current_price, price_ts = await self._get_real_price(asset)

                # Staleness guard: skip stop/trail/partial when the best available price
                # quote is more than 10 minutes old AND the market is open.
                # After-hours: last close price is valid — don't suspend exits overnight.
                # Age-out (section 4) still runs on stale price (wall-clock, not price).
                _price_stale = False
                _price_age_s = 0.0
                if current_price > 0 and price_ts is not None:
                    now_utc = datetime.now(timezone.utc)
                    _price_age_s = (now_utc - price_ts).total_seconds()
                    if self._quote_stale(_price_age_s, pos.exchange or "CRYPTO"):
                        _price_stale = True
                        if asset not in self._stale_price_warned:
                            self._stale_price_warned.add(asset)
                            logger.warning(
                                "stale_position_price",
                                asset=asset,
                                age_s=round(_price_age_s),
                            )
                            self._fire(self._notifier.send_message(
                                f"⚠️ STALE PRICE: {asset} position price quote is "
                                f"{_price_age_s / 60:.0f} min old — "
                                f"stop/trail/partial exits SUSPENDED until feed recovers."
                            ))
                    else:
                        # Price freshened — allow re-alert if it goes stale again
                        self._stale_price_warned.discard(asset)

                # F7: stale feed + last-known price ALREADY past the hard stop → flatten now.
                # Suspending exits (block below) would let a confirmed loss run until the feed
                # recovers or age-out (hours). The market close fills at the true exchange price,
                # so acting on a stale-but-breached quote is safe (real price is ≥ this bad).
                if _price_stale and current_price > 0 and pos.entry_price > 0:
                    if pos.direction == "long":
                        _stale_gain = (current_price - pos.entry_price) / pos.entry_price
                    else:
                        _stale_gain = (pos.entry_price - current_price) / pos.entry_price
                    _exch_st = getattr(pos, "exchange", "")
                    _base_st = self._crypto_stop_loss_pct if _exch_st == "CRYPTO" else self._stop_loss_pct
                    _stop_st = max(self._position_stops.get(asset, _base_st), _base_st)
                    if _stale_gain < -_stop_st:
                        logger.warning("stale_emergency_flatten", asset=asset,
                                       loss_pct=round(_stale_gain * 100, 2),
                                       threshold_pct=round(_stop_st * 100, 2))
                        if getattr(pos, "is_live", False):
                            if getattr(pos, "stop_order_id", None):
                                try:
                                    await broker_factory.get_broker(_exch_st, self._portfolio, pos.direction).cancel_stop_order(asset, pos.stop_order_id)
                                    pos.stop_order_id = None
                                    self._fire(self._persist_stop_order_id(pos))
                                except Exception as _e:
                                    logger.warning(f"Failed to cancel resting stop for {asset}: {_e}")
                            broker = broker_factory.get_broker(_exch_st, self._portfolio, pos.direction)
                            res = await broker.close_position(asset, pos.size, pos.direction, full_exit=True)
                            if res.success and res.fill_price:
                                current_price = res.fill_price
                            elif not res.success:
                                logger.error("live_stale_emergency_close_failed", asset=asset, error=res.error)
                                self._fire(self._notifier.send_message(
                                    f"🚨 LIVE CLOSE FAILED (stale emergency): {asset} — kept open for retry ({res.error})"))
                                continue  # keep tracked; retry next tick
                        closed = self._portfolio.close_position(asset, current_price)
                        if closed:
                            self._position_peaks.pop(asset, None)
                            self._position_stops.pop(asset, None)
                            self._fire(self._redis.hdel(_STOPS_KEY, asset))
                            self._partial_exits.pop(asset, None)
                            self._fire(self._persist_partial_exits(asset, set()))
                            await self._persist_close_trade(closed, "stale_emergency_stop")
                            await self._notifier.trade_closed(closed, "stale_emergency_stop")
                            logger.info("trade_closed", signal_id=closed.signal_id, asset=asset,
                                        direction=closed.direction,
                                        entry_price=round(closed.entry_price, 4),
                                        exit_price=round(closed.exit_price, 4),
                                        pnl=round(closed.pnl, 4), fee=round(closed.fee, 4),
                                        reason="stale_emergency_stop",
                                        mode="live" if getattr(closed, "is_live", False) else _DB_PAPER_MODE)
                            self._log_portfolio_update(closed.signal_id, asset, 0.0)
                        continue

                # Stale feed for too long → flatten regardless of last-known price. F7 above only
                # fires when the *stale* quote already shows a stop breach; a feed dead well past the
                # suspend window means we hold a live leveraged position BLIND to any flash move.
                # The broker close fills at the true exchange price, so a dead local feed can't block it.
                if (self._blind_flatten_due(getattr(pos, "is_live", False), _price_stale,
                                            _price_age_s, self._stale_flatten_s)
                        and pos.entry_price > 0):
                    logger.warning("stale_blind_flatten", asset=asset,
                                   age_s=round(_price_age_s), threshold_s=self._stale_flatten_s)
                    _exch_bf = getattr(pos, "exchange", "")
                    if getattr(pos, "stop_order_id", None):
                        try:
                            await broker_factory.get_broker(_exch_bf, self._portfolio, pos.direction).cancel_stop_order(asset, pos.stop_order_id)
                            pos.stop_order_id = None
                            self._fire(self._persist_stop_order_id(pos))
                        except Exception as _e:
                            logger.warning(f"Failed to cancel resting stop for {asset}: {_e}")
                    broker = broker_factory.get_broker(_exch_bf, self._portfolio, pos.direction)
                    res = await broker.close_position(asset, pos.size, pos.direction, full_exit=True)
                    if not res.success:
                        logger.error("live_stale_blind_close_failed", asset=asset, error=res.error)
                        self._fire(self._notifier.send_message(
                            f"🚨 LIVE CLOSE FAILED (stale-blind {_price_age_s / 60:.0f}m): {asset} — kept open, retry next tick ({res.error})"))
                        continue  # keep tracked; retry next tick
                    _exit_px = res.fill_price or current_price
                    closed = self._portfolio.close_position(asset, _exit_px)
                    if closed:
                        self._position_peaks.pop(asset, None)
                        self._position_stops.pop(asset, None)
                        self._fire(self._redis.hdel(_STOPS_KEY, asset))
                        self._partial_exits.pop(asset, None)
                        self._fire(self._persist_partial_exits(asset, set()))
                        await self._persist_close_trade(closed, "stale_blind_flatten")
                        await self._notifier.trade_closed(closed, "stale_blind_flatten")
                        logger.info("trade_closed", signal_id=closed.signal_id, asset=asset,
                                    direction=closed.direction,
                                    entry_price=round(closed.entry_price, 4),
                                    exit_price=round(closed.exit_price, 4),
                                    pnl=round(closed.pnl, 4), fee=round(closed.fee, 4),
                                    reason="stale_blind_flatten",
                                    mode="live" if getattr(closed, "is_live", False) else _DB_PAPER_MODE)
                        self._log_portfolio_update(closed.signal_id, asset, 0.0)
                    continue

                # Need valid, fresh price for partial/trail/stop checks; age-out falls back to entry
                if current_price > 0 and pos.entry_price > 0 and not _price_stale:
                    # Direction-agnostic gain pct (positive = winning, negative = losing)
                    if pos.direction == "long":
                        gain_pct = (current_price - pos.entry_price) / pos.entry_price
                    else:
                        gain_pct = (pos.entry_price - current_price) / pos.entry_price

                    # Auto-recover missing stop order (e.g. after restart or failed initial placement)
                    # Try once per asset per engine lifetime — -4120 accounts can't use exchange stops.
                    if (getattr(pos, "is_live", False)
                            and not getattr(pos, "stop_order_id", None)
                            and asset not in self._stop_place_tried):
                        self._stop_place_tried.add(asset)
                        try:
                            _brk = broker_factory.get_broker(getattr(pos, "exchange", ""), self._portfolio, pos.direction)
                            _new_sid = await _brk.place_stop_for_asset(asset, pos.direction, pos.entry_price, pos.size)
                            if _new_sid:
                                pos.stop_order_id = _new_sid
                                self._fire(self._persist_stop_order_id(pos))
                                logger.info("initial_stop_recovered", asset=asset, stop_order_id=_new_sid)
                            else:
                                logger.warning("initial_stop_unavailable", asset=asset,
                                               hint="exchange rejected stop — software stop only")
                        except Exception as _se:
                            logger.warning("initial_stop_recover_failed", asset=asset, error=str(_se))

                    # ── 1. Partial profit targets ──────────────────────────────
                    # One tranche per tick: after T1 fires we `continue` so that T2
                    # evaluates on the next cycle with a fresh market price.
                    taken = self._partial_exits.get(asset, set())
                    if "T1" not in taken and gain_pct >= self._partial_t1_pct:  # env-tunable; clears round-trip fees
                        if getattr(pos, "is_live", False):
                            # A tranche below the exchange's ~$5 pair minimum can
                            # never fill — mark it taken (skip, full exit later via
                            # trail/age-out) instead of retrying a doomed order.
                            if pos.size * 0.33 * current_price < 6.0:
                                taken.add("T1")
                                self._partial_exits[asset] = taken
                                self._fire(self._persist_partial_exits(asset, taken))
                                logger.info("live_partial_t1_too_small", asset=asset,
                                            tranche_usd=round(pos.size * 0.33 * current_price, 2))
                                continue
                            # Sprint 57: cancel resting stop before software-side partial close.
                            if getattr(pos, 'stop_order_id', None):
                                try:
                                    await broker_factory.get_broker(getattr(pos, "exchange", ""), self._portfolio, pos.direction).cancel_stop_order(asset, pos.stop_order_id)
                                    pos.stop_order_id = None
                                    self._fire(self._persist_stop_order_id(pos))
                                except Exception as _e:
                                    logger.warning(f"Failed to cancel resting stop for {asset}: {_e}")
                            broker = broker_factory.get_broker(getattr(pos, "exchange", ""), self._portfolio, pos.direction)
                            res = await broker.close_position(asset, pos.size * 0.33, pos.direction, maker=True)
                            if not res.success:
                                # Don't record a partial the exchange didn't take.
                                logger.warning("live_partial_t1_failed", asset=asset, error=res.error)
                                continue
                            if res.fill_price:
                                current_price = res.fill_price
                            # Record only what the exchange actually closed: a maker
                            # partial fill with a failed market remainder returns
                            # res.qty < requested; a hardcoded 0.33 would diverge the
                            # engine's tracked size from the exchange.
                            t1_frac = min(0.33, res.qty / pos.size) if (pos.size > 0 and res.qty) else 0.33
                        else:
                            t1_frac = 0.33
                        partial = self._portfolio.partial_close_position(asset, t1_frac, current_price)
                        if partial:
                            taken.add("T1")
                            self._partial_exits[asset] = taken
                            self._fire(self._persist_partial_exits(asset, taken))
                            await self._persist_close_trade(partial, "partial_t1")
                            await self._notifier.trade_closed(partial, "partial_t1")
                            logger.info("tranche_1_taken", asset=asset,
                                        gain_pct=round(gain_pct * 100, 2), pnl=round(partial.pnl, 4),
                                        mode="live" if getattr(pos, "is_live", False) else _DB_PAPER_MODE)
                            # Re-place resting stop for remaining 67%
                            if getattr(pos, "is_live", False):
                                try:
                                    new_sid = await broker.place_stop_for_asset(
                                        asset, pos.direction, pos.entry_price, pos.size)
                                    if new_sid:
                                        pos.stop_order_id = new_sid
                                        logger.info("t1_stop_replaced", asset=asset,
                                                    stop_order_id=new_sid)
                                        self._fire(self._persist_stop_order_id(pos))
                                except Exception as _se:
                                    logger.warning("t1_stop_replace_failed",
                                                   asset=asset, error=str(_se))
                        continue  # re-evaluate T2 next tick with fresh price

                    if "T1" in taken and "T2" not in taken and gain_pct >= self._partial_t2_pct:  # env-tunable; widened to ride further
                        if getattr(pos, "is_live", False):
                            if pos.size * 0.50 * current_price < 6.0:
                                taken.add("T2")
                                self._partial_exits[asset] = taken
                                self._fire(self._persist_partial_exits(asset, taken))
                                logger.info("live_partial_t2_too_small", asset=asset,
                                            tranche_usd=round(pos.size * 0.50 * current_price, 2))
                                continue
                            # Sprint 57: cancel resting stop before software-side partial close.
                            if getattr(pos, 'stop_order_id', None):
                                try:
                                    await broker_factory.get_broker(getattr(pos, "exchange", ""), self._portfolio, pos.direction).cancel_stop_order(asset, pos.stop_order_id)
                                    pos.stop_order_id = None
                                    self._fire(self._persist_stop_order_id(pos))
                                except Exception as _e:
                                    logger.warning(f"Failed to cancel resting stop for {asset}: {_e}")
                            broker = broker_factory.get_broker(getattr(pos, "exchange", ""), self._portfolio, pos.direction)
                            res = await broker.close_position(asset, pos.size * 0.50, pos.direction, maker=True)
                            if not res.success:
                                # Don't record a partial the exchange didn't take.
                                logger.warning("live_partial_t2_failed", asset=asset, error=res.error)
                                continue
                            if res.fill_price:
                                current_price = res.fill_price
                            # Record only what the exchange actually closed (see T1).
                            t2_frac = min(0.50, res.qty / pos.size) if (pos.size > 0 and res.qty) else 0.50
                        else:
                            t2_frac = 0.50
                        partial = self._portfolio.partial_close_position(asset, t2_frac, current_price)
                        if partial:
                            taken.add("T2")
                            self._partial_exits[asset] = taken
                            self._fire(self._persist_partial_exits(asset, taken))
                            await self._persist_close_trade(partial, "partial_t2")
                            await self._notifier.trade_closed(partial, "partial_t2")
                            logger.info("tranche_2_taken", asset=asset,
                                        gain_pct=round(gain_pct * 100, 2), pnl=round(partial.pnl, 4),
                                        mode="live" if getattr(pos, "is_live", False) else _DB_PAPER_MODE)
                            # Re-place resting stop for remaining ~17%
                            if getattr(pos, "is_live", False):
                                try:
                                    new_sid = await broker.place_stop_for_asset(
                                        asset, pos.direction, pos.entry_price, pos.size)
                                    if new_sid:
                                        pos.stop_order_id = new_sid
                                        logger.info("t2_stop_replaced", asset=asset,
                                                    stop_order_id=new_sid)
                                        self._fire(self._persist_stop_order_id(pos))
                                except Exception as _se:
                                    logger.warning("t2_stop_replace_failed",
                                                   asset=asset, error=str(_se))

                    pos = self._portfolio.open_positions.get(asset)
                    if pos is None:
                        continue

                    # ── 2. Trailing stop ───────────────────────────────────────
                    peak = self._position_peaks.get(asset, pos.entry_price)
                    # Two-stage trail: tighten from 1.5% to 0.8% once T2 fires,
                    # so the final tranche rides momentum but locks in above +0.7%.
                    _effective_trail = (
                        0.008 if "T2" in taken else self._trail_stop_pct
                    )
                    if pos.direction == "long":
                        if current_price > peak:
                            self._position_peaks[asset] = current_price
                            peak = current_price
                        gain_from_entry = (peak - pos.entry_price) / pos.entry_price
                        trail_triggered = (
                            gain_from_entry >= self._trail_activate_pct
                            and current_price <= peak * (1 - _effective_trail)
                        )
                    else:
                        if current_price < peak:
                            self._position_peaks[asset] = current_price
                            peak = current_price
                        gain_from_entry = (pos.entry_price - peak) / pos.entry_price
                        trail_triggered = (
                            gain_from_entry >= self._trail_activate_pct
                            and current_price >= peak * (1 + _effective_trail)
                        )

                    # If hard stop is also breached (e.g., stale-price wake-up with big gap),
                    # defer trail to hard stop block so the close reason is correct.
                    if trail_triggered and gain_pct is not None:
                        _exch_sp = getattr(pos, "exchange", "")
                        _base_sp = self._crypto_stop_loss_pct if _exch_sp == "CRYPTO" else self._stop_loss_pct
                        _eff_sp = max(self._position_stops.get(asset, _base_sp), _base_sp)
                        if gain_pct < -_eff_sp:
                            trail_triggered = False  # hard stop takes priority

                    # When trail activates (but hasn't fired yet) and no T1 stop placed yet,
                    # move the resting hard-stop to entry price (break-even stop).
                    # Prevents trail firing at a loss when price bounces past entry between polls.
                    # Note: place_stop_for_asset adds stop_pct on top of entry_price (wrong for
                    # break-even); call place_stop_market_order directly with the raw entry_price.
                    trail_active = gain_from_entry >= self._trail_activate_pct
                    if (trail_active and asset not in self._trail_activated
                            and "T1" not in taken
                            and getattr(pos, "is_live", False)
                            and getattr(pos, "stop_order_id", None)):
                        try:
                            _brk = broker_factory.get_broker(
                                getattr(pos, "exchange", ""), self._portfolio, pos.direction)
                            await _brk.cancel_stop_order(asset, pos.stop_order_id)
                            # Break-even: stop fires at entry price itself (not entry ± stop_pct).
                            # _brk is always BinanceFuturesBroker here (live + stop_order_id set).
                            _be_side = "BUY" if pos.direction == "short" else "SELL"
                            from brokers.binance_futures import _SYMBOL_MAP as _FUT_MAP
                            _be_pair = _FUT_MAP.get(asset)
                            if _be_pair and hasattr(_brk, "place_stop_market_order"):
                                new_sid = await _brk.place_stop_market_order(
                                    _be_pair, pos.size, _be_side, pos.entry_price)
                            else:
                                new_sid = None
                            if new_sid:
                                pos.stop_order_id = new_sid
                                self._fire(self._persist_stop_order_id(pos))
                                logger.info("trail_breakeven_stop_placed", asset=asset,
                                            entry=pos.entry_price, gain_from_entry=round(gain_from_entry, 4))
                        except Exception as _te:
                            logger.warning("trail_breakeven_stop_failed", asset=asset, error=str(_te))
                        self._trail_activated.add(asset)

                    if trail_triggered:
                        if getattr(pos, "is_live", False):
                            # Sprint 57: cancel resting stop before software-side close.
                            if getattr(pos, 'stop_order_id', None):
                                try:
                                    await broker_factory.get_broker(getattr(pos, "exchange", ""), self._portfolio, pos.direction).cancel_stop_order(asset, pos.stop_order_id)
                                    pos.stop_order_id = None
                                    self._fire(self._persist_stop_order_id(pos))
                                except Exception as _e:
                                    logger.warning(f"Failed to cancel resting stop for {asset}: {_e}")
                            broker = broker_factory.get_broker(getattr(pos, "exchange", ""), self._portfolio, pos.direction)
                            res = await broker.close_position(asset, pos.size, pos.direction, full_exit=True)
                            if res.success and res.fill_price:
                                current_price = res.fill_price
                            elif not res.success:
                                logger.error("live_trail_stop_close_failed", asset=asset, error=res.error)
                                self._fire(self._notifier.send_message(
                                    f"🚨 LIVE CLOSE FAILED (trail stop): {asset} — kept open for retry "
                                    f"({res.error})"))
                                continue  # keep tracked; retry next tick
                        closed = self._portfolio.close_position(asset, current_price)
                        if closed:
                            self._position_peaks.pop(asset, None)
                            self._position_stops.pop(asset, None)
                            self._fire(self._redis.hdel(_STOPS_KEY, asset))
                            self._partial_exits.pop(asset, None)
                            self._fire(self._persist_partial_exits(asset, set()))
                            await self._persist_close_trade(closed, "trail_stop")
                            await self._notifier.trade_closed(closed, "trail_stop")
                            if closed.pnl and closed.pnl > 0:
                                self._asset_stop_strikes.pop(asset, None)
                                self._fire(self._persist_strike(asset, 0))
                            logger.info(
                                "trade_closed",
                                signal_id=closed.signal_id, asset=asset,
                                direction=closed.direction,
                                entry_price=round(closed.entry_price, 4),
                                exit_price=round(closed.exit_price, 4),
                                pnl=round(closed.pnl, 4), fee=round(closed.fee, 4),
                                peak_price=round(peak, 4), reason="trail_stop",
                                mode="live" if getattr(closed, "is_live", False) else _DB_PAPER_MODE,
                            )
                            self._log_portfolio_update(closed.signal_id, asset, 0.0)
                        continue

                    # ── 2b. No-momentum scratch-exit (crypto shorts only) ──────
                    # Backtest: shorts with MFE < 0.2% after 2 h are 45%-WR losers.
                    # Close at market to recover dead-weight capital.
                    if (self._no_momentum_hours > 0
                            and pos.direction == "short"
                            and getattr(pos, "exchange", "") == "CRYPTO"):
                        _nm_hold_h = (
                            (datetime.now(timezone.utc) - pos.opened_at).total_seconds()
                            / 3600.0
                        )
                        if (
                            _nm_hold_h >= self._no_momentum_hours
                            and gain_from_entry < self._no_momentum_min_mfe
                        ):
                            exit_price = current_price
                            if getattr(pos, "is_live", False):
                                if getattr(pos, "stop_order_id", None):
                                    try:
                                        await broker_factory.get_broker(
                                            getattr(pos, "exchange", ""),
                                            self._portfolio, pos.direction,
                                        ).cancel_stop_order(asset, pos.stop_order_id)
                                        pos.stop_order_id = None
                                        self._fire(self._persist_stop_order_id(pos))
                                    except Exception as _e:
                                        logger.warning(
                                            f"Failed to cancel resting stop for {asset}: {_e}"
                                        )
                                broker = broker_factory.get_broker(
                                    getattr(pos, "exchange", ""), self._portfolio, pos.direction
                                )
                                res = await broker.close_position(
                                    asset, pos.size, pos.direction, full_exit=True, maker=True
                                )
                                if res.success and res.fill_price:
                                    exit_price = res.fill_price
                                elif not res.success:
                                    logger.error(
                                        "live_no_momentum_close_failed",
                                        asset=asset, error=res.error,
                                    )
                                    self._fire(self._notifier.send_message(
                                        f"🚨 LIVE CLOSE FAILED (no momentum): {asset} — "
                                        f"kept open for retry ({res.error})"
                                    ))
                                    continue  # keep tracked; retry next tick
                            closed = self._portfolio.close_position(asset, exit_price)
                            if closed:
                                self._position_peaks.pop(asset, None)
                                self._position_stops.pop(asset, None)
                                self._fire(self._redis.hdel(_STOPS_KEY, asset))
                                self._partial_exits.pop(asset, None)
                                self._fire(self._persist_partial_exits(asset, set()))
                                await self._persist_close_trade(closed, "no_momentum")
                                await self._notifier.trade_closed(closed, "no_momentum")
                                logger.info(
                                    "trade_closed",
                                    signal_id=closed.signal_id, asset=asset,
                                    direction=closed.direction,
                                    entry_price=round(closed.entry_price, 4),
                                    exit_price=round(exit_price, 4),
                                    pnl=round(closed.pnl, 4), fee=round(closed.fee, 4),
                                    hold_h=round(_nm_hold_h, 2),
                                    mfe=round(gain_from_entry, 4),
                                    reason="no_momentum",
                                    mode="live" if getattr(closed, "is_live", False) else _DB_PAPER_MODE,
                                )
                                self._log_portfolio_update(closed.signal_id, asset, 0.0)
                            continue

                    # ── 3. Hard stop loss ─────────────────────────────────────
                    if True:
                        _exch = getattr(pos, "exchange", "")
                        _base_stop = self._crypto_stop_loss_pct if _exch == "CRYPTO" else self._stop_loss_pct
                        stop_pct = max(self._position_stops.get(asset, _base_stop), _base_stop)
                        if gain_pct < -stop_pct:
                            await self._notifier.stop_loss_fired(pos, current_price, gain_pct)
                            logger.warning(
                                "stop_loss_triggered", asset=asset, direction=pos.direction,
                                entry_price=round(pos.entry_price, 4),
                                current_price=round(current_price, 4),
                                loss_pct=round(gain_pct * 100, 2),
                                threshold_pct=round(stop_pct * 100, 2),
                            )
                            if getattr(pos, "is_live", False):
                                # Sprint 57: cancel resting stop before software-side close.
                                if getattr(pos, 'stop_order_id', None):
                                    try:
                                        await broker_factory.get_broker(getattr(pos, "exchange", ""), self._portfolio, pos.direction).cancel_stop_order(asset, pos.stop_order_id)
                                        pos.stop_order_id = None
                                        self._fire(self._persist_stop_order_id(pos))
                                    except Exception as _e:
                                        logger.warning(f"Failed to cancel resting stop for {asset}: {_e}")
                                broker = broker_factory.get_broker(getattr(pos, "exchange", ""), self._portfolio, pos.direction)
                                res = await broker.close_position(asset, pos.size, pos.direction, full_exit=True)
                                if res.success and res.fill_price:
                                    current_price = res.fill_price
                                elif not res.success:
                                    logger.error("live_stop_loss_close_failed", asset=asset, error=res.error)
                                    self._fire(self._notifier.send_message(
                                        f"🚨 LIVE CLOSE FAILED (stop loss): {asset} — kept open for retry "
                                        f"({res.error})"))
                                    continue  # keep tracked; retry next tick
                            closed = self._portfolio.close_position(asset, current_price)
                            if closed:
                                self._position_peaks.pop(asset, None)
                                self._position_stops.pop(asset, None)
                                self._fire(self._redis.hdel(_STOPS_KEY, asset))
                                self._partial_exits.pop(asset, None)
                                self._fire(self._persist_partial_exits(asset, set()))
                                await self._persist_close_trade(closed, "stop_loss")
                                await self._notifier.trade_closed(closed, "stop_loss")
                                strikes = self._asset_stop_strikes.get(asset, 0) + 1
                                self._asset_stop_strikes[asset] = strikes
                                self._fire(self._persist_strike(asset, strikes))
                                if strikes >= 2:
                                    self._asset_cooldown_until[asset] = time.monotonic() + 86400
                                    self._fire(self._persist_cooldown(asset, 86400.0))
                                    logger.warning("asset_cooldown_activated", asset=asset, strikes=strikes)
                                logger.info(
                                    "trade_closed",
                                    signal_id=closed.signal_id, asset=asset,
                                    direction=closed.direction,
                                    entry_price=round(closed.entry_price, 4),
                                    exit_price=round(closed.exit_price, 4),
                                    pnl=round(closed.pnl, 4), fee=round(closed.fee, 4),
                                    reason="stop_loss",
                                    mode="live" if getattr(closed, "is_live", False) else _DB_PAPER_MODE,
                                )
                                self._log_portfolio_update(closed.signal_id, asset, 0.0)
                            continue

                    # ── 3.5 Regime-conflict forced exit ───────────────────────
                    # Close long positions that are counter-regime after 6h if losing 2%+.
                    # Shorts are intentionally excluded: OOS lift is measured at 24h without
                    # this early exit; cutting shorts in risk_on prevents the edge from
                    # playing out. Time-stop (block 3.7) at half max_hold covers bad shorts.
                    _age_h = (datetime.now(timezone.utc) - pos.opened_at).total_seconds() / 3600.0
                    _conflict = (
                        pos.direction == "long"
                        and self._macro_regime in ("risk_off", "tightening", "volatility_regime")
                    )
                    _regime_stable_min = 30.0 / 60.0  # 30 minutes in hours
                    _regime_age_h = (time.monotonic() - self._regime_since) / 3600.0
                    if _conflict and _age_h > 6 and gain_pct < -0.02 and _regime_age_h > _regime_stable_min:
                        if getattr(pos, "is_live", False):
                            if getattr(pos, "stop_order_id", None):
                                try:
                                    await broker_factory.get_broker(_exch, self._portfolio, pos.direction).cancel_stop_order(asset, pos.stop_order_id)
                                    pos.stop_order_id = None
                                    self._fire(self._persist_stop_order_id(pos))
                                except Exception as _e:
                                    logger.warning(f"Failed to cancel resting stop for {asset}: {_e}")
                            broker = broker_factory.get_broker(_exch, self._portfolio, pos.direction)
                            res = await broker.close_position(asset, pos.size, pos.direction, full_exit=True)
                            if res.success and res.fill_price:
                                current_price = res.fill_price
                            elif not res.success:
                                logger.error("live_regime_conflict_close_failed", asset=asset, error=res.error)
                                self._fire(self._notifier.send_message(
                                    f"🚨 LIVE CLOSE FAILED (regime_conflict): {asset} — kept open ({res.error})"))
                                continue
                        closed = self._portfolio.close_position(asset, current_price)
                        if closed:
                            self._position_peaks.pop(asset, None)
                            self._position_stops.pop(asset, None)
                            self._fire(self._redis.hdel(_STOPS_KEY, asset))
                            self._partial_exits.pop(asset, None)
                            self._fire(self._persist_partial_exits(asset, set()))
                            await self._persist_close_trade(closed, "regime_conflict")
                            await self._notifier.trade_closed(closed, "regime_conflict")
                            logger.info(
                                "trade_closed", signal_id=closed.signal_id, asset=asset,
                                direction=closed.direction,
                                entry_price=round(closed.entry_price, 4),
                                exit_price=round(closed.exit_price, 4),
                                pnl=round(closed.pnl, 4), fee=round(closed.fee, 4),
                                reason="regime_conflict",
                                macro_regime=self._macro_regime,
                                age_h=round(_age_h, 1),
                                mode="live" if getattr(closed, "is_live", False) else _DB_PAPER_MODE,
                            )
                            self._log_portfolio_update(closed.signal_id, asset, 0.0)
                        continue

                    # ── 3.7 Time-stop-at-loss ─────────────────────────────────
                    # After 75% of max_hold has elapsed with a real loss (>3%),
                    # cut. 14d data: 60%/-2% fired 11x at 0% WR / -$29.19,
                    # cutting trades before partial_t1/t2 targets (100% WR).
                    _half_hold = pos.max_hold_hours * 0.75
                    if _age_h > _half_hold and gain_pct < -0.03:
                        if getattr(pos, "is_live", False):
                            if getattr(pos, "stop_order_id", None):
                                try:
                                    await broker_factory.get_broker(_exch, self._portfolio, pos.direction).cancel_stop_order(asset, pos.stop_order_id)
                                    pos.stop_order_id = None
                                    self._fire(self._persist_stop_order_id(pos))
                                except Exception as _e:
                                    logger.warning(f"Failed to cancel resting stop for {asset}: {_e}")
                            broker = broker_factory.get_broker(_exch, self._portfolio, pos.direction)
                            res = await broker.close_position(asset, pos.size, pos.direction, full_exit=True)
                            if res.success and res.fill_price:
                                current_price = res.fill_price
                            elif not res.success:
                                logger.error("live_time_stop_close_failed", asset=asset, error=res.error)
                                self._fire(self._notifier.send_message(
                                    f"⚠️ LIVE CLOSE FAILED (time_stop): {asset} — kept open ({res.error})"))
                                continue
                        closed = self._portfolio.close_position(asset, current_price)
                        if closed:
                            self._position_peaks.pop(asset, None)
                            self._position_stops.pop(asset, None)
                            self._fire(self._redis.hdel(_STOPS_KEY, asset))
                            self._partial_exits.pop(asset, None)
                            self._fire(self._persist_partial_exits(asset, set()))
                            await self._persist_close_trade(closed, "time_stop_loss")
                            await self._notifier.trade_closed(closed, "time_stop_loss")
                            logger.info(
                                "trade_closed", signal_id=closed.signal_id, asset=asset,
                                direction=closed.direction,
                                entry_price=round(closed.entry_price, 4),
                                exit_price=round(closed.exit_price, 4),
                                pnl=round(closed.pnl, 4), fee=round(closed.fee, 4),
                                reason="time_stop_loss",
                                age_h=round(_age_h, 1),
                                gain_pct=round(gain_pct * 100, 3),
                                half_hold_h=_half_hold,
                                mode="live" if getattr(closed, "is_live", False) else _DB_PAPER_MODE,
                            )
                            self._log_portfolio_update(closed.signal_id, asset, 0.0)
                        continue

                # ── 4. Age-out (lowest priority; runs even with stale price) ───
                if asset in aged_assets:
                    exit_price = current_price if current_price > 0 else pos.entry_price
                    if getattr(pos, "is_live", False):
                        # Sprint 57: cancel resting stop before software-side close.
                        if getattr(pos, 'stop_order_id', None):
                            try:
                                await broker_factory.get_broker(getattr(pos, "exchange", ""), self._portfolio, pos.direction).cancel_stop_order(asset, pos.stop_order_id)
                                pos.stop_order_id = None
                                self._fire(self._persist_stop_order_id(pos))
                            except Exception as _e:
                                logger.warning(f"Failed to cancel resting stop for {asset}: {_e}")
                        broker = broker_factory.get_broker(getattr(pos, "exchange", ""), self._portfolio, pos.direction)
                        # max_hold is a soft TIME exit (not a price stop), so rest a
                        # post-only maker leg first to pay 0.02% instead of 0.05% — the
                        # helper MARKET-takers any unfilled remainder in the same call,
                        # so the position still fully closes within MAKER_EXIT_WAIT_SEC.
                        # ponytail: reuse the TP maker timeout (10s); add a separate
                        # AGEOUT_MAKER_WAIT_SEC only if 10s proves too long for time exits.
                        res = await broker.close_position(asset, pos.size, pos.direction, full_exit=True, maker=True)
                        if res.success and res.fill_price:
                            exit_price = res.fill_price
                        elif not res.success:
                            logger.error("live_age_out_close_failed", asset=asset, error=res.error)
                            self._fire(self._notifier.send_message(
                                f"🚨 LIVE CLOSE FAILED (max hold): {asset} — kept open for retry "
                                f"({res.error})"))
                            continue  # keep tracked; retry next tick
                    closed = self._portfolio.close_position(asset, exit_price)
                    if closed:
                        self._position_peaks.pop(asset, None)
                        self._position_stops.pop(asset, None)
                        self._fire(self._redis.hdel(_STOPS_KEY, asset))
                        self._partial_exits.pop(asset, None)
                        self._fire(self._persist_partial_exits(asset, set()))
                        await self._persist_close_trade(closed, "max_hold_exceeded")
                        await self._notifier.trade_closed(closed, "max_hold_exceeded")
                        logger.info(
                            "trade_closed",
                            signal_id=closed.signal_id, asset=asset, confidence=0.0,
                            direction=closed.direction,
                            entry_price=round(closed.entry_price, 4),
                            exit_price=round(closed.exit_price, 4),
                            pnl=round(closed.pnl, 4), fee=round(closed.fee, 4),
                            reason="max_hold_exceeded",
                            mode="live" if getattr(closed, "is_live", False) else _DB_PAPER_MODE,
                        )
                        self._log_portfolio_update(closed.signal_id, asset, 0.0)
            except Exception as exc:
                logger.error(
                    "check_positions_error",
                    asset=asset,
                    error=str(exc),
                )

        # Prune trail_activated for any positions that closed during this cycle
        self._trail_activated &= set(self._portfolio.open_positions.keys())

    # ── Price refresh (mark-to-market) ───────────────────────────────────────

    async def _refresh_portfolio_prices(self) -> None:
        """Fetch latest prices (and their timestamps) for all open positions in one batched query.

        Stores results in self._portfolio._live_prices (price) and self._live_price_ts
        (dict[str, datetime]) so _check_positions can detect stale quotes before
        triggering stop/trail/partial exits.
        """
        if not self._portfolio.open_positions:
            return
        symbols = list(self._portfolio.open_positions.keys())
        try:
            async with self._db.acquire() as conn:
                rows = await conn.fetch(
                    """
                    SELECT DISTINCT ON (symbol) symbol, price, ts
                      FROM ticker_quotes
                     WHERE symbol = ANY($1) AND price > 0
                     ORDER BY symbol, ts DESC
                    """,
                    symbols,
                )
            prices: dict[str, float] = {}
            for r in rows:
                if r["price"]:
                    prices[r["symbol"]] = float(r["price"])
                    ts = r["ts"]
                    if ts is not None and ts.tzinfo is None:
                        ts = ts.replace(tzinfo=timezone.utc)
                    self._live_price_ts[r["symbol"]] = ts
            if prices:
                self._portfolio.update_prices(prices)
                # Also keep VIX current from this same price data if VIX is in set
                if "VIX" in prices:
                    self._vix_price = prices["VIX"]
        except Exception as exc:
            logger.warning("batch_price_refresh_failed", error=str(exc))

    # ── Price resolution ──────────────────────────────────────────────────────

    async def _get_real_price(self, asset: str, fallback: float = 0.0) -> tuple[float, datetime | None]:
        """Return (price, ts) from ticker_quotes — most recent row for this symbol.

        Falls back (in order):
          1. fallback argument (e.g. entry_price from the position) with ts=None
          2. (0.0, None) — callers guard against price=0

        The _latest_score fallback is intentionally removed: composite_score is not
        a USD price and using it for stop-loss comparisons triggers false closes.

        ts is returned so callers can detect stale quotes before acting on the price.
        """
        try:
            async with self._db.acquire() as conn:
                row = await conn.fetchrow(
                    """
                    SELECT price, ts FROM ticker_quotes
                     WHERE symbol = $1
                     ORDER BY ts DESC
                     LIMIT 1
                    """,
                    asset,
                )
            if row and row["price"]:
                ts = row["ts"]
                if ts is not None and ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
                return float(row["price"]), ts
        except Exception as exc:
            logger.warning("real_price_fetch_failed", asset=asset, error=str(exc))

        if fallback and fallback > 0:
            return fallback, None

        return 0.0, None

    async def _latest_composite_score(self, asset: str) -> float:
        """Return the most recent composite_score from signals_registry (NOT a price).
        Returns 50.0 (mid-range neutral) when no data is available.
        Do NOT use this as a price proxy — composite_score is on a 0-100 scale."""
        try:
            async with self._db.acquire() as conn:
                row = await conn.fetchrow(
                    """
                    SELECT composite_score FROM signals_registry
                     WHERE ticker = $1 AND composite_score IS NOT NULL
                     ORDER BY emitted_at DESC LIMIT 1
                    """,
                    asset,
                )
            if row and row["composite_score"]:
                return float(row["composite_score"])
        except Exception:
            pass
        return 50.0

    # ── Volatility & strategy quality ────────────────────────────────────────

    async def _get_realized_vol(self, asset: str) -> float:
        """Return realized per-period return stdev from last 20 ticker_quotes prices (0 = no data)."""
        try:
            async with self._db.acquire() as conn:
                rows = await conn.fetch(
                    "SELECT price FROM ticker_quotes WHERE symbol = $1 AND price > 0 ORDER BY ts DESC LIMIT 25",
                    asset,
                )
            prices = [float(r["price"]) for r in rows]
            if len(prices) < 5:
                return 0.0
            returns = [(prices[i] - prices[i + 1]) / prices[i + 1] for i in range(len(prices) - 1)]
            n = len(returns)
            mean = sum(returns) / n
            variance = sum((r - mean) ** 2 for r in returns) / max(1, n - 1)
            return variance ** 0.5
        except Exception:
            return 0.0

    async def _refresh_strategy_quality(self) -> None:
        """Load per-engine quality scores from signal_edge_aggregations (Phase D)."""
        if time.monotonic() - self._quality_fetched_at < _QUALITY_CACHE_TTL:
            return
        try:
            async with self._db.acquire() as conn:
                rows = await conn.fetch(
                    """
                    SELECT engine_name, AVG(quality_score) AS avg_quality
                      FROM signal_edge_aggregations
                     WHERE computed_at > NOW() - INTERVAL '48 hours'
                       AND sample_size >= 10
                     GROUP BY engine_name
                    """
                )
            self._strategy_quality = {r["engine_name"]: float(r["avg_quality"]) for r in rows}
            self._quality_fetched_at = time.monotonic()
            if self._strategy_quality:
                logger.info("strategy_quality_loaded",
                            count=len(self._strategy_quality),
                            scores={k: round(v, 3) for k, v in self._strategy_quality.items()})
        except Exception as exc:
            logger.warning("strategy_quality_fetch_failed", error=str(exc))

    # ── Calibration error cache ───────────────────────────────────────────────

    async def _refresh_calibration_errors(self) -> None:
        """Load per-engine calibration_error from signal_scores (most recent row per engine)."""
        if time.monotonic() - self._calib_fetched_at < _CALIB_CACHE_TTL:
            return
        try:
            async with self._db.acquire() as conn:
                rows = await conn.fetch(
                    """
                    SELECT DISTINCT ON (engine_name)
                        engine_name, calibration_error
                      FROM signal_scores
                     WHERE calibration_error IS NOT NULL
                       AND window_hours <= 6
                     ORDER BY engine_name, computed_at DESC, window_hours ASC
                    """
                )
            self._calibration_errors = {
                r["engine_name"]: float(r["calibration_error"]) for r in rows
            }
            self._calib_fetched_at = time.monotonic()
            if self._calibration_errors:
                drifted = {k: round(v, 3) for k, v in self._calibration_errors.items() if v > 0.3}
                if drifted:
                    logger.info("calibration_errors_loaded", drifted=drifted)
        except Exception as exc:
            logger.warning("calibration_fetch_failed", error=str(exc))

    # ── Pillar 2: Autonomous Engine Lifecycle ─────────────────────────────────

    async def _refresh_engine_exclusions(self) -> None:
        """Auto-exclude engines with poor signal accuracy; readmit when they recover.

        Criteria for exclusion: win_rate < 0.45 AND quality_score < 0.30 over ≥20 samples.
        Criteria for readmission: win_rate > 0.55 AND quality_score > 0.35 over fresh ≥20 samples.
        Hardcoded _EXCLUDED_ENGINES are never readmitted by this method.
        State persists in Redis set trading:excluded_engines across restarts.
        """
        if time.monotonic() - self._engine_excl_fetched_at < _ENGINE_EXCLUSION_TTL:
            return
        self._engine_excl_fetched_at = time.monotonic()
        try:
            # signal_edge_metrics rows are per (ticker, direction, window_hours) slice.
            # Judging an engine on a single arbitrary slice (the old DISTINCT ON pattern)
            # excluded TrendDetectionEngine based on one bad ticker/window combination.
            # Aggregate sample-weighted across slices, restricted to window_hours <= 24
            # — use 24h horizon matching router/OOS evaluation window.
            async with self._db.acquire() as conn:
                rows = await conn.fetch(
                    """
                    SELECT engine_name,
                           SUM(win_rate * sample_size) / NULLIF(SUM(sample_size), 0)      AS win_rate,
                           SUM(quality_score * sample_size) / NULLIF(SUM(sample_size), 0) AS quality_score,
                           SUM(sample_size) AS sample_size
                      FROM signal_edge_metrics
                     WHERE computed_at > NOW() - INTERVAL '48 hours'
                       AND window_hours <= 24
                       AND sample_size >= 5
                       AND win_rate IS NOT NULL
                     GROUP BY engine_name
                    HAVING SUM(sample_size) >= 20
                    """
                )
            currently_excluded: set[str] = set(await self._redis.smembers(_DYNAMIC_EXCLUSIONS_KEY) or [])
            newly_excluded: list[str] = []
            newly_readmitted: list[str] = []

            # Self-heal: protected engines must never sit in the Redis exclusion set
            # (e.g. excluded by a previous version of this method).
            for engine in _PROTECTED_ENGINES & currently_excluded:
                await self._redis.srem(_DYNAMIC_EXCLUSIONS_KEY, engine)
                currently_excluded.discard(engine)
                logger.warning("protected_engine_restored", engine=engine)
                await self._notifier.send_message(
                    f"🛡️ Protected engine restored: `{engine}` — removed from auto-exclusion set."
                )

            for r in rows:
                engine = r["engine_name"]
                if engine in _EXCLUDED_ENGINES:
                    continue  # hardcoded exclusions are permanent
                if engine in _PROTECTED_ENGINES:
                    continue  # never auto-excluded
                wr = float(r["win_rate"] or 0.0)
                qs = float(r["quality_score"] or 0.0)

                if engine in currently_excluded:
                    # Re-evaluate: readmit if win rate recovered (qs is noisy at short horizons)
                    if wr > 0.50:
                        await self._redis.srem(_DYNAMIC_EXCLUSIONS_KEY, engine)
                        currently_excluded.discard(engine)
                        newly_readmitted.append(engine)
                else:
                    # Candidate for exclusion
                    if wr < 0.45 and qs < 0.30:
                        await self._redis.sadd(_DYNAMIC_EXCLUSIONS_KEY, engine)
                        currently_excluded.add(engine)
                        newly_excluded.append(engine)

            self._dynamic_excluded_engines = currently_excluded

            for engine in newly_excluded:
                logger.warning("engine_auto_excluded", engine=engine)
                await self._notifier.send_message(
                    f"⚙️ Engine auto-excluded: `{engine}`\n"
                    f"Accuracy / quality below threshold — signals suppressed."
                )
            for engine in newly_readmitted:
                logger.info("engine_auto_readmitted", engine=engine)
                await self._notifier.send_message(
                    f"✅ Engine readmitted: `{engine}`\n"
                    f"Accuracy recovered — signals re-enabled."
                )
            if self._dynamic_excluded_engines:
                logger.info("dynamic_engine_exclusions", excluded=list(self._dynamic_excluded_engines))
        except Exception as exc:
            logger.warning("engine_exclusion_refresh_failed", error=str(exc))

    # ── Pillar 3: Autonomous Asset Lifecycle ──────────────────────────────────

    async def _refresh_asset_bans(self) -> None:
        """Auto-ban assets with sustained losses; unban after 7 days if performance improves.

        Ban trigger (any one): WR < 25% with ≥8 trades, OR total PnL < -$30 over last 20 trades.
        Unban trigger: WR > 40% over new sample of ≥5 trades since ban date,
                       OR 7 days elapsed with no new trades at all.
        .env BANNED_ASSETS are never modified by this method.
        """
        if time.monotonic() - self._asset_lifecycle_fetched_at < _ASSET_LIFECYCLE_TTL:
            return
        self._asset_lifecycle_fetched_at = time.monotonic()
        import json as _json
        import os as _os

        static_banned = {a.strip().upper() for a in _os.getenv("BANNED_ASSETS", "").split(",") if a.strip()}

        try:
            async with self._db.acquire() as conn:
                rows = await conn.fetch(
                    """
                    SELECT asset,
                           COUNT(*) AS total,
                           COUNT(*) FILTER (WHERE pnl > 0) AS wins,
                           SUM(pnl) AS total_pnl,
                           MAX(last_closed) AS last_trade_at
                      FROM (
                          SELECT signal_id, asset, SUM(pnl) AS pnl,
                                 MAX(closed_at) AS last_closed
                            FROM paper_trades
                           WHERE close_reason != 'dedup_cleanup'
                             AND pnl IS NOT NULL
                             AND closed_at IS NOT NULL
                           GROUP BY signal_id, asset
                      ) positions
                     GROUP BY asset
                    HAVING COUNT(*) >= 8
                    """
                )

            now_utc = datetime.now(timezone.utc)
            for r in rows:
                asset = r["asset"].upper()
                if asset in static_banned:
                    continue  # never override .env bans
                total = int(r["total"])
                wins  = int(r["wins"])
                wr = wins / total
                total_pnl = float(r["total_pnl"] or 0.0)

                current_ban = await self._redis.hget(_DYNAMIC_BANS_KEY, asset)

                if current_ban:
                    # Check unban conditions
                    try:
                        ban_info = _json.loads(current_ban)
                        ban_ts = ban_info.get("ts", 0)
                    except Exception:
                        ban_ts = 0

                    days_since_ban = (time.time() - ban_ts) / 86400
                    if wr > 0.40 and total >= 5:
                        await self._redis.hdel(_DYNAMIC_BANS_KEY, asset)
                        logger.info("asset_auto_unbanned", asset=asset, wr=round(wr, 3))
                        await self._notifier.send_message(
                            f"✅ `{asset}` auto-unbanned — WR recovered to {wr:.0%} ({total} trades)"
                        )
                    elif days_since_ban >= 7 and total == 0:
                        await self._redis.hdel(_DYNAMIC_BANS_KEY, asset)
                        logger.info("asset_ban_expired", asset=asset)
                else:
                    # Check ban conditions
                    if wr < 0.25 or total_pnl < -30.0:
                        reason = (
                            f"WR {wr:.0%} / PnL ${total_pnl:+.2f} over {total} trades"
                        )
                        ban_payload = _json.dumps({"reason": reason, "ts": time.time()})
                        await self._redis.hset(_DYNAMIC_BANS_KEY, asset, ban_payload)
                        logger.warning("asset_auto_banned", asset=asset, wr=round(wr, 3),
                                       total_pnl=round(total_pnl, 2))
                        await self._notifier.send_message(
                            f"🚫 `{asset}` auto-banned — {reason}"
                        )
        except Exception as exc:
            logger.warning("asset_lifecycle_refresh_failed", error=str(exc))

    # ── Pillar 4: Regime-Adaptive Parameter Profiles ──────────────────────────

    async def _refresh_regime_profile(self) -> None:
        """Select bull/bear/neutral parameter profile based on macro regime + VIX.

        Profile baselines shift all parameters together; walk-forward tuner then
        applies ±0.02 nudges on top. Profile transition blends 25% new / 75% current
        to avoid sudden threshold jumps.
        """
        if time.monotonic() - self._regime_profile_fetched_at < _REGIME_PROFILE_TTL:
            return
        self._regime_profile_fetched_at = time.monotonic()
        try:
            if self._macro_regime in ("risk_on",) and self._vix_price < 20.0:
                new_profile = "bull"
            elif self._macro_regime in ("risk_off", "tightening", "volatility_regime") or self._vix_price > 25.0:
                new_profile = "bear"
            else:
                new_profile = "neutral"

            if new_profile == self._regime_profile:
                return

            # Dwell gate: hold the current profile for at least 2h before switching again.
            elapsed = time.monotonic() - self._regime_profile_changed_at
            if self._regime_profile_changed_at > 0 and elapsed < 7200:
                logger.debug("regime_profile_dwell", current=self._regime_profile,
                             candidate=new_profile, elapsed_s=round(elapsed))
                return

            old_profile = self._regime_profile
            self._regime_profile = new_profile
            self._regime_profile_changed_at = time.monotonic()
            profile = _REGIME_PROFILES[new_profile]

            # Blend from the frozen env-loaded anchors so repeated regime flips cannot
            # drift baselines away from governance-configured values.
            self._baseline_long_threshold  = self._anchor_long_threshold  * 0.75 + profile["long_thr"]  * 0.25
            self._baseline_short_threshold = self._anchor_short_threshold * 0.75 + profile["short_thr"] * 0.25
            self._baseline_trail_activate  = self._anchor_trail_activate  * 0.75 + profile["trail_act"] * 0.25
            self._trail_activate_pct = self._baseline_trail_activate

            await self._redis.set(_REGIME_PROFILE_KEY, new_profile)
            logger.info("regime_profile_changed", old=old_profile, new=new_profile,
                        vix=round(self._vix_price, 1), macro=self._macro_regime)
            await self._notifier.send_message(
                f"📊 Regime profile: `{old_profile}` → `{new_profile}`\n"
                f"VIX {self._vix_price:.1f} | Macro: {self._macro_regime}\n"
                f"New baselines — long: {self._baseline_long_threshold:.3f}, "
                f"short: {self._baseline_short_threshold:.3f}"
            )
        except Exception as exc:
            logger.warning("regime_profile_refresh_failed", error=str(exc))

    # ── Funding rate gate ─────────────────────────────────────────────────────

    async def _refresh_funding_rates(self) -> None:
        """Fetch BTC + ETH perpetual funding rates from Binance (public, no auth).

        Funding rate < -0.1% means shorts are paying longs — shorts are over-crowded
        and a squeeze is likely. New short entries are blocked in this condition.
        Cached for 30 minutes (_FUNDING_RATE_TTL). Fails open so a network issue
        never prevents all trading.
        """
        if time.monotonic() - self._funding_rate_fetched_at < _FUNDING_RATE_TTL:
            return
        self._funding_rate_fetched_at = time.monotonic()
        try:
            import aiohttp
            timeout = aiohttp.ClientTimeout(total=8)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                for symbol in ("BTCUSDT", "ETHUSDT"):
                    async with session.get(
                        _FUNDING_RATE_URL, params={"symbol": symbol}
                    ) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            fr = float(data.get("lastFundingRate", 0.0))
                            self._funding_rates[symbol] = fr
            if self._funding_rates:
                logger.info("funding_rates_refreshed",
                            btc=round(self._funding_rates.get("BTCUSDT", 0) * 100, 4),
                            eth=round(self._funding_rates.get("ETHUSDT", 0) * 100, 4))
        except Exception as exc:
            logger.warning("funding_rate_fetch_failed", error=str(exc))
            # Keep stale rates; fail-open (don't clear self._funding_rates)

    async def _poll_funding_payments(self) -> None:
        """Poll Binance FUNDING_FEE income history and store in funding_payments table.

        Runs only in live mode. TTL-gated at 5 minutes. Uses tran_id PK for
        idempotent inserts so overlapping windows are safe.
        """
        _FUNDING_POLL_TTL = 300.0  # 5 minutes
        if not self._live:
            return
        if time.monotonic() - self._funding_poll_at < _FUNDING_POLL_TTL:
            return
        self._funding_poll_at = time.monotonic()
        try:
            from brokers.binance_futures import BinanceFuturesBroker
            broker = BinanceFuturesBroker()
            if not broker._configured:
                return
            async with self._db.acquire() as conn:
                row = await conn.fetchrow(
                    "SELECT EXTRACT(EPOCH FROM MAX(paid_at))::BIGINT * 1000 AS last_ms FROM funding_payments"
                )
            last_ms = int(row["last_ms"]) if row and row["last_ms"] else 0
            if last_ms == 0:
                # first run: look back 48h
                import time as _time
                last_ms = int((_time.time() - 48 * 3600) * 1000)
            else:
                # 1h overlap — PK dedupes duplicates
                last_ms -= 3_600_000
            records = await broker.fetch_funding_income(last_ms)
            if not records:
                return
            async with self._db.acquire() as conn:
                await conn.executemany(
                    """
                    INSERT INTO funding_payments (tran_id, symbol, income, paid_at)
                    VALUES ($1, $2, $3, to_timestamp($4 / 1000.0))
                    ON CONFLICT (tran_id) DO NOTHING
                    """,
                    [
                        (int(r["tranId"]), r["symbol"], float(r["income"]), int(r["time"]))
                        for r in records
                    ],
                )
            logger.info("funding_payments_polled", count=len(records),
                        total_income=round(sum(float(r["income"]) for r in records), 4))
        except Exception as exc:
            logger.warning("funding_payments_poll_failed", error=str(exc))

    async def _refresh_engine_weights(self) -> None:
        """Load per-engine confidence multipliers set by governance_engine.py.

        Redis hash trading:engine_weights: engine_name → float multiplier.
        Values < 1.0 reduce confidence (penalise poor engines).
        Values > 1.0 boost confidence (reward good engines).
        Refreshed every 10 minutes; stale values kept on Redis error.
        """
        _WEIGHTS_TTL = 600
        if time.monotonic() - self._weights_fetched_at < _WEIGHTS_TTL:
            return
        self._weights_fetched_at = time.monotonic()
        try:
            raw = await self._redis.hgetall("trading:engine_weights")
            if raw:
                self._engine_weights = {k: float(v) for k, v in raw.items()}
                logger.debug("engine_weights_loaded", weights=self._engine_weights)
        except Exception as exc:
            logger.warning("engine_weights_fetch_failed", error=str(exc))

    # ── Walk-forward threshold self-tuning (Phase 7) ─────────────────────────

    async def _refresh_thresholds(self) -> None:
        """Raise entry thresholds when a direction is losing; loosen when winning.

        Uses last-48h paper_trades to compute per-direction win rates, then nudges
        _score_long_threshold / _score_short_threshold by ±0.02 to filter weak entries.
        Bounds: [0.10, 0.40] for long; [-0.40, -0.10] for short.
        """
        if time.monotonic() - self._tuning_fetched_at < _TUNING_CACHE_TTL:
            return
        self._tuning_fetched_at = time.monotonic()
        try:
            async with self._db.acquire() as conn:
                # Position-level outcomes: GROUP BY signal_id first so partial
                # exits don't count the same position multiple times.
                rows = await conn.fetch(
                    """
                    SELECT direction,
                           COUNT(*) AS total,
                           COUNT(*) FILTER (WHERE pnl > 0) AS wins
                      FROM (
                          SELECT signal_id, direction, SUM(pnl) AS pnl
                            FROM paper_trades
                           WHERE closed_at > NOW() - INTERVAL '48 hours'
                             AND close_reason != 'dedup_cleanup'
                             AND pnl IS NOT NULL
                             AND ($1::bool = FALSE OR direction = 'short')
                           GROUP BY signal_id, direction
                      ) positions
                     GROUP BY direction
                    """,
                    self._live,
                )
                trail_rows = await conn.fetch(
                    """
                    SELECT AVG(pnl) AS avg_pnl, COUNT(*) AS n
                      FROM paper_trades
                     WHERE close_reason = 'trail_stop'
                       AND closed_at > NOW() - INTERVAL '48 hours'
                       AND pnl IS NOT NULL
                    """
                )
                asset_rows = await conn.fetch(
                    """
                    SELECT asset,
                           COUNT(*) AS total,
                           COUNT(*) FILTER (WHERE pnl > 0) AS wins
                      FROM (
                          SELECT signal_id, asset, SUM(pnl) AS pnl
                            FROM paper_trades
                           WHERE closed_at > NOW() - INTERVAL '30 days'
                             AND close_reason != 'dedup_cleanup'
                             AND pnl IS NOT NULL
                           GROUP BY signal_id, asset
                      ) positions
                     GROUP BY asset
                    HAVING COUNT(*) >= 15
                    """
                )
            directions_seen: set[str] = set()
            for r in rows:
                total = int(r["total"])
                if total < 5:
                    # Insufficient data: decay 10% toward the user-set anchor (not
                    # the regime-blended baseline) so the env-configured threshold is
                    # respected when there is no performance evidence to deviate from it.
                    if r["direction"] == "long":
                        self._score_long_threshold += (
                            (self._anchor_long_threshold - self._score_long_threshold) * 0.10
                        )
                    elif r["direction"] == "short":
                        self._score_short_threshold += (
                            (self._anchor_short_threshold - self._score_short_threshold) * 0.10
                        )
                    continue
                directions_seen.add(r["direction"])
                wr = int(r["wins"]) / total
                # Dampen step for small samples: 6 trades → step=0.006 vs full 0.02 at 20+.
                step = 0.02 * min(1.0, total / 20)
                if r["direction"] == "long":
                    long_drift = abs(self._score_long_threshold - self._baseline_long_threshold)
                    at_cap = self._score_long_threshold >= 0.40 - 1e-6 or long_drift > 0.15
                    if at_cap:
                        self._long_at_cap_cycles += 1
                    else:
                        self._long_at_cap_cycles = 0
                    if self._long_at_cap_cycles >= 2:
                        # Threshold pinned at extreme ≥2 cycles — hard 50% pull toward baseline.
                        self._score_long_threshold += (
                            (self._baseline_long_threshold - self._score_long_threshold) * 0.50
                        )
                        self._long_at_cap_cycles = 0
                    else:
                        if long_drift > 0.08:
                            self._score_long_threshold += (
                                (self._baseline_long_threshold - self._score_long_threshold) * 0.15
                            )
                        if wr < 0.40:   # losing — raise threshold (be more selective)
                            self._score_long_threshold = min(0.40, self._score_long_threshold + step)
                        elif wr > 0.55:  # winning — loosen threshold (catch more signals)
                            self._score_long_threshold = max(0.10, self._score_long_threshold - step)
                elif r["direction"] == "short":
                    short_drift = abs(self._score_short_threshold - self._baseline_short_threshold)
                    at_floor = self._score_short_threshold <= -0.40 + 1e-6 or short_drift > 0.15
                    if at_floor:
                        self._short_at_cap_cycles += 1
                    else:
                        self._short_at_cap_cycles = 0
                    if self._short_at_cap_cycles >= 2:
                        self._score_short_threshold += (
                            (self._baseline_short_threshold - self._score_short_threshold) * 0.50
                        )
                        self._short_at_cap_cycles = 0
                    else:
                        if short_drift > 0.08:
                            self._score_short_threshold += (
                                (self._baseline_short_threshold - self._score_short_threshold) * 0.15
                            )
                        if wr < 0.40:
                            self._score_short_threshold = max(-0.40, self._score_short_threshold - step)
                        elif wr > 0.55:
                            # Ceiling = most permissive of anchor and baseline. Without
                            # the anchor term, min() picks baseline (-0.1125 < anchor
                            # -0.09 numerically) and "loosening" silently tightens when
                            # the anchor is less negative than the blended baseline.
                            ceiling = max(-0.13, self._baseline_short_threshold, self._anchor_short_threshold)
                            self._score_short_threshold = min(ceiling, self._score_short_threshold + step)
            # Also decay any direction absent from the result set entirely.
            # Decay toward anchor (user-set), not baseline (regime-blended), so
            # SCORE_SHORT_THRESHOLD/-LONG_THRESHOLD env vars are respected when
            # there is no performance evidence that justifies a tighter threshold.
            if "long" not in directions_seen:
                self._score_long_threshold += (
                    (self._anchor_long_threshold - self._score_long_threshold) * 0.10
                )
            if "short" not in directions_seen:
                self._score_short_threshold += (
                    (self._anchor_short_threshold - self._score_short_threshold) * 0.10
                )

            # Pillar 1: trail_stop_pct extension — adjust based on quality of trail exits
            if trail_rows and int(trail_rows[0]["n"] or 0) >= 3:
                avg_trail_pnl = float(trail_rows[0]["avg_pnl"] or 0.0)
                if avg_trail_pnl < 0:
                    # Trail is too tight — tightening too early, widen it
                    self._trail_stop_pct = min(0.030, self._trail_stop_pct + 0.001)
                elif avg_trail_pnl > 0:
                    # Trail exits are profitable — decay back toward env baseline
                    self._trail_stop_pct += (self._trail_stop_pct_base - self._trail_stop_pct) * 0.10
            else:
                # Insufficient data — decay toward env baseline
                self._trail_stop_pct += (self._trail_stop_pct_base - self._trail_stop_pct) * 0.10

            # Pillar 1: per-asset threshold relaxation — reward proven winners
            new_asset_thresholds: dict[str, float] = {}
            for r in asset_rows:
                wr = int(r["wins"]) / int(r["total"])
                if wr > 0.55:
                    # Relax threshold: allow slightly weaker signals from proven assets
                    new_asset_thresholds[r["asset"]] = -0.02
                elif wr < 0.35:
                    # Tighten threshold: require stronger signals from losing assets
                    new_asset_thresholds[r["asset"]] = +0.02
            if new_asset_thresholds != self._asset_thresholds:
                self._asset_thresholds = new_asset_thresholds
                logger.info("asset_thresholds_updated", adjustments=new_asset_thresholds)
                # Persist per-asset threshold offsets to Redis so they survive container
                # restarts without waiting for the next 1-hour tuning cycle.
                try:
                    key = "trading:asset_thresholds"
                    await self._redis.delete(key)
                    if new_asset_thresholds:
                        await self._redis.hset(key, mapping={
                            a: str(v) for a, v in new_asset_thresholds.items()
                        })
                except Exception as _exc:
                    logger.warning("asset_thresholds_persist_failed", error=str(_exc))

            logger.info(
                "thresholds_tuned",
                long_threshold=round(self._score_long_threshold, 3),
                short_threshold=round(self._score_short_threshold, 3),
                trail_stop_pct=round(self._trail_stop_pct * 100, 2),
                regime_profile=self._regime_profile,
            )

            # Governance hot-reload: apply any parameter overrides written by governance_engine.py
            # without requiring a container restart.  Keys expire after 24h automatically.
            # param → (baseline_attr, live_attr, anchor_attr, scale)
            # scale converts the raw governance value to the engine's internal representation.
            gov_map = {
                "SCORE_SHORT_THRESHOLD": ("_baseline_short_threshold", "_score_short_threshold", "_anchor_short_threshold", 1.0),
                "SCORE_LONG_THRESHOLD":  ("_baseline_long_threshold",  "_score_long_threshold",  "_anchor_long_threshold",  1.0),
                "TRAIL_ACTIVATE_PCT":    ("_baseline_trail_activate",  "_trail_activate_pct",    "_anchor_trail_activate",   0.01),
                "TRAIL_STOP_PCT":        ("_trail_stop_pct_base",      "_trail_stop_pct",        None,                       0.01),
            }
            for env_key, (baseline_attr, live_attr, anchor_attr, scale) in gov_map.items():
                raw = await self._redis.get(f"{_GOV_OVERRIDE_PREFIX}{env_key}")
                if raw is not None:
                    try:
                        new_val = float(raw) * scale
                        # Cap trail-activate to prevent governance from inflating it above max
                        if live_attr in ("_trail_activate_pct", "_baseline_trail_activate"):
                            new_val = min(new_val, _MAX_TRAIL_ACT_PCT)
                        old_baseline = getattr(self, baseline_attr)
                        if abs(new_val - old_baseline) > 1e-6:
                            setattr(self, baseline_attr, new_val)
                            setattr(self, live_attr, new_val)
                            # Anchor stays pinned to the env-var origin — do not overwrite
                            # it here, or repeated regime transitions will compound-drift
                            # the baseline away from the governance-configured value.
                            logger.info("governance_hot_reload",
                                        param=env_key, old=round(old_baseline, 4),
                                        new=round(new_val, 4))
                    except (ValueError, TypeError):
                        pass

            # Alert when thresholds drift significantly from the regime baseline
            long_drift  = abs(self._score_long_threshold  - self._baseline_long_threshold)
            short_drift = abs(self._score_short_threshold - self._baseline_short_threshold)
            if long_drift > 0.08 or short_drift > 0.08:
                in_mem_ok = time.monotonic() - self._drift_alerted_at >= 86400
                if in_mem_ok:
                    try:
                        first_fire = await self._redis.set(_DRIFT_ALERT_KEY, "1", nx=True, ex=86400)
                    except Exception:
                        first_fire = True  # Redis down → fail open, still alert
                else:
                    first_fire = False
                if first_fire:
                    self._drift_alerted_at = time.monotonic()
                    await self._notifier.send_message(
                        f"⚠️ Threshold drift alert [{self._regime_profile} regime]\n"
                        f"Long:  {self._score_long_threshold:.3f} (baseline {self._baseline_long_threshold:.3f})\n"
                        f"Short: {self._score_short_threshold:.3f} (baseline {self._baseline_short_threshold:.3f})\n"
                        f"Check recent win rates — thresholds may be pinned at an extreme."
                    )

            # Prune cooldowns that have expired (runs hourly with the tuning tick)
            now = time.monotonic()
            expired = [a for a, t in list(self._asset_cooldown_until.items()) if t <= now]
            for a in expired:
                self._asset_cooldown_until.pop(a, None)
                self._asset_stop_strikes.pop(a, None)
                try:
                    await self._redis.hdel(_COOLDOWNS_KEY, a)
                    await self._redis.hdel(_STRIKES_KEY, a)
                except Exception:
                    pass
            if expired:
                logger.info("cooldowns_expired", assets=expired)

        except Exception as exc:
            logger.warning("threshold_tuning_failed", error=str(exc))

    # ── Trend & direction gates ───────────────────────────────────────────────

    async def _is_trend_aligned(self, asset: str, direction: str) -> bool:
        """Block entries that fight the local price trend (20-period SMA on recent quotes).

        Long allowed only when price is at or above 97% of SMA.
        Short allowed only when price is at or below 103% of SMA.
        Returns True (allow) when data is insufficient.
        """
        try:
            async with self._db.acquire() as conn:
                rows = await conn.fetch(
                    "SELECT price FROM ticker_quotes WHERE symbol = $1 AND price > 0 "
                    "ORDER BY ts DESC LIMIT 25",
                    asset,
                )
            if len(rows) < 10:
                return True
            prices = list(reversed([float(r["price"]) for r in rows]))
            # Fixed 20-period window regardless of how many rows were fetched.
            # Using len(prices) produced a 10–25-period SMA depending on data
            # availability, making the ±3% tolerance inconsistent across assets.
            sma = sum(prices[-20:]) / min(20, len(prices))
            current = prices[-1]
            if direction == "long":
                aligned = current >= sma * 0.97
            else:
                aligned = current <= sma * 1.03
            if not aligned:
                logger.debug("trend_misaligned", asset=asset, direction=direction,
                             current=round(current, 4), sma=round(sma, 4))
            return aligned
        except Exception:
            return True

    async def _btc_trend_allows(self, direction: str) -> bool:
        """Require BTC to trend in the same direction before opening any CRYPTO position.

        Uses 50-period hourly SMA (~2 days) — one representative price per hour so the
        SMA reflects a real multi-day trend rather than 4 hours of 5-minute noise.
        Returns True (allow) when BTC data is unavailable.
        """
        try:
            async with self._db.acquire() as conn:
                rows = await conn.fetch(
                    """
                    SELECT price FROM (
                        SELECT DISTINCT ON (date_trunc('hour', ts))
                               price, ts
                          FROM ticker_quotes
                         WHERE symbol = 'BTC' AND price > 0
                         ORDER BY date_trunc('hour', ts) DESC, ts DESC
                    ) hourly
                    ORDER BY ts DESC LIMIT 60
                    """,
                )
            if len(rows) < 20:
                return True
            prices = list(reversed([float(r["price"]) for r in rows]))
            sma = sum(prices[-50:]) / min(50, len(prices))
            current = prices[-1]
            if direction == "long":
                allowed = current >= sma * 0.99
            else:
                allowed = current <= sma * 1.01
            if not allowed:
                logger.debug("btc_gate_blocked", direction=direction,
                             btc_price=round(current, 0), btc_sma50=round(sma, 0))
            return allowed
        except Exception:
            return True

    # ── RSI computation (inline — TechnicalEngine is in a separate service) ─────

    @staticmethod
    def _compute_rsi(prices: list[float], period: int = 14) -> float:
        n = min(period, len(prices) - 1)
        if n < 2:
            return 50.0
        deltas = [prices[i] - prices[i - 1] for i in range(1, len(prices))]
        gains  = [max(0.0, d) for d in deltas[-n:]]
        losses = [abs(min(0.0, d)) for d in deltas[-n:]]
        avg_gain = sum(gains) / n
        avg_loss = sum(losses) / n
        if avg_loss == 0 and avg_gain == 0:
            return 50.0  # no price movement — truly neutral, not overbought
        if avg_loss == 0:
            return 100.0
        return 100.0 - 100.0 / (1.0 + avg_gain / avg_loss)

    async def _get_rsi(self, asset: str) -> float:
        """RSI(14) from recent ticker_quotes. Returns 50 (neutral) on missing data."""
        try:
            async with self._db.acquire() as conn:
                rows = await conn.fetch(
                    "SELECT price FROM ticker_quotes WHERE symbol=$1 AND price>0 "
                    "ORDER BY ts DESC LIMIT 20", asset,
                )
            if len(rows) < 5:
                return 50.0
            return self._compute_rsi(list(reversed([float(r["price"]) for r in rows])))
        except Exception:
            return 50.0

    async def _get_rsi_and_vol(self, asset: str) -> tuple[float, float]:
        """Return (RSI-14, realized_vol) from a single ticker_quotes fetch (25 rows).

        Avoids two separate DB round-trips when both values are needed in the same
        signal evaluation pass. Returns (50.0, 0.0) on missing/error data.
        """
        try:
            async with self._db.acquire() as conn:
                rows = await conn.fetch(
                    "SELECT price FROM ticker_quotes WHERE symbol=$1 AND price>0 "
                    "ORDER BY ts DESC LIMIT 25", asset,
                )
            if len(rows) < 5:
                return 50.0, 0.0
            prices_desc = [float(r["price"]) for r in rows]
            prices_asc  = list(reversed(prices_desc))
            rsi = self._compute_rsi(prices_asc)
            returns = [(prices_desc[i] - prices_desc[i + 1]) / prices_desc[i + 1]
                       for i in range(len(prices_desc) - 1)]
            n = len(returns)
            mean = sum(returns) / n
            vol = (sum((r - mean) ** 2 for r in returns) / max(1, n - 1)) ** 0.5
            return rsi, vol
        except Exception:
            return 50.0, 0.0

    async def _compute_atr_stop(self, asset: str, vol: float | None = None) -> float:
        """ATR-proxy stop-loss: 2.5x realized per-period volatility.

        Adapts stop to market conditions:
          Calm  (vol <0.4%/period) → ~1.0% stop
          Normal(vol  0.5-0.8%)   → ~1.5% stop
          Spiky (vol >1.0%)       → up to 3.0% stop
        Clamped [0.8%, 3.0%] to prevent degenerate cases.

        Pass vol when it's already been fetched (e.g. from _get_rsi_and_vol) to
        avoid a redundant ticker_quotes round-trip.
        """
        if vol is None:
            vol = await self._get_realized_vol(asset)
        if vol <= 0:
            return self._stop_loss_pct
        return max(0.008, min(0.030, vol * 2.5))

    async def _get_kelly_fraction(self, exchange: str, direction: str = "") -> float | None:
        """Half-Kelly position fraction from last 25 trades on this exchange+direction.

        Direction-aware so crypto short winners aren't diluted by disabled long losers.
        Returns None when sample is insufficient (< 10 trades).
        Negative Kelly (no edge) → 1% minimum to stay in the game.
        """
        try:
            # Aggregate by signal_id: partial exits (T1/T2) write separate rows
            # for the same position; counting them as independent trades inflates
            # the win rate (partials are by construction wins).
            async with self._db.acquire() as conn:
                if direction:
                    rows = await conn.fetch(
                        """
                        SELECT SUM(pnl) AS pnl, SUM(notional) AS notional
                        FROM paper_trades
                        WHERE closed_at IS NOT NULL
                          AND exchange = $1
                          AND direction = $2
                          AND close_reason NOT IN ('dedup_cleanup')
                          AND pnl IS NOT NULL AND notional > 0
                        GROUP BY signal_id
                        ORDER BY MAX(closed_at) DESC LIMIT 25
                        """, exchange, direction,
                    )
                else:
                    rows = await conn.fetch(
                        """
                        SELECT SUM(pnl) AS pnl, SUM(notional) AS notional
                        FROM paper_trades
                        WHERE closed_at IS NOT NULL
                          AND exchange = $1
                          AND close_reason NOT IN ('dedup_cleanup')
                          AND pnl IS NOT NULL AND notional > 0
                        GROUP BY signal_id
                        ORDER BY MAX(closed_at) DESC LIMIT 25
                        """, exchange,
                    )
            if len(rows) < 10:
                return None
            results = [(float(r["pnl"]), float(r["notional"])) for r in rows]
            pnl_pcts = [p / n for p, n in results]
            wins   = [x for x in pnl_pcts if x > 0]
            losses = [x for x in pnl_pcts if x < 0]
            if not wins or not losses:
                return None
            win_rate = len(wins) / len(results)
            avg_win  = sum(wins) / len(wins)
            avg_loss = abs(sum(losses) / len(losses))
            if avg_loss < 0.001:
                return None
            R = avg_win / avg_loss
            kelly = win_rate - (1.0 - win_rate) / R
            if kelly <= 0:
                return 0.01   # negative edge → minimum bet
            return max(0.01, min(0.08, kelly * 0.5))   # half-Kelly, clamped
        except Exception:
            return None

    async def _is_btc_bull_regime(self) -> bool:
        """Return True when BTC is clearly above its 200-period SMA.

        Uses hourly-bucketed prices so the 200-period SMA spans ~8 days of real
        market data rather than 17 hours of 5-minute CoinGecko polls.
        The DISTINCT ON (hour) pattern picks one representative price per hour
        (the latest quote in that hour), giving a stable 200-hour SMA.

        Hysteresis band:
          Enter BULL : BTC >= SMA × 1.02
          Exit  BULL : BTC <  SMA × 0.99
          In band    : maintain last cached state

        Missing data → fail CLOSED (bear) to protect capital.
        """
        try:
            async with self._db.acquire() as conn:
                rows = await conn.fetch(
                    """
                    SELECT price FROM (
                        SELECT DISTINCT ON (date_trunc('hour', ts))
                               price, ts
                          FROM ticker_quotes
                         WHERE symbol = 'BTC' AND price > 0
                         ORDER BY date_trunc('hour', ts) DESC, ts DESC
                    ) hourly
                    ORDER BY ts DESC
                    LIMIT 210
                    """,
                )
            if len(rows) < 100:
                self._btc_bull_cache = False
                return False
            prices = list(reversed([float(r["price"]) for r in rows]))
            sma200 = sum(prices[-200:]) / min(200, len(prices))
            current = prices[-1]
            ratio = current / sma200

            if ratio >= 1.02:
                self._btc_bull_cache = True
            elif ratio < 0.99:
                self._btc_bull_cache = False
            # else: 0.99–1.02 neutral band → keep last cached state (hysteresis)

            result = self._btc_bull_cache if self._btc_bull_cache is not None else False
            logger.debug("btc_regime", btc=round(current, 0), sma200=round(sma200, 0),
                         ratio=round(ratio, 4), bull=result)
            return result
        except Exception:
            return self._btc_bull_cache if self._btc_bull_cache is not None else False

    async def _get_exchange_performance_scale(self, exchange: str) -> float:
        """Scale position size by recent per-exchange win rate (last 72h).

        Bad streak → smaller bets. Good streak → slightly larger (max 1.2x).
        Only looks at trades from the current trading mode so that paper
        trade history does not poison live position sizing on first startup.
        """
        try:
            current_mode = "live" if self._live else _DB_PAPER_MODE
            async with self._db.acquire() as conn:
                row = await conn.fetchrow(
                    """
                    SELECT COUNT(*) AS n,
                           COUNT(*) FILTER (WHERE pnl > 0) AS wins
                    FROM (
                        SELECT signal_id, SUM(pnl) AS pnl
                          FROM paper_trades
                         WHERE closed_at IS NOT NULL
                           AND exchange = $1
                           AND mode = $2
                           AND close_reason NOT IN ('dedup_cleanup')
                           AND closed_at > NOW() - INTERVAL '72 hours'
                           AND pnl IS NOT NULL
                         GROUP BY signal_id
                    ) positions
                    """, exchange, current_mode,
                )
            n = int(row["n"] or 0)
            if n < 5:
                return 1.0
            wr = int(row["wins"] or 0) / n
            if wr < 0.25:
                return 0.5    # very bad streak → half size
            if wr < 0.35:
                return 0.70
            if wr > 0.60:
                return 1.20   # hot streak → modestly larger
            return 1.0
        except Exception:
            return 1.0

    async def _get_tech_score(self, asset: str) -> float:
        """Fetch latest technical_score from market_trends (0.0 on missing data)."""
        try:
            async with self._db.acquire() as conn:
                row = await conn.fetchrow(
                    "SELECT technical_score FROM market_trends WHERE ticker = $1", asset,
                )
            return float(row["technical_score"]) if row and row["technical_score"] is not None else 0.0
        except Exception:
            return 0.0

    async def _tech_score_allows(self, asset: str, direction: str, exchange: str = "",
                                   cached_tech: float | None = None,
                                   social_score: float = 0.0) -> bool:
        """Require technical_score to actively CONFIRM the signal direction.

        Long:  tech >= +0.10 required.
        Short: base requirement tech <= -0.10, relaxed to <= -0.02 in BTC bear market.
               Further relaxed to <= +0.12 when social_score < -0.50 (social panic
               overrides weak tech — extreme fear correlates with real selling pressure
               even when price hasn't broken down yet on the chart).

        Data: 30-day analysis showed LINK/DOGE shorts with social=-0.70, tech=+0.05
              were blocked despite being genuine setups. DOGE shorts historically
              have 67% WR. Social < -0.50 is a strong secondary confirmation for shorts.

        cached_tech: pass signal.tech_score to avoid a redundant DB fetch.
        Returns True when no data is available (fail-open on missing data only).
        """
        try:
            tech = cached_tech if cached_tech is not None else await self._get_tech_score(asset)
            if tech == 0.0:
                return True  # no data — fail open

            if direction == "long" and tech < 0.10:
                logger.debug("tech_rejects_long", asset=asset, tech=round(tech, 3))
                return False

            if direction == "short":
                if exchange == "CRYPTO" and not await self._is_btc_bull_regime():
                    # Bear market base threshold
                    short_threshold = -0.02
                    # Social panic override: extreme fear (-0.50+) means the crowd is
                    # already selling even if the chart hasn't caught up yet.
                    # Relax up to +0.12 proportionally based on social conviction.
                    if social_score < -0.50:
                        panic_bonus = min(0.14, (abs(social_score) - 0.50) * 0.40)
                        short_threshold = min(0.12, short_threshold + panic_bonus)
                        logger.debug("social_panic_override", asset=asset,
                                     social=round(social_score, 3),
                                     tech_threshold=round(short_threshold, 3))
                else:
                    short_threshold = -0.10
                if tech > short_threshold:
                    logger.debug("tech_rejects_short", asset=asset, tech=round(tech, 3),
                                 threshold=round(short_threshold, 3))
                    return False
            return True
        except Exception:
            return True

    async def _entry_momentum_ok(self, asset: str, direction: str, exchange: str) -> bool:
        """Block entries that are chasing a recent price spike or dump (CRYPTO only).

        CoinGecko polls every 5 min → 7 rows = 35 min window.
        If CRYPTO price moved > 1.5% in 35 min the entry is stale — we're chasing.
        Not applied to stocks (daily candles — 7 rows = 7 days, 1.5% is normal).
        """
        if exchange != "CRYPTO":
            return True
        try:
            async with self._db.acquire() as conn:
                rows = await conn.fetch(
                    "SELECT price FROM ticker_quotes WHERE symbol=$1 AND price>0 "
                    "ORDER BY ts DESC LIMIT 7",
                    asset,
                )
            if len(rows) < 5:
                return True
            prices = [float(r["price"]) for r in rows]  # newest first
            recent, baseline = prices[0], prices[-1]
            pct = (recent - baseline) / baseline
            if direction == "long" and pct > 0.015:    # price pumped >1.5%
                logger.debug("anti_pump", asset=asset, pct=round(pct * 100, 2))
                return False
            if direction == "short" and pct < -0.015:  # price dumped >1.5%
                logger.debug("anti_dump", asset=asset, pct=round(pct * 100, 2))
                return False
            return True
        except Exception:
            return True

    async def _portfolio_heat_ok(self) -> bool:
        """Pause new entries when open positions are already losing > 1.5% of balance.

        Prevents stacking new bets while existing trades are underwater.
        """
        try:
            snap = self._portfolio.snapshot()
            unrealized = float(snap.get("unrealized_pnl", 0))
            balance = float(snap.get("balance", 10000))
            if balance > 0 and unrealized / balance < -0.015:
                logger.info("portfolio_heat_pause",
                            unrealized=round(unrealized, 2),
                            pct=round(unrealized / balance * 100, 2))
                return False
            return True
        except Exception:
            return True

    # ── Multi-timeframe consensus (Phase 4) ──────────────────────────────────

    async def _check_signal_consensus(self, ticker: str, direction: str) -> bool:
        """Return True only if ≥60% of last-6h signals for this ticker agree on direction."""
        try:
            async with self._db.acquire() as conn:
                rows = await conn.fetch(
                    """
                    SELECT direction, composite_score
                      FROM signals_registry
                     WHERE ticker = $1
                       AND emitted_at > NOW() - INTERVAL '6 hours'
                       AND composite_score IS NOT NULL
                       AND engine_name != ALL($2)
                     ORDER BY emitted_at DESC
                     LIMIT 20
                    """,
                    ticker,
                    list(_EXCLUDED_ENGINES) + list(self._dynamic_excluded_engines),
                )
            if len(rows) < 3:
                return True  # insufficient history — let it through

            def _agrees(r) -> bool:
                d = r["direction"] or "neutral"
                score = float(r["composite_score"] or 0.0)
                # Consensus measures directional lean, not entry strength.
                # Small dead-band avoids flat-zero rows counting for either side.
                _BAND = 0.02
                if direction == "long":
                    return d == "risk_on" or (d == "neutral" and score >= _BAND)
                return d == "risk_off" or (d == "neutral" and score <= -_BAND)

            matching = sum(1 for r in rows if _agrees(r))
            agreement = matching / len(rows)
            if agreement < 0.60:
                logger.debug(
                    "consensus_failed",
                    ticker=ticker,
                    direction=direction,
                    agreement=round(agreement, 2),
                    signals=len(rows),
                )
                return False
            return True
        except Exception as exc:
            logger.warning("consensus_check_error", ticker=ticker, error=str(exc))
            return True  # fail-open on DB error

    # ── Macro regime ──────────────────────────────────────────────────────────

    async def _fetch_macro_regime(self) -> str:
        """Read the latest global macro regime and VIX price."""
        try:
            async with self._db.acquire() as conn:
                row = await conn.fetchrow(
                    "SELECT regime FROM macro_regimes ORDER BY computed_at DESC LIMIT 1"
                )
                vix_row = await conn.fetchrow(
                    "SELECT price FROM ticker_quotes WHERE symbol = 'VIX' ORDER BY ts DESC LIMIT 1"
                )
            if vix_row and vix_row["price"]:
                self._vix_price = float(vix_row["price"])
            if row and row["regime"]:
                regime = row["regime"]
                if regime != self._macro_regime:
                    self._regime_since = time.monotonic()
                logger.info("macro_regime_updated", regime=regime, vix=round(self._vix_price, 2))
                return regime
        except Exception as exc:
            logger.warning("macro_regime_fetch_failed", error=str(exc))
        return "unknown"

    # ── Portfolio logging ─────────────────────────────────────────────────────

    def _log_portfolio_update(self, signal_id: str, asset: str, confidence: float) -> None:
        snap = self._portfolio.snapshot()
        logger.info(
            "portfolio_update",
            signal_id=signal_id,
            asset=asset,
            confidence=round(confidence, 4),
            **snap,
        )

    # ── Trade persistence (PostgreSQL) ────────────────────────────────────────

    async def _persist_open_trade(self, trade: PaperTrade) -> None:
        try:
            async with self._db.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO paper_trades
                        (id, signal_id, asset, exchange, direction,
                         entry_price, size, notional, fee, mode, opened_at,
                         router_action, router_edge,
                         stop_order_id, exchange_order_id)
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
                    ON CONFLICT (signal_id) WHERE closed_at IS NULL DO NOTHING
                    """,
                    trade.trade_id, trade.signal_id, trade.asset,
                    trade.exchange or "", trade.direction,
                    trade.entry_price, trade.size, trade.notional, trade.fee,
                    "live" if trade.is_live else _DB_PAPER_MODE,
                    trade.opened_at,
                    trade.router_action or None,
                    trade.router_edge if trade.router_action else None,
                    getattr(trade, "stop_order_id", None),
                    getattr(trade, "exchange_order_id", None),
                )
        except Exception as exc:
            logger.warning("persist_open_trade_failed", asset=trade.asset, error=str(exc))

    async def _persist_stop_order_id(self, trade: PaperTrade) -> None:
        """Update stop/exchange order IDs in DB after they change (partial exits, trail reset)."""
        try:
            async with self._db.acquire() as conn:
                await conn.execute(
                    "UPDATE paper_trades SET stop_order_id=$2, exchange_order_id=$3 WHERE id=$1",
                    trade.trade_id,
                    getattr(trade, "stop_order_id", None),
                    getattr(trade, "exchange_order_id", None),
                )
        except Exception as exc:
            logger.warning("persist_stop_order_id_failed", asset=trade.asset, error=str(exc))

    async def _persist_close_trade(self, trade: PaperTrade, reason: str) -> None:
        trade.close_reason = reason  # expose to Prometheus label in main.py _attach_metrics
        try:
            async with self._db.acquire() as conn:
                # Partial-exit trades are synthetic (fresh UUID, never INSERTed at open).
                # UPSERT covers both cases: INSERT for partial trades, UPDATE for normal closes.
                await conn.execute(
                    """
                    INSERT INTO paper_trades
                        (id, signal_id, asset, exchange, direction,
                         entry_price, size, notional, fee, mode, opened_at,
                         router_action, router_edge,
                         exit_price, pnl, close_reason, closed_at)
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)
                    ON CONFLICT (id) DO UPDATE SET
                        exit_price=$14, fee=$9, pnl=$15,
                        close_reason=$16, closed_at=$17
                    """,
                    trade.trade_id, trade.signal_id, trade.asset,
                    trade.exchange or "", trade.direction,
                    trade.entry_price, trade.size, trade.notional, trade.fee,
                    "live" if trade.is_live else _DB_PAPER_MODE,
                    trade.opened_at,
                    trade.router_action or None,
                    trade.router_edge if trade.router_action else None,
                    trade.exit_price, trade.pnl, reason, trade.closed_at,
                )
        except Exception as exc:
            logger.warning("persist_close_trade_failed", asset=trade.asset, error=str(exc))

        # Sprint 38f: post-trade reflection — LLM lesson stored per closed trade,
        # injected into future audits (TradingAgents-style learning loop).
        self._fire(self._llm_auditor.reflect(trade))
        # Post-mortem: structured verdict for live trades (verdict/lesson/regime_fit).
        self._fire(self._llm_auditor.post_mortem(trade, {"regime": self._macro_regime}))

        # Best-effort signal feedback write
        await self._write_signal_feedback(trade)
        self._fire(self._publish_portfolio_edge())

    async def _write_signal_feedback(self, trade: PaperTrade) -> None:
        """Write trade outcome to signal_feedback and update signal_outcomes (Phase A)."""
        try:
            notional = trade.notional or 1.0
            pnl = trade.pnl or 0.0
            held_h = 0
            if trade.opened_at and trade.closed_at:
                held_h = max(1, int((trade.closed_at - trade.opened_at).total_seconds() / 3600))

            outcome = "win" if pnl > 0 else ("loss" if pnl < 0 else "neutral")
            pnl_pct = round(pnl / notional * 100, 4)
            # signal_outcomes.outcome_score_delta is a DECIMAL return (0.005 = 0.5%),
            # matching the price-based outcome tracker; pnl_score stays in percent.
            pnl_frac = round(pnl / notional, 6)
            # Map trade direction + PnL to actual price direction for signal_outcomes
            if trade.direction == "long":
                actual_dir = "up" if pnl > 0 else "down"
            else:
                actual_dir = "down" if pnl > 0 else "up"
            direction_correct = pnl > 0

            async with self._db.acquire() as conn:
                # 1. Update signal_outcomes.actual_direction for all evaluation windows.
                # NOTE: outcome_score_delta is intentionally NOT written here — it is
                # computed from real price returns by the outcome tracker (Sprint 35)
                # and writing engine-reported pnl_frac would corrupt Truth Router data.
                try:
                    await conn.execute(
                        """
                        UPDATE signal_outcomes
                           SET actual_direction  = $1,
                               direction_correct = $2
                         WHERE signal_id = $3
                           AND actual_direction IS NULL
                        """,
                        actual_dir, direction_correct, int(trade.signal_id),
                    )
                except Exception:
                    pass  # signal_id not numeric or FK absent — skip

                # 2. Look up signal_trace for engine-level feedback.
                # signal_trace.signal_id is always NULL (engines write trace before
                # the signal is archived to signals_registry and assigned an ID).
                # Use ticker + time proximity as the link instead.
                # Window is 20 min back (4 CRYPTO poll cycles) to avoid matching a
                # different signal from an hour ago for the same asset.
                # The upper bound ($2 + 5 min) ensures only traces that existed when
                # the trade opened can match (prevents future-signal attribution).
                trace = await conn.fetchrow(
                    """SELECT id, cycle_id, engine_name, provider, signal_type
                         FROM signal_trace
                        WHERE ticker = $1
                          AND created_at > $2::timestamptz - INTERVAL '20 minutes'
                          AND created_at <= $2::timestamptz + INTERVAL '5 minutes'
                        ORDER BY ABS(EXTRACT(EPOCH FROM (created_at - $2::timestamptz))) ASC
                        LIMIT 1""",
                    trade.asset, trade.opened_at,
                )
                if not trace:
                    return

                # 3. Write (or update) signal_feedback — DO UPDATE so re-evaluations overwrite
                await conn.execute(
                    """
                    INSERT INTO signal_feedback
                        (signal_trace_id, cycle_id, engine_name, provider,
                         signal_type, outcome_type, pnl_score,
                         direction_correct, confidence_score,
                         evaluation_window_h, market_context)
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
                    ON CONFLICT (signal_trace_id, evaluation_window_h) DO UPDATE
                        SET outcome_type      = EXCLUDED.outcome_type,
                            pnl_score         = EXCLUDED.pnl_score,
                            direction_correct = EXCLUDED.direction_correct,
                            evaluated_at      = now()
                    """,
                    trace["id"],
                    trace["cycle_id"],
                    trace["engine_name"],
                    trace["provider"],
                    trace["signal_type"] or "directional",
                    outcome,
                    pnl_pct,
                    direction_correct,
                    None,
                    held_h,
                    {"asset": trade.asset, "exchange": trade.exchange,
                     "entry": trade.entry_price, "exit": trade.exit_price},
                )
        except Exception as exc:
            logger.warning("signal_feedback_write_failed", asset=trade.asset, error=str(exc))

    # ── Watermark + persistent state (Redis) ──────────────────────────────────

    async def _load_watermark(self) -> int:
        try:
            val = await self._redis.get(WATERMARK_KEY)
            if val:
                return int(val)
        except Exception as exc:
            logger.warning("watermark_load_failed", error=str(exc))
        return 0

    async def _save_watermark(self) -> None:
        try:
            await self._redis.set(WATERMARK_KEY, str(self._watermark))
        except Exception as exc:
            logger.warning("watermark_save_failed", error=str(exc))

    async def _load_persistent_state(self) -> None:
        """Restore peak_balance and asset cooldowns from Redis on startup.

        Without this, a container restart resets drawdown protection (peak_balance=0)
        and clears stop-strike cooldowns — allowing re-entry on assets that should
        still be cooling down after two consecutive stop-losses.
        """
        try:
            pb = await self._redis.get(_PEAK_BALANCE_KEY)
            if pb:
                self._peak_balance = float(pb)
                logger.info("peak_balance_restored", peak_balance=round(self._peak_balance, 2))
        except Exception as exc:
            logger.warning("peak_balance_load_failed", error=str(exc))

        try:
            now = time.time()
            entries = await self._redis.hgetall(_COOLDOWNS_KEY)
            if entries:
                loaded = 0
                for asset, expiry_bytes in entries.items():
                    asset_str = asset.decode() if isinstance(asset, bytes) else asset
                    expiry = float(expiry_bytes)
                    if expiry > now:
                        remaining = expiry - now
                        self._asset_cooldown_until[asset_str] = time.monotonic() + remaining
                        loaded += 1
                if loaded:
                    logger.info("cooldowns_restored", count=loaded)
        except Exception as exc:
            logger.warning("cooldowns_load_failed", error=str(exc))

        # Restore stop-strike counts so the circuit-breaker survives container restarts.
        # Without this, an asset with 1 strike resets to 0 on restart and needs 2 more
        # stop-losses before the 24h cooldown activates instead of just 1.
        try:
            strikes = await self._redis.hgetall(_STRIKES_KEY)
            if strikes:
                for asset, count_bytes in strikes.items():
                    asset_str = asset.decode() if isinstance(asset, bytes) else asset
                    self._asset_stop_strikes[asset_str] = int(count_bytes)
                logger.info("stop_strikes_restored", count=len(strikes))
        except Exception as exc:
            logger.warning("stop_strikes_load_failed", error=str(exc))

        # Restore partial exit state so T1/T2 tranches aren't double-closed after a restart.
        try:
            import json as _json
            partial_entries = await self._redis.hgetall(_PARTIAL_EXITS_KEY)
            if partial_entries:
                for asset, taken_bytes in partial_entries.items():
                    asset_str = asset.decode() if isinstance(asset, bytes) else asset
                    taken_list = _json.loads(taken_bytes.decode() if isinstance(taken_bytes, bytes) else taken_bytes)
                    if asset_str in self._portfolio.open_positions:
                        self._partial_exits[asset_str] = set(taken_list)
                logger.info("partial_exits_restored", count=len(partial_entries))
        except Exception as exc:
            logger.warning("partial_exits_load_failed", error=str(exc))

        # Restore ATR stop pcts so the tighter entry-time stop survives container restarts.
        # Without this, _check_positions falls back to the base 8%/12% stop for every
        # already-open position, silently widening stops set at entry.
        try:
            stop_entries = await self._redis.hgetall(_STOPS_KEY)
            if stop_entries:
                loaded = 0
                for asset, stop_bytes in stop_entries.items():
                    asset_str = asset.decode() if isinstance(asset, bytes) else asset
                    if asset_str in self._portfolio.open_positions:
                        self._position_stops[asset_str] = float(stop_bytes)
                        loaded += 1
                if loaded:
                    logger.info("atr_stops_restored", count=loaded)
        except Exception as exc:
            logger.warning("atr_stops_load_failed", error=str(exc))

        # Restore per-asset threshold offsets computed by the walk-forward tuner.
        try:
            at_entries = await self._redis.hgetall("trading:asset_thresholds")
            if at_entries:
                self._asset_thresholds = {
                    (k.decode() if isinstance(k, bytes) else k): float(v)
                    for k, v in at_entries.items()
                }
                logger.info("asset_thresholds_restored", count=len(self._asset_thresholds))
        except Exception as exc:
            logger.warning("asset_thresholds_load_failed", error=str(exc))

        # Restore cached regime profile so thresholds are correct before the first
        # _refresh_regime_profile() cycle completes (within 30 min of startup).
        try:
            rp = await self._redis.get(_REGIME_PROFILE_KEY)
            if rp:
                rp_str = rp.decode() if isinstance(rp, bytes) else rp
                if rp_str in _REGIME_PROFILES:
                    self._regime_profile = rp_str
                    profile = _REGIME_PROFILES[rp_str]
                    self._baseline_long_threshold  = self._anchor_long_threshold  * 0.75 + profile["long_thr"]  * 0.25
                    self._baseline_short_threshold = self._anchor_short_threshold * 0.75 + profile["short_thr"] * 0.25
                    self._baseline_trail_activate  = self._anchor_trail_activate  * 0.75 + profile["trail_act"] * 0.25
                    self._trail_activate_pct = self._baseline_trail_activate
                    logger.info("regime_profile_restored", profile=rp_str,
                                short_baseline=round(self._baseline_short_threshold, 3),
                                trail_activate=round(self._baseline_trail_activate, 4))
        except Exception as exc:
            logger.warning("regime_profile_restore_failed", error=str(exc))

    async def _persist_peak_balance(self) -> None:
        try:
            await self._redis.set(_PEAK_BALANCE_KEY, str(self._peak_balance))
        except Exception as exc:
            logger.warning("peak_balance_save_failed", error=str(exc))

    async def _persist_partial_exits(self, asset: str, taken: set) -> None:
        """Persist which profit tranches (T1/T2) were taken so restarts don't double-close."""
        try:
            import json as _json
            if taken:
                await self._redis.hset(_PARTIAL_EXITS_KEY, asset, _json.dumps(sorted(taken)))
            else:
                await self._redis.hdel(_PARTIAL_EXITS_KEY, asset)
        except Exception as exc:
            logger.warning("partial_exits_save_failed", asset=asset, error=str(exc))

    async def _persist_cooldown(self, asset: str, duration_s: float = 86400.0) -> None:
        """Save a cooldown expiry (wall-clock unix timestamp) to Redis so it survives restarts."""
        try:
            expiry = time.time() + duration_s
            await self._redis.hset(_COOLDOWNS_KEY, asset, str(expiry))
        except Exception as exc:
            logger.warning("cooldown_save_failed", asset=asset, error=str(exc))

    async def _persist_strike(self, asset: str, count: int) -> None:
        """Persist stop-strike count to Redis so it survives container restarts."""
        try:
            if count > 0:
                await self._redis.hset(_STRIKES_KEY, asset, str(count))
            else:
                await self._redis.hdel(_STRIKES_KEY, asset)
        except Exception as exc:
            logger.warning("strike_save_failed", asset=asset, error=str(exc))

    async def _persist_atr_stop(self, asset: str, stop_pct: float) -> None:
        """Persist ATR stop pct to Redis (TTL 48h) so it survives container restarts."""
        try:
            await self._redis.hset(_STOPS_KEY, asset, str(stop_pct))
            await self._redis.expire(_STOPS_KEY, 172800)  # 48h TTL matches docstring
        except Exception as exc:
            logger.warning("atr_stop_save_failed", asset=asset, error=str(exc))

    async def _push_readiness_summary(self) -> None:
        """Push daily status summary. Live mode: labeled account overview. Paper: readiness checklist."""
        _TARGET_TRADES = 30
        _TARGET_WIN_RATE = 0.45
        _TARGET_DRAWDOWN = 0.08
        try:
            initial_equity = float(os.getenv("INITIAL_BALANCE", "10000"))
            async with self._db.acquire() as conn:
                paper_row = await conn.fetchrow(
                    """
                    SELECT
                        COUNT(*) FILTER (WHERE close_reason != 'dedup_cleanup') AS total_trades,
                        COUNT(*) FILTER (WHERE pnl > 0 AND close_reason != 'dedup_cleanup') AS wins,
                        SUM(pnl) FILTER (WHERE close_reason != 'dedup_cleanup') AS total_pnl
                    FROM paper_trades WHERE closed_at IS NOT NULL AND mode = $1
                    """,
                    _DB_PAPER_MODE,
                )
                live_row = await conn.fetchrow(
                    """
                    SELECT
                        COUNT(*) FILTER (WHERE close_reason != 'dedup_cleanup') AS total_trades,
                        COUNT(*) FILTER (WHERE pnl > 0 AND close_reason != 'dedup_cleanup') AS wins,
                        SUM(pnl) FILTER (WHERE close_reason != 'dedup_cleanup') AS total_pnl
                    FROM paper_trades WHERE closed_at IS NOT NULL AND mode = 'live'
                    """
                ) if self._live else None
                pnl_rows = await conn.fetch(
                    """
                    SELECT pnl FROM paper_trades
                     WHERE closed_at IS NOT NULL AND close_reason != 'dedup_cleanup'
                       AND pnl IS NOT NULL AND mode = $1
                     ORDER BY closed_at ASC
                    """,
                    _DB_PAPER_MODE,
                )
            p_total = int(paper_row["total_trades"] or 0)
            p_wins  = int(paper_row["wins"] or 0)
            p_wr    = p_wins / p_total if p_total else 0.0
            p_pnl   = float(paper_row["total_pnl"] or 0.0)
            # legacy fallback for paper-mode readiness check
            total     = p_total
            wins      = p_wins
            win_rate  = p_wr
            total_pnl = p_pnl

            snap = self._portfolio.snapshot()
            paper_balance = snap.get("balance", initial_equity)

            if self._live:
                l_total = int(live_row["total_trades"] or 0) if live_row else 0
                l_wins  = int(live_row["wins"] or 0) if live_row else 0
                l_wr    = l_wins / l_total if l_total else 0.0
                l_pnl   = float(live_row["total_pnl"] or 0.0) if live_row else 0.0
                open_pos = snap.get("positions", {})
                open_count = len(open_pos)
                open_assets = ", ".join(sorted(open_pos.keys())) if open_pos else "none"
                lines = [
                    "📊 Daily Status — LIVE MODE",
                    f"Binance USDT (real):  ${self._live_wallet_usdt:.2f}",
                    f"Paper sim (virtual):  ${paper_balance:,.2f}",
                    "",
                    f"Open positions: {open_count}  [{open_assets}]",
                    "",
                    f"LIVE  — trades: {l_total}  WR: {l_wr:.0%}  PnL: ${l_pnl:+.2f}",
                    f"PAPER — trades: {p_total}  WR: {p_wr:.0%}  PnL: ${p_pnl:+.2f}",
                ]
                await self._notifier.send_message("\n".join(lines))
                return

            # Paper mode: readiness checklist
            running = initial_equity
            peak = initial_equity
            drawdown = 0.0
            for r in pnl_rows:
                running += float(r["pnl"])
                if running > peak:
                    peak = running
                dd = (peak - running) / peak if peak > 0 else 0.0
                if dd > drawdown:
                    drawdown = dd

            criteria = {
                f"Trades {total}/{_TARGET_TRADES}": total >= _TARGET_TRADES,
                f"Win rate {win_rate:.0%}/{_TARGET_WIN_RATE:.0%}": win_rate >= _TARGET_WIN_RATE,
                f"Max drawdown {drawdown:.1%}/{_TARGET_DRAWDOWN:.0%}": drawdown <= _TARGET_DRAWDOWN,
            }
            ready = all(criteria.values())
            _is_sim = _REDIS_PREFIX == "paper_sim"
            status_icon = "✅" if ready else "⏳"
            header = "📊 Paper Sim Daily Status" if _is_sim else f"{status_icon} Live Readiness Daily Update"
            lines = [header, f"Balance: ${paper_balance:,.2f}  |  PnL: ${total_pnl:+.2f}"]
            for label, ok in criteria.items():
                lines.append(f"{'✅' if ok else '❌'} {label}")
            if _is_sim:
                lines.append("\nShadow sim — live engine is active, no action needed.")
            elif ready:
                lines.append("\nREADY FOR LIVE TRADING — set TRADING_MODE=live")
            else:
                remain = max(0, _TARGET_TRADES - total)
                lines.append(f"\nNeed {remain} more trades to evaluate graduation.")

            await self._notifier.send_message("\n".join(lines))
        except Exception as exc:
            logger.warning("readiness_push_failed", error=str(exc))
