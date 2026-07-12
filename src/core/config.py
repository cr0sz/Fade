from __future__ import annotations

import json
from functools import lru_cache
from typing import Any

from pydantic import field_validator, model_validator
from pydantic_settings import (
    BaseSettings,
    DotEnvSettingsSource,
    EnvSettingsSource,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
)


# ── Flexible list env-source ──────────────────────────────────────────────────
#
# pydantic-settings v2 calls json.loads() on ALL "complex" fields (list, dict, …)
# before validators run.  This means:
#
#   DEFAULT_US_WATCHLIST=["AAPL","MSFT"]   → json.loads → list (correct)
#   DEFAULT_US_WATCHLIST=AAPL,MSFT         → json.loads fails → SettingsError
#   DEFAULT_US_WATCHLIST=                  → json.loads('') fails → SettingsError
#
# The mixin below overrides decode_complex_value for list-typed fields so that
# bare integers, comma-separated strings, and JSON arrays all work correctly.
# Non-list complex fields (e.g. SECTOR_KEYWORDS dict) keep standard JSON behaviour.


class _FlexListMixin:
    """Mixin: comma-separated / bare-value / JSON-array support for list fields."""

    def decode_complex_value(
        self, field_name: str, field: Any, value: str
    ) -> Any:
        v = value.strip() if isinstance(value, str) else value
        annotation = getattr(field, "annotation", None)
        is_list_field = getattr(annotation, "__origin__", None) is list

        if is_list_field:
            if not v:
                return []
            try:
                result = json.loads(v)
                if isinstance(result, list):
                    return result
                # json.loads succeeded but returned a scalar (e.g. bare int "123456789")
                # Fall through to comma-split which handles it as a single-element list.
            except (json.JSONDecodeError, ValueError):
                pass
            # Comma-separated fallback: "123,456" → ["123", "456"]
            return [item.strip() for item in v.split(",") if item.strip()]

        # Non-list complex field: keep standard json.loads (SettingsError on failure)
        return json.loads(v)


class _FlexEnvSource(_FlexListMixin, EnvSettingsSource):
    """EnvSettingsSource with flexible list parsing (os.environ)."""


class _FlexDotEnvSource(_FlexListMixin, DotEnvSettingsSource):
    """DotEnvSettingsSource with flexible list parsing (.env file)."""


# ── Settings ──────────────────────────────────────────────────────────────────


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        mc = settings_cls.model_config
        flex_dotenv = _FlexDotEnvSource(
            settings_cls,
            env_file=mc.get("env_file"),
            env_file_encoding=mc.get("env_file_encoding", "utf-8"),
        )
        return (
            init_settings,
            _FlexEnvSource(settings_cls),
            flex_dotenv,
            file_secret_settings,
        )

    # ── Application ───────────────────────────────────────────────────────────
    APP_NAME: str = "Fade"
    APP_VERSION: str = "0.1.0"
    DEBUG: bool = False
    LOG_LEVEL: str = "INFO"

    # ── Database ──────────────────────────────────────────────────────────────
    POSTGRES_HOST: str = "postgres"
    POSTGRES_PORT: int = 5432
    POSTGRES_DB: str = "mktintel"
    POSTGRES_USER: str = "mktintel"
    POSTGRES_PASSWORD: str = ""
    DB_POOL_MIN: int = 2
    DB_POOL_MAX: int = 10

    @property
    def database_url(self) -> str:
        return (
            f"postgresql://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}"
            f"@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"
        )

    # ── Redis ─────────────────────────────────────────────────────────────────
    REDIS_HOST: str = "redis"
    REDIS_PORT: int = 6379
    REDIS_DB: int = 0
    REDIS_PASSWORD: str = ""

    @property
    def redis_url(self) -> str:
        if self.REDIS_PASSWORD:
            return f"redis://:{self.REDIS_PASSWORD}@{self.REDIS_HOST}:{self.REDIS_PORT}/{self.REDIS_DB}"
        return f"redis://{self.REDIS_HOST}:{self.REDIS_PORT}/{self.REDIS_DB}"

    # ── Cost Control ──────────────────────────────────────────────────────────
    DAILY_BUDGET_USD: float = 5.0
    BUDGET_WARNING_THRESHOLD: float = 0.7   # warn at 70% used
    BUDGET_FALLBACK_THRESHOLD: float = 0.85  # switch to cheap provider at 85%
    BUDGET_HARD_LIMIT_THRESHOLD: float = 0.95  # block AI at 95%

    # ── AI Providers ──────────────────────────────────────────────────────────
    OPENAI_API_KEY: str = ""
    OPENAI_MODEL: str = "gpt-4o-mini"
    OPENAI_MAX_TOKENS: int = 2000
    OPENAI_TEMPERATURE: float = 0.1
    OPENAI_INPUT_COST_PER_1M: float = 0.15
    OPENAI_OUTPUT_COST_PER_1M: float = 0.60

    ANTHROPIC_API_KEY: str = ""
    ANTHROPIC_MODEL: str = "claude-haiku-4-5-20251001"
    ANTHROPIC_MAX_TOKENS: int = 2000
    ANTHROPIC_INPUT_COST_PER_1M: float = 0.80
    ANTHROPIC_OUTPUT_COST_PER_1M: float = 4.00

    # ── Market Data Providers ─────────────────────────────────────────────────
    POLYGON_API_KEY: str = ""
    POLYGON_BASE_URL: str = "https://api.polygon.io"
    POLYGON_RATE_LIMIT: int = 5  # calls/min on free tier

    FINNHUB_API_KEY: str = ""
    FINNHUB_BASE_URL: str = "https://finnhub.io/api/v1"
    FINNHUB_RATE_LIMIT: int = 30  # calls/sec on free tier

    ALPHA_VANTAGE_API_KEY: str = ""
    ALPHA_VANTAGE_BASE_URL: str = "https://www.alphavantage.co"
    ALPHA_VANTAGE_RATE_LIMIT: int = 5  # calls/min free tier

    # ── News Providers ────────────────────────────────────────────────────────
    NEWSAPI_KEY: str = ""
    NEWSAPI_BASE_URL: str = "https://newsapi.org/v2"
    NEWSAPI_RATE_LIMIT: int = 100  # requests/day free tier

    BLOOMBERG_API_KEY: str = ""  # enterprise (optional — RSS works without it)
    BLOOMBERG_BASE_URL: str = "https://api.bloomberg.com"
    BLOOMBERG_RSS_FEEDS: list[str] = [
        "https://feeds.bloomberg.com/markets/news.rss",
        "https://feeds.bloomberg.com/technology/news.rss",
        "https://feeds.bloomberg.com/politics/news.rss",
    ]
    BLOOMBERG_RATE_LIMIT_DELAY: float = 2.0

    REUTERS_API_KEY: str = ""
    REUTERS_BASE_URL: str = "https://api.reuters.com"

    # ── Financial Times (RSS — no key required) ───────────────────────────────
    FT_RSS_FEEDS: list[str] = [
        "https://www.ft.com/rss/home",
        "https://www.ft.com/markets?format=rss",
        "https://www.ft.com/world/economy?format=rss",
    ]
    FT_RATE_LIMIT_DELAY: float = 2.0

    # ── CNBC (RSS — no key required) ─────────────────────────────────────────
    CNBC_RSS_FEEDS: list[str] = [
        "https://www.cnbc.com/id/100003114/device/rss/rss.html",
        "https://www.cnbc.com/id/10001147/device/rss/rss.html",
        "https://www.cnbc.com/id/15839135/device/rss/rss.html",
        "https://www.cnbc.com/id/19854910/device/rss/rss.html",
    ]
    CNBC_RATE_LIMIT_DELAY: float = 2.0

    # ── Social Providers ──────────────────────────────────────────────────────
    REDDIT_USER_AGENT: str = "MarketIntelBot/0.1"
    REDDIT_SUBREDDITS: list[str] = [
        "wallstreetbets", "stocks", "investing", "SecurityAnalysis",
        "StockMarket", "options", "ValueInvesting", "BIST",
        "CryptoCurrency", "Bitcoin", "ethfinance", "solana",
        "CryptoMarkets", "altcoin",
        # Stock-specific subs for our active NASDAQ positions
        "nvda_stock", "teslainvestorsclub",
        # Crypto-specific subs for better coverage
        "ethtrader", "XRP",
    ]

    TWITTER_BEARER_TOKEN: str = ""
    TWITTER_API_KEY: str = ""
    TWITTER_API_SECRET: str = ""
    TWITTER_ACCESS_TOKEN: str = ""
    TWITTER_ACCESS_SECRET: str = ""
    TWITTER_RATE_LIMIT: int = 15  # requests per 15min window

    # ── Smart Money Providers ─────────────────────────────────────────────────
    WHALE_ALERT_API_KEY: str = ""
    WHALE_ALERT_BASE_URL: str = "https://api.whale-alert.io/v1"
    WHALE_ALERT_RATE_LIMIT: int = 10  # calls/min free tier

    SEC_EDGAR_BASE_URL: str = "https://efts.sec.gov/LATEST/search-index"
    SEC_EDGAR_USER_AGENT: str = "MarketIntel contact@example.com"

    # ── Discord Bot ───────────────────────────────────────────────────────────────
    DISCORD_BOT_TOKEN:      str = ""
    DISCORD_GUILD_ID:       str = ""   # integer string; empty = global commands (1h delay)
    DISCORD_ADMIN_USER_ID:  str = ""   # integer string; empty = anyone can use admin commands

    # ── Scheduler Intervals (seconds) ─────────────────────────────────────────
    SCHEDULER_NEWS_INTERVAL: int = 300           # 5 minutes
    SCHEDULER_SOCIAL_INTERVAL: int = 900         # 15 minutes
    SCHEDULER_SMART_MONEY_INTERVAL: int = 3600   # 1 hour
    SCHEDULER_AI_DIGEST_INTERVAL: int = 3600     # 1 hour
    SCHEDULER_TREND_INTERVAL: int = 900          # 15 minutes
    SCHEDULER_CLEANUP_INTERVAL: int = 86400      # daily
    SCHEDULER_MARKET_DATA_INTERVAL: int = 300    # 5 minutes (crypto + BIST prices)

    # ── Intelligence Engine Config ────────────────────────────────────────────
    NEWS_FETCH_LIMIT: int = 300        # articles per provider per cycle
    NEWS_RETENTION_DAYS: int = 30
    SOCIAL_FETCH_LIMIT: int = 200      # posts per source per cycle
    SOCIAL_RETENTION_DAYS: int = 7
    TOP_TICKERS_FOR_LLM: int = 20      # max tickers to LLM-score per cycle

    SENTIMENT_HYPE_VELOCITY_THRESHOLD: float = 3.0   # 3x avg = hype signal
    SENTIMENT_PANIC_VELOCITY_THRESHOLD: float = 3.0
    SENTIMENT_HYPE_SCORE_THRESHOLD: float = 0.7
    SENTIMENT_PANIC_SCORE_THRESHOLD: float = -0.7

    # AI response cache TTL
    AI_CACHE_TTL_DIGEST: int = 3600     # 1 hour for daily digests
    AI_CACHE_TTL_QUERY: int = 300       # 5 min for live queries

    # ── Hybrid Deduplication ─────────────────────────────────────────────────
    DEDUP_USE_EMBEDDINGS: bool = False   # Level-3 semantic dedup (costs API tokens)
    DEDUP_SEMANTIC_THRESHOLD: float = 0.92  # cosine similarity threshold

    # ── Circuit Breaker Defaults ─────────────────────────────────────────────
    CIRCUIT_FAILURE_THRESHOLD: int = 5    # failures to open circuit
    CIRCUIT_RECOVERY_TIMEOUT: float = 60.0  # seconds before half-open attempt
    CIRCUIT_SUCCESS_THRESHOLD: int = 2    # successes in half-open to close

    # ── Noise Filter (Level 2) ────────────────────────────────────────────────
    NOISE_MIN_ENGAGEMENT: float = 3.0          # min upvote score to pass filter
    NOISE_BOT_BURST_WINDOW: int = 300          # seconds: burst detection window
    NOISE_BOT_BURST_THRESHOLD: int = 5         # posts in window = bot burst

    # ── Signal Smoothing (Level 2) ────────────────────────────────────────────
    TREND_SMOOTHING_ALPHA: float = 0.3         # EMA coefficient (0=ignore current, 1=no smoothing)
    TREND_VOLATILITY_DAMPENING: float = 0.8    # multiply composite by this during high-vol regime
    TREND_SMOOTHING_REDIS_TTL: int = 86400     # seconds: EMA history TTL in Redis (24h)

    # ── Level 3: Institutional Flow Engine ───────────────────────────────────
    FLOW_ANALYSIS_WINDOW_HOURS: int = 24        # smart money lookback window
    FLOW_PERSISTENCE_HISTORY_DAYS: int = 7     # days to maintain direction history
    FLOW_ACCELERATION_THRESHOLD: float = 10.0  # strength delta to call "building"
    FLOW_STRENGTH_BASE_USD: float = 10_000.0   # $10k baseline for strength calculation

    # ── Level 3: ETF Rotation Engine ─────────────────────────────────────────
    ETF_ROTATION_VELOCITY_THRESHOLD: float = 0.05  # min velocity delta for rotation signal

    # ── Level 3: Macro Regime Engine ─────────────────────────────────────────
    MACRO_REGIME_SHIFT_THRESHOLD: float = 0.15  # min score delta to call a regime shift

    # ── Level 3: Scheduler ────────────────────────────────────────────────────
    SCHEDULER_INTELLIGENCE_INTERVAL: int = 900  # 15 min: L3 full cycle

    # ── Level 4: Signal Validation + Backtesting ──────────────────────────────
    VALIDATION_ENABLED: bool = True
    BACKTEST_WINDOW_DAYS: int = 30            # days of signals to include in a backtest run
    SIGNAL_RETENTION_DAYS: int = 365          # how long to keep signals_registry rows
    ARCHIVER_BATCH_SIZE: int = 500            # rows per archiver poll cycle per table
    ARCHIVER_POLL_INTERVAL: int = 30          # seconds between archiver poll cycles
    BACKTEST_EVAL_INTERVAL: int = 900         # seconds between backtest evaluation cycles
    SIGNAL_OUTCOME_WINDOWS: list[int] = [4, 24, 72]  # evaluation windows in hours

    # ── Watched Tickers (pre-seeded) ──────────────────────────────────────────
    DEFAULT_US_WATCHLIST: list[str] = [
        "AAPL", "MSFT", "NVDA", "GOOGL", "AMZN",
        "META", "TSLA", "SPY", "QQQ", "IWM",
        "GLD", "TLT", "VIX",
    ]
    DEFAULT_BIST_WATCHLIST: list[str] = [
        "THYAO.IS", "GARAN.IS", "ASELS.IS", "KCHOL.IS",
        "EREGL.IS", "BIMAS.IS", "SISE.IS", "TCELL.IS",
        "AKBNK.IS", "YKBNK.IS",
    ]

    # ── Sector Keywords (for classification) ──────────────────────────────────
    SECTOR_KEYWORDS: dict[str, list[str]] = {
        "technology": ["software", "semiconductor", "AI", "cloud", "chip", "data center"],
        "finance": ["bank", "insurance", "fintech", "payments", "lending"],
        "energy": ["oil", "gas", "renewable", "solar", "wind", "petroleum"],
        "healthcare": ["pharma", "biotech", "medical", "drug", "FDA"],
        "consumer": ["retail", "e-commerce", "consumer goods", "restaurant"],
        "industrial": ["manufacturing", "aerospace", "defense", "logistics"],
        "real_estate": ["REIT", "real estate", "property", "housing"],
        "materials": ["mining", "metals", "chemicals", "commodities"],
        "utilities": ["electric", "water", "natural gas utility"],
        "telecom": ["telecom", "wireless", "broadband", "5G"],
    }

    @model_validator(mode="after")
    def _require_critical_secrets(self) -> "Settings":
        if not self.POSTGRES_PASSWORD:
            raise ValueError(
                "POSTGRES_PASSWORD is not set. "
                "Set it in your .env file or as an environment variable. "
                "Refusing to start with an empty database password."
            )
        return self

    @field_validator(
        "DEFAULT_US_WATCHLIST",
        "DEFAULT_BIST_WATCHLIST", "REDDIT_SUBREDDITS",
        "FT_RSS_FEEDS", "CNBC_RSS_FEEDS", "BLOOMBERG_RSS_FEEDS",
        "SIGNAL_OUTCOME_WINDOWS",
        mode="before",
    )
    @classmethod
    def parse_list(cls, v: object) -> list:
        """Normalize list values from any source after env-source pre-processing.

        The custom env sources above handle bare strings and comma-separated
        formats before this runs.  This validator covers two remaining cases:

        1. JSON int arrays like [123456789] that json.loads decodes to a list of
           ints — pydantic v2 will NOT coerce int→str for list[str] fields, so we
           do it here.  str→int coercion for list[int] fields (SIGNAL_OUTCOME_WINDOWS)
           is handled by pydantic itself.

        2. Values supplied programmatically (tests, init_settings) that arrive as
           already-a-list or as a raw string.
        """
        if isinstance(v, list):
            # Coerce every item to str so pydantic can re-coerce to the target type.
            # For list[str] fields: stays as str.
            # For list[int] fields: pydantic coerces "4" → 4 in lax mode.
            return [str(item) for item in v]
        if isinstance(v, str):
            v = v.strip()
            if not v:
                return []
            if v.startswith("["):
                try:
                    parsed = json.loads(v)
                    if isinstance(parsed, list):
                        return [str(item) for item in parsed]
                except (json.JSONDecodeError, ValueError):
                    pass
            return [item.strip() for item in v.split(",") if item.strip()]
        # Scalar (int/float) that slipped through — wrap as single-item list
        return [str(v)] if v is not None else []


@lru_cache
def get_settings() -> Settings:
    return Settings()
