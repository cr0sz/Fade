from .alpha_vantage import AlphaVantageProvider
from .coingecko import CoinGeckoProvider
from .finnhub import FinnhubProvider
from .polygon import PolygonProvider
from .yfinance_provider import YFinanceProvider

MARKET_PROVIDERS: dict[str, type] = {
    "polygon": PolygonProvider,
    "finnhub": FinnhubProvider,
    "alpha_vantage": AlphaVantageProvider,
    "coingecko": CoinGeckoProvider,
    "yfinance": YFinanceProvider,
}

__all__ = [
    "MARKET_PROVIDERS",
    "PolygonProvider",
    "FinnhubProvider",
    "AlphaVantageProvider",
    "CoinGeckoProvider",
    "YFinanceProvider",
]
