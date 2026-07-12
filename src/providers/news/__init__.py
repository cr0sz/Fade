from .bloomberg import BloombergProvider
from .cnbc import CNBCProvider
from .crypto_rss import CryptoRSSProvider
from .cryptopanic import CryptoPanicsNewsProvider
from .ft import FTProvider
from .newsapi import NewsAPIProvider
from .reddit_rss import RedditRSSProvider
from .reuters import ReutersProvider
from .turkish_rss import TurkishRSSProvider
from .yahoo_finance_rss import YahooFinanceRSSProvider

NEWS_PROVIDERS: dict[str, type] = {
    "newsapi": NewsAPIProvider,
    "bloomberg": BloombergProvider,
    "ft": FTProvider,
    "cnbc": CNBCProvider,
    "reuters": ReutersProvider,
    "crypto_rss": CryptoRSSProvider,
    "cryptopanic": CryptoPanicsNewsProvider,
    "yahoo_rss": YahooFinanceRSSProvider,
    "turkish_rss": TurkishRSSProvider,
    "reddit_rss": RedditRSSProvider,
}

__all__ = [
    "NEWS_PROVIDERS",
    "NewsAPIProvider",
    "BloombergProvider",
    "FTProvider",
    "CNBCProvider",
    "CryptoPanicsNewsProvider",
    "ReutersProvider",
    "CryptoRSSProvider",
    "RedditRSSProvider",
    "YahooFinanceRSSProvider",
    "TurkishRSSProvider",
]
