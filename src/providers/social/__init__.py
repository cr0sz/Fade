from .cryptopanic_social import CryptoPanicSocialProvider
from .fear_greed import FearGreedProvider
from .reddit import RedditProvider
from .twitter import TwitterProvider

SOCIAL_PROVIDERS: dict[str, type] = {
    "reddit": RedditProvider,
    "twitter": TwitterProvider,
}

__all__ = [
    "SOCIAL_PROVIDERS",
    "CryptoPanicSocialProvider",
    "FearGreedProvider",
    "RedditProvider",
    "TwitterProvider",
]
