"""ETF configuration and validation."""

import math
from dataclasses import dataclass
from typing import Literal

Provider = Literal["ssga", "ishares", "nasdaq"]

MIN_EXPECTED_RATIO = 0.9


@dataclass(frozen=True)
class ETFConfig:
    url: str
    provider: Provider
    watchlist_name: str
    expected_count: int


ETF_CONFIGS = {
    "spy": ETFConfig(
        url="https://www.ssga.com/us/en/individual/library-content/products/fund-data/etfs/us/holdings-daily-us-en-spy.xlsx",
        provider="ssga",
        watchlist_name="sp500",
        expected_count=503,
    ),
    "mdy": ETFConfig(
        url="https://www.ssga.com/us/en/individual/library-content/products/fund-data/etfs/us/holdings-daily-us-en-mdy.xlsx",
        provider="ssga",
        watchlist_name="sp400",
        expected_count=400,
    ),
    "spsm": ETFConfig(
        url="https://www.ssga.com/us/en/individual/library-content/products/fund-data/etfs/us/holdings-daily-us-en-spsm.xlsx",
        provider="ssga",
        watchlist_name="sp600",
        expected_count=600,
    ),
    "qqq": ETFConfig(
        url="https://api.nasdaq.com/api/quote/list-type/nasdaq100",
        provider="nasdaq",
        watchlist_name="nasdaq100",
        expected_count=100,
    ),
    "iwm": ETFConfig(
        url="https://www.blackrock.com/varnish-api/blk-one01-product-data/product-data/api/v1/get-fund-document?appType=PRODUCT_PAGE&appSubType=ISHARES&targetSite=us-ishares&locale=en_US&portfolioId=239710&userType=individual&component=holdings",
        provider="ishares",
        watchlist_name="russell2000",
        expected_count=2000,
    ),
}


def validate_count(symbol: str, count: int) -> None:
    """Fail closed on empty or suspiciously small source data."""
    if count == 0:
        raise ValueError(f"{symbol.upper()} returned zero rows")

    expected = ETF_CONFIGS[symbol].expected_count
    minimum = math.ceil(expected * MIN_EXPECTED_RATIO)
    if count < minimum:
        raise ValueError(
            f"{symbol.upper()} returned {count} rows; expected at least {minimum}"
        )
