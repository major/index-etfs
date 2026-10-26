"""ETF configuration and validation."""

import math
from dataclasses import dataclass
from typing import Literal
from urllib.parse import urlsplit

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


def validate_catalog(configs: dict[str, ETFConfig] | None = None) -> None:
    """Validate catalog fields and ensure generated output paths are unique."""
    if configs is None:
        configs = ETF_CONFIGS

    generated_paths = {"metadata/latest.json": "metadata"}
    for symbol, config in configs.items():
        if not isinstance(config, ETFConfig):
            raise ValueError(f"Invalid ETF config for {symbol!r}")

        for field_name, name in (
            ("symbol", symbol),
            ("watchlist_name", config.watchlist_name),
        ):
            if not isinstance(name, str) or not name or not all(
                character.isascii()
                and (
                    character.islower()
                    or character.isdigit()
                    or character in "_-"
                )
                for character in name
            ):
                raise ValueError(
                    f"Invalid {field_name} {name!r}; names must use lowercase "
                    "ASCII letters, digits, underscores, or hyphens"
                )

        if not isinstance(config.provider, str) or config.provider not in {
            "ssga",
            "ishares",
            "nasdaq",
        }:
            raise ValueError(f"Unsupported provider {config.provider!r} for {symbol}")

        if (
            isinstance(config.expected_count, bool)
            or not isinstance(config.expected_count, int)
            or config.expected_count <= 0
        ):
            raise ValueError(
                f"Invalid expected_count for {symbol}: {config.expected_count!r}"
            )

        try:
            parsed_url = urlsplit(config.url)
            url_is_valid = parsed_url.scheme in {"http", "https"} and bool(
                parsed_url.hostname
            )
        except (TypeError, ValueError):
            url_is_valid = False
        if not url_is_valid:
            raise ValueError(f"Invalid HTTP(S) URL for {symbol}: {config.url!r}")

        for path in (
            f"tickers/{symbol}.txt",
            f"watchlists/{config.watchlist_name}.txt",
            f"watchlists/{config.watchlist_name}-grouped.txt",
        ):
            if path in generated_paths:
                raise ValueError(
                    f"Generated output path collision at {path!r} between "
                    f"{generated_paths[path]!r} and {symbol!r}"
                )
            generated_paths[path] = symbol


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
