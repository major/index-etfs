"""ETF configuration and validation."""

import math
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Literal
from urllib.parse import urlsplit

Provider = Literal[
    "ssga",
    "ishares",
    "nasdaq",
    "vaneck",
    "invesco",
    "ark",
    "globalx",
    "vanguard",
    "firsttrust",
    "flexshares",
    "tema",
    "amplify",
]

MIN_EXPECTED_RATIO = 0.9
UNQUALIFIED_TICKER_SCOPE = (
    "Unqualified ticker subset; foreign-market-qualified and blank holdings excluded; "
    "not verified exchange classification."
)


@dataclass(frozen=True)
class ETFConfig:
    url: str
    provider: Provider
    watchlist_name: str
    expected_count: int
    scope_note: str | None = None


ETF_CONFIGS = frozendict({
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
    "smh": ETFConfig(
        url="https://www.vaneck.com/us/en/investments/semiconductor-etf-smh/downloads/holdings/",
        provider="vaneck",
        watchlist_name="smh",
        expected_count=25,
    ),
    "gdx": ETFConfig(
        url="https://www.vaneck.com/us/en/investments/gold-miners-etf-gdx/downloads/holdings/",
        provider="vaneck",
        watchlist_name="gdx-us",
        expected_count=32,
        scope_note="US-listed subset; foreign-market-qualified holdings are excluded.",
    ),
    "xlb": ETFConfig(
        url="https://www.ssga.com/us/en/individual/library-content/products/fund-data/etfs/us/holdings-daily-us-en-xlb.xlsx",
        provider="ssga",
        watchlist_name="xlb",
        expected_count=24,
    ),
    "xlc": ETFConfig(
        url="https://www.ssga.com/us/en/individual/library-content/products/fund-data/etfs/us/holdings-daily-us-en-xlc.xlsx",
        provider="ssga",
        watchlist_name="xlc",
        expected_count=22,
    ),
    "xle": ETFConfig(
        url="https://www.ssga.com/us/en/individual/library-content/products/fund-data/etfs/us/holdings-daily-us-en-xle.xlsx",
        provider="ssga",
        watchlist_name="xle",
        expected_count=21,
    ),
    "xlf": ETFConfig(
        url="https://www.ssga.com/us/en/individual/library-content/products/fund-data/etfs/us/holdings-daily-us-en-xlf.xlsx",
        provider="ssga",
        watchlist_name="xlf",
        expected_count=76,
    ),
    "xli": ETFConfig(
        url="https://www.ssga.com/us/en/individual/library-content/products/fund-data/etfs/us/holdings-daily-us-en-xli.xlsx",
        provider="ssga",
        watchlist_name="xli",
        expected_count=83,
    ),
    "xlk": ETFConfig(
        url="https://www.ssga.com/us/en/individual/library-content/products/fund-data/etfs/us/holdings-daily-us-en-xlk.xlsx",
        provider="ssga",
        watchlist_name="xlk",
        expected_count=75,
    ),
    "xlp": ETFConfig(
        url="https://www.ssga.com/us/en/individual/library-content/products/fund-data/etfs/us/holdings-daily-us-en-xlp.xlsx",
        provider="ssga",
        watchlist_name="xlp",
        expected_count=35,
    ),
    "xlre": ETFConfig(
        url="https://www.ssga.com/us/en/individual/library-content/products/fund-data/etfs/us/holdings-daily-us-en-xlre.xlsx",
        provider="ssga",
        watchlist_name="xlre",
        expected_count=30,
    ),
    "xlu": ETFConfig(
        url="https://www.ssga.com/us/en/individual/library-content/products/fund-data/etfs/us/holdings-daily-us-en-xlu.xlsx",
        provider="ssga",
        watchlist_name="xlu",
        expected_count=31,
    ),
    "xlv": ETFConfig(
        url="https://www.ssga.com/us/en/individual/library-content/products/fund-data/etfs/us/holdings-daily-us-en-xlv.xlsx",
        provider="ssga",
        watchlist_name="xlv",
        expected_count=61,
    ),
    "xly": ETFConfig(
        url="https://www.ssga.com/us/en/individual/library-content/products/fund-data/etfs/us/holdings-daily-us-en-xly.xlsx",
        provider="ssga",
        watchlist_name="xly",
        expected_count=47,
    ),
    "xme": ETFConfig(
        url="https://www.ssga.com/us/en/individual/library-content/products/fund-data/etfs/us/holdings-daily-us-en-xme.xlsx",
        provider="ssga",
        watchlist_name="xme",
        expected_count=38,
    ),
    "xop": ETFConfig(
        url="https://www.ssga.com/us/en/individual/library-content/products/fund-data/etfs/us/holdings-daily-us-en-xop.xlsx",
        provider="ssga",
        watchlist_name="xop",
        expected_count=51,
    ),
    "kre": ETFConfig(
        url="https://www.ssga.com/us/en/individual/library-content/products/fund-data/etfs/us/holdings-daily-us-en-kre.xlsx",
        provider="ssga",
        watchlist_name="kre",
        expected_count=167,
    ),
    "kbe": ETFConfig(
        url="https://www.ssga.com/us/en/individual/library-content/products/fund-data/etfs/us/holdings-daily-us-en-kbe.xlsx",
        provider="ssga",
        watchlist_name="kbe",
        expected_count=107,
    ),
    "xrt": ETFConfig(
        url="https://www.ssga.com/us/en/individual/library-content/products/fund-data/etfs/us/holdings-daily-us-en-xrt.xlsx",
        provider="ssga",
        watchlist_name="xrt",
        expected_count=74,
    ),
    "xtl": ETFConfig(
        url="https://www.ssga.com/us/en/individual/library-content/products/fund-data/etfs/us/holdings-daily-us-en-xtl.xlsx",
        provider="ssga",
        watchlist_name="xtl",
        expected_count=39,
    ),
    "gdxj": ETFConfig(
        url="https://www.vaneck.com/us/en/investments/junior-gold-miners-etf-gdxj/downloads/holdings/",
        provider="vaneck",
        watchlist_name="gdxj-us",
        expected_count=53,
        scope_note="US-listed subset; foreign-market-qualified holdings are excluded.",
    ),
    "moo": ETFConfig(
        url="https://www.vaneck.com/us/en/investments/agribusiness-etf-moo/downloads/holdings/",
        provider="vaneck",
        watchlist_name="moo-us",
        expected_count=20,
        scope_note="US-listed subset; foreign-market-qualified holdings are excluded.",
    ),
    "nlr": ETFConfig(
        url="https://www.vaneck.com/us/en/investments/uranium-nuclear-energy-etf-nlr/downloads/holdings/",
        provider="vaneck",
        watchlist_name="nlr-us",
        expected_count=15,
        scope_note="US-listed subset; foreign-market-qualified holdings are excluded.",
    ),
    "oih": ETFConfig(
        url="https://www.vaneck.com/us/en/investments/oil-services-etf-oih/downloads/holdings/",
        provider="vaneck",
        watchlist_name="oih",
        expected_count=25,
    ),
    "crak": ETFConfig(
        url="https://www.vaneck.com/us/en/investments/oil-refiners-etf-crak/downloads/holdings/",
        provider="vaneck",
        watchlist_name="crak-us",
        expected_count=8,
        scope_note="US-listed subset; foreign-market-qualified holdings are excluded.",
    ),
    "espo": ETFConfig(
        url="https://www.vaneck.com/us/en/investments/video-gaming-esports-etf-espo/downloads/holdings/",
        provider="vaneck",
        watchlist_name="espo-us",
        expected_count=5,
        scope_note="US-listed subset; foreign-market-qualified holdings are excluded.",
    ),
    "soxx": ETFConfig(
        url="https://www.ishares.com/us/products/239705/SOXX/latest-holdings.csv",
        provider="ishares",
        watchlist_name="soxx",
        expected_count=34,
        scope_note="USD-denominated holdings; not exhaustive equity classification.",
    ),
    "igv": ETFConfig(
        url="https://www.ishares.com/us/products/239771/IGV/latest-holdings.csv",
        provider="ishares",
        watchlist_name="igv",
        expected_count=111,
        scope_note="USD-denominated holdings; not exhaustive equity classification.",
    ),
    "ibb": ETFConfig(
        url="https://www.ishares.com/us/products/239699/IBB/latest-holdings.csv",
        provider="ishares",
        watchlist_name="ibb",
        expected_count=242,
        scope_note="USD-denominated holdings; not exhaustive equity classification.",
    ),
    "ihi": ETFConfig(
        url="https://www.ishares.com/us/products/239516/IHI/latest-holdings.csv",
        provider="ishares",
        watchlist_name="ihi",
        expected_count=54,
        scope_note="USD-denominated holdings; not exhaustive equity classification.",
    ),
    "ihe": ETFConfig(
        url="https://www.ishares.com/us/products/239519/IHE/latest-holdings.csv",
        provider="ishares",
        watchlist_name="ihe",
        expected_count=57,
        scope_note="USD-denominated holdings; not exhaustive equity classification.",
    ),
    "ihf": ETFConfig(
        url="https://www.ishares.com/us/products/239510/IHF/latest-holdings.csv",
        provider="ishares",
        watchlist_name="ihf",
        expected_count=65,
        scope_note="USD-denominated holdings; not exhaustive equity classification.",
    ),
    "ita": ETFConfig(
        url="https://www.ishares.com/us/products/239502/ITA/latest-holdings.csv",
        provider="ishares",
        watchlist_name="ita",
        expected_count=52,
        scope_note="USD-denominated holdings; not exhaustive equity classification.",
    ),
    "iyt": ETFConfig(
        url="https://www.ishares.com/us/products/239501/IYT/latest-holdings.csv",
        provider="ishares",
        watchlist_name="iyt",
        expected_count=48,
        scope_note="USD-denominated holdings; not exhaustive equity classification.",
    ),
    "iai": ETFConfig(
        url="https://www.ishares.com/us/products/239504/IAI/latest-holdings.csv",
        provider="ishares",
        watchlist_name="iai",
        expected_count=39,
        scope_note="USD-denominated holdings; not exhaustive equity classification.",
    ),
    "aiq": ETFConfig(
        url="https://www.globalxetfs.com/funds/aiq/",
        provider="globalx",
        watchlist_name="aiq-us",
        expected_count=66,
        scope_note=UNQUALIFIED_TICKER_SCOPE,
    ),
    "bug": ETFConfig(
        url="https://www.globalxetfs.com/funds/bug/",
        provider="globalx",
        watchlist_name="bug-us",
        expected_count=23,
        scope_note=UNQUALIFIED_TICKER_SCOPE,
    ),
    "copx": ETFConfig(
        url="https://www.globalxetfs.com/funds/copx/",
        provider="globalx",
        watchlist_name="copx-us",
        expected_count=4,
        scope_note=UNQUALIFIED_TICKER_SCOPE,
    ),
    "ura": ETFConfig(
        url="https://www.globalxetfs.com/funds/ura/",
        provider="globalx",
        watchlist_name="ura-us",
        expected_count=12,
        scope_note=UNQUALIFIED_TICKER_SCOPE,
    ),
    "sil": ETFConfig(
        url="https://www.globalxetfs.com/funds/sil/",
        provider="globalx",
        watchlist_name="sil-us",
        expected_count=9,
        scope_note=UNQUALIFIED_TICKER_SCOPE,
    ),
    "mlpx": ETFConfig(
        url="https://www.globalxetfs.com/funds/mlpx/",
        provider="globalx",
        watchlist_name="mlpx-us",
        expected_count=29,
        scope_note=UNQUALIFIED_TICKER_SCOPE,
    ),
    "lit": ETFConfig(
        url="https://www.globalxetfs.com/funds/lit/",
        provider="globalx",
        watchlist_name="lit-us",
        expected_count=7,
        scope_note=UNQUALIFIED_TICKER_SCOPE,
    ),
    "pave": ETFConfig(
        url="https://www.globalxetfs.com/funds/pave/",
        provider="globalx",
        watchlist_name="pave-us",
        expected_count=98,
        scope_note=UNQUALIFIED_TICKER_SCOPE,
    ),
    "igpt": ETFConfig(
        url="https://dng-api.invesco.com/cache/v1/accounts/en_US/shareclasses/46137V639/holdings/fund?idType=cusip&productType=ETF",
        provider="invesco",
        watchlist_name="igpt-us",
        expected_count=70,
        scope_note=UNQUALIFIED_TICKER_SCOPE,
    ),
    "tan": ETFConfig(
        url="https://dng-api.invesco.com/cache/v1/accounts/en_US/shareclasses/46138G706/holdings/fund?idType=cusip&productType=ETF",
        provider="invesco",
        watchlist_name="tan-us",
        expected_count=24,
        scope_note=UNQUALIFIED_TICKER_SCOPE,
    ),
    "kbwb": ETFConfig(
        url="https://dng-api.invesco.com/cache/v1/accounts/en_US/shareclasses/46138E628/holdings/fund?idType=cusip&productType=ETF",
        provider="invesco",
        watchlist_name="kbwb-us",
        expected_count=24,
        scope_note=UNQUALIFIED_TICKER_SCOPE,
    ),
    "pej": ETFConfig(
        url="https://dng-api.invesco.com/cache/v1/accounts/en_US/shareclasses/46137V720/holdings/fund?idType=cusip&productType=ETF",
        provider="invesco",
        watchlist_name="pej-us",
        expected_count=30,
        scope_note=UNQUALIFIED_TICKER_SCOPE,
    ),
    "pbj": ETFConfig(
        url="https://dng-api.invesco.com/cache/v1/accounts/en_US/shareclasses/46137V753/holdings/fund?idType=cusip&productType=ETF",
        provider="invesco",
        watchlist_name="pbj-us",
        expected_count=30,
        scope_note=UNQUALIFIED_TICKER_SCOPE,
    ),
    "arkg": ETFConfig(
        url="https://assets.ark-funds.com/fund-documents/funds-etf-csv/ARK_GENOMIC_REVOLUTION_ETF_ARKG_HOLDINGS.csv",
        provider="ark",
        watchlist_name="arkg-us",
        expected_count=33,
        scope_note=UNQUALIFIED_TICKER_SCOPE,
    ),
    "arkf": ETFConfig(
        url="https://assets.ark-funds.com/fund-documents/funds-etf-csv/ARK_BLOCKCHAIN_&_FINTECH_INNOVATION_ETF_ARKF_HOLDINGS.csv",
        provider="ark",
        watchlist_name="arkf-us",
        expected_count=33,
        scope_note=UNQUALIFIED_TICKER_SCOPE,
    ),
    "cibr": ETFConfig(
        url="https://www.ftportfolios.com/Retail/Etf/EtfHoldings.aspx?Ticker=CIBR",
        provider="firsttrust",
        watchlist_name="cibr-us",
        expected_count=41,
        scope_note=UNQUALIFIED_TICKER_SCOPE,
    ),
    "grid": ETFConfig(
        url="https://www.ftportfolios.com/Retail/Etf/EtfHoldings.aspx?Ticker=GRID",
        provider="firsttrust",
        watchlist_name="grid-us",
        expected_count=41,
        scope_note=UNQUALIFIED_TICKER_SCOPE,
    ),
    "gunr": ETFConfig(
        url="https://www.flexshares.com/content/dam/ntflexshares/fund/gunr/gunr-holdings.csv",
        provider="flexshares",
        watchlist_name="gunr-us",
        expected_count=107,
        scope_note=UNQUALIFIED_TICKER_SCOPE,
    ),
    "volt": ETFConfig(
        url="https://temaetfs.com/volt",
        provider="tema",
        watchlist_name="volt-us",
        expected_count=24,
        scope_note=UNQUALIFIED_TICKER_SCOPE,
    ),
    "vnq": ETFConfig(
        url="https://investor.vanguard.com/irr/funds/profile/VNQ-AdditionalFundData",
        provider="vanguard",
        watchlist_name="vnq",
        expected_count=138,
        scope_note="EQ rows only; underlying fund positions excluded, no lookthrough.",
    ),
    "blok": ETFConfig(
        url="https://firestore.googleapis.com/v1/projects/amplify-etfs-data-feed/databases/%28default%29/documents/funds/BLOK/holdings?pageSize=1&orderBy=__name__%20desc",
        provider="amplify",
        watchlist_name="blok-us",
        expected_count=44,
        scope_note=(
            UNQUALIFIED_TICKER_SCOPE
            + " Listed fund positions are included without lookthrough."
        ),
    ),
})

MARKET_GROUP_EXCLUSIONS = frozendict(
    {
        "ibit": "Bitcoin exposure, not an equity fund.",
        "uso": "Commodity futures exposure, not an equity fund.",
        "uga": "Commodity futures exposure, not an equity fund.",
        "ung": "Commodity futures exposure, not an equity fund.",
    }
)


def validate_catalog(configs: Mapping[str, ETFConfig] | None = None) -> None:
    """Validate catalog fields and ensure generated output paths are unique."""
    if configs is None:
        configs = ETF_CONFIGS

    generated_paths = {"metadata/latest.json": "metadata"}
    for symbol, config in configs.items():
        if not isinstance(config, ETFConfig):
            raise ValueError(  # noqa: TRY004  # Catalog validation consistently raises ValueError.
                f"Invalid ETF config for {symbol!r}"
            )

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
            "vaneck",
            "invesco",
            "ark",
            "globalx",
            "vanguard",
            "firsttrust",
            "flexshares",
            "tema",
            "amplify",
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
