"""Tests for the configured market group ETF inventory."""

import pytest

from index_etfs.catalog import (
    ETF_CONFIGS,
    MARKET_GROUP_EXCLUSIONS,
    UNQUALIFIED_TICKER_SCOPE,
)

MARKET_GROUP_SYMBOLS = (
    "xlk",
    "smh",
    "soxx",
    "igv",
    "cibr",
    "aiq",
    "igpt",
    "bug",
    "blok",
    "xlv",
    "ibb",
    "ihi",
    "arkg",
    "ihe",
    "ihf",
    "xlb",
    "gdx",
    "gdxj",
    "copx",
    "gunr",
    "ura",
    "sil",
    "moo",
    "xme",
    "xle",
    "nlr",
    "oih",
    "xop",
    "crak",
    "mlpx",
    "tan",
    "lit",
    "uso",
    "uga",
    "ung",
    "xli",
    "ita",
    "iyt",
    "xlf",
    "kbwb",
    "kre",
    "kbe",
    "iai",
    "ibit",
    "arkf",
    "xlre",
    "vnq",
    "xlu",
    "pave",
    "volt",
    "grid",
    "xly",
    "xrt",
    "espo",
    "pej",
    "xlc",
    "xtl",
    "xlp",
    "pbj",
)

NON_EQUITY_EXCLUSIONS = {
    "ibit": "Bitcoin exposure, not an equity fund.",
    "uso": "Commodity futures exposure, not an equity fund.",
    "uga": "Commodity futures exposure, not an equity fund.",
    "ung": "Commodity futures exposure, not an equity fund.",
}


@pytest.mark.parametrize("symbol", MARKET_GROUP_SYMBOLS)
def test_market_group_symbol_has_one_catalog_disposition(symbol: str) -> None:
    """Ensure every source symbol is configured or explicitly excluded, but not both."""
    assert (symbol in ETF_CONFIGS) != (symbol in MARKET_GROUP_EXCLUSIONS)


def test_market_group_inventory_has_expected_coverage() -> None:
    """Keep the 59-symbol input, 55 supported funds, and 60-item catalog aligned."""
    assert len(MARKET_GROUP_SYMBOLS) == 59
    assert len(set(MARKET_GROUP_SYMBOLS)) == 59
    assert len(ETF_CONFIGS) == 60
    assert sum(symbol in ETF_CONFIGS for symbol in MARKET_GROUP_SYMBOLS) == 55


def test_only_non_equity_funds_are_excluded() -> None:
    """Keep exclusions limited to bitcoin and commodity futures funds."""
    assert dict(MARKET_GROUP_EXCLUSIONS) == NON_EQUITY_EXCLUSIONS
    assert set(MARKET_GROUP_SYMBOLS) - set(NON_EQUITY_EXCLUSIONS) <= set(ETF_CONFIGS)


@pytest.mark.parametrize(
    "provider",
    ["globalx", "invesco", "ark", "firsttrust", "flexshares", "tema", "amplify"],
)
def test_unverified_exchange_scope_is_clear_for_syntax_filtered_providers(
    provider: str,
) -> None:
    """Disclose that syntax-filtered tickers are not verified exchange listings."""
    configs = [config for config in ETF_CONFIGS.values() if config.provider == provider]
    expected_scope = UNQUALIFIED_TICKER_SCOPE
    if provider == "amplify":
        expected_scope += " Listed fund positions are included without lookthrough."

    assert configs
    assert all(config.scope_note == expected_scope for config in configs)
    assert all("not verified exchange classification" in config.scope_note for config in configs)
