"""Characterize provider file formats using small local fixtures."""

import io
import json
from contextlib import ExitStack
from pathlib import Path
from unittest.mock import patch

import polars as pl
import pytest

from index_etfs import sources
from index_etfs.catalog import ETFConfig
from index_etfs.sources import (
    filter_and_clean,
    load_ishares_csv,
    load_nasdaq_json,
    load_ssga_excel,
)

FIXTURES = Path(__file__).parent / "fixtures"


def _fixture_stream(filename: str) -> io.BytesIO:
    """Return a fresh in-memory stream for a checked-in fixture."""
    return io.BytesIO((FIXTURES / filename).read_bytes())


def test_load_ssga_excel_parses_real_header_and_cleans_holdings() -> None:
    """Parse the SSGA workbook header at row index four and retain source fields."""
    # Arrange
    url = "https://example.com/ssga.xlsx"
    fixture = _fixture_stream("ssga_holdings.xlsx")

    # Act
    with patch("index_etfs.sources.read_url", return_value=fixture) as mock_read_url:
        holdings = load_ssga_excel(url)

    cleaned = filter_and_clean(holdings, "ssga")

    # Assert
    mock_read_url.assert_called_once_with(url)
    assert holdings.columns == ["Ticker", "Name", "Weight", "Local Currency"]
    assert holdings.row(0) == ("ZETA", "Zeta Inc", 3.25, "USD")
    assert cleaned["Ticker"].to_list() == ["AAA", "BRK/B", "ZETA"]


def test_load_ishares_csv_parses_after_nine_preamble_rows() -> None:
    """Parse the iShares CSV columns after its nine preamble rows."""
    # Arrange
    url = "https://example.com/ishares.csv"
    fixture = _fixture_stream("ishares_holdings.csv")

    # Act
    with patch("index_etfs.sources.read_url", return_value=fixture) as mock_read_url:
        holdings = load_ishares_csv(url)

    cleaned = filter_and_clean(holdings, "ishares")

    # Assert
    mock_read_url.assert_called_once_with(url)
    assert holdings.columns == ["Ticker", "Name", "Weight", "Market Currency"]
    assert holdings.row(0) == ("ZETA", "Zeta Inc", 3.25, "USD")
    assert cleaned["Ticker"].to_list() == ["AAA", "BRK/B", "ZETA"]


def test_load_ishares_csv_renames_market_weight_when_weight_percent_is_absent() -> None:
    """Use the verified IAI Market Weight column when Weight (%) is absent."""
    # Arrange
    url = "https://example.com/iai.csv"
    payload = b"preamble\n" * 9 + (
        b"Ticker,Name,Market Weight,Market Currency\n"
        b"IAI,Test Holdings,0.75,USD\n"
    )

    # Act
    with patch("index_etfs.sources.read_url", return_value=io.BytesIO(payload)):
        holdings = load_ishares_csv(url)

    # Assert
    assert holdings.columns == ["Ticker", "Name", "Weight", "Market Currency"]
    assert holdings.row(0) == ("IAI", "Test Holdings", 0.75, "USD")


def test_load_ishares_csv_ignores_unprojected_price_inference() -> None:
    """Ignore comma-formatted values in unused columns beyond inference samples."""
    # Arrange
    url = "https://example.com/ishares.csv"
    rows = [
        f"TICKER{index},Holding {index},{index + 0.5},USD,1234.56"
        for index in range(100)
    ]
    rows.append('LAST,Last Holding,10.5,USD,"1,234.56"')
    csv_text = "Ticker,Name,Weight (%),Market Currency,Price\n" + "\n".join(rows)
    payload = b"preamble\n" * 9 + csv_text.encode() + b"\n"

    # Act
    with patch("index_etfs.sources.read_url", return_value=io.BytesIO(payload)):
        holdings = load_ishares_csv(url)

    # Assert
    assert holdings.columns == ["Ticker", "Name", "Weight", "Market Currency"]
    assert holdings.height == 101
    assert holdings.schema["Weight"] == pl.Float64
    assert holdings["Weight"][-1] == 10.5


def test_load_ishares_csv_rejects_missing_weight_columns() -> None:
    """Reject iShares CSV schemas that contain neither supported weight column."""
    # Arrange
    url = "https://example.com/ishares.csv"
    payload = b"preamble\n" * 9 + (
        b"Ticker,Name,Market Currency\n"
        b"AAA,Example,USD\n"
    )

    # Act and assert
    with (
        patch("index_etfs.sources.read_url", return_value=io.BytesIO(payload)),
        pytest.raises(ValueError, match=r"Weight \(%\) or Market Weight"),
    ):
        load_ishares_csv(url)


@pytest.mark.parametrize(
    ("provider", "loader_name"),
    [
        ("invesco", "load_invesco_json"),
        ("ark", "load_ark_csv"),
        ("globalx", "load_globalx_csv"),
        ("vanguard", "load_vanguard_json"),
        ("firsttrust", "load_firsttrust_html"),
        ("flexshares", "load_flexshares_csv"),
        ("tema", "load_tema_csv"),
        ("amplify", "load_amplify_json"),
    ],
)
def test_load_holdings_dispatches_new_providers(provider: str, loader_name: str) -> None:
    """Call the loader assigned to each newly supported provider key."""
    # Arrange
    url = "https://example.com/holdings"
    config = ETFConfig(
        url=url,
        provider=provider,
        watchlist_name="test",
        expected_count=1,
    )
    expected = pl.DataFrame({"Ticker": ["AAA"]})
    loader_names = [
        "load_ssga_excel",
        "load_vaneck_excel",
        "load_ishares_csv",
        "load_nasdaq_json",
        "load_invesco_json",
        "load_ark_csv",
        "load_globalx_csv",
        "load_vanguard_json",
        "load_firsttrust_html",
        "load_flexshares_csv",
        "load_tema_csv",
        "load_amplify_json",
    ]

    # Act
    with ExitStack() as stack:
        mocks = {
            name: stack.enter_context(patch.object(sources, name, return_value=expected))
            for name in loader_names
        }
        holdings = sources.load_holdings(config)

    # Assert
    assert holdings is expected
    for name, mock_loader in mocks.items():
        if name == loader_name:
            mock_loader.assert_called_once_with(url)
        else:
            mock_loader.assert_not_called()


def test_load_nasdaq_json_normalizes_only_provider_symbols() -> None:
    """Strip Nasdaq symbols and replace slashes before cleaning holdings."""
    # Arrange
    url = "https://example.com/nasdaq.json"
    fixture = _fixture_stream("nasdaq_symbols.json")

    # Act
    with patch("index_etfs.sources.read_url", return_value=fixture) as mock_read_url:
        holdings = load_nasdaq_json(url)

    cleaned = filter_and_clean(holdings, "nasdaq")

    # Assert
    mock_read_url.assert_called_once_with(url)
    assert holdings["Ticker"].to_list() == [
        "MSFT",
        "BRK.B",
        "AAA",
        "BRK.B",
        "WITH SPACE",
        "WITH_UNDERSCORE",
    ]
    assert cleaned["Ticker"].to_list() == ["AAA", "BRK.B", "MSFT"]


@pytest.mark.parametrize(
    ("payload", "message"),
    [
        (b'{"data":{}}', "Nasdaq response"),
        (b'{"data":{"data":{"rows":[]}}}', "Nasdaq response"),
        (b'{"data":{"data":{"rows":[{}]}}}', "symbols"),
    ],
)
def test_load_nasdaq_json_rejects_missing_or_empty_symbols(payload: bytes, message: str) -> None:
    """Reject Nasdaq responses without usable rows or symbols."""
    # Arrange
    url = "https://example.com/nasdaq.json"

    # Act and assert
    with patch(
        "index_etfs.sources.read_url", return_value=io.BytesIO(payload)
    ), pytest.raises(ValueError, match=message):
        load_nasdaq_json(url)


def test_load_nasdaq_json_rejects_invalid_json() -> None:
    """Allow malformed JSON to fail with the decoder's error."""
    # Arrange
    url = "https://example.com/nasdaq.json"

    # Act and assert
    with patch(
        "index_etfs.sources.read_url", return_value=io.BytesIO(b"not JSON")
    ), pytest.raises(json.JSONDecodeError):
        load_nasdaq_json(url)
