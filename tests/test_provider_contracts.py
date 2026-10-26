"""Characterize provider file formats using small local fixtures."""

import io
import json
from pathlib import Path
from unittest.mock import patch

import pytest

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
    # Duplicates remain for now. A future unique-output change should update this contract.
    assert cleaned["Ticker"].to_list() == ["AAA", "AAA", "BRK/B", "ZETA"]


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
    # Duplicates remain for now. A future unique-output change should update this contract.
    assert cleaned["Ticker"].to_list() == ["AAA", "AAA", "BRK/B", "ZETA"]


def test_load_nasdaq_json_normalizes_only_provider_symbols() -> None:
    """Strip Nasdaq symbols and replace slashes without removing duplicates."""
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
    # Duplicates remain for now. A future unique-output change should update this contract.
    assert cleaned["Ticker"].to_list() == ["AAA", "BRK.B", "BRK.B", "MSFT"]


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
