"""Characterize parsing and filtering of ARK holdings CSV files."""

from unittest.mock import patch

import polars as pl
import pytest

from index_etfs.ark import load_ark_csv

CSV_HEADER = "date,fund,company,ticker,cusip,shares,market value ($),weight (%)\n"


def test_load_ark_csv_keeps_equity_tickers_and_normalizes_share_classes() -> None:
    """Keep valid US equity symbols while excluding cash and qualified symbols."""
    # Arrange
    url = "https://example.com/ark.csv"
    rows = (
        b"2025-01-01,ARKK,Berkshire Hathaway Inc,BRK/B,084670702,10,100,1\n"
        b"2025-01-01,ARKK,Example Corp,abc,000000001,10,100,1\n"
        b"2025-01-01,ARKK,International Example,EXAMPLE US,000000002,10,100,1\n"
        b"2025-01-01,ARKK,Cash and Cash Equivalents,USD,,100,100,1\n"
        b"2025-01-01,ARKK,Example Corp,TOO-LONGTICKER,000000003,10,100,1\n"
        b"2025-01-01,ARKK,Example Corp,,000000004,10,100,1\n"
        b"2025-01-01,ARKK,Total holdings,TOTAL,,0,0,100\n"
    )

    # Act
    with patch("index_etfs.ark.fetch_bytes", return_value=CSV_HEADER.encode() + rows) as fetch:
        holdings = load_ark_csv(url)

    # Assert
    fetch.assert_called_once()
    request = fetch.call_args.args[0]
    assert request.full_url == url
    assert request.get_header("User-agent").startswith("Mozilla/")
    assert holdings.columns == ["Ticker"]
    assert holdings.schema == {"Ticker": pl.String}
    assert holdings["Ticker"].to_list() == ["BRK.B", "ABC"]


@pytest.mark.parametrize(
    "header",
    [
        "date,fund,company,cusip,shares,market value ($),weight (%)\n",
        "fund,company,ticker,cusip,shares,market value ($),weight (%)\n",
        "date,fund,ticker,cusip,shares,market value ($),weight (%)\n",
    ],
)
def test_load_ark_csv_rejects_missing_required_headers(header: str) -> None:
    """Reject CSV headers without a date, company, or ticker field."""
    # Arrange
    url = "https://example.com/ark.csv"

    # Act and assert
    with (
        patch("index_etfs.ark.fetch_bytes", return_value=header.encode()),
        pytest.raises(ValueError, match="required columns"),
    ):
        load_ark_csv(url)


@pytest.mark.parametrize("payload", [CSV_HEADER.encode(), (CSV_HEADER + "\n").encode()])
def test_load_ark_csv_returns_typed_empty_frame_when_no_equity_holdings(payload: bytes) -> None:
    """Return an empty string-typed ticker frame for valid empty holdings."""
    # Arrange
    url = "https://example.com/ark.csv"

    # Act
    with patch("index_etfs.ark.fetch_bytes", return_value=payload):
        holdings = load_ark_csv(url)

    # Assert
    assert holdings.columns == ["Ticker"]
    assert holdings.schema == {"Ticker": pl.String}
    assert holdings.height == 0
