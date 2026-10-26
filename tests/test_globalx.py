"""Test Global X holdings downloads and ticker cleanup."""

import urllib.error
from datetime import UTC, datetime
from unittest.mock import patch

import polars as pl
import pytest

from index_etfs.globalx import load_globalx_csv

HOLDINGS_URL = "https://www.globalxetfs.com/funds/aiq/"
CSV_HEADER = "% of Net Assets,Ticker,Name,SEDOL,Market Price ($),Shares Held,Market Value ($)"


class FrozenDateTime(datetime):
    """Provide a stable UTC date to provider request tests."""

    @classmethod
    def now(cls, tz: object = UTC) -> datetime:
        """Return the fixed current date in the requested timezone."""
        return cls(2026, 5, 10, tzinfo=tz or UTC)


def _payload(rows: str = "") -> bytes:
    """Build a small Global X CSV response with its two preamble lines."""
    return f"Global X ETFs\nHoldings as of 05/08/2026\n{CSV_HEADER}\n{rows}".encode()


def _http_error(code: int) -> urllib.error.HTTPError:
    """Create an HTTP error for a mocked holdings request."""
    return urllib.error.HTTPError("https://assets.globalxetfs.com/", code, "error", {}, None)


def test_load_globalx_csv_falls_back_from_weekend_to_available_snapshot() -> None:
    """Try each UTC calendar date until a holdings snapshot is found."""
    # Arrange
    payload = _payload("1.0,AAA,Alpha,123,10,1,10\n")
    with (
        patch("index_etfs.globalx.datetime", FrozenDateTime),
        patch(
            "index_etfs.globalx.fetch_bytes",
            side_effect=[_http_error(404), _http_error(404), payload],
        ) as fetch_bytes,
    ):
        # Act
        holdings = load_globalx_csv(HOLDINGS_URL)

    # Assert
    base = "https://assets.globalxetfs.com/funds/holdings/aiq_full-holdings_"
    assert [entry.args[0].full_url for entry in fetch_bytes.call_args_list] == [
        base + "20260510.csv",
        base + "20260509.csv",
        base + "20260508.csv",
    ]
    assert all(entry.args[0].get_header("User-agent") for entry in fetch_bytes.call_args_list)
    assert holdings.equals(pl.DataFrame({"Ticker": ["AAA"]}))


def test_load_globalx_csv_uses_lowercase_fund_symbol_in_asset_url() -> None:
    """Use the lowercase fund path symbol in the official snapshot URL."""
    # Arrange
    with (
        patch("index_etfs.globalx.datetime", FrozenDateTime),
        patch("index_etfs.globalx.fetch_bytes", return_value=_payload()) as fetch_bytes,
    ):
        # Act
        load_globalx_csv(HOLDINGS_URL)

    # Assert
    request = fetch_bytes.call_args.args[0]
    assert request.full_url == (
        "https://assets.globalxetfs.com/funds/holdings/aiq_full-holdings_20260510.csv"
    )


def test_load_globalx_csv_exhausts_eight_dates() -> None:
    """Stop after the current UTC date and seven preceding calendar dates."""
    # Arrange
    with (
        patch("index_etfs.globalx.datetime", FrozenDateTime),
        patch("index_etfs.globalx.fetch_bytes", side_effect=[_http_error(404)] * 8) as fetch_bytes,
        pytest.raises(FileNotFoundError, match="AIQ"),
    ):
        # Act
        load_globalx_csv(HOLDINGS_URL)

    assert fetch_bytes.call_count == 8
    attempted_urls = [entry.args[0].full_url for entry in fetch_bytes.call_args_list]
    assert attempted_urls[0].endswith("aiq_full-holdings_20260510.csv")
    assert attempted_urls[-1].endswith("aiq_full-holdings_20260503.csv")


def test_load_globalx_csv_propagates_non_404_http_errors() -> None:
    """Do not fall back to an older date for errors other than HTTP 404."""
    # Arrange
    error = _http_error(500)
    with (
        patch("index_etfs.globalx.datetime", FrozenDateTime),
        patch("index_etfs.globalx.fetch_bytes", side_effect=error) as fetch_bytes,
        pytest.raises(urllib.error.HTTPError) as raised,
    ):
        # Act
        load_globalx_csv(HOLDINGS_URL)

    assert raised.value is error
    fetch_bytes.assert_called_once()


def test_load_globalx_csv_rejects_malformed_schema_without_fallback() -> None:
    """Treat a successful but malformed CSV as an error, not a missing date."""
    # Arrange
    malformed = b"Global X\nHoldings as of 05/08/2026\nTicker,Name\nAAA,Alpha\n"
    with (
        patch("index_etfs.globalx.datetime", FrozenDateTime),
        patch("index_etfs.globalx.fetch_bytes", return_value=malformed) as fetch_bytes,
        pytest.raises(ValueError, match="schema"),
    ):
        # Act
        load_globalx_csv(HOLDINGS_URL)

    fetch_bytes.assert_called_once()


def test_load_globalx_csv_normalizes_and_filters_tickers() -> None:
    """Normalize slash share classes and reject cash, qualified, and invalid symbols."""
    # Arrange
    payload = _payload(
        "1, brk/b ,Berkshire,123,1,1,1\n"
        "1,abc,Lowercase,123,1,1,1\n"
        "1,,Cash,123,1,1,1\n"
        "1,NYSE:ABC,Qualified,123,1,1,1\n"
        "1,TOOLONGSYMBOL,Long,123,1,1,1\n"
        "1,BAD_SYMBOL,Invalid,123,1,1,1\n"
        "1,BRK.B,Already normalized,123,1,1,1\n"
    )
    with (
        patch("index_etfs.globalx.datetime", FrozenDateTime),
        patch("index_etfs.globalx.fetch_bytes", return_value=payload),
    ):
        # Act
        holdings = load_globalx_csv(HOLDINGS_URL)

    # Assert
    assert holdings.schema == {"Ticker": pl.String}
    assert holdings["Ticker"].to_list() == ["BRK.B", "ABC", "BRK.B"]
