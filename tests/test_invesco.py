"""Tests for parsing Invesco's JSON holdings feed."""

import json
from unittest.mock import patch

import polars as pl
import pytest

from index_etfs.invesco import load_invesco_json


def _holding(ticker: str | None, security_type: str | None, currency: str | None) -> dict:
    """Return one compact Invesco holding record."""
    return {
        "ticker": ticker,
        "securityTypeCode": security_type,
        "currency": currency,
    }


def test_load_invesco_json_keeps_supported_us_equity_symbols() -> None:
    """Retain the supported equity types and normalize slash share classes."""
    # Arrange
    url = "https://example.com/invesco.json"
    payload = {
        "metadata": {"asOfDate": "2026-01-01"},
        "holdings": [
            _holding("MSFT", "COM", "USD"),
            _holding("ABC/A", "ADR", "USD"),
            _holding("XYZ", "DR", "USD"),
            _holding("REIT1", "REIT", "USD"),
            _holding("CASH", "CASH", "USD"),
            _holding("7203 JP", "COM", "JPY"),
            _holding("TOO-LONG-SYMBOL", "COM", "USD"),
            _holding("lower", "COM", "USD"),
            _holding(None, "COM", "USD"),
            _holding("NULLTYPE", None, "USD"),
            _holding("NULLCURRENCY", "COM", None),
        ],
    }

    # Act
    with patch("index_etfs.invesco.fetch_bytes", return_value=json.dumps(payload).encode()) as fetch:
        holdings = load_invesco_json(url)

    # Assert
    fetch.assert_called_once()
    request = fetch.call_args.args[0]
    assert request.full_url == url
    assert request.get_header("User-agent")
    assert holdings.columns == ["Ticker"]
    assert holdings.schema == {"Ticker": pl.String}
    assert holdings["Ticker"].to_list() == ["MSFT", "ABC.A", "XYZ", "REIT1"]


@pytest.mark.parametrize(
    ("payload", "message"),
    [
        ({"metadata": {}}, "holdings"),
        ({"holdings": [{}]}, "ticker"),
        ({"holdings": [{"ticker": "ABC", "currency": "USD"}]}, "securityTypeCode"),
        ({"holdings": [{"ticker": "ABC", "securityTypeCode": "COM"}]}, "currency"),
    ],
)
def test_load_invesco_json_rejects_missing_required_fields(payload: dict, message: str) -> None:
    """Fail clearly when the feed or a holding omits required fields."""
    # Arrange
    url = "https://example.com/invesco.json"

    # Act and assert
    with (
        patch("index_etfs.invesco.fetch_bytes", return_value=json.dumps(payload).encode()),
        pytest.raises(ValueError, match=message),
    ):
        load_invesco_json(url)


def test_load_invesco_json_returns_empty_string_typed_frame_without_equities() -> None:
    """Return a typed empty ticker column when no valid equities are present."""
    # Arrange
    url = "https://example.com/invesco.json"
    payload = {"metadata": {}, "holdings": [_holding("CASH", "CASH", "USD")]}

    # Act
    with patch("index_etfs.invesco.fetch_bytes", return_value=json.dumps(payload).encode()):
        holdings = load_invesco_json(url)

    # Assert
    assert holdings.columns == ["Ticker"]
    assert holdings.schema == {"Ticker": pl.String}
    assert holdings.is_empty()
