"""Tests for parsing Vanguard's additional fund data JSON feed."""

import json
from unittest.mock import patch

import polars as pl
import pytest

from index_etfs.vanguard import load_vanguard_json


def _holding(ticker: str | None, security_main_type: str) -> dict:
    """Return one compact Vanguard holding record."""
    return {"ticker": ticker, "securityMainType": security_main_type}


def test_load_vanguard_json_keeps_equities_and_normalizes_share_classes() -> None:
    """Keep valid equities and exclude funds and non-US-style ticker symbols."""
    # Arrange
    url = "https://example.com/vanguard.json"
    payload = {
        "holdingDetails": {
            "equityHoldings": [
                _holding("MSFT", "EQ"),
                _holding("BRK/B", "EQ"),
                _holding("VRTPX", "MF"),
                _holding(None, "EQ"),
                _holding("7203 JP", "EQ"),
                _holding("NYSE:ABC", "EQ"),
            ]
        }
    }

    # Act
    with patch("index_etfs.vanguard.fetch_bytes", return_value=json.dumps(payload).encode()) as fetch:
        holdings = load_vanguard_json(url)

    # Assert
    fetch.assert_called_once()
    request = fetch.call_args.args[0]
    assert request.full_url == url
    assert request.get_header("User-agent")
    assert holdings.columns == ["Ticker"]
    assert holdings.schema == {"Ticker": pl.String}
    assert holdings["Ticker"].to_list() == ["MSFT", "BRK.B"]


@pytest.mark.parametrize(
    "payload",
    [
        {},
        {"holdingDetails": {}},
        {"holdingDetails": {"equityHoldings": {}}},
        {"holdingDetails": {"equityHoldings": [{}]}},
    ],
)
def test_load_vanguard_json_rejects_missing_required_structure(payload: dict) -> None:
    """Fail clearly when the response or holding omits required structure."""
    # Arrange
    url = "https://example.com/vanguard.json"

    # Act and assert
    with (
        patch("index_etfs.vanguard.fetch_bytes", return_value=json.dumps(payload).encode()),
        pytest.raises((TypeError, ValueError)),
    ):
        load_vanguard_json(url)


def test_load_vanguard_json_returns_typed_empty_frame_without_valid_equities() -> None:
    """Return an empty string-typed ticker column when no equity passes filtering."""
    # Arrange
    url = "https://example.com/vanguard.json"
    payload = {"holdingDetails": {"equityHoldings": [_holding("VRTPX", "MF")]}}

    # Act
    with patch("index_etfs.vanguard.fetch_bytes", return_value=json.dumps(payload).encode()):
        holdings = load_vanguard_json(url)

    # Assert
    assert holdings.columns == ["Ticker"]
    assert holdings.schema == {"Ticker": pl.String}
    assert holdings.is_empty()


def test_load_vanguard_json_skips_equity_without_ticker() -> None:
    """Skip an equity holding whose issuer record has no ticker field."""
    # Arrange
    url = "https://example.com/vanguard.json"
    payload = {"holdingDetails": {"equityHoldings": [{"securityMainType": "EQ"}]}}

    # Act
    with patch("index_etfs.vanguard.fetch_bytes", return_value=json.dumps(payload).encode()):
        holdings = load_vanguard_json(url)

    # Assert
    assert holdings.columns == ["Ticker"]
    assert holdings.schema == {"Ticker": pl.String}
    assert holdings.is_empty()
