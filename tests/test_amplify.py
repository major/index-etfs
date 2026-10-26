"""Test parsing and filtering of Amplify Firestore holdings."""

import json
from unittest.mock import patch

import polars as pl
import pytest

from index_etfs.amplify import load_amplify_json

AMPLIFY_URL = (
    "https://firestore.googleapis.com/v1/projects/amplify-etfs-data-feed/"
    "databases/%28default%29/documents/funds/BLOK/holdings?pageSize=1&"
    "orderBy=__name__%20desc"
)


def _holding(ticker: str, name: str, holding_type: str = "equity") -> dict:
    """Build one Firestore typed-value holding record."""
    return {
        "mapValue": {
            "fields": {
                "StockTicker": {"stringValue": ticker},
                "SecurityName": {"stringValue": name},
                "holding_type": {"stringValue": holding_type},
            }
        }
    }


def _document(*holdings: dict) -> dict:
    """Build a Firestore document containing typed holding records."""
    return {
        "fields": {
            "holdings": {
                "arrayValue": {"values": list(holdings)}
            }
        }
    }


def _payload(*documents: dict) -> bytes:
    """Encode Firestore documents as UTF-8 JSON bytes."""
    return json.dumps({"documents": list(documents)}).encode()


def test_load_amplify_json_keeps_listed_equities_and_funds() -> None:
    """Normalize valid listed symbols and exclude non-equity or invalid rows."""
    # Arrange
    payload = _payload(
        _document(
            _holding("BRK/B", "Berkshire Hathaway Inc."),
            _holding("arkb", "ARK 21Shares Bitcoin ETF"),
            _holding("BTC", "Bitcoin"),
            _holding("ETH", "Ethereum"),
            _holding("HODL", "VanEck Bitcoin ETF"),
            _holding("CASH", "Cash balance", "cash"),
            _holding("USD", "Money market fund", "money_market"),
            _holding("ABC US", "Foreign qualified listing"),
            _holding("TOO-LONGTICKER", "Invalid long symbol"),
            _holding("A/B/C", "Invalid slash symbol"),
        )
    )

    # Act
    with patch("index_etfs.amplify.fetch_bytes", return_value=payload) as fetch:
        holdings = load_amplify_json(AMPLIFY_URL)

    # Assert
    fetch.assert_called_once()
    request = fetch.call_args.args[0]
    assert request.full_url == AMPLIFY_URL
    assert request.get_header("User-agent").startswith("Mozilla/")
    assert holdings.columns == ["Ticker"]
    assert holdings.schema == {"Ticker": pl.String}
    assert holdings["Ticker"].to_list() == ["BRK.B", "ARKB", "BTC", "ETH", "HODL"]


def test_load_amplify_json_uses_first_document_from_latest_first_response() -> None:
    """Use the first Firestore document returned by the descending query."""
    # Arrange
    payload = _payload(
        _document(_holding("LATEST", "Latest position")),
        _document(_holding("OLDER", "Older position")),
    )

    # Act
    with patch("index_etfs.amplify.fetch_bytes", return_value=payload):
        holdings = load_amplify_json(AMPLIFY_URL)

    # Assert
    assert holdings["Ticker"].to_list() == ["LATEST"]


@pytest.mark.parametrize("array_value", [{"values": []}, {}])
def test_load_amplify_json_returns_typed_empty_frame_for_empty_array(
    array_value: dict,
) -> None:
    """Return a string-typed empty ticker frame for an explicit empty array."""
    # Arrange
    payload = json.dumps(
        {"documents": [{"fields": {"holdings": {"arrayValue": array_value}}}]}
    ).encode()

    # Act
    with patch("index_etfs.amplify.fetch_bytes", return_value=payload):
        holdings = load_amplify_json(AMPLIFY_URL)

    # Assert
    assert holdings.columns == ["Ticker"]
    assert holdings.schema == {"Ticker": pl.String}
    assert holdings.is_empty()


@pytest.mark.parametrize(
    "payload",
    [
        b"{}",
        b'{"documents": []}',
        b'{"documents": [{"fields": {}}]}',
        b'{"documents": [{"fields": {"holdings": {}}}]}',
    ],
)
def test_load_amplify_json_rejects_missing_documents_or_holdings(payload: bytes) -> None:
    """Fail closed when Firestore omits a document or holdings array."""
    # Arrange and act
    with (
        patch("index_etfs.amplify.fetch_bytes", return_value=payload),
        pytest.raises(ValueError),
    ):
        load_amplify_json(AMPLIFY_URL)


@pytest.mark.parametrize(
    "holding",
    [
        {},
        {"mapValue": {}},
        {"mapValue": {"fields": {"StockTicker": {"stringValue": "MSFT"}}}},
        {
            "mapValue": {
                "fields": {
                    "StockTicker": {"stringValue": "MSFT"},
                    "SecurityName": {"stringValue": "Microsoft"},
                }
            }
        },
        {
            "mapValue": {
                "fields": {
                    "StockTicker": {"stringValue": "MSFT"},
                    "SecurityName": {"stringValue": "Microsoft"},
                    "holding_type": {"integerValue": "1"},
                }
            }
        },
    ],
)
def test_load_amplify_json_rejects_holdings_without_required_fields(
    holding: dict,
) -> None:
    """Fail closed when a Firestore holding lacks typed required fields."""
    # Arrange
    payload = _payload(_document(holding))

    # Act and assert
    with (
        patch("index_etfs.amplify.fetch_bytes", return_value=payload),
        pytest.raises((ValueError, TypeError), match="required"),
    ):
        load_amplify_json(AMPLIFY_URL)
