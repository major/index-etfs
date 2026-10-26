"""Test retry integration for public data downloads and scanner requests."""

import io
import json
import urllib.error
from email.message import Message
from unittest.mock import patch

import pytest

from index_etfs.outputs import TRADINGVIEW_SCAN_URL, tradingview_symbols
from index_etfs.sources import FIREFOX_HEADERS, read_url


def test_read_url_recovers_from_transient_http_error() -> None:
    """Retry a transient source download and return its response bytes."""
    # Arrange
    headers = Message()
    error = urllib.error.HTTPError(
        "https://example.com/holdings.csv", 503, "unavailable", headers, io.BytesIO()
    )
    response = io.BytesIO(b"ticker,data\nAAPL,holdings\n")

    # Act
    with (
        patch("index_etfs.http.urllib.request.urlopen", side_effect=[error, response]) as urlopen,
        patch("index_etfs.http.time.sleep") as sleep,
    ):
        result = read_url("https://example.com/holdings.csv")

    # Assert
    assert result.read() == b"ticker,data\nAAPL,holdings\n"
    assert urlopen.call_count == 2
    request = urlopen.call_args.args[0]
    assert request.get_header("User-agent") == FIREFOX_HEADERS["User-Agent"]
    assert request.get_header("Accept") == FIREFOX_HEADERS["Accept"]
    assert request.get_header("Accept-language") == FIREFOX_HEADERS["Accept-Language"]
    assert sleep.call_args_list == [((1,), {})]


def test_tradingview_scan_recovers_from_transient_http_error() -> None:
    """Retry the read-only scanner POST while preserving its request payload."""
    # Arrange
    headers = Message()
    error = urllib.error.HTTPError(
        TRADINGVIEW_SCAN_URL, 503, "unavailable", headers, io.BytesIO()
    )
    response = io.BytesIO(
        b'{"data":[{"s":"NASDAQ:AAPL","d":["AAPL","Technology"]}]}'
    )

    # Act
    with (
        patch("index_etfs.http.urllib.request.urlopen", side_effect=[error, response]) as urlopen,
        patch("index_etfs.http.time.sleep") as sleep,
    ):
        result = tradingview_symbols(["AAPL"])

    # Assert
    assert result == {"AAPL": ("NASDAQ:AAPL", "Technology")}
    assert urlopen.call_count == 2
    request = urlopen.call_args.args[0]
    assert request.full_url == TRADINGVIEW_SCAN_URL
    assert request.get_method() == "POST"
    assert request.get_header("User-agent") == FIREFOX_HEADERS["User-Agent"]
    assert request.get_header("Content-type") == "application/json"
    assert request.get_header("Origin") == "https://www.tradingview.com"
    assert request.get_header("Referer") == "https://www.tradingview.com/"
    assert json.loads(request.data) == {
        "filter": [{"left": "name", "operation": "in_range", "right": ["AAPL"]}],
        "columns": ["name", "sector"],
        "markets": ["america"],
    }
    assert sleep.call_args_list == [((1,), {})]


def test_tradingview_scan_does_not_retry_malformed_json() -> None:
    """Raise malformed scanner JSON without retrying the successful HTTP read."""
    # Arrange
    response = io.BytesIO(b"not JSON")

    # Act and assert
    with (
        patch("index_etfs.http.urllib.request.urlopen", return_value=response) as urlopen,
        patch("index_etfs.http.time.sleep") as sleep,
        pytest.raises(ValueError, match="Invalid TradingView scanner JSON"),
    ):
        tradingview_symbols(["AAPL"])

    assert urlopen.call_count == 1
    sleep.assert_not_called()
