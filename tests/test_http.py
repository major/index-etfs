"""Test bounded retries for public HTTP data requests."""

import http.client
import io
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from email.message import Message
from email.utils import format_datetime
from unittest.mock import MagicMock, call, patch

import pytest

from index_etfs.http import fetch_bytes


def _make_http_error(status: int, headers: Message | None = None) -> tuple[urllib.error.HTTPError, io.BytesIO]:
    """Build an HTTP error with a trackable response body."""
    body = io.BytesIO(b"error body")
    error = urllib.error.HTTPError(
        "https://example.com/holdings",
        status,
        "request failed",
        headers or Message(),
        body,
    )
    return error, body


def test_fetch_bytes_sends_request_with_timeout_and_reads_response() -> None:
    """Pass the original request and a 30-second timeout to urlopen."""
    # Arrange
    request = urllib.request.Request(
        "https://example.com/holdings", headers={"X-Test": "request header"}
    )
    response = MagicMock()
    response.__enter__.return_value = response
    response.read.return_value = b"holdings"

    # Act
    with patch("index_etfs.http.urllib.request.urlopen", return_value=response) as urlopen:
        result = fetch_bytes(request)

    # Assert
    assert result == b"holdings"
    urlopen.assert_called_once_with(request, timeout=30)
    assert request.get_header("X-test") == "request header"
    response.__exit__.assert_called_once()


@pytest.mark.parametrize(
    "error",
    [
        urllib.error.URLError("offline"),
        TimeoutError("timed out"),
        ConnectionError("connection reset"),
        http.client.IncompleteRead(b"partial"),
    ],
)
def test_fetch_bytes_retries_transient_url_errors(error: Exception) -> None:
    """Retry supported URL and connection failures before returning bytes."""
    # Arrange
    request = urllib.request.Request("https://example.com/holdings")
    response = io.BytesIO(b"holdings")

    # Act
    with (
        patch("index_etfs.http.urllib.request.urlopen", side_effect=[error, response]) as urlopen,
        patch("index_etfs.http.time.sleep") as sleep,
    ):
        result = fetch_bytes(request)

    # Assert
    assert result == b"holdings"
    assert urlopen.call_count == 2
    sleep.assert_called_once_with(1)


@pytest.mark.parametrize("status", [429, 500, 502, 503, 504])
def test_fetch_bytes_retries_selected_http_statuses(status: int) -> None:
    """Retry the selected transient HTTP status codes and close error bodies."""
    # Arrange
    request = urllib.request.Request("https://example.com/holdings")
    error, body = _make_http_error(status)
    response = io.BytesIO(b"holdings")

    # Act
    with (
        patch("index_etfs.http.urllib.request.urlopen", side_effect=[error, response]) as urlopen,
        patch("index_etfs.http.time.sleep") as sleep,
    ):
        result = fetch_bytes(request)

    # Assert
    assert result == b"holdings"
    assert urlopen.call_count == 2
    sleep.assert_called_once_with(1)
    assert body.closed


@pytest.mark.parametrize("status", [400, 401, 404])
def test_fetch_bytes_does_not_retry_permanent_http_errors(status: int) -> None:
    """Raise permanent client errors after one attempt and close their bodies."""
    # Arrange
    request = urllib.request.Request("https://example.com/holdings")
    error, body = _make_http_error(status)

    # Act and assert
    with (
        patch("index_etfs.http.urllib.request.urlopen", side_effect=error) as urlopen,
        patch("index_etfs.http.time.sleep") as sleep,
        pytest.raises(urllib.error.HTTPError) as raised,
    ):
        fetch_bytes(request)

    assert raised.value is error
    urlopen.assert_called_once_with(request, timeout=30)
    sleep.assert_not_called()
    assert body.closed


def test_fetch_bytes_retries_response_read_failures() -> None:
    """Retry an incomplete response read and close the failed response."""
    # Arrange
    request = urllib.request.Request("https://example.com/holdings")
    incomplete_response = MagicMock()
    incomplete_response.__enter__.return_value = incomplete_response
    incomplete_response.read.side_effect = http.client.IncompleteRead(b"partial")
    response = io.BytesIO(b"complete holdings")

    # Act
    with (
        patch(
            "index_etfs.http.urllib.request.urlopen",
            side_effect=[incomplete_response, response],
        ) as urlopen,
        patch("index_etfs.http.time.sleep") as sleep,
    ):
        result = fetch_bytes(request)

    # Assert
    assert result == b"complete holdings"
    assert urlopen.call_count == 2
    sleep.assert_called_once_with(1)
    incomplete_response.__exit__.assert_called_once()


def test_fetch_bytes_raises_original_error_with_context_after_three_attempts() -> None:
    """Make three attempts, add request context, and avoid a final sleep."""
    # Arrange
    request = urllib.request.Request("https://example.com/holdings")
    error = TimeoutError("upstream timed out")

    # Act
    with (
        patch("index_etfs.http.urllib.request.urlopen", side_effect=error) as urlopen,
        patch("index_etfs.http.time.sleep") as sleep,
        pytest.raises(TimeoutError) as raised,
    ):
        fetch_bytes(request)

    # Assert
    assert raised.value is error
    assert urlopen.call_count == 3
    assert urlopen.call_args_list == [call(request, timeout=30)] * 3
    assert sleep.call_args_list == [call(1), call(2)]
    assert any("https://example.com/holdings" in note for note in error.__notes__)
    assert any("3 attempts" in note for note in error.__notes__)


@pytest.mark.parametrize(
    ("retry_after", "expected_delay"),
    [("2", 2), ("99", 5), ("0", 0), ("-1", 1), ("invalid", 1)],
)
def test_fetch_bytes_handles_retry_after_seconds(
    retry_after: str, expected_delay: int
) -> None:
    """Honor valid Retry-After seconds, cap long delays, and fall back when invalid."""
    # Arrange
    request = urllib.request.Request("https://example.com/holdings")
    headers = Message()
    headers["Retry-After"] = retry_after
    error, _ = _make_http_error(429, headers)
    response = io.BytesIO(b"holdings")

    # Act
    with (
        patch("index_etfs.http.urllib.request.urlopen", side_effect=[error, response]),
        patch("index_etfs.http.time.sleep") as sleep,
    ):
        result = fetch_bytes(request)

    # Assert
    assert result == b"holdings"
    sleep.assert_called_once_with(expected_delay)


def test_fetch_bytes_caps_retry_after_http_date() -> None:
    """Parse an HTTP-date Retry-After value and cap its delay at five seconds."""
    # Arrange
    request = urllib.request.Request("https://example.com/holdings")
    headers = Message()
    headers["Retry-After"] = format_datetime(
        datetime.now(timezone.utc) + timedelta(seconds=30), usegmt=True
    )
    error, _ = _make_http_error(503, headers)
    response = io.BytesIO(b"holdings")

    # Act
    with (
        patch("index_etfs.http.urllib.request.urlopen", side_effect=[error, response]),
        patch("index_etfs.http.time.sleep") as sleep,
    ):
        result = fetch_bytes(request)

    # Assert
    assert result == b"holdings"
    sleep.assert_called_once_with(5)


def test_fetch_bytes_parses_asctime_retry_after_date_as_utc() -> None:
    """Treat the timezone-free obsolete HTTP-date format as UTC."""
    # Arrange
    request = urllib.request.Request("https://example.com/holdings")
    headers = Message()
    headers["Retry-After"] = datetime(2099, 1, 1, tzinfo=timezone.utc).strftime(
        "%a %b %d %H:%M:%S %Y"
    )
    error, _ = _make_http_error(503, headers)
    response = io.BytesIO(b"holdings")

    # Act
    with (
        patch("index_etfs.http.urllib.request.urlopen", side_effect=[error, response]),
        patch("index_etfs.http.time.sleep") as sleep,
    ):
        result = fetch_bytes(request)

    # Assert
    assert result == b"holdings"
    sleep.assert_called_once_with(5)
