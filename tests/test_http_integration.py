"""Test retry integration for public holdings downloads."""

import io
import urllib.error
from email.message import Message
from unittest.mock import patch

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
