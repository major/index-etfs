"""Fetch public HTTP data with retries for safe, read-only requests only."""

import http.client
import time
import urllib.error
import urllib.request
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime

MAX_ATTEMPTS = 3
RETRYABLE_STATUS_CODES = frozenset({429, 500, 502, 503, 504})
MAX_RETRY_AFTER_SECONDS = 5


def _retry_delay(attempt: int, retry_after: str | None) -> float:
    """Return a capped Retry-After delay or the attempt's short fallback."""
    fallback = attempt + 1
    if retry_after is None:
        return fallback

    value = retry_after.strip()
    try:
        seconds = int(value)
    except ValueError:
        try:
            retry_date = parsedate_to_datetime(value)
            if retry_date.tzinfo is None:
                retry_date = retry_date.replace(tzinfo=UTC)
            seconds = (retry_date - datetime.now(UTC)).total_seconds()
        except (IndexError, OverflowError, TypeError, ValueError):
            return fallback

    if seconds < 0:
        return fallback
    return min(seconds, MAX_RETRY_AFTER_SECONDS)


def fetch_bytes(request: urllib.request.Request) -> bytes:
    """Fetch all response bytes, retrying transient failures up to three times.

    Only use this helper with safe, read-only requests because requests, including
    POST requests, may be sent more than once.
    """
    for attempt in range(MAX_ATTEMPTS):
        retry_after = None
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                return response.read()
        except urllib.error.HTTPError as error:
            if error.code not in RETRYABLE_STATUS_CODES:
                error.close()
                raise

            headers = error.headers
            retry_after = headers.get("Retry-After") if headers is not None else None
            error.close()
            last_error = error
        except (
            urllib.error.URLError,
            TimeoutError,
            ConnectionError,
            http.client.IncompleteRead,
        ) as error:
            last_error = error

        if attempt == MAX_ATTEMPTS - 1:
            last_error.add_note(
                f"Request URL: {request.full_url}; failed after {MAX_ATTEMPTS} attempts."
            )
            raise last_error

        time.sleep(_retry_delay(attempt, retry_after))
