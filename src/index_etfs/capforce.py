"""Load CapForce holdings from the JSON block behind its Download Holdings button."""

import json
import urllib.request
from html.parser import HTMLParser

import polars as pl

from .http import fetch_bytes

CAPFORCE_HOLDINGS_ATTRIBUTE = "data-holdings-json"


class _HoldingsJsonParser(HTMLParser):
    """Capture the text of the first script tagged with the holdings JSON attribute."""

    def __init__(self) -> None:
        """Initialize empty capture state for the holdings script."""
        super().__init__()
        self.block: str | None = None
        self._capturing = False
        self._parts: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        """Start capturing when the first marked script opens."""
        # 🧾 Only the first marked script is used; later copies are ignored.
        if tag != "script" or self.block is not None:
            return
        if any(name == CAPFORCE_HOLDINGS_ATTRIBUTE for name, _value in attrs):
            self._capturing = True

    def handle_data(self, data: str) -> None:
        """Collect script text while the holdings block is open."""
        if self._capturing:
            self._parts.append(data)

    def handle_endtag(self, tag: str) -> None:
        """Save the captured script text when its closing tag arrives."""
        if tag == "script" and self._capturing:
            self.block = "".join(self._parts)
            self._capturing = False


def _extract_holdings_json(page: str) -> object:
    """Return the parsed JSON from the page's holdings block or fail descriptively."""
    parser = _HoldingsJsonParser()
    parser.feed(page)
    parser.close()
    if parser.block is None:
        raise ValueError("CapForce page is missing the holdings JSON block")

    try:
        return json.loads(parser.block)
    except json.JSONDecodeError as exc:
        raise ValueError("CapForce holdings JSON is not valid JSON") from exc


def load_capforce_html(url: str) -> pl.DataFrame:
    """Fetch a CapForce holdings page and return stripped equity tickers."""
    request = urllib.request.Request(url)
    holdings = _extract_holdings_json(fetch_bytes(request).decode("utf-8"))
    if not isinstance(holdings, list):
        raise ValueError(  # noqa: TRY004  # Malformed payloads consistently raise ValueError.
            "CapForce holdings JSON must be a list"
        )

    tickers = []
    for row in holdings:
        if not isinstance(row, dict):
            raise ValueError(  # noqa: TRY004  # Malformed payloads consistently raise ValueError.
                "CapForce holding must be a JSON object"
            )
        holding_type = row.get("holding_type")
        if not isinstance(holding_type, str):
            raise ValueError(  # noqa: TRY004  # Malformed payloads consistently raise ValueError.
                "CapForce holding must have a string holding_type"
            )
        if holding_type != "equity":
            continue

        # 🧾 Equity rows may have a blank or null ticker, but the key must exist.
        if "holding_ticker" not in row:
            raise ValueError("CapForce equity holding must include holding_ticker")
        ticker = row["holding_ticker"]
        if ticker is None:
            continue
        if not isinstance(ticker, str):
            raise ValueError(  # noqa: TRY004  # Malformed payloads consistently raise ValueError.
                "CapForce equity holding must have a string holding_ticker"
            )

        ticker = ticker.strip()
        if ticker:
            tickers.append(ticker)

    return pl.DataFrame({"Ticker": pl.Series("Ticker", tickers, dtype=pl.String)})
