"""Load and filter FlexShares and Tema ETF holdings CSV files."""

import csv
import html
import io
import re
import urllib.request
from html.parser import HTMLParser
from urllib.parse import urljoin, urlparse

import polars as pl

from .http import fetch_bytes

SPECIALTY_USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64; rv:140.0) Gecko/20100101 Firefox/140.0"
)
TEMA_CSV_HOST = "temaetfs.com"
TEMA_CSV_PATH = re.compile(
    r"/hubfs/Website/Holdings/VOLT-holdings-[A-Za-z0-9_-]+\.csv"
)
TICKER_PATTERN = re.compile(r"[A-Z][A-Z0-9]*(?:[.-][A-Z])?")
NON_EQUITY_TICKERS = frozenset({"CASH", "USD", "TOTAL", "SUBTOTAL", "PRIVATE"})
TEMA_REQUIRED_COLUMNS = frozenset(
    {
        "holdings_date",
        "ticker",
        "cusip",
        "proper_name",
        "shares",
        "market_value",
        "percent_of_nav",
        "is_cash",
        "country",
        "sector",
    }
)


class _HrefParser(HTMLParser):
    """Collect href attribute values from an HTML page."""

    def __init__(self) -> None:
        """Initialize the parser with an empty list of links."""
        super().__init__()
        self.hrefs: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        """Save href attributes from each opening HTML tag."""
        for name, value in attrs:
            if name.casefold() == "href" and value is not None:
                self.hrefs.append(value)


def _read_csv_records(
    payload: bytes, provider: str, required_columns: frozenset[str]
) -> tuple[list[str], list[dict[str, str]]]:
    """Decode a provider CSV and validate its header and row widths."""
    reader = csv.reader(io.StringIO(payload.decode("utf-8-sig"), newline=""), strict=True)
    header = next(reader, None)
    if not header:
        raise ValueError(f"{provider} CSV is missing its required header")

    columns = [column.strip().casefold() for column in header]
    if len(columns) != len(set(columns)):
        raise ValueError(f"{provider} CSV has duplicate column names")

    missing_columns = sorted(required_columns - set(columns))
    if missing_columns:
        missing = ", ".join(missing_columns)
        raise ValueError(f"{provider} CSV is missing required columns: {missing}")

    records = []
    for row_number, values in enumerate(reader, start=2):
        if not values or (len(values) == 1 and not values[0].strip()):
            continue
        if len(values) != len(columns):
            raise ValueError(f"{provider} CSV has a malformed row at line {row_number}")
        records.append(dict(zip(columns, values, strict=True)))
    return columns, records


def _ticker_or_none(value: str) -> str | None:
    """Normalize a ticker and reject symbols outside the supported listing shape."""
    ticker = value.strip().upper().replace("/", ".")
    if (
        len(ticker) > 10
        or TICKER_PATTERN.fullmatch(ticker) is None
        or ticker in NON_EQUITY_TICKERS
    ):
        return None
    return ticker


def _ticker_frame(tickers: list[str]) -> pl.DataFrame:
    """Build a frame that always has one string-typed Ticker column."""
    return pl.DataFrame({"Ticker": tickers}, schema={"Ticker": pl.String})


def _is_flexshares_cash_name(name: str) -> bool:
    """Identify whole-word cash and exact currency descriptions."""
    normalized = " ".join(name.casefold().split())
    return re.search(r"\bcash\b", normalized) is not None or normalized in {
        "u.s. dollar",
        "us dollar",
        "foreign currency",
        "currency",
    }


def load_flexshares_csv(url: str) -> pl.DataFrame:
    """Fetch FlexShares holdings and return only valid equity ticker symbols."""
    request = urllib.request.Request(url, headers={"User-Agent": SPECIALTY_USER_AGENT})
    payload = fetch_bytes(request)
    columns, records = _read_csv_records(payload, "FlexShares", frozenset({"ticker"}))
    has_name = "name" in columns

    tickers = []
    for record in records:
        ticker = _ticker_or_none(record["ticker"])
        if ticker is None:
            continue
        if has_name and _is_flexshares_cash_name(record["name"]):
            continue
        tickers.append(ticker)
    return _ticker_frame(tickers)


def _find_tema_csv_url(page_url: str, page_html: str) -> str:
    """Find an official dated VOLT holdings CSV link in Tema page HTML."""
    parser = _HrefParser()
    parser.feed(page_html)
    for raw_href in parser.hrefs:
        candidate = urljoin(page_url, html.unescape(raw_href.strip()))
        parsed = urlparse(candidate)
        if (
            parsed.scheme.casefold() == "https"
            and parsed.netloc.casefold() == TEMA_CSV_HOST
            and TEMA_CSV_PATH.fullmatch(parsed.path) is not None
        ):
            return candidate
    raise ValueError("Tema page did not contain an official VOLT holdings CSV link")


def _is_true(value: str) -> bool:
    """Return whether a CSV boolean field uses a recognized true value."""
    return value.strip().casefold() in {"true", "1", "yes", "y"}


def load_tema_csv(url: str) -> pl.DataFrame:
    """Discover and load Tema's current VOLT CSV, returning equity tickers only."""
    page_request = urllib.request.Request(
        url, headers={"User-Agent": SPECIALTY_USER_AGENT}
    )
    page_html = fetch_bytes(page_request).decode("utf-8-sig")
    csv_url = _find_tema_csv_url(url, page_html)

    csv_request = urllib.request.Request(
        csv_url, headers={"User-Agent": SPECIALTY_USER_AGENT}
    )
    _, records = _read_csv_records(
        fetch_bytes(csv_request), "Tema", TEMA_REQUIRED_COLUMNS
    )

    tickers = []
    for record in records:
        if _is_true(record["is_cash"]):
            continue
        if "private" in record["proper_name"].casefold():
            continue
        ticker = _ticker_or_none(record["ticker"])
        if ticker is not None:
            tickers.append(ticker)
    return _ticker_frame(tickers)
