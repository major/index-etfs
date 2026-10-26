"""Load and filter holdings from ARK Invest CSV files."""

import csv
import io
import re
import urllib.request

import polars as pl

from .http import fetch_bytes

ARK_USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64; rv:140.0) Gecko/20100101 Firefox/140.0"
)
ARK_REQUIRED_COLUMNS = frozenset({"date", "company", "ticker"})
ARK_TICKER_PATTERN = re.compile(r"[A-Z][A-Z0-9]*(?:[.-][A-Z])?")
ARK_NON_EQUITY_TICKERS = frozenset({"CASH", "USD", "TOTAL", "SUBTOTAL", "GRANDTOTAL"})
ARK_NON_EQUITY_COMPANIES = frozenset(
    {
        "cash",
        "cash and cash equivalents",
        "cash equivalents",
        "u.s. dollar",
        "us dollar",
        "foreign currency",
        "currency",
    }
)


def load_ark_csv(url: str) -> pl.DataFrame:
    """Fetch an ARK holdings CSV and return valid equity ticker symbols only."""
    request = urllib.request.Request(url, headers={"User-Agent": ARK_USER_AGENT})
    content = fetch_bytes(request).decode("utf-8-sig")
    reader = csv.reader(io.StringIO(content, newline=""))
    header = next(reader, None)
    normalized_header = [column.strip().casefold() for column in header or []]
    missing_columns = sorted(ARK_REQUIRED_COLUMNS - set(normalized_header))
    if missing_columns:
        missing = ", ".join(missing_columns)
        raise ValueError(f"ARK CSV is missing required columns: {missing}")

    ticker_index = normalized_header.index("ticker")
    company_index = normalized_header.index("company")
    tickers = []
    for row in reader:
        if not row:
            continue

        raw_ticker = row[ticker_index].strip() if ticker_index < len(row) else ""
        ticker = raw_ticker.upper().replace("/", ".")
        company = row[company_index].strip().casefold() if company_index < len(row) else ""
        if (
            not ticker
            or len(ticker) > 10
            or ARK_TICKER_PATTERN.fullmatch(ticker) is None
            or ticker in ARK_NON_EQUITY_TICKERS
            or company in ARK_NON_EQUITY_COMPANIES
        ):
            continue
        tickers.append(ticker)

    return pl.DataFrame({"Ticker": tickers}, schema={"Ticker": pl.String})
