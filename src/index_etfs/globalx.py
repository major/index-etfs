"""Download and parse holdings CSV files from Global X ETFs."""

import re
import urllib.error
import urllib.parse
import urllib.request
from datetime import UTC, datetime, timedelta

import polars as pl

from .http import fetch_bytes

HOLDINGS_BASE_URL = "https://assets.globalxetfs.com/funds/holdings"
USER_AGENT = "Mozilla/5.0 (compatible; index-etfs/1.0)"
EXPECTED_COLUMNS = [
    "% of Net Assets",
    "Ticker",
    "Name",
    "SEDOL",
    "Market Price ($)",
    "Shares Held",
    "Market Value ($)",
]
TICKER_PATTERN = re.compile(r"^[A-Z][A-Z0-9]*(?:[.-][A-Z])?$")


def load_globalx_csv(url: str) -> pl.DataFrame:
    """Load Global X holdings from the newest CSV snapshot available this week."""
    path_parts = urllib.parse.urlsplit(url).path.strip("/").split("/")
    try:
        fund_index = path_parts.index("funds")
        symbol = path_parts[fund_index + 1].upper()
    except (ValueError, IndexError) as error:
        raise ValueError(f"Expected a Global X fund page URL, got: {url}") from error

    today = datetime.now(UTC).date()
    payload = None
    for days_back in range(8):
        snapshot_date = today - timedelta(days=days_back)
        filename = f"{symbol.lower()}_full-holdings_{snapshot_date:%Y%m%d}.csv"
        request = urllib.request.Request(
            f"{HOLDINGS_BASE_URL}/{filename}",
            headers={"User-Agent": USER_AGENT},
        )
        try:
            payload = fetch_bytes(request)
        except urllib.error.HTTPError as error:
            if error.code != 404:
                raise
            continue
        break

    if payload is None:
        raise FileNotFoundError(
            f"No Global X holdings snapshot found for {symbol} from "
            f"{today:%Y-%m-%d} through {(today - timedelta(days=7)):%Y-%m-%d}"
        )

    holdings = pl.read_csv(payload, skip_rows=2, infer_schema=False)
    if holdings.columns != EXPECTED_COLUMNS:
        raise ValueError(
            "Global X holdings CSV schema does not match the expected columns: "
            f"{holdings.columns}"
        )

    ticker = (
        pl.col("Ticker")
        .cast(pl.String, strict=False)
        .str.strip_chars()
        .str.to_uppercase()
        .str.replace_all("/", ".")
    )
    return (
        holdings.select(ticker.alias("Ticker"))
        .filter(
            pl.col("Ticker").is_not_null()
            & (pl.col("Ticker").str.len_chars() <= 10)
            & pl.col("Ticker").str.contains(TICKER_PATTERN.pattern)
        )
    )
