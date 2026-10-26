"""Load First Trust holdings from its rendered ETF holdings page."""

import re
import urllib.request
from html.parser import HTMLParser

import polars as pl

from .http import fetch_bytes

USER_AGENT = "Mozilla/5.0 (compatible; index-etfs/1.0)"
REQUIRED_HEADERS = ("security name", "identifier", "cusip", "classification")
TICKER_PATTERN = re.compile(r"[A-Z][A-Z0-9]*(?:\.[AB])?")
SLASH_SHARE_CLASS_PATTERN = re.compile(r"([A-Z][A-Z0-9]*)/([A-Z])")


class _HoldingsTableParser(HTMLParser):
    """Collect text rows from rendered HTML tables without mixing nested tables."""

    def __init__(self) -> None:
        """Initialize storage and parser state for table rows."""
        super().__init__()
        self.tables: list[list[list[str]]] = []
        self._table_stack: list[list[list[str]]] = []
        self._row_stack: list[list[str] | None] = []
        self._cell_stack: list[list[str] | None] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        """Track table, row, and cell boundaries for each nested table."""
        del attrs
        if tag == "table":
            self._table_stack.append([])
            self._row_stack.append(None)
            self._cell_stack.append(None)
            return

        if not self._table_stack:
            return
        if tag == "tr":
            self._row_stack[-1] = []
        elif tag in {"th", "td"} and self._row_stack[-1] is not None:
            self._cell_stack[-1] = []

    def handle_data(self, data: str) -> None:
        """Append text only when it belongs to a direct table cell."""
        if self._cell_stack and self._cell_stack[-1] is not None:
            self._cell_stack[-1].append(data)

    def handle_endtag(self, tag: str) -> None:
        """Close cell, row, or table boundaries and save completed table rows."""
        if not self._table_stack:
            return
        if tag in {"th", "td"} and self._cell_stack[-1] is not None:
            row = self._row_stack[-1]
            if row is not None:
                row.append(" ".join("".join(self._cell_stack[-1]).split()))
            self._cell_stack[-1] = None
        elif tag == "tr" and self._row_stack[-1] is not None:
            self._table_stack[-1].append(self._row_stack[-1])
            self._row_stack[-1] = None
        elif tag == "table":
            self.tables.append(self._table_stack.pop())
            self._row_stack.pop()
            self._cell_stack.pop()


def _find_holdings_rows(
    tables: list[list[list[str]]],
) -> tuple[list[list[str]], dict[str, int]]:
    """Return the body rows and column positions for the required table schema."""
    for table in tables:
        for header_index, row in enumerate(table):
            headers = [cell.strip().casefold() for cell in row]
            if not all(header in headers for header in REQUIRED_HEADERS):
                continue
            if not any("shares" in header or "quantity" in header for header in headers):
                continue

            column_indexes = {header: headers.index(header) for header in REQUIRED_HEADERS}
            return table[header_index + 1 :], column_indexes

    raise ValueError("First Trust response does not contain the required holdings table")


def _normalize_ticker(identifier: str) -> str | None:
    """Normalize an allowed slash share class and validate the US-style symbol."""
    ticker = identifier.strip()
    slash_share_class = SLASH_SHARE_CLASS_PATTERN.fullmatch(ticker)
    if slash_share_class is not None:
        ticker = f"{slash_share_class.group(1)}.{slash_share_class.group(2)}"

    if len(ticker) > 10 or TICKER_PATTERN.fullmatch(ticker) is None:
        return None
    return ticker


def load_firsttrust_html(url: str) -> pl.DataFrame:
    """Fetch First Trust's rendered holdings table and return US-style equities."""
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    parser = _HoldingsTableParser()
    parser.feed(fetch_bytes(request).decode("utf-8", errors="replace"))
    parser.close()

    rows, column_indexes = _find_holdings_rows(parser.tables)
    required_index = max(column_indexes.values())
    tickers = []
    for row in rows:
        if len(row) <= required_index:
            continue

        classification = row[column_indexes["classification"]].strip()
        if not classification or classification.casefold() in {"cash", "other"}:
            continue

        ticker = _normalize_ticker(row[column_indexes["identifier"]])
        if ticker is not None:
            tickers.append(ticker)

    return pl.DataFrame({"Ticker": pl.Series("Ticker", tickers, dtype=pl.String)})
