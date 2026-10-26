"""Test CSV loaders for specialty ETF holdings providers."""

from unittest.mock import patch

import polars as pl
import pytest

from index_etfs.specialty_csv import load_flexshares_csv, load_tema_csv

FLEXSHARES_HEADER = (
    "Date,CUSIP,ISIN,SEDOL,Name,Ticker,Market Value-Local,Market Value-Base,"
    "Fund Weight %,Shares Held,Coupon,Maturity,Sector Type,Country\n"
)
TEMA_HEADER = (
    "holdings_date,ticker,cusip,proper_name,shares,market_value,percent_of_nav,"
    "is_cash,country,sector\n"
)


def _mock_fetch(payload: bytes):
    """Return a fetch_bytes mock that captures the request made by a loader."""
    return patch("index_etfs.specialty_csv.fetch_bytes", return_value=payload)


def test_load_flexshares_csv_filters_cash_and_foreign_qualified_tickers() -> None:
    """Keep listed equities and return a string-only ticker frame."""
    # Arrange
    payload = (
        FLEXSHARES_HEADER
        + "2026-01-01,1,1,1,Microsoft Corp,MSFT,1,1,1,1,,,Technology,US\n"
        + "2026-01-01,2,2,2,Cash and Cash Equivalents,CSH,1,1,1,1,,,Cash,US\n"
        + "2026-01-01,3,3,3,Foreign issuer,ABC US,1,1,1,1,,,Equity,GB\n"
        + "2026-01-01,4,4,4,Berkshire Hathaway Inc,BKR/B,1,1,1,1,,,Equity,US\n"
        + "2026-01-01,5,5,5,Missing symbol,,1,1,1,1,,,Equity,US\n"
    ).encode()
    url = "https://example.com/flexshares.csv"

    # Act
    with _mock_fetch(payload) as mock_fetch:
        holdings = load_flexshares_csv(url)

    # Assert
    request = mock_fetch.call_args.args[0]
    assert request.full_url == url
    assert request.get_header("User-agent")
    assert holdings.columns == ["Ticker"]
    assert holdings.schema == {"Ticker": pl.String}
    assert holdings["Ticker"].to_list() == ["MSFT", "BKR.B"]


@pytest.mark.parametrize(
    ("name", "ticker", "expected"),
    [
        ("Cash and Cash Equivalents", "CSH", []),
        ("CASH equivalents", "CSH", []),
        ("U.S. Dollar", "USDX", []),
        ("Foreign Currency", "FCY", []),
        ("Currency", "CUR", []),
        ("FirstCash Holdings Inc", "FCFS", ["FCFS"]),
    ],
)
def test_load_flexshares_csv_matches_cash_names_without_rejecting_firstcash(
    name: str, ticker: str, expected: list[str]
) -> None:
    """Filter whole-word cash descriptions but retain FirstCash equity."""
    # Arrange
    payload = (
        FLEXSHARES_HEADER
        + f"2026-01-01,1,1,1,{name},{ticker},1,1,1,1,,,Equity,US\n"
    ).encode()

    # Act
    with _mock_fetch(payload):
        holdings = load_flexshares_csv("https://example.com/flexshares.csv")

    # Assert
    assert holdings["Ticker"].to_list() == expected


def test_load_flexshares_csv_requires_ticker_header() -> None:
    """Reject a FlexShares CSV without its required ticker column."""
    # Arrange
    payload = b"Date,Name\n2026-01-01,Example Corp\n"

    # Act and assert
    with _mock_fetch(payload), pytest.raises(ValueError, match="ticker"):
        load_flexshares_csv("https://example.com/flexshares.csv")


def test_load_flexshares_csv_returns_typed_empty_frame() -> None:
    """Keep the ticker column string-typed when no rows qualify."""
    # Arrange
    payload = (FLEXSHARES_HEADER + "2026-01-01,1,1,1,Cash,CSH,1,1,1,1,,,Cash,US\n").encode()

    # Act
    with _mock_fetch(payload):
        holdings = load_flexshares_csv("https://example.com/flexshares.csv")

    # Assert
    assert holdings.schema == {"Ticker": pl.String}
    assert holdings.is_empty()


def test_load_tema_csv_discovers_current_holdings_link_and_filters_rows() -> None:
    """Discover the dated official CSV URL and exclude cash and private rows."""
    # Arrange
    html = b"""
        <html><a href="/hubfs/Website/Holdings/VOLT-holdings-20991231.csv?download=1&amp;x=2">
        Download</a></html>
    """
    csv_payload = (
        TEMA_HEADER
        + "2026-01-01,MSFT,1,Microsoft Corp,10,100,1,false,US,Technology\n"
        + "2026-01-01,BRK/B,2,Berkshire Hathaway Inc,10,100,1,false,US,Financials\n"
        + "2026-01-01,CASH,3,Cash,10,100,1,TrUe,US,Cash\n"
        + "2026-01-01,PRIV,4,Private company exposure,10,100,1,false,US,Technology\n"
        + "2026-01-01,ABC LN,5,Foreign issuer,10,100,1,false,GB,Technology\n"
        + "2026-01-01,,6,Unlisted issuer,10,100,1,false,US,Technology\n"
    ).encode()
    page_url = "https://temaetfs.com/volt"

    # Act
    with patch(
        "index_etfs.specialty_csv.fetch_bytes", side_effect=[html, csv_payload]
    ) as mock_fetch:
        holdings = load_tema_csv(page_url)

    # Assert
    requests = [call.args[0] for call in mock_fetch.call_args_list]
    assert requests[0].full_url == page_url
    assert requests[1].full_url == (
        "https://temaetfs.com/hubfs/Website/Holdings/"
        "VOLT-holdings-20991231.csv?download=1&x=2"
    )
    assert all(request.get_header("User-agent") for request in requests)
    assert holdings.columns == ["Ticker"]
    assert holdings.schema == {"Ticker": pl.String}
    assert holdings["Ticker"].to_list() == ["MSFT", "BRK.B"]


@pytest.mark.parametrize(
    "href",
    [
        "https://evil.example/hubfs/Website/Holdings/VOLT-holdings-20260101.csv",
        "https://temaetfs.com/other/VOLT-holdings-20260101.csv",
        "//evil.example/hubfs/Website/Holdings/VOLT-holdings-20260101.csv",
    ],
)
def test_load_tema_csv_rejects_nonofficial_download_links(href: str) -> None:
    """Fail closed when the page has no allowed official holdings CSV link."""
    # Arrange
    html = f'<a href="{href}">Download</a>'.encode()

    # Act and assert
    with (
        patch("index_etfs.specialty_csv.fetch_bytes", return_value=html),
        pytest.raises(ValueError, match="official VOLT holdings CSV"),
    ):
        load_tema_csv("https://temaetfs.com/volt")


def test_load_tema_csv_rejects_missing_required_columns() -> None:
    """Reject a discovered Tema CSV with a malformed schema."""
    # Arrange
    html = b'<a href="/hubfs/Website/Holdings/VOLT-holdings-20260101.csv">CSV</a>'
    csv_payload = b"holdings_date,ticker,proper_name\n2026-01-01,MSFT,Microsoft\n"

    # Act and assert
    with patch(
        "index_etfs.specialty_csv.fetch_bytes", side_effect=[html, csv_payload]
    ), pytest.raises(ValueError, match="missing required columns"):
        load_tema_csv("https://temaetfs.com/volt")


def test_load_tema_csv_returns_typed_empty_frame() -> None:
    """Keep a typed ticker column when all rows are cash holdings."""
    # Arrange
    html = b'<a href="/hubfs/Website/Holdings/VOLT-holdings-20260101.csv">CSV</a>'
    csv_payload = (
        TEMA_HEADER
        + "2026-01-01,CASH,1,Cash,10,100,1,true,US,Cash\n"
    ).encode()

    # Act
    with patch(
        "index_etfs.specialty_csv.fetch_bytes", side_effect=[html, csv_payload]
    ):
        holdings = load_tema_csv("https://temaetfs.com/volt")

    # Assert
    assert holdings.schema == {"Ticker": pl.String}
    assert holdings.is_empty()
