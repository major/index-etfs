"""Tests for First Trust's rendered HTML holdings table."""

from unittest.mock import patch

import polars as pl
import pytest

from index_etfs.firsttrust import load_firsttrust_html

HOLDINGS_PAGE = """
<html><body>
  <table>
    <tr><th>Security Name</th><th>Identifier</th></tr>
    <tr><td>Unrelated</td><td>WRONG</td></tr>
  </table>
  <table>
    <tr>
      <th>Security&#32;Name</th><th>Identifier</th><th>CUSIP</th>
      <th>Classification</th><th>Shares / Quantity</th><th>Weight</th>
    </tr>
    <tr><td>Alpha &amp; Beta Inc.</td><td>AAPL</td><td>037833100</td>
      <td>Technology Hardware</td><td>100</td><td>5%</td></tr>
    <tr><td>Class B Company</td><td>BRK/B</td><td>084670702</td>
      <td>Financial Services</td><td>100</td><td>5%</td></tr>
    <tr><td>Class A Company</td><td>XYZ.A</td><td>123456789</td>
      <td>Software</td><td>100</td><td>5%</td></tr>
    <tr><td>US Dollar</td><td>$USD</td><td></td><td>Other</td><td>10</td><td>1%</td></tr>
    <tr><td>Canadian Dollar</td><td>$CAD</td><td></td><td>Other</td><td>10</td><td>1%</td></tr>
    <tr><td>Other asset</td><td>OTHER</td><td></td><td>Other</td><td>10</td><td>1%</td></tr>
    <tr><td>Cash position</td><td>CASH1</td><td></td><td>Cash</td><td>10</td><td>1%</td></tr>
    <tr><td>French listing</td><td>ABC.FP</td><td></td><td>Software</td><td>10</td><td>1%</td></tr>
    <tr><td>Japanese listing</td><td>ABC.JP</td><td></td><td>Software</td><td>10</td><td>1%</td></tr>
    <tr><td>Canadian listing</td><td>ABC.CN</td><td></td><td>Software</td><td>10</td><td>1%</td></tr>
    <tr><td>Unsupported class</td><td>ABC/C</td><td></td><td>Software</td><td>10</td><td>1%</td></tr>
    <tr><td>Too long</td><td>ABCDEFGHIJK</td><td></td><td>Software</td><td>10</td><td>1%</td></tr>
    <tr><td>Lowercase</td><td>lower</td><td></td><td>Software</td><td>10</td><td>1%</td></tr>
  </table>
</body></html>
"""


def test_load_firsttrust_html_selects_holdings_table_and_filters_identifiers() -> None:
    """Keep US-style equity symbols from the rendered holdings table."""
    url = "https://example.com/holdings"

    with patch("index_etfs.firsttrust.fetch_bytes", return_value=HOLDINGS_PAGE.encode()) as fetch:
        holdings = load_firsttrust_html(url)

    fetch.assert_called_once()
    request = fetch.call_args.args[0]
    assert request.full_url == url
    assert request.get_header("User-agent")
    assert holdings.columns == ["Ticker"]
    assert holdings.schema == {"Ticker": pl.String}
    assert holdings["Ticker"].to_list() == ["AAPL", "BRK.B", "XYZ.A"]


@pytest.mark.parametrize(
    "html",
    [
        "<html><table><tr><th>Security Name</th><th>Identifier</th></tr></table></html>",
        (
            "<html><table><tr><th>Security Name</th><th>Identifier</th><th>CUSIP</th>"
            "<th>Shares / Quantity</th></tr></table></html>"
        ),
    ],
)
def test_load_firsttrust_html_rejects_missing_holdings_schema(html: str) -> None:
    """Fail clearly if the page has no table with the required columns."""
    with (
        patch("index_etfs.firsttrust.fetch_bytes", return_value=html.encode()),
        pytest.raises(ValueError, match="required holdings table"),
    ):
        load_firsttrust_html("https://example.com/holdings")


def test_load_firsttrust_html_returns_typed_empty_frame_when_no_tickers_qualify() -> None:
    """Return a string-typed ticker column when every row is excluded."""
    html = HOLDINGS_PAGE.replace("AAPL", "$USD").replace("BRK/B", "ABC.FP").replace("XYZ.A", "ABC.JP")

    with patch("index_etfs.firsttrust.fetch_bytes", return_value=html.encode()):
        holdings = load_firsttrust_html("https://example.com/holdings")

    assert holdings.schema == {"Ticker": pl.String}
    assert holdings.is_empty()
