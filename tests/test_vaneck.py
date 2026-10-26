"""Tests for VanEck workbook parsing and US-listed ticker filtering."""

import io
import json
from http.cookiejar import CookieJar
from pathlib import Path
from unittest.mock import patch
from urllib.request import HTTPCookieProcessor, OpenerDirector, Request
from zipfile import ZIP_DEFLATED, ZipFile

import pytest
from fastexcel import CalamineError

from index_etfs import sources
from index_etfs.catalog import ETFConfig
from index_etfs.outputs import write_metadata
from index_etfs.sources import filter_and_clean, load_holdings, load_vaneck_excel

FIXTURE = Path(__file__).parent / "fixtures" / "vaneck_holdings.xlsx"
URL = "https://www.vaneck.com/us/en/example/holdings/?fund=smh"


def test_gdx_metadata_labels_the_us_listed_subset(tmp_path: Path) -> None:
    """Keep the GDX partial-coverage label in published metadata."""
    write_metadata({"gdx": 32, "smh": 25}, tmp_path)

    etfs = json.loads((tmp_path / "metadata/latest.json").read_text())["etfs"]
    assert etfs["gdx"]["scope_note"] == (
        "US-listed subset; foreign-market-qualified holdings are excluded."
    )
    assert "scope_note" not in etfs["smh"]


def test_load_vaneck_excel_parses_stocks_and_excludes_non_us_tickers(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Parse stock rows, trim tickers, and exclude foreign and cash tickers."""
    # Arrange
    fixture = FIXTURE.read_bytes()
    with patch("index_etfs.sources.fetch_bytes", return_value=fixture) as fetch:
        # Act
        holdings = load_vaneck_excel(URL)
    cleaned = filter_and_clean(holdings, "vaneck")

    # Assert
    fetch.assert_called_once()
    assert holdings.columns[1] == "Ticker"
    assert holdings.get_column("Asset Class").unique().to_list() == ["Stock"]
    assert holdings["Ticker"].to_list() == [
        "NVDA",
        "AAPL",
        "AAPL",
        "NST AU",
        "2259 HK",
        "PE&OLES* MM",
        None,
        "",
        "--",
        "-USD CASH-",
    ]
    assert cleaned["Ticker"].to_list() == ["AAPL", "NVDA"]
    warning = capsys.readouterr().out
    assert "excluded 3 VanEck stock rows" in warning
    assert "US-listed subset" in warning
    assert "collapsed 1 repeated ticker rows" in warning


def test_load_vaneck_excel_uses_a_fresh_cookie_opener_per_download(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Use a private cookie jar and browser headers for each download."""
    # Arrange
    fixture = FIXTURE.read_bytes()
    calls: list[tuple[Request, OpenerDirector]] = []
    global_opener = sources.urllib.request._opener

    def fake_fetch(request: Request, *, opener: OpenerDirector) -> bytes:
        """Capture the request and opener while returning the local workbook."""
        calls.append((request, opener))
        return fixture

    monkeypatch.setattr(sources, "fetch_bytes", fake_fetch)

    # Act
    load_vaneck_excel(URL)
    load_vaneck_excel(URL)

    # Assert
    assert len(calls) == 2
    cookie_jars = []
    for request, opener in calls:
        assert request.full_url == URL
        assert request.get_header("User-agent") == sources.FIREFOX_HEADERS["User-Agent"]
        processors = [
            handler
            for handler in opener.handlers
            if isinstance(handler, HTTPCookieProcessor)
        ]
        assert len(processors) == 1
        assert isinstance(processors[0].cookiejar, CookieJar)
        cookie_jars.append(processors[0].cookiejar)

    assert calls[0][1] is not calls[1][1]
    assert cookie_jars[0] is not cookie_jars[1]
    assert sources.urllib.request._opener is global_opener


def test_load_holdings_dispatches_to_the_vaneck_loader() -> None:
    """Route VanEck provider configurations through the workbook loader."""
    # Arrange
    config = ETFConfig(URL, "vaneck", 2)
    with patch("index_etfs.sources.fetch_bytes", return_value=FIXTURE.read_bytes()):
        # Act
        holdings = load_holdings(config)

    # Assert
    assert holdings["Ticker"].to_list()[0] == "NVDA"


def test_load_vaneck_excel_rejects_missing_required_columns() -> None:
    """Raise a clear error when the workbook omits a required column."""
    # Arrange
    source = io.BytesIO(FIXTURE.read_bytes())
    with ZipFile(source) as workbook:
        workbook_parts = {name: workbook.read(name) for name in workbook.namelist()}
    worksheet_path = "xl/worksheets/sheet1.xml"
    workbook_parts[worksheet_path] = workbook_parts[worksheet_path].replace(
        b"Asset Class", b"Asset Type"
    )
    malformed_workbook = io.BytesIO()
    with ZipFile(malformed_workbook, "w", ZIP_DEFLATED) as workbook:
        for name, contents in workbook_parts.items():
            workbook.writestr(name, contents)

    # Act and assert
    with patch(
        "index_etfs.sources.fetch_bytes", return_value=malformed_workbook.getvalue()
    ), pytest.raises(ValueError, match="Asset Class"):
        load_vaneck_excel(URL)


def test_load_vaneck_excel_rejects_an_empty_download() -> None:
    """Let the spreadsheet parser report an empty download as invalid input."""
    # Arrange
    with (
        patch("index_etfs.sources.fetch_bytes", return_value=b""),
        pytest.raises(CalamineError, match="Cannot detect file format"),
    ):
        load_vaneck_excel(URL)
