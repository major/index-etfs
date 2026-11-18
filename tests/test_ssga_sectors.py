"""Tests for excluding named futures contracts from SSGA holdings."""

import io
from pathlib import Path
from unittest.mock import patch

import polars as pl
import pytest

from index_etfs.sources import filter_and_clean, load_ssga_excel

FIXTURES = Path(__file__).parent / "fixtures"


def _ssga_holdings(
    tickers: list[str], names: list[str | None] | None = None
) -> pl.DataFrame:
    """Build a small SSGA holdings frame with optional Name values."""
    data: dict[str, list[str | None]] = {
        "Ticker": tickers,
        "Local Currency": ["USD"] * len(tickers),
    }
    if names is not None:
        data["Name"] = names
    return pl.DataFrame(data)


@pytest.mark.parametrize(
    "name",
    [
        "XAB MATERIALS     DEC26",
        "S+P EMINI COM SER DEC26",
        "EMINI S+P REESTATEDEC26",
        "XAV  HEALTH CARE  DEC26",
        "CRUDE OIL JAN27",
        "S+P E-MINI MAR28",
        "DOW FUTURES SEP99",
    ],
)
def test_ssga_excludes_named_futures_contracts(name: str) -> None:
    """Exclude SSGA holdings whose names end with a futures contract label."""
    # Arrange
    holdings = _ssga_holdings(["FUT"], [name])

    # Act
    cleaned = filter_and_clean(holdings, "ssga")

    # Assert
    assert cleaned["Ticker"].to_list() == []


@pytest.mark.parametrize(
    "name",
    [
        "CONTRA HOLOGIC INCORPO",
        "contra hologic incorpo",
        "  CONTRA SOME CO",
    ],
)
def test_ssga_excludes_contra_cvr_rows(name: str) -> None:
    """Exclude SSGA contingent value rights labeled with a CONTRA name prefix."""
    # Arrange
    holdings = _ssga_holdings(["2602335D"], [name])

    # Act
    cleaned = filter_and_clean(holdings, "ssga")

    # Assert
    assert cleaned["Ticker"].to_list() == []


def test_ssga_preserves_stocks_with_contract_like_tickers() -> None:
    """Keep ordinary holdings even when their ticker resembles a contract label."""
    # Arrange
    holdings = _ssga_holdings(
        ["JAN", "MAR27", "DEC26", "CTRN", "HOLX"],
        [
            "Janus Henderson Group",
            "Marathon Oil",
            "December Gold Inc",
            "CONTRARIAN CAPITAL",
            "Hologic Inc",
        ],
    )

    # Act
    cleaned = filter_and_clean(holdings, "ssga")

    # Assert
    assert cleaned["Ticker"].to_list() == ["CTRN", "DEC26", "HOLX", "JAN", "MAR27"]


def test_ssga_preserves_rows_with_null_names() -> None:
    """Keep SSGA rows when the optional Name field is null."""
    # Arrange
    holdings = _ssga_holdings(["NULLNAME"], [None])

    # Act
    cleaned = filter_and_clean(holdings, "ssga")

    # Assert
    assert cleaned["Ticker"].to_list() == ["NULLNAME"]


def test_ssga_preserves_compatibility_when_name_column_is_absent() -> None:
    """Keep cleaning SSGA data that does not contain a Name column."""
    # Arrange
    holdings = _ssga_holdings(["NAMESLESS"])

    # Act
    cleaned = filter_and_clean(holdings, "ssga")

    # Assert
    assert cleaned["Ticker"].to_list() == ["NAMESLESS"]


def test_non_ssga_provider_does_not_filter_futures_named_holdings() -> None:
    """Limit futures-name filtering to SSGA rows only."""
    # Arrange
    holdings = pl.DataFrame(
        {"Ticker": ["FUT"], "Name": ["S+P EMINI COM SER DEC26"]}
    )

    # Act
    cleaned = filter_and_clean(holdings, "nasdaq")

    # Assert
    assert cleaned["Ticker"].to_list() == ["FUT"]


def test_ssga_excel_parser_and_cleaner_exclude_contracts() -> None:
    """Parse a row-five SSGA workbook header and filter its futures rows."""
    # Arrange
    url = "https://example.com/ssga-sectors.xlsx"
    workbook = io.BytesIO((FIXTURES / "ssga_sector_holdings.xlsx").read_bytes())

    # Act
    with patch("index_etfs.sources.read_url", return_value=workbook):
        holdings = load_ssga_excel(url)
    cleaned = filter_and_clean(holdings, "ssga")

    # Assert
    assert holdings.columns == [
        "Name",
        "Ticker",
        "Identifier",
        "SEDOL",
        "Weight",
        "Sector",
        "Shares Held",
        "Local Currency",
    ]
    assert holdings["Name"].head(4).to_list() == [
        "XAB MATERIALS     DEC26",
        "S+P EMINI COM SER DEC26",
        "EMINI S+P REESTATEDEC26",
        "XAV  HEALTH CARE  DEC26",
    ]
    # "Contra holding" (2602335D) is a CVR and is excluded by the CONTRA name rule.
    assert cleaned["Ticker"].to_list() == ["AAA", "NULLNM"]
