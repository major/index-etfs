"""Tests for ticker cleaning behavior."""

import polars as pl
import pytest

from index_etfs.sources import filter_and_clean


def test_filter_and_clean_collapses_duplicate_rows_and_reports_count(capsys: pytest.CaptureFixture[str]) -> None:
    """Collapse repeated ticker rows and report their count and provider."""
    # Arrange
    holdings = pl.DataFrame({"Ticker": ["BBB", "AAA", "AAA", "BBB", "AAA"]})

    # Act
    cleaned = filter_and_clean(holdings, "nasdaq")

    # Assert
    warning = capsys.readouterr().out
    assert cleaned["Ticker"].to_list() == ["AAA", "BBB"]
    assert "3 repeated ticker rows" in warning
    assert "nasdaq" in warning


def test_filter_and_clean_does_not_report_unique_tickers(capsys: pytest.CaptureFixture[str]) -> None:
    """Return sorted unique tickers without a warning when none repeat."""
    # Arrange
    holdings = pl.DataFrame({"Ticker": ["BBB", "AAA"]})

    # Act
    cleaned = filter_and_clean(holdings, "nasdaq")

    # Assert
    assert cleaned["Ticker"].to_list() == ["AAA", "BBB"]
    assert capsys.readouterr().out == ""


@pytest.mark.parametrize(
    ("holdings", "message"),
    [
        (pl.DataFrame({"Name": ["Example"]}), "Ticker"),
        (pl.DataFrame({"Ticker": [123]}), "string dtype"),
    ],
)
def test_filter_and_clean_rejects_invalid_ticker_column(holdings: pl.DataFrame, message: str) -> None:
    """Reject nonempty holdings without a string ticker column."""
    # Arrange
    provider = "nasdaq"

    # Act and assert
    with pytest.raises(ValueError, match=message):
        filter_and_clean(holdings, provider)


@pytest.mark.parametrize(
    "holdings",
    [pl.DataFrame({"Ticker": []}), pl.DataFrame(), pl.DataFrame({"Ticker": pl.Series([], dtype=pl.Int64)})],
)
def test_filter_and_clean_preserves_empty_input_behavior(holdings: pl.DataFrame) -> None:
    """Preserve the established result shape for empty holdings."""
    # Arrange
    provider = "nasdaq"

    # Act
    cleaned = filter_and_clean(holdings, provider)

    # Assert
    assert cleaned.columns == ["Ticker"]
    assert cleaned.is_empty()
