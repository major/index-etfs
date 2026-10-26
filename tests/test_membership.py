"""Tests for ticker membership diagnostics."""

import json
from pathlib import Path

import polars as pl
import pytest

from index_etfs import catalog, holdings, outputs
from index_etfs.catalog import ETFConfig
from index_etfs.changes import compare_membership


def test_compare_membership_without_baseline_returns_unavailable_result(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Treat a missing baseline as a first run without additions."""
    baseline_path = tmp_path / "spy.txt"

    result = compare_membership(["AAPL"], baseline_path)

    assert result == {
        "baseline_available": False,
        "previous_count": None,
        "added_count": 0,
        "removed_count": 0,
        "added_tickers": [],
        "removed_tickers": [],
    }
    assert capsys.readouterr().out == ""


@pytest.mark.parametrize("contents", ["", " \n\t\n "])
def test_compare_membership_warns_for_empty_baseline(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], contents: str
) -> None:
    """Treat empty and whitespace-only baseline files as unavailable."""
    baseline_path = tmp_path / "spy.txt"
    baseline_path.write_text(contents, encoding="utf-8")

    result = compare_membership(["AAPL"], baseline_path)

    assert result["baseline_available"] is False
    assert result["previous_count"] is None
    assert result["added_count"] == result["removed_count"] == 0
    assert result["added_tickers"] == result["removed_tickers"] == []
    warning = capsys.readouterr().out.lower()
    assert "empty" in warning
    assert "spy" in warning


def test_compare_membership_reports_unchanged_unique_membership(tmp_path: Path) -> None:
    """Report no changes when current tickers match the baseline."""
    baseline_path = tmp_path / "spy.txt"
    baseline_path.write_text("AAPL\nMSFT\n", encoding="utf-8")

    result = compare_membership(["MSFT", "AAPL"], baseline_path)

    assert result == {
        "baseline_available": True,
        "previous_count": 2,
        "added_count": 0,
        "removed_count": 0,
        "added_tickers": [],
        "removed_tickers": [],
    }


def test_compare_membership_reports_sorted_additions_and_removals(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Report sorted changes even when previous and current counts match."""
    baseline_path = tmp_path / "sp500.txt"
    baseline_path.write_text("OLD2\nKEEP\nOLD1\n", encoding="utf-8")

    result = compare_membership(["NEW2", "KEEP", "NEW1"], baseline_path)

    assert result == {
        "baseline_available": True,
        "previous_count": 3,
        "added_count": 2,
        "removed_count": 2,
        "added_tickers": ["NEW1", "NEW2"],
        "removed_tickers": ["OLD1", "OLD2"],
    }
    message = capsys.readouterr().out
    assert "SP500" in message
    assert "baseline 3, current 3, added 2, removed 2" in message


def test_compare_membership_deduplicates_baseline_and_current(tmp_path: Path) -> None:
    """Count each ticker once regardless of duplicates in either source."""
    baseline_path = tmp_path / "spy.txt"
    baseline_path.write_text(" AAPL \nAAPL\n\nMSFT\n", encoding="utf-8")

    result = compare_membership(["MSFT", "AAPL", "AAPL"], baseline_path)

    assert result["previous_count"] == 2
    assert result["added_count"] == result["removed_count"] == 0


def test_compare_membership_does_not_reject_replaced_membership(tmp_path: Path) -> None:
    """Report a fully replaced set without applying a membership threshold."""
    baseline_path = tmp_path / "spy.txt"
    baseline_path.write_text("OLD1\nOLD2\n", encoding="utf-8")

    result = compare_membership(["NEW1", "NEW2"], baseline_path)

    assert result["baseline_available"] is True
    assert result["added_tickers"] == ["NEW1", "NEW2"]
    assert result["removed_tickers"] == ["OLD1", "OLD2"]


def test_compare_membership_propagates_read_errors(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Do not disguise baseline read failures as unavailable baselines."""
    baseline_path = tmp_path / "spy.txt"
    baseline_path.touch()

    def fail_read(_path: Path, *, encoding: str | None = None) -> str:
        """Raise the read failure used to characterize error propagation."""
        assert encoding == "utf-8"
        raise PermissionError("baseline cannot be read")

    monkeypatch.setattr(Path, "read_text", fail_read)

    with pytest.raises(PermissionError, match="cannot be read"):
        compare_membership(["AAPL"], baseline_path)


def test_main_writes_membership_diagnostics_after_fetching(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Include prior membership in metadata during a successful full refresh."""
    # Arrange
    configs = {"alpha": ETFConfig("https://example.com/alpha", "nasdaq", 2)}
    monkeypatch.setattr(catalog, "ETF_CONFIGS", configs)
    monkeypatch.setattr(holdings, "ETF_CONFIGS", configs)
    monkeypatch.setattr(outputs, "ETF_CONFIGS", configs)
    ticker_path = tmp_path / "tickers" / "alpha.txt"
    ticker_path.parent.mkdir(parents=True)
    ticker_path.write_text("KEEP\nOLD\n", encoding="utf-8")
    events: list[str] = []

    def load(_config: ETFConfig) -> pl.DataFrame:
        """Return fresh ticker rows and record that fetching ran first."""
        events.append("fetch")
        return pl.DataFrame({"Ticker": ["KEEP", "NEW"]})

    def compare_and_record(
        tickers: list[str], baseline_path: Path
    ) -> dict[str, bool | int | None | list[str]]:
        """Record membership comparison while calling the real implementation."""
        events.append("compare")
        return compare_membership(tickers, baseline_path)

    monkeypatch.setattr(holdings, "load_holdings", load)
    monkeypatch.setattr(holdings, "compare_membership", compare_and_record)
    monkeypatch.setattr(holdings.Path, "cwd", lambda: tmp_path)

    # Act
    holdings.main()

    # Assert
    metadata = json.loads((tmp_path / "metadata" / "latest.json").read_text())
    details = metadata["etfs"]["alpha"]
    assert events == ["fetch", "compare"]
    assert details["count"] == 2
    assert details["membership_changes"] == {
        "baseline_available": True,
        "previous_count": 2,
        "added_count": 1,
        "removed_count": 1,
        "added_tickers": ["NEW"],
        "removed_tickers": ["OLD"],
    }
    assert ticker_path.read_text(encoding="utf-8") == "KEEP\nNEW\n"


def test_main_metadata_write_failure_preserves_tickers_and_metadata(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Do not publish ticker changes when staged metadata writing fails."""
    # Arrange
    configs = {"alpha": ETFConfig("https://example.com/alpha", "nasdaq", 2)}
    monkeypatch.setattr(catalog, "ETF_CONFIGS", configs)
    monkeypatch.setattr(holdings, "ETF_CONFIGS", configs)
    monkeypatch.setattr(outputs, "ETF_CONFIGS", configs)
    ticker_path = tmp_path / "tickers" / "alpha.txt"
    ticker_path.parent.mkdir(parents=True)
    ticker_path.write_text("OLD\nKEEP\n", encoding="utf-8")
    metadata_path = tmp_path / "metadata" / "latest.json"
    metadata_path.parent.mkdir(parents=True)
    metadata_path.write_text('{"previous": true}\n', encoding="utf-8")

    def load(_config: ETFConfig) -> pl.DataFrame:
        """Return fresh ticker rows before metadata writing fails."""
        return pl.DataFrame({"Ticker": ["KEEP", "NEW"]})

    def fail_metadata_write(*_args: object, **_kwargs: object) -> None:
        """Raise the metadata error that must prevent staged publication."""
        raise RuntimeError("metadata write failed")

    monkeypatch.setattr(holdings, "load_holdings", load)
    monkeypatch.setattr(holdings, "write_metadata", fail_metadata_write)
    monkeypatch.setattr(holdings.Path, "cwd", lambda: tmp_path)
    original_tickers = ticker_path.read_text(encoding="utf-8")
    original_metadata = metadata_path.read_text(encoding="utf-8")

    # Act and assert
    with pytest.raises(RuntimeError, match="metadata write failed"):
        holdings.main()

    assert ticker_path.read_text(encoding="utf-8") == original_tickers
    assert metadata_path.read_text(encoding="utf-8") == original_metadata
