"""Tests for deterministic TradingView symbol resolution and output rendering."""

import json
from pathlib import Path
from unittest.mock import call, patch

import polars as pl
import pytest

from index_etfs.outputs import (
    TradingViewResolution,
    _scan_tradingview,
    resolve_tradingview,
    save_holdings,
    tradingview_symbols,
    write_metadata,
)


def scanner_row(symbol: str, name: str, sector: str | None) -> dict:
    """Build one valid TradingView scanner row."""
    return {"s": symbol, "d": [name, sector]}


def test_resolution_is_order_invariant_and_deduplicates_identical_rows() -> None:
    """Equivalent scanner rows produce the same result in any order."""
    rows = [
        scanner_row("NASDAQ:AAPL", "AAPL", "Technology"),
        scanner_row("NASDAQ:AAPL", "AAPL", "Technology"),
        scanner_row("NYSE:MSFT", "MSFT", "Technology"),
        scanner_row("NYSE:CONFLICT", "CONFLICT", "Technology"),
        scanner_row("NYSE:CONFLICT", "CONFLICT", "Utilities"),
        scanner_row("NYSE:AMBIG", "AMBIG", "Technology"),
        scanner_row("NASDAQ:AMBIG", "AMBIG", "Financial"),
    ]
    expected = TradingViewResolution(
        symbols={
            "AAPL": ("NASDAQ:AAPL", "Technology"),
            "CONFLICT": ("NYSE:CONFLICT", "Other"),
            "MSFT": ("NYSE:MSFT", "Technology"),
        },
        unresolved=[],
        ambiguous=["AMBIG"],
        sector_conflicts=["CONFLICT"],
    )

    with patch("index_etfs.outputs._scan_tradingview", return_value=rows):
        forward = resolve_tradingview(["MSFT", "AAPL", "AAPL", "AMBIG", "CONFLICT"])
    with patch("index_etfs.outputs._scan_tradingview", return_value=list(reversed(rows))):
        reverse = resolve_tradingview(["AAPL", "MSFT", "CONFLICT", "AMBIG"])

    assert forward == expected
    assert reverse == expected


def test_resolution_reports_ambiguous_missing_and_sector_conflict_tickers(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Ambiguous names fall back, while conflicting sectors resolve to Other."""
    rows = [
        scanner_row("NYSE:AAA", "AAA", "Technology"),
        scanner_row("NASDAQ:AAA", "AAA", "Financial"),
        scanner_row("NYSE:BBB", "BBB", "Technology"),
        scanner_row("NYSE:BBB", "BBB", "Utilities"),
        scanner_row("NYSE:UNRELATED", "UNRELATED", "Energy"),
    ]

    with patch("index_etfs.outputs._scan_tradingview", return_value=rows):
        result = resolve_tradingview(["MISSING", "BBB", "AAA"])

    assert result == TradingViewResolution(
        symbols={"BBB": ("NYSE:BBB", "Other")},
        unresolved=["MISSING"],
        ambiguous=["AAA"],
        sector_conflicts=["BBB"],
    )
    output = capsys.readouterr().out
    assert "MISSING" in output
    assert "ambiguous" in output.lower()
    assert "sector" in output.lower()
    with patch("index_etfs.outputs._scan_tradingview", return_value=rows):
        assert tradingview_symbols(["AAA"]) == {}


def test_resolution_ignores_valid_rows_outside_the_requested_union() -> None:
    """Valid rows returned outside the requested batch do not enter the result."""
    with patch(
        "index_etfs.outputs._scan_tradingview",
        return_value=[scanner_row("NASDAQ:OTHER", "OTHER", "Technology")],
    ) as scan:
        result = resolve_tradingview(["AAPL"])

    assert scan.call_args_list == [call(["AAPL"])]
    assert result.symbols == {}
    assert result.unresolved == ["AAPL"]


@pytest.mark.parametrize(
    ("tickers", "chunk_size", "expected_chunks"),
    [
        (list(reversed([f"T{i:03}" for i in range(199)])), 200, 1),
        ([f"T{i:03}" for i in range(200)], 200, 1),
        ([f"T{i:03}" for i in range(201)], 200, 2),
        (["B", "A", "A", "B", "C"], 1, 3),
    ],
)
def test_resolution_batches_sorted_unique_input_once(
    tickers: list[str], chunk_size: int, expected_chunks: int
) -> None:
    """Resolution sorts and deduplicates input before making bounded scans."""
    with patch("index_etfs.outputs._scan_tradingview", return_value=[]) as scan:
        result = resolve_tradingview(tickers, chunk_size=chunk_size)

    unique_tickers = sorted(set(tickers))
    assert scan.call_count == expected_chunks
    assert [ticker for batch in scan.call_args_list for ticker in batch.args[0]] == unique_tickers
    assert result.unresolved == unique_tickers


def test_resolution_rejects_zero_chunk_size() -> None:
    """A zero chunk size is rejected before scanning."""
    with pytest.raises(ValueError, match="chunk_size"):
        resolve_tradingview(["AAPL"], chunk_size=0)


@pytest.mark.parametrize(
    "payload",
    [
        [],
        {},
        {"data": {}},
        {"data": [None]},
        {"data": [{"d": ["AAPL", "Technology"]}]},
        {"data": [{"s": 4, "d": ["AAPL", "Technology"]}]},
        {"data": [{"s": "", "d": ["AAPL", "Technology"]}]},
        {"data": [{"s": "NASDAQ", "d": ["AAPL", "Technology"]}]},
        {"data": [{"s": "NASDAQ:", "d": ["AAPL", "Technology"]}]},
        {"data": [{"s": "NASDAQ:AAPL", "d": None}]},
        {"data": [{"s": "NASDAQ:AAPL", "d": ["", "Technology"]}]},
        {"data": [{"s": "NASDAQ:AAPL", "d": [4, "Technology"]}]},
        {"data": [{"s": "NASDAQ:AAPL", "d": ["AAPL"]}]},
        {"data": [{"s": "NASDAQ:AAPL", "d": ["AAPL", 4]}]},
    ],
)
def test_scanner_rejects_malformed_response_shapes(payload: object) -> None:
    """Malformed scanner payloads raise a clear shape error."""
    with patch(
        "index_etfs.outputs.fetch_bytes",
        return_value=json.dumps(payload).encode(),
    ), pytest.raises(ValueError, match="TradingView scanner"):
        _scan_tradingview(["AAPL"])


def test_scanner_accepts_empty_data_list() -> None:
    """An empty scanner result is a valid response."""
    with patch("index_etfs.outputs.fetch_bytes", return_value=b'{"data":[]}'):
        assert _scan_tradingview(["AAPL"]) == []


@pytest.mark.parametrize("sector", [None, ""])
def test_empty_or_missing_sector_resolves_to_other(sector: str | None) -> None:
    """The scanner accepts nullable and empty sectors and groups them as Other."""
    payload = json.dumps({"data": [scanner_row("NASDAQ:AAPL", "AAPL", sector)]}).encode()

    with patch("index_etfs.outputs.fetch_bytes", return_value=payload):
        result = resolve_tradingview(["AAPL"])

    assert result.symbols == {"AAPL": ("NASDAQ:AAPL", "Other")}


def test_scanner_failure_propagates() -> None:
    """Scanner transport errors are not hidden as empty results."""
    with patch("index_etfs.outputs.fetch_bytes", side_effect=OSError("offline")), pytest.raises(
        OSError, match="offline"
    ):
        _scan_tradingview(["AAPL"])


def test_save_holdings_uses_precomputed_empty_symbols_without_network(tmp_path: Path) -> None:
    """An explicitly empty mapping renders bare tickers without a scanner call."""
    with patch("index_etfs.outputs.tradingview_symbols", side_effect=AssertionError("network lookup")):
        save_holdings(pl.DataFrame({"Ticker": ["AAPL"]}), "spy", tmp_path, symbols={})

    assert (tmp_path / "watchlists" / "sp500.txt").read_text() == "AAPL\n"
    assert (tmp_path / "watchlists" / "sp500-grouped.txt").read_text() == "###Other\nAAPL\n"


def test_write_metadata_merges_details_by_etf_symbol(tmp_path: Path) -> None:
    """Optional per-ETF details are included in that ETF's watchlist record."""
    write_metadata(
        {"spy": 503},
        tmp_path,
        details={"spy": {"unresolved_count": 1, "unresolved": ["AAPL"]}},
    )

    metadata = json.loads((tmp_path / "metadata" / "latest.json").read_text())
    assert metadata["watchlists"]["sp500"]["unresolved_count"] == 1
    assert metadata["watchlists"]["sp500"]["unresolved"] == ["AAPL"]
