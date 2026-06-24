"""Tests for the holdings pipeline."""

import io
import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

import index_etfs.catalog as catalog
import index_etfs.holdings as holdings
from index_etfs.catalog import ETFConfig, validate_count
from index_etfs.holdings import (
    get_etf_holdings,
    get_iwm_holdings,
    get_mdy_holdings,
    get_qqq_holdings,
    get_spsm_holdings,
    get_spy_holdings,
)
from index_etfs.outputs import save_holdings, tradingview_symbols, watchlist_lines, write_metadata
from index_etfs.sources import (
    FIREFOX_HEADERS,
    filter_and_clean,
    load_holdings,
    load_ishares_csv,
    load_nasdaq_json,
    load_ssga_excel,
    read_url,
)


@pytest.fixture
def sample_ssga_data() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "Ticker": ["AAPL", "MSFT", "GOOGL", "-", "NVDA"],
            "Name": ["Apple Inc", "Microsoft", "Alphabet", "Cash", "NVIDIA"],
            "Weight": [7.5, 6.2, 4.1, 0.5, 3.8],
            "Local Currency": ["USD", "USD", "USD", "USD", "EUR"],
        }
    )


@pytest.fixture
def sample_ishares_data() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "Ticker": ["AAPL", "MSFT", "-", "GOOGL"],
            "Name": ["Apple Inc", "Microsoft", "Cash", "Alphabet"],
            "Weight": [2.5, 2.0, 0.1, 1.8],
            "Market Currency": ["USD", "USD", "USD", "GBP"],
        }
    )


@pytest.mark.parametrize(
    ("provider", "rows", "expected"),
    [
        ("ssga", "sample_ssga_data", ["AAPL", "GOOGL", "MSFT"]),
        ("ishares", "sample_ishares_data", ["AAPL", "MSFT"]),
        ("nasdaq", None, ["AAPL", "GOOGL", "MSFT"]),
    ],
)
def test_filter_and_clean(provider: str, rows: str | None, expected: list[str], request: pytest.FixtureRequest) -> None:
    df = request.getfixturevalue(rows) if rows else pl.DataFrame({"Ticker": ["MSFT", "", "AAPL", "GOOGL"]})

    assert filter_and_clean(df, provider).to_dict(as_series=False) == {"Ticker": expected}


def test_filter_and_clean_handles_empty_dataframe() -> None:
    result = filter_and_clean(pl.DataFrame({"Ticker": []}), "nasdaq")

    assert result.to_dict(as_series=False) == {"Ticker": []}


@patch("index_etfs.sources.read_url", return_value=io.BytesIO())
@patch("index_etfs.sources.pl.read_excel")
def test_load_ssga_excel_handles_polars_shapes(mock_read_excel: MagicMock, _mock_read_url: MagicMock) -> None:
    df = pl.DataFrame({"Ticker": ["AAPL"]})
    mock_read_excel.side_effect = [{"Sheet1": df}, df]

    assert load_ssga_excel("https://example.com/file.xlsx").equals(df)
    assert load_ssga_excel("https://example.com/file.xlsx").equals(df)


@patch("index_etfs.sources.read_url", return_value=io.BytesIO())
@patch("index_etfs.sources.pl.read_csv")
def test_load_ishares_csv_renames_weight(mock_read_csv: MagicMock, _mock_read_url: MagicMock) -> None:
    mock_read_csv.return_value = pl.DataFrame(
        {"Ticker": ["AAPL"], "Name": ["Apple"], "Weight (%)": [5.0], "Market Currency": ["USD"]}
    )

    assert load_ishares_csv("https://example.com/file.csv").columns == [
        "Ticker",
        "Name",
        "Weight",
        "Market Currency",
    ]


@pytest.mark.parametrize(
    ("payload", "match"),
    [
        (b'{"data":{"data":{"rows":[{"symbol":"AAPL"},{"symbol":"BRK/B"}]}}}', None),
        (b'{"data":{}}', "Nasdaq response"),
        (b'{"data":{"data":{"rows":[{}]}}}', "symbols"),
    ],
)
def test_load_nasdaq_json(payload: bytes, match: str | None) -> None:
    with patch("index_etfs.sources.read_url", return_value=io.BytesIO(payload)):
        if match:
            with pytest.raises(ValueError, match=match):
                load_nasdaq_json("https://example.com/data.json")
        else:
            assert load_nasdaq_json("https://example.com/data.json")["Ticker"].to_list() == ["AAPL", "BRK.B"]


@patch("index_etfs.sources.urllib.request.urlopen", return_value=io.BytesIO(b"ok"))
def test_read_url_uses_headers(mock_urlopen: MagicMock) -> None:
    assert read_url("https://example.com/data.csv").read() == b"ok"
    assert mock_urlopen.call_args.args[0].get_header("User-agent") == FIREFOX_HEADERS["User-Agent"]


def test_load_holdings_rejects_unknown_provider() -> None:
    with pytest.raises(ValueError, match="Unknown provider"):
        load_holdings(ETFConfig("https://example.com/data", "bad", "bad", 1))  # type: ignore[arg-type]


@patch(
    "index_etfs.outputs.urllib.request.urlopen",
    return_value=io.BytesIO(
        b'{"data":[{"s":"NASDAQ:AAPL","d":["AAPL","Technology Services","Consumer Electronics"]}]}'
    ),
)
def test_tradingview_symbols(mock_urlopen: MagicMock) -> None:
    assert tradingview_symbols(["AAPL"]) == {"AAPL": ("NASDAQ:AAPL", "Technology Services")}
    request = mock_urlopen.call_args.args[0]
    assert request.get_header("Content-type") == "application/json"
    assert json.loads(request.data)["columns"] == ["name", "sector", "industry"]


def test_tradingview_symbols_rejects_invalid_chunk_size() -> None:
    with pytest.raises(ValueError, match="chunk_size"):
        tradingview_symbols(["AAPL"], chunk_size=0)


def test_watchlist_lines_groups_by_header() -> None:
    assert watchlist_lines(
        ["AAPL", "MSFT", "BRK.B"],
        {"AAPL": ("NASDAQ:AAPL", "Technology"), "MSFT": ("NASDAQ:MSFT", "Technology")},
    ) == ["###Other", "BRK.B", "###Technology", "NASDAQ:AAPL", "NASDAQ:MSFT"]


@patch("index_etfs.outputs.tradingview_symbols", return_value={"AAPL": ("NASDAQ:AAPL", "Technology")})
def test_save_holdings_writes_outputs(_mock_symbols: MagicMock, tmp_path: Path) -> None:
    save_holdings(pl.DataFrame({"Ticker": ["AAPL"]}), "spy", tmp_path)
    save_holdings(pl.DataFrame({"Ticker": ["TEST"]}), "test", tmp_path)

    assert (tmp_path / "tickers" / "spy.txt").read_text() == "AAPL\n"
    assert (tmp_path / "watchlists" / "sp500.txt").read_text() == "###Technology\nNASDAQ:AAPL\n"
    assert (tmp_path / "tickers" / "test.txt").read_text() == "TEST\n"


def test_write_metadata(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    write_metadata({"spy": 503}, tmp_path)
    monkeypatch.chdir(tmp_path)
    write_metadata({"qqq": 100})

    metadata = json.loads((tmp_path / "metadata" / "latest.json").read_text())
    assert metadata["generated_at"].endswith("Z")
    assert metadata["watchlists"]["nasdaq100"]["source_url"] == catalog.ETF_CONFIGS["qqq"].url


@patch("index_etfs.outputs.tradingview_symbols", return_value={})
def test_get_etf_holdings_downloads_cleans_and_writes(_mock_symbols: MagicMock, tmp_path: Path) -> None:
    rows = [{"symbol": f"TICK{i}"} for i in range(100)]
    payload = json.dumps({"data": {"data": {"rows": rows}}}).encode()

    with patch("index_etfs.sources.read_url", return_value=io.BytesIO(payload)):
        result = get_etf_holdings("QQQ", tmp_path)

    assert len(result) == 100
    assert (tmp_path / "tickers" / "qqq.txt").read_text().startswith("TICK0\n")
    assert (tmp_path / "watchlists" / "nasdaq100.txt").exists()


def test_get_etf_holdings_rejects_unknown_symbol() -> None:
    with pytest.raises(ValueError, match="Unknown ETF symbol"):
        get_etf_holdings("INVALID")


def test_validate_count_rejects_empty_and_tiny_results() -> None:
    with pytest.raises(ValueError, match="zero rows"):
        validate_count("spy", 0)
    with pytest.raises(ValueError, match="expected at least"):
        validate_count("iwm", 1799)
    validate_count("iwm", 1800)


@pytest.mark.parametrize(
    ("func", "expected_symbol"),
    [
        (get_spy_holdings, "spy"),
        (get_mdy_holdings, "mdy"),
        (get_spsm_holdings, "spsm"),
        (get_qqq_holdings, "qqq"),
        (get_iwm_holdings, "iwm"),
    ],
)
@patch("index_etfs.holdings.get_etf_holdings")
def test_individual_etf_functions(mock_get_holdings: MagicMock, func: object, expected_symbol: str) -> None:
    mock_get_holdings.return_value = pl.DataFrame()

    func()

    mock_get_holdings.assert_called_once_with(expected_symbol)


@patch("index_etfs.holdings.write_metadata")
@patch("index_etfs.holdings.get_etf_holdings")
def test_main_downloads_all_configured_etfs(mock_get_holdings: MagicMock, mock_write_metadata: MagicMock) -> None:
    mock_get_holdings.side_effect = [
        pl.DataFrame({"Ticker": range(config.expected_count)})
        for config in catalog.ETF_CONFIGS.values()
    ]

    holdings.main()

    assert mock_get_holdings.call_count == len(catalog.ETF_CONFIGS)
    mock_write_metadata.assert_called_once()
