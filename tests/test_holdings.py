"""🧪 Tests for the holdings module."""

import io
import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

import index_etfs.holdings as holdings
from index_etfs.holdings import (
    FIREFOX_HEADERS,
    _filter_and_clean,
    _tradingview_prefixes,
    _validate_count,
    _write_metadata,
    _load_ishares_csv,
    _load_nasdaq_json,
    _load_ssga_excel,
    _read_url,
    _save_holdings,
    get_etf_holdings,
    get_iwm_holdings,
    get_mdy_holdings,
    get_qqq_holdings,
    get_spsm_holdings,
    get_spy_holdings,
)


# 🎯 Test Fixtures


@pytest.fixture
def sample_ssga_data() -> pl.DataFrame:
    """Create sample SSGA-style holdings data."""
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
    """Create sample iShares-style holdings data."""
    return pl.DataFrame(
        {
            "Ticker": ["AAPL", "MSFT", "-", "GOOGL"],
            "Name": ["Apple Inc", "Microsoft", "Cash", "Alphabet"],
            "Weight": [2.5, 2.0, 0.1, 1.8],
            "Market Currency": ["USD", "USD", "USD", "GBP"],
        }
    )


@pytest.fixture
def sample_nasdaq_data() -> pl.DataFrame:
    """Create sample Nasdaq-100 holdings data."""
    return pl.DataFrame({"Ticker": ["AAPL", "MSFT", "GOOGL"]})


@pytest.fixture
def temp_output_dir(tmp_path: Path) -> Path:
    """Create a temporary directory for test outputs."""
    return tmp_path / "holdings_output"


# 🧹 Tests for _filter_and_clean


@pytest.mark.parametrize(
    ("provider", "expected_len"),
    [
        ("ssga", 3),  # Should filter out EUR and "-" ticker
        ("ishares", 2),  # Should filter out GBP and "-" ticker
        ("nasdaq", 3),  # No currency filtering for nasdaq
    ],
)
def test_filter_and_clean(
    provider: str,
    expected_len: int,
    sample_ssga_data: pl.DataFrame,
    sample_ishares_data: pl.DataFrame,
    sample_nasdaq_data: pl.DataFrame,
) -> None:
    """🧪 Test filtering and cleaning data for different providers."""
    if provider == "ssga":
        data = sample_ssga_data
    elif provider == "ishares":
        data = sample_ishares_data
    else:
        data = sample_nasdaq_data

    result = _filter_and_clean(data, provider)

    assert len(result) == expected_len
    assert set(result.columns) == {"Ticker"}
    # ✅ Check sorted alphabetically
    tickers = result["Ticker"].to_list()
    assert tickers == sorted(tickers)


def test_filter_and_clean_removes_dash_tickers(sample_ssga_data: pl.DataFrame) -> None:
    """🧪 Test that dash tickers are filtered out."""
    result = _filter_and_clean(sample_ssga_data, "ssga")
    assert "-" not in result["Ticker"].to_list()


def test_filter_and_clean_keeps_only_usd(sample_ssga_data: pl.DataFrame) -> None:
    """🧪 Test that only USD currencies are kept for SSGA."""
    result = _filter_and_clean(sample_ssga_data, "ssga")
    # 💰 Should only have USD tickers (NVDA has EUR so should be filtered)
    assert len(result) == 3
    assert all(ticker in ["AAPL", "MSFT", "GOOGL"] for ticker in result["Ticker"])


# 💾 Tests for _save_holdings


def test_save_holdings_creates_txt_file(
    sample_ssga_data: pl.DataFrame, temp_output_dir: Path
) -> None:
    """🧪 Test that save_holdings creates a ticker text file."""
    df = _filter_and_clean(sample_ssga_data, "ssga")
    _save_holdings(df, "test", temp_output_dir)

    assert (temp_output_dir / "test.txt").exists()


def test_save_holdings_writes_sorted_tickers_with_trailing_newline(
    sample_ssga_data: pl.DataFrame, temp_output_dir: Path
) -> None:
    """🧪 Test ticker text output shape."""
    df = _filter_and_clean(sample_ssga_data, "ssga")
    _save_holdings(df, "test", temp_output_dir)

    assert (temp_output_dir / "test.txt").read_text() == "AAPL\nGOOGL\nMSFT\n"


def test_save_holdings_defaults_to_cwd(
    sample_ssga_data: pl.DataFrame, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """🧪 Test that save_holdings uses current directory when no output_dir given."""
    monkeypatch.chdir(tmp_path)
    df = _filter_and_clean(sample_ssga_data, "ssga")
    _save_holdings(df, "test")

    assert (tmp_path / "test.txt").exists()


@patch("index_etfs.holdings._tradingview_prefixes")
def test_save_holdings_writes_tradingview_watchlist(
    mock_prefixes: MagicMock, sample_ssga_data: pl.DataFrame, temp_output_dir: Path
) -> None:
    """🧪 Test TradingView watchlist output shape."""
    mock_prefixes.return_value = {
        "AAPL": "NASDAQ:AAPL",
        "GOOGL": "NASDAQ:GOOGL",
        "MSFT": "NASDAQ:MSFT",
    }
    df = _filter_and_clean(sample_ssga_data, "ssga")

    _save_holdings(df, "spy", temp_output_dir)

    assert (temp_output_dir / "watchlists" / "sp500.txt").read_text() == (
        "NASDAQ:AAPL\nNASDAQ:GOOGL\nNASDAQ:MSFT\n"
    )


# 🏢 Tests for loader functions


@patch("index_etfs.holdings._read_url", return_value=io.BytesIO())
@patch("index_etfs.holdings.pl.read_excel")
def test_load_ssga_excel_handles_dict_response(
    mock_read_excel: MagicMock, _mock_read_url: MagicMock
) -> None:
    """🧪 Test loading SSGA Excel when response is a dict."""
    mock_df = pl.DataFrame({"Ticker": ["AAPL"], "Weight": [7.5]})
    mock_read_excel.return_value = {"Sheet1": mock_df}

    result = _load_ssga_excel("http://example.com/data.xlsx")

    assert isinstance(result, pl.DataFrame)
    assert result.equals(mock_df)


@patch("index_etfs.holdings._read_url", return_value=io.BytesIO())
@patch("index_etfs.holdings.pl.read_excel")
def test_load_ssga_excel_handles_dataframe_response(
    mock_read_excel: MagicMock, _mock_read_url: MagicMock
) -> None:
    """🧪 Test loading SSGA Excel when response is already a DataFrame."""
    mock_df = pl.DataFrame({"Ticker": ["AAPL"], "Weight": [7.5]})
    mock_read_excel.return_value = mock_df

    result = _load_ssga_excel("http://example.com/data.xlsx")

    assert isinstance(result, pl.DataFrame)
    assert result.equals(mock_df)


@patch("index_etfs.holdings._read_url", return_value=io.BytesIO())
@patch("index_etfs.holdings.pl.read_csv")
def test_load_ishares_csv_renames_weight_column(
    mock_read_csv: MagicMock, _mock_read_url: MagicMock
) -> None:
    """🧪 Test that iShares loader renames Weight (%) column."""
    mock_df = pl.DataFrame(
        {
            "Ticker": ["AAPL"],
            "Name": ["Apple"],
            "Weight (%)": [5.0],
            "Market Currency": ["USD"],
        }
    )
    mock_read_csv.return_value = mock_df

    result = _load_ishares_csv("http://example.com/data.csv")

    assert "Weight" in result.columns
    assert "Weight (%)" not in result.columns


@patch(
    "index_etfs.holdings._read_url",
    return_value=io.BytesIO(
        b'{"data":{"data":{"rows":[{"symbol":"AAPL"},{"symbol":"BRK/B"}]}}}'
    ),
)
def test_load_nasdaq_json_returns_tickers(_mock_read_url: MagicMock) -> None:
    """🧪 Test loading Nasdaq-100 JSON."""
    result = _load_nasdaq_json("http://example.com/data.json")

    assert result["Ticker"].to_list() == ["AAPL", "BRK.B"]


@patch("index_etfs.holdings._read_url", return_value=io.BytesIO(b'{"data":{}}'))
def test_load_nasdaq_json_raises_on_missing_rows(_mock_read_url: MagicMock) -> None:
    """🧪 Test Nasdaq JSON fails closed on bad payloads."""
    with pytest.raises(ValueError, match="Nasdaq response"):
        _load_nasdaq_json("http://example.com/data.json")


@patch(
    "index_etfs.holdings._read_url",
    return_value=io.BytesIO(b'{"data":{"data":{"rows":[{}]}}}'),
)
def test_load_nasdaq_json_raises_on_missing_symbols(_mock_read_url: MagicMock) -> None:
    """🧪 Test Nasdaq JSON fails closed when rows have no symbols."""
    with pytest.raises(ValueError, match="symbols"):
        _load_nasdaq_json("http://example.com/data.json")


@patch("index_etfs.holdings.urllib.request.urlopen", return_value=io.BytesIO(b"ok"))
def test_read_url_uses_firefox_headers(mock_urlopen: MagicMock) -> None:
    """🧪 Test URL reads use browser-ish headers."""
    assert _read_url("http://example.com/data.csv").read() == b"ok"

    request = mock_urlopen.call_args.args[0]
    assert request.get_header("User-agent") == FIREFOX_HEADERS["User-Agent"]


@patch(
    "index_etfs.holdings.urllib.request.urlopen",
    return_value=io.BytesIO(
        b'{"data":[{"s":"NASDAQ:AAPL","d":["AAPL","NASDAQ","stock"]}]}'
    ),
)
def test_tradingview_prefixes_maps_symbols(mock_urlopen: MagicMock) -> None:
    """🧪 Test TradingView scanner mapping."""
    assert _tradingview_prefixes(["AAPL"]) == {"AAPL": "NASDAQ:AAPL"}

    request = mock_urlopen.call_args.args[0]
    assert request.get_header("Content-type") == "application/json"


# 📈 Tests for get_etf_holdings


def test_get_etf_holdings_raises_on_unknown_symbol() -> None:
    """🧪 Test that get_etf_holdings raises ValueError for unknown symbols."""
    with pytest.raises(ValueError, match="Unknown ETF symbol"):
        get_etf_holdings("INVALID")  # type: ignore[arg-type]



@patch("index_etfs.holdings._load_ssga_excel")
@patch("index_etfs.holdings._filter_and_clean")
@patch("index_etfs.holdings._save_holdings")
def test_get_etf_holdings_calls_correct_loader_for_ssga(
    mock_save: MagicMock,
    mock_filter: MagicMock,
    mock_load: MagicMock,
    sample_ssga_data: pl.DataFrame,
) -> None:
    """🧪 Test that SSGA ETFs use the correct loader."""
    mock_load.return_value = sample_ssga_data
    mock_filter.return_value = sample_ssga_data

    with patch("index_etfs.holdings._validate_count"):
        get_etf_holdings("spy")

    mock_load.assert_called_once()
    mock_filter.assert_called_once_with(sample_ssga_data, "ssga")
    mock_save.assert_called_once()


@patch("index_etfs.holdings._load_ishares_csv")
@patch("index_etfs.holdings._filter_and_clean")
@patch("index_etfs.holdings._save_holdings")
def test_get_etf_holdings_calls_correct_loader_for_ishares(
    mock_save: MagicMock,
    mock_filter: MagicMock,
    mock_load: MagicMock,
    sample_ishares_data: pl.DataFrame,
) -> None:
    """🧪 Test that iShares ETFs use the correct loader."""
    mock_load.return_value = sample_ishares_data
    mock_filter.return_value = sample_ishares_data

    with patch("index_etfs.holdings._validate_count"):
        get_etf_holdings("iwm")

    mock_load.assert_called_once()
    mock_filter.assert_called_once_with(sample_ishares_data, "ishares")
    mock_save.assert_called_once()


@patch("index_etfs.holdings._load_nasdaq_json")
@patch("index_etfs.holdings._filter_and_clean")
@patch("index_etfs.holdings._save_holdings")
def test_get_etf_holdings_calls_correct_loader_for_nasdaq(
    mock_save: MagicMock,
    mock_filter: MagicMock,
    mock_load: MagicMock,
    sample_nasdaq_data: pl.DataFrame,
) -> None:
    """🧪 Test that Nasdaq-100 uses the correct loader."""
    mock_load.return_value = sample_nasdaq_data
    mock_filter.return_value = sample_nasdaq_data

    with patch("index_etfs.holdings._validate_count"):
        get_etf_holdings("qqq")

    mock_load.assert_called_once()
    mock_filter.assert_called_once_with(sample_nasdaq_data, "nasdaq")
    mock_save.assert_called_once()


@patch("index_etfs.holdings._load_ssga_excel")
@patch("index_etfs.holdings._filter_and_clean")
@patch("index_etfs.holdings._save_holdings")
def test_get_etf_holdings_passes_output_dir(
    mock_save: MagicMock,
    mock_filter: MagicMock,
    mock_load: MagicMock,
    sample_ssga_data: pl.DataFrame,
    temp_output_dir: Path,
) -> None:
    """🧪 Test that output_dir is passed to save function."""
    mock_load.return_value = sample_ssga_data
    mock_filter.return_value = sample_ssga_data

    with patch("index_etfs.holdings._validate_count"):
        get_etf_holdings("spy", output_dir=temp_output_dir)

    mock_save.assert_called_once()
    call_args = mock_save.call_args
    assert call_args[0][1] == "spy"  # symbol
    assert call_args[0][2] == temp_output_dir  # output_dir


@patch("index_etfs.holdings._load_ssga_excel")
@patch("index_etfs.holdings._filter_and_clean")
@patch("index_etfs.holdings._save_holdings")
def test_get_etf_holdings_returns_dataframe(
    mock_save: MagicMock,
    mock_filter: MagicMock,
    mock_load: MagicMock,
    sample_ssga_data: pl.DataFrame,
) -> None:
    """🧪 Test that get_etf_holdings returns a DataFrame."""
    mock_load.return_value = sample_ssga_data
    cleaned_df = _filter_and_clean(sample_ssga_data, "ssga")
    mock_filter.return_value = cleaned_df

    with patch("index_etfs.holdings._validate_count"):
        result = get_etf_holdings("spy")

    assert isinstance(result, pl.DataFrame)
    assert result.equals(cleaned_df)


# 🎯 Tests for individual ETF functions


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
def test_individual_etf_functions(
    mock_get_holdings: MagicMock, func: callable, expected_symbol: str
) -> None:
    """🧪 Test that individual ETF functions call get_etf_holdings correctly."""
    mock_get_holdings.return_value = pl.DataFrame()

    func()

    mock_get_holdings.assert_called_once_with(expected_symbol)


# 🔧 Edge case tests


def test_filter_and_clean_handles_empty_dataframe() -> None:
    """🧪 Test that filter_and_clean handles empty DataFrames gracefully."""
    empty_df = pl.DataFrame(
        {
            "Ticker": [],
            "Name": [],
            "Weight": [],
            "Local Currency": [],
        }
    )

    result = _filter_and_clean(empty_df, "ssga")

    assert len(result) == 0
    assert set(result.columns) == {"Ticker"}  # Only returns Ticker column


def test_save_holdings_creates_directory_if_not_exists(
    sample_ssga_data: pl.DataFrame, tmp_path: Path
) -> None:
    """🧪 Test that save_holdings creates output directory if it doesn't exist."""
    output_dir = tmp_path / "nested" / "output" / "dir"
    assert not output_dir.exists()

    df = _filter_and_clean(sample_ssga_data, "ssga")
    _save_holdings(df, "test", output_dir)

    assert output_dir.exists()
    assert (output_dir / "test.txt").exists()


def test_get_etf_holdings_handles_case_insensitive_symbols() -> None:
    """🧪 Test that get_etf_holdings handles uppercase symbols."""
    with patch("index_etfs.holdings._load_ssga_excel") as mock_load, patch(
        "index_etfs.holdings._filter_and_clean"
    ) as mock_filter, patch("index_etfs.holdings._save_holdings"):
        mock_df = pl.DataFrame({"Ticker": ["AAPL"], "Name": ["Apple"], "Weight": [7.5]})
        mock_load.return_value = mock_df
        mock_filter.return_value = mock_df

        # 🔤 Should work with uppercase
        with patch("index_etfs.holdings._validate_count"):
            result = get_etf_holdings("SPY")  # type: ignore[arg-type]

        assert isinstance(result, pl.DataFrame)
        mock_load.assert_called_once()


def test_get_etf_holdings_raises_on_unknown_provider(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """🧪 Test that bad provider config fails closed."""
    monkeypatch.setitem(
        holdings.ETF_CONFIGS,
        "bad",
        {"url": "http://example.com/data", "provider": "bad"},
    )

    with pytest.raises(ValueError, match="Unknown provider"):
        get_etf_holdings("bad")  # type: ignore[arg-type]


def test_validate_count_rejects_empty_and_tiny_results() -> None:
    """🧪 Test source count validation."""
    with pytest.raises(ValueError, match="zero rows"):
        _validate_count("spy", 0)

    with pytest.raises(ValueError, match="expected at least"):
        _validate_count("iwm", 1799)

    _validate_count("iwm", 1800)


@patch("index_etfs.holdings._load_ssga_excel")
@patch("index_etfs.holdings._save_holdings")
def test_get_etf_holdings_validates_before_saving(
    mock_save: MagicMock, mock_load: MagicMock
) -> None:
    """🧪 Test bad counts fail before files are written."""
    mock_load.return_value = pl.DataFrame(
        {"Ticker": [], "Local Currency": [], "Name": [], "Weight": []}
    )

    with pytest.raises(ValueError, match="zero rows"):
        get_etf_holdings("spy")

    mock_save.assert_not_called()


def test_write_metadata_creates_latest_json(tmp_path: Path) -> None:
    """🧪 Test metadata output shape."""
    _write_metadata({"spy": 503, "qqq": 101}, tmp_path)

    metadata = json.loads((tmp_path / "metadata" / "latest.json").read_text())

    assert metadata["generated_at"].endswith("Z")
    assert metadata["watchlists"]["sp500"]["count"] == 503
    assert metadata["watchlists"]["nasdaq100"]["source"] == "qqq"
    assert "source_url" in metadata["watchlists"]["sp500"]


@patch("index_etfs.holdings._write_metadata")
@patch("index_etfs.holdings.get_etf_holdings")
def test_main_downloads_all_configured_etfs(
    mock_get_holdings: MagicMock, mock_write_metadata: MagicMock
) -> None:
    """🧪 Test CLI entry point processes every configured ETF."""
    mock_get_holdings.side_effect = [
        pl.DataFrame({"Ticker": range(holdings.EXPECTED_COUNTS[symbol])})
        for symbol in holdings.ETF_CONFIGS
    ]

    holdings.main()

    assert mock_get_holdings.call_count == len(holdings.ETF_CONFIGS)
    mock_write_metadata.assert_called_once()
