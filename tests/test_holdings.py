"""🧪 Tests for the holdings module."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from index_etfs.holdings import (
    ETF_CONFIGS,
    _filter_and_clean,
    _load_direxion_csv,
    _load_ishares_csv,
    _load_ssga_excel,
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
def sample_direxion_data() -> pl.DataFrame:
    """Create sample Direxion-style holdings data (already renamed)."""
    return pl.DataFrame(
        {
            "Ticker": ["AAPL", "MSFT", "GOOGL"],
            "Name": ["Apple Inc", "Microsoft", "Alphabet"],
            "Weight": [10.5, 9.2, 8.1],
        }
    )


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
        ("direxion", 3),  # No filtering for direxion
    ],
)
def test_filter_and_clean(
    provider: str,
    expected_len: int,
    sample_ssga_data: pl.DataFrame,
    sample_ishares_data: pl.DataFrame,
    sample_direxion_data: pl.DataFrame,
) -> None:
    """🧪 Test filtering and cleaning data for different providers."""
    if provider == "ssga":
        data = sample_ssga_data
    elif provider == "ishares":
        data = sample_ishares_data
    else:
        data = sample_direxion_data

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


def test_save_holdings_creates_files(
    sample_ssga_data: pl.DataFrame, temp_output_dir: Path
) -> None:
    """🧪 Test that save_holdings creates CSV and MD files."""
    df = _filter_and_clean(sample_ssga_data, "ssga")
    _save_holdings(df, "test", temp_output_dir)

    assert (temp_output_dir / "test.csv").exists()
    assert (temp_output_dir / "test.md").exists()


def test_save_holdings_csv_has_rank_column(
    sample_ssga_data: pl.DataFrame, temp_output_dir: Path
) -> None:
    """🧪 Test that CSV output includes Rank column."""
    df = _filter_and_clean(sample_ssga_data, "ssga")
    _save_holdings(df, "test", temp_output_dir)

    result = pl.read_csv(temp_output_dir / "test.csv")
    assert "Rank" in result.columns


def test_save_holdings_defaults_to_cwd(
    sample_ssga_data: pl.DataFrame, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """🧪 Test that save_holdings uses current directory when no output_dir given."""
    monkeypatch.chdir(tmp_path)
    df = _filter_and_clean(sample_ssga_data, "ssga")
    _save_holdings(df, "test")

    assert (tmp_path / "test.csv").exists()
    assert (tmp_path / "test.md").exists()


# 🏢 Tests for loader functions


@patch("index_etfs.holdings.pl.read_excel")
def test_load_ssga_excel_handles_dict_response(mock_read_excel: MagicMock) -> None:
    """🧪 Test loading SSGA Excel when response is a dict."""
    mock_df = pl.DataFrame({"Ticker": ["AAPL"], "Weight": [7.5]})
    mock_read_excel.return_value = {"Sheet1": mock_df}

    result = _load_ssga_excel("http://example.com/data.xlsx")

    assert isinstance(result, pl.DataFrame)
    assert result.equals(mock_df)


@patch("index_etfs.holdings.pl.read_excel")
def test_load_ssga_excel_handles_dataframe_response(mock_read_excel: MagicMock) -> None:
    """🧪 Test loading SSGA Excel when response is already a DataFrame."""
    mock_df = pl.DataFrame({"Ticker": ["AAPL"], "Weight": [7.5]})
    mock_read_excel.return_value = mock_df

    result = _load_ssga_excel("http://example.com/data.xlsx")

    assert isinstance(result, pl.DataFrame)
    assert result.equals(mock_df)


@patch("index_etfs.holdings.pl.read_csv")
def test_load_ishares_csv_renames_weight_column(mock_read_csv: MagicMock) -> None:
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


@pytest.mark.parametrize(
    "input_columns",
    [
        # Old format
        {"Ticker": ["AAPL"], "Description": ["Apple Inc"], "% of Net Assets": [10.5]},
        # New format (as of 2025)
        {
            "StockTicker": ["AAPL"],
            "SecurityDescription": ["Apple Inc"],
            "HoldingsPercent": [10.5],
        },
    ],
)
@patch("index_etfs.holdings.pl.read_csv")
def test_load_direxion_csv_renames_columns(
    mock_read_csv: MagicMock, input_columns: dict
) -> None:
    """🧪 Test that Direxion loader renames columns properly (handles format changes)."""
    mock_df = pl.DataFrame(input_columns)
    mock_read_csv.return_value = mock_df

    result = _load_direxion_csv("http://example.com/data.csv")

    assert "Ticker" in result.columns
    assert "Name" in result.columns
    assert "Weight" in result.columns
    # Original columns should be renamed
    for col in input_columns.keys():
        if col not in ["Ticker", "Name", "Weight"]:
            assert col not in result.columns


# 📈 Tests for get_etf_holdings


def test_get_etf_holdings_raises_on_unknown_symbol() -> None:
    """🧪 Test that get_etf_holdings raises ValueError for unknown symbols."""
    with pytest.raises(ValueError, match="Unknown ETF symbol"):
        get_etf_holdings("INVALID")  # type: ignore[arg-type]


@pytest.mark.parametrize("symbol", ["spy", "mdy", "spsm", "qqq", "iwm"])
def test_etf_configs_exist(symbol: str) -> None:
    """🧪 Test that all expected ETF configs are present."""
    assert symbol in ETF_CONFIGS
    assert "url" in ETF_CONFIGS[symbol]
    assert "provider" in ETF_CONFIGS[symbol]


@pytest.mark.parametrize(
    ("symbol", "expected_provider"),
    [
        ("spy", "ssga"),
        ("mdy", "ssga"),
        ("spsm", "ssga"),
        ("qqq", "direxion"),
        ("iwm", "ishares"),
    ],
)
def test_etf_configs_have_correct_providers(
    symbol: str, expected_provider: str
) -> None:
    """🧪 Test that ETF configs have correct provider mappings."""
    assert ETF_CONFIGS[symbol]["provider"] == expected_provider


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

    get_etf_holdings("iwm")

    mock_load.assert_called_once()
    mock_filter.assert_called_once_with(sample_ishares_data, "ishares")
    mock_save.assert_called_once()


@patch("index_etfs.holdings._load_direxion_csv")
@patch("index_etfs.holdings._filter_and_clean")
@patch("index_etfs.holdings._save_holdings")
def test_get_etf_holdings_calls_correct_loader_for_direxion(
    mock_save: MagicMock,
    mock_filter: MagicMock,
    mock_load: MagicMock,
    sample_direxion_data: pl.DataFrame,
) -> None:
    """🧪 Test that Direxion ETFs use the correct loader."""
    mock_load.return_value = sample_direxion_data
    mock_filter.return_value = sample_direxion_data

    get_etf_holdings("qqq")

    mock_load.assert_called_once()
    mock_filter.assert_called_once_with(sample_direxion_data, "direxion")
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
    assert set(result.columns) == {"Ticker", "Name", "Weight"}


def test_save_holdings_creates_directory_if_not_exists(
    sample_ssga_data: pl.DataFrame, tmp_path: Path
) -> None:
    """🧪 Test that save_holdings creates output directory if it doesn't exist."""
    output_dir = tmp_path / "nested" / "output" / "dir"
    assert not output_dir.exists()

    df = _filter_and_clean(sample_ssga_data, "ssga")
    _save_holdings(df, "test", output_dir)

    assert output_dir.exists()
    assert (output_dir / "test.csv").exists()


def test_get_etf_holdings_handles_case_insensitive_symbols() -> None:
    """🧪 Test that get_etf_holdings handles uppercase symbols."""
    with patch("index_etfs.holdings._load_ssga_excel") as mock_load, patch(
        "index_etfs.holdings._filter_and_clean"
    ) as mock_filter, patch("index_etfs.holdings._save_holdings"):
        mock_df = pl.DataFrame({"Ticker": ["AAPL"], "Name": ["Apple"], "Weight": [7.5]})
        mock_load.return_value = mock_df
        mock_filter.return_value = mock_df

        # 🔤 Should work with uppercase
        result = get_etf_holdings("SPY")  # type: ignore[arg-type]

        assert isinstance(result, pl.DataFrame)
        mock_load.assert_called_once()
