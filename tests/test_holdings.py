"""Tests for the holdings pipeline."""

import io
import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from index_etfs import catalog, holdings, outputs
from index_etfs.catalog import ETFConfig, validate_count
from index_etfs.holdings import get_etf_holdings
from index_etfs.outputs import save_holdings, write_metadata
from index_etfs.sources import (
    FIREFOX_HEADERS,
    filter_and_clean,
    load_holdings,
    load_ishares_csv,
    load_nasdaq_json,
    load_ssga_excel,
    read_url,
)


def test_etf_config_registry_keys_preserve_order() -> None:
    """Keep configured ETF symbols and their iteration order stable."""
    assert list(catalog.ETF_CONFIGS) == [
        "spy",
        "mdy",
        "spsm",
        "qqq",
        "iwm",
        "smh",
        "gdx",
        "xlb",
        "xlc",
        "xle",
        "xlf",
        "xli",
        "xlk",
        "xlp",
        "xlre",
        "xlu",
        "xlv",
        "xly",
        "xme",
        "xop",
        "kre",
        "kbe",
        "xrt",
        "xtl",
        "gdxj",
        "moo",
        "nlr",
        "oih",
        "crak",
        "espo",
        "soxx",
        "igv",
        "ibb",
        "ihi",
        "ihe",
        "ihf",
        "ita",
        "iyt",
        "iai",
        "aiq",
        "bug",
        "copx",
        "ura",
        "sil",
        "mlpx",
        "lit",
        "pave",
        "igpt",
        "tan",
        "kbwb",
        "pej",
        "pbj",
        "arkg",
        "arkf",
        "cibr",
        "grid",
        "gunr",
        "volt",
        "vnq",
        "blok",
    ]


@pytest.mark.parametrize(
    ("symbol", "expected"),
    [
        (
            "spy",
            ETFConfig(
                url="https://www.ssga.com/us/en/individual/library-content/products/fund-data/etfs/us/holdings-daily-us-en-spy.xlsx",
                provider="ssga",
                expected_count=503,
            ),
        ),
        (
            "qqq",
            ETFConfig(
                url="https://api.nasdaq.com/api/quote/list-type/nasdaq100",
                provider="nasdaq",
                expected_count=100,
            ),
        ),
        (
            "smh",
            ETFConfig(
                url="https://www.vaneck.com/us/en/investments/semiconductor-etf-smh/downloads/holdings/",
                provider="vaneck",
                expected_count=25,
            ),
        ),
        (
            "gdx",
            ETFConfig(
                url="https://www.vaneck.com/us/en/investments/gold-miners-etf-gdx/downloads/holdings/",
                provider="vaneck",
                expected_count=32,
                scope_note="US-listed subset; foreign-market-qualified holdings are excluded.",
            ),
        ),
    ],
)
def test_etf_config_registry_lookup(symbol: str, expected: ETFConfig) -> None:
    """Keep representative ETF configuration lookups unchanged."""
    assert catalog.ETF_CONFIGS[symbol] == expected


@pytest.mark.parametrize(
    ("symbol", "expected_count"),
    [
        ("xlb", 24),
        ("xlc", 22),
        ("xle", 21),
        ("xlf", 76),
        ("xli", 83),
        ("xlk", 75),
        ("xlp", 35),
        ("xlre", 30),
        ("xlu", 31),
        ("xlv", 61),
        ("xly", 47),
    ],
)
def test_spdr_sector_etf_config(symbol: str, expected_count: int) -> None:
    """Configure each SPDR sector ETF with its official source and count."""
    # Arrange
    expected = ETFConfig(
        url=(
            "https://www.ssga.com/us/en/individual/library-content/products/"
            f"fund-data/etfs/us/holdings-daily-us-en-{symbol}.xlsx"
        ),
        provider="ssga",
        expected_count=expected_count,
    )

    # Act
    actual = catalog.ETF_CONFIGS[symbol]

    # Assert
    assert actual == expected


@pytest.mark.parametrize("operation", ["assignment", "deletion"])
def test_etf_config_registry_rejects_mutation(operation: str) -> None:
    """Reject item assignment and deletion on the ETF configuration registry."""
    with pytest.raises(TypeError):
        if operation == "assignment":
            catalog.ETF_CONFIGS["spy"] = catalog.ETF_CONFIGS["mdy"]
        else:
            del catalog.ETF_CONFIGS["spy"]


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
        load_holdings(ETFConfig("https://example.com/data", "bad", 1))  # type: ignore[arg-type]


def test_save_holdings_writes_ticker_output_without_watchlists(tmp_path: Path) -> None:
    """Saving ticker output creates no TradingView watchlist files."""
    # Arrange
    holdings = pl.DataFrame({"Ticker": ["MSFT", "AAPL"]})

    # Act
    save_holdings(holdings, "spy", tmp_path)

    # Assert
    assert (tmp_path / "tickers" / "spy.txt").read_text() == "MSFT\nAAPL\n"
    assert not (tmp_path / "watchlists").exists()


def test_write_metadata(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Write metadata to the requested path or the current working directory."""
    # Arrange / Act
    write_metadata({"spy": 503}, tmp_path)
    monkeypatch.chdir(tmp_path)
    write_metadata({"qqq": 100})

    # Assert
    metadata = json.loads((tmp_path / "metadata" / "latest.json").read_text())
    assert metadata["generated_at"].endswith("Z")
    assert metadata["etfs"]["qqq"]["source_url"] == catalog.ETF_CONFIGS["qqq"].url


def test_write_metadata_includes_membership_changes_by_etf_symbol(
    tmp_path: Path,
) -> None:
    """Metadata details are grouped by ETF and describe membership only."""
    # Arrange
    membership_changes = {
        "baseline_available": True,
        "previous_count": 2,
        "added_count": 1,
        "removed_count": 0,
        "added_tickers": ["MSFT"],
        "removed_tickers": [],
    }

    # Act
    write_metadata(
        {"spy": 503},
        tmp_path,
        details={"spy": {"membership_changes": membership_changes}},
    )

    # Assert
    metadata = json.loads((tmp_path / "metadata" / "latest.json").read_text())
    assert metadata["etfs"]["spy"]["membership_changes"] == membership_changes
    assert "unresolved_count" not in metadata["etfs"]["spy"]


def test_get_etf_holdings_uses_only_provider_and_writes_tickers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Fetching one ETF calls its provider and writes only a ticker file."""
    # Arrange
    provider_data = pl.DataFrame({"Ticker": [f"TICK{i:03}" for i in range(90)]})
    load = MagicMock(return_value=provider_data)
    monkeypatch.setattr(holdings, "load_holdings", load)

    # Act
    result = get_etf_holdings("QQQ", tmp_path)

    # Assert
    load.assert_called_once_with(catalog.ETF_CONFIGS["qqq"])
    assert result.height == 90
    assert (tmp_path / "tickers" / "qqq.txt").read_text().startswith("TICK000\n")
    assert not (tmp_path / "watchlists").exists()


def test_get_etf_holdings_rejects_unknown_symbol() -> None:
    with pytest.raises(ValueError, match="Unknown ETF symbol"):
        get_etf_holdings("INVALID")


def test_validate_count_rejects_empty_and_tiny_results() -> None:
    with pytest.raises(ValueError, match="zero rows"):
        validate_count("spy", 0)
    with pytest.raises(ValueError, match="expected at least"):
        validate_count("iwm", 1799)
    validate_count("iwm", 1800)


@pytest.mark.parametrize(("symbol", "minimum"), [("smh", 23), ("gdx", 29)])
def test_validate_count_vaneck_minimum_boundary(symbol: str, minimum: int) -> None:
    """Accept the 90% count threshold and reject the count immediately below."""
    # Arrange
    below_minimum = minimum - 1

    # Act / Assert
    with pytest.raises(ValueError, match=f"expected at least {minimum}"):
        validate_count(symbol, below_minimum)
    assert validate_count(symbol, minimum) is None


def test_main_fetches_each_etf_and_publishes_membership_diagnostics(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The full refresh fetches providers and records only membership diagnostics."""
    configs = {
        "alpha": ETFConfig("https://example.com/alpha", "nasdaq", 2),
        "beta": ETFConfig("https://example.com/beta", "nasdaq", 2),
    }
    # Arrange
    monkeypatch.setattr(catalog, "ETF_CONFIGS", configs)
    monkeypatch.setattr(holdings, "ETF_CONFIGS", configs)
    monkeypatch.setattr(outputs, "ETF_CONFIGS", configs)
    events: list[str] = []

    def load(config: ETFConfig) -> pl.DataFrame:
        """Return mocked source data for one configured ETF provider."""
        events.append(f"fetch:{config.url.rsplit('/', 1)[-1]}")
        tickers = (
            ["AAPL", "AAA"]
            if config.url.endswith("alpha")
            else ["AAPL", "BBB"]
        )
        return pl.DataFrame({"Ticker": tickers})

    monkeypatch.setattr(holdings, "load_holdings", load)
    monkeypatch.setattr(holdings.Path, "cwd", lambda: tmp_path)

    # Act
    holdings.main()

    # Assert
    assert events == ["fetch:alpha", "fetch:beta"]
    assert (tmp_path / "tickers" / "alpha.txt").read_text() == "AAA\nAAPL\n"
    assert (tmp_path / "tickers" / "beta.txt").read_text() == "AAPL\nBBB\n"
    assert not (tmp_path / "watchlists").exists()
    metadata = json.loads((tmp_path / "metadata" / "latest.json").read_text())
    assert metadata["etfs"]["alpha"]["count"] == 2
    assert (
        metadata["etfs"]["alpha"]["membership_changes"]["baseline_available"]
        is False
    )
    assert set(metadata["etfs"]["alpha"]) == {
        "count",
        "expected_count",
        "minimum_count",
        "source",
        "source_url",
        "membership_changes",
    }
