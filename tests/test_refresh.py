"""Integration tests for a staged full holdings refresh."""

import json
from pathlib import Path
from urllib.request import Request

import polars as pl
import pytest

from index_etfs import catalog, holdings, outputs
from index_etfs.catalog import ETFConfig


@pytest.fixture
def small_catalog(monkeypatch: pytest.MonkeyPatch) -> dict[str, ETFConfig]:
    """Replace all shared catalog references with two small test ETFs."""
    configs = {
        "alpha": ETFConfig("https://example.com/alpha", "nasdaq", "alpha", 3),
        "beta": ETFConfig("https://example.com/beta", "nasdaq", "beta", 2),
    }
    monkeypatch.setattr(catalog, "ETF_CONFIGS", configs)
    monkeypatch.setattr(holdings, "ETF_CONFIGS", configs)
    monkeypatch.setattr(outputs, "ETF_CONFIGS", configs)
    return configs


def _holdings_for(config: ETFConfig) -> pl.DataFrame:
    """Return deterministic provider rows with one overlap and one unresolved ticker."""
    tickers = ["AAA", "AAPL", "MISSING"] if config.watchlist_name == "alpha" else ["AAPL", "BBB"]
    return pl.DataFrame({"Ticker": tickers})


def _seed_outputs(output_dir: Path) -> dict[str, bytes]:
    """Create an old generated output set and return its exact bytes."""
    files = {
        "tickers/alpha.txt": b"OLD_ALPHA\n",
        "tickers/beta.txt": b"OLD_BETA\n",
        "watchlists/alpha.txt": b"OLD_ALPHA\n",
        "watchlists/alpha-grouped.txt": b"###Other\nOLD_ALPHA\n",
        "watchlists/beta.txt": b"OLD_BETA\n",
        "watchlists/beta-grouped.txt": b"###Other\nOLD_BETA\n",
        "metadata/latest.json": b'{"old": true}\n',
    }
    for relative_path, content in files.items():
        path = output_dir / relative_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
    return files


def _generated_snapshot(output_dir: Path) -> dict[str, bytes]:
    """Read all generated ticker, watchlist, and metadata files below a root."""
    return {
        path.relative_to(output_dir).as_posix(): path.read_bytes()
        for directory in ("tickers", "watchlists", "metadata")
        for path in sorted((output_dir / directory).glob("**/*"))
        if path.is_file()
    }


def _assert_no_staging_directories(output_dir: Path) -> None:
    """Ensure every temporary refresh directory was removed."""
    assert list(output_dir.glob(".holdings-*")) == []


@pytest.mark.parametrize(
    ("failure", "error", "message"),
    [
        ("provider", OSError, "late provider failure"),
        ("validation", ValueError, "expected at least"),
        ("scanner", OSError, "scanner failure"),
        ("staging", OSError, "staged write failure"),
    ],
)
def test_refresh_failures_preserve_existing_generated_files(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    small_catalog: dict[str, ETFConfig],
    failure: str,
    error: type[Exception],
    message: str,
) -> None:
    """Provider, validation, scanner, and staging errors do not publish partial outputs."""
    expected = _seed_outputs(tmp_path)
    monkeypatch.setattr(holdings.Path, "cwd", lambda: tmp_path)

    def load(config: ETFConfig) -> pl.DataFrame:
        """Return provider data or fail at the selected refresh stage."""
        if failure == "provider" and config.watchlist_name == "beta":
            raise OSError("late provider failure")
        if failure == "validation" and config.watchlist_name == "alpha":
            return pl.DataFrame({"Ticker": ["AAA"]})
        return _holdings_for(config)

    monkeypatch.setattr(holdings, "load_holdings", load)
    if failure == "scanner":
        def fail_scanner(_request: Request) -> bytes:
            """Raise the selected scanner transport failure."""
            raise OSError("scanner failure")

        monkeypatch.setattr(outputs, "fetch_bytes", fail_scanner)
    else:
        monkeypatch.setattr(outputs, "fetch_bytes", lambda _request: b'{"data":[]}')
    if failure == "staging":
        def fail_metadata(*_args: object, **_kwargs: object) -> None:
            """Raise the selected staged metadata write failure."""
            raise OSError("staged write failure")

        monkeypatch.setattr(holdings, "write_metadata", fail_metadata)

    with pytest.raises(error, match=message):
        holdings.main()

    assert _generated_snapshot(tmp_path) == expected
    _assert_no_staging_directories(tmp_path)


def test_first_refresh_failure_leaves_no_generated_files(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    small_catalog: dict[str, ETFConfig],
) -> None:
    """A failed first scanner lookup leaves generated output paths absent."""
    monkeypatch.setattr(holdings.Path, "cwd", lambda: tmp_path)
    monkeypatch.setattr(holdings, "load_holdings", _holdings_for)

    def fail_scanner(_request: Request) -> bytes:
        """Simulate an offline scanner during the first refresh."""
        raise OSError("offline")

    monkeypatch.setattr(outputs, "fetch_bytes", fail_scanner)

    with pytest.raises(OSError, match="offline"):
        holdings.main()

    assert _generated_snapshot(tmp_path) == {}
    _assert_no_staging_directories(tmp_path)


def test_duplicate_heavy_source_fails_validation_before_scanning_or_publication(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    small_catalog: dict[str, ETFConfig],
) -> None:
    """Duplicate collapse below the minimum count preserves old files and skips scanning."""
    expected = _seed_outputs(tmp_path)
    monkeypatch.setattr(holdings.Path, "cwd", lambda: tmp_path)

    def load(config: ETFConfig) -> pl.DataFrame:
        """Return enough duplicate provider rows to fail after unique cleanup."""
        raw = pl.DataFrame({"Ticker": ["AAA", "AAA", "AAA"]})
        assert len(raw) >= small_catalog[config.watchlist_name].expected_count
        return raw

    def fail_if_scanned(_tickers: list[str]) -> None:
        """Fail if orchestration proceeds to shared symbol resolution."""
        pytest.fail("scanner was called after duplicate rows failed validation")

    monkeypatch.setattr(holdings, "load_holdings", load)
    monkeypatch.setattr(holdings, "resolve_tradingview", fail_if_scanned)

    with pytest.raises(ValueError, match="expected at least 3"):
        holdings.main()

    assert _generated_snapshot(tmp_path) == expected
    _assert_no_staging_directories(tmp_path)


def test_successful_refresh_resolves_union_once_and_writes_matching_metadata(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    small_catalog: dict[str, ETFConfig],
) -> None:
    """One shared scanner lookup produces per-ETF outputs and accurate metadata."""
    monkeypatch.setattr(holdings.Path, "cwd", lambda: tmp_path)
    monkeypatch.setattr(holdings, "load_holdings", _holdings_for)
    resolve_calls: list[list[str]] = []
    original_resolve = outputs.resolve_tradingview

    def resolve(tickers: list[str]) -> outputs.TradingViewResolution:
        """Track the shared ticker-union call while using the real resolver."""
        resolve_calls.append(tickers)
        return original_resolve(tickers)

    monkeypatch.setattr(holdings, "resolve_tradingview", resolve)
    scanner_requests: list[list[str]] = []
    rows = [
        {"s": "NASDAQ:AAPL", "d": ["AAPL", "Technology"]},
        {"s": "NYSE:AAA", "d": ["AAA", "Technology"]},
        {"s": "NASDAQ:AAA", "d": ["AAA", "Finance"]},
        {"s": "NYSE:BBB", "d": ["BBB", "Technology"]},
        {"s": "NYSE:BBB", "d": ["BBB", "Utilities"]},
    ]

    def scan(request: Request) -> bytes:
        """Return scanner rows and record requested ticker batches."""
        payload = json.loads(request.data)
        scanner_requests.append(payload["filter"][0]["right"])
        return json.dumps({"data": rows}).encode()

    monkeypatch.setattr(outputs, "fetch_bytes", scan)
    replaced_targets: list[str] = []
    original_replace = Path.replace

    def record_replace(path: Path, target: str | Path) -> Path:
        """Record published targets before performing their normal replacement."""
        replaced_targets.append(Path(target).relative_to(tmp_path).as_posix())
        return original_replace(path, target)

    monkeypatch.setattr(Path, "replace", record_replace)

    holdings.main()

    assert resolve_calls == [["AAA", "AAPL", "BBB", "MISSING"]]
    assert scanner_requests == [["AAA", "AAPL", "BBB", "MISSING"]]
    assert replaced_targets[-1] == "metadata/latest.json"
    assert (tmp_path / "tickers" / "alpha.txt").read_text() == "AAA\nAAPL\nMISSING\n"
    assert (tmp_path / "tickers" / "beta.txt").read_text() == "AAPL\nBBB\n"
    assert (tmp_path / "watchlists" / "alpha.txt").read_text() == "AAA\nNASDAQ:AAPL\nMISSING\n"

    metadata = json.loads((tmp_path / "metadata" / "latest.json").read_text())
    alpha = metadata["watchlists"]["alpha"]
    beta = metadata["watchlists"]["beta"]
    assert alpha["count"] == 3
    assert alpha["unresolved_count"] == 1
    assert alpha["unresolved_tickers"] == ["MISSING"]
    assert alpha["ambiguous_count"] == 1
    assert alpha["ambiguous_tickers"] == ["AAA"]
    assert alpha["sector_conflict_count"] == 0
    assert alpha["sector_conflict_tickers"] == []
    assert beta["count"] == 2
    assert beta["unresolved_count"] == 0
    assert beta["unresolved_tickers"] == []
    assert beta["ambiguous_count"] == 0
    assert beta["ambiguous_tickers"] == []
    assert beta["sector_conflict_count"] == 1
    assert beta["sector_conflict_tickers"] == ["BBB"]
    _assert_no_staging_directories(tmp_path)


def test_metadata_replace_failure_propagates(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    small_catalog: dict[str, ETFConfig],
) -> None:
    """A final metadata replacement error is returned to the caller and staging is cleaned."""
    old_outputs = _seed_outputs(tmp_path)
    monkeypatch.setattr(holdings.Path, "cwd", lambda: tmp_path)
    monkeypatch.setattr(holdings, "load_holdings", _holdings_for)
    monkeypatch.setattr(outputs, "fetch_bytes", lambda _request: b'{"data":[]}')
    original_replace = Path.replace

    def fail_metadata_replace(path: Path, target: str | Path) -> Path:
        """Raise when publication reaches the final metadata target."""
        if Path(target) == tmp_path / "metadata" / "latest.json":
            raise OSError("metadata replace failure")
        return original_replace(path, target)

    monkeypatch.setattr(Path, "replace", fail_metadata_replace)

    with pytest.raises(OSError, match="metadata replace failure"):
        holdings.main()

    assert (tmp_path / "metadata" / "latest.json").read_bytes() == old_outputs[
        "metadata/latest.json"
    ]
    assert (tmp_path / "tickers" / "alpha.txt").read_text() == "AAA\nAAPL\nMISSING\n"
    _assert_no_staging_directories(tmp_path)
