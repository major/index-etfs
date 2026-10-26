"""Integration tests for a staged full holdings refresh."""

import json
from pathlib import Path

import polars as pl
import pytest

from index_etfs import catalog, holdings, outputs
from index_etfs.catalog import ETFConfig


@pytest.fixture
def small_catalog(monkeypatch: pytest.MonkeyPatch) -> dict[str, ETFConfig]:
    """Replace all shared catalog references with two small test ETFs."""
    configs = {
        "alpha": ETFConfig("https://example.com/alpha", "nasdaq", 3),
        "beta": ETFConfig("https://example.com/beta", "nasdaq", 2),
    }
    monkeypatch.setattr(catalog, "ETF_CONFIGS", configs)
    monkeypatch.setattr(holdings, "ETF_CONFIGS", configs)
    monkeypatch.setattr(outputs, "ETF_CONFIGS", configs)
    return configs


def _holdings_for(config: ETFConfig) -> pl.DataFrame:
    """Return deterministic provider rows with one overlap and one unknown ticker."""
    tickers = ["AAA", "AAPL", "MISSING"] if config.expected_count == 3 else ["AAPL", "BBB"]
    return pl.DataFrame({"Ticker": tickers})


def _seed_outputs(output_dir: Path) -> dict[str, bytes]:
    """Create old ticker and metadata outputs and return their exact bytes."""
    files = {
        "tickers/alpha.txt": b"OLD_ALPHA\n",
        "tickers/beta.txt": b"OLD_BETA\n",
        "metadata/latest.json": b'{"old": true}\n',
    }
    for relative_path, content in files.items():
        path = output_dir / relative_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
    return files


def _generated_snapshot(output_dir: Path) -> dict[str, bytes]:
    """Read ticker and metadata outputs, including any unexpected watchlist artifacts."""
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
    """Provider, validation, and staging errors do not publish partial outputs."""
    expected = _seed_outputs(tmp_path)
    monkeypatch.setattr(holdings.Path, "cwd", lambda: tmp_path)

    def load(config: ETFConfig) -> pl.DataFrame:
        """Return provider data or fail at the selected refresh stage."""
        if failure == "provider" and config.expected_count == 2:
            raise OSError("late provider failure")
        if failure == "validation" and config.expected_count == 3:
            return pl.DataFrame({"Ticker": ["AAA"]})
        return _holdings_for(config)

    monkeypatch.setattr(holdings, "load_holdings", load)
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
    """A first provider failure leaves generated output paths absent."""
    monkeypatch.setattr(holdings.Path, "cwd", lambda: tmp_path)

    def fail_provider(_config: ETFConfig) -> pl.DataFrame:
        """Simulate an unavailable provider during the first refresh."""
        raise OSError("offline")

    monkeypatch.setattr(holdings, "load_holdings", fail_provider)

    with pytest.raises(OSError, match="offline"):
        holdings.main()

    assert _generated_snapshot(tmp_path) == {}
    _assert_no_staging_directories(tmp_path)


def test_duplicate_heavy_source_fails_validation_before_membership_or_publication(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    small_catalog: dict[str, ETFConfig],
) -> None:
    """Duplicate collapse below the minimum count preserves old files."""
    expected = _seed_outputs(tmp_path)
    monkeypatch.setattr(holdings.Path, "cwd", lambda: tmp_path)

    def load(config: ETFConfig) -> pl.DataFrame:
        """Return enough duplicate provider rows to fail after unique cleanup."""
        raw = pl.DataFrame({"Ticker": ["AAA", "AAA", "AAA"]})
        assert len(raw) >= config.expected_count
        return raw

    def fail_if_membership_is_checked(*_args: object, **_kwargs: object) -> None:
        """Fail if refresh proceeds to membership comparison after validation."""
        pytest.fail("membership was checked after duplicate rows failed validation")

    monkeypatch.setattr(holdings, "load_holdings", load)
    monkeypatch.setattr(holdings, "compare_membership", fail_if_membership_is_checked)

    with pytest.raises(ValueError, match="expected at least 3"):
        holdings.main()

    assert _generated_snapshot(tmp_path) == expected
    _assert_no_staging_directories(tmp_path)


def test_successful_refresh_writes_tickers_and_symbol_keyed_metadata(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    small_catalog: dict[str, ETFConfig],
) -> None:
    """A successful refresh publishes ticker files and ETF-keyed metadata."""
    monkeypatch.setattr(holdings.Path, "cwd", lambda: tmp_path)
    monkeypatch.setattr(holdings, "load_holdings", _holdings_for)
    replaced_targets: list[str] = []
    original_replace = Path.replace

    def record_replace(path: Path, target: str | Path) -> Path:
        """Record published targets before performing their normal replacement."""
        replaced_targets.append(Path(target).relative_to(tmp_path).as_posix())
        return original_replace(path, target)

    monkeypatch.setattr(Path, "replace", record_replace)

    holdings.main()

    assert replaced_targets[-1] == "metadata/latest.json"
    assert (tmp_path / "tickers" / "alpha.txt").read_text() == "AAA\nAAPL\nMISSING\n"
    assert (tmp_path / "tickers" / "beta.txt").read_text() == "AAPL\nBBB\n"
    assert not (tmp_path / "watchlists").exists()

    metadata = json.loads((tmp_path / "metadata" / "latest.json").read_text())
    entries = metadata["etfs"]
    assert set(entries) == {"alpha", "beta"}
    alpha = entries["alpha"]
    beta = entries["beta"]
    assert alpha["count"] == 3
    assert alpha["expected_count"] == 3
    assert alpha["source"] == "alpha"
    assert alpha["source_url"] == "https://example.com/alpha"
    assert alpha["membership_changes"] == {
        "baseline_available": False,
        "previous_count": None,
        "added_count": 0,
        "removed_count": 0,
        "added_tickers": [],
        "removed_tickers": [],
    }
    assert beta["count"] == 2
    assert beta["expected_count"] == 2
    assert beta["source"] == "beta"
    assert beta["source_url"] == "https://example.com/beta"
    assert beta["membership_changes"] == alpha["membership_changes"]
    assert not {
        "unresolved_count",
        "unresolved_tickers",
        "ambiguous_count",
        "ambiguous_tickers",
        "sector_conflict_count",
        "sector_conflict_tickers",
    }.intersection(alpha)
    _assert_no_staging_directories(tmp_path)


def test_metadata_replace_failure_propagates_after_ticker_publication(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    small_catalog: dict[str, ETFConfig],
) -> None:
    """Metadata is replaced last, so its failure leaves old metadata intact."""
    old_outputs = _seed_outputs(tmp_path)
    monkeypatch.setattr(holdings.Path, "cwd", lambda: tmp_path)
    monkeypatch.setattr(holdings, "load_holdings", _holdings_for)
    original_replace = Path.replace
    replaced_targets: list[str] = []

    def fail_metadata_replace(path: Path, target: str | Path) -> Path:
        """Raise when publication reaches the final metadata target."""
        relative_target = Path(target).relative_to(tmp_path).as_posix()
        replaced_targets.append(relative_target)
        if Path(target) == tmp_path / "metadata" / "latest.json":
            raise OSError("metadata replace failure")
        return original_replace(path, target)

    monkeypatch.setattr(Path, "replace", fail_metadata_replace)

    with pytest.raises(OSError, match="metadata replace failure"):
        holdings.main()

    assert replaced_targets[-1] == "metadata/latest.json"
    assert (tmp_path / "metadata" / "latest.json").read_bytes() == old_outputs[
        "metadata/latest.json"
    ]
    assert (tmp_path / "tickers" / "alpha.txt").read_text() == "AAA\nAAPL\nMISSING\n"
    assert (tmp_path / "tickers" / "beta.txt").read_text() == "AAPL\nBBB\n"
    assert not (tmp_path / "watchlists").exists()
    _assert_no_staging_directories(tmp_path)
