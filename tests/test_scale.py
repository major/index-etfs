"""Offline scale smoke tests for the full holdings refresh."""

import io
import json
import urllib.request
from pathlib import Path

import pytest

from index_etfs import catalog, holdings, http, outputs
from index_etfs.catalog import ETFConfig


def test_full_refresh_handles_fifteen_overlapping_indexes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Refresh 15 overlapping configurations from one provider with 230 unique tickers."""
    configs = {
        f"index{index:02}": ETFConfig(
            url=f"https://nasdaq.example.test/index/{index:02}",
            provider="nasdaq",
            expected_count=20,
        )
        for index in range(15)
    }
    tickers_by_symbol = {
        symbol: [f"TICK{ticker:03}" for ticker in range(start, start + 20)]
        for start, symbol in zip(
            (index * 15 for index in range(15)), configs, strict=True
        )
    }
    tickers_by_url = {
        config.url: tickers_by_symbol[symbol]
        for symbol, config in configs.items()
    }
    ticker_union = sorted(
        {ticker for tickers in tickers_by_symbol.values() for ticker in tickers}
    )
    assert len(ticker_union) == 230

    monkeypatch.setattr(catalog, "ETF_CONFIGS", configs)
    monkeypatch.setattr(holdings, "ETF_CONFIGS", configs)
    monkeypatch.setattr(outputs, "ETF_CONFIGS", configs)
    monkeypatch.chdir(tmp_path)

    events: list[tuple[str, str]] = []

    def mock_urlopen(
        request: str | urllib.request.Request, timeout: float = 30
    ) -> io.BytesIO:
        """Return fresh source JSON at the HTTP boundary."""
        assert timeout == 30
        url = request.full_url if isinstance(request, urllib.request.Request) else request
        method = request.get_method() if isinstance(request, urllib.request.Request) else "GET"
        events.append((method, url))

        assert method == "GET"
        assert url in tickers_by_url
        payload = {
            "data": {
                "data": {
                    "rows": [
                        {"symbol": ticker} for ticker in tickers_by_url[url]
                    ]
                }
            }
        }

        return io.BytesIO(json.dumps(payload).encode("utf-8"))

    def fail_on_sleep(*_args: object, **_kwargs: object) -> None:
        """Fail if the offline smoke test unexpectedly enters retry backoff."""
        raise AssertionError("HTTP retry sleep is not expected in this test")

    monkeypatch.setattr(urllib.request, "urlopen", mock_urlopen)
    monkeypatch.setattr(http.time, "sleep", fail_on_sleep)

    holdings.main()

    assert events == [
        ("GET", config.url) for config in configs.values()
    ]

    ticker_files = sorted((tmp_path / "tickers").glob("*.txt"))
    assert len(ticker_files) == 15
    for symbol, tickers in tickers_by_symbol.items():
        ticker_lines = (tmp_path / "tickers" / f"{symbol}.txt").read_text(
            encoding="utf-8"
        )
        assert ticker_lines == "".join(f"{ticker}\n" for ticker in sorted(tickers))
        assert ticker_lines.endswith("\n")

    assert not (tmp_path / "watchlists").exists()

    metadata = json.loads(
        (tmp_path / "metadata" / "latest.json").read_text(encoding="utf-8")
    )
    entries = metadata["etfs"]
    assert len(entries) == 15
    assert set(entries) == set(configs)
    for symbol, config in configs.items():
        entry = entries[symbol]
        assert entry["count"] == 20
        assert entry["source"] == symbol
        assert entry["source_url"] == config.url
        assert entry["membership_changes"] == {
            "baseline_available": False,
            "previous_count": None,
            "added_count": 0,
            "removed_count": 0,
            "added_tickers": [],
            "removed_tickers": [],
        }
        assert not {
            "unresolved_count",
            "unresolved_tickers",
            "ambiguous_count",
            "ambiguous_tickers",
            "sector_conflict_count",
            "sector_conflict_tickers",
        }.intersection(entry)
