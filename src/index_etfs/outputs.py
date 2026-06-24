"""Write ticker, watchlist, and metadata outputs."""

import json
import math
import urllib.request
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

from .catalog import ETF_CONFIGS, MIN_EXPECTED_RATIO
from .sources import FIREFOX_HEADERS

TRADINGVIEW_SCAN_URL = "https://scanner.tradingview.com/america/scan"


def write_metadata(results: dict[str, int], output_dir: Path | None = None) -> None:
    """Write latest generated counts and source URLs."""
    if output_dir is None:
        output_dir = Path.cwd()

    metadata = {
        "generated_at": datetime.now(UTC)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z"),
        "watchlists": {
            config.watchlist_name: {
                "count": count,
                "expected_count": config.expected_count,
                "minimum_count": math.ceil(config.expected_count * MIN_EXPECTED_RATIO),
                "source": symbol,
                "source_url": config.url,
            }
            for symbol, count in results.items()
            for config in [ETF_CONFIGS[symbol]]
        },
    }

    metadata_dir = output_dir / "metadata"
    metadata_dir.mkdir(parents=True, exist_ok=True)
    (metadata_dir / "latest.json").write_text(json.dumps(metadata, indent=2) + "\n")


def _scan_tradingview(tickers: list[str]) -> list[dict]:
    """Fetch one TradingView scanner page."""
    payload = {
        "filter": [{"left": "name", "operation": "in_range", "right": tickers}],
        "columns": ["name", "sector", "industry"],
        "markets": ["america"],
    }
    request = urllib.request.Request(
        TRADINGVIEW_SCAN_URL,
        data=json.dumps(payload).encode(),
        headers=FIREFOX_HEADERS
        | {
            "Content-Type": "application/json",
            "Origin": "https://www.tradingview.com",
            "Referer": "https://www.tradingview.com/",
        },
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.load(response).get("data", [])


def tradingview_symbols(tickers: list[str], chunk_size: int = 200) -> dict[str, tuple[str, str]]:
    """Map tickers to TradingView symbols and sector-ish group names."""
    if chunk_size <= 0:
        raise ValueError("chunk_size must be positive")

    symbols = {}

    for start in range(0, len(tickers), chunk_size):
        chunk = tickers[start : start + chunk_size]
        for row in _scan_tradingview(chunk):
            data = row.get("d", [])
            ticker = data[0] if data else None
            symbol = row.get("s", "")
            group = next((value for value in data[1:3] if value), "Other")
            if ticker in chunk and ":" in symbol:
                symbols.setdefault(ticker, (symbol, group))

    return symbols


def watchlist_lines(tickers: list[str], symbols: dict[str, tuple[str, str]]) -> list[str]:
    """Build TradingView watchlist lines grouped by ### headers."""
    groups: dict[str, list[str]] = {}
    for ticker in tickers:
        symbol, group = symbols.get(ticker, (ticker, "Other"))
        groups.setdefault(group, []).append(symbol)

    return [line for group in sorted(groups) for line in (f"###{group}", *groups[group])]


def _write_lines(path: Path, lines: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n")


def _save_watchlist(tickers: list[str], watchlist_name: str, output_dir: Path) -> None:
    symbols = tradingview_symbols(tickers)
    missing = [ticker for ticker in tickers if ticker not in symbols]
    if missing:
        print(f"⚠️  No TradingView metadata for {', '.join(missing)}")

    _write_lines(
        output_dir / "watchlists" / f"{watchlist_name}.txt",
        watchlist_lines(tickers, symbols),
    )


def save_holdings(df: pl.DataFrame, symbol: str, output_dir: Path | None = None) -> None:
    """Save ticker symbols and TradingView watchlists."""
    if output_dir is None:
        output_dir = Path.cwd()

    tickers = df["Ticker"].to_list()
    _write_lines(output_dir / "tickers" / f"{symbol}.txt", tickers)

    if config := ETF_CONFIGS.get(symbol):
        _save_watchlist(tickers, config.watchlist_name, output_dir)
