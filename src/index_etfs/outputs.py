"""Write ticker, watchlist, and metadata outputs."""

import json
import math
import urllib.request
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

from .catalog import ETF_CONFIGS, MIN_EXPECTED_RATIO
from .http import fetch_bytes
from .sources import FIREFOX_HEADERS

TRADINGVIEW_SCAN_URL = "https://scanner.tradingview.com/america/scan"


@dataclass
class TradingViewResolution:
    """Resolved TradingView symbols and tickers that need review."""

    symbols: dict[str, tuple[str, str]]
    unresolved: list[str]
    ambiguous: list[str]
    sector_conflicts: list[str]


def write_metadata(
    results: dict[str, int],
    output_dir: Path | None = None,
    *,
    details: dict[str, dict] | None = None,
) -> None:
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
                **({"scope_note": config.scope_note} if config.scope_note else {}),
                **(details or {}).get(symbol, {}),
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
        "columns": ["name", "sector"],
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
    try:
        payload = json.loads(fetch_bytes(request))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("Invalid TradingView scanner JSON response") from error

    if not isinstance(payload, dict):
        raise ValueError("Invalid TradingView scanner response: expected an object")  # noqa: TRY004
    rows = payload.get("data")
    if not isinstance(rows, list):
        raise ValueError("Invalid TradingView scanner response: data must be a list")  # noqa: TRY004

    for index, row in enumerate(rows):
        if not isinstance(row, dict):
            message = f"Invalid TradingView scanner response: row {index} must be an object"
            raise ValueError(message)  # noqa: TRY004

        symbol = row.get("s")
        if not isinstance(symbol, str) or not symbol.strip():
            raise ValueError(f"Invalid TradingView scanner response: row {index} has an invalid symbol")
        exchange, separator, ticker_symbol = symbol.partition(":")
        if (
            not separator
            or symbol.count(":") != 1
            or not exchange.strip()
            or not ticker_symbol.strip()
        ):
            raise ValueError(
                f"Invalid TradingView scanner response: row {index} symbol must be exchange:symbol"
            )

        data = row.get("d")
        if not isinstance(data, list) or len(data) < 2:
            raise ValueError(f"Invalid TradingView scanner response: row {index} data must have two values")
        name, sector = data[:2]
        if not isinstance(name, str) or not name.strip():
            raise ValueError(f"Invalid TradingView scanner response: row {index} has an invalid name")
        if sector is not None and not isinstance(sector, str):
            raise ValueError(f"Invalid TradingView scanner response: row {index} has an invalid sector")

    return rows


def resolve_tradingview(tickers: list[str], chunk_size: int = 200) -> TradingViewResolution:
    """Resolve a sorted, unique ticker union into unambiguous TradingView symbols."""
    if chunk_size <= 0:
        raise ValueError("chunk_size must be positive")

    unique_tickers = sorted(set(tickers))
    candidates: dict[str, set[str]] = {}
    sectors_by_symbol: dict[str, set[str]] = {}

    for start in range(0, len(unique_tickers), chunk_size):
        chunk = unique_tickers[start : start + chunk_size]
        chunk_tickers = set(chunk)
        for row in _scan_tradingview(chunk):
            ticker, sector = row["d"][:2]
            if ticker not in chunk_tickers:
                continue

            symbol = row["s"]
            candidates.setdefault(ticker, set()).add(symbol)
            sectors_by_symbol.setdefault(symbol, set()).add(sector or "Other")

    symbols: dict[str, tuple[str, str]] = {}
    ambiguous = [ticker for ticker in unique_tickers if len(candidates.get(ticker, ())) > 1]
    unresolved = [ticker for ticker in unique_tickers if ticker not in candidates]
    sector_conflicts = [
        ticker
        for ticker in unique_tickers
        if any(len(sectors_by_symbol[symbol]) > 1 for symbol in candidates.get(ticker, ()))
    ]

    for ticker in unique_tickers:
        ticker_symbols = candidates.get(ticker, set())
        if len(ticker_symbols) != 1:
            continue

        symbol = next(iter(ticker_symbols))
        sectors = sectors_by_symbol[symbol]
        group = "Other" if len(sectors) > 1 else next(iter(sectors))
        symbols[ticker] = (symbol, group)

    if unresolved:
        print(f"⚠️  No TradingView metadata for {', '.join(unresolved)}")
    if ambiguous:
        print(f"⚠️  Ambiguous TradingView symbols for {', '.join(ambiguous)}")
    if sector_conflicts:
        print(f"⚠️  Conflicting TradingView sectors for {', '.join(sector_conflicts)}")

    return TradingViewResolution(symbols, unresolved, ambiguous, sector_conflicts)


def tradingview_symbols(tickers: list[str], chunk_size: int = 200) -> dict[str, tuple[str, str]]:
    """Map tickers to TradingView symbols for callers using the legacy API."""
    return resolve_tradingview(tickers, chunk_size).symbols


def watchlist_lines(tickers: list[str], symbols: dict[str, tuple[str, str]]) -> list[str]:
    """Build ungrouped TradingView watchlist lines."""
    return [symbols.get(ticker, (ticker, "Other"))[0] for ticker in tickers]


def grouped_watchlist_lines(tickers: list[str], symbols: dict[str, tuple[str, str]]) -> list[str]:
    """Build TradingView watchlist lines grouped by ### headers."""
    groups: dict[str, list[str]] = {}
    for ticker in tickers:
        symbol, group = symbols.get(ticker, (ticker, "Other"))
        groups.setdefault(group, []).append(symbol)

    return [line for group in sorted(groups) for line in (f"###{group}", *groups[group])]


def _write_lines(path: Path, lines: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n")


def _save_watchlist(
    tickers: list[str],
    watchlist_name: str,
    output_dir: Path,
    *,
    symbols: dict[str, tuple[str, str]] | None = None,
) -> None:
    """Write plain and grouped TradingView watchlists."""
    if symbols is None:
        symbols = tradingview_symbols(tickers)

    watchlist_dir = output_dir / "watchlists"
    _write_lines(watchlist_dir / f"{watchlist_name}.txt", watchlist_lines(tickers, symbols))
    _write_lines(
        watchlist_dir / f"{watchlist_name}-grouped.txt",
        grouped_watchlist_lines(tickers, symbols),
    )


def save_holdings(
    df: pl.DataFrame,
    symbol: str,
    output_dir: Path | None = None,
    *,
    symbols: dict[str, tuple[str, str]] | None = None,
) -> None:
    """Save ticker symbols and TradingView watchlists."""
    if output_dir is None:
        output_dir = Path.cwd()

    tickers = df["Ticker"].to_list()
    _write_lines(output_dir / "tickers" / f"{symbol}.txt", tickers)

    if config := ETF_CONFIGS.get(symbol):
        _save_watchlist(tickers, config.watchlist_name, output_dir, symbols=symbols)
