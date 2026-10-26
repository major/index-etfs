"""📊 Extract ticker symbols from index ETFs."""

from pathlib import Path
from tempfile import TemporaryDirectory

import polars as pl

from .catalog import ETF_CONFIGS, validate_catalog, validate_count
from .outputs import resolve_tradingview, save_holdings, write_metadata
from .sources import filter_and_clean, load_holdings


def _fetch_holdings(symbol_lower: str) -> pl.DataFrame:
    """Fetch, clean, and validate holdings for a normalized ETF symbol."""
    config = ETF_CONFIGS[symbol_lower]
    df = filter_and_clean(load_holdings(config), config.provider)
    validate_count(symbol_lower, len(df))
    return df


def get_etf_holdings(
    symbol: str,
    output_dir: Path | None = None,
) -> pl.DataFrame:
    """Download, clean, save, and return holdings for one configured ETF."""
    symbol_lower = symbol.lower()

    if symbol_lower not in ETF_CONFIGS:
        raise ValueError(
            f"Unknown ETF symbol: {symbol}. "
            f"Valid symbols: {', '.join(ETF_CONFIGS.keys())}"
        )

    df = _fetch_holdings(symbol_lower)
    save_holdings(df, symbol_lower, output_dir)
    return df


def _publish_staged_files(staging_dir: Path, output_dir: Path) -> None:
    """Replace generated outputs from staging, with metadata published last."""
    staged_files = sorted(path for path in staging_dir.rglob("*") if path.is_file())
    staged_metadata = staging_dir / "metadata" / "latest.json"
    if staged_metadata not in staged_files:
        raise ValueError("Staged refresh is missing metadata/latest.json")

    for staged_file in staged_files:
        if staged_file == staged_metadata:
            continue
        destination = output_dir / staged_file.relative_to(staging_dir)
        destination.parent.mkdir(parents=True, exist_ok=True)
        staged_file.replace(destination)

    metadata_destination = output_dir / "metadata" / "latest.json"
    metadata_destination.parent.mkdir(parents=True, exist_ok=True)
    staged_metadata.replace(metadata_destination)


def main() -> None:
    """🚀 Download all ETF holdings."""
    validate_catalog()
    output_dir = Path.cwd()
    results: dict[str, pl.DataFrame] = {}

    for symbol in ETF_CONFIGS:
        print(f"📥 Fetching and validating {symbol.upper()}...")
        results[symbol] = _fetch_holdings(symbol)
        print(f"  ✅ {symbol.upper()} ready ({len(results[symbol])} rows)")

    tickers = sorted(
        {ticker for df in results.values() for ticker in df["Ticker"].to_list()}
    )
    print(f"🔎 Resolving TradingView symbols for {len(tickers)} unique tickers...")
    resolution = resolve_tradingview(tickers)

    tickers_by_symbol = {
        symbol: set(df["Ticker"].to_list()) for symbol, df in results.items()
    }
    details = {}
    for symbol, etf_tickers in tickers_by_symbol.items():
        unresolved = [
            ticker for ticker in resolution.unresolved if ticker in etf_tickers
        ]
        ambiguous = [
            ticker for ticker in resolution.ambiguous if ticker in etf_tickers
        ]
        sector_conflicts = [
            ticker for ticker in resolution.sector_conflicts if ticker in etf_tickers
        ]
        details[symbol] = {
            "unresolved_count": len(unresolved),
            "unresolved_tickers": unresolved,
            "ambiguous_count": len(ambiguous),
            "ambiguous_tickers": ambiguous,
            "sector_conflict_count": len(sector_conflicts),
            "sector_conflict_tickers": sector_conflicts,
        }
    counts = {symbol: len(df) for symbol, df in results.items()}

    output_dir.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(dir=output_dir, prefix=".holdings-") as staging:
        staging_dir = Path(staging)
        for symbol, df in results.items():
            save_holdings(df, symbol, staging_dir, symbols=resolution.symbols)
        write_metadata(counts, staging_dir, details=details)
        _publish_staged_files(staging_dir, output_dir)

    print("🎉 All holdings downloaded successfully!")


if __name__ == "__main__":
    main()
