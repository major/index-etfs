"""📊 Extract ticker symbols from index ETFs."""

from pathlib import Path

import polars as pl

from .catalog import ETF_CONFIGS, validate_count
from .outputs import save_holdings, write_metadata
from .sources import filter_and_clean, load_holdings


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

    config = ETF_CONFIGS[symbol_lower]
    df = filter_and_clean(load_holdings(config), config.provider)
    validate_count(symbol_lower, len(df))
    save_holdings(df, symbol_lower, output_dir)
    return df


def get_spy_holdings() -> pl.DataFrame:
    """📊 Get ticker symbols from the SPY ETF."""
    return get_etf_holdings("spy")


def get_mdy_holdings() -> pl.DataFrame:
    """📊 Get ticker symbols from the MDY ETF."""
    return get_etf_holdings("mdy")


def get_spsm_holdings() -> pl.DataFrame:
    """📊 Get ticker symbols from the SPSM ETF."""
    return get_etf_holdings("spsm")


def get_qqq_holdings() -> pl.DataFrame:
    """📊 Get ticker symbols from the Nasdaq-100."""
    return get_etf_holdings("qqq")


def get_iwm_holdings() -> pl.DataFrame:
    """📊 Get ticker symbols from the IWM ETF."""
    return get_etf_holdings("iwm")


def main() -> None:
    """🚀 Download all ETF holdings."""
    print("📥 Downloading ETF holdings...")
    output_dir = Path.cwd()
    results = {}

    for symbol in ETF_CONFIGS:
        print(f"  ⏳ Processing {symbol.upper()}...")
        df = get_etf_holdings(symbol, output_dir)
        count = len(df)
        results[symbol] = count
        print(f"  ✅ {symbol.upper()} complete! ({count} rows)")

    write_metadata(results, output_dir)
    print("🎉 All holdings downloaded successfully!")


if __name__ == "__main__":
    main()
