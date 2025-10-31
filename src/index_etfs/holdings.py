"""📊 Extract ticker symbols from various index ETFs.

This module downloads ETF holdings from various providers (SSGA, iShares, Direxion)
and extracts just the ticker symbols (e.g., AAPL, MSFT, GOOGL).
"""

from pathlib import Path
from typing import Literal

import polars as pl


# 🎯 ETF Configuration
ETF_CONFIGS = {
    "spy": {
        "url": "https://www.ssga.com/us/en/individual/library-content/products/fund-data/etfs/us/holdings-daily-us-en-spy.xlsx",
        "provider": "ssga",
    },
    "mdy": {
        "url": "https://www.ssga.com/us/en/individual/library-content/products/fund-data/etfs/us/holdings-daily-us-en-mdy.xlsx",
        "provider": "ssga",
    },
    "spsm": {
        "url": "https://www.ssga.com/us/en/individual/library-content/products/fund-data/etfs/us/holdings-daily-us-en-spsm.xlsx",
        "provider": "ssga",
    },
    "qqq": {
        "url": "https://www.direxion.com/holdings/QQQE.csv",
        "provider": "direxion",
    },
    "iwm": {
        "url": "https://www.ishares.com/us/products/239710/ishares-russell-2000-etf/1467271812596.ajax?fileType=csv&fileName=IWM_holdings&dataType=fund",
        "provider": "ishares",
    },
}

ETFSymbol = Literal["spy", "mdy", "spsm", "qqq", "iwm"]


def _load_ssga_excel(url: str) -> pl.DataFrame:
    """🏢 Load SSGA Excel format holdings data.

    Args:
        url: URL to the SSGA Excel file

    Returns:
        Polars DataFrame with holdings data
    """
    df_dict = pl.read_excel(url, sheet_id=0, read_options={"header_row": 4})

    # 🎲 Handle both dict and DataFrame returns
    if isinstance(df_dict, dict):
        df = list(df_dict.values())[0]
    else:
        df = df_dict

    return df


def _load_ishares_csv(url: str) -> pl.DataFrame:
    """🏢 Load iShares CSV format holdings data.

    Args:
        url: URL to the iShares CSV file

    Returns:
        Polars DataFrame with holdings data
    """
    df = pl.read_csv(
        url,
        skip_rows=9,
        columns=["Ticker", "Name", "Weight (%)", "Market Currency"],
        truncate_ragged_lines=True,
    )
    df = df.rename({"Weight (%)": "Weight"})
    return df


def _load_direxion_csv(url: str) -> pl.DataFrame:
    """🏢 Load Direxion CSV format holdings data.

    Args:
        url: URL to the Direxion CSV file

    Returns:
        Polars DataFrame with holdings data
    """
    df = pl.read_csv(url, skip_rows=5, truncate_ragged_lines=True)

    # 🔄 Map possible column name variations to standard names
    rename_map = {}

    # Handle ticker column (could be "Ticker" or "StockTicker")
    if "StockTicker" in df.columns:
        rename_map["StockTicker"] = "Ticker"
    elif "Ticker" not in df.columns:
        raise ValueError("No ticker column found in Direxion CSV")

    # Handle name/description column
    if "SecurityDescription" in df.columns:
        rename_map["SecurityDescription"] = "Name"
    elif "Description" in df.columns:
        rename_map["Description"] = "Name"
    elif "Name" not in df.columns:
        raise ValueError("No name/description column found in Direxion CSV")

    # Handle weight column
    if "HoldingsPercent" in df.columns:
        rename_map["HoldingsPercent"] = "Weight"
    elif "% of Net Assets" in df.columns:
        rename_map["% of Net Assets"] = "Weight"
    elif "Weight" not in df.columns:
        raise ValueError("No weight column found in Direxion CSV")

    if rename_map:
        df = df.rename(rename_map)

    return df


def _filter_and_clean(df: pl.DataFrame, provider: str) -> pl.DataFrame:
    """🧹 Filter and clean holdings data based on provider format.

    Args:
        df: Input DataFrame
        provider: ETF provider name (ssga, ishares, direxion)

    Returns:
        Cleaned DataFrame with just Ticker column (uppercase symbols only)
    """
    if provider in ("ssga", "ishares"):
        currency_col = "Local Currency" if provider == "ssga" else "Market Currency"
        df = df.filter((pl.col(currency_col) == "USD") & (pl.col("Ticker") != "-"))

    # 🎯 Filter out null/empty tickers, get uppercase symbols only, sorted alphabetically
    df = df.filter(
        pl.col("Ticker").is_not_null() & (pl.col("Ticker") != "") & (pl.col("Ticker") != "-")
    ).select("Ticker").sort("Ticker")
    return df


def _save_holdings(df: pl.DataFrame, symbol: str, output_dir: Path | None = None) -> None:
    """💾 Save ticker symbols to a text file.

    Args:
        df: DataFrame containing ticker symbols
        symbol: ETF symbol (used for filename)
        output_dir: Optional output directory (defaults to current directory)
    """
    if output_dir is None:
        output_dir = Path.cwd()

    output_dir.mkdir(parents=True, exist_ok=True)

    # 📝 Save as simple newline-separated list of tickers
    tickers = df["Ticker"].to_list()
    (output_dir / f"{symbol}.txt").write_text("\n".join(tickers) + "\n")


def get_etf_holdings(
    symbol: ETFSymbol,
    output_dir: Path | None = None,
) -> pl.DataFrame:
    """📈 Get ticker symbols for a specific ETF.

    This is the main function for downloading and extracting ticker symbols.
    It handles different provider formats automatically.

    Args:
        symbol: ETF symbol (spy, mdy, spsm, qqq, iwm)
        output_dir: Optional directory to save output file

    Returns:
        DataFrame with just the Ticker column (uppercase symbols, sorted alphabetically)

    Raises:
        ValueError: If the ETF symbol is not recognized
    """
    symbol_lower = symbol.lower()

    if symbol_lower not in ETF_CONFIGS:
        raise ValueError(
            f"Unknown ETF symbol: {symbol}. "
            f"Valid symbols: {', '.join(ETF_CONFIGS.keys())}"
        )

    config = ETF_CONFIGS[symbol_lower]
    provider = config["provider"]
    url = config["url"]

    # 🔄 Load data based on provider
    if provider == "ssga":
        df = _load_ssga_excel(url)
    elif provider == "ishares":
        df = _load_ishares_csv(url)
    elif provider == "direxion":
        df = _load_direxion_csv(url)
    else:
        raise ValueError(f"Unknown provider: {provider}")

    # 🧹 Clean and filter
    df = _filter_and_clean(df, provider)

    # 💾 Save outputs
    _save_holdings(df, symbol_lower, output_dir)

    return df


def get_spy_holdings() -> pl.DataFrame:
    """📊 Get ticker symbols from the SPY ETF.

    Returns:
        DataFrame with SPY ticker symbols
    """
    return get_etf_holdings("spy")


def get_mdy_holdings() -> pl.DataFrame:
    """📊 Get ticker symbols from the MDY ETF.

    Returns:
        DataFrame with MDY ticker symbols
    """
    return get_etf_holdings("mdy")


def get_spsm_holdings() -> pl.DataFrame:
    """📊 Get ticker symbols from the SPSM ETF.

    Returns:
        DataFrame with SPSM ticker symbols
    """
    return get_etf_holdings("spsm")


def get_qqq_holdings() -> pl.DataFrame:
    """📊 Get ticker symbols from the QQQ ETF (using QQQE data as proxy).

    Returns:
        DataFrame with QQQ ticker symbols
    """
    return get_etf_holdings("qqq")


def get_iwm_holdings() -> pl.DataFrame:
    """📊 Get ticker symbols from the IWM ETF.

    Returns:
        DataFrame with IWM ticker symbols
    """
    return get_etf_holdings("iwm")


def main() -> None:
    """🚀 Main function to download all ETF holdings."""
    print("📥 Downloading ETF holdings...")

    for symbol in ETF_CONFIGS.keys():
        print(f"  ⏳ Processing {symbol.upper()}...")
        get_etf_holdings(symbol)  # type: ignore[arg-type]
        print(f"  ✅ {symbol.upper()} complete!")

    print("🎉 All holdings downloaded successfully!")


if __name__ == "__main__":
    main()
