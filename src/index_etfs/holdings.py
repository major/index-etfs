"""📊 Get the holdings of various index ETFs.

This module provides functions to download and process ETF holdings data
from various providers (SSGA, iShares, Direxion).
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
    df = pl.read_csv(url, truncate_ragged_lines=True)
    df = df.rename(
        {"Ticker": "Ticker", "Description": "Name", "% of Net Assets": "Weight"}
    )
    return df


def _filter_and_clean(df: pl.DataFrame, provider: str) -> pl.DataFrame:
    """🧹 Filter and clean holdings data based on provider format.

    Args:
        df: Input DataFrame
        provider: ETF provider name (ssga, ishares, direxion)

    Returns:
        Cleaned and filtered DataFrame with Ticker, Name, Weight columns
    """
    if provider in ("ssga", "ishares"):
        currency_col = "Local Currency" if provider == "ssga" else "Market Currency"
        df = df.filter((pl.col(currency_col) == "USD") & (pl.col("Ticker") != "-"))

    # 📋 Select and sort common columns
    df = df.select(["Ticker", "Name", "Weight"]).sort("Weight", descending=True)
    return df


def _save_holdings(df: pl.DataFrame, symbol: str, output_dir: Path | None = None) -> None:
    """💾 Save holdings data to CSV and Markdown files.

    Args:
        df: DataFrame containing holdings
        symbol: ETF symbol (used for filename)
        output_dir: Optional output directory (defaults to current directory)
    """
    if output_dir is None:
        output_dir = Path.cwd()

    output_dir.mkdir(parents=True, exist_ok=True)

    # 📊 Add ranking and save
    df_with_rank = df.with_row_index("Rank")
    df_with_rank.write_csv(output_dir / f"{symbol}.csv")
    (output_dir / f"{symbol}.md").write_text(df.to_pandas().to_markdown())


def get_etf_holdings(
    symbol: ETFSymbol,
    output_dir: Path | None = None,
) -> pl.DataFrame:
    """📈 Get holdings for a specific ETF.

    This is the main function for downloading and processing ETF holdings.
    It handles different provider formats automatically.

    Args:
        symbol: ETF symbol (spy, mdy, spsm, qqq, iwm)
        output_dir: Optional directory to save output files

    Returns:
        DataFrame with Ticker, Name, and Weight columns sorted by weight

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
    """📊 Get the holdings of the SPY ETF.

    Returns:
        DataFrame with SPY holdings
    """
    return get_etf_holdings("spy")


def get_mdy_holdings() -> pl.DataFrame:
    """📊 Get the holdings of the MDY ETF.

    Returns:
        DataFrame with MDY holdings
    """
    return get_etf_holdings("mdy")


def get_spsm_holdings() -> pl.DataFrame:
    """📊 Get the holdings of the SPSM ETF.

    Returns:
        DataFrame with SPSM holdings
    """
    return get_etf_holdings("spsm")


def get_qqq_holdings() -> pl.DataFrame:
    """📊 Get the holdings of the QQQ ETF (using QQQE data as proxy).

    Returns:
        DataFrame with QQQ holdings
    """
    return get_etf_holdings("qqq")


def get_iwm_holdings() -> pl.DataFrame:
    """📊 Get the holdings of the IWM ETF.

    Returns:
        DataFrame with IWM holdings
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
