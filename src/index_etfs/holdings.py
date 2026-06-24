"""📊 Extract ticker symbols from various index ETFs.

This module downloads ETF/index holdings from public providers (SSGA, iShares, Nasdaq)
and extracts just the ticker symbols (e.g., AAPL, MSFT, GOOGL).
"""

import io
import json
import math
import urllib.request
from datetime import UTC, datetime
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
        "url": "https://api.nasdaq.com/api/quote/list-type/nasdaq100",
        "provider": "nasdaq",
    },
    "iwm": {
        "url": "https://www.blackrock.com/varnish-api/blk-one01-product-data/product-data/api/v1/get-fund-document?appType=PRODUCT_PAGE&appSubType=ISHARES&targetSite=us-ishares&locale=en_US&portfolioId=239710&userType=individual&component=holdings",
        "provider": "ishares",
    },
}

ETFSymbol = Literal["spy", "mdy", "spsm", "qqq", "iwm"]

FIREFOX_HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:140.0) Gecko/20100101 Firefox/140.0",
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.5",
}

TRADINGVIEW_SCAN_URL = "https://scanner.tradingview.com/america/scan"
WATCHLIST_NAMES = {
    "spy": "sp500",
    "mdy": "sp400",
    "spsm": "sp600",
    "qqq": "nasdaq100",
    "iwm": "russell2000",
}
EXPECTED_COUNTS = {
    "spy": 503,
    "mdy": 400,
    "spsm": 600,
    "qqq": 100,
    "iwm": 2000,
}
MIN_EXPECTED_RATIO = 0.9


def _validate_count(symbol: str, count: int) -> None:
    """Fail closed on empty or suspiciously small source data."""
    if count == 0:
        raise ValueError(f"{symbol.upper()} returned zero rows")

    expected = EXPECTED_COUNTS[symbol]
    minimum = math.ceil(expected * MIN_EXPECTED_RATIO)
    if count < minimum:
        raise ValueError(
            f"{symbol.upper()} returned {count} rows; expected at least {minimum}"
        )


def _write_metadata(results: dict[str, int], output_dir: Path | None = None) -> None:
    """Write latest generated counts and source URLs."""
    if output_dir is None:
        output_dir = Path.cwd()

    metadata = {
        "generated_at": datetime.now(UTC)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z"),
        "watchlists": {
            WATCHLIST_NAMES[symbol]: {
                "count": count,
                "expected_count": EXPECTED_COUNTS[symbol],
                "minimum_count": math.ceil(EXPECTED_COUNTS[symbol] * MIN_EXPECTED_RATIO),
                "source": symbol,
                "source_url": ETF_CONFIGS[symbol]["url"],
            }
            for symbol, count in results.items()
        },
    }

    metadata_dir = output_dir / "metadata"
    metadata_dir.mkdir(parents=True, exist_ok=True)
    (metadata_dir / "latest.json").write_text(json.dumps(metadata, indent=2) + "\n")


def _read_url(url: str) -> io.BytesIO:
    """Read a URL with browser-ish headers."""
    request = urllib.request.Request(url, headers=FIREFOX_HEADERS)
    with urllib.request.urlopen(request, timeout=30) as response:
        return io.BytesIO(response.read())


def _tradingview_prefixes(tickers: list[str], chunk_size: int = 200) -> dict[str, str]:
    """Map tickers to TradingView exchange-prefixed symbols."""
    prefixes = {}
    headers = FIREFOX_HEADERS | {"Content-Type": "application/json"}

    for start in range(0, len(tickers), chunk_size):
        chunk = tickers[start : start + chunk_size]
        payload = {
            "filter": [{"left": "name", "operation": "in_range", "right": chunk}],
            "columns": ["name"],
            "markets": ["america"],
        }
        request = urllib.request.Request(
            TRADINGVIEW_SCAN_URL,
            data=json.dumps(payload).encode(),
            headers=headers,
        )
        with urllib.request.urlopen(request, timeout=30) as response:
            rows = json.load(response).get("data", [])

        for row in rows:
            ticker = row.get("d", [None])[0]
            symbol = row.get("s", "")
            if ticker in chunk and ":" in symbol:
                prefixes.setdefault(ticker, symbol)

    return prefixes


def _load_ssga_excel(url: str) -> pl.DataFrame:
    """🏢 Load SSGA Excel format holdings data.

    Args:
        url: URL to the SSGA Excel file

    Returns:
        Polars DataFrame with holdings data
    """
    df_dict = pl.read_excel(_read_url(url), sheet_id=0, read_options={"header_row": 4})

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
        _read_url(url),
        skip_rows=9,
        columns=["Ticker", "Name", "Weight (%)", "Market Currency"],
        truncate_ragged_lines=True,
    )
    df = df.rename({"Weight (%)": "Weight"})
    return df


def _load_nasdaq_json(url: str) -> pl.DataFrame:
    """🏢 Load Nasdaq-100 symbols from Nasdaq's public JSON endpoint."""
    rows = json.load(_read_url(url)).get("data", {}).get("data", {}).get("rows")
    if not rows:
        raise ValueError("Nasdaq response did not contain any Nasdaq-100 rows")

    tickers = [row["symbol"].strip().replace("/", ".") for row in rows if row.get("symbol")]
    if not tickers:
        raise ValueError("Nasdaq response did not contain any symbols")

    return pl.DataFrame({"Ticker": tickers})


def _filter_and_clean(df: pl.DataFrame, provider: str) -> pl.DataFrame:
    """🧹 Filter and clean holdings data based on provider format.

    Args:
        df: Input DataFrame
        provider: ETF provider name (ssga, ishares, nasdaq)

    Returns:
        Cleaned DataFrame with just Ticker column (uppercase symbols only)
    """
    # 🏃 Handle empty DataFrame early - just select Ticker column
    if len(df) == 0:
        return df.select("Ticker") if "Ticker" in df.columns else pl.DataFrame({"Ticker": []})

    if provider in ("ssga", "ishares"):
        currency_col = "Local Currency" if provider == "ssga" else "Market Currency"
        df = df.filter((pl.col(currency_col) == "USD") & (pl.col("Ticker") != "-"))

    # 🎯 Filter out null/empty tickers, special placeholders, get uppercase symbols only, sorted alphabetically
    df = df.filter(
        pl.col("Ticker").is_not_null()
        & (pl.col("Ticker") != "")
        & (pl.col("Ticker") != "-")
        & ~pl.col("Ticker").str.contains("_", literal=True)  # Exclude CASH_USD and similar placeholders
        & ~pl.col("Ticker").str.contains(" ", literal=True)  # Exclude warrants like "BBBY WS"
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
    tickers_dir = output_dir / "tickers"
    tickers_dir.mkdir(exist_ok=True)
    (tickers_dir / f"{symbol}.txt").write_text("\n".join(tickers) + "\n")

    watchlist_name = WATCHLIST_NAMES.get(symbol)
    if watchlist_name:
        watchlist_dir = output_dir / "watchlists"
        watchlist_dir.mkdir(exist_ok=True)
        prefixes = _tradingview_prefixes(tickers)
        missing = [ticker for ticker in tickers if ticker not in prefixes]
        if missing:
            print(f"⚠️  No TradingView prefix for {', '.join(missing)}")
        lines = [prefixes.get(ticker, ticker) for ticker in tickers]
        (watchlist_dir / f"{watchlist_name}.txt").write_text("\n".join(lines) + "\n")


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
    elif provider == "nasdaq":
        df = _load_nasdaq_json(url)
    else:
        raise ValueError(f"Unknown provider: {provider}")

    # 🧹 Clean and filter
    df = _filter_and_clean(df, provider)
    _validate_count(symbol_lower, len(df))

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
    """📊 Get ticker symbols from the Nasdaq-100.

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
    output_dir = Path.cwd()
    results = {}

    for symbol in ETF_CONFIGS.keys():
        print(f"  ⏳ Processing {symbol.upper()}...")
        df = get_etf_holdings(symbol, output_dir)  # type: ignore[arg-type]
        count = len(df)
        results[symbol] = count
        print(f"  ✅ {symbol.upper()} complete! ({count} rows)")

    _write_metadata(results, output_dir)
    print("🎉 All holdings downloaded successfully!")


if __name__ == "__main__":
    main()
