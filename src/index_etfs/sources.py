"""Download and clean holdings data from upstream providers."""

import io
import json
import urllib.request

import polars as pl

from .catalog import ETFConfig, Provider

FIREFOX_HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:140.0) Gecko/20100101 Firefox/140.0",
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.5",
}


def read_url(url: str) -> io.BytesIO:
    """Read a URL with browser-ish headers."""
    request = urllib.request.Request(url, headers=FIREFOX_HEADERS)
    with urllib.request.urlopen(request, timeout=30) as response:
        return io.BytesIO(response.read())


def load_ssga_excel(url: str) -> pl.DataFrame:
    """Load SSGA Excel format holdings data."""
    df_dict = pl.read_excel(read_url(url), sheet_id=0, read_options={"header_row": 4})
    return list(df_dict.values())[0] if isinstance(df_dict, dict) else df_dict


def load_ishares_csv(url: str) -> pl.DataFrame:
    """Load iShares CSV format holdings data."""
    df = pl.read_csv(
        read_url(url),
        skip_rows=9,
        columns=["Ticker", "Name", "Weight (%)", "Market Currency"],
        truncate_ragged_lines=True,
    )
    return df.rename({"Weight (%)": "Weight"})


def load_nasdaq_json(url: str) -> pl.DataFrame:
    """Load Nasdaq-100 symbols from Nasdaq's public JSON endpoint."""
    rows = json.load(read_url(url)).get("data", {}).get("data", {}).get("rows")
    if not rows:
        raise ValueError("Nasdaq response did not contain any Nasdaq-100 rows")

    tickers = [row["symbol"].strip().replace("/", ".") for row in rows if row.get("symbol")]
    if not tickers:
        raise ValueError("Nasdaq response did not contain any symbols")

    return pl.DataFrame({"Ticker": tickers})


def load_holdings(config: ETFConfig) -> pl.DataFrame:
    """Load holdings from the configured provider."""
    loaders = {
        "ssga": load_ssga_excel,
        "ishares": load_ishares_csv,
        "nasdaq": load_nasdaq_json,
    }
    try:
        loader = loaders[config.provider]
    except KeyError as exc:
        raise ValueError(f"Unknown provider: {config.provider}") from exc
    return loader(config.url)


def filter_and_clean(df: pl.DataFrame, provider: Provider) -> pl.DataFrame:
    """Filter provider holdings down to sorted ticker symbols."""
    if len(df) == 0:
        return df.select("Ticker") if "Ticker" in df.columns else pl.DataFrame({"Ticker": []})

    if provider in ("ssga", "ishares"):
        currency_col = "Local Currency" if provider == "ssga" else "Market Currency"
        df = df.filter((pl.col(currency_col) == "USD") & (pl.col("Ticker") != "-"))

    return df.filter(
        pl.col("Ticker").is_not_null()
        & (pl.col("Ticker") != "")
        & (pl.col("Ticker") != "-")
        & ~pl.col("Ticker").str.contains("_", literal=True)
        & ~pl.col("Ticker").str.contains(" ", literal=True)
    ).select("Ticker").sort("Ticker")
