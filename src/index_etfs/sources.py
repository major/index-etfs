"""Download and clean holdings data from upstream providers."""

import http.cookiejar
import io
import json
import urllib.request

import polars as pl

from .amplify import load_amplify_json
from .ark import load_ark_csv
from .catalog import ETFConfig, Provider
from .firsttrust import load_firsttrust_html
from .globalx import load_globalx_csv
from .http import fetch_bytes
from .invesco import load_invesco_json
from .specialty_csv import load_flexshares_csv, load_tema_csv
from .vanguard import load_vanguard_json

FIREFOX_HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:140.0) Gecko/20100101 Firefox/140.0",
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.5",
}


def read_url(url: str) -> io.BytesIO:
    """Read a URL with browser-ish headers."""
    request = urllib.request.Request(url, headers=FIREFOX_HEADERS)
    return io.BytesIO(fetch_bytes(request))


def load_ssga_excel(url: str) -> pl.DataFrame:
    """Load SSGA Excel format holdings data."""
    df_dict = pl.read_excel(read_url(url), sheet_id=0, read_options={"header_row": 4})
    return next(iter(df_dict.values())) if isinstance(df_dict, dict) else df_dict


def load_vaneck_excel(url: str) -> pl.DataFrame:
    """Load VanEck workbook stock holdings with trimmed ticker symbols."""
    cookie_jar = http.cookiejar.CookieJar()
    opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cookie_jar))
    request = urllib.request.Request(url, headers=FIREFOX_HEADERS)
    workbook = io.BytesIO(fetch_bytes(request, opener=opener))
    df_dict = pl.read_excel(workbook, sheet_id=0, read_options={"header_row": 2})
    df = next(iter(df_dict.values())) if isinstance(df_dict, dict) else df_dict

    required_columns = {"Ticker", "Asset Class"}
    missing_columns = sorted(required_columns - set(df.columns))
    if missing_columns:
        missing = ", ".join(missing_columns)
        raise ValueError(f"VanEck workbook is missing required columns: {missing}")

    return df.filter(pl.col("Asset Class") == "Stock").with_columns(
        pl.col("Ticker").cast(pl.String, strict=False).str.strip_chars()
    )


def load_ishares_csv(url: str) -> pl.DataFrame:
    """Load iShares CSV format holdings data."""
    df = pl.read_csv(
        read_url(url),
        skip_rows=9,
        infer_schema=False,
        truncate_ragged_lines=True,
    )
    required_columns = {"Ticker", "Name", "Market Currency"}
    missing_columns = sorted(required_columns - set(df.columns))
    if missing_columns:
        missing = ", ".join(missing_columns)
        raise ValueError(f"iShares CSV is missing required columns: {missing}")

    weight_column = "Weight (%)" if "Weight (%)" in df.columns else "Market Weight"
    if weight_column not in df.columns:
        raise ValueError(
            "iShares CSV is missing required weight column: Weight (%) or Market Weight"
        )

    return df.select(
        "Ticker",
        "Name",
        pl.col(weight_column).cast(pl.Float64, strict=False).alias("Weight"),
        "Market Currency",
    )


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
        "vaneck": load_vaneck_excel,
        "ishares": load_ishares_csv,
        "nasdaq": load_nasdaq_json,
        "invesco": load_invesco_json,
        "ark": load_ark_csv,
        "globalx": load_globalx_csv,
        "vanguard": load_vanguard_json,
        "firsttrust": load_firsttrust_html,
        "flexshares": load_flexshares_csv,
        "tema": load_tema_csv,
        "amplify": load_amplify_json,
    }
    try:
        loader = loaders[config.provider]
    except KeyError as exc:
        raise ValueError(f"Unknown provider: {config.provider}") from exc
    return loader(config.url)


def filter_and_clean(df: pl.DataFrame, provider: Provider) -> pl.DataFrame:
    """Filter provider holdings down to sorted, unique ticker symbols."""
    if len(df) == 0:
        return df.select("Ticker") if "Ticker" in df.columns else pl.DataFrame({"Ticker": []})

    if "Ticker" not in df.columns:
        raise ValueError("Holdings data is missing the required 'Ticker' column")
    if df.schema["Ticker"] != pl.String:
        raise ValueError(f"Ticker column must have string dtype, got {df.schema['Ticker']}")

    if provider == "ssga":
        if "Name" in df.columns:
            # SSGA labels futures with a trailing month code and two-digit delivery year.
            futures_contract = (
                pl.col("Name")
                .cast(pl.String, strict=False)
                .str.strip_chars()
                .str.to_uppercase()
                .str.contains(r"(?:JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)\d{2}$")
                .fill_null(False)
            )
            df = df.filter(~futures_contract)
        df = df.filter((pl.col("Local Currency") == "USD") & (pl.col("Ticker") != "-"))
    elif provider == "ishares":
        df = df.filter((pl.col("Market Currency") == "USD") & (pl.col("Ticker") != "-"))
    elif provider == "vaneck":
        ticker = pl.col("Ticker")
        cash_markers = ["--", "-USD CASH-"]
        foreign_qualified = (
            ticker.is_not_null()
            & ticker.str.contains(r"\s")
            & ~ticker.is_in(cash_markers)
        )
        excluded_count = df.filter(foreign_qualified).height
        if excluded_count > 0:
            print(
                f"Warning: excluded {excluded_count} VanEck stock rows outside the "
                "US-listed subset."
            )
        df = df.filter(~foreign_qualified & ~ticker.is_in(cash_markers))

    cleaned = df.filter(
        pl.col("Ticker").is_not_null()
        & (pl.col("Ticker") != "")
        & (pl.col("Ticker") != "-")
        & ~pl.col("Ticker").str.contains("_", literal=True)
        & ~pl.col("Ticker").str.contains(" ", literal=True)
    ).select("Ticker")
    duplicate_count = len(cleaned) - cleaned["Ticker"].n_unique()
    if duplicate_count > 0:
        print(f"Warning: collapsed {duplicate_count} repeated ticker rows for provider {provider}.")

    return cleaned.unique().sort("Ticker")
