"""Parse equity holdings from Vanguard's public additional fund data feed."""

import json
import re
import urllib.request

import polars as pl

from .http import fetch_bytes

USER_AGENT = "Mozilla/5.0 (compatible; index-etfs/1.0)"
US_TICKER_PATTERN = r"[A-Z][A-Z0-9]*(?:[.-][A-Z])?"
REQUIRED_HOLDING_FIELDS = ("securityMainType",)


def load_vanguard_json(url: str) -> pl.DataFrame:
    """Fetch Vanguard holdings and return valid US-style equity tickers."""
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    payload = json.loads(fetch_bytes(request))
    if not isinstance(payload, dict) or "holdingDetails" not in payload:
        raise ValueError("Vanguard response is missing required 'holdingDetails'")

    holding_details = payload["holdingDetails"]
    if not isinstance(holding_details, dict) or "equityHoldings" not in holding_details:
        raise ValueError("Vanguard response is missing required 'equityHoldings'")

    holdings = holding_details["equityHoldings"]
    if not isinstance(holdings, list):
        raise TypeError("Vanguard response 'equityHoldings' must be a list")

    tickers = []
    for index, holding in enumerate(holdings):
        if not isinstance(holding, dict):
            raise TypeError(f"Vanguard holding at index {index} must be an object")

        missing_fields = [field for field in REQUIRED_HOLDING_FIELDS if field not in holding]
        if missing_fields:
            missing = ", ".join(missing_fields)
            raise ValueError(
                f"Vanguard holding at index {index} is missing required fields: {missing}"
            )

        ticker = holding.get("ticker")
        if holding["securityMainType"] != "EQ" or not isinstance(ticker, str):
            continue

        normalized_ticker = ticker.strip().upper().replace("/", ".")
        if len(normalized_ticker) <= 10 and re.fullmatch(US_TICKER_PATTERN, normalized_ticker):
            tickers.append(normalized_ticker)

    return pl.DataFrame({"Ticker": pl.Series("Ticker", tickers, dtype=pl.String)})
