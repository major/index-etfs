"""Parse holdings from Invesco's public JSON feed."""

import json
import re
import urllib.request

import polars as pl

from .http import fetch_bytes

EQUITY_SECURITY_TYPES = frozenset({"COM", "ADR", "DR", "REIT"})
US_TICKER_PATTERN = r"[A-Z][A-Z0-9]*(?:[.-][A-Z])?"
USER_AGENT = "Mozilla/5.0 (compatible; index-etfs/1.0)"
REQUIRED_HOLDING_FIELDS = ("ticker", "securityTypeCode", "currency")


def load_invesco_json(url: str) -> pl.DataFrame:
    """Fetch Invesco holdings and return valid US-style equity tickers."""
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    payload = json.loads(fetch_bytes(request))
    if not isinstance(payload, dict) or "holdings" not in payload:
        raise ValueError("Invesco response is missing required 'holdings'")

    holdings = payload["holdings"]
    if not isinstance(holdings, list):
        raise TypeError("Invesco response 'holdings' must be a list")

    tickers = []
    for index, holding in enumerate(holdings):
        if not isinstance(holding, dict):
            raise TypeError(f"Invesco holding at index {index} must be an object")

        missing_fields = [field for field in REQUIRED_HOLDING_FIELDS if field not in holding]
        if missing_fields:
            missing = ", ".join(missing_fields)
            raise ValueError(f"Invesco holding at index {index} is missing required fields: {missing}")

        ticker = holding["ticker"]
        security_type = holding["securityTypeCode"]
        currency = holding["currency"]
        if not all(isinstance(value, str) and value for value in (ticker, security_type, currency)):
            continue
        if security_type not in EQUITY_SECURITY_TYPES:
            continue

        normalized_ticker = ticker.strip().replace("/", ".")
        if len(normalized_ticker) <= 10 and re.fullmatch(US_TICKER_PATTERN, normalized_ticker):
            tickers.append(normalized_ticker)

    return pl.DataFrame({"Ticker": pl.Series("Ticker", tickers, dtype=pl.String)})
