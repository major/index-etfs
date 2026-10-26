"""Load equity and listed fund holdings from Amplify's Firestore feed."""

import json
import re
import urllib.request
from typing import Any

import polars as pl

from .http import fetch_bytes

AMPLIFY_USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64; rv:140.0) Gecko/20100101 Firefox/140.0"
)
AMPLIFY_TICKER_PATTERN = re.compile(r"[A-Z][A-Z0-9]*(?:[.-][A-Z])?")
AMPLIFY_REQUIRED_FIELDS = ("StockTicker", "SecurityName", "holding_type")


def _required_string(fields: dict[str, Any], name: str) -> str:
    """Return a required Firestore stringValue or fail closed on malformed data."""
    value = fields.get(name)
    if not isinstance(value, dict) or "stringValue" not in value:
        raise ValueError(f"Amplify holding is missing required string field: {name}")
    if not isinstance(value["stringValue"], str):
        raise TypeError(f"Amplify holding required field is not a string: {name}")
    return value["stringValue"]


def load_amplify_json(url: str) -> pl.DataFrame:
    """Fetch Amplify holdings and return valid listed equity and fund tickers."""
    request = urllib.request.Request(url, headers={"User-Agent": AMPLIFY_USER_AGENT})
    payload = json.loads(fetch_bytes(request).decode("utf-8"))

    documents = payload.get("documents") if isinstance(payload, dict) else None
    if not isinstance(documents, list) or not documents:
        raise ValueError("Amplify response is missing its required documents")

    document = documents[0]
    fields = document.get("fields") if isinstance(document, dict) else None
    holdings = fields.get("holdings") if isinstance(fields, dict) else None
    if not isinstance(holdings, dict) or "arrayValue" not in holdings:
        raise ValueError("Amplify document is missing its required holdings array")
    array_value = holdings["arrayValue"]
    if not isinstance(array_value, dict):
        raise TypeError("Amplify document holdings array has an invalid type")

    # Firestore can represent an explicit empty array without a values member.
    records = array_value.get("values", [])
    if not isinstance(records, list):
        raise TypeError("Amplify holdings array has malformed values")

    tickers = []
    for record in records:
        if not isinstance(record, dict) or "mapValue" not in record:
            raise ValueError("Amplify holding is missing its required fields")
        map_value = record["mapValue"]
        if not isinstance(map_value, dict):
            raise TypeError("Amplify holding map has an invalid type")
        if "fields" not in map_value:
            raise ValueError("Amplify holding is missing its required fields")
        holding_fields = map_value["fields"]
        if not isinstance(holding_fields, dict):
            raise TypeError("Amplify holding fields have an invalid type")

        ticker, _security_name, holding_type = (
            _required_string(holding_fields, field) for field in AMPLIFY_REQUIRED_FIELDS
        )
        if holding_type.strip().casefold() != "equity":
            continue

        normalized_ticker = ticker.strip().upper().replace("/", ".")
        if (
            len(normalized_ticker) <= 10
            and AMPLIFY_TICKER_PATTERN.fullmatch(normalized_ticker) is not None
        ):
            tickers.append(normalized_ticker)

    return pl.DataFrame({"Ticker": tickers}, schema={"Ticker": pl.String})
