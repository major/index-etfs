"""Test CapForce holdings parsing and FFTY integration through public APIs."""

import json
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from index_etfs import sources
from index_etfs.catalog import ETFConfig
from index_etfs.holdings import get_etf_holdings

FFTY_URL = "https://www.capforceetf.com/ffty/details"


def _fetch_patch(payload: bytes) -> Any:
    """Patch the stdlib opener used by fetch_bytes so tests make no network calls."""
    response = MagicMock()
    response.read.return_value = payload
    response_context = MagicMock()
    response_context.__enter__.return_value = response
    return patch("urllib.request.urlopen", return_value=response_context)


def _row(
    ticker: object,
    holding_type: str = "equity",
    asset_type: str = "common_stock",
) -> dict[str, object]:
    """Build one CapForce holdings row with the fields the page publishes."""
    return {
        "holding_ticker": ticker,
        "holding_type": holding_type,
        "asset_type": asset_type,
        "fund_ticker": "FFTY",
    }


def _page(json_block: str | None) -> bytes:
    """Build a short CapForce-style page with unrelated markup around the holdings JSON."""
    # 🧾 The top-ten table and analytics JSON must never be read as holdings.
    holdings_script = (
        f'<script type="application/json" data-holdings-json>{json_block}</script>'
        if json_block is not None
        else ""
    )
    return (
        "<!DOCTYPE html><html><body>"
        "<table><tr><th>Ticker</th><th>Weight</th></tr>"
        "<tr><td>ZZZ</td><td>9.9%</td></tr></table>"
        '<script type="application/json" id="analytics">'
        '[{"holding_ticker": "NOPE", "holding_type": "equity"}]</script>'
        "<button>Download Holdings</button>"
        f"{holdings_script}</body></html>"
    ).encode()


def test_ffty_uses_complete_capforce_equity_holdings(tmp_path: Path) -> None:
    """Return every distinct equity ticker from the CapForce JSON block, sorted and saved."""
    # Arrange
    tickers = [f"TST{index:02d}" for index in range(48)] + ["BRK.B", "BF.B"]
    rows = [
        _row(ticker, asset_type="" if index == 0 else "common_stock")
        for index, ticker in enumerate(tickers)
    ]
    rows[5]["holding_ticker"] = " TST05 "
    rows += [
        _row("TST01"),
        _row("USD", holding_type="cash", asset_type=""),
        _row(None),
        _row("   "),
    ]
    page = _page(json.dumps(rows))

    # Act
    with _fetch_patch(page) as urlopen:
        holdings = get_etf_holdings("FFTY", output_dir=tmp_path)

    # Assert
    assert urlopen.call_args.args[0].full_url == FFTY_URL
    expected = sorted(tickers)
    assert holdings.schema == {"Ticker": pl.String}
    assert holdings["Ticker"].to_list() == expected
    saved = (tmp_path / "tickers" / "ffty.txt").read_text()
    assert saved == "\n".join(expected) + "\n"


@pytest.mark.parametrize(
    ("page", "message"),
    [
        (_page(None), "missing the holdings JSON block"),
        (_page('[{"holding_ticker": "AAA"'), "not valid JSON"),
        (_page('{"holding_ticker": "AAA"}'), "must be a list"),
        (_page('["AAA"]'), "must be a JSON object"),
        (_page(json.dumps([{"holding_type": "equity"}])), "holding_ticker"),
        (_page(json.dumps([_row(123)])), "holding_ticker"),
        (_page(json.dumps([{"holding_ticker": "AAA"}])), "holding_type"),
        (
            _page(json.dumps([{"holding_ticker": "AAA", "holding_type": None}])),
            "holding_type",
        ),
    ],
)
def test_capforce_rejects_invalid_payload(
    tmp_path: Path, page: bytes, message: str
) -> None:
    """Fail with a descriptive ValueError when the holdings block is malformed."""
    # Act and assert
    with _fetch_patch(page), pytest.raises(ValueError, match=message):
        get_etf_holdings("FFTY", output_dir=tmp_path)


@pytest.mark.parametrize(
    ("equity_count", "message"),
    [
        (0, "FFTY returned zero rows"),
        (44, "FFTY returned 44 rows; expected at least 45"),
    ],
)
def test_ffty_rejects_incomplete_holdings_without_writing(
    tmp_path: Path, equity_count: int, message: str
) -> None:
    """Keep the published ticker file when CapForce returns too few holdings."""
    # Arrange
    output = tmp_path / "tickers" / "ffty.txt"
    output.parent.mkdir(parents=True)
    output.write_text("OLD\n")
    rows = [_row(f"TST{index:02d}") for index in range(equity_count)]

    # Act and assert
    with (
        _fetch_patch(_page(json.dumps(rows))),
        pytest.raises(ValueError, match=message),
    ):
        get_etf_holdings("FFTY", output_dir=tmp_path)
    assert output.read_text() == "OLD\n"


@pytest.mark.parametrize(
    "rows",
    [
        [],
        [_row("USD", holding_type="cash", asset_type="")],
        [_row(None), _row("  ")],
    ],
)
def test_capforce_returns_typed_empty_without_usable_rows(
    rows: list[dict[str, object]],
) -> None:
    """Return a string-typed empty frame when no equity row has a usable ticker."""
    # Arrange
    config = ETFConfig(url=FFTY_URL, provider="capforce", expected_count=50)

    # Act
    with _fetch_patch(_page(json.dumps(rows))):
        holdings = sources.load_holdings(config)

    # Assert
    assert holdings.columns == ["Ticker"]
    assert holdings.schema == {"Ticker": pl.String}
    assert holdings.is_empty()
