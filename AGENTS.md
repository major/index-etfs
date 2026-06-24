# AGENTS.md - index-etfs

> Keep this short. Update it when commands, layout, providers, or generated outputs change.

## Project

Downloads public ETF/index holdings and writes ticker lists for humans and TradingView.

- Python 3.14, `uv`, Polars
- CLI: `uv run get-holdings`
- Main code: `src/index_etfs/holdings.py`
- Tests: `tests/test_holdings.py`

## Progressive discovery

Do not read the whole repo first. Start here:

1. **User-facing shape**: `README.md` for outputs and install/run commands.
2. **Config/flow**: `src/index_etfs/holdings.py`.
   - `ETF_CONFIGS`: source URLs/providers.
   - Provider loaders: `_load_ssga_excel`, `_load_ishares_csv`, `_load_nasdaq_json`.
   - Cleanup: `_filter_and_clean`.
   - Output: `_save_holdings`, `_tradingview_symbols`, `_watchlist_lines`.
3. **Tests for behavior**: `tests/test_holdings.py`.
4. **CI/update behavior** only if needed: `.github/workflows/ci.yml`, `.github/workflows/main.yml`.
5. **Generated data** only when changing data output: `tickers/*.txt`, `watchlists/*.txt`, and `metadata/*.json`.

## Layout

```text
src/index_etfs/holdings.py   downloader, cleaners, writers, CLI entry point
tests/test_holdings.py       mocked unit tests; no network required
tickers/*.txt                plain ticker outputs, one ticker per line
watchlists/*.txt             TradingView outputs, ###Section headers + EXCHANGE:TICKER lines
metadata/*.json              latest generated counts/timestamps
.github/workflows/ci.yml     lint + tests on PR/push
.github/workflows/main.yml   scheduled/manual data refresh and commit
```

## Commands

```bash
uv sync --locked --all-extras --dev
uv run ruff check .
uv run pytest
uv run get-holdings          # networked; rewrites ticker/watchlist files
```

## Conventions

- Keep the pipeline small: fetch -> parse -> filter -> write.
- Prefer `pathlib.Path`, stdlib HTTP, and Polars already used here.
- Do not add providers or dependencies unless the requested ETF needs them.
- Tests should mock network/provider reads; avoid live HTTP in unit tests.
- Output files are sorted and newline-terminated.
- TradingView files use section headers and exchange-prefixed symbols when the scanner can resolve them.

## Git hygiene

- Code-only change: update tests, do not refresh generated ticker files unless behavior affects them.
- Data refresh: commit `tickers/*.txt` with matching `watchlists/*.txt` and `metadata/*.json`.
- Do not commit `.venv/`, caches, coverage files, or temporary downloads.
