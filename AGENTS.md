# AGENTS.md - index-etfs

> Keep this short. Update it when commands, layout, providers, or generated outputs change.

## Project

Downloads public ETF/index holdings and writes ticker lists for humans and TradingView.

- Python 3.15, `uv`, Polars
- CLI: `uv run get-holdings`
- Main code: `src/index_etfs/holdings.py`
- Tests: `tests/test_holdings.py`, `tests/test_catalog.py`, `tests/test_provider_contracts.py`, `tests/test_vaneck.py`; small provider fixtures in `tests/fixtures/`.

## Progressive discovery

Do not read the whole repo first. Start here:

1. **User-facing shape**: `README.md` for outputs and install/run commands.
2. **Config/flow**:
   - `src/index_etfs/catalog.py`: `ETF_CONFIGS`, expected counts, validation.
   - `src/index_etfs/sources.py`: provider loaders and cleanup.
   - `src/index_etfs/outputs.py`: ticker/watchlist/metadata writers.
   - `src/index_etfs/holdings.py`: public API and CLI orchestration.
3. **Tests for behavior**: `tests/test_holdings.py`.
4. **CI/update behavior** only if needed: `.github/workflows/ci.yml`, `.github/workflows/data-refresh.yml`.
5. **Generated data** only when changing data output: `tickers/*.txt`, `watchlists/*.txt`, and `metadata/*.json`.

## Layout

```text
src/index_etfs/catalog.py    ETF config and count validation
src/index_etfs/sources.py    downloader, provider parsers, cleaners
src/index_etfs/http.py       bounded retries for safe, read-only HTTP requests
src/index_etfs/changes.py    membership diagnostics against published ticker snapshots
src/index_etfs/outputs.py    ticker, watchlist, and metadata writers
src/index_etfs/holdings.py   public API and CLI entry point
tests/test_holdings.py       mocked unit tests; no network required
tickers/*.txt                plain ticker outputs, one ticker per line
watchlists/*.txt             TradingView outputs, ###Section headers + EXCHANGE:TICKER lines
metadata/*.json              latest generated counts/timestamps
.github/workflows/ci.yml     lint + tests on PR/push
.github/workflows/data-refresh.yml   scheduled/manual data refresh and commit
```

## Commands

```bash
uv sync --locked --all-extras --dev
uv run ruff check .
uv run pytest
uv run get-holdings          # networked; rewrites ticker/watchlist files
```

## Conventions

- Keep the full refresh small: fetch -> parse -> filter unique tickers -> validate -> resolve union -> stage -> publish metadata last.
- Prefer `pathlib.Path`, stdlib HTTP, and Polars already used here.
- Do not add providers or dependencies unless the requested ETF needs them.
- Tests should mock network/provider reads; avoid live HTTP in unit tests.
- VanEck workbooks use a cookie-enabled download, a third-row header, and an `Asset Class` stock filter. GDX outputs only the US-listed subset, documented by `scope_note` in metadata.
- `tests/test_membership.py` covers change diagnostics; `tests/test_scale.py` runs 15 overlapping synthetic configurations offline.
- Output files are sorted and newline-terminated.
- Full refresh staging protects existing files from preparation failures, not crashes or replacement failures. Tests in `tests/test_refresh.py` cover this boundary.
- TradingView files use section headers and exchange-prefixed symbols when the scanner can resolve them.

## Git hygiene

- Code-only change: update tests, do not refresh generated ticker files unless behavior affects them.
- Data refresh: commit `tickers/*.txt` with matching `watchlists/*.txt` and `metadata/*.json`.
- Do not commit `.venv/`, caches, coverage files, or temporary downloads.
