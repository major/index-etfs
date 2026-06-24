# TradingView watchlists plan

## Goal

Extend this repo so it still publishes plain ETF ticker files, and also publishes TradingView-importable index watchlists:

```text
watchlists/
  sp500.txt
  nasdaq100.txt
  sp400.txt
  sp600.txt
  russell2000.txt
metadata/
  latest.json
```

TradingView files use one sorted `EXCHANGE:TICKER` per line, with no headers.

## Current repo fit

- Keep the existing `uv` project, `index_etfs.holdings` module, and `get-holdings` entry point.
- Do not add `requirements.txt` or a separate `scripts/` runner; that duplicates existing packaging.
- Reuse existing loaders for SPY, MDY, SPSM, and IWM.
- Replace the current QQQE proxy for Nasdaq-100 with Nasdaq's public Nasdaq-100 JSON API.
- Keep root `spy.txt`, `mdy.txt`, `spsm.txt`, `qqq.txt`, and `iwm.txt` for backward compatibility unless we intentionally remove them later.

## Success criteria

- `uv run get-holdings` writes the existing root ticker files.
- `uv run get-holdings` also writes all five `watchlists/*.txt` files and `metadata/latest.json`.
- Watchlist rows are sorted and formatted as `EXCHANGE:TICKER`.
- Empty sources fail the run.
- Counts below 90% of expected fail the run.
- Tests pass with `uv run pytest`.
- GitHub Actions commits changed root ticker files, `watchlists/*.txt`, and `metadata/latest.json`.

## Work plan

### 0. Fix the existing safety net

- Update tests so `_save_holdings()` expectations match current `.txt` output, or restore CSV/Markdown only if we truly want them.
- Add small tests around output text sorting and trailing newline.

### 1. Model indices separately from ETF source symbols

Add an index config beside `ETF_CONFIGS`:

| Output | Source | Existing loader? | Min count |
| --- | --- | --- | --- |
| `sp500` | SPY / SSGA XLSX | yes | 453 |
| `sp400` | MDY / SSGA XLSX | yes | 360 |
| `sp600` | SPSM / SSGA XLSX | yes | 540 |
| `russell2000` | IWM / iShares CSV | yes | 1800 |
| `nasdaq100` | Nasdaq JSON API | no | 90 |

### 2. Add Nasdaq-100 loader

- Fetch `https://api.nasdaq.com/api/quote/list-type/nasdaq100`.
- Parse tickers into the same one-column `Ticker` DataFrame used by the rest of the code.
- Preserve the existing `qqq.txt` output only if useful; otherwise document the change.

### 3. Add exchange-prefix mapping

- First use provider exchange columns if present.
- For anything still missing, call TradingView scanner in chunks and read the returned `name` like `NASDAQ:AAPL`.
- If a few lookups fail, warn and write the bare ticker; do not fail solely for that.
- Preserve dotted share-class tickers (`BRK.B`, `BF.B`, `MOG.A`).

### 4. Write watchlists and metadata

- Add `_save_watchlist(rows, path)` for `EXCHANGE:TICKER` lines.
- Add `_save_metadata()` with generated timestamp, source names/URLs, source row counts, watchlist row counts, and unresolved exchange prefixes.
- Keep this in the existing module unless it gets genuinely hard to read.

### 5. Validate before writing/committing

- Fail on zero rows for any required source.
- Fail below min counts from the table above.
- Warn, do not fail, for small ETF/index count drift.

### 6. Update automation and docs

- Update `.github/workflows/main.yml` to `git add *.txt watchlists/*.txt metadata/*.json README.md`.
- Update README with raw download links and TradingView import steps.
- Mention that root files are plain tickers and `watchlists/` files are TradingView imports.

## Open questions

1. Do we want to keep publishing `qqq.txt`, or replace it with `nasdaq100.txt` only?
2. Should root `*.txt` files remain forever for compatibility, or be deprecated after watchlists land?
3. Does TradingView import accept every generated dotted share-class symbol in practice? Test one import manually.
