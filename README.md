# 📈 Index ETF Ticker Symbols

<p align="center">
  <strong>Fresh ticker lists for major US indexes — ready for TradingView, scripts, and quick market scans.</strong>
</p>

<p align="center">
  <a href="https://github.com/major/index-etfs/actions/workflows/ci.yml"><img alt="CI" src="https://github.com/major/index-etfs/actions/workflows/ci.yml/badge.svg"></a>
  <a href="https://github.com/major/index-etfs/actions/workflows/data-refresh.yml"><img alt="Data refresh" src="https://github.com/major/index-etfs/actions/workflows/data-refresh.yml/badge.svg"></a>
  <a href="https://codecov.io/gh/major/index-etfs"><img alt="Coverage" src="https://codecov.io/gh/major/index-etfs/graph/badge.svg"></a>
  <img alt="Python 3.15" src="https://img.shields.io/badge/python-3.15-3776AB?logo=python&logoColor=white">
  <img alt="Built with uv" src="https://img.shields.io/badge/built%20with-uv-654FF0">
</p>

> ⚠️ Not financial advice. Just a tiny robot that turns public ETF holdings into useful ticker files.

## 🚀 Download files

Grab the TradingView version for watchlist imports, or the plain ticker file for scripts and spreadsheets.

| Index | ETF | TradingView watchlist | Grouped watchlist | Plain tickers | Approx. count |
|---|---|---|---|---|---:|
| S&P 500 | SPY | [💾 sp500.txt](https://raw.githubusercontent.com/major/index-etfs/main/watchlists/sp500.txt) | [💾 sp500-grouped.txt](https://raw.githubusercontent.com/major/index-etfs/main/watchlists/sp500-grouped.txt) | [💾 spy.txt](tickers/spy.txt) | ~503 |
| Nasdaq-100 | QQQ | [💾 nasdaq100.txt](https://raw.githubusercontent.com/major/index-etfs/main/watchlists/nasdaq100.txt) | [💾 nasdaq100-grouped.txt](https://raw.githubusercontent.com/major/index-etfs/main/watchlists/nasdaq100-grouped.txt) | [💾 qqq.txt](tickers/qqq.txt) | ~101 |
| S&P MidCap 400 | MDY | [💾 sp400.txt](https://raw.githubusercontent.com/major/index-etfs/main/watchlists/sp400.txt) | [💾 sp400-grouped.txt](https://raw.githubusercontent.com/major/index-etfs/main/watchlists/sp400-grouped.txt) | [💾 mdy.txt](tickers/mdy.txt) | ~400 |
| S&P SmallCap 600 | SPSM | [💾 sp600.txt](https://raw.githubusercontent.com/major/index-etfs/main/watchlists/sp600.txt) | [💾 sp600-grouped.txt](https://raw.githubusercontent.com/major/index-etfs/main/watchlists/sp600-grouped.txt) | [💾 spsm.txt](tickers/spsm.txt) | ~605 |
| Russell 2000 | IWM | [💾 russell2000.txt](https://raw.githubusercontent.com/major/index-etfs/main/watchlists/russell2000.txt) | [💾 russell2000-grouped.txt](https://raw.githubusercontent.com/major/index-etfs/main/watchlists/russell2000-grouped.txt) | [💾 iwm.txt](tickers/iwm.txt) | ~1973 |
| MVIS US Listed Semiconductor 25 Index | SMH | [💾 smh.txt](https://raw.githubusercontent.com/major/index-etfs/main/watchlists/smh.txt) | [💾 smh-grouped.txt](https://raw.githubusercontent.com/major/index-etfs/main/watchlists/smh-grouped.txt) | [💾 smh.txt](tickers/smh.txt) | ~25 |
| MarketVector Global Gold Miners Index (US-listed subset) | GDX | [💾 gdx-us.txt](https://raw.githubusercontent.com/major/index-etfs/main/watchlists/gdx-us.txt) | [💾 gdx-us-grouped.txt](https://raw.githubusercontent.com/major/index-etfs/main/watchlists/gdx-us-grouped.txt) | [💾 gdx.txt](tickers/gdx.txt) | ~32 |

TradingView watchlists contain `EXCHANGE:TICKER` symbols. Grouped files add sector-only `###Section` headers. Plain files contain bare tickers.

SMH tracks the MVIS US Listed Semiconductor 25 Index. GDX tracks the MarketVector Global Gold Miners Index, but the files here are only its US-listed subset. In a verified October 23, 2026 snapshot, the GDX workbook had 59 stock rows: 32 unqualified US-listed symbols and 27 foreign-market-qualified symbols. The output excludes those 27 symbols without removing suffixes or creating US aliases. It is not the full global GDX constituent list. The GDX scope is also recorded in `metadata/latest.json` as `scope_note`.

## 🛠️ Run it locally

Requirements: Python 3.15 and `uv`.

Python 3.15's [`frozendict`](https://docs.python.org/3.15/whatsnew/3.15.html#pep-814-add-frozendict-built-in-type) keeps the ETF configuration registry immutable.

```bash
uv sync
uv run get-holdings
```

That rewrites `tickers/*.txt` and `watchlists/*.txt` from the latest available holdings data.

A full refresh fetches and validates every source, then resolves the combined set of unique tickers through TradingView. Repeated ticker rows are reported and collapsed before count validation. Missing or ambiguous TradingView matches use bare tickers; conflicting sectors use `Other`. Per-index metadata records these lookup issues.

All files are prepared in a temporary directory before publication. Fetch, validation, lookup, or preparation failures leave existing outputs unchanged. Publication replaces individual files with metadata last; it is not crash-atomic, does not roll back replacement failures, and does not coordinate concurrent runs. The single-ETF Python API keeps its existing immediate-write behavior.

Holdings downloads and read-only TradingView requests retry selected transient failures up to three times. Each attempt uses a 30-second socket timeout, with short waits capped at five seconds. This is not a total refresh deadline. Permanent HTTP errors and parsing errors fail without retries.

## Adding an index

- Start with a representative source file and check its terms of use. ETF holdings are proxies for index membership, not official constituent lists. Nasdaq-100 currently uses a direct Nasdaq endpoint rather than QQQ holdings.
- Add a configuration in `src/index_etfs/catalog.py` with a positive expected count and safe, unique output names. Names such as `foo` and `foo-grouped` collide with generated grouped watchlists and are rejected before a full refresh starts.
- Reuse a provider only when its format matches: SSGA uses an Excel header at row five, iShares skips nine CSV preamble rows, Nasdaq expects a Nasdaq-100-specific JSON shape, and VanEck uses an Excel header on row three with cookie-enabled downloads.
- Check filtering carefully. SSGA and iShares cleanup keeps USD rows, which does not by itself prove that a row is an equity. VanEck filters to `Asset Class` = `Stock`; GDX also excludes foreign-market-qualified symbols for its US-listed subset. TradingView lookup supports the America market only.
- Add a tiny offline fixture and parser test, run `uv run ruff check .` and `uv run pytest`, then check matching ticker, watchlist, and metadata outputs in a separate data refresh.

Provider holdings pages are publicly accessible, but public access does not grant a redistribution license. Check the provider's terms before using or republishing source data.

## 🔎 Track changes

Want to see index additions and removals? Check the [main branch commits](https://github.com/major/index-etfs/commits/main/) after each refresh.

Full refreshes also compare unique tickers with the previous `tickers/<symbol>.txt` snapshot. Each watchlist in `metadata/latest.json` includes `membership_changes`: baseline availability, previous count, and sorted additions and removals with their counts. Changes are reported even when the total count stays the same, but they do not block rebalances or prove official index membership.

Missing or empty baselines have `baseline_available: false`, `previous_count: null`, and empty change lists. Empty baselines produce a warning. Preparation failures do not advance the published baseline; publication itself has the per-file limitations described above.

The offline scale test covers 15 overlapping synthetic configurations through the real pipeline with mocked HTTP responses. It does not establish compatibility with 15 real feeds. New index additions still need representative provider fixtures and a separate data refresh.
