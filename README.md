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
| S&P 500 Materials | XLB | [💾 xlb.txt](https://raw.githubusercontent.com/major/index-etfs/main/watchlists/xlb.txt) | [💾 xlb-grouped.txt](https://raw.githubusercontent.com/major/index-etfs/main/watchlists/xlb-grouped.txt) | [💾 xlb.txt](tickers/xlb.txt) | ~24 |
| S&P 500 Communication Services | XLC | [💾 xlc.txt](https://raw.githubusercontent.com/major/index-etfs/main/watchlists/xlc.txt) | [💾 xlc-grouped.txt](https://raw.githubusercontent.com/major/index-etfs/main/watchlists/xlc-grouped.txt) | [💾 xlc.txt](tickers/xlc.txt) | ~22 |
| S&P 500 Energy | XLE | [💾 xle.txt](https://raw.githubusercontent.com/major/index-etfs/main/watchlists/xle.txt) | [💾 xle-grouped.txt](https://raw.githubusercontent.com/major/index-etfs/main/watchlists/xle-grouped.txt) | [💾 xle.txt](tickers/xle.txt) | ~21 |
| S&P 500 Financials | XLF | [💾 xlf.txt](https://raw.githubusercontent.com/major/index-etfs/main/watchlists/xlf.txt) | [💾 xlf-grouped.txt](https://raw.githubusercontent.com/major/index-etfs/main/watchlists/xlf-grouped.txt) | [💾 xlf.txt](tickers/xlf.txt) | ~76 |
| S&P 500 Industrials | XLI | [💾 xli.txt](https://raw.githubusercontent.com/major/index-etfs/main/watchlists/xli.txt) | [💾 xli-grouped.txt](https://raw.githubusercontent.com/major/index-etfs/main/watchlists/xli-grouped.txt) | [💾 xli.txt](tickers/xli.txt) | ~83 |
| S&P 500 Technology | XLK | [💾 xlk.txt](https://raw.githubusercontent.com/major/index-etfs/main/watchlists/xlk.txt) | [💾 xlk-grouped.txt](https://raw.githubusercontent.com/major/index-etfs/main/watchlists/xlk-grouped.txt) | [💾 xlk.txt](tickers/xlk.txt) | ~75 |
| S&P 500 Consumer Staples | XLP | [💾 xlp.txt](https://raw.githubusercontent.com/major/index-etfs/main/watchlists/xlp.txt) | [💾 xlp-grouped.txt](https://raw.githubusercontent.com/major/index-etfs/main/watchlists/xlp-grouped.txt) | [💾 xlp.txt](tickers/xlp.txt) | ~35 |
| S&P 500 Real Estate | XLRE | [💾 xlre.txt](https://raw.githubusercontent.com/major/index-etfs/main/watchlists/xlre.txt) | [💾 xlre-grouped.txt](https://raw.githubusercontent.com/major/index-etfs/main/watchlists/xlre-grouped.txt) | [💾 xlre.txt](tickers/xlre.txt) | ~30 |
| S&P 500 Utilities | XLU | [💾 xlu.txt](https://raw.githubusercontent.com/major/index-etfs/main/watchlists/xlu.txt) | [💾 xlu-grouped.txt](https://raw.githubusercontent.com/major/index-etfs/main/watchlists/xlu-grouped.txt) | [💾 xlu.txt](tickers/xlu.txt) | ~31 |
| S&P 500 Health Care | XLV | [💾 xlv.txt](https://raw.githubusercontent.com/major/index-etfs/main/watchlists/xlv.txt) | [💾 xlv-grouped.txt](https://raw.githubusercontent.com/major/index-etfs/main/watchlists/xlv-grouped.txt) | [💾 xlv.txt](tickers/xlv.txt) | ~61 |
| S&P 500 Consumer Discretionary | XLY | [💾 xly.txt](https://raw.githubusercontent.com/major/index-etfs/main/watchlists/xly.txt) | [💾 xly-grouped.txt](https://raw.githubusercontent.com/major/index-etfs/main/watchlists/xly-grouped.txt) | [💾 xly.txt](tickers/xly.txt) | ~47 |

TradingView watchlists contain `EXCHANGE:TICKER` symbols. Grouped files add sector-only `###Section` headers. Plain files contain bare tickers.

SMH tracks the MVIS US Listed Semiconductor 25 Index. GDX tracks the MarketVector Global Gold Miners Index, but the files here are only its US-listed subset. In a verified October 23, 2026 snapshot, the GDX workbook had 59 stock rows: 32 unqualified US-listed symbols and 27 foreign-market-qualified symbols. The output excludes those 27 symbols without removing suffixes or creating US aliases. It is not the full global GDX constituent list. The GDX scope is also recorded in `metadata/latest.json` as `scope_note`.

## 🌐 Market-group funds

The catalog has 60 holdings configurations. The market-group request contains 59 symbols: 55 supported holdings feeds and four explicit non-equity exclusions. The 55 feeds comprise 13 funds already shown in the download table and 42 newly configured funds. The other five catalog entries are the existing broad-market funds SPY, MDY, SPSM, QQQ, and IWM.

The table below links to local generated files for the 42 newly configured funds. These links work in this checkout and do not depend on a published branch. Together with the existing links above, they cover all 55 supported market-group funds.

| Issuer | Fund | Watchlist | Grouped | Plain tickers |
|---|---|---|---|---|
| State Street (SSGA) | XME | [watchlist](watchlists/xme.txt) | [grouped](watchlists/xme-grouped.txt) | [plain](tickers/xme.txt) |
| State Street (SSGA) | XOP | [watchlist](watchlists/xop.txt) | [grouped](watchlists/xop-grouped.txt) | [plain](tickers/xop.txt) |
| State Street (SSGA) | KRE | [watchlist](watchlists/kre.txt) | [grouped](watchlists/kre-grouped.txt) | [plain](tickers/kre.txt) |
| State Street (SSGA) | KBE | [watchlist](watchlists/kbe.txt) | [grouped](watchlists/kbe-grouped.txt) | [plain](tickers/kbe.txt) |
| State Street (SSGA) | XRT | [watchlist](watchlists/xrt.txt) | [grouped](watchlists/xrt-grouped.txt) | [plain](tickers/xrt.txt) |
| State Street (SSGA) | XTL | [watchlist](watchlists/xtl.txt) | [grouped](watchlists/xtl-grouped.txt) | [plain](tickers/xtl.txt) |
| VanEck | GDXJ | [watchlist](watchlists/gdxj-us.txt) | [grouped](watchlists/gdxj-us-grouped.txt) | [plain](tickers/gdxj.txt) |
| VanEck | MOO | [watchlist](watchlists/moo-us.txt) | [grouped](watchlists/moo-us-grouped.txt) | [plain](tickers/moo.txt) |
| VanEck | NLR | [watchlist](watchlists/nlr-us.txt) | [grouped](watchlists/nlr-us-grouped.txt) | [plain](tickers/nlr.txt) |
| VanEck | OIH | [watchlist](watchlists/oih.txt) | [grouped](watchlists/oih-grouped.txt) | [plain](tickers/oih.txt) |
| VanEck | CRAK | [watchlist](watchlists/crak-us.txt) | [grouped](watchlists/crak-us-grouped.txt) | [plain](tickers/crak.txt) |
| VanEck | ESPO | [watchlist](watchlists/espo-us.txt) | [grouped](watchlists/espo-us-grouped.txt) | [plain](tickers/espo.txt) |
| iShares | SOXX | [watchlist](watchlists/soxx.txt) | [grouped](watchlists/soxx-grouped.txt) | [plain](tickers/soxx.txt) |
| iShares | IGV | [watchlist](watchlists/igv.txt) | [grouped](watchlists/igv-grouped.txt) | [plain](tickers/igv.txt) |
| iShares | IBB | [watchlist](watchlists/ibb.txt) | [grouped](watchlists/ibb-grouped.txt) | [plain](tickers/ibb.txt) |
| iShares | IHI | [watchlist](watchlists/ihi.txt) | [grouped](watchlists/ihi-grouped.txt) | [plain](tickers/ihi.txt) |
| iShares | IHE | [watchlist](watchlists/ihe.txt) | [grouped](watchlists/ihe-grouped.txt) | [plain](tickers/ihe.txt) |
| iShares | IHF | [watchlist](watchlists/ihf.txt) | [grouped](watchlists/ihf-grouped.txt) | [plain](tickers/ihf.txt) |
| iShares | ITA | [watchlist](watchlists/ita.txt) | [grouped](watchlists/ita-grouped.txt) | [plain](tickers/ita.txt) |
| iShares | IYT | [watchlist](watchlists/iyt.txt) | [grouped](watchlists/iyt-grouped.txt) | [plain](tickers/iyt.txt) |
| iShares | IAI | [watchlist](watchlists/iai.txt) | [grouped](watchlists/iai-grouped.txt) | [plain](tickers/iai.txt) |
| Global X | AIQ | [watchlist](watchlists/aiq-us.txt) | [grouped](watchlists/aiq-us-grouped.txt) | [plain](tickers/aiq.txt) |
| Global X | BUG | [watchlist](watchlists/bug-us.txt) | [grouped](watchlists/bug-us-grouped.txt) | [plain](tickers/bug.txt) |
| Global X | COPX | [watchlist](watchlists/copx-us.txt) | [grouped](watchlists/copx-us-grouped.txt) | [plain](tickers/copx.txt) |
| Global X | URA | [watchlist](watchlists/ura-us.txt) | [grouped](watchlists/ura-us-grouped.txt) | [plain](tickers/ura.txt) |
| Global X | SIL | [watchlist](watchlists/sil-us.txt) | [grouped](watchlists/sil-us-grouped.txt) | [plain](tickers/sil.txt) |
| Global X | MLPX | [watchlist](watchlists/mlpx-us.txt) | [grouped](watchlists/mlpx-us-grouped.txt) | [plain](tickers/mlpx.txt) |
| Global X | LIT | [watchlist](watchlists/lit-us.txt) | [grouped](watchlists/lit-us-grouped.txt) | [plain](tickers/lit.txt) |
| Global X | PAVE | [watchlist](watchlists/pave-us.txt) | [grouped](watchlists/pave-us-grouped.txt) | [plain](tickers/pave.txt) |
| Invesco | IGPT | [watchlist](watchlists/igpt-us.txt) | [grouped](watchlists/igpt-us-grouped.txt) | [plain](tickers/igpt.txt) |
| Invesco | TAN | [watchlist](watchlists/tan-us.txt) | [grouped](watchlists/tan-us-grouped.txt) | [plain](tickers/tan.txt) |
| Invesco | KBWB | [watchlist](watchlists/kbwb-us.txt) | [grouped](watchlists/kbwb-us-grouped.txt) | [plain](tickers/kbwb.txt) |
| Invesco | PEJ | [watchlist](watchlists/pej-us.txt) | [grouped](watchlists/pej-us-grouped.txt) | [plain](tickers/pej.txt) |
| Invesco | PBJ | [watchlist](watchlists/pbj-us.txt) | [grouped](watchlists/pbj-us-grouped.txt) | [plain](tickers/pbj.txt) |
| ARK | ARKG | [watchlist](watchlists/arkg-us.txt) | [grouped](watchlists/arkg-us-grouped.txt) | [plain](tickers/arkg.txt) |
| ARK | ARKF | [watchlist](watchlists/arkf-us.txt) | [grouped](watchlists/arkf-us-grouped.txt) | [plain](tickers/arkf.txt) |
| First Trust | CIBR | [watchlist](watchlists/cibr-us.txt) | [grouped](watchlists/cibr-us-grouped.txt) | [plain](tickers/cibr.txt) |
| First Trust | GRID | [watchlist](watchlists/grid-us.txt) | [grouped](watchlists/grid-us-grouped.txt) | [plain](tickers/grid.txt) |
| FlexShares | GUNR | [watchlist](watchlists/gunr-us.txt) | [grouped](watchlists/gunr-us-grouped.txt) | [plain](tickers/gunr.txt) |
| Tema | VOLT | [watchlist](watchlists/volt-us.txt) | [grouped](watchlists/volt-us-grouped.txt) | [plain](tickers/volt.txt) |
| Vanguard | VNQ | [watchlist](watchlists/vnq.txt) | [grouped](watchlists/vnq-grouped.txt) | [plain](tickers/vnq.txt) |
| Amplify | BLOK | [watchlist](watchlists/blok-us.txt) | [grouped](watchlists/blok-us-grouped.txt) | [plain](tickers/blok.txt) |

The request also named four funds that are not equity holdings feeds. `IBIT` has Bitcoin exposure, `USO` has oil futures exposure, `UGA` has gasoline futures exposure, and `UNG` has natural gas futures exposure. These are recorded in `MARKET_GROUP_EXCLUSIONS` in `src/index_etfs/catalog.py` and are not downloaded.

The `-us` suffix is a configured output name for a filtered subset. It is not a certified US exchange classification or a complete holdings list. Some providers' symbol formats are used to omit foreign-market-qualified rows, so those foreign holdings do not appear in the subset. Global X holdings are dated snapshots found within a bounded date window. Tema's official product page supplies the current dated holdings CSV link. The new provider families use official holdings sources from Invesco, ARK, Global X, Vanguard, First Trust, FlexShares, Tema, and Amplify.

Filters vary by provider and are not a universal equity-classification test. Some parsers remove cash, currency, futures, or other non-equity rows when the source exposes enough information. The iShares behavior keeps USD-denominated rows only, as it does for the existing IWM output; USD currency does not prove that a holding is an equity, and other-currency rows are omitted. BLOK includes listed fund positions without looking through to their holdings. VNQ keeps `EQ` rows but excludes underlying fund positions without lookthrough.

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

- Start with a representative source file and check its terms of use. ETF holdings, including SPDR sector holdings, are proxies for index membership, not official constituent lists. Nasdaq-100 currently uses a direct Nasdaq endpoint rather than QQQ holdings.
- Add a configuration in `src/index_etfs/catalog.py` with a positive expected count and safe, unique output names. Names such as `foo` and `foo-grouped` collide with generated grouped watchlists and are rejected before a full refresh starts.
- Reuse a provider only when its format matches. Supported providers are SSGA, iShares, Nasdaq, VanEck, Invesco, ARK, Global X, Vanguard, First Trust, FlexShares, Tema, and Amplify. New provider parsers live in focused modules under `src/index_etfs/` and are wired through `sources.py`.
- Check filtering carefully. SSGA removes futures-like names and keeps USD rows; iShares keeps USD-denominated rows, which does not prove a row is an equity. VanEck uses its stock asset-class field and removes cash and foreign-qualified rows. Other providers apply source-specific cash, security-type, classification, or ticker-format filters. These are not exhaustive exchange or equity classifications. TradingView lookup supports the America market only.
- Add a tiny offline fixture and parser test, update provider contract coverage where needed, run `uv run ruff check .` and `uv run pytest`, then check matching ticker, watchlist, and metadata outputs in a separate data refresh.

Provider holdings pages are publicly accessible, but public access does not grant a redistribution license. Check the provider's terms before using or republishing source data.

## 🔎 Track changes

Want to see index additions and removals? Check the [main branch commits](https://github.com/major/index-etfs/commits/main/) after each refresh.

Full refreshes also compare unique tickers with the previous `tickers/<symbol>.txt` snapshot. Each watchlist in `metadata/latest.json` includes `membership_changes`: baseline availability, previous count, and sorted additions and removals with their counts. Changes are reported even when the total count stays the same, but they do not block rebalances or prove official index membership.

Missing or empty baselines have `baseline_available: false`, `previous_count: null`, and empty change lists. Empty baselines produce a warning. Preparation failures do not advance the published baseline; publication itself has the per-file limitations described above.

The offline scale test covers 15 overlapping synthetic configurations through the real pipeline with mocked HTTP responses. It does not establish compatibility with 15 real feeds. Future index additions still need representative provider fixtures and a separate data refresh.
