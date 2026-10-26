# 📈 Index ETF Ticker Symbols

<p align="center">
  <strong>Fresh ticker lists for major US indexes, ready for scripts and spreadsheets.</strong>
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

Download plain ticker files for scripts and spreadsheets.

| Index | ETF | Plain tickers | Approx. count |
|---|---|---|---:|
| S&P 500 | SPY | [💾 spy.txt](tickers/spy.txt) | ~503 |
| Nasdaq-100 | QQQ | [💾 qqq.txt](tickers/qqq.txt) | ~101 |
| S&P MidCap 400 | MDY | [💾 mdy.txt](tickers/mdy.txt) | ~400 |
| S&P SmallCap 600 | SPSM | [💾 spsm.txt](tickers/spsm.txt) | ~605 |
| Russell 2000 | IWM | [💾 iwm.txt](tickers/iwm.txt) | ~1973 |
| MVIS US Listed Semiconductor 25 Index | SMH | [💾 smh.txt](tickers/smh.txt) | ~25 |
| MarketVector Global Gold Miners Index (US-listed subset) | GDX | [💾 gdx.txt](tickers/gdx.txt) | ~32 |
| S&P 500 Materials | XLB | [💾 xlb.txt](tickers/xlb.txt) | ~24 |
| S&P 500 Communication Services | XLC | [💾 xlc.txt](tickers/xlc.txt) | ~22 |
| S&P 500 Energy | XLE | [💾 xle.txt](tickers/xle.txt) | ~21 |
| S&P 500 Financials | XLF | [💾 xlf.txt](tickers/xlf.txt) | ~76 |
| S&P 500 Industrials | XLI | [💾 xli.txt](tickers/xli.txt) | ~83 |
| S&P 500 Technology | XLK | [💾 xlk.txt](tickers/xlk.txt) | ~75 |
| S&P 500 Consumer Staples | XLP | [💾 xlp.txt](tickers/xlp.txt) | ~35 |
| S&P 500 Real Estate | XLRE | [💾 xlre.txt](tickers/xlre.txt) | ~30 |
| S&P 500 Utilities | XLU | [💾 xlu.txt](tickers/xlu.txt) | ~31 |
| S&P 500 Health Care | XLV | [💾 xlv.txt](tickers/xlv.txt) | ~61 |
| S&P 500 Consumer Discretionary | XLY | [💾 xly.txt](tickers/xly.txt) | ~47 |

SMH tracks the MVIS US Listed Semiconductor 25 Index. GDX tracks the MarketVector Global Gold Miners Index, but the files here are only its US-listed subset. In a verified October 23, 2026 snapshot, the GDX workbook had 59 stock rows: 32 unqualified US-listed symbols and 27 foreign-market-qualified symbols. The output excludes those 27 symbols without removing suffixes or creating US aliases. It is not the full global GDX constituent list. The GDX scope is also recorded in `metadata/latest.json` as `scope_note`.

## 🌐 Market-group funds

The catalog has 60 holdings configurations. The market-group request contains 59 symbols: 55 supported holdings feeds and four explicit non-equity exclusions. The 55 feeds comprise 13 funds already shown in the download table and 42 newly configured funds. The other five catalog entries are the existing broad-market funds SPY, MDY, SPSM, QQQ, and IWM.

The table below links to local ticker files for the 42 newly configured funds. Together with the existing links above, they cover all 55 supported market-group funds.

| Issuer | Fund | Plain tickers |
|---|---|---|
| State Street (SSGA) | XME | [plain](tickers/xme.txt) |
| State Street (SSGA) | XOP | [plain](tickers/xop.txt) |
| State Street (SSGA) | KRE | [plain](tickers/kre.txt) |
| State Street (SSGA) | KBE | [plain](tickers/kbe.txt) |
| State Street (SSGA) | XRT | [plain](tickers/xrt.txt) |
| State Street (SSGA) | XTL | [plain](tickers/xtl.txt) |
| VanEck | GDXJ | [plain](tickers/gdxj.txt) |
| VanEck | MOO | [plain](tickers/moo.txt) |
| VanEck | NLR | [plain](tickers/nlr.txt) |
| VanEck | OIH | [plain](tickers/oih.txt) |
| VanEck | CRAK | [plain](tickers/crak.txt) |
| VanEck | ESPO | [plain](tickers/espo.txt) |
| iShares | SOXX | [plain](tickers/soxx.txt) |
| iShares | IGV | [plain](tickers/igv.txt) |
| iShares | IBB | [plain](tickers/ibb.txt) |
| iShares | IHI | [plain](tickers/ihi.txt) |
| iShares | IHE | [plain](tickers/ihe.txt) |
| iShares | IHF | [plain](tickers/ihf.txt) |
| iShares | ITA | [plain](tickers/ita.txt) |
| iShares | IYT | [plain](tickers/iyt.txt) |
| iShares | IAI | [plain](tickers/iai.txt) |
| Global X | AIQ | [plain](tickers/aiq.txt) |
| Global X | BUG | [plain](tickers/bug.txt) |
| Global X | COPX | [plain](tickers/copx.txt) |
| Global X | URA | [plain](tickers/ura.txt) |
| Global X | SIL | [plain](tickers/sil.txt) |
| Global X | MLPX | [plain](tickers/mlpx.txt) |
| Global X | LIT | [plain](tickers/lit.txt) |
| Global X | PAVE | [plain](tickers/pave.txt) |
| Invesco | IGPT | [plain](tickers/igpt.txt) |
| Invesco | TAN | [plain](tickers/tan.txt) |
| Invesco | KBWB | [plain](tickers/kbwb.txt) |
| Invesco | PEJ | [plain](tickers/pej.txt) |
| Invesco | PBJ | [plain](tickers/pbj.txt) |
| ARK | ARKG | [plain](tickers/arkg.txt) |
| ARK | ARKF | [plain](tickers/arkf.txt) |
| First Trust | CIBR | [plain](tickers/cibr.txt) |
| First Trust | GRID | [plain](tickers/grid.txt) |
| FlexShares | GUNR | [plain](tickers/gunr.txt) |
| Tema | VOLT | [plain](tickers/volt.txt) |
| Vanguard | VNQ | [plain](tickers/vnq.txt) |
| Amplify | BLOK | [plain](tickers/blok.txt) |

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

That rewrites `tickers/*.txt` from the latest available holdings data.

A full refresh fetches and validates every source. Repeated ticker rows are reported and collapsed before count validation. The `etfs` object in `metadata/latest.json` is keyed by ETF symbol.

Ticker files and metadata are prepared in a temporary directory before publication. Fetch, validation, or preparation failures leave existing outputs unchanged. Publication replaces individual files with metadata last; it is not crash-atomic, does not roll back replacement failures, and does not coordinate concurrent runs. The single-ETF Python API keeps its existing immediate-write behavior.

Holdings downloads retry selected transient failures up to three times. Each attempt uses a 30-second socket timeout, with short waits capped at five seconds. This is not a total refresh deadline. Permanent HTTP errors and parsing errors fail without retries.

## Adding an index

- Start with a representative source file and check its terms of use. ETF holdings, including SPDR sector holdings, are proxies for index membership, not official constituent lists. Nasdaq-100 currently uses a direct Nasdaq endpoint rather than QQQ holdings.
- Add a configuration in `src/index_etfs/catalog.py` with a positive expected count and safe, unique output names.
- Reuse a provider only when its format matches. Supported providers are SSGA, iShares, Nasdaq, VanEck, Invesco, ARK, Global X, Vanguard, First Trust, FlexShares, Tema, and Amplify. New provider parsers live in focused modules under `src/index_etfs/` and are wired through `sources.py`.
- Check filtering carefully. SSGA removes futures-like names and keeps USD rows; iShares keeps USD-denominated rows, which does not prove a row is an equity. VanEck uses its stock asset-class field and removes cash and foreign-qualified rows. Other providers apply source-specific cash, security-type, classification, or ticker-format filters. These are not exhaustive exchange or equity classifications.
- Add a tiny offline fixture and parser test, update provider contract coverage where needed, run `uv run ruff check .` and `uv run pytest`, then check matching ticker and metadata outputs in a separate data refresh.

Provider holdings pages are publicly accessible, but public access does not grant a redistribution license. Check the provider's terms before using or republishing source data.

## 🔎 Track changes

Want to see index additions and removals? Check the [main branch commits](https://github.com/major/index-etfs/commits/main/) after each refresh.

Full refreshes also compare unique tickers with the previous `tickers/<symbol>.txt` snapshot. Each entry in the `metadata/latest.json` `etfs` object includes `membership_changes`: baseline availability, previous count, and sorted additions and removals with their counts. Changes are reported even when the total count stays the same, but they do not block rebalances or prove official index membership.

Missing or empty baselines have `baseline_available: false`, `previous_count: null`, and empty change lists. Empty baselines produce a warning. Preparation failures do not advance the published baseline; publication itself has the per-file limitations described above.

The offline scale test covers 15 overlapping synthetic configurations through the real pipeline with mocked HTTP responses. It does not establish compatibility with 15 real feeds. Future index additions still need representative provider fixtures and a separate data refresh.
