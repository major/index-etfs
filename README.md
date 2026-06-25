# 📈 Index ETF Ticker Symbols

<p align="center">
  <strong>Fresh ticker lists for major US indexes — ready for TradingView, scripts, and quick market scans.</strong>
</p>

<p align="center">
  <a href="https://github.com/major/index-etfs/actions/workflows/ci.yml"><img alt="CI" src="https://github.com/major/index-etfs/actions/workflows/ci.yml/badge.svg"></a>
  <a href="https://github.com/major/index-etfs/actions/workflows/data-refresh.yml"><img alt="Data refresh" src="https://github.com/major/index-etfs/actions/workflows/data-refresh.yml/badge.svg"></a>
  <a href="https://codecov.io/gh/major/index-etfs"><img alt="Coverage" src="https://codecov.io/gh/major/index-etfs/graph/badge.svg"></a>
  <img alt="Python 3.14" src="https://img.shields.io/badge/python-3.14-3776AB?logo=python&logoColor=white">
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
| Russell 2000 | IWM | [💾 russell2000.txt](https://raw.githubusercontent.com/major/index-etfs/main/watchlists/russell2000.txt) | [💾 russell2000-grouped.txt](https://raw.githubusercontent.com/major/index-etfs/main/watchlists/russell2000-grouped.txt) | [💾 iwm.txt](tickers/iwm.txt) | ~1906 |

TradingView watchlists contain `EXCHANGE:TICKER` symbols. Grouped files add sector-only `###Section` headers. Plain files contain bare tickers.

## 🛠️ Run it locally

```bash
uv sync
uv run get-holdings
```

That rewrites `tickers/*.txt` and `watchlists/*.txt` from the latest available holdings data.

## 🔎 Track changes

Want to see index additions and removals? Check the [main branch commits](https://github.com/major/index-etfs/commits/main/) after each refresh.
