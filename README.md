# 📊 Index ETF Ticker Symbols

Extract ticker symbols from various index ETFs, including TradingView-importable watchlists.

_Not financial advice, just a fun project._

## TradingView watchlists

Download one of these files, then import it in TradingView's watchlist menu.

| Index | TradingView file | Count |
|-----|---------|-------|
| S&P 500 | [download sp500.txt](https://raw.githubusercontent.com/major/index-etfs/main/watchlists/sp500.txt) | ~503 |
| Nasdaq-100 | [download nasdaq100.txt](https://raw.githubusercontent.com/major/index-etfs/main/watchlists/nasdaq100.txt) | ~101 |
| S&P MidCap 400 | [download sp400.txt](https://raw.githubusercontent.com/major/index-etfs/main/watchlists/sp400.txt) | ~400 |
| S&P SmallCap 600 | [download sp600.txt](https://raw.githubusercontent.com/major/index-etfs/main/watchlists/sp600.txt) | ~605 |
| Russell 2000 | [download russell2000.txt](https://raw.githubusercontent.com/major/index-etfs/main/watchlists/russell2000.txt) | ~1906 |

TradingView files contain one `EXCHANGE:TICKER` symbol per line, sorted alphabetically by ticker.
Latest generated counts and source URLs are in [metadata/latest.json](metadata/latest.json).

## Plain ticker files

| ETF | Tickers | Count |
|-----|---------|-------|
| SPY | [spy.txt](spy.txt) | ~503 |
| MDY | [mdy.txt](mdy.txt) | ~400 |
| SPSM | [spsm.txt](spsm.txt) | ~605 |
| QQQ | [qqq.txt](qqq.txt) | ~101 |
| IWM | [iwm.txt](iwm.txt) | ~1906 |

Plain files contain one ticker symbol per line, sorted alphabetically.

## Usage

```bash
# Install dependencies
uv sync

# Download all ETF ticker symbols
uv run get-holdings
```

To spot changes over time, look at the [commits in the main branch](commits/main/).
