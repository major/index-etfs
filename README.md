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

## Plain ticker files

| ETF | Tickers | Count |
|-----|---------|-------|
| SPY | [spy.txt](tickers/spy.txt) | ~503 |
| MDY | [mdy.txt](tickers/mdy.txt) | ~400 |
| SPSM | [spsm.txt](tickers/spsm.txt) | ~605 |
| QQQ | [qqq.txt](tickers/qqq.txt) | ~101 |
| IWM | [iwm.txt](tickers/iwm.txt) | ~1906 |

Plain files in `tickers/` contain one ticker symbol per line, sorted alphabetically.

## Usage

```bash
# Install dependencies
uv sync

# Download all ETF ticker symbols
uv run get-holdings
```

To spot changes over time, look at the [commits in the main branch](commits/main/).
