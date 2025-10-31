# 📊 Index ETF Ticker Symbols

Extract ticker symbols from various index ETFs. Just the symbols (like AAPL, MSFT, NVDA), nothing else.

_Not financial advice, just a fun project._

## Supported ETFs

| ETF | Tickers | Count |
|-----|---------|-------|
| SPY | [spy.txt](spy.txt) | ~503 |
| MDY | [mdy.txt](mdy.txt) | ~402 |
| SPSM | [spsm.txt](spsm.txt) | ~605 |
| QQQ | [qqq.txt](qqq.txt) | ~102 |
| IWM | [iwm.txt](iwm.txt) | ~1965 |

Each file contains one ticker symbol per line, sorted alphabetically.

## Usage

```bash
# Install dependencies
uv sync

# Download all ETF ticker symbols
uv run get-holdings
```

To spot changes over time, look at the [commits in the main branch](commits/main/).
