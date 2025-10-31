# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Python project that tracks holdings of various index ETFs (SPY, MDY, SPSM, QQQ, IWM) by downloading data from different providers (SSGA, iShares, Direxion) and generating CSV and Markdown reports. The project uses `uv` for dependency management and is built for Python 3.13.

## Development Setup

```bash
# Install dependencies including dev dependencies
uv sync --extra dev

# The virtual environment is in .venv/
source .venv/bin/activate
```

## Testing

```bash
# Run all tests with coverage
uv run pytest

# Run a specific test
uv run pytest tests/test_holdings.py::test_filter_and_clean

# Run tests matching a pattern
uv run pytest -k "test_save"
```

The project is configured with pytest-cov to show coverage reports automatically. Target coverage is 88%+.

## Running the Application

```bash
# Download all ETF holdings (generates CSV and MD files in current directory)
get-holdings

# Or run directly with uv
uv run get-holdings
```

This fetches holdings data from provider APIs and creates `{symbol}.csv` and `{symbol}.md` files in the current directory.

## Code Architecture

### Core Module: `src/index_etfs/holdings.py`

The codebase uses a **provider-based architecture** where each ETF data provider (SSGA, iShares, Direxion) has:
- Different data formats (Excel vs CSV)
- Different column names that need normalization
- Different filtering requirements (currency, ticker validation)

**Key architectural pattern:**
1. `ETF_CONFIGS` dict maps ETF symbols → provider + URL
2. Provider-specific loader functions (`_load_ssga_excel`, `_load_ishares_csv`, `_load_direxion_csv`) handle format differences
3. Universal `_filter_and_clean()` normalizes all data to common schema: `[Ticker, Name, Weight]`
4. `get_etf_holdings()` orchestrates: load → filter → save

**Adding new ETFs:** Just add an entry to `ETF_CONFIGS` with the provider type and URL. If it's a new provider format, add a new `_load_{provider}_*()` function.

### Data Flow

```
Provider API → Provider Loader → _filter_and_clean() → _save_holdings() → CSV + MD files
```

All data passes through the same filtering/cleaning pipeline regardless of source provider, ensuring consistent output format.

### Testing Strategy

Tests use pytest fixtures for sample data and mock the provider loaders to avoid network calls. Each provider loader is tested independently, then integration is tested through `get_etf_holdings()`. Parametrized tests cover all 5 ETFs.

## Python Standards

- Python 3.13 with type hints using `Literal` for restricted string values
- PEP 257 docstrings
- Uses Polars DataFrames (not pandas) for data processing
- Private functions prefixed with `_`
