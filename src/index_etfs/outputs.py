"""Write ticker and metadata outputs."""

import json
import math
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

from .catalog import ETF_CONFIGS, MIN_EXPECTED_RATIO


def write_metadata(
    results: dict[str, int],
    output_dir: Path | None = None,
    *,
    details: dict[str, dict] | None = None,
) -> None:
    """Write the latest generated counts, source URLs, and ETF details."""
    if output_dir is None:
        output_dir = Path.cwd()

    metadata = {
        "generated_at": datetime.now(UTC)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z"),
        "etfs": {
            symbol: {
                "count": count,
                "expected_count": config.expected_count,
                "minimum_count": math.ceil(config.expected_count * MIN_EXPECTED_RATIO),
                "source": symbol,
                "source_url": config.url,
                **({"scope_note": config.scope_note} if config.scope_note else {}),
                **(details or {}).get(symbol, {}),
            }
            for symbol, count in results.items()
            for config in [ETF_CONFIGS[symbol]]
        },
    }

    metadata_dir = output_dir / "metadata"
    metadata_dir.mkdir(parents=True, exist_ok=True)
    (metadata_dir / "latest.json").write_text(json.dumps(metadata, indent=2) + "\n")


def _write_lines(path: Path, lines: list[str]) -> None:
    """Write lines to a file with a trailing newline."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n")


def save_holdings(
    df: pl.DataFrame,
    symbol: str,
    output_dir: Path | None = None,
) -> None:
    """Save one ETF's ticker symbols."""
    if output_dir is None:
        output_dir = Path.cwd()

    _write_lines(output_dir / "tickers" / f"{symbol}.txt", df["Ticker"].to_list())
