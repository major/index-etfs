"""Compare fetched ticker membership with the last published baseline."""

from pathlib import Path


def compare_membership(
    tickers: list[str], baseline_path: Path
) -> dict[str, bool | int | None | list[str]]:
    """Return a diagnostic comparing current tickers with a published baseline."""
    try:
        baseline_text = baseline_path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return {
            "baseline_available": False,
            "previous_count": None,
            "added_count": 0,
            "removed_count": 0,
            "added_tickers": [],
            "removed_tickers": [],
        }

    baseline = {line.strip() for line in baseline_text.splitlines() if line.strip()}
    if not baseline:
        print(f"⚠️  Empty membership baseline for {baseline_path.stem.upper()}")
        return {
            "baseline_available": False,
            "previous_count": None,
            "added_count": 0,
            "removed_count": 0,
            "added_tickers": [],
            "removed_tickers": [],
        }

    current = set(tickers)
    added_tickers = sorted(current - baseline)
    removed_tickers = sorted(baseline - current)
    if added_tickers or removed_tickers:
        print(
            f"⚠️  Membership changes for {baseline_path.stem.upper()}: "
            f"baseline {len(baseline)}, current {len(current)}, "
            f"added {len(added_tickers)}, removed {len(removed_tickers)}"
        )

    return {
        "baseline_available": True,
        "previous_count": len(baseline),
        "added_count": len(added_tickers),
        "removed_count": len(removed_tickers),
        "added_tickers": added_tickers,
        "removed_tickers": removed_tickers,
    }
