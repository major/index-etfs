"""Tests for ETF catalog validation."""

import pytest

from index_etfs import catalog, holdings
from index_etfs.catalog import ETFConfig, validate_catalog


def test_validate_catalog_accepts_current_configuration() -> None:
    """The runtime catalog and safe punctuation satisfy output contracts."""
    validate_catalog()
    validate_catalog(
        {
            "safe_name-1": ETFConfig(
                "https://example.com/data", "ssga", 1
            )
        }
    )


@pytest.mark.parametrize(
    "symbol",
    ["smh", "gdx"],
)
def test_validate_catalog_accepts_vaneck_configs(symbol: str) -> None:
    """Accept valid VanEck provider configs."""
    # Arrange
    config = ETFConfig("https://example.com/data", "vaneck", 1)

    # Act
    result = validate_catalog({symbol: config})

    # Assert
    assert result is None


@pytest.mark.parametrize(
    "provider",
    ["unsupported", ""],
)
def test_validate_catalog_rejects_unsupported_provider(provider: str) -> None:
    """Only providers with a loader should be accepted."""
    config = ETFConfig("https://example.com/data", provider, 1)  # type: ignore[arg-type]

    with pytest.raises(ValueError, match="provider"):
        validate_catalog({"safe": config})


@pytest.mark.parametrize("expected_count", [0, -1, True, 1.5])
def test_validate_catalog_rejects_invalid_expected_count(expected_count: object) -> None:
    """Expected row counts must be positive integers, not booleans."""
    config = ETFConfig(
        "https://example.com/data", "ssga", expected_count  # type: ignore[arg-type]
    )

    with pytest.raises(ValueError, match="expected_count"):
        validate_catalog({"safe": config})


@pytest.mark.parametrize(
    "url",
    ["", "ftp://example.com/data", "https:///data", "not-a-url"],
)
def test_validate_catalog_rejects_invalid_url(url: str) -> None:
    """Source URLs must use HTTP or HTTPS and include a host."""
    config = ETFConfig(url, "ssga", 1)

    with pytest.raises(ValueError, match="URL"):
        validate_catalog({"safe": config})


@pytest.mark.parametrize(
    "symbol",
    [
        "",
        "../escape",
        "UPPER",
        "has space",
        "é",
    ],
)
def test_validate_catalog_rejects_unsafe_symbols(symbol: str) -> None:
    """Catalog symbols must be safe ticker filename components."""
    config = ETFConfig("https://example.com/data", "ssga", 1)

    with pytest.raises(ValueError, match="name"):
        validate_catalog({symbol: config})


def test_main_rejects_invalid_catalog_before_fetching_or_writing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Main must validate the catalog before any fetch or output operation."""
    configs = dict(catalog.ETF_CONFIGS)
    config = configs["spy"]
    configs["spy"] = ETFConfig(
        config.url,
        "unsupported",
        config.expected_count,
    )  # type: ignore[arg-type]
    monkeypatch.setattr(catalog, "ETF_CONFIGS", frozendict(configs))
    monkeypatch.setattr(
        holdings,
        "get_etf_holdings",
        lambda *_args: pytest.fail("fetch called"),
    )
    monkeypatch.setattr(
        holdings, "write_metadata", lambda *_args: pytest.fail("write called")
    )

    with pytest.raises(ValueError, match="provider"):
        holdings.main()
