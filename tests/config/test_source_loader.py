"""Unit tests for the YAML loader at src/config/source_loader.py.

Verifies that all five committed ``config/sources/*.yaml`` files validate
under the discriminated union, and that error paths (missing file, schema
mismatch) raise clearly.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
import yaml
from pydantic import ValidationError

from src.config.source_loader import load_source_config
from src.config.source_registry import FlatFileSourceConfig, RestApiSourceConfig

if TYPE_CHECKING:
    from pathlib import Path

# --- Happy paths against the real committed YAML files ---


@pytest.mark.parametrize(
    "source_name,expected_cls",
    [
        ("cpsc", RestApiSourceConfig),
        ("fda", RestApiSourceConfig),
        ("usda", RestApiSourceConfig),
        ("usda_establishments", RestApiSourceConfig),
        ("nhtsa", FlatFileSourceConfig),
    ],
)
def test_load_real_yaml_returns_expected_type(source_name: str, expected_cls: type) -> None:
    """All five committed YAMLs validate under the discriminated union."""
    config = load_source_config(source_name)
    assert isinstance(config, expected_cls)
    assert config.source_name == source_name


def test_cpsc_yaml_carries_documented_intent_fields() -> None:
    config = load_source_config("cpsc")
    assert isinstance(config, RestApiSourceConfig)
    assert config.base_url.startswith("https://www.saferproducts.gov/")
    assert config.incremental_filter_param == "LastPublishDateStart"
    assert config.default_lookback_days == 1
    assert config.format_param == "json"


def test_fda_yaml_timeout_is_60_seconds() -> None:
    """The Wave 2 fix: FDA YAML says 60s; once loader is wired, that's runtime."""
    config = load_source_config("fda")
    assert isinstance(config, RestApiSourceConfig)
    assert config.timeout_seconds == 60.0


def test_usda_yamls_etag_enabled_is_true() -> None:
    """USDA recall + establishment ETag flipped on 2026-05-09 (Finding P)."""
    for source_name in ("usda", "usda_establishments"):
        config = load_source_config(source_name)
        assert isinstance(config, RestApiSourceConfig)
        assert config.etag_enabled is True


def test_nhtsa_yaml_has_historical_seed_urls() -> None:
    config = load_source_config("nhtsa")
    assert isinstance(config, FlatFileSourceConfig)
    assert config.historical_seed_urls is not None
    assert len(config.historical_seed_urls) >= 1
    # PRE_2010 is the historical archive consumed by NhtsaDeepRescanLoader.
    assert any("PRE_2010" in url for url in config.historical_seed_urls)


# --- Error paths ---


def test_load_missing_file_raises_filenotfound() -> None:
    with pytest.raises(FileNotFoundError) as exc_info:
        load_source_config("nonexistent_source")
    assert "nonexistent_source" in str(exc_info.value)


def test_load_invalid_shape_raises_validation_error(tmp_path: Path) -> None:
    """A shape mismatch (e.g., unknown source_type) raises ValidationError."""
    bad_yaml = tmp_path / "bad.yaml"
    bad_yaml.write_text(
        yaml.safe_dump(
            {
                "source_name": "bad",
                "source_type": "html_scraping",  # not in our discriminated union
                "base_url": "https://example.test",
            }
        )
    )

    # Re-route the loader to the tmp dir for this one test.
    from src.config import source_loader

    original_dir = source_loader._CONFIG_DIR
    source_loader._CONFIG_DIR = tmp_path
    try:
        with pytest.raises(ValidationError):
            load_source_config("bad")
    finally:
        source_loader._CONFIG_DIR = original_dir


def test_load_extra_field_rejected_under_forbid(tmp_path: Path) -> None:
    """A YAML with a key the schema doesn't declare fails loudly."""
    bad_yaml = tmp_path / "extra.yaml"
    bad_yaml.write_text(
        yaml.safe_dump(
            {
                "source_name": "extra",
                "source_type": "rest_api",
                "base_url": "https://example.test",
                "totally_unknown_key": "boom",
            }
        )
    )

    from src.config import source_loader

    original_dir = source_loader._CONFIG_DIR
    source_loader._CONFIG_DIR = tmp_path
    try:
        with pytest.raises(ValidationError) as exc_info:
            load_source_config("extra")
        assert "totally_unknown_key" in str(exc_info.value)
    finally:
        source_loader._CONFIG_DIR = original_dir
