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

from src.config.source_loader import deep_merge, load_source_config
from src.config.source_registry import (
    FlatFileSourceConfig,
    HtmlScrapingSourceConfig,
    RestApiSourceConfig,
)

if TYPE_CHECKING:
    from collections.abc import Iterator
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


# --- deep_merge (pure, no I/O) ---


def test_deep_merge_scalar_overlay_replaces_base() -> None:
    assert deep_merge({"a": 1, "b": 2}, {"b": 99}) == {"a": 1, "b": 99}


def test_deep_merge_adds_overlay_only_key() -> None:
    assert deep_merge({"a": 1}, {"c": 3}) == {"a": 1, "c": 3}


def test_deep_merge_list_overlay_replaces_wholesale_not_concatenated() -> None:
    # Lists are atomic — the overlay value replaces the base list entirely.
    assert deep_merge({"urls": ["x", "y"]}, {"urls": ["z"]}) == {"urls": ["z"]}


def test_deep_merge_nested_dict_merges_field_by_field() -> None:
    base = {"credentials": {"user": "u", "key": "base-key"}, "rate_limit_rps": 1.0}
    overlay = {"credentials": {"key": "prod-key"}}
    # Only the nested ``key`` is overridden; ``user`` survives, top-level untouched.
    assert deep_merge(base, overlay) == {
        "credentials": {"user": "u", "key": "prod-key"},
        "rate_limit_rps": 1.0,
    }


def test_deep_merge_does_not_mutate_inputs() -> None:
    base = {"credentials": {"user": "u"}}
    overlay = {"credentials": {"key": "k"}}
    deep_merge(base, overlay)
    assert base == {"credentials": {"user": "u"}}
    assert overlay == {"credentials": {"key": "k"}}


def test_deep_merge_dict_replaces_non_dict_base() -> None:
    # If base value isn't a dict, an overlay dict replaces it wholesale (no merge).
    assert deep_merge({"a": 5}, {"a": {"nested": True}}) == {"a": {"nested": True}}


# --- Per-environment overlay layering (C21 / ADR 0012) ---


def _write_base(tmp_path: Path) -> None:
    """A minimal valid REST source base file in ``tmp_path``."""
    (tmp_path / "ovl.yaml").write_text(
        yaml.safe_dump(
            {
                "source_name": "ovl",
                "source_type": "rest_api",
                "base_url": "https://base.example.test",
                "timeout_seconds": 30.0,
                "rate_limit_rps": 1.0,
            }
        )
    )


@pytest.fixture
def _route_config_dir(tmp_path: Path) -> Iterator[Path]:
    """Point the loader at ``tmp_path`` for the duration of one test, then restore."""
    from src.config import source_loader

    original_dir = source_loader._CONFIG_DIR
    source_loader._CONFIG_DIR = tmp_path
    try:
        yield tmp_path
    finally:
        source_loader._CONFIG_DIR = original_dir


def test_no_overlay_file_returns_base_unchanged(
    _route_config_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """(a) env set but no ``<source>.<env>.yaml`` → base file only."""
    monkeypatch.setenv("RECALLS_ENV", "prod")
    _write_base(_route_config_dir)
    config = load_source_config("ovl")
    assert isinstance(config, RestApiSourceConfig)
    assert config.base_url == "https://base.example.test"
    assert config.timeout_seconds == 30.0


def test_recalls_env_unset_ignores_existing_overlay(
    _route_config_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """(b) RECALLS_ENV unset → base only, even though an overlay file is present."""
    monkeypatch.delenv("RECALLS_ENV", raising=False)
    _write_base(_route_config_dir)
    (_route_config_dir / "ovl.prod.yaml").write_text(
        yaml.safe_dump({"base_url": "https://prod.example.test"})
    )
    config = load_source_config("ovl")
    assert isinstance(config, RestApiSourceConfig)
    # Overlay ignored — base wins because RECALLS_ENV is unset.
    assert config.base_url == "https://base.example.test"


def test_recalls_env_empty_string_ignores_overlay(
    _route_config_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An empty/whitespace RECALLS_ENV is treated as unset (no overlay)."""
    monkeypatch.setenv("RECALLS_ENV", "   ")
    _write_base(_route_config_dir)
    (_route_config_dir / "ovl.prod.yaml").write_text(
        yaml.safe_dump({"base_url": "https://prod.example.test"})
    )
    config = load_source_config("ovl")
    assert isinstance(config, RestApiSourceConfig)
    assert config.base_url == "https://base.example.test"


def test_overlay_merges_scalar_and_nested_fields(
    _route_config_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """(c) env set + overlay present → fields merged, incl. a nested-dict merge."""
    monkeypatch.setenv("RECALLS_ENV", "prod")
    # Base carries a nested ``credentials`` dict so we can prove a nested merge.
    (_route_config_dir / "ovl.yaml").write_text(
        yaml.safe_dump(
            {
                "source_name": "ovl",
                "source_type": "rest_api",
                "base_url": "https://base.example.test",
                "timeout_seconds": 30.0,
                "rate_limit_rps": 1.0,
                "credentials": {"user": "base-user", "key": "base-key"},
            }
        )
    )
    (_route_config_dir / "ovl.prod.yaml").write_text(
        yaml.safe_dump(
            {
                "base_url": "https://prod.example.test",  # scalar replace
                "credentials": {"key": "prod-key"},  # nested-dict merge
            }
        )
    )
    config = load_source_config("ovl")
    assert isinstance(config, RestApiSourceConfig)
    assert config.base_url == "https://prod.example.test"  # overridden
    assert config.timeout_seconds == 30.0  # untouched base scalar survives
    assert config.credentials is not None
    # Nested merge: prod overrides ``key`` but base ``user`` survives.
    assert config.credentials == {"user": "base-user", "key": "prod-key"}


def test_unknown_overlay_field_raises_validation_error(
    _route_config_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """(d) an overlay key the schema doesn't declare fails loud under extra='forbid'."""
    monkeypatch.setenv("RECALLS_ENV", "prod")
    _write_base(_route_config_dir)
    (_route_config_dir / "ovl.prod.yaml").write_text(
        yaml.safe_dump({"totally_unknown_overlay_key": "boom"})
    )
    with pytest.raises(ValidationError) as exc_info:
        load_source_config("ovl")
    assert "totally_unknown_overlay_key" in str(exc_info.value)


def test_overlay_can_switch_to_a_different_source_type(
    _route_config_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A valid overlay that changes discriminated-union fields re-validates correctly.

    Guards that the merged dict — not the base alone — is what gets validated.
    """
    monkeypatch.setenv("RECALLS_ENV", "staging")
    (_route_config_dir / "ovl.yaml").write_text(
        yaml.safe_dump(
            {
                "source_name": "ovl",
                "source_type": "html_scraping",
                "start_url": "https://base.example.test/list",
                "scrape_delay_seconds": 1.0,
            }
        )
    )
    (_route_config_dir / "ovl.staging.yaml").write_text(
        yaml.safe_dump({"scrape_delay_seconds": 5.0})
    )
    config = load_source_config("ovl")
    assert isinstance(config, HtmlScrapingSourceConfig)
    assert config.scrape_delay_seconds == 5.0
