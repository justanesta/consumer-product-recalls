"""Unit tests for the discriminated-union source-config models per ADR 0012.

Covers:
- Discriminator dispatch (rest_api → RestApiSourceConfig, flat_file → FlatFileSourceConfig).
- ``extra="forbid"`` strictness.
- ``build_extractor_kwargs`` field-set introspection across all 5 extractor classes.
- The deep-rescan ``exclude`` parameter for the etag_enabled invariant.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest
from pydantic import TypeAdapter, ValidationError

from src.config.source_registry import (
    DEEP_RESCAN_BY_SOURCE_NAME,
    EXTRACTOR_BY_SOURCE_NAME,
    FlatFileSourceConfig,
    RestApiSourceConfig,
    SourceConfig,
    build_extractor_kwargs,
)
from src.extractors.cpsc import CpscExtractor
from src.extractors.fda import FdaDeepRescanLoader, FdaExtractor
from src.extractors.nhtsa import NhtsaDeepRescanLoader, NhtsaExtractor
from src.extractors.usda import UsdaDeepRescanLoader, UsdaExtractor
from src.extractors.usda_establishment import UsdaEstablishmentExtractor

_ADAPTER: TypeAdapter[SourceConfig] = TypeAdapter(SourceConfig)


def _stub_settings() -> Any:
    """A MagicMock standing in for Settings — build_extractor_kwargs only stuffs
    it into the kwargs dict, so the test doesn't need a real instance."""
    return MagicMock(name="Settings")


# --- Discriminator dispatch ---


def test_rest_api_discriminator_returns_rest_api_config() -> None:
    raw = {
        "source_name": "cpsc",
        "source_type": "rest_api",
        "base_url": "https://example.test",
    }
    config = _ADAPTER.validate_python(raw)
    assert isinstance(config, RestApiSourceConfig)
    assert config.source_type == "rest_api"
    assert config.timeout_seconds == 30.0  # default for REST


def test_flat_file_discriminator_returns_flat_file_config() -> None:
    raw = {
        "source_name": "nhtsa",
        "source_type": "flat_file",
        "file_url": "https://example.test/file.zip",
    }
    config = _ADAPTER.validate_python(raw)
    assert isinstance(config, FlatFileSourceConfig)
    assert config.source_type == "flat_file"
    assert config.timeout_seconds == 120.0  # default for flat-file (longer)


def test_unknown_source_type_raises() -> None:
    raw = {"source_name": "x", "source_type": "html_scraping", "base_url": "x"}
    with pytest.raises(ValidationError):
        _ADAPTER.validate_python(raw)


def test_extra_field_rejected_under_forbid() -> None:
    raw = {
        "source_name": "cpsc",
        "source_type": "rest_api",
        "base_url": "https://example.test",
        "unknown_field": "should_fail",
    }
    with pytest.raises(ValidationError) as exc_info:
        _ADAPTER.validate_python(raw)
    assert "unknown_field" in str(exc_info.value)


def test_missing_required_base_url_rejected() -> None:
    raw = {"source_name": "cpsc", "source_type": "rest_api"}
    with pytest.raises(ValidationError) as exc_info:
        _ADAPTER.validate_python(raw)
    assert "base_url" in str(exc_info.value)


def test_missing_required_file_url_rejected() -> None:
    raw = {"source_name": "nhtsa", "source_type": "flat_file"}
    with pytest.raises(ValidationError) as exc_info:
        _ADAPTER.validate_python(raw)
    assert "file_url" in str(exc_info.value)


# --- Documented-intent fields are accepted but typed ---


def test_documented_intent_fields_accepted_on_rest_config() -> None:
    """incremental_filter_param, default_lookback_days, etc. validate cleanly
    even though the loader excludes them from extractor kwargs."""
    raw = {
        "source_name": "cpsc",
        "source_type": "rest_api",
        "base_url": "https://example.test",
        "incremental_filter_param": "LastPublishDateStart",
        "incremental_filter_format": "%Y-%m-%d",
        "default_lookback_days": 1,
        "format_param": "json",
        "credentials": None,
    }
    config = _ADAPTER.validate_python(raw)
    assert isinstance(config, RestApiSourceConfig)
    assert config.incremental_filter_param == "LastPublishDateStart"
    assert config.default_lookback_days == 1


# --- Registry dicts ---


def test_extractor_registry_covers_all_eight_sources() -> None:
    # Eight sources after Phase 5d Step 7 detail lands uscg_manufacturer_details
    # (2026-05-30). Prior count was seven after the listing source landed; when a
    # ninth lands, bump this test and the deep-rescan companion together.
    from src.extractors.uscg import UscgScrapingExtractor
    from src.extractors.uscg_manufacturer import UscgManufacturerExtractor
    from src.extractors.uscg_manufacturer_detail import UscgManufacturerDetailExtractor

    assert set(EXTRACTOR_BY_SOURCE_NAME.keys()) == {
        "cpsc",
        "fda",
        "usda",
        "usda_establishments",
        "nhtsa",
        "uscg",
        "uscg_manufacturers",
        "uscg_manufacturer_details",
    }
    assert EXTRACTOR_BY_SOURCE_NAME["cpsc"] is CpscExtractor
    assert EXTRACTOR_BY_SOURCE_NAME["fda"] is FdaExtractor
    assert EXTRACTOR_BY_SOURCE_NAME["usda"] is UsdaExtractor
    assert EXTRACTOR_BY_SOURCE_NAME["usda_establishments"] is UsdaEstablishmentExtractor
    assert EXTRACTOR_BY_SOURCE_NAME["nhtsa"] is NhtsaExtractor
    assert EXTRACTOR_BY_SOURCE_NAME["uscg"] is UscgScrapingExtractor
    assert EXTRACTOR_BY_SOURCE_NAME["uscg_manufacturers"] is UscgManufacturerExtractor
    assert EXTRACTOR_BY_SOURCE_NAME["uscg_manufacturer_details"] is UscgManufacturerDetailExtractor


def test_deep_rescan_registry_covers_six_sources() -> None:
    # Six after Phase 5d Step 7 detail lands uscg_manufacturer_details (2026-05-30).
    # All three USCG sources have deep-rescan loaders; the manufacturer-detail one
    # is a full ~16.3k-row sweep (Tier-2) vs the incremental listing-delta path.
    # CPSC and USDA establishments have no deep-rescan path.
    from src.extractors.uscg import UscgDeepRescanLoader
    from src.extractors.uscg_manufacturer import UscgManufacturerDeepRescanLoader
    from src.extractors.uscg_manufacturer_detail import UscgManufacturerDetailDeepRescanLoader

    assert set(DEEP_RESCAN_BY_SOURCE_NAME.keys()) == {
        "fda",
        "usda",
        "nhtsa",
        "uscg",
        "uscg_manufacturers",
        "uscg_manufacturer_details",
    }
    assert DEEP_RESCAN_BY_SOURCE_NAME["fda"] is FdaDeepRescanLoader
    assert DEEP_RESCAN_BY_SOURCE_NAME["usda"] is UsdaDeepRescanLoader
    assert DEEP_RESCAN_BY_SOURCE_NAME["nhtsa"] is NhtsaDeepRescanLoader
    assert DEEP_RESCAN_BY_SOURCE_NAME["uscg"] is UscgDeepRescanLoader
    assert DEEP_RESCAN_BY_SOURCE_NAME["uscg_manufacturers"] is UscgManufacturerDeepRescanLoader
    assert (
        DEEP_RESCAN_BY_SOURCE_NAME["uscg_manufacturer_details"]
        is UscgManufacturerDetailDeepRescanLoader
    )


def test_html_scraping_to_extractor_kwargs_returns_full_shape() -> None:
    """``HtmlScrapingSourceConfig.to_extractor_kwargs`` returns the kwargs
    dict the registry consumes during extractor instantiation. Mirrors the
    REST + flat-file variants and ensures every YAML-declared field reaches
    the extractor (or is filtered out by ``build_extractor_kwargs`` if the
    extractor class doesn't declare it)."""
    from src.config.source_registry import HtmlScrapingSourceConfig

    config = HtmlScrapingSourceConfig(
        source_name="uscg",
        source_type="html_scraping",
        start_url="https://example.invalid/listing",
        timeout_seconds=45.0,
        scrape_delay_seconds=2.0,
        expected_columns=["A", "B", "C"],
        rate_limit_rps=0.5,
    )
    kwargs = config.to_extractor_kwargs()
    assert kwargs == {
        "start_url": "https://example.invalid/listing",
        "timeout_seconds": 45.0,
        "scrape_delay_seconds": 2.0,
        "expected_columns": ["A", "B", "C"],
        "rate_limit_rps": 0.5,
    }


def test_extractor_registry_excludes_deep_rescan_only_sources() -> None:
    """CPSC and USDA establishments don't have a deep-rescan path."""
    assert "cpsc" not in DEEP_RESCAN_BY_SOURCE_NAME
    assert "usda_establishments" not in DEEP_RESCAN_BY_SOURCE_NAME


# --- build_extractor_kwargs introspection ---


def test_build_kwargs_filters_etag_enabled_for_cpsc() -> None:
    """CpscExtractor doesn't declare etag_enabled. YAML's etag_enabled (None
    here) should be filtered out so it never reaches the constructor."""
    config = RestApiSourceConfig(
        source_name="cpsc",
        source_type="rest_api",
        base_url="https://example.test",
        timeout_seconds=30.0,
        etag_enabled=None,
    )
    settings = _stub_settings()
    kwargs = build_extractor_kwargs(config, CpscExtractor, settings)
    assert "etag_enabled" not in kwargs
    assert kwargs["base_url"] == "https://example.test"
    assert kwargs["timeout_seconds"] == 30.0
    assert kwargs["settings"] is settings


def test_build_kwargs_passes_etag_enabled_for_usda() -> None:
    """UsdaExtractor.etag_enabled is a real Pydantic field; YAML value flows
    through."""
    config = RestApiSourceConfig(
        source_name="usda",
        source_type="rest_api",
        base_url="https://example.test/recall",
        timeout_seconds=60.0,
        etag_enabled=True,
    )
    kwargs = build_extractor_kwargs(config, UsdaExtractor, _stub_settings())
    assert kwargs["etag_enabled"] is True
    assert kwargs["timeout_seconds"] == 60.0


def test_build_kwargs_drops_none_values_so_class_defaults_apply() -> None:
    """rate_limit_rps=None should not enter kwargs — let the extractor's class
    default of None take effect (and avoid passing an explicit None that would
    be indistinguishable from 'not set')."""
    config = RestApiSourceConfig(
        source_name="cpsc",
        source_type="rest_api",
        base_url="https://example.test",
        rate_limit_rps=None,
    )
    kwargs = build_extractor_kwargs(config, CpscExtractor, _stub_settings())
    assert "rate_limit_rps" not in kwargs


def test_build_kwargs_excludes_documented_intent_fields() -> None:
    """incremental_filter_param etc. are typed on the config but not in any
    extractor's model_fields — they should never reach the constructor."""
    config = RestApiSourceConfig(
        source_name="cpsc",
        source_type="rest_api",
        base_url="https://example.test",
        incremental_filter_param="LastPublishDateStart",
        default_lookback_days=1,
        format_param="json",
    )
    kwargs = build_extractor_kwargs(config, CpscExtractor, _stub_settings())
    for key in ("incremental_filter_param", "default_lookback_days", "format_param"):
        assert key not in kwargs


def test_build_kwargs_for_flat_file_passes_file_url() -> None:
    config = FlatFileSourceConfig(
        source_name="nhtsa",
        source_type="flat_file",
        file_url="https://example.test/data.zip",
        timeout_seconds=120.0,
    )
    kwargs = build_extractor_kwargs(config, NhtsaExtractor, _stub_settings())
    assert kwargs["file_url"] == "https://example.test/data.zip"
    assert kwargs["timeout_seconds"] == 120.0


def test_build_kwargs_exclude_drops_etag_enabled_for_deep_rescan_invariant() -> None:
    """UsdaDeepRescanLoader.etag_enabled = False is a class invariant. YAML's
    etag_enabled: true (correct for the routine path) must NOT override it on
    the deep-rescan path."""
    config = RestApiSourceConfig(
        source_name="usda",
        source_type="rest_api",
        base_url="https://example.test/recall",
        etag_enabled=True,
    )
    kwargs = build_extractor_kwargs(
        config,
        UsdaDeepRescanLoader,
        _stub_settings(),
        exclude=frozenset({"etag_enabled"}),
    )
    assert "etag_enabled" not in kwargs


def test_build_kwargs_passes_historical_seed_urls_for_nhtsa_deep_rescan() -> None:
    config = FlatFileSourceConfig(
        source_name="nhtsa",
        source_type="flat_file",
        file_url="https://example.test/post.zip",
        historical_seed_urls=["https://example.test/pre.zip"],
    )
    kwargs = build_extractor_kwargs(config, NhtsaDeepRescanLoader, _stub_settings())
    assert kwargs["historical_seed_urls"] == ["https://example.test/pre.zip"]


def test_build_kwargs_omits_historical_seed_urls_for_routine_nhtsa() -> None:
    """NhtsaExtractor (routine path) doesn't declare historical_seed_urls —
    only the deep-rescan loader does."""
    config = FlatFileSourceConfig(
        source_name="nhtsa",
        source_type="flat_file",
        file_url="https://example.test/post.zip",
        historical_seed_urls=["https://example.test/pre.zip"],
    )
    kwargs = build_extractor_kwargs(config, NhtsaExtractor, _stub_settings())
    assert "historical_seed_urls" not in kwargs
