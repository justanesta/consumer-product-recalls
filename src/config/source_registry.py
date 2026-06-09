"""Source-config registry and Pydantic discriminated-union models per ADR 0012.

The CLI loads a YAML file from ``config/sources/<source>.yaml``, validates it
through ``SourceConfig`` (a discriminated union keyed on ``source_type``), and
hands the resulting model + the target extractor class to
``build_extractor_kwargs`` to produce the kwargs dict for the extractor's
constructor.

Two key design choices:

1. **Lightweight registry as static dicts.** ADR 0012 explicitly rejects
   dcpy's heavy ``ConnectorRegistry`` ("we have three operation types and five
   sources; direct Python imports are simpler than runtime dispatch at this
   scale"). The mapping below is exactly that — a 9-entry dict and an 8-entry
   dict, populated at module import time (the ADR's "five sources" referenced
   the original five recall sources; ``usda_establishments`` was added in
   Phase 5b.2, ``uscg_manufacturers`` + ``uscg_manufacturer_details`` in
   Phase 5d Step 7, and ``fda_press_releases`` (the FDA Tier-3 per-event
   press-release source) — all sibling non-recall enrichment sources).
   Tests that need to mock a specific extractor class
   do so via ``patch.dict("src.config.source_registry.
   EXTRACTOR_BY_SOURCE_NAME", {"cpsc": mock_cls})`` rather than patching the
   class symbol in its defining module.

2. **Field-set introspection over hand-coded dispatch.** Each extractor class
   is a Pydantic model exposing ``model_fields``. ``build_extractor_kwargs``
   filters YAML-derived kwargs against that set, so the asymmetry between
   ``UsdaExtractor`` (which has ``etag_enabled``) and ``CpscExtractor`` (which
   does not) is handled automatically without per-class code.

Wire scope (per the Wave 2 plan): only ``base_url`` / ``file_url``,
``timeout_seconds``, ``rate_limit_rps``, ``etag_enabled`` (REST sources only),
and ``historical_seed_urls`` (NHTSA deep-rescan only) flow from YAML to
extractor constructors. The other YAML fields (``incremental_filter_param``,
``incremental_filter_format``, ``default_lookback_days``, ``format_param``,
``credentials``, ``expected_field_count``) are typed-but-unconsumed: they
validate cleanly under ``extra="forbid"`` and travel with the config object,
but are excluded from extractor kwargs. Future waves can wire individual
fields by extending ``to_extractor_kwargs`` and the corresponding extractor
class without YAML schema changes.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from src.extractors.cpsc import CpscDeepRescanLoader, CpscExtractor
from src.extractors.fda import FdaDeepRescanLoader, FdaExtractor
from src.extractors.fda_press_release import (
    FdaPressReleaseDeepRescanLoader,
    FdaPressReleaseExtractor,
)
from src.extractors.nhtsa import NhtsaDeepRescanLoader, NhtsaExtractor
from src.extractors.uscg import UscgDeepRescanLoader, UscgScrapingExtractor
from src.extractors.uscg_manufacturer import (
    UscgManufacturerDeepRescanLoader,
    UscgManufacturerExtractor,
)
from src.extractors.uscg_manufacturer_detail import (
    UscgManufacturerDetailDeepRescanLoader,
    UscgManufacturerDetailExtractor,
)
from src.extractors.usda import UsdaDeepRescanLoader, UsdaExtractor
from src.extractors.usda_establishment import UsdaEstablishmentExtractor

if TYPE_CHECKING:
    from src.config.settings import Settings
    from src.extractors._base import Extractor


# Source-name → extractor class for the routine ``recalls extract <source>``
# command. Direct Python imports per ADR 0012's deliberate rejection of dcpy's
# heavy registry framework.
EXTRACTOR_BY_SOURCE_NAME: dict[str, type[Extractor[Any]]] = {
    "cpsc": CpscExtractor,
    "fda": FdaExtractor,
    "usda": UsdaExtractor,
    "usda_establishments": UsdaEstablishmentExtractor,
    "nhtsa": NhtsaExtractor,
    "uscg": UscgScrapingExtractor,
    "uscg_manufacturers": UscgManufacturerExtractor,
    "uscg_manufacturer_details": UscgManufacturerDetailExtractor,
    "fda_press_releases": FdaPressReleaseExtractor,
}

# Source-name → deep-rescan loader class for ``recalls deep-rescan <source>``.
# Eight of nine sources have a deep-rescan path; only usda_establishments does
# not — it is a full-dump directory, so a deep-rescan is redundant (every
# routine run is already a full snapshot). CpscDeepRescanLoader
# uses a fixed LastPublishDateStart floor and bypasses the incremental-size guard
# (the API has no end-date param). USCG's deep-rescan is symmetry-only — same
# fetches as the incremental path, differs only in not touching freshness; see
# UscgDeepRescanLoader docstring. UscgManufacturerDeepRescanLoader follows
# the same symmetry-only pattern.
DEEP_RESCAN_BY_SOURCE_NAME: dict[str, type[Extractor[Any]]] = {
    "cpsc": CpscDeepRescanLoader,
    "fda": FdaDeepRescanLoader,
    "usda": UsdaDeepRescanLoader,
    "nhtsa": NhtsaDeepRescanLoader,
    "uscg": UscgDeepRescanLoader,
    "uscg_manufacturers": UscgManufacturerDeepRescanLoader,
    "uscg_manufacturer_details": UscgManufacturerDetailDeepRescanLoader,
    "fda_press_releases": FdaPressReleaseDeepRescanLoader,
}


class _BaseSourceConfig(BaseModel):
    """Shared YAML fields across all source types.

    The documented-intent fields (``credentials``, ``default_lookback_days``,
    ``format_param``, ``incremental_filter_param``, ``incremental_filter_format``)
    are typed here so YAML files validate cleanly under ``extra="forbid"`` even
    though no extractor consumes them today. ``to_extractor_kwargs`` excludes
    them from the constructor kwargs.
    """

    model_config = ConfigDict(extra="forbid")

    source_name: str
    rate_limit_rps: float | None = None

    # Documented-intent fields — see module docstring.
    credentials: dict[str, Any] | None = None
    default_lookback_days: int | None = None
    format_param: str | None = None
    incremental_filter_param: str | None = None
    incremental_filter_format: str | None = None


class RestApiSourceConfig(_BaseSourceConfig):
    """Config shape for REST API sources (CPSC, FDA, USDA recall, USDA establishments)."""

    source_type: Literal["rest_api"]
    base_url: str
    timeout_seconds: float = 30.0
    # USDA recall + USDA establishments declare ``etag_enabled`` as a Pydantic
    # field; CPSC/FDA do not. ``build_extractor_kwargs`` filters via
    # ``model_fields`` so passing this only takes effect on extractors that
    # accept it.
    etag_enabled: bool | None = None

    def to_extractor_kwargs(self) -> dict[str, Any]:
        """Return candidate kwargs; non-applicable fields filter out via model_fields."""
        return {
            "base_url": self.base_url,
            "timeout_seconds": self.timeout_seconds,
            "rate_limit_rps": self.rate_limit_rps,
            "etag_enabled": self.etag_enabled,
        }


class FlatFileSourceConfig(_BaseSourceConfig):
    """Config shape for flat-file sources (NHTSA)."""

    source_type: Literal["flat_file"]
    file_url: str
    timeout_seconds: float = 120.0
    # ``historical_seed_urls`` IS wired — consumed by ``NhtsaDeepRescanLoader``.
    # ``expected_field_count`` and ``etag_enabled`` are typed-but-unconsumed
    # (a flat-file extractor field change would be code work anyway).
    historical_seed_urls: list[str] | None = None
    expected_field_count: int | None = None
    etag_enabled: bool | None = None

    def to_extractor_kwargs(self) -> dict[str, Any]:
        return {
            "file_url": self.file_url,
            "timeout_seconds": self.timeout_seconds,
            "rate_limit_rps": self.rate_limit_rps,
            "historical_seed_urls": self.historical_seed_urls,
        }


class HtmlScrapingSourceConfig(_BaseSourceConfig):
    """Config shape for HTML-scraping sources (USCG, Phase 5d Step 2 first use).

    ``start_url`` is the entry point — the listing page for paginated
    scrape sources. ``scrape_delay_seconds`` enforces polite throttling
    via ``HtmlScrapingExtractor._throttle()`` (minimum-inter-request
    interval contract). ``expected_columns`` is the listing-page
    schema-drift fence — see ``UscgScrapingExtractor._parse_listing_page``.
    """

    source_type: Literal["html_scraping"]
    start_url: str
    timeout_seconds: float = 30.0
    scrape_delay_seconds: float = 1.0
    expected_columns: list[str] | None = None

    def to_extractor_kwargs(self) -> dict[str, Any]:
        return {
            "start_url": self.start_url,
            "timeout_seconds": self.timeout_seconds,
            "scrape_delay_seconds": self.scrape_delay_seconds,
            "expected_columns": self.expected_columns,
            "rate_limit_rps": self.rate_limit_rps,
        }


SourceConfig = Annotated[
    RestApiSourceConfig | FlatFileSourceConfig | HtmlScrapingSourceConfig,
    Field(discriminator="source_type"),
]


def build_extractor_kwargs(
    config: RestApiSourceConfig | FlatFileSourceConfig | HtmlScrapingSourceConfig,
    extractor_cls: type[Extractor[Any]],
    settings: Settings,
    *,
    exclude: frozenset[str] = frozenset(),
) -> dict[str, Any]:
    """Build the kwargs dict for ``extractor_cls(**kwargs)``.

    Filters the YAML-derived candidate kwargs against ``extractor_cls.model_fields``
    so each constructor receives only fields it declares. Drops ``None`` values
    so class defaults take effect when YAML doesn't specify a value.

    ``exclude`` lets callers force-drop fields even if the class accepts them —
    used by the deep-rescan path to preserve ``UsdaDeepRescanLoader.etag_enabled
    = False`` against a YAML ``etag_enabled: true`` (correct for the routine
    path but invariant-violating for deep-rescan).
    """
    candidate = config.to_extractor_kwargs()
    accepted = set(extractor_cls.model_fields.keys())
    kwargs: dict[str, Any] = {
        k: v for k, v in candidate.items() if k in accepted and v is not None and k not in exclude
    }
    kwargs["settings"] = settings
    return kwargs
