"""
VCR integration tests for UsdaEstablishmentExtractor.

Live-recorded cassettes replay real FSIS Establishment Listing API responses
and verify the Pydantic schema handles the actual API shape. Hand-constructed
tests (patched _fetch / side effects) cover code paths the live API won't
produce on demand.

USDA establishment-specific VCR note: same shape as the recall extractor — no
filter_query_parameters or filter_headers override needed. The endpoint is
unauthenticated (no auth headers, no signature param) and full-dump-only (no
pagination). ETag conditional-GET is scaffolded but disabled by default
(``etag_enabled=False``) per Finding A revision 2026-05-03 — the API DOES
emit ETag/Last-Modified under browser fingerprint, viability under study.

Cassette inventory:
  Live-recorded (real FSIS responses):
    test_happy_path_full_dump.yaml  — full ~7,945-record GET to /v/1 (Finding B)

  Hand-constructed (patched _fetch, no YAML):
    test_content_hash_dedup            — repeated full-dump → loader returns 0
    test_not_modified_304              — 304 → _not_modified path, _touch_freshness
    test_etag_contradiction_guard      — 304 with advanced last-modified → ExtractionError
    test_transient_500                 — 5xx → TransientExtractionError
    test_rate_limit_429                — HTTP 429 → RateLimitError
    test_malformed_record              — extra forbidden field → quarantine
    test_oversized_response_guard      — >_MAX_TOTAL_RECORDS → guard fires

The full-dump nature of the API (Finding A) means there is no meaningful
"quoted-name-filter" cassette at the bronze layer — that lookup pattern is
silver-side via dbt staging joins, not an extractor call. The original
implementation_plan.md text predates that scoping decision; the single-cassette
shape here is correct.

To record live cassettes (requires network access; clean IP for Akamai):
    uv run pytest --vcr-record=all \\
        tests/integration/test_usda_establishments_live_cassettes.py \\
        -k "happy_path_full_dump"

Commit the generated YAML under tests/fixtures/cassettes/usda_establishments/.
Until cassettes are recorded, live tests skip automatically.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
import sqlalchemy as sa

from src.config.settings import Settings
from src.extractors._base import ExtractionError, RateLimitError, TransientExtractionError
from src.extractors.usda_establishment import (
    _MAX_TOTAL_RECORDS,
    UsdaEstablishmentExtractor,
)

_BASE_URL = "https://www.fsis.usda.gov/fsis/api/establishments/v/1"
_FAKE_R2_PATH = "usda_establishments/cassette-test/placeholder.json"

_REQUIRED_ENV = {
    "NEON_DATABASE_URL": "postgresql://user:pass@localhost/test",
    "R2_ACCOUNT_ID": "test-account",
    "R2_ACCESS_KEY_ID": "test-key-id",
    "R2_SECRET_ACCESS_KEY": "test-secret",
    "R2_BUCKET_NAME": "test-bucket",
}

_VALID_RAW: dict[str, Any] = {
    "establishment_id": "6163082",
    "establishment_name": "CS Beef Packers, LLC",
    "establishment_number": "M630",
    "address": "123 Main St",
    "city": "Kuna",
    "state": "ID",
    "zip": "83634",
    "LatestMPIActiveDate": "2026-04-27",
    "status_regulated_est": "",
    "activities": ["Meat Processing"],
    "dbas": [],
}


@pytest.fixture(scope="module")
def vcr_cassette_dir() -> str:
    return str(Path(__file__).parent.parent / "fixtures" / "cassettes" / "usda_establishments")


@pytest.fixture(autouse=True)
def skip_if_no_cassette(request: pytest.FixtureRequest, vcr_cassette_dir: str) -> None:
    marker = request.node.get_closest_marker("vcr")
    if not marker:
        return
    record_mode = request.config.getoption("--vcr-record", default="none")
    if record_mode in ("all", "new_episodes"):
        return
    cassette_path = Path(vcr_cassette_dir) / (request.node.name + ".yaml")
    if not cassette_path.exists():
        pytest.skip(
            "Cassette not yet recorded — run: "
            "uv run pytest --vcr-record=all "
            "tests/integration/test_usda_establishments_live_cassettes.py "
            "-k 'happy_path_full_dump'"
        )


@pytest.fixture
def vcr_extractor(monkeypatch: pytest.MonkeyPatch) -> UsdaEstablishmentExtractor:
    for k, v in _REQUIRED_ENV.items():
        monkeypatch.setenv(k, v)
    mock_engine = MagicMock(spec=sa.Engine)
    mock_r2 = MagicMock()
    mock_r2.land.return_value = _FAKE_R2_PATH
    with (
        patch("sqlalchemy.create_engine", return_value=mock_engine),
        patch("src.extractors.usda_establishment.R2LandingClient", return_value=mock_r2),
    ):
        settings = Settings()  # type: ignore[call-arg]
        return UsdaEstablishmentExtractor(base_url=_BASE_URL, settings=settings)


def _run(
    extractor: UsdaEstablishmentExtractor,
    prior_etag: str | None = None,
    prior_last_modified: str | None = None,
) -> Any:
    """Run the extractor with DB/R2 mocked; HTTP goes through VCR (or a patched _fetch).

    ``prior_etag`` / ``prior_last_modified`` patch ``_read_etag_state`` so ETag
    tests can simulate a populated source_watermarks row. Default None values
    keep all existing tests behaving as before (cold cache).
    """
    with (
        patch.object(extractor, "_read_etag_state", return_value=(prior_etag, prior_last_modified)),
        patch("src.extractors.usda_establishment.BronzeLoader") as mock_loader_cls,
    ):
        mock_loader_cls.return_value.load.return_value = 0
        mock_engine: MagicMock = extractor._engine  # type: ignore[assignment]
        mock_engine.begin.return_value.__enter__ = lambda _: MagicMock()
        mock_engine.begin.return_value.__exit__ = MagicMock(return_value=False)
        return extractor.run()


# ---------------------------------------------------------------------------
# Scenario 1: Happy path, full dump — no query params, ~7,945 records (Finding B)
# Validates real API response shape: city present (Finding D blind spot
# resolved 2026-05-01), false-sentinel on geolocation/county (Finding C),
# array-whitespace on activities/dbas (Finding C), latest_mpi_active_date 100%
# populated (Finding G), status_regulated_est exhaustively '' or 'Inactive'.
# ---------------------------------------------------------------------------


@pytest.mark.vcr
def test_happy_path_full_dump(vcr_extractor: UsdaEstablishmentExtractor) -> None:
    result = _run(vcr_extractor)
    assert result.records_fetched > 0
    # Sanity guard — dataset shouldn't exceed _MAX_TOTAL_RECORDS=20_000.
    # If it does, something upstream changed shape and the count guard would
    # fire before we got here.
    assert result.records_fetched <= _MAX_TOTAL_RECORDS
    # Phase 5b.2 first extraction (2026-05-01) confirmed 7,945 records;
    # leave headroom for organic growth.
    assert result.records_fetched >= 7_500
    assert result.records_rejected_validate == 0
    assert result.records_rejected_invariants == 0
    assert result.rejection_rate == 0.0


# ---------------------------------------------------------------------------
# Scenario 2: Content-hash dedup — patched _fetch returns the same payload
# twice; loader returns 0 on the second call to confirm idempotency (ADR 0007).
# Hand-constructed rather than cassette-driven because it's a loader behavior
# check, not an API contract check.
# ---------------------------------------------------------------------------


def test_content_hash_dedup(vcr_extractor: UsdaEstablishmentExtractor) -> None:
    payload = [_VALID_RAW]
    with patch.object(vcr_extractor, "_fetch", return_value=(payload, 200, None, None)):
        result = _run(vcr_extractor)
    assert result.records_fetched == 1
    # The loader is mocked to return 0 by _run; this asserts the wiring, not
    # real dedup. Real dedup is covered by tests/bronze/test_loader.py.
    assert result.records_loaded == 0


# ---------------------------------------------------------------------------
# Scenario 3: 304 Not Modified — ETag matches, server short-circuits download
# Mirrors test_not_modified_304 in test_usda_live_cassettes.py — keep in sync.
# Exercises the _not_modified=True lifecycle: land_raw skips R2, load_bronze
# calls _touch_freshness (not loader.load). Establishment-side viability of
# this path is gated on etag_viability.sql per implementation_plan.md
# § "USDA establishment ETag enablement".
# ---------------------------------------------------------------------------


def test_not_modified_304(vcr_extractor: UsdaEstablishmentExtractor) -> None:
    _ETAG = '"1777668683"'
    _LAST_MODIFIED = "Fri, 01 May 2026 20:51:23 GMT"
    with patch.object(
        vcr_extractor,
        "_fetch",
        return_value=([], 304, _ETAG, _LAST_MODIFIED),
    ):
        # Same last-modified in prior state and 304 response → no contradiction
        result = _run(vcr_extractor, prior_etag=_ETAG, prior_last_modified=_LAST_MODIFIED)

    assert result.records_fetched == 0
    assert result.records_loaded == 0
    assert result.rejection_rate == 0.0
    # R2 land() must not be called when there is no new data to land
    assert vcr_extractor._r2_client.land.call_count == 0  # type: ignore[attr-defined]


# ---------------------------------------------------------------------------
# Scenario 4: ETag contradiction guard — 304 paired with advanced last-modified
# Mirrors test_etag_contradiction_guard in test_usda_live_cassettes.py — keep in
# sync. The CDN returns a stale-positive 304: the ETag matched but the dataset
# actually changed (last-modified advanced). ExtractionError requires manual
# watermark repair (NULL source_watermarks.last_etag for usda_establishments
# and re-run).
# ---------------------------------------------------------------------------


def test_etag_contradiction_guard(vcr_extractor: UsdaEstablishmentExtractor) -> None:
    _ETAG = '"stale_etag"'
    _PRIOR_LM = "Wed, 29 Apr 2026 14:29:36 GMT"
    _CURRENT_LM = "Thu, 30 Apr 2026 00:00:00 GMT"  # advanced → contradiction
    with (
        patch.object(
            vcr_extractor,
            "_fetch",
            return_value=([], 304, _ETAG, _CURRENT_LM),
        ),
        pytest.raises(ExtractionError),
    ):
        _run(vcr_extractor, prior_etag=_ETAG, prior_last_modified=_PRIOR_LM)


# ---------------------------------------------------------------------------
# Scenario 5: Transient 5xx → TransientExtractionError (retried by base class)
# ---------------------------------------------------------------------------


def test_transient_500(vcr_extractor: UsdaEstablishmentExtractor) -> None:
    with (
        patch("time.sleep"),  # stub tenacity backoff so retries are instant
        patch.object(
            vcr_extractor,
            "_fetch",
            side_effect=TransientExtractionError("upstream 503"),
        ),
        pytest.raises(TransientExtractionError),
    ):
        _run(vcr_extractor)


# ---------------------------------------------------------------------------
# Scenario 4: Rate limit 429 → RateLimitError
# ---------------------------------------------------------------------------


def test_rate_limit_429(vcr_extractor: UsdaEstablishmentExtractor) -> None:
    with (
        patch("time.sleep"),  # stub tenacity backoff so retries are instant
        patch.object(
            vcr_extractor,
            "_fetch",
            side_effect=RateLimitError(retry_after=60.0),
        ),
        pytest.raises(RateLimitError) as exc_info,
    ):
        _run(vcr_extractor)
    assert exc_info.value.retry_after == 60.0


# ---------------------------------------------------------------------------
# Scenario 5: Malformed record — extra forbidden field routes to quarantine.
# Mirrors the city-bug surface from Phase 5b.2 first extraction.
# ---------------------------------------------------------------------------


def test_malformed_record(vcr_extractor: UsdaEstablishmentExtractor) -> None:
    # Mix one bad record with 20 good ones so the rejection rate (1/21 ≈ 4.8%)
    # stays under the default 5% threshold and the run completes normally.
    # Otherwise ExtractionAbortedError fires before we can assert on the result.
    bad_record = {**_VALID_RAW, "unexpected_field": "boom"}
    payload = [_VALID_RAW] * 20 + [bad_record]
    with patch.object(vcr_extractor, "_fetch", return_value=(payload, 200, None, None)):
        result = _run(vcr_extractor)
    assert result.records_fetched == 21
    assert result.records_valid == 20
    assert result.records_rejected_validate == 1


# ---------------------------------------------------------------------------
# Scenario 6: Oversized response — count guard fires above _MAX_TOTAL_RECORDS.
# Catches an upstream shape change (e.g., a sibling endpoint silently merging
# in or duplication of records).
# ---------------------------------------------------------------------------


def test_oversized_response_guard(vcr_extractor: UsdaEstablishmentExtractor) -> None:
    oversized = [_VALID_RAW] * (_MAX_TOTAL_RECORDS + 1)
    # the guard raises TransientExtractionError, which _TRANSIENT_RETRY retries
    # 5x with backoff — patch time.sleep so the retries are instant.
    with (
        patch("time.sleep"),
        patch.object(vcr_extractor, "_fetch", return_value=(oversized, 200, None, None)),
        pytest.raises(TransientExtractionError, match="exceeds guard"),
    ):
        _run(vcr_extractor)
