"""
VCR integration tests for UsdaExtractor.

Live-recorded cassettes replay real USDA FSIS API responses and verify the
Pydantic schema handles the actual API shape. Hand-constructed tests (patched
_fetch / side effects) cover code paths the live API won't produce on demand.

USDA-specific VCR note: no filter_query_parameters or filter_headers override
is needed. USDA has no auth credentials (unauthenticated public API) and no
cache-busting signature param (unlike FDA). The weekly-rotating Firefox UA in
cassette request headers is harmless — VCR matches on URI/method/body, not headers.

Cassette inventory:
  Live-recorded (real FSIS responses):
    test_happy_path_full_dump.yaml  — full ~2,001-record no-filter GET (Finding B)
    test_content_hash_dedup.yaml    — same GET, loader returns 0 (idempotency)

  Hand-constructed (patched _fetch, no YAML):
    test_not_modified_304           — 304 → _not_modified path, _touch_freshness
    test_etag_contradiction_guard   — 304 with advanced last-modified → ExtractionError
    test_transient_500              — 5xx / network → TransientExtractionError
    test_rate_limit_429             — HTTP 429 → RateLimitError with retry_after
    test_malformed_record           — forbidden extra field → quarantine routing
    test_oversized_response_guard   — >5,000 records → TransientExtractionError guard
    test_bilingual_orphan_quarantine— Spanish record without English sibling → invariant reject

The full-dump nature of USDA's API (Finding D) means all happy-path request
variations collapse to one shape — unlike CPSC's three date-window cassettes or
FDA's single-page cassette. Two live cassettes are the complete necessary set;
no post-recording trimming required.

To record live cassettes (requires network access; ensure a clean IP to avoid
Akamai bot-reputation degradation — Finding O in
documentation/usda/recall_api_observations.md):
    uv run pytest --vcr-record=all tests/integration/test_usda_live_cassettes.py \\
        -k "happy_path_full_dump or content_hash_dedup"

Commit the generated YAML files under tests/fixtures/cassettes/usda/.
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
from src.extractors.usda import _MAX_INCREMENTAL_RECORDS, UsdaExtractor

_BASE_URL = "https://www.fsis.usda.gov/fsis/api/recall/v/1"
_FAKE_R2_PATH = "usda/cassette-test/placeholder.json"

_REQUIRED_ENV = {
    "NEON_DATABASE_URL": "postgresql://user:pass@localhost/test",
    "R2_ACCOUNT_ID": "test-account",
    "R2_ACCESS_KEY_ID": "test-key-id",
    "R2_SECRET_ACCESS_KEY": "test-secret",
    "R2_BUCKET_NAME": "test-bucket",
}


@pytest.fixture(scope="module")
def vcr_cassette_dir() -> str:
    return str(Path(__file__).parent.parent / "fixtures" / "cassettes" / "usda")


@pytest.fixture(autouse=True)
def skip_if_no_cassette(request: pytest.FixtureRequest, vcr_cassette_dir: str) -> None:
    # Only applies to @pytest.mark.vcr tests — hand-constructed tests run without cassettes.
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
            "uv run pytest --vcr-record=all tests/integration/test_usda_live_cassettes.py "
            "-k 'happy_path_full_dump or content_hash_dedup'"
        )


@pytest.fixture
def vcr_extractor(monkeypatch: pytest.MonkeyPatch) -> UsdaExtractor:
    for k, v in _REQUIRED_ENV.items():
        monkeypatch.setenv(k, v)
    mock_engine = MagicMock(spec=sa.Engine)
    mock_r2 = MagicMock()
    mock_r2.land.return_value = _FAKE_R2_PATH
    with (
        patch("sqlalchemy.create_engine", return_value=mock_engine),
        patch("src.extractors.usda.R2LandingClient", return_value=mock_r2),
    ):
        settings = Settings()  # type: ignore[call-arg]
        return UsdaExtractor(base_url=_BASE_URL, settings=settings)


def _run(
    extractor: UsdaExtractor,
    prior_etag: str | None = None,
    prior_last_modified: str | None = None,
) -> Any:
    """Run the extractor with DB/R2 mocked; HTTP goes through VCR (or a patched _fetch)."""
    with (
        patch.object(extractor, "_read_etag_state", return_value=(prior_etag, prior_last_modified)),
        patch("src.extractors.usda.BronzeLoader") as mock_loader_cls,
    ):
        mock_loader_cls.return_value.load.return_value = 0
        mock_engine: MagicMock = extractor._engine  # type: ignore[assignment]
        mock_engine.begin.return_value.__enter__ = lambda _: MagicMock()
        mock_engine.begin.return_value.__exit__ = MagicMock(return_value=False)
        return extractor.run()


# ---------------------------------------------------------------------------
# Scenario 1: Happy path, full dump — no query params, ~2,001 records (Finding B)
# Validates real API response shape: boolean-string coercion (Finding L), Optional
# fields (Finding C), bilingual pairs (Finding F), field_active_notice nullable
# (Finding C addendum), field_recall_url undocumented field (Finding H),
# PHA-format recall numbers (Finding I).
# ---------------------------------------------------------------------------


@pytest.mark.vcr
def test_happy_path_full_dump(vcr_extractor: UsdaExtractor) -> None:
    result = _run(vcr_extractor)
    assert result.records_fetched > 0
    assert result.records_fetched <= 2500  # sanity guard — dataset should not have exploded
    assert result.records_rejected_validate == 0
    assert result.records_rejected_invariants == 0
    assert result.rejection_rate == 0.0


# ---------------------------------------------------------------------------
# Scenario 2: Content-hash dedup — same full-dump GET, loader returns 0
# Confirms records_loaded == 0 when all records are already present in bronze
# (ADR 0007 idempotency). Records its own cassette for independent auditability.
# ---------------------------------------------------------------------------


@pytest.mark.vcr
def test_content_hash_dedup(vcr_extractor: UsdaExtractor) -> None:
    result = _run(vcr_extractor)
    assert result.records_fetched > 0
    assert result.records_loaded == 0


# ---------------------------------------------------------------------------
# Scenario 3: 304 Not Modified — ETag matches, server short-circuits download
# Exercises the _not_modified=True lifecycle: land_raw skips R2, load_bronze
# calls _touch_freshness (not loader.load). Finding N / Finding N addendum.
# ---------------------------------------------------------------------------


def test_not_modified_304(vcr_extractor: UsdaExtractor) -> None:
    _ETAG = '"1777596670"'
    _LAST_MODIFIED = "Wed, 29 Apr 2026 14:29:36 GMT"
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
# The CDN returns a stale-positive 304: the ETag matched but the dataset actually
# changed (last-modified advanced). ExtractionError requires manual watermark
# repair (NULL source_watermarks.last_etag and re-run). Finding N addendum.
# ---------------------------------------------------------------------------


def test_etag_contradiction_guard(vcr_extractor: UsdaExtractor) -> None:
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
# Scenario 5: Transient 500 — propagates as TransientExtractionError
# ---------------------------------------------------------------------------


def test_transient_500(vcr_extractor: UsdaExtractor) -> None:
    with (
        patch("time.sleep"),
        patch.object(vcr_extractor, "_fetch", side_effect=TransientExtractionError("HTTP 500")),
        pytest.raises(TransientExtractionError),
    ):
        _run(vcr_extractor)


# ---------------------------------------------------------------------------
# Scenario 6: Rate limit 429 — propagates as RateLimitError with retry_after
# ---------------------------------------------------------------------------


def test_rate_limit_429(vcr_extractor: UsdaExtractor) -> None:
    with (
        patch("time.sleep"),
        patch.object(vcr_extractor, "_fetch", side_effect=RateLimitError(retry_after=60.0)),
        pytest.raises(RateLimitError) as exc_info,
    ):
        _run(vcr_extractor)
    assert exc_info.value.retry_after == 60.0


# ---------------------------------------------------------------------------
# Scenario 7: Malformed record — extra forbidden field → quarantined row
# Uses rejection_threshold=1.0 so the 50% rate (1 of 2 rows bad) doesn't abort.
# This test is about quarantine routing, not the threshold abort path.
# ---------------------------------------------------------------------------


def _valid_usda_row() -> dict[str, Any]:
    return {
        "field_recall_number": "001-2020",
        "langcode": "English",
        "field_title": "Test Recall",
        "field_recall_date": "2020-01-15",
        "field_recall_type": "Active Recall",
        "field_recall_classification": "Class I",
        "field_archive_recall": "False",
        "field_has_spanish": "False",
    }


def test_malformed_record(monkeypatch: pytest.MonkeyPatch) -> None:
    for k, v in _REQUIRED_ENV.items():
        monkeypatch.setenv(k, v)
    mock_engine = MagicMock(spec=sa.Engine)
    mock_r2 = MagicMock()
    mock_r2.land.return_value = _FAKE_R2_PATH
    with (
        patch("sqlalchemy.create_engine", return_value=mock_engine),
        patch("src.extractors.usda.R2LandingClient", return_value=mock_r2),
    ):
        settings = Settings()  # type: ignore[call-arg]
        extractor = UsdaExtractor(base_url=_BASE_URL, settings=settings, rejection_threshold=1.0)

    malformed_row = _valid_usda_row()
    malformed_row["UNEXPECTED_EXTRA_FIELD"] = "forbidden"  # extra="forbid" rejects this

    valid_row = _valid_usda_row()
    valid_row["field_recall_number"] = "002-2020"

    mock_engine.begin.return_value.__enter__ = lambda _: MagicMock()
    mock_engine.begin.return_value.__exit__ = MagicMock(return_value=False)

    with (
        patch.object(extractor, "_read_etag_state", return_value=(None, None)),
        patch.object(
            extractor, "_fetch", return_value=([valid_row, malformed_row], 200, None, None)
        ),
        patch("src.extractors.usda.BronzeLoader") as mock_loader_cls,
    ):
        mock_loader_cls.return_value.load.return_value = 1
        result = extractor.run()

    assert result.records_fetched == 2
    assert result.records_rejected_validate == 1
    load_call = mock_loader_cls.return_value.load.call_args
    quarantined = load_call.args[2] if load_call.args else load_call.kwargs.get("quarantined", [])
    assert len(quarantined) == 1
    assert quarantined[0].failure_stage == "validate_records"


# ---------------------------------------------------------------------------
# Scenario 8: Oversized response guard — >5,000 records → TransientExtractionError
# Catches upstream dataset explosion or API shape drift that would cause the
# full-dump extractor to silently ingest an unexpected volume.
# ---------------------------------------------------------------------------


def test_oversized_response_guard(vcr_extractor: UsdaExtractor) -> None:
    oversized = [{} for _ in range(_MAX_INCREMENTAL_RECORDS + 1)]
    with (
        patch("time.sleep"),
        patch.object(vcr_extractor, "_fetch", return_value=(oversized, 200, None, None)),
        pytest.raises(TransientExtractionError, match="exceeds guard"),
    ):
        _run(vcr_extractor)


# ---------------------------------------------------------------------------
# Scenario 9: Bilingual orphan quarantine — Spanish record without English sibling
# Exercises check_usda_bilingual_pairing in check_invariants (ADR 0006, Finding F).
# One English record passes all checks; one Spanish record with a different
# recall_number has no English sibling and is quarantined at invariants.
# rejection_threshold=1.0 allows the 50% rate without aborting.
# ---------------------------------------------------------------------------


def test_bilingual_orphan_quarantine(monkeypatch: pytest.MonkeyPatch) -> None:
    for k, v in _REQUIRED_ENV.items():
        monkeypatch.setenv(k, v)
    mock_engine = MagicMock(spec=sa.Engine)
    mock_r2 = MagicMock()
    mock_r2.land.return_value = _FAKE_R2_PATH
    with (
        patch("sqlalchemy.create_engine", return_value=mock_engine),
        patch("src.extractors.usda.R2LandingClient", return_value=mock_r2),
    ):
        settings = Settings()  # type: ignore[call-arg]
        extractor = UsdaExtractor(base_url=_BASE_URL, settings=settings, rejection_threshold=1.0)

    english_row: dict[str, Any] = {
        "field_recall_number": "001-2020",
        "langcode": "English",
        "field_title": "Test Recall",
        "field_recall_date": "2020-01-15",
        "field_recall_type": "Active Recall",
        "field_recall_classification": "Class I",
        "field_archive_recall": "False",
        "field_has_spanish": "False",
    }
    # Recall number 999-2020 has no English sibling → orphan at invariants
    orphan_spanish_row: dict[str, Any] = {
        "field_recall_number": "999-2020",
        "langcode": "Spanish",
        "field_title": "Retiro de prueba",
        "field_recall_date": "2020-01-15",
        "field_recall_type": "Active Recall",
        "field_recall_classification": "Class I",
        "field_archive_recall": "False",
        "field_has_spanish": "True",
    }

    mock_engine.begin.return_value.__enter__ = lambda _: MagicMock()
    mock_engine.begin.return_value.__exit__ = MagicMock(return_value=False)

    with (
        patch.object(extractor, "_read_etag_state", return_value=(None, None)),
        patch.object(
            extractor,
            "_fetch",
            return_value=([english_row, orphan_spanish_row], 200, None, None),
        ),
        patch("src.extractors.usda.BronzeLoader") as mock_loader_cls,
    ):
        mock_loader_cls.return_value.load.return_value = 1
        result = extractor.run()

    assert result.records_fetched == 2
    assert result.records_rejected_validate == 0
    assert result.records_rejected_invariants == 1
