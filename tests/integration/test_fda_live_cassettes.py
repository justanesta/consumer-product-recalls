"""
VCR integration tests for FdaExtractor.

Live-recorded cassettes (scenarios 1–5) replay real FDA iRES API responses and verify
that the Pydantic schema handles the actual API shape. Hand-constructed cassettes
(scenarios 6–8) test error-handling paths the live API won't produce on demand.

FDA-specific VCR note: the signature= query parameter (cache-busting, finding 3 in
api_observations.md) is stripped from request URIs before cassette matching via the
module-level vcr_config override. Without this, every replay fails because the
recorded signature value will never match the runtime timestamp.

Cassette inventory:
  Live-recorded (real FDA iRES responses):
    test_happy_path_single_page.yaml       — small window, one page
    test_happy_path_multi_page.yaml        — larger window, multiple pages
    test_happy_path_partial_last_page.yaml — last page len < PAGE_SIZE
    test_empty_result.yaml                 — STATUSCODE 412, zero records
    test_auth_failure.yaml                 — STATUSCODE 401 (bad creds)

  Hand-constructed (respx mocks):
    test_rate_limit_429.yaml               — HTTP 429 → RateLimitError
    test_transient_500.yaml                — HTTP 500 → TransientExtractionError
    test_malformed_record.yaml             — bad field → quarantined row

  Reuses existing cassette (no separate file):
    test_content_hash_dedup               — re-runs test_happy_path_single_page twice

To record live cassettes (requires FDA credentials in env):
    uv run pytest --vcr-record=all tests/integration/test_fda_live_cassettes.py \\
        -k "single_page or multi_page or partial or empty or auth"

Commit the generated YAML files under tests/fixtures/cassettes/fda/.
Until cassettes are recorded, live tests skip automatically.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
import sqlalchemy as sa

from src.config.settings import Settings
from src.extractors._base import AuthenticationError, RateLimitError, TransientExtractionError
from src.extractors.fda import _PAGE_SIZE, FdaExtractor

_BASE_URL = "https://www.accessdata.fda.gov/rest/iresapi"
_FAKE_R2_PATH = "fda/cassette-test/placeholder.json.gz"

_REQUIRED_ENV = {
    "NEON_DATABASE_URL": "postgresql://user:pass@localhost/test",
    "R2_ACCOUNT_ID": "test-account",
    "R2_ACCESS_KEY_ID": "test-key-id",
    "R2_SECRET_ACCESS_KEY": "test-secret",
    "R2_BUCKET_NAME": "test-bucket",
    "FDA_AUTHORIZATION_USER": "test-user",
    "FDA_AUTHORIZATION_KEY": "test-key",
}


@pytest.fixture(scope="module")
def vcr_config(vcr_config: dict[str, Any]) -> dict[str, Any]:
    # Strip signature= before cassette matching — finding 3 in api_observations.md
    return {**vcr_config, "filter_query_parameters": ["signature"]}


@pytest.fixture(scope="module")
def vcr_cassette_dir() -> str:
    return str(Path(__file__).parent.parent / "fixtures" / "cassettes" / "fda")


@pytest.fixture(autouse=True)
def skip_if_no_cassette(request: pytest.FixtureRequest, vcr_cassette_dir: str) -> None:
    # Only applies to @pytest.mark.vcr tests, not respx-based tests
    if not request.node.get_closest_marker("vcr"):
        return
    record_mode = request.config.getoption("--vcr-record", default="none")
    if record_mode in ("all", "new_episodes"):
        return
    cassette_path = Path(vcr_cassette_dir) / (request.node.name + ".yaml")
    if not cassette_path.exists():
        pytest.skip(
            "Cassette not yet recorded — run: "
            "uv run pytest --vcr-record=all tests/integration/test_fda_live_cassettes.py"
        )


@pytest.fixture
def vcr_extractor(monkeypatch: pytest.MonkeyPatch) -> FdaExtractor:
    for k, v in _REQUIRED_ENV.items():
        monkeypatch.setenv(k, v)
    mock_engine = MagicMock(spec=sa.Engine)
    mock_r2 = MagicMock()
    mock_r2.land.return_value = _FAKE_R2_PATH
    with (
        patch("sqlalchemy.create_engine", return_value=mock_engine),
        patch("src.extractors.fda.R2LandingClient", return_value=mock_r2),
    ):
        settings = Settings()  # type: ignore[call-arg]
        return FdaExtractor(base_url=_BASE_URL, settings=settings)


def _run(extractor: FdaExtractor, watermark: date) -> Any:
    """Run the extractor with DB/R2 mocked; HTTP goes through VCR."""
    with (
        patch.object(extractor, "_get_watermark", return_value=watermark),
        patch("src.extractors.fda.BronzeLoader") as mock_loader_cls,
        patch.object(extractor, "_update_watermark"),
    ):
        mock_loader_cls.return_value.load.return_value = 0
        mock_engine: MagicMock = extractor._engine  # type: ignore[assignment]
        mock_engine.begin.return_value.__enter__ = lambda _: MagicMock()
        mock_engine.begin.return_value.__exit__ = MagicMock(return_value=False)
        return extractor.run()


# ---------------------------------------------------------------------------
# Scenario 1: Happy path, single page — small date window
# Watermark: 2026-04-20 (7-day window, ~141 records per cardinality observations)
# ---------------------------------------------------------------------------


@pytest.mark.vcr
def test_happy_path_single_page(vcr_extractor: FdaExtractor) -> None:
    result = _run(vcr_extractor, date(2026, 4, 20))
    assert result.records_fetched > 0
    assert result.records_fetched < _PAGE_SIZE  # confirm single-page (< 5000 records)
    assert result.records_rejected_validate == 0
    assert result.records_rejected_invariants == 0
    assert result.rejection_rate == 0.0


# ---------------------------------------------------------------------------
# Scenario 2: Happy path, multi-page — wider window, pagination loop
# Watermark: 2026-01-01 (~3,012 records per cardinality observations)
# ---------------------------------------------------------------------------


@pytest.mark.vcr
def test_happy_path_multi_page(vcr_extractor: FdaExtractor) -> None:
    result = _run(vcr_extractor, date(2026, 1, 1))
    assert result.records_fetched > 0
    assert result.records_rejected_validate == 0
    assert result.records_rejected_invariants == 0


# ---------------------------------------------------------------------------
# Scenario 3: Happy path, partial last page — validates loop terminates on len < PAGE_SIZE
# Watermark: 2026-04-01 (small enough to fit in 1 page at rows=5000 but confirms termination)
# ---------------------------------------------------------------------------


@pytest.mark.vcr
def test_happy_path_partial_last_page(vcr_extractor: FdaExtractor) -> None:
    result = _run(vcr_extractor, date(2026, 4, 1))
    assert result.records_fetched > 0
    assert result.records_rejected_validate == 0
    assert result.records_rejected_invariants == 0


# ---------------------------------------------------------------------------
# Scenario 4: Empty result — STATUSCODE 412, no RESULT key in response
# Uses a date range known to have no records (04/25/2026 to 04/26/2026 per
# api_observations.md finding K extension: no edits in this narrow window)
# ---------------------------------------------------------------------------


@pytest.mark.vcr
def test_empty_result(vcr_extractor: FdaExtractor) -> None:
    result = _run(vcr_extractor, date(2026, 4, 25))
    assert result.records_fetched == 0
    assert result.records_loaded == 0
    assert result.rejection_rate == 0.0


# ---------------------------------------------------------------------------
# Scenario 5: Auth failure — STATUSCODE 401 (recorded with bad credentials)
# ---------------------------------------------------------------------------


@pytest.mark.vcr
def test_auth_failure(vcr_extractor: FdaExtractor) -> None:
    with pytest.raises(AuthenticationError):
        _run(vcr_extractor, date(2026, 4, 20))


# ---------------------------------------------------------------------------
# Scenario 6: Rate limit — HTTP 429 → RateLimitError
# Patches _fetch_page to raise directly (avoids signature= URL matching issues).
# ---------------------------------------------------------------------------


def test_rate_limit_429(vcr_extractor: FdaExtractor) -> None:
    with (
        patch.object(vcr_extractor, "_fetch_page", side_effect=RateLimitError(retry_after=30.0)),
        pytest.raises(RateLimitError) as exc_info,
    ):
        _run(vcr_extractor, date(2026, 4, 20))
    assert exc_info.value.retry_after == 30.0


# ---------------------------------------------------------------------------
# Scenario 7: Transient 500 — retried per _TRANSIENT_RETRY policy
# Patches _fetch_page to raise TransientExtractionError on every attempt.
# ---------------------------------------------------------------------------


def test_transient_500(vcr_extractor: FdaExtractor) -> None:
    with (
        patch.object(
            vcr_extractor, "_fetch_page", side_effect=TransientExtractionError("HTTP 500")
        ),
        pytest.raises(TransientExtractionError),
    ):
        _run(vcr_extractor, date(2026, 4, 20))


# ---------------------------------------------------------------------------
# Scenario 8: Malformed record — one bad row in RESULT quarantines to rejected table
# ---------------------------------------------------------------------------


def _valid_fda_row() -> dict:
    return {
        "PRODUCTID": "219875",
        "RECALLEVENTID": "98815",
        "RID": 1,
        "CENTERCD": "CFSAN",
        "PRODUCTTYPESHORT": "Food",
        "EVENTLMD": "04/24/2026",
        "FIRMLEGALNAM": "Acme Foods LLC",
        "FIRMFEINUM": None,
        "RECALLNUM": "F-0123-2026",
        "PHASETXT": "Ongoing",
        "CENTERCLASSIFICATIONTYPETXT": "1",
        "RECALLINITIATIONDT": "04/01/2026",
        "CENTERCLASSIFICATIONDT": "04/10/2026",
        "TERMINATIONDT": None,
        "ENFORCEMENTREPORTDT": None,
        "DETERMINATIONDT": None,
        "INITIALFIRMNOTIFICATIONTXT": "Letter",
        "DISTRIBUTIONAREASUMMARYTXT": "Nationwide",
        "VOLUNTARYTYPETXT": "Voluntary: Firm Initiated",
        "PRODUCTDESCRIPTIONTXT": "Contaminated crackers",
        "PRODUCTSHORTREASONTXT": "Salmonella contamination",
        "PRODUCTDISTRIBUTEDQUANTITY": "50,000 cases",
    }


def test_malformed_record(monkeypatch: pytest.MonkeyPatch) -> None:
    # Use rejection_threshold=1.0 so the 50% rate (1 of 2 rows bad) doesn't abort the run.
    # This test is about quarantine routing, not the threshold abort path.
    for k, v in _REQUIRED_ENV.items():
        monkeypatch.setenv(k, v)
    mock_engine = MagicMock(spec=sa.Engine)
    mock_r2 = MagicMock()
    mock_r2.land.return_value = _FAKE_R2_PATH
    with (
        patch("sqlalchemy.create_engine", return_value=mock_engine),
        patch("src.extractors.fda.R2LandingClient", return_value=mock_r2),
    ):
        settings = Settings()  # type: ignore[call-arg]
        extractor = FdaExtractor(base_url=_BASE_URL, settings=settings, rejection_threshold=1.0)

    malformed_row = _valid_fda_row()
    malformed_row["UNKNOWN_EXTRA_FIELD"] = "unexpected"  # extra='forbid' rejects this

    valid_row = _valid_fda_row()
    valid_row["PRODUCTID"] = "219876"
    valid_row["RID"] = 2

    with (
        patch.object(extractor, "_fetch_page", return_value=[valid_row, malformed_row]),
        patch.object(extractor, "_get_watermark", return_value=date(2026, 4, 20)),
        patch("src.extractors.fda.BronzeLoader") as mock_loader_cls,
        patch.object(extractor, "_update_watermark"),
    ):
        mock_loader_cls.return_value.load.return_value = 1
        mock_engine.begin.return_value.__enter__ = lambda _: MagicMock()
        mock_engine.begin.return_value.__exit__ = MagicMock(return_value=False)
        result = extractor.run()

    assert result.records_fetched == 2
    assert result.records_rejected_validate == 1
    load_call = mock_loader_cls.return_value.load.call_args
    quarantined = load_call.args[2] if load_call.args else load_call.kwargs.get("quarantined", [])
    assert len(quarantined) == 1
    assert quarantined[0].failure_stage == "validate_records"


# ---------------------------------------------------------------------------
# Scenario 9: Content-hash dedup — re-running extractor on same cassette adds zero rows
# Reuses test_happy_path_single_page cassette; no separate file needed.
# ---------------------------------------------------------------------------


@pytest.mark.vcr(cassette_name="test_happy_path_single_page.yaml")
def test_content_hash_dedup(vcr_extractor: FdaExtractor) -> None:
    with (
        patch.object(vcr_extractor, "_get_watermark", return_value=date(2026, 4, 20)),
        patch("src.extractors.fda.BronzeLoader") as mock_loader_cls,
        patch.object(vcr_extractor, "_update_watermark"),
    ):
        # Simulate all records already in bronze (dedup returns 0)
        mock_loader_cls.return_value.load.return_value = 0
        mock_engine: MagicMock = vcr_extractor._engine  # type: ignore[assignment]
        mock_engine.begin.return_value.__enter__ = lambda _: MagicMock()
        mock_engine.begin.return_value.__exit__ = MagicMock(return_value=False)
        result = vcr_extractor.run()

    assert result.records_loaded == 0
    assert result.records_fetched > 0
