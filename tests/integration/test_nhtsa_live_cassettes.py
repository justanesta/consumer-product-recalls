"""
VCR + respx integration tests for NhtsaExtractor.

NHTSA's HTTP shape is genuinely simpler than CPSC/FDA/USDA — single GET, no
pagination, no auth, no conditional GETs. The cassette here serves a deliberately
narrow purpose: it archives the **HTTP envelope** (real ETag, Last-Modified,
x-amz-version-id, content-type, cache-control headers from NHTSA's S3) so that
test-time replay exercises the actual httpx → response → header-handling path.
The schema-evolution archive responsibility — capturing what the response BODY
looks like over time — is fulfilled separately by `scripts/nhtsa/probe_watermarks.sh`
and `documentation/nhtsa/watermark_probes.jsonl`, which run continuously across
all 15 NHTSA URLs (more comprehensive than any single cassette could be).

**Cassette deviation from CPSC/USDA pattern.** CPSC/USDA cassettes commit
real ~14 MB live response bodies because the response body shape IS the schema
they're archiving. NHTSA's response body is a binary ZIP whose schema is
captured via `body_sha256` in the watermark probe; storing 30+ MB of binary-
encoded ZIP body in a YAML cassette would duplicate that role at large commit
cost. Instead, this cassette uses **real headers + a synthetic body** —
specifically, the existing 1.8 KB `tests/fixtures/nhtsa/sample_recalls.zip`
fixture (10 hand-built rows). The extractor doesn't depend on production
body content for correctness; it just needs a valid TSV-ZIP to decompress and
parse. The 10-row fixture exercises the full extract → validate →
check_invariants → load_bronze pipeline without committing real bytes.

**Recording procedure (one-time, when first creating or when refreshing
NHTSA HTTP-shape evidence):**

    # 1. Live-record against real NHTSA. This produces ~14 MB YAML.
    pytest --vcr-record=all tests/integration/test_nhtsa_live_cassettes.py \\
        -k test_happy_path_full_dump

    # 2. Hand-edit the resulting tests/fixtures/cassettes/nhtsa/test_happy_path_full_dump.yaml
    #    to replace the response body. Pseudo-procedure:
    #    a. Read tests/fixtures/nhtsa/sample_recalls.zip → bytes
    #    b. Base64-encode (cassette body is base64 in YAML)
    #    c. Replace the YAML body string with the encoded fixture
    #    d. Update Content-Length header to match new body byte count
    #    e. Save → ~5-10 KB final cassette
    #
    #    A small helper script in scripts/nhtsa/swap_cassette_body.py is
    #    optional future work; for now hand-edit is fine.

ADR 0014 + ADR 0015 framing: this cassette is the test-time HTTP-client
integration archive. It is COMPLEMENTARY to the watermark probe (which is
the continuous schema-evolution archive). Both run independently; both
serve. See `documentation/decisions/0031-silver-row-fragmentation-strategy.md`
for cross-source reasoning.

Cassette inventory:
  Live-recorded headers + synthetic fixture body:
    test_happy_path_full_dump.yaml — single GET to FLAT_RCL_POST_2010.zip;
                                     real S3 headers; 10-row TSV fixture body

  Hand-constructed (respx mocks, no YAML file):
    test_rate_limit_429              — HTTP 429 → RateLimitError
    test_transient_500               — HTTP 500 → TransientExtractionError

Until the cassette is recorded, the live test skips automatically.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import httpx
import pytest
import respx
import sqlalchemy as sa

from src.config.settings import Settings
from src.extractors._base import RateLimitError, TransientExtractionError
from src.extractors.nhtsa import _INCREMENTAL_URL, NhtsaExtractor

_FIXTURE_DIR = Path(__file__).parent.parent / "fixtures" / "nhtsa"
_FIXTURE_ZIP = _FIXTURE_DIR / "sample_recalls.zip"
_FAKE_R2_PATH = "nhtsa/cassette-test/placeholder.zip.gz"

_REQUIRED_ENV = {
    "NEON_DATABASE_URL": "postgresql://user:pass@localhost/test",
    "R2_ACCOUNT_ID": "test-account",
    "R2_ACCESS_KEY_ID": "test-key-id",
    "R2_SECRET_ACCESS_KEY": "test-secret",
    "R2_BUCKET_NAME": "test-bucket",
}

# Number of rows in tests/fixtures/nhtsa/sample_recalls.zip — the synthetic
# cassette body. Sourced from existing unit tests (test_nhtsa_extractor.py:114).
_FIXTURE_ROW_COUNT = 10


@pytest.fixture(scope="module")
def vcr_cassette_dir() -> str:
    return str(Path(__file__).parent.parent / "fixtures" / "cassettes" / "nhtsa")


@pytest.fixture(autouse=True)
def skip_if_no_cassette(request: pytest.FixtureRequest, vcr_cassette_dir: str) -> None:
    """Skip @pytest.mark.vcr tests when the cassette file isn't recorded yet."""
    marker = request.node.get_closest_marker("vcr")
    if not marker:
        return
    record_mode = request.config.getoption("--vcr-record", default="none")
    if record_mode in ("all", "new_episodes"):
        return
    cassette_name = marker.kwargs.get("cassette_name") or (request.node.name + ".yaml")
    cassette_path = Path(vcr_cassette_dir) / cassette_name
    if not cassette_path.exists():
        pytest.skip(
            "Cassette not yet recorded — see this module's docstring for the "
            "live-record + hand-edit-body procedure."
        )


@pytest.fixture
def vcr_extractor(monkeypatch: pytest.MonkeyPatch) -> NhtsaExtractor:
    for k, v in _REQUIRED_ENV.items():
        monkeypatch.setenv(k, v)
    mock_engine = MagicMock(spec=sa.Engine)
    mock_r2 = MagicMock()
    mock_r2.land.return_value = _FAKE_R2_PATH
    with (
        patch("sqlalchemy.create_engine", return_value=mock_engine),
        patch("src.extractors.nhtsa.R2LandingClient", return_value=mock_r2),
    ):
        settings = Settings()  # type: ignore[call-arg]
        return NhtsaExtractor(settings=settings)


def _run(extractor: NhtsaExtractor) -> Any:
    """Run the extractor with DB / BronzeLoader / R2 / watermark mocked; HTTP via VCR."""
    with (
        patch("src.extractors.nhtsa.BronzeLoader") as mock_loader_cls,
        patch.object(extractor, "_touch_freshness"),
    ):
        mock_loader_cls.from_contract.return_value.load.return_value = 0
        mock_engine: MagicMock = extractor._engine  # type: ignore[assignment]
        mock_engine.begin.return_value.__enter__ = lambda _: MagicMock()
        mock_engine.begin.return_value.__exit__ = MagicMock(return_value=False)
        return extractor.run()


# ---------------------------------------------------------------------------
# Scenario 1: Happy path full dump — single GET → 200, ZIP body decompresses
# to 10-row TSV (synthetic body; real S3 response headers from cassette).
# ---------------------------------------------------------------------------


@pytest.mark.vcr
def test_happy_path_full_dump(vcr_extractor: NhtsaExtractor) -> None:
    result = _run(vcr_extractor)
    # Record count matches the synthetic fixture body (10 rows in
    # sample_recalls.zip). If this fails after re-recording with the procedure
    # in this module's docstring, double-check that the cassette body was
    # swapped to the fixture and not left at the live ~240k-row payload.
    assert result.records_fetched == _FIXTURE_ROW_COUNT
    assert result.records_rejected_validate == 0
    assert result.records_rejected_invariants == 0
    assert result.rejection_rate == 0.0


# ---------------------------------------------------------------------------
# Scenario 2: Rate limit 429 — extractor raises RateLimitError.
# Mocked at the httpx-transport layer via respx so the REAL `_download_to_temp`
# error-mapping path runs (NHTSA's S3 won't return 429 on demand). Verifies
# `_flat_file.py:137` honors the Retry-After header value. Style mirrors
# `test_cpsc_extractor.py:158-167` and `test_fda_extractor.py:226-237`.
# ---------------------------------------------------------------------------


def test_rate_limit_429(vcr_extractor: NhtsaExtractor) -> None:
    with (
        respx.mock,
        patch("time.sleep"),  # skip tenacity waits
    ):
        respx.get(_INCREMENTAL_URL).mock(
            return_value=httpx.Response(429, headers={"Retry-After": "60"})
        )
        with pytest.raises(RateLimitError) as exc_info:
            _run(vcr_extractor)
    assert exc_info.value.retry_after == 60.0


# ---------------------------------------------------------------------------
# Scenario 3: Transient 500 — TransientExtractionError surfaces. Same respx
# pattern as 429. Retry-policy iteration count is unit-tested separately in
# `tests/extractors/`; this test verifies the 5xx-to-TransientExtractionError
# translation in `_flat_file.py:142`.
# ---------------------------------------------------------------------------


def test_transient_500(vcr_extractor: NhtsaExtractor) -> None:
    with (
        respx.mock,
        patch("time.sleep"),  # skip tenacity waits
    ):
        respx.get(_INCREMENTAL_URL).mock(return_value=httpx.Response(500))
        with pytest.raises(TransientExtractionError):
            _run(vcr_extractor)
