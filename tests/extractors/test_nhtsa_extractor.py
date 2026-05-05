"""Unit tests for NhtsaExtractor and NhtsaDeepRescanLoader.

Mocks the httpx HTTP client and the SQLAlchemy/R2 clients; exercises
the real ``extract → land_raw → validate_records → check_invariants →
load_bronze`` lifecycle against the deterministic fixture ZIP at
``tests/fixtures/nhtsa/sample_recalls.zip``.
"""

from __future__ import annotations

from datetime import UTC, timedelta
from datetime import datetime as dt
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import httpx
import pytest
import sqlalchemy as sa

from src.config.settings import Settings
from src.extractors._base import TransientExtractionError
from src.extractors.nhtsa import (
    _DRIFT_FAILURE_KEY,
    _EXPECTED_FIELDS,
    _HISTORICAL_PRE_2010_URL,
    _INCREMENTAL_URL,
    _MAX_INCREMENTAL_RECORDS,
    NhtsaDeepRescanLoader,
    NhtsaExtractor,
)
from src.schemas.nhtsa import NhtsaRecord

_FIXTURE_DIR = Path(__file__).parent.parent / "fixtures" / "nhtsa"
_FIXTURE_ZIP = _FIXTURE_DIR / "sample_recalls.zip"

_FAKE_R2_PATH = "nhtsa/2026-05-05/abc.zip"

_REQUIRED_ENV = {
    "NEON_DATABASE_URL": "postgresql://user:pass@localhost/test",
    "R2_ACCOUNT_ID": "test-account",
    "R2_ACCESS_KEY_ID": "test-key-id",
    "R2_SECRET_ACCESS_KEY": "test-secret",
    "R2_BUCKET_NAME": "test-bucket",
}


def _make_zip_response(zip_bytes: bytes) -> httpx.Response:
    """Build an httpx.Response whose body is the given ZIP bytes."""
    request = httpx.Request("GET", _INCREMENTAL_URL)
    return httpx.Response(
        200,
        request=request,
        content=zip_bytes,
        headers={
            "etag": '"deadbeef"',
            "last-modified": "Mon, 05 May 2026 07:04:23 GMT",
            "content-type": "application/octet-stream",
            "x-amz-version-id": "VERSIONXYZ",
        },
    )


@pytest.fixture
def fixture_zip_bytes() -> bytes:
    return _FIXTURE_ZIP.read_bytes()


@pytest.fixture
def extractor(monkeypatch: pytest.MonkeyPatch) -> NhtsaExtractor:
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
        ext = NhtsaExtractor(settings=settings)
    return ext


@pytest.fixture
def deep_rescan(monkeypatch: pytest.MonkeyPatch) -> NhtsaDeepRescanLoader:
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
        return NhtsaDeepRescanLoader(settings=settings)


# ---------------------------------------------------------------------------
# extract — happy path against the fixture ZIP
# ---------------------------------------------------------------------------


class TestExtract:
    def test_happy_path_returns_10_records(
        self, extractor: NhtsaExtractor, fixture_zip_bytes: bytes
    ) -> None:
        response = _make_zip_response(fixture_zip_bytes)
        with patch("httpx.Client") as mock_client:
            mock_client.return_value.__enter__.return_value.get.return_value = response
            records = extractor.extract()

        assert len(records) == 10
        # Record dicts are keyed by lowercase RCL.txt names.
        first = records[0]
        assert first["record_id"] == "200001"
        assert first["campno"] == "23V123000"
        assert "<A HREF=" in first["desc_defect"]  # embedded HTML preserved

    def test_capture_inner_hash_populated(
        self, extractor: NhtsaExtractor, fixture_zip_bytes: bytes
    ) -> None:
        response = _make_zip_response(fixture_zip_bytes)
        with patch("httpx.Client") as mock_client:
            mock_client.return_value.__enter__.return_value.get.return_value = response
            extractor.extract()

        # Both forensic hashes populated — wrapper is the ZIP, inner is the
        # decompressed TSV. Per Finding J, inner is the authoritative
        # change-detection oracle.
        assert extractor._captured_response_body_sha256 is not None
        assert extractor._captured_response_inner_content_sha256 is not None
        assert (
            extractor._captured_response_body_sha256
            != extractor._captured_response_inner_content_sha256
        )

    def test_wrapper_bytes_stashed_for_land_raw(
        self, extractor: NhtsaExtractor, fixture_zip_bytes: bytes
    ) -> None:
        response = _make_zip_response(fixture_zip_bytes)
        with patch("httpx.Client") as mock_client:
            mock_client.return_value.__enter__.return_value.get.return_value = response
            extractor.extract()
        # The wrapper bytes are stashed in PrivateAttr so land_raw can
        # write them to R2 without re-downloading on a retry of that step.
        assert extractor._wrapper_bytes == fixture_zip_bytes

    def test_since_filter_drops_old_rows(
        self,
        monkeypatch: pytest.MonkeyPatch,
        fixture_zip_bytes: bytes,
    ) -> None:
        """`--since` drops rows whose RCDATE is earlier than the cutoff.

        The fixture's 10 rows have RCDATE values spanning 2006-05-10 (pre-2007
        record) through 2025-01-05. A cutoff of 2024-01-01 should keep 4 rows
        (the modern recalls with RCDATE >= 2024).
        """
        from datetime import date as _date

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
            ext = NhtsaExtractor(settings=settings, since=_date(2024, 1, 1))

        response = _make_zip_response(fixture_zip_bytes)
        with patch("httpx.Client") as mock_client:
            mock_client.return_value.__enter__.return_value.get.return_value = response
            records = ext.extract()

        # Confirm every kept row has RCDATE >= 20240101.
        for r in records:
            rcdate = r.get("rcdate", "")
            assert rcdate >= "20240101", f"--since filter let through rcdate={rcdate!r}"
        # The fixture has 3 rows with RCDATE >= 20240101 (rows 1, 8, 10).
        # Tighten this if the fixture changes.
        assert len(records) == 3

    def test_since_filter_drops_empty_rcdate(
        self,
        monkeypatch: pytest.MonkeyPatch,
        fixture_zip_bytes: bytes,
    ) -> None:
        """When `--since` is active, rows with empty RCDATE are dropped.

        Empty RCDATE only appears in PRE_2010 (5 records) which the
        incremental path doesn't see, but the filter should still
        defensively drop them rather than silently passing them through.
        """
        from datetime import date as _date

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
            ext = NhtsaExtractor(settings=settings, since=_date(1900, 1, 1))

        # Inject a synthetic TSV body with one row that has empty RCDATE.
        cells = ["x"] * _EXPECTED_FIELDS
        # rcdate (field 16) is at array index 15.
        cells[15] = ""
        body = ("\t".join(cells) + "\r\n").encode("utf-8")

        response = _make_zip_response(b"unused")
        with (
            patch("httpx.Client") as mock_client,
            patch.object(ext, "_decompress_zip", return_value=(body, "FAKE.txt")),
        ):
            mock_client.return_value.__enter__.return_value.get.return_value = response
            records = ext.extract()
        assert records == []

    def test_count_guard_fires_when_corpus_explodes(self, extractor: NhtsaExtractor) -> None:
        # Synthesize a TSV body with > _MAX_INCREMENTAL_RECORDS rows. We
        # don't need a ZIP — patch _decompress_zip to bypass that step.
        oversized_count = _MAX_INCREMENTAL_RECORDS + 1
        # Each row needs 29 fields; build a minimal-shape row.
        cells = ["x"] * _EXPECTED_FIELDS
        row = "\t".join(cells)
        body = ("\r\n".join([row] * oversized_count) + "\r\n").encode("utf-8")

        response = _make_zip_response(b"unused")
        with (
            patch("httpx.Client") as mock_client,
            patch.object(extractor, "_decompress_zip", return_value=(body, "FAKE.txt")),
        ):
            mock_client.return_value.__enter__.return_value.get.return_value = response
            with pytest.raises(TransientExtractionError) as exc_info:
                extractor.extract()
        assert "exceeds guard" in str(exc_info.value)


# ---------------------------------------------------------------------------
# validate_records — drift markers + Pydantic errors → quarantine
# ---------------------------------------------------------------------------


class TestValidateRecords:
    def test_valid_records_instantiated(
        self, extractor: NhtsaExtractor, fixture_zip_bytes: bytes
    ) -> None:
        response = _make_zip_response(fixture_zip_bytes)
        with patch("httpx.Client") as mock_client:
            mock_client.return_value.__enter__.return_value.get.return_value = response
            raw = extractor.extract()
        valid, quarantined = extractor.validate_records(raw)
        assert len(valid) == 10
        assert len(quarantined) == 0
        assert all(isinstance(r, NhtsaRecord) for r in valid)

    def test_drift_marker_routed_to_quarantine(self, extractor: NhtsaExtractor) -> None:
        # Marker dict — what extract() produces for a row with !=29 fields.
        drift_marker: dict[str, Any] = {
            _DRIFT_FAILURE_KEY: "Row 0 has 30 fields; expected 29.",
            "_drift_raw_line": "field1\tfield2\t...",
        }
        valid, quarantined = extractor.validate_records([drift_marker])
        assert valid == []
        assert len(quarantined) == 1
        q = quarantined[0]
        assert q.failure_stage == "extract"
        assert q.source_recall_id is None  # marker has no record_id
        assert "30 fields" in q.failure_reason

    def test_pydantic_error_routed_to_quarantine(self, extractor: NhtsaExtractor) -> None:
        # Bad row: missing required campno field.
        bad_row = {
            "record_id": "999999",
            # campno omitted
            "maketxt": "FAKE",
            "modeltxt": "X",
            "yeartxt": "2024",
            "compname": "FOO",
            "mfgname": "BAR",
            "rcltype": "V",
            "potaff": "1",
            "mfgtxt": "BAR",
            "rcdate": "20240120",
            "desc_defect": "...",
            "conequence_defect": "...",
            "corrective_action": "...",
        }
        valid, quarantined = extractor.validate_records([bad_row])
        assert valid == []
        assert len(quarantined) == 1
        q = quarantined[0]
        assert q.failure_stage == "validate_records"
        assert q.source_recall_id == "999999"


# ---------------------------------------------------------------------------
# check_invariants — null id and bad date routing
# ---------------------------------------------------------------------------


class TestCheckInvariants:
    def test_null_source_id_quarantined(self, extractor: NhtsaExtractor) -> None:
        # Construct a NhtsaRecord then mutate to break the invariant. Easier:
        # build a record dict with an empty record_id (which becomes
        # source_recall_id via alias) — but that fails the strict Pydantic
        # parse first. Simulate post-validation state by mocking.
        record = NhtsaRecord.model_construct(
            source_recall_id="",
            campno="X",
            maketxt="X",
            modeltxt="X",
            yeartxt="2024",
            compname="X",
            mfgname="X",
            rcltype="V",
            potaff="1",
            mfgtxt="X",
            rcdate=dt(2024, 1, 1, tzinfo=UTC),
            desc_defect="X",
            conequence_defect="X",
            corrective_action="X",
        )
        passing, quarantined = extractor.check_invariants([record])
        assert passing == []
        assert len(quarantined) == 1
        assert "source_recall_id" in quarantined[0].failure_reason

    def test_future_date_quarantined(self, extractor: NhtsaExtractor) -> None:
        future = dt.now(UTC) + timedelta(days=2)
        record = NhtsaRecord.model_construct(
            source_recall_id="200001",
            campno="X",
            maketxt="X",
            modeltxt="X",
            yeartxt="2024",
            compname="X",
            mfgname="X",
            rcltype="V",
            potaff="1",
            mfgtxt="X",
            rcdate=future,
            desc_defect="X",
            conequence_defect="X",
            corrective_action="X",
        )
        passing, quarantined = extractor.check_invariants([record])
        assert passing == []
        assert len(quarantined) == 1
        assert "future" in quarantined[0].failure_reason


# ---------------------------------------------------------------------------
# NhtsaDeepRescanLoader — pulls both archives, no count guard, no watermark
# ---------------------------------------------------------------------------


class TestDeepRescan:
    def test_extract_pulls_both_archives(
        self,
        deep_rescan: NhtsaDeepRescanLoader,
        fixture_zip_bytes: bytes,
    ) -> None:
        # Both URLs return the same fixture ZIP for simplicity. The
        # iterator concatenates rows from both — so we expect 2 × 10 = 20.
        response = _make_zip_response(fixture_zip_bytes)
        with patch("httpx.Client") as mock_client:
            mock_get = mock_client.return_value.__enter__.return_value.get
            mock_get.return_value = response
            records = deep_rescan.extract()

        urls_called = [call.args[0] for call in mock_get.call_args_list]
        assert _INCREMENTAL_URL in urls_called
        assert _HISTORICAL_PRE_2010_URL in urls_called
        # Two archives × 10 fixture rows each.
        assert len(records) == 20

    def test_extract_no_count_guard(
        self,
        deep_rescan: NhtsaDeepRescanLoader,
        fixture_zip_bytes: bytes,
    ) -> None:
        # Synthesize a body with > _MAX_INCREMENTAL_RECORDS rows for both
        # archives. The deep-rescan path must NOT raise.
        cells = ["x"] * _EXPECTED_FIELDS
        row = "\t".join(cells)
        body = ("\r\n".join([row] * 600_000) + "\r\n").encode("utf-8")
        response = _make_zip_response(b"unused")
        with (
            patch("httpx.Client") as mock_client,
            patch.object(deep_rescan, "_decompress_zip", return_value=(body, "FAKE.txt")),
        ):
            mock_client.return_value.__enter__.return_value.get.return_value = response
            # Should NOT raise — deep-rescan has no guard.
            records = deep_rescan.extract()
        assert len(records) == 1_200_000  # 2 archives × 600k rows each

    def test_load_bronze_skips_watermark_advance(
        self,
        deep_rescan: NhtsaDeepRescanLoader,
    ) -> None:
        # Confirm load_bronze does NOT call _touch_freshness (which would
        # advance source_watermarks.last_successful_extract_at).
        with (
            patch("src.extractors.nhtsa.BronzeLoader") as mock_loader_cls,
            patch.object(deep_rescan, "_touch_freshness") as mock_touch,
        ):
            mock_loader_cls.return_value.load.return_value = 0
            # The mock engine's begin() returns a context manager.
            mock_conn = MagicMock()
            deep_rescan._engine.begin.return_value.__enter__.return_value = mock_conn  # type: ignore[attr-defined]
            deep_rescan.load_bronze([], [], "manifest/path")

        mock_touch.assert_not_called()

    def test_land_raw_writes_manifest_and_returns_manifest_path(
        self,
        deep_rescan: NhtsaDeepRescanLoader,
    ) -> None:
        # Stash wrapper bytes for both archives.
        deep_rescan._wrapper_bytes = b"post-2010 wrapper"
        deep_rescan._pre_2010_wrapper_bytes = b"pre-2010 wrapper"
        deep_rescan._post_2010_inner_sha256 = "post-hash"
        deep_rescan._pre_2010_inner_sha256 = "pre-hash"

        # Three .land() calls expected: pre ZIP, post ZIP, manifest JSON.
        deep_rescan._r2_client.land.side_effect = [  # type: ignore[attr-defined]
            "nhtsa/post.zip",
            "nhtsa/pre.zip",
            "nhtsa/manifest.json",
        ]
        path = deep_rescan.land_raw([])
        assert path == "nhtsa/manifest.json"
        assert deep_rescan._r2_client.land.call_count == 3  # type: ignore[attr-defined]

        # Manifest content should reference both archive paths and inner hashes.
        manifest_call = deep_rescan._r2_client.land.call_args_list[-1]  # type: ignore[attr-defined]
        manifest_content = manifest_call.kwargs["content"]
        assert b"pre-hash" in manifest_content
        assert b"post-hash" in manifest_content
        assert _HISTORICAL_PRE_2010_URL.encode() in manifest_content
        assert _INCREMENTAL_URL.encode() in manifest_content
