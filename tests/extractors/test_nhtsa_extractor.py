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
from src.extractors._base import ExtractionResult, TransientExtractionError
from src.extractors.nhtsa import (
    _DRIFT_FAILURE_KEY,
    _DRIFT_RAW_LINE_KEY,
    _EXPECTED_FIELDS,
    _HISTORICAL_PRE_2010_URL,
    _INCREMENTAL_URL,
    _NHTSA_SOURCE,
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


def _make_extractor(
    monkeypatch: pytest.MonkeyPatch,
    *,
    cls: type[NhtsaExtractor] = NhtsaExtractor,
    since: Any = None,
) -> NhtsaExtractor:
    """Construct an extractor with the engine + R2 client mocked.

    Used by both the per-test fixtures (which return the default
    incremental-extractor shape) and by tests that need a custom
    ``since`` value or the deep-rescan subclass.
    """
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
        kwargs: dict[str, Any] = {"settings": settings}
        if since is not None:
            kwargs["since"] = since
        return cls(**kwargs)


@pytest.fixture
def extractor(monkeypatch: pytest.MonkeyPatch) -> NhtsaExtractor:
    return _make_extractor(monkeypatch)


@pytest.fixture
def deep_rescan(monkeypatch: pytest.MonkeyPatch) -> NhtsaDeepRescanLoader:
    return _make_extractor(monkeypatch, cls=NhtsaDeepRescanLoader)  # type: ignore[return-value]


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

    def test_first_row_field_mapping_pins_field_names_order(
        self, extractor: NhtsaExtractor, fixture_zip_bytes: bytes
    ) -> None:
        """A reorder of ``_FIELD_NAMES`` would silently swap column values.

        The previous test asserts only on ``record_id`` and ``campno``,
        which sit at indices 0–1 and would survive most swaps further
        down the tuple. This test pins values across the whole row so a
        reorder anywhere in the 29-tuple is caught.
        """
        response = _make_zip_response(fixture_zip_bytes)
        with patch("httpx.Client") as mock_client:
            mock_client.return_value.__enter__.return_value.get.return_value = response
            records = extractor.extract()

        first = records[0]
        # Spread the assertions across early/middle/late indices to catch
        # any swap, not just adjacent ones.
        assert first["record_id"] == "200001"  # index 0
        assert first["maketxt"] == "DAMON"  # index 2
        assert first["yeartxt"] == "2024"  # index 4
        assert first["mfgname"] == "THOR MOTOR COACH"  # index 7
        assert first["bgman"] == "20230101"  # index 8
        assert first["rcdate"] == "20240120"  # index 15
        assert first["fmvss"] == "208"  # index 18
        assert first["do_not_drive"] == "No"  # index 27
        assert first["park_outside"] == "No"  # index 28

    def test_drift_row_routed_to_marker_dict(self, extractor: NhtsaExtractor) -> None:
        """A row with !=29 fields produces a marker dict, not a record dict.

        Targets the field-count drift branch in ``extract()`` (Finding F:
        NHTSA has historically added columns at the right edge of RCL.txt
        4 times in 18 years; we want a row to appear in quarantine, not
        crash the extractor or silently corrupt bronze with a misaligned
        row).
        """
        good = ["g"] * _EXPECTED_FIELDS  # 29 fields — passes
        bad = ["b"] * (_EXPECTED_FIELDS + 1)  # 30 fields — drift
        body = ("\t".join(good) + "\r\n" + "\t".join(bad) + "\r\n").encode("utf-8")

        response = _make_zip_response(b"unused")
        with (
            patch("httpx.Client") as mock_client,
            patch.object(extractor, "_decompress_zip", return_value=(body, "FAKE.txt")),
        ):
            mock_client.return_value.__enter__.return_value.get.return_value = response
            records = extractor.extract()

        # Two entries returned — one record dict, one marker dict.
        assert len(records) == 2
        record_dict, marker_dict = records
        assert _DRIFT_FAILURE_KEY not in record_dict
        assert _DRIFT_FAILURE_KEY in marker_dict
        # The marker carries the raw line (for forensic replay) and a
        # human-readable failure reason mentioning both counts.
        assert _DRIFT_RAW_LINE_KEY in marker_dict
        assert "30" in marker_dict[_DRIFT_FAILURE_KEY]
        assert "29" in marker_dict[_DRIFT_FAILURE_KEY]

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

        Derive the expected count from the fixture rather than hardcoding,
        so future fixture rebuilds don't silently pass with a stale count.
        """
        from datetime import date as _date

        cutoff = _date(2024, 1, 1)
        cutoff_str = cutoff.strftime("%Y%m%d")

        # First pass: parse the fixture ourselves (with no --since) to learn
        # how many rows pass the same predicate. This couples the test to
        # the fixture's contents only via the predicate itself, not via a
        # magic number.
        baseline = _make_extractor(monkeypatch)
        response = _make_zip_response(fixture_zip_bytes)
        with patch("httpx.Client") as mock_client:
            mock_client.return_value.__enter__.return_value.get.return_value = response
            all_records = baseline.extract()
        expected = sum(1 for r in all_records if r.get("rcdate") and r["rcdate"] >= cutoff_str)

        # Second pass: re-extract with --since active and confirm the count
        # matches our independently-derived expectation.
        ext = _make_extractor(monkeypatch, since=cutoff)
        with patch("httpx.Client") as mock_client:
            mock_client.return_value.__enter__.return_value.get.return_value = response
            records = ext.extract()

        for r in records:
            rcdate = r.get("rcdate", "")
            assert rcdate >= cutoff_str, f"--since let through rcdate={rcdate!r}"
        assert len(records) == expected
        # Sanity: the cutoff actually filters something — otherwise the test
        # would pass trivially.
        assert expected < len(all_records)

    def test_since_filter_drops_empty_rcdate(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """When `--since` is active, rows with empty RCDATE are dropped.

        Empty RCDATE only appears in PRE_2010 (5 records) which the
        incremental path doesn't see, but the filter should still
        defensively drop them rather than silently passing them through.
        """
        from datetime import date as _date

        ext = _make_extractor(monkeypatch, since=_date(1900, 1, 1))

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

    def test_count_guard_fires_when_threshold_exceeded(
        self,
        extractor: NhtsaExtractor,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The behavior under test is the comparison
        ``len(records) > _MAX_INCREMENTAL_RECORDS``, not the specific
        threshold value. Patch the constant down to a tiny number and
        synthesize a few rows over it; same branch, instant test.
        """
        monkeypatch.setattr("src.extractors.nhtsa._MAX_INCREMENTAL_RECORDS", 5)
        cells = ["x"] * _EXPECTED_FIELDS
        row = "\t".join(cells)
        # 6 rows > patched threshold of 5 → guard fires.
        body = ("\r\n".join([row] * 6) + "\r\n").encode("utf-8")

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
        # Use the constants the source defines, so a rename of either key
        # name forces the test to be revisited rather than silently still
        # passing with a malformed dict shape.
        drift_marker: dict[str, Any] = {
            _DRIFT_FAILURE_KEY: "Row 0 has 30 fields; expected 29.",
            _DRIFT_RAW_LINE_KEY: "field1\tfield2\t...",
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

    def test_valid_record_passes_through(self, extractor: NhtsaExtractor) -> None:
        """A record with a non-empty source_recall_id and a sane rcdate
        passes both invariants and lands in ``passing`` (not quarantined).

        The two failure-path tests above don't exercise the success
        branch; without this, a regression that quarantines every record
        would still pass those tests.
        """
        record = NhtsaRecord.model_construct(
            source_recall_id="200001",
            campno="23V123000",
            maketxt="DAMON",
            modeltxt="INTRUDER",
            yeartxt="2024",
            compname="EQUIPMENT:RV:LPG SYSTEM",
            mfgname="THOR MOTOR COACH",
            rcltype="V",
            potaff="1500",
            mfgtxt="THOR MOTOR COACH",
            rcdate=dt(2024, 1, 20, tzinfo=UTC),
            desc_defect="X",
            conequence_defect="X",
            corrective_action="X",
        )
        passing, quarantined = extractor.check_invariants([record])
        assert quarantined == []
        assert passing == [record]


# ---------------------------------------------------------------------------
# land_raw (incremental) — wrapper ZIP bytes round-trip through R2
# ---------------------------------------------------------------------------


class TestLandRaw:
    def test_writes_wrapper_bytes_to_r2_and_returns_path(self, extractor: NhtsaExtractor) -> None:
        """The bronze "raw" per ADR 0007 is the wrapper ZIP NHTSA served,
        not the decompressed TSV — so ``land_raw`` writes
        ``_wrapper_bytes`` (stashed during extract) verbatim with a
        ``.zip`` suffix, and stashes the returned path for downstream
        steps' use.
        """
        extractor._wrapper_bytes = b"fake wrapper zip bytes"
        extractor._r2_client.land.return_value = "nhtsa/2026-05-06/abc.zip"  # type: ignore[attr-defined]

        path = extractor.land_raw([])

        assert path == "nhtsa/2026-05-06/abc.zip"
        # Side effect: the path is stashed on the extractor so
        # validate_records / check_invariants can stamp it onto
        # quarantine records (raw_landing_path).
        assert extractor._current_landing_path == path
        extractor._r2_client.land.assert_called_once_with(  # type: ignore[attr-defined]
            source=_NHTSA_SOURCE,
            content=b"fake wrapper zip bytes",
            suffix="zip",
        )


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
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Same trick as the incremental guard-fires test — patch the
        threshold constant down to something that the synthesized body
        comfortably exceeds, then assert deep-rescan still does NOT
        raise. Demonstrates the absence of inheritance, fast.
        """
        monkeypatch.setattr("src.extractors.nhtsa._MAX_INCREMENTAL_RECORDS", 5)
        cells = ["x"] * _EXPECTED_FIELDS
        row = "\t".join(cells)
        # 6 rows > patched threshold of 5; the incremental path would
        # raise here. Deep-rescan must not.
        body = ("\r\n".join([row] * 6) + "\r\n").encode("utf-8")
        response = _make_zip_response(b"unused")
        with (
            patch("httpx.Client") as mock_client,
            patch.object(deep_rescan, "_decompress_zip", return_value=(body, "FAKE.txt")),
        ):
            mock_client.return_value.__enter__.return_value.get.return_value = response
            records = deep_rescan.extract()
        # Two archives × 6 synthesized rows each, no guard, no raise.
        assert len(records) == 12

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

    def test_extract_routes_drift_rows_to_marker_dicts(
        self,
        deep_rescan: NhtsaDeepRescanLoader,
    ) -> None:
        """The deep-rescan path has its own field-count drift branch
        (parallel to the incremental extractor's). A 30-field row in
        either archive must produce a marker dict, not a record dict —
        same Finding F semantics as the incremental path.
        """
        good = ["g"] * _EXPECTED_FIELDS
        bad = ["b"] * (_EXPECTED_FIELDS + 1)
        body = ("\t".join(good) + "\r\n" + "\t".join(bad) + "\r\n").encode("utf-8")

        response = _make_zip_response(b"unused")
        with (
            patch("httpx.Client") as mock_client,
            patch.object(deep_rescan, "_decompress_zip", return_value=(body, "FAKE.txt")),
        ):
            mock_client.return_value.__enter__.return_value.get.return_value = response
            records = deep_rescan.extract()

        # 4 entries: 1 good + 1 drift per archive × 2 archives.
        assert len(records) == 4
        marker_count = sum(1 for r in records if _DRIFT_FAILURE_KEY in r)
        record_count = len(records) - marker_count
        assert marker_count == 2
        assert record_count == 2
        # Marker rows carry the raw line and a 30-vs-29 message.
        markers = [r for r in records if _DRIFT_FAILURE_KEY in r]
        for m in markers:
            assert _DRIFT_RAW_LINE_KEY in m
            assert "30" in m[_DRIFT_FAILURE_KEY]
            assert "29" in m[_DRIFT_FAILURE_KEY]

    def test_extract_pre_2010_inner_hash_does_not_overwrite_post_capture(
        self,
        deep_rescan: NhtsaDeepRescanLoader,
        fixture_zip_bytes: bytes,
    ) -> None:
        """The PRE_2010 hash lives in its own PrivateAttr; the canonical
        ``_captured_response_inner_content_sha256`` keeps the POST_2010
        value so day-over-day diffs on that column track the rolling-current
        archive (matching the incremental path's semantics).
        """
        response = _make_zip_response(fixture_zip_bytes)
        with patch("httpx.Client") as mock_client:
            mock_client.return_value.__enter__.return_value.get.return_value = response
            deep_rescan.extract()

        # Both PRE and POST hashes are populated — and the canonical capture
        # equals the POST hash (not the PRE hash).
        assert deep_rescan._pre_2010_inner_sha256
        assert deep_rescan._post_2010_inner_sha256
        assert (
            deep_rescan._captured_response_inner_content_sha256
            == deep_rescan._post_2010_inner_sha256
        )
        # PRE and POST happen to be the same fixture here, so we can't
        # assert PRE != POST. The non-overwrite guarantee is structural:
        # _capture_flatfile_response is only called once (with POST).

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


# ---------------------------------------------------------------------------
# load_bronze (incremental) — calls BronzeLoader, advances watermark
# ---------------------------------------------------------------------------


class TestLoadBronze:
    def test_calls_bronze_loader_and_touches_watermark(self, extractor: NhtsaExtractor) -> None:
        """The incremental path delegates to BronzeLoader and advances
        ``source_watermarks.last_successful_extract_at`` via
        ``_touch_freshness``. Deep-rescan owns neither of those — see
        ``TestDeepRescan.test_load_bronze_skips_watermark_advance`` for
        the contrast.
        """
        with (
            patch("src.extractors.nhtsa.BronzeLoader") as mock_loader_cls,
            patch.object(extractor, "_touch_freshness") as mock_touch,
        ):
            mock_loader_cls.return_value.load.return_value = 7
            mock_conn = MagicMock()
            extractor._engine.begin.return_value.__enter__.return_value = mock_conn  # type: ignore[attr-defined]

            count = extractor.load_bronze([], [], "nhtsa/abc.zip")

        assert count == 7
        mock_loader_cls.assert_called_once()
        # identity_fields=("source_recall_id",) per the source — RECORD_ID
        # is unique across the corpus (TSV field 1).
        assert mock_loader_cls.call_args.kwargs["identity_fields"] == ("source_recall_id",)
        mock_loader_cls.return_value.load.assert_called_once_with(
            mock_conn, [], [], "nhtsa/abc.zip"
        )
        mock_touch.assert_called_once_with(mock_conn)


# ---------------------------------------------------------------------------
# _touch_freshness — bumps source_watermarks for monitoring
# ---------------------------------------------------------------------------


class TestTouchFreshness:
    def test_executes_update_against_source_watermarks(self, extractor: NhtsaExtractor) -> None:
        """NHTSA has no usable cursor or ETag (Findings B + C); the
        watermark row exists solely so freshness monitoring can see the
        run as recent. This test confirms ``_touch_freshness`` issues an
        UPDATE filtered to ``source = 'nhtsa'``.
        """
        mock_conn = MagicMock()
        extractor._touch_freshness(mock_conn)

        mock_conn.execute.assert_called_once()
        stmt = mock_conn.execute.call_args.args[0]
        # Verify it's an UPDATE on source_watermarks (the SQL string is
        # the simplest portable check across SA versions).
        compiled = str(stmt)
        assert "UPDATE source_watermarks" in compiled
        # Sanity-check the bound params: source name and both timestamp fields.
        params = stmt.compile().params
        assert "last_successful_extract_at" in params
        assert "updated_at" in params


# ---------------------------------------------------------------------------
# _record_run — forensic capture for extraction_runs
# ---------------------------------------------------------------------------


class TestRecordRun:
    def test_happy_path_inserts_row_with_all_capture_fields(
        self, extractor: NhtsaExtractor
    ) -> None:
        """When the run succeeded and ``_capture_flatfile_response`` ran,
        the inserted row carries every column added by migrations 0010
        + 0011 — including the new ``response_inner_content_sha256``
        which is the change-detection oracle for ZIP wrappers (Finding J).
        """
        # Pre-populate the forensic state as ``extract`` would have done.
        extractor._captured_response_status_code = 200
        extractor._captured_response_etag = '"deadbeef"'
        extractor._captured_response_last_modified = "Mon, 05 May 2026 07:04:23 GMT"
        extractor._captured_response_body_sha256 = "wrapper-hash"
        extractor._captured_response_inner_content_sha256 = "inner-hash"
        extractor._captured_response_headers = {"x-amz-version-id": "VERSIONXYZ"}

        result = ExtractionResult(
            source=_NHTSA_SOURCE,
            run_id="rid-1",
            records_fetched=10,
            records_landed=10,
            records_valid=9,
            records_rejected_validate=1,
            records_rejected_invariants=0,
            records_loaded=9,
            raw_landing_path="nhtsa/abc.zip",
        )
        mock_conn = MagicMock()
        extractor._engine.begin.return_value.__enter__.return_value = mock_conn  # type: ignore[attr-defined]

        started = dt(2026, 5, 5, 12, 0, tzinfo=UTC)
        extractor._record_run(
            run_id="rid-1",
            started_at=started,
            status="success",
            result=result,
            error_message=None,
            change_type="routine",
        )

        mock_conn.execute.assert_called_once()
        # Pull the bound values out of the INSERT statement.
        stmt = mock_conn.execute.call_args.args[0]
        values = stmt.compile().params
        assert values["source"] == _NHTSA_SOURCE
        assert values["status"] == "success"
        assert values["change_type"] == "routine"
        assert values["records_extracted"] == 10
        assert values["records_inserted"] == 9
        assert values["records_rejected"] == 1  # validate(1) + invariants(0)
        assert values["raw_landing_path"] == "nhtsa/abc.zip"
        assert values["response_status_code"] == 200
        assert values["response_body_sha256"] == "wrapper-hash"
        # Migration 0011's column — the whole reason this extractor exists
        # in its current form. Pin its presence explicitly.
        assert values["response_inner_content_sha256"] == "inner-hash"

    def test_omits_result_fields_when_result_is_none(self, extractor: NhtsaExtractor) -> None:
        """``status="failure"`` runs may have no ``ExtractionResult`` —
        e.g., the extractor blew up before producing one. The row must
        still insert, just without the records_* / raw_landing_path columns.
        """
        # No capture either — failure can happen before _capture_flatfile_response.
        mock_conn = MagicMock()
        extractor._engine.begin.return_value.__enter__.return_value = mock_conn  # type: ignore[attr-defined]

        extractor._record_run(
            run_id="rid-2",
            started_at=dt(2026, 5, 5, 12, 0, tzinfo=UTC),
            status="failure",
            result=None,
            error_message="boom",
        )

        stmt = mock_conn.execute.call_args.args[0]
        values = stmt.compile().params
        assert values["status"] == "failure"
        assert values["error_message"] == "boom"
        # records_* keys are absent from `row` so SA binds them as NULL —
        # we just confirm the values dict does not carry truthy counts.
        assert values.get("records_extracted") is None
        assert values.get("response_status_code") is None

    def test_db_failure_is_logged_not_raised(self, extractor: NhtsaExtractor) -> None:
        """Bronze has already committed by the time ``_record_run`` is
        called; a failure here (e.g., FK violation if the
        ``source_watermarks`` row is missing) must NOT propagate.
        Mirrors the Phase 5b.2 incident captured in
        ``test_usda_establishment_extractor.py::TestRecordRun``.
        """
        mock_conn = MagicMock()
        mock_conn.execute.side_effect = RuntimeError("FK violation")
        extractor._engine.begin.return_value.__enter__.return_value = mock_conn  # type: ignore[attr-defined]
        extractor._engine.begin.return_value.__exit__.return_value = False  # type: ignore[attr-defined]

        # Should not raise.
        extractor._record_run(
            run_id="rid-3",
            started_at=dt(2026, 5, 5, 12, 0, tzinfo=UTC),
            status="success",
        )
