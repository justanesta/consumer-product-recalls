"""Tests for R2 replay (re-ingest) — Phase 6d.

Three layers:
  * the ``parse_landed_payload`` seam (``Extractor`` raising default / ``RestApiExtractor``
    json.loads override) — the bytes→records inverse of ``land_raw``;
  * the ``REINGEST_CONFIG_BY_SOURCE_NAME`` map — exactly the 5 JSON REST sources, each loader
    built from the source's dedup contract;
  * the ``reingest_window`` orchestration — mock engine + r2 + stub loader, asserting it replays
    each candidate payload, writes via the loader primitive (not the source's load_bronze), and
    records a rebaseline run per payload without writing a presence manifest.
"""

from __future__ import annotations

import json
from datetime import date
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
import sqlalchemy as sa

from src.bronze.reingest import (
    REINGEST_CONFIG_BY_SOURCE_NAME,
    REINGEST_VALID_CHANGE_TYPES,
    ReingestConfig,
    ReingestResult,
    reingest_window,
    select_candidate_runs,
    validate_and_check,
)
from src.config.settings import Settings
from src.extractors._base import Extractor, QuarantineRecord
from src.extractors.cpsc import CpscExtractor

# ---------------------------------------------------------------------------
# parse_landed_payload seam
# ---------------------------------------------------------------------------

_BASE_URL = "https://www.saferproducts.gov/RestWebServices/Recall"
_REQUIRED_ENV = {
    "NEON_DATABASE_URL": "postgresql://user:pass@localhost/test",
    "R2_ACCOUNT_ID": "test-account",
    "R2_ACCESS_KEY_ID": "test-key-id",
    "R2_SECRET_ACCESS_KEY": "test-secret",
    "R2_BUCKET_NAME": "test-bucket",
}
_VALID_RAW: dict[str, Any] = {
    "RecallID": 24001,
    "RecallNumber": "24-001",
    "RecallDate": "2024-01-15",
    "LastPublishDate": "2024-01-15",
    "Title": "Widget Recall",
}


@pytest.fixture
def cpsc_extractor(monkeypatch: pytest.MonkeyPatch) -> CpscExtractor:
    """A real CpscExtractor (RestApiExtractor) with mocked engine + R2, for the seam test."""
    for key, val in _REQUIRED_ENV.items():
        monkeypatch.setenv(key, val)
    with (
        patch("sqlalchemy.create_engine", return_value=MagicMock(spec=sa.Engine)),
        patch("src.extractors.cpsc.R2LandingClient", return_value=MagicMock()),
    ):
        settings = Settings()  # type: ignore[call-arg]
        return CpscExtractor(base_url=_BASE_URL, settings=settings)


class _MiniExtractor(Extractor[Any]):
    """Bare ``Extractor`` (NOT a REST/flat/html op-type) so ``parse_landed_payload`` falls through
    to the raising default — and ``model_post_init`` (engine/R2 creation) never fires."""

    def extract(self) -> list[dict[str, Any]]:
        return []

    def land_raw(self, raw_records: list[dict[str, Any]]) -> str:
        return ""

    def validate_records(
        self, raw_records: list[dict[str, Any]]
    ) -> tuple[list[Any], list[QuarantineRecord]]:
        return [], []

    def check_invariants(self, records: list[Any]) -> tuple[list[Any], list[QuarantineRecord]]:
        return [], []

    def load_bronze(
        self, records: list[Any], quarantined: list[QuarantineRecord], raw_landing_path: str
    ) -> int:
        return 0


class TestParseLandedPayload:
    def test_rest_source_round_trips_json_dumps(self, cpsc_extractor: CpscExtractor) -> None:
        # land_raw writes json.dumps(raw_records); parse_landed_payload is the exact inverse.
        raw_bytes = json.dumps([_VALID_RAW], default=str).encode("utf-8")
        assert cpsc_extractor.parse_landed_payload(raw_bytes) == [_VALID_RAW]

    def test_rest_source_empty_array(self, cpsc_extractor: CpscExtractor) -> None:
        assert cpsc_extractor.parse_landed_payload(b"[]") == []

    def test_rest_source_non_array_raises(self, cpsc_extractor: CpscExtractor) -> None:
        with pytest.raises(ValueError, match="not a JSON array"):
            cpsc_extractor.parse_landed_payload(b'{"RecallNumber": "24-001"}')

    def test_non_rest_source_raises_not_implemented(self) -> None:
        ext = _MiniExtractor(source_name="nhtsa")
        with pytest.raises(NotImplementedError, match="deep-rescan"):
            ext.parse_landed_payload(b"anything")


# ---------------------------------------------------------------------------
# REINGEST_CONFIG_BY_SOURCE_NAME — the JSON-source allow-set + contract wiring
# ---------------------------------------------------------------------------


class TestReingestConfigMap:
    def test_exactly_the_five_json_sources(self) -> None:
        assert set(REINGEST_CONFIG_BY_SOURCE_NAME) == {
            "cpsc",
            "fda",
            "usda",
            "usda_establishments",
            "fda_press_releases",
        }

    def test_excludes_non_json_sources(self) -> None:
        # NHTSA (flat file) + USCG (HTML) must NOT be re-ingestable — they use deep-rescan.
        for source in ("nhtsa", "uscg", "uscg_manufacturers", "uscg_manufacturer_details"):
            assert source not in REINGEST_CONFIG_BY_SOURCE_NAME

    def test_change_types_are_rebaseline_only(self) -> None:
        assert {"schema_rebaseline", "hash_helper_rebaseline"} == REINGEST_VALID_CHANGE_TYPES

    def test_fda_loader_excludes_rid_from_hash(self) -> None:
        assert REINGEST_CONFIG_BY_SOURCE_NAME["fda"].loader._hash_exclude_fields == frozenset(
            {"rid"}
        )

    def test_usda_loader_composite_identity(self) -> None:
        assert REINGEST_CONFIG_BY_SOURCE_NAME["usda"].loader._identity_fields == (
            "source_recall_id",
            "langcode",
        )


# ---------------------------------------------------------------------------
# reingest_window orchestration — mock engine + r2 + stub loader/extractor
# ---------------------------------------------------------------------------


class _FakeResult:
    def __init__(self, rows: list) -> None:
        self._rows = rows

    def all(self) -> list:
        return self._rows

    def first(self) -> object:
        return self._rows[0] if self._rows else None


class _FakeConn:
    def __init__(self, rows: list) -> None:
        self._rows = rows
        self.executed: list = []

    def execute(self, stmt: object) -> _FakeResult:
        self.executed.append(stmt)
        return _FakeResult(self._rows)

    def __enter__(self) -> _FakeConn:
        return self

    def __exit__(self, *_a: object) -> bool:
        return False


class _FakeEngine:
    """connect() yields the candidate rows; begin() yields a fresh writable conn per payload."""

    def __init__(self, candidate_rows: list) -> None:
        self._candidates = candidate_rows
        self.begin_conns: list[_FakeConn] = []

    def connect(self) -> _FakeConn:
        return _FakeConn(self._candidates)

    def begin(self) -> _FakeConn:
        conn = _FakeConn([])
        self.begin_conns.append(conn)
        return conn


def _candidate(run_id: str, key: str) -> SimpleNamespace:
    return SimpleNamespace(run_id=run_id, raw_landing_path=key)


def _stub_ext(new_key: str) -> MagicMock:
    ext = MagicMock()
    ext.parse_landed_payload.return_value = [_VALID_RAW]
    ext.land_raw.return_value = new_key
    record = SimpleNamespace(source_recall_id="24-001")
    ext.validate_records.return_value = ([record], [])
    ext.check_invariants.return_value = ([record], [])
    return ext


def _stub_config(load_return: int) -> tuple[ReingestConfig, MagicMock]:
    # Real cpsc bronze/rejected tables so the original-timestamp SELECT builds; stub loader.
    base = REINGEST_CONFIG_BY_SOURCE_NAME["cpsc"]
    loader = MagicMock()
    loader.load.return_value = load_return
    return ReingestConfig(base.bronze_table, base.rejected_table, loader), loader


class TestReingestWindow:
    def test_no_candidates_reports_zero(self) -> None:
        engine: Any = _FakeEngine([])
        config, loader = _stub_config(1)
        result = reingest_window(
            engine,
            MagicMock(),
            _stub_ext("k"),
            source="cpsc",
            config=config,
            from_date=date(2024, 1, 1),
            to_date=date(2024, 12, 31),
            change_type="schema_rebaseline",
        )
        assert result.payloads_found == 0
        assert result.payloads_replayed == 0
        loader.load.assert_not_called()

    def test_dry_run_finds_but_does_not_replay(self) -> None:
        engine: Any = _FakeEngine([_candidate("r1", "cpsc/2024-01-15/a.json.gz")])
        config, loader = _stub_config(1)
        result = reingest_window(
            engine,
            MagicMock(),
            _stub_ext("k"),
            source="cpsc",
            config=config,
            from_date=date(2024, 1, 1),
            to_date=date(2024, 12, 31),
            change_type="schema_rebaseline",
            dry_run=True,
        )
        assert result.dry_run is True
        assert result.payloads_found == 1
        assert result.payloads_replayed == 0
        loader.load.assert_not_called()

    def test_replays_each_payload_and_records_rebaseline_run(self) -> None:
        engine = _FakeEngine(
            [
                _candidate("r1", "cpsc/2024-01-15/a.json.gz"),
                _candidate("r2", "cpsc/2024-02-20/b.json.gz"),
            ]
        )
        r2 = MagicMock()
        ext = _stub_ext("cpsc/2026-06-06/new.json.gz")
        config, loader = _stub_config(1)

        result = reingest_window(
            engine,  # type: ignore[arg-type]
            r2,
            ext,
            source="cpsc",
            config=config,
            from_date=date(2024, 1, 1),
            to_date=date(2024, 12, 31),
            change_type="schema_rebaseline",
        )

        assert result == ReingestResult(
            "cpsc",
            date(2024, 1, 1),
            date(2024, 12, 31),
            payloads_found=2,
            payloads_replayed=2,
            rows_inserted=2,
            dry_run=False,
        )
        # Each original payload was read from R2 and re-landed to a fresh key.
        assert r2.get_raw.call_count == 2
        assert ext.land_raw.call_count == 2
        # Wrote via the loader primitive (NOT ext.load_bronze) with the fresh key.
        assert loader.load.call_count == 2
        ext.load_bronze.assert_not_called()
        for call in loader.load.call_args_list:
            assert call.args[3] == "cpsc/2026-06-06/new.json.gz"  # raw_landing_path = new key
            # Loaded at the ORIGINAL payload's timestamp (None in this mock), never now().
            assert "extraction_timestamp" in call.kwargs
        # One extraction_runs insert per payload (begin() opened twice); no manifest table touched.
        assert len(engine.begin_conns) == 2
        # Each rebaseline run records replayed_from_run_id = the ORIGINAL run's id (lineage,
        # migration 0029) and carries the rebaseline change_type. In the begin block the first
        # execute is the original-timestamp SELECT; the second is the _record_reingest_run INSERT.
        run1 = engine.begin_conns[0].executed[1].compile().params
        run2 = engine.begin_conns[1].executed[1].compile().params
        assert run1["replayed_from_run_id"] == "r1"
        assert run2["replayed_from_run_id"] == "r2"
        assert run1["change_type"] == "schema_rebaseline"


class TestValidateAndCheck:
    def test_concats_quarantine_from_both_stages(self) -> None:
        ext = MagicMock()
        q1 = MagicMock(spec=QuarantineRecord)
        q2 = MagicMock(spec=QuarantineRecord)
        rec = SimpleNamespace(source_recall_id="X")
        ext.validate_records.return_value = ([rec], [q1])
        ext.check_invariants.return_value = ([rec], [q2])
        passing, quarantined = validate_and_check(ext, [{"a": 1}])
        assert passing == [rec]
        assert quarantined == [q1, q2]


def test_select_candidate_runs_smoke() -> None:
    """select_candidate_runs builds a SELECT and maps rows to (run_id, raw_landing_path)."""
    conn = _FakeConn([_candidate("r1", "cpsc/2024-01-15/a.json.gz")])
    out = select_candidate_runs(conn, "cpsc", date(2024, 1, 1), date(2024, 12, 31))  # type: ignore[arg-type]
    assert out == [("r1", "cpsc/2024-01-15/a.json.gz")]


def test_select_candidate_runs_force_builds_valid_query() -> None:
    """force=True drops the already-replayed exclusion; both paths must compile + map rows."""
    conn = _FakeConn([_candidate("r1", "cpsc/2024-01-15/a.json.gz")])
    out = select_candidate_runs(conn, "cpsc", date(2024, 1, 1), date(2024, 12, 31), force=True)  # type: ignore[arg-type]
    assert out == [("r1", "cpsc/2024-01-15/a.json.gz")]
