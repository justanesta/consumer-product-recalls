"""Tests for scripts/backfill_manifest.py — the USDA presence-manifest backfill (ADR 0028 Mech C).

Pure layer: ``format_census_report`` (the census-first go/no-go surface).
I/O layer: ``backfill_usda`` against a mock engine — asserts it replays each candidate and submits
its presence roster under the run's ORIGINAL run_id (the FK target + lifecycle run-count key), with
``replay_to_passing`` patched so no real R2/DB is touched.
"""

from __future__ import annotations

import sys
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock, patch

# scripts/ is not on sys.path by default; add the repo root so we can import the script.
_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from scripts.backfill_manifest import (  # noqa: E402  — sys.path mutated above
    BackfillResult,
    CensusReport,
    backfill_usda,
    format_census_report,
    select_backfillable_runs,
)

# ---------------------------------------------------------------------------
# format_census_report — pure
# ---------------------------------------------------------------------------


class TestFormatCensusReport:
    def test_with_floor(self) -> None:
        report = CensusReport(
            total_payload_runs=40,
            with_run_id=38,
            null_run_id=2,
            floor=datetime(2026, 4, 12, 9, 30, tzinfo=UTC),
            backfillable=35,
        )
        out = format_census_report(report)
        assert "successful runs with a landed payload : 40" in out
        assert "with usable run_id (backfillable set)  : 38" in out
        assert "NULL run_id (permanently un-backfillable): 2" in out
        assert "2026-04-12" in out
        assert "--apply will insert): 35" in out

    def test_without_floor_renders_na(self) -> None:
        report = CensusReport(0, 0, 0, None, 0)
        assert "earliest recoverable)  : n/a" in format_census_report(report)


# ---------------------------------------------------------------------------
# backfill_usda — mock engine + patched replay
# ---------------------------------------------------------------------------


class _FakeResult:
    def __init__(self, rows: list) -> None:
        self._rows = rows

    def all(self) -> list:
        return self._rows


class _FakeConn:
    def __init__(self, rows: list) -> None:
        self._rows = rows
        self.inserted: list = []

    def execute(self, stmt: object, params: object = None) -> _FakeResult:
        if params is not None:
            self.inserted.append(params)
        return _FakeResult(self._rows)

    def __enter__(self) -> _FakeConn:
        return self

    def __exit__(self, *_a: object) -> bool:
        return False


class _FakeEngine:
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


class TestBackfillUsda:
    def test_submits_rows_under_original_run_id(self) -> None:
        engine: Any = _FakeEngine([_candidate("hist-run-1", "usda/2026-05-01/a.json.gz")])
        record = SimpleNamespace(source_recall_id="004-2020", langcode="English")
        with patch("scripts.backfill_manifest.replay_to_passing", return_value=[record]):
            result = backfill_usda(engine, MagicMock(), MagicMock())

        assert result == BackfillResult(runs_processed=1, manifest_rows_submitted=1)
        # One begin() txn; the submitted row carries the ORIGINAL run_id, not a fresh one.
        assert len(engine.begin_conns) == 1
        submitted_rows = engine.begin_conns[0].inserted[0]
        assert submitted_rows == [
            {
                "run_id": "hist-run-1",
                "source": "usda",
                "source_recall_id": "004-2020",
                "langcode": "English",
            }
        ]

    def test_dry_run_replays_but_does_not_insert(self) -> None:
        engine: Any = _FakeEngine([_candidate("hist-run-1", "usda/2026-05-01/a.json.gz")])
        record = SimpleNamespace(source_recall_id="004-2020", langcode="English")
        with patch("scripts.backfill_manifest.replay_to_passing", return_value=[record]):
            result = backfill_usda(engine, MagicMock(), MagicMock(), dry_run=True)
        # Counts the would-be rows (the preview) but opens NO write transaction.
        assert result == BackfillResult(runs_processed=1, manifest_rows_submitted=1)
        assert engine.begin_conns == []

    def test_no_candidates_is_a_noop(self) -> None:
        engine: Any = _FakeEngine([])
        with patch("scripts.backfill_manifest.replay_to_passing") as replay:
            result = backfill_usda(engine, MagicMock(), MagicMock())
        assert result == BackfillResult(runs_processed=0, manifest_rows_submitted=0)
        replay.assert_not_called()

    def test_run_with_no_passing_records_submits_nothing(self) -> None:
        engine: Any = _FakeEngine([_candidate("hist-run-2", "usda/2026-05-02/b.json.gz")])
        with patch("scripts.backfill_manifest.replay_to_passing", return_value=[]):
            result = backfill_usda(engine, MagicMock(), MagicMock())
        assert result == BackfillResult(runs_processed=1, manifest_rows_submitted=0)
        assert engine.begin_conns == []  # no insert txn opened


def test_select_backfillable_runs_maps_rows() -> None:
    conn = _FakeConn([_candidate("r1", "usda/2026-05-01/a.json.gz")])
    out = select_backfillable_runs(conn)  # type: ignore[arg-type]
    assert out == [("r1", "usda/2026-05-01/a.json.gz")]
