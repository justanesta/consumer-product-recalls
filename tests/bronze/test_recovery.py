from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from unittest.mock import MagicMock

import pytest

from src.bronze.recovery import (
    RECOVERY_CONFIG_BY_SOURCE_NAME,
    RecoveryConfig,
    RecoveryResult,
    coerce_dumped_datetimes,
    datetime_field_names,
    reason_contains,
    reconstruct,
    recover_quarantined,
    recoverable_past_date_sanity,
)

# ---------------------------------------------------------------------------
# Valid source payloads (API/extractor shape, alias-keyed) for round-trip tests.
# ---------------------------------------------------------------------------

_FDA_ROW: dict = {
    "PRODUCTID": "116746",
    "RECALLEVENTID": "64675",
    "RID": 7,
    "CENTERCD": "CDRH",
    "PRODUCTTYPESHORT": "Devices",
    "EVENTLMD": "02/25/2020",
    "FIRMLEGALNAM": "Siemens Medical Solutions USA,  Inc",
    "RECALLINITIATIONDT": "04/01/2013",
    "CENTERCLASSIFICATIONDT": "04/01/2013",
    "TERMINATIONDT": "08/28/2014",
    "ENFORCEMENTREPORTDT": "04/10/2013",
    "DETERMINATIONDT": "03/15/2013",
    "POSTEDINTERNETDT": "04/02/2013",
}

_CPSC_ROW: dict = {
    "RecallNumber": "26419",
    "RecallID": 26419,
    "RecallDate": "2024-01-15",
    "LastPublishDate": "2024-01-20",
}

_NHTSA_ROW: dict = {
    "record_id": "200001",
    "campno": "23V123000",
    "maketxt": "DAMON",
    "modeltxt": "INTRUDER",
    "yeartxt": "2024",
    "compname": "EQUIPMENT:RV:LPG SYSTEM",
    "mfgname": "THOR MOTOR COACH",
    "rcltype": "V",
    "potaff": "1500",
    "mfgtxt": "THOR MOTOR COACH",
    "rcdate": "20240120",
    "desc_defect": "Sample defect description",
    "conequence_defect": "Sample consequence",
    "corrective_action": "Sample corrective action",
    "bgman": "20230101",
    "endman": "20231231",
    "odate": "20240115",
    "datea": "20240122",
    "rcl_cmpt_id": "000037237000216701000000332",
    "mfr_comp_name": "Acme Tank Co",
    "mfr_comp_desc": "LPG storage tank",
    "mfr_comp_ptno": "TANK-2024-A",
}

_USDA_ROW: dict = {
    "field_recall_number": "004-2020",
    "langcode": "English",
    "field_title": "Sample recall title",
    "field_recall_date": "2020-05-15",
    "field_recall_type": "Active Recall",
    "field_recall_classification": "Class I",
    "field_archive_recall": "True",
    "field_has_spanish": "True",
    "field_active_notice": "False",
    "field_last_modified_date": "2020-05-20",
    "field_closed_date": "2020-06-30",
}

_USCG_ROW: dict = {
    "number": "22MF0627",
    "details_url": "https://uscgboating.org/recalls/recall-detail.php?id=22MF0627",
}


def _build(source: str, row: dict[str, Any]):
    """Build a real record for a source via its model, exercising reconstruct's inverse."""
    return RECOVERY_CONFIG_BY_SOURCE_NAME[source].record_model.model_validate(row)


_ROWS_BY_SOURCE = {
    "fda": _FDA_ROW,
    "cpsc": _CPSC_ROW,
    "nhtsa": _NHTSA_ROW,
    "usda": _USDA_ROW,
    "uscg": _USCG_ROW,
}


class TestDatetimeFieldNames:
    """Regression guard for the types.UnionType handling (a typing.Union-only check
    returns the empty set for every Annotated date field — plan §0.4)."""

    def test_fda(self) -> None:
        assert datetime_field_names(
            RECOVERY_CONFIG_BY_SOURCE_NAME["fda"].record_model
        ) == frozenset(
            {
                "event_lmd",
                "recall_initiation_dt",
                "center_classification_dt",
                "termination_dt",
                "enforcement_report_dt",
                "determination_dt",
                "posted_internet_dt",
            }
        )

    def test_nhtsa(self) -> None:
        assert datetime_field_names(
            RECOVERY_CONFIG_BY_SOURCE_NAME["nhtsa"].record_model
        ) == frozenset({"bgman", "datea", "endman", "odate", "rcdate"})

    def test_cpsc(self) -> None:
        assert datetime_field_names(
            RECOVERY_CONFIG_BY_SOURCE_NAME["cpsc"].record_model
        ) == frozenset({"last_publish_date", "recall_date"})

    def test_usda(self) -> None:
        assert datetime_field_names(
            RECOVERY_CONFIG_BY_SOURCE_NAME["usda"].record_model
        ) == frozenset({"recall_date", "last_modified_date", "closed_date"})

    def test_uscg(self) -> None:
        assert datetime_field_names(
            RECOVERY_CONFIG_BY_SOURCE_NAME["uscg"].record_model
        ) == frozenset(
            {
                "opened_on",
                "case_open_date",
                "case_close_date",
                "campaign_open_date",
                "campaign_close_date",
                "last_date",
            }
        )


class TestCoerceDumpedDatetimes:
    _DATES = frozenset({"recall_initiation_dt", "posted_internet_dt", "event_lmd"})

    def test_iso_becomes_datetime(self) -> None:
        out = coerce_dumped_datetimes(
            {"recall_initiation_dt": "0013-03-05T00:00:00+00:00"}, self._DATES
        )
        assert out["recall_initiation_dt"] == datetime(13, 3, 5, tzinfo=UTC)

    def test_transposed_century(self) -> None:
        out = coerce_dumped_datetimes(
            {"posted_internet_dt": "0212-12-07T00:00:00+00:00"}, self._DATES
        )
        assert out["posted_internet_dt"] == datetime(212, 12, 7, tzinfo=UTC)

    def test_none_untouched(self) -> None:
        assert coerce_dumped_datetimes({"event_lmd": None}, self._DATES)["event_lmd"] is None

    def test_empty_string_untouched(self) -> None:
        assert coerce_dumped_datetimes({"event_lmd": ""}, self._DATES)["event_lmd"] == ""

    def test_non_date_field_untouched(self) -> None:
        out = coerce_dumped_datetimes({"recall_num": "F-0123-2026"}, self._DATES)
        assert out["recall_num"] == "F-0123-2026"

    def test_input_not_mutated(self) -> None:
        original = {"recall_initiation_dt": "0013-03-05T00:00:00+00:00"}
        coerce_dumped_datetimes(original, self._DATES)
        assert original["recall_initiation_dt"] == "0013-03-05T00:00:00+00:00"


class TestRecoverablePredicate:
    def test_past_date_sanity_recoverable(self) -> None:
        assert recoverable_past_date_sanity(
            "invariants", "recall_initiation_dt is more than 70 years in the past: 0013-03-05"
        )

    def test_future_date_not_recoverable(self) -> None:
        assert not recoverable_past_date_sanity(
            "invariants", "recall_initiation_dt is in the future: 2099-01-01"
        )

    def test_null_source_id_not_recoverable(self) -> None:
        assert not recoverable_past_date_sanity("invariants", "source_recall_id is null or empty")

    def test_validate_stage_not_recoverable(self) -> None:
        assert not recoverable_past_date_sanity(
            "validate_records", "more than 70 years in the past: x"
        )

    def test_none_reason(self) -> None:
        assert not recoverable_past_date_sanity("invariants", None)

    def test_empty_reason(self) -> None:
        assert not recoverable_past_date_sanity("invariants", "")


class TestReasonContains:
    def test_matches_invariant_stage(self) -> None:
        pred = reason_contains("custom reason")
        assert pred("invariants", "some custom reason here")

    def test_still_gates_on_invariants_stage(self) -> None:
        # Even an override must not pull in validate-stage rejections.
        pred = reason_contains("custom reason")
        assert not pred("validate_records", "some custom reason here")


class TestReconstructRoundTrip:
    """The guard that the introspected date set is complete AND the model round-trips
    losslessly (so BronzeLoader computes a stable content_hash). Per-source — this is what
    caught the missing populate_by_name=True on the USCG schemas (plan §0.5)."""

    @pytest.mark.parametrize("source", ["fda", "cpsc", "nhtsa", "usda", "uscg"])
    def test_round_trip_identity(self, source: str) -> None:
        model = RECOVERY_CONFIG_BY_SOURCE_NAME[source].record_model
        dump = _build(source, _ROWS_BY_SOURCE[source]).model_dump(mode="json")
        rebuilt = reconstruct(model, dump)
        assert rebuilt.model_dump(mode="json") == dump

    def test_fda_recovers_dropped_century(self) -> None:
        model = RECOVERY_CONFIG_BY_SOURCE_NAME["fda"].record_model
        dump = _build("fda", {**_FDA_ROW, "RECALLINITIATIONDT": "03/05/0013"}).model_dump(
            mode="json"
        )
        assert dump["recall_initiation_dt"].startswith("0013-")
        rebuilt_dump = reconstruct(model, dump).model_dump(mode="json")
        assert rebuilt_dump["recall_initiation_dt"].startswith("0013-")

    def test_uscg_coerces_populated_dates(self) -> None:
        # The minimal USCG fixture has all-null dates, so the parametrized round-trip never
        # exercises USCG date coercion. Inject populated ISO dates (as a real quarantined
        # raw_record would carry) to exercise coerce_dumped_datetimes + the date passthrough.
        model = RECOVERY_CONFIG_BY_SOURCE_NAME["uscg"].record_model
        base = _build("uscg", _USCG_ROW).model_dump(mode="json")
        dump = {
            **base,
            "opened_on": "2022-06-01T00:00:00Z",  # listing-format field
            "case_open_date": "2022-06-15T00:00:00Z",  # details-format field
        }
        rebuilt_dump = reconstruct(model, dump).model_dump(mode="json")
        assert rebuilt_dump["opened_on"].startswith("2022-06-01")
        assert rebuilt_dump["case_open_date"].startswith("2022-06-15")


class TestRecoveryConfigMap:
    def test_exactly_the_five_date_sanity_sources(self) -> None:
        assert set(RECOVERY_CONFIG_BY_SOURCE_NAME) == {"fda", "cpsc", "nhtsa", "usda", "uscg"}

    def test_fda_excludes_rid_from_hash(self) -> None:
        assert RECOVERY_CONFIG_BY_SOURCE_NAME["fda"].loader._hash_exclude_fields == frozenset(
            {"rid"}
        )

    def test_nhtsa_uses_incremental_eleven_tuple(self) -> None:
        loader = RECOVERY_CONFIG_BY_SOURCE_NAME["nhtsa"].loader
        # Exact tuple (not just count) — a swapped field would silently change dedup.
        assert loader._identity_fields == (
            "campno",
            "maketxt",
            "modeltxt",
            "yeartxt",
            "compname",
            "rcl_cmpt_id",
            "mfr_comp_ptno",
            "mfr_comp_desc",
            "mfr_comp_name",
            "endman",
            "bgman",
        )
        assert loader._hash_exclude_fields == frozenset({"source_recall_id"})
        assert loader._within_batch_dedup is True
        assert loader._allow_null_identity is True

    def test_usda_composite_identity(self) -> None:
        assert RECOVERY_CONFIG_BY_SOURCE_NAME["usda"].loader._identity_fields == (
            "source_recall_id",
            "langcode",
        )


# ---------------------------------------------------------------------------
# recover_quarantined orchestration — mock engine + stub loader.
# ---------------------------------------------------------------------------


class _FakeResult:
    def __init__(self, rows: list) -> None:
        self._rows = rows

    def first(self):
        return self._rows[0] if self._rows else None

    def all(self) -> list:
        return self._rows


class _FakeConn:
    """Returns canned results for successive execute() calls; usable as a context manager."""

    def __init__(self, results: list[list]) -> None:
        self._results = results
        self._i = 0

    def execute(self, _stmt) -> _FakeResult:
        result = _FakeResult(self._results[self._i])
        self._i += 1
        return result

    def __enter__(self) -> _FakeConn:
        return self

    def __exit__(self, *_a: object) -> bool:
        return False


class _FakeEngine:
    def __init__(self, read_results: list[list]) -> None:
        self._read_results = read_results

    def connect(self) -> _FakeConn:
        return _FakeConn(self._read_results)

    def begin(self) -> _FakeConn:
        return _FakeConn([])


def _fda_config_with_stub_loader(load_return: int) -> tuple[RecoveryConfig, MagicMock]:
    base = RECOVERY_CONFIG_BY_SOURCE_NAME["fda"]
    stub = MagicMock()
    stub.load.return_value = load_return
    cfg = RecoveryConfig(base.record_model, base.bronze_table, base.rejected_table, stub)
    return cfg, stub


def _rejected_row(raw_record: dict[str, Any]):
    row = MagicMock()
    row.id = 1
    row.raw_record = raw_record
    row.failure_stage = "invariants"
    row.failure_reason = "recall_initiation_dt is more than 70 years in the past: 0013-03-05"
    return row


class TestRecoverQuarantined:
    def test_no_rejections_returns_empty(self) -> None:
        cfg, stub = _fda_config_with_stub_loader(0)
        engine: Any = _FakeEngine([[]])  # latest_rejection_landing_path → None
        result = recover_quarantined(
            engine, source="fda", config=cfg, is_recoverable=recoverable_past_date_sanity
        )
        assert result == RecoveryResult("fda", None, 0, 0, False)
        stub.load.assert_not_called()

    def test_dry_run_reconstructs_but_does_not_write(self) -> None:
        cfg, stub = _fda_config_with_stub_loader(1)
        dump = _build("fda", _FDA_ROW).model_dump(mode="json")
        engine: Any = _FakeEngine(
            [
                [("fda/2026-06-01/x.json.gz",)],  # landing path
                [_rejected_row(dump)],  # recoverable rows
                [(datetime(2026, 6, 1, tzinfo=UTC),)],  # seed timestamp
            ]
        )
        result = recover_quarantined(
            engine,
            source="fda",
            config=cfg,
            is_recoverable=recoverable_past_date_sanity,
            dry_run=True,
        )
        assert result.candidates == 1
        assert result.inserted == 0
        assert result.dry_run is True
        stub.load.assert_not_called()

    def test_normal_run_loads_and_reports(self) -> None:
        cfg, stub = _fda_config_with_stub_loader(1)
        dump = _build("fda", _FDA_ROW).model_dump(mode="json")
        engine: Any = _FakeEngine(
            [
                [("fda/2026-06-01/x.json.gz",)],
                [_rejected_row(dump)],
                [(datetime(2026, 6, 1, tzinfo=UTC),)],
            ]
        )
        result = recover_quarantined(
            engine, source="fda", config=cfg, is_recoverable=recoverable_past_date_sanity
        )
        assert result.candidates == 1
        assert result.inserted == 1
        assert result.landing_path == "fda/2026-06-01/x.json.gz"
        stub.load.assert_called_once()
        # Verify the loader call wiring: quarantined empty, landing_path + seed ts forwarded.
        call = stub.load.call_args
        assert call.args[2] == []
        assert call.args[3] == "fda/2026-06-01/x.json.gz"
        assert call.kwargs["extraction_timestamp"] == datetime(2026, 6, 1, tzinfo=UTC)

    def test_predicate_filters_all_to_zero(self) -> None:
        cfg, stub = _fda_config_with_stub_loader(1)
        dump = _build("fda", _FDA_ROW).model_dump(mode="json")
        engine: Any = _FakeEngine(
            [
                [("fda/2026-06-01/x.json.gz",)],
                [_rejected_row(dump)],
                [(datetime(2026, 6, 1, tzinfo=UTC),)],
            ]
        )
        # A predicate that matches nothing → 0 candidates, no write.
        result = recover_quarantined(
            engine,
            source="fda",
            config=cfg,
            is_recoverable=lambda _s, _r: False,
        )
        assert result.candidates == 0
        assert result.inserted == 0
        stub.load.assert_not_called()
