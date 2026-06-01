from __future__ import annotations

import sys
from pathlib import Path

# scripts/ is not on sys.path by default; add the repo root so we can import the
# recovery driver as a regular module for testing its pure logic.
_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from datetime import UTC, datetime  # noqa: E402

from scripts.fda.recover_rejected_invariant_records import (  # noqa: E402  — sys.path mutated
    _FDA_DATE_FIELDS,
    coerce_dumped_dates,
    is_recoverable_invariant_rejection,
    reconstruct_record,
)
from src.schemas.fda import FdaRecord  # noqa: E402

# A full valid FDA row in the source's API shape (validation aliases + MM/DD/YYYY dates),
# every date field populated — so the round-trip test exercises all of _FDA_DATE_FIELDS.
_SEED_INPUT: dict = {
    "PRODUCTID": "116746",
    "RECALLEVENTID": "64675",
    "RID": 7,
    "CENTERCD": "CDRH",
    "PRODUCTTYPESHORT": "Devices",
    "EVENTLMD": "02/25/2020",
    "FIRMLEGALNAM": "Siemens Medical Solutions USA,  Inc",
    "FIRMFEINUM": "1610287",
    "RECALLNUM": "Z-1234-2013",
    "PHASETXT": "Terminated",
    "CENTERCLASSIFICATIONTYPETXT": "2",
    "RECALLINITIATIONDT": "04/01/2013",
    "CENTERCLASSIFICATIONDT": "04/01/2013",
    "TERMINATIONDT": "08/28/2014",
    "ENFORCEMENTREPORTDT": "04/10/2013",
    "DETERMINATIONDT": "03/15/2013",
    "INITIALFIRMNOTIFICATIONTXT": "Letter",
    "DISTRIBUTIONAREASUMMARYTXT": "Nationwide",
    "VOLUNTARYTYPETXT": "Voluntary: Firm Initiated",
    "PRODUCTDESCRIPTIONTXT": "SIEMENS brand COHERENCE RT Therapist",
    "PRODUCTSHORTREASONTXT": "Software defect",
    "PRODUCTDISTRIBUTEDQUANTITY": "120 units",
    "CODEINFORMATION": "RTT MR1.5",
    "FIRMCITYNAM": "Malvern",
    "FIRMCOUNTRYNAM": "United States",
    "FIRMLINE1ADR": "40 Liberty Blvd",
    "FIRMLINE2ADR": None,
    "FIRMPOSTALCD": "19355",
    "FIRMSTATECD": "PA",
    "FIRMSTATEPRVNCNAM": "Pennsylvania",
    "FIRMSURVIVINGNAM": None,
    "FIRMSURVIVINGFEI": None,
    "POSTEDINTERNETDT": "04/02/2013",
}


class TestIsRecoverableInvariantRejection:
    def test_past_date_sanity_failure_is_recoverable(self) -> None:
        assert is_recoverable_invariant_rejection(
            "invariants",
            "recall_initiation_dt is more than 70 years in the past: 0013-03-05T00:00:00+00:00",
        )

    def test_future_date_failure_is_not_recoverable(self) -> None:
        # The future-date branch is genuinely bad data, not the century-typo class.
        assert not is_recoverable_invariant_rejection(
            "invariants", "recall_initiation_dt is in the future: 2099-01-01T00:00:00+00:00"
        )

    def test_null_source_id_failure_is_not_recoverable(self) -> None:
        assert not is_recoverable_invariant_rejection(
            "invariants", "source_recall_id is null or empty"
        )

    def test_validate_stage_is_not_recoverable(self) -> None:
        # Only invariant-stage rejections are in scope, never schema (validate) rejects.
        assert not is_recoverable_invariant_rejection(
            "validate_records", "recall_initiation_dt is more than 70 years in the past: x"
        )

    def test_none_reason_is_not_recoverable(self) -> None:
        assert not is_recoverable_invariant_rejection("invariants", None)

    def test_none_stage_is_not_recoverable(self) -> None:
        assert not is_recoverable_invariant_rejection(None, "more than 70 years in the past: x")

    def test_empty_reason_is_not_recoverable(self) -> None:
        assert not is_recoverable_invariant_rejection("invariants", "")


class TestCoerceDumpedDates:
    def test_iso_string_becomes_datetime(self) -> None:
        out = coerce_dumped_dates({"recall_initiation_dt": "0013-03-05T00:00:00+00:00"})
        assert out["recall_initiation_dt"] == datetime(13, 3, 5, tzinfo=UTC)

    def test_transposed_century_year_parses(self) -> None:
        # 0212 (the 2012 transposition) must also coerce cleanly.
        out = coerce_dumped_dates({"posted_internet_dt": "0212-12-07T00:00:00+00:00"})
        assert out["posted_internet_dt"] == datetime(212, 12, 7, tzinfo=UTC)

    def test_none_left_untouched(self) -> None:
        out = coerce_dumped_dates({"termination_dt": None})
        assert out["termination_dt"] is None

    def test_empty_string_left_untouched(self) -> None:
        out = coerce_dumped_dates({"event_lmd": ""})
        assert out["event_lmd"] == ""

    def test_non_date_field_untouched(self) -> None:
        # A non-date key is never converted, even if its value looks date-ish.
        out = coerce_dumped_dates({"recall_num": "F-0123-2026"})
        assert out["recall_num"] == "F-0123-2026"

    def test_absent_date_field_is_no_op(self) -> None:
        out = coerce_dumped_dates({"source_recall_id": "54047"})
        assert out == {"source_recall_id": "54047"}

    def test_input_not_mutated(self) -> None:
        original = {"recall_initiation_dt": "0013-03-05T00:00:00+00:00"}
        coerce_dumped_dates(original)
        assert original["recall_initiation_dt"] == "0013-03-05T00:00:00+00:00"

    def test_all_known_date_fields_are_real_schema_fields(self) -> None:
        # Guard against drift: every name we coerce must exist on FdaRecord.
        assert set(FdaRecord.model_fields) >= _FDA_DATE_FIELDS


class TestReconstructRecord:
    def test_round_trip_identity(self) -> None:
        # The critical guard: reconstructing a dumped record reproduces the SAME dump, so
        # BronzeLoader computes an identical content_hash. A missing entry in
        # _FDA_DATE_FIELDS would make this raise (ISO string hits the MM/DD/YYYY parser).
        original = FdaRecord.model_validate(_SEED_INPUT)
        dump = original.model_dump(mode="json")
        rebuilt = reconstruct_record(dump)
        assert rebuilt.model_dump(mode="json") == dump

    def test_recovers_dropped_century_record(self) -> None:
        # The actual recovery case: a payload whose recall_initiation_dt year is a typo
        # (0013) but whose other dates are modern. Reconstruction must succeed and keep
        # the raw (uncorrected) year — bronze stores raw; repair is a silver concern.
        dump = FdaRecord.model_validate(
            {**_SEED_INPUT, "RECALLINITIATIONDT": "03/05/0013"}
        ).model_dump(mode="json")
        assert dump["recall_initiation_dt"].startswith("0013-")  # year survived the dump

        rebuilt = reconstruct_record(dump)
        assert rebuilt.recall_initiation_dt is not None
        assert rebuilt.recall_initiation_dt.year == 13
        assert rebuilt.source_recall_id == "116746"

    def test_round_trip_with_null_dates(self) -> None:
        row = {
            **_SEED_INPUT,
            "TERMINATIONDT": None,
            "ENFORCEMENTREPORTDT": None,
            "DETERMINATIONDT": None,
        }
        dump = FdaRecord.model_validate(row).model_dump(mode="json")
        rebuilt = reconstruct_record(dump)
        assert rebuilt.termination_dt is None
        assert rebuilt.model_dump(mode="json") == dump
