from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime, timedelta

from pydantic import BaseModel

from src.extractors._base import QuarantineRecord

_MAX_RECALL_AGE_DAYS = 70 * 365


def check_null_source_id(source_recall_id: str | None) -> str | None:
    """
    Return a failure reason if source_recall_id is null or empty, else None.
    Applied to all sources — records without an ID cannot be content-hash-deduped
    (ADR 0007) and cannot be traced in the _rejected table (ADR 0013).
    """
    if not source_recall_id or not source_recall_id.strip():
        return "source_recall_id is null or empty"
    return None


def check_date_sanity(
    dt: datetime | None,
    field_name: str = "published_at",
) -> str | None:
    """
    Return a failure reason if dt is in the future or more than 70 years in the past.
    Applied to all sources as a sanity guard (ADR 0013).
    dt must be timezone-aware; naive datetimes are forbidden by bronze Pydantic schemas.
    """
    if dt is None:
        return None
    now = datetime.now(UTC)
    if dt > now:
        return f"{field_name} is in the future: {dt.isoformat()}"
    if dt < now - timedelta(days=_MAX_RECALL_AGE_DAYS):
        return f"{field_name} is more than 70 years in the past: {dt.isoformat()}"
    return None


def check_usda_bilingual_pairing[T: BaseModel](
    records: list[T],
    *,
    recall_number_fn: Callable[[T], str],
    is_spanish_fn: Callable[[T], bool],
    raw_landing_path: str,
) -> tuple[list[T], list[QuarantineRecord]]:
    """
    Quarantine Spanish records that have no English counterpart in the batch.

    USDA publishes each recall as two API records — one English, one Spanish — sharing
    the same field_recall_number. If a Spanish record arrives without its English sibling,
    the silver builder cannot collapse them correctly (ADR 0006 edge case). The Spanish
    record is quarantined; the silver pipeline will pick it up on the next successful
    extraction cycle once the English counterpart appears.

    English records always pass through.
    Spanish records without an English sibling in the same batch are quarantined with
    failure_stage='invariants'.

    Args:
        records: Full batch of validated USDA records for this extraction run.
        recall_number_fn: Extracts the shared recall number (field_recall_number).
        is_spanish_fn: Returns True if the record is the Spanish variant.
        raw_landing_path: Stored on quarantine rows for traceability.
    """
    english_recall_numbers: set[str] = {
        recall_number_fn(r) for r in records if not is_spanish_fn(r)
    }

    passing: list[T] = []
    quarantined: list[QuarantineRecord] = []

    for record in records:
        if not is_spanish_fn(record):
            passing.append(record)
            continue

        recall_number = recall_number_fn(record)
        if recall_number in english_recall_numbers:
            passing.append(record)
        else:
            quarantined.append(
                QuarantineRecord(
                    source_recall_id=recall_number,
                    raw_record=record.model_dump(mode="json"),
                    failure_reason="Spanish record has no English counterpart in batch",
                    failure_stage="invariants",
                    raw_landing_path=raw_landing_path,
                )
            )

    return passing, quarantined


# --------------------------------------------------------------------------------------
# Per-record invariant registry
#
# Each extractor's check_invariants() used to inline its own `check_null_source_id(...) or
# check_date_sanity(record.<field>, "<field>")` chain. The registry below makes the
# per-source invariant *set* declarative — a plain tuple of bound callables, NOT a config
# DSL — so the set is reviewable in one place and the date-field-name divergence is
# explicit. check_usda_bilingual_pairing stays a separate batch-level call (it needs the
# whole batch), not a per-record invariant.
# --------------------------------------------------------------------------------------

PerRecordInvariant = Callable[[BaseModel], str | None]


def _null_id_invariant(record: BaseModel) -> str | None:
    """Per-record wrapper around :func:`check_null_source_id`."""
    return check_null_source_id(getattr(record, "source_recall_id", None))


def _date_sanity_invariant(field_name: str) -> PerRecordInvariant:
    """Build a :func:`check_date_sanity` invariant bound to one source's date field.

    The field name varies per source (CPSC ``recall_date``, NHTSA ``rcdate``, USCG
    ``opened_on``, ...), so it is closed over here rather than encoded in a config DSL.
    """

    def _invariant(record: BaseModel) -> str | None:
        return check_date_sanity(getattr(record, field_name), field_name)

    return _invariant


def run_per_record_invariants(
    record: BaseModel, invariants: tuple[PerRecordInvariant, ...]
) -> str | None:
    """Return the first failing invariant's reason, or None if all pass.

    Mirrors the short-circuit ``A or B`` chaining each extractor used inline.
    """
    for invariant in invariants:
        failure = invariant(record)
        if failure is not None:
            return failure
    return None


# Keys MUST match EXTRACTOR_BY_SOURCE_NAME (a test asserts this).
PER_RECORD_INVARIANTS_BY_SOURCE_NAME: dict[str, tuple[PerRecordInvariant, ...]] = {
    "cpsc": (_null_id_invariant, _date_sanity_invariant("recall_date")),
    "fda": (_null_id_invariant, _date_sanity_invariant("recall_initiation_dt")),
    "usda": (_null_id_invariant, _date_sanity_invariant("recall_date")),
    "nhtsa": (_null_id_invariant, _date_sanity_invariant("rcdate")),
    "uscg": (_null_id_invariant, _date_sanity_invariant("opened_on")),
    # The three sources below expose no recall-publication timestamp — only nullable /
    # administrative dates — so date_sanity deliberately does NOT apply (see each
    # extractor's check_invariants). Listed explicitly so the omission is a reviewed
    # choice, not a silent gap.
    "usda_establishments": (_null_id_invariant,),
    "uscg_manufacturers": (_null_id_invariant,),
    "uscg_manufacturer_details": (_null_id_invariant,),
}
