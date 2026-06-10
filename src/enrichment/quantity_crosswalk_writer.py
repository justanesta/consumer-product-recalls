"""Quantity-crosswalk writer (C13) — the I/O boundary around the pure ``quantity.parse_quantity``.

Reads the distinct FDA ``product_distributed_quantity`` + USDA ``qty_recovered`` strings from the
``stg_*`` views (the SAME source ``recall_product`` reads, so the join keys line up byte-for-byte),
parses each through ``parse_quantity``, and truncate-reloads ``quantity_crosswalk``. Silver
``recall_product`` LEFT JOINs it on ``number_of_units`` (the raw string) for the four structured
columns. Mirrors ``crosswalk_writer`` (firm resolution): ``build_quantity_crosswalk_rows`` is pure
(testable without a DB); ``write_quantity_crosswalk`` does the read/transform/write.

Run order (transform cron): ``dbt build --select staging`` -> ``recalls parse-quantities`` ->
``dbt build`` (recall_product joins the crosswalk). Idempotent — a deterministic full recompute, no
API/watermark side effects.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import sqlalchemy as sa

from src.enrichment.quantity import parse_quantity

if TYPE_CHECKING:
    from collections.abc import Iterable

    from sqlalchemy import Engine

# Distinct raw quantity strings, read AS-IS (staging already nullif'd ''), so raw_quantity equals
# recall_product.number_of_units exactly — no btrim/normalize here or the LEFT JOIN would miss
# whitespace-padded values.
_READ_DISTINCT = sa.text(
    """
    select distinct product_distributed_quantity as raw
    from stg_fda_recalls
    where product_distributed_quantity is not null
    union
    select distinct qty_recovered
    from stg_usda_fsis_recalls
    where qty_recovered is not null
    """
)

# Core Table for the batched insert (psycopg2 insertmanyvalues), mirroring crosswalk_writer /
# BronzeLoader — a raw text() INSERT would be one round-trip per row.
_metadata = sa.MetaData()
_quantity_crosswalk = sa.Table(
    "quantity_crosswalk",
    _metadata,
    sa.Column("raw_quantity", sa.Text),
    sa.Column("quantity_value", sa.Numeric),
    sa.Column("quantity_unit", sa.Text),
    sa.Column("quantity_category", sa.Text),
    sa.Column("quantity_basis", sa.Text),
)


@dataclass(frozen=True)
class QuantitySummary:
    """Outcome of a parse-quantities run."""

    distinct_values: int
    rows_written: int
    parsed_value: int  # rows with a non-null quantity_value
    parsed_unit: int  # rows with a non-null quantity_unit
    dry_run: bool


def build_quantity_crosswalk_rows(raw_values: Iterable[str]) -> list[dict[str, object]]:
    """Map distinct raw quantity strings to quantity_crosswalk row dicts (pure).

    De-dupes on the raw string (it is the primary key; FDA and USDA can share a value).
    """
    seen: set[str] = set()
    rows: list[dict[str, object]] = []
    for raw in raw_values:
        if raw is None or raw in seen:
            continue
        seen.add(raw)
        parsed = parse_quantity(raw)
        rows.append(
            {
                "raw_quantity": raw,
                "quantity_value": parsed.value,
                "quantity_unit": parsed.unit,
                "quantity_category": parsed.category,
                "quantity_basis": parsed.basis,
            }
        )
    return rows


def write_quantity_crosswalk(engine: Engine, *, dry_run: bool = False) -> QuantitySummary:
    """Read distinct staging quantity strings, parse, truncate-reload ``quantity_crosswalk``."""
    with engine.connect() as conn:
        raw_values = [row[0] for row in conn.execute(_READ_DISTINCT)]

    rows = build_quantity_crosswalk_rows(raw_values)
    parsed_value = sum(1 for r in rows if r["quantity_value"] is not None)
    parsed_unit = sum(1 for r in rows if r["quantity_unit"] is not None)

    if not dry_run:
        with engine.begin() as conn:
            conn.execute(sa.text("truncate table quantity_crosswalk"))
            if rows:
                conn.execute(_quantity_crosswalk.insert(), rows)

    return QuantitySummary(
        distinct_values=len(raw_values),
        rows_written=0 if dry_run else len(rows),
        parsed_value=parsed_value,
        parsed_unit=parsed_unit,
        dry_run=dry_run,
    )
