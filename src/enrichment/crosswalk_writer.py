"""Firm-crosswalk writer (Phase 6b PR 6b.1) — the I/O boundary around the pure
``firm_normalization`` cleaning.

Reads the distinct CPSC firm-role names from ``cpsc_recalls_bronze`` (latest version
per source_recall_id), maps each through ``clean_firm_name`` / ``extract_firm_dba``,
and (re)writes ``firm_crosswalk`` as a truncate-and-reload. The crosswalk is keyed by
``firm_id = md5(upper(trim(raw_name)))`` — the SAME key silver ``firm.sql`` /
``recall_event_firm.sql`` compute — so they LEFT JOIN it to pick up
``canonical_firm_id`` (the cluster representative), ``clean_name``, ``extracted_dba``,
and the resolution ``match_confidence``.

In 6b.1 ``canonical_firm_id = md5(upper(trim(clean_name)))`` (cleaning-only: raw
variants that clean to the same name share a canonical). PR 6b.4 extends this writer
to overlay RapidFuzz clustering — same table, same join.

``build_crosswalk_rows`` is pure (testable without a DB); ``resolve_firm_crosswalk``
does the read/transform/write. The md5 is computed over the SQL-produced
``upper(trim(name))`` string, byte-identical to Postgres ``md5(upper(trim(...)))`` in
the silver models, so the JOIN keys match.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import TYPE_CHECKING

import sqlalchemy as sa

from src.enrichment.firm_normalization import clean_firm_name, extract_firm_dba

if TYPE_CHECKING:
    from collections.abc import Sequence

    from sqlalchemy import Engine

# Bumped when the cleaning logic changes, so a crosswalk row records which resolver
# produced it (auditable rebuilds). 6b.4 introduces a 'cpsc-clean+rapidfuzz-vN'.
RESOLVER_VERSION = "cpsc-clean-v1"

# Distinct CPSC firm names: one row per upper(trim(name)) with a representative raw.
# Latest version per source_recall_id (mirrors stg_cpsc_recalls; for the single-shot
# seed it is a no-op, but stays correct if incrementals bank edits).
_DISTINCT_CPSC_NAMES = sa.text(
    """
    with latest as (
        select manufacturers, importers, distributors,
               row_number() over (
                   partition by source_recall_id order by extraction_timestamp desc
               ) as rn
        from cpsc_recalls_bronze
    ),
    names as (
        select e.value ->> 'name' as raw
        from latest l, jsonb_array_elements(coalesce(l.manufacturers, '[]'::jsonb)) e
        where l.rn = 1
        union all
        select e.value ->> 'name'
        from latest l, jsonb_array_elements(coalesce(l.importers, '[]'::jsonb)) e
        where l.rn = 1
        union all
        select e.value ->> 'name'
        from latest l, jsonb_array_elements(coalesce(l.distributors, '[]'::jsonb)) e
        where l.rn = 1
    )
    select upper(trim(raw)) as firm_norm, min(raw) as rep_raw
    from names
    where nullif(trim(raw), '') is not null
    group by upper(trim(raw))
    """
)

# SQLAlchemy Core table for the BATCHED insert. A Core Table.insert() construct (NOT a
# raw text() INSERT) lets SQLAlchemy rewrite the executemany into multi-row VALUES
# batches (psycopg2 insertmanyvalues). A raw text() INSERT falls back to one network
# round-trip per row — ~10k slow round-trips to Neon. Mirrors BronzeLoader's bulk load.
_metadata = sa.MetaData()
_firm_crosswalk = sa.Table(
    "firm_crosswalk",
    _metadata,
    sa.Column("firm_id", sa.Text),
    sa.Column("canonical_firm_id", sa.Text),
    sa.Column("canonical_name", sa.Text),
    sa.Column("clean_name", sa.Text),
    sa.Column("extracted_dba", sa.Text),
    sa.Column("match_confidence", sa.Text),
    sa.Column("match_score", sa.Numeric),
    sa.Column("resolver_version", sa.Text),
)


def _md5(text: str) -> str:
    # Non-cryptographic identity hash; mirrors Postgres md5() used in the silver
    # firm models so the crosswalk JOIN keys line up.
    return hashlib.md5(text.encode("utf-8")).hexdigest()  # noqa: S324


@dataclass(frozen=True)
class ResolveSummary:
    """Outcome of a resolve-firms run."""

    distinct_names: int
    rows_written: int
    cleaned_count: int
    dba_count: int
    dry_run: bool


def build_crosswalk_rows(pairs: Sequence[tuple[str, str]]) -> list[dict[str, object]]:
    """Map (firm_norm, rep_raw) pairs to firm_crosswalk row dicts (pure).

    ``firm_norm`` is the SQL-produced ``upper(trim(name))`` (the firm_id input);
    ``rep_raw`` a representative original-case name to clean for display.
    """
    rows: list[dict[str, object]] = []
    for firm_norm, rep_raw in pairs:
        clean = clean_firm_name(rep_raw)
        clean_norm = clean.upper().strip() or firm_norm
        dba = extract_firm_dba(rep_raw)
        if dba:
            confidence = "cpsc_dba_extract_exact"
        elif clean_norm != firm_norm:
            confidence = "cpsc_suffix_strip_exact"
        else:
            confidence = "exact_name"
        rows.append(
            {
                "firm_id": _md5(firm_norm),
                "canonical_firm_id": _md5(clean_norm),
                "canonical_name": clean or rep_raw,
                "clean_name": clean or rep_raw,
                "extracted_dba": dba,
                "match_confidence": confidence,
                "match_score": None,
                "resolver_version": RESOLVER_VERSION,
            }
        )
    return rows


def resolve_firm_crosswalk(engine: Engine, *, dry_run: bool = False) -> ResolveSummary:
    """Rebuild firm_crosswalk from the current CPSC bronze names (truncate-and-reload)."""
    with engine.connect() as conn:
        pairs = [(row.firm_norm, row.rep_raw) for row in conn.execute(_DISTINCT_CPSC_NAMES)]

    rows = build_crosswalk_rows(pairs)
    cleaned = sum(1 for r in rows if r["match_confidence"] == "cpsc_suffix_strip_exact")
    dba = sum(1 for r in rows if r["extracted_dba"] is not None)

    if not dry_run:
        with engine.begin() as conn:
            conn.execute(sa.text("truncate table firm_crosswalk"))
            if rows:
                conn.execute(_firm_crosswalk.insert(), rows)

    return ResolveSummary(
        distinct_names=len(pairs),
        rows_written=0 if dry_run else len(rows),
        cleaned_count=cleaned,
        dba_count=dba,
        dry_run=dry_run,
    )
