"""Firm-crosswalk writer (Phase 6b PRs 6b.1 + 6b.4) — the I/O boundary around the pure
``firm_normalization`` cleaning.

Reads the distinct firm-role names from ALL FIVE sources' staging views (CPSC's three
JSONB arrays, FDA ``firm_legal_nam``, USDA ``establishment``, NHTSA ``mfgname``/
``mfgtxt``, USCG's directory-coalesced anchor), maps each through ``clean_firm_name``
(universal DBA strip + a SOURCE-GATED geo-suffix strip — on for CPSC/NHTSA, off for the
FEI/establishment_number/MIC-backed sources, ADR 0037 amendment; NO parenthetical strip, so
paren-variants are left to RapidFuzz) plus ``extract_firm_dba`` / ``extract_paren_aliases``
for the alternate names, and (re)writes ``firm_crosswalk`` as a truncate-and-reload. The
crosswalk is keyed by
``firm_id = md5(upper(trim(raw_name)))`` — the SAME key silver ``firm.sql`` /
``recall_event_firm.sql`` compute — so they LEFT JOIN it to pick up
``canonical_firm_id`` (the cluster representative), ``clean_name``, ``alternate_names``,
and the resolution ``match_confidence``.

Why STAGING views, not bronze (6b.4 change from 6b.1's CPSC-only bronze read): the
silver firm models normalize on the *staging* output, and USDA HTML-entity decode +
USCG directory-coalesce happen in staging — reading bronze would desync the firm_id
join keys. ``stg_*`` are ``materialized: view`` in ``public`` (dbt_project.yml), so this
is a plain read. It does couple the resolver to "staging is built": run order is
``dbt build`` (staging) -> ``recalls resolve-firms`` -> ``dbt build`` (firm + bridge).

In 6b.1/6b.4-Increment-1 ``canonical_firm_id = md5(upper(trim(clean_name)))``
(cleaning-only: raw variants that clean to the same name share a canonical). PR 6b.4
Increment 2 overlays RapidFuzz clustering + the FDA FEI forced-merge edges, repointing
``canonical_firm_id`` to the cluster representative — same table, same join.

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
from sqlalchemy.dialects.postgresql import JSONB

from src.enrichment.firm_normalization import (
    clean_firm_name,
    extract_firm_dba,
    extract_paren_aliases,
)

if TYPE_CHECKING:
    from collections.abc import Sequence

    from sqlalchemy import Engine

# Bumped when the cleaning logic changes, so a crosswalk row records which resolver
# produced it (auditable rebuilds). v4 = source-gated geo-strip (ADR 0037 amendment: geo on
# for cpsc/nhtsa, off for fda/usda/uscg); 6b.4-Increment-2 stamps 'allsrc-clean+rapidfuzz-vN'.
RESOLVER_VERSION = "allsrc-clean-v4"

# Sources whose structured id (FEI / establishment_number / MIC) carries the authoritative
# within-source identity -> geo-strip OFF (ADR 0037 amendment). CPSC + NHTSA are name-only
# (no structured id) -> geo-strip ON (NHTSA guarded against the integral-name over-strip).
_GEO_OFF_SOURCES = frozenset({"fda", "usda", "uscg"})


def _geo_mode_for(sources: str) -> str:
    """Pick ONE geo-strip mode for a name from its comma-joined source-set.

    Precedence ``off > guarded > full``: a name carried by ANY structured-id source resolves
    to ``'off'`` (the structured-id source wins), so a name shared across a geo-on and a
    geo-off source still produces exactly ONE crosswalk row — no cross-source PK conflict by
    construction (the geo gate is the only source-dependent cleaning; DBA strip is universal,
    paren strip is nowhere). RapidFuzz recovers the demoted within-source merge later.
    """
    srcs = set(sources.split(","))
    if srcs & _GEO_OFF_SOURCES:
        return "off"
    if "nhtsa" in srcs:
        return "guarded"
    return "full"


# Distinct firm names across ALL FIVE sources: one row per upper(trim(name)) with a
# representative raw AND its source-set (string_agg), so the resolver picks one geo-mode per
# name via _geo_mode_for. Reads the STAGING views (not bronze) so firm_norm is byte-identical
# to firm.sql's normalized_name (USDA HTML-decode + USCG directory-coalesce happen in staging;
# staging also already projects latest-per-recall, so no row_number() here). The five-source
# union + coalesce MIRRORS firm.sql's all_normalized CTE EXACTLY — keep them in lockstep or
# firm_id join keys diverge.
_DISTINCT_FIRM_NAMES = sa.text(
    """
    with names as (
        -- CPSC: manufacturers / importers / distributors JSONB arrays.
        select 'cpsc' as source, e.value ->> 'name' as raw
        from stg_cpsc_recalls c, jsonb_array_elements(coalesce(c.manufacturers, '[]'::jsonb)) e
        union all
        select 'cpsc', e.value ->> 'name'
        from stg_cpsc_recalls c, jsonb_array_elements(coalesce(c.importers, '[]'::jsonb)) e
        union all
        select 'cpsc', e.value ->> 'name'
        from stg_cpsc_recalls c, jsonb_array_elements(coalesce(c.distributors, '[]'::jsonb)) e
        union all
        -- FDA: recalling establishment legal name.
        select 'fda', firm_legal_nam
        from stg_fda_recalls
        where firm_legal_nam is not null
        union all
        -- USDA: free-text FSIS establishment (HTML-decoded in staging).
        select 'usda', establishment
        from stg_usda_fsis_recalls
        where establishment is not null
        union all
        -- NHTSA: filer (mfgname) + manufacturer (mfgtxt), the post-#58 two-firm split.
        select 'nhtsa', mfgname from stg_nhtsa_recalls where mfgname is not null
        union all
        select 'nhtsa', mfgtxt from stg_nhtsa_recalls where mfgtxt is not null
        union all
        -- USCG: directory.company_name > recalls.company_name > mic (firm.sql priority).
        select 'uscg', coalesce(m.company_name, r.company_name, r.mic)
        from stg_uscg_recalls r
        left join stg_uscg_manufacturers m
            on upper(trim(r.mic)) = upper(trim(m.mic))
    )
    select upper(trim(raw)) as firm_norm,
           min(raw) as rep_raw,
           string_agg(distinct source, ',' order by source) as sources
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
    # none_as_null=True: persist Python None as SQL NULL, NOT JSONB 'null' (the default) —
    # else firm.sql's jsonb_array_elements_text errors on the no-alias rows.
    sa.Column("alternate_names", JSONB(none_as_null=True)),
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
    alias_count: int
    dry_run: bool


def build_crosswalk_rows(triples: Sequence[tuple[str, str, str]]) -> list[dict[str, object]]:
    """Map (firm_norm, rep_raw, sources) triples to firm_crosswalk row dicts (pure).

    ``firm_norm`` is the SQL-produced ``upper(trim(name))`` (the firm_id input, byte-
    identical to silver ``md5(upper(trim(name)))``); ``rep_raw`` a representative
    original-case name; ``sources`` the comma-joined source-set. Each name is cleaned by
    ``clean_firm_name`` with the geo-strip GATED per source (``_geo_mode_for(sources)``;
    DBA strip is universal, paren strip is nowhere), so ``canonical_firm_id = md5(cleaned)``
    and raw variants that clean to one name share a canonical. ``alternate_names`` collects
    the DBA brand + any brand-bearing parentheticals (``extract_paren_aliases``) for search /
    fuzzy-match recall. ``match_confidence``: DBA > geo-suffix (only when geo ACTUALLY fired,
    not mere whitespace/case normalization) > exact_name; 6b.4 Increment 2's RapidFuzz pass
    overlays the fuzzy tiers + FEI forced-merges on top.
    """
    rows: list[dict[str, object]] = []
    for firm_norm, rep_raw, sources in triples:
        geo_mode = _geo_mode_for(sources)
        dba = extract_firm_dba(rep_raw)
        clean = clean_firm_name(rep_raw, geo_mode=geo_mode)
        clean_norm = clean.upper().strip() or firm_norm
        # alternate_names: DBA brand first, then brand-bearing parentheticals; de-duplicated.
        aliases: list[str] = []
        seen_alias: set[str] = set()
        for alias in ([dba] if dba else []) + extract_paren_aliases(rep_raw):
            if alias.upper() not in seen_alias:
                seen_alias.add(alias.upper())
                aliases.append(alias)
        if dba:
            confidence = "dba_extract_exact"
        elif clean != clean_firm_name(rep_raw, geo_mode="off"):
            # geo strip ACTUALLY changed the name (vs a geo-off clean) — not just whitespace.
            confidence = "geo_suffix_strip_exact"
        else:
            confidence = "exact_name"
        rows.append(
            {
                "firm_id": _md5(firm_norm),
                "canonical_firm_id": _md5(clean_norm),
                "canonical_name": clean or rep_raw,
                "clean_name": clean or rep_raw,
                "alternate_names": aliases or None,
                "match_confidence": confidence,
                "match_score": None,
                "resolver_version": RESOLVER_VERSION,
            }
        )
    return rows


def resolve_firm_crosswalk(engine: Engine, *, dry_run: bool = False) -> ResolveSummary:
    """Rebuild firm_crosswalk from the current all-source staging names (truncate-reload).

    Requires the ``stg_*`` views to be built (run after ``dbt build`` of staging).
    """
    with engine.connect() as conn:
        triples = [
            (row.firm_norm, row.rep_raw, row.sources) for row in conn.execute(_DISTINCT_FIRM_NAMES)
        ]

    rows = build_crosswalk_rows(triples)
    # cleaned = names whose canonical differs from their own firm_id, i.e. cleaning merged
    # them into another name's canonical (the deterministic-floor collapse count).
    cleaned = sum(1 for r in rows if r["canonical_firm_id"] != r["firm_id"])
    aliased = sum(1 for r in rows if r["alternate_names"])

    if not dry_run:
        with engine.begin() as conn:
            conn.execute(sa.text("truncate table firm_crosswalk"))
            if rows:
                conn.execute(_firm_crosswalk.insert(), rows)

    return ResolveSummary(
        distinct_names=len(triples),
        rows_written=0 if dry_run else len(rows),
        cleaned_count=cleaned,
        alias_count=aliased,
        dry_run=dry_run,
    )
