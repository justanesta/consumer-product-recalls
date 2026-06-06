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
from collections import defaultdict
from dataclasses import dataclass
from typing import TYPE_CHECKING

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

from src.enrichment import firm_resolution as fr
from src.enrichment.firm_normalization import (
    clean_firm_name,
    extract_firm_dba,
    extract_paren_aliases,
)
from src.enrichment.never_merge import NEVER_MERGE

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

# FDA FEI identity rows (silver firm_fei_edges, built by dbt). current_fei =
# coalesce(surviving_fei, fei) is FDA's own post-rename establishment id; firm_id mirrors this
# resolver's md5(upper(trim(name))) so the Tier-0 current-FEI groups land on crosswalk rows.
# Prerequisite: `dbt build --select firm_fei_edges` before resolve-firms.
_FEI_EDGES = sa.text("select firm_id, current_fei, current_name from firm_fei_edges")

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
    fei_merged: int
    fuzzy_merged: int
    fei_gated: int
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


def apply_clustering(
    rows: list[dict[str, object]],
    fei_rows: Sequence[tuple[str, str, str]],
    *,
    rollup: bool = True,
    fei_merge: bool = False,
    typo_threshold: float = fr.TYPO_THRESHOLD,
    rollup_threshold: float = fr.ROLLUP_THRESHOLD,
) -> tuple[int, int, int]:
    """Overlay the name-grain resolution onto the deterministic rows (pure).

    The clustering universe is the DISTINCT clean names (the nodes ``firm.sql`` regroups on).
    Tier 1 = name-variant / typo repair; Tier 2 (``rollup``) = >=2-token entity rollup. This is the
    SHIPPED firm grain: name/brand cluster, uniform with the other four sources (whose structured
    ids are attributes, not merge keys). Tier 0 = FDA FEI grouping is **opt-in** (``fei_merge``,
    default OFF) and DEFERRED — establishment-grain FEI chains unrelated firms across owner changes
    (ADR 0037); FEI rides on ``firm.observed_company_ids`` as an attribute regardless. For every row
    whose clean name lands in a multi-member cluster, repoints ``canonical_firm_id`` and stamps the
    tier's ``match_confidence`` (+ ``match_score`` for score-based tiers); singletons keep their
    deterministic canonical + confidence. Mutates ``rows`` in place; returns
    ``(fei_merged, fuzzy_merged, fei_gated)`` counts (the FEI ones are 0 unless ``fei_merge``).
    """

    def node_of(row: dict[str, object]) -> str:
        return str(row["clean_name"]).upper().strip()

    clean_of_firm_id: dict[str, str] = {}
    display_of: dict[str, str] = {}
    for r in rows:
        cn = node_of(r)
        clean_of_firm_id[str(r["firm_id"])] = cn
        disp = str(r["clean_name"])
        if cn not in display_of or disp < display_of[cn]:
            display_of[cn] = disp  # stable original-case representative for the canonical_name

    if fei_merge:
        must_link, fei_gated = fr.fei_resolve(fei_rows, clean_of_firm_id)
    else:
        must_link, fei_gated = [], 0  # Tier 0 deferred: FEI is an attribute, not a merge key
    assignment = fr.cluster_names(
        sorted(display_of),
        must_link,
        rollup=rollup,
        typo_threshold=typo_threshold,
        rollup_threshold=rollup_threshold,
        forbid=NEVER_MERGE,
    )

    # v4 (6b.6): place/generic Tier-2 denylist split + never_merge override + identical-set guard.
    tiers = ("0" if fei_merge else "") + "1" + ("2" if rollup else "")
    version = f"allsrc-tier{tiers}-roll{int(rollup_threshold)}-v4"
    fei_merged = fuzzy_merged = 0
    for r in rows:
        r["resolver_version"] = version
        a = assignment.get(node_of(r))
        if a is None or a.method == "singleton":
            continue  # keep the deterministic canonical + confidence
        r["canonical_firm_id"] = _md5(a.canonical)
        r["canonical_name"] = display_of.get(a.canonical, a.canonical)
        r["match_confidence"] = a.method
        r["match_score"] = round(a.score, 1) if a.score is not None else None
        if a.method == "fei_exact":
            fei_merged += 1
        else:
            fuzzy_merged += 1
    return fei_merged, fuzzy_merged, fei_gated


def resolve_firm_crosswalk(
    engine: Engine,
    *,
    dry_run: bool = False,
    rollup: bool = True,
    fei_merge: bool = False,
    rollup_threshold: float = fr.ROLLUP_THRESHOLD,
) -> ResolveSummary:
    """Rebuild firm_crosswalk: deterministic clean + name-grain resolution (truncate-reload).

    Requires the ``stg_*`` views (run after ``dbt build --select staging``). ``rollup`` toggles
    Tier 2 (entity rollup, default on); Tier 1 (name repair) always runs. ``fei_merge`` (default
    OFF, deferred — ADR 0037) opts into Tier 0 FDA FEI grouping and additionally requires
    ``firm_fei_edges``; the shipped name/brand grain does not read it.
    """
    with engine.connect() as conn:
        triples = [
            (row.firm_norm, row.rep_raw, row.sources) for row in conn.execute(_DISTINCT_FIRM_NAMES)
        ]
        fei_rows = (
            [(row.firm_id, row.current_fei, row.current_name) for row in conn.execute(_FEI_EDGES)]
            if fei_merge
            else []
        )

    rows = build_crosswalk_rows(triples)
    fei_merged, fuzzy_merged, fei_gated = apply_clustering(
        rows, fei_rows, rollup=rollup, fei_merge=fei_merge, rollup_threshold=rollup_threshold
    )
    # cleaned = rows whose canonical differs from their own firm_id (cleaning + clustering).
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
        fei_merged=fei_merged,
        fuzzy_merged=fuzzy_merged,
        fei_gated=fei_gated,
        dry_run=dry_run,
    )


# Every crosswalk row in a canonical that used the Tier-2 (rapidfuzz_rollup) path — the reviewable
# universe (6b.6 audit). Pulls all members of a rollup-bearing canonical (incl. its name_variant
# members) so the review sees the whole cluster.
_ROLLUP_CLUSTERS = sa.text(
    """
    select clean_name, canonical_firm_id, canonical_name, match_score
    from firm_crosswalk
    where canonical_firm_id in (
        select canonical_firm_id from firm_crosswalk where match_confidence = 'rapidfuzz_rollup'
    )
    """
)


def audit_rollup_clusters(
    engine: Engine, *, reviewed_ok: frozenset[str] = frozenset()
) -> list[fr.RollupReview]:
    """Read firm_crosswalk and rank its Tier-2 clusters by false-merge suspicion (6b.6 review loop).

    Groups the rollup-bearing clusters by ``canonical_firm_id``, collects DISTINCT clean names + the
    rollup score, and delegates the risk ranking to ``firm_resolution.review_rollup_clusters``.
    ``reviewed_ok`` (confirmed-legit cluster signatures) drop out, so the report shows only the
    NEW/unreviewed merges. Read-only — feeds the ``recalls audit-firm-rollups`` report + GHA cron.
    """
    by_canon: dict[str, dict[str, float | None]] = defaultdict(dict)
    canon_name: dict[str, str] = {}
    with engine.connect() as conn:
        for row in conn.execute(_ROLLUP_CLUSTERS):
            cid = str(row.canonical_firm_id)
            name = str(row.clean_name)
            score = float(row.match_score) if row.match_score is not None else None
            if name not in by_canon[cid] or by_canon[cid][name] is None:
                by_canon[cid][name] = score  # prefer a non-null rollup score for the name
            canon_name.setdefault(cid, str(row.canonical_name))
    clusters = [(canon_name[cid], list(members.items())) for cid, members in by_canon.items()]
    return fr.review_rollup_clusters(clusters, reviewed_ok=reviewed_ok)
