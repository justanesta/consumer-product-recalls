# 0037 — Firm resolution runs as a Python stage, not in-warehouse SQL/pg_trgm

- **Status:** Accepted
- **Date:** 2026-06-04

> Decided during Phase 6b design (2026-06-03, the "Lane-F hybrid" in
> `project_scope/phase-6b-execution-plan.md` §2) and implemented across PRs 6b.1 + 6b.4;
> formalized here so the rejected SQL-fuzzy alternative is on record with its reasoning.
>
> **Amendment 2026-06-04 (same-day refinement, pre-merge):** the deterministic cleaner does
> NOT strip parentheticals. A cross-source blast-radius review
> (`scripts/sql/cross_source/silver/probe_cleaning_blast_radius_by_source.sql`) showed a
> blanket paren strip is too blunt — abbreviation-prefix over-truncation
> (`FENGM (Hong Kong Fengmang International Co. Ltd.)` → `FENGM`), brand loss, and `(DBA)`
> mashups. This *reinforces* the decision below: paren-VARIANTS are left to RapidFuzz (the
> layer built for soft variant matching), and only paren-BRANDS are lifted into
> `firm.alternate_names` (`extract_paren_aliases`).
>
> **Geo-strip is SOURCE-GATED** (refined the same day by a per-source empirical + adversarial
> workflow over the dumped corpus): ON for the name-only sources `{cpsc, nhtsa}`, OFF for the
> structured-id sources `{fda, usda, uscg}` whose FEI / establishment_number / MIC carry the
> authoritative within-source identity (geo-strip over-strips their integral "X of <State>"
> names — `BLOODCENTER OF WISCONSIN` → `BLOODCENTER` — and wrong-merges distinct
> establishments). NHTSA geo is *guarded* — it never reduces a name to a bare single token
> (`WINNEBAGO OF INDIANA` → `WINNEBAGO` would collide with the distinct `WINNEBAGO INDUSTRIES
> INC.`) while still stripping the multi-token dealer cohort (`AUTO TRIM DESIGN OF TEXAS` →
> `AUTO TRIM DESIGN`). The gate is a single source-membership check inside the one shared
> cleaner driven by a source tag threaded through the resolver's name query — the global
> `md5(raw_name)` crosswalk key, `firm.sql`, and the lockstep coupling are UNTOUCHED (a
> source-aware composite key was rejected: it would fragment the Honda/Tyson cross-source
> rollups that are `firm.sql`'s headline goal). A name shared across a geo-on and a geo-off
> source resolves to geo-off (structured-id source wins), so there is no cross-source PK
> conflict; the rare demoted within-source merge (the "Pfizer class") is recovered by
> RapidFuzz. DBA-strip stays universal; paren-strip stays nowhere.
>
> **Amendment 2026-06-04 (tiered resolution + FEI reframe — replaces the first clusterer):** the
> initial clusterer (single rule: `token_set_ratio` subset ⇒ merge, guarded by a document-
> frequency hub cutoff, plus an *any-shared-FEI* + surviving-FEI must-link) **catastrophically
> over-merged** on a real resolve and is replaced. Two root causes, both empirically reproduced
> on the 28,599-name corpus: (1) **DF cannot separate a brand from a common word at the same
> frequency** (`KAWASAKI` df 18 ≈ `TRUE` df 20), so the subset rule + DF guard chained dozens of
> hubs (`TRUE`/`SUN`/`York`/`ATLAS`/single-letter `K&B`); (2) **an FEI is an *establishment*
> (facility) id, not a firm id** — permanent per physical location, reassigned on ownership/
> operational change — so an unconditional shared-FEI/succession merge welded unrelated firms
> (`Whole Foods` = `Stryker`, `Teva` = `Bayer`) via shared registrants / sentinels / succession
> chains. The replacement is **three tiers** (`src/enrichment/firm_resolution.py`):
>
> - **Tier 0 — FEI (deterministic).** Group FDA names by `current_fei = coalesce(firm_surviving_fei,
>   firm_fei_num)` (FDA's own rename resolution; `firmsurvivingfei` is the current FEI "if changed
>   since the recall"). A current-FEI fanning out past `FEI_FANOUT_CAP` (6) distinct names is a
>   registrant/facility/sentinel → gated. FEI stays an *attribute* + resolution input, never a key.
> - **Tier 1 — name repair (always on).** Merge identical distinctive-token sets (corp-forms
>   dropped, **content words kept** — `Sun Valley Foods` ≠ `Sun`) or `token_sort_ratio` typos.
> - **Tier 2 — entity rollup (optional, `--rollup`).** Merge ≥2 shared distinctive multi-char
>   tokens above `token_set_ratio` threshold, **refused when every shared token is a place /
>   common compound** (`src/enrichment/place_words.py`). A **place denylist beats a brand
>   allowlist** here: the residual false-merge mode is geographic 2-token coincidence
>   (`San Antonio Bakery` + `…Eye Bank`), a finite, gazetteer-seeded, *proactive* vocabulary that
>   does not balloon — whereas an allowlist is large and reactive (rejected).
>
> Load-bearing subtlety: **two stop-sets** — blocking + Tier-2 scoring drop high-DF boilerplate;
> Tier-1's identical-set test drops corp-forms only (else common nouns dropped as "generic"
> re-introduce the hub class). Measured: Tier 0+1 → ~6,000 names consolidated, 17 clusters ≥6 (all
> legit); Tier 2 adds ~1,300 more (recognizable brand families: Coca-Cola, Kawasaki, P&G…), ~90%
> precise pre-denylist. The match_confidence vocabulary is now `fei_exact` / `name_variant_exact`
> / `name_typo_high` / `rapidfuzz_rollup` / `singleton`. The Python-stage + additive-canonical
> decision below is UNCHANGED; only the in-stage algorithm changed.

## Context

The firm dimension (ADR 0002) dedups firms by exact normalized name (`md5(upper(trim(name)))`).
Cross-source firm entity resolution (Phase 6b) has to collapse variants exact-match cannot —
three classes of work:

1. **Deterministic name cleaning** — trailing geographic suffix (`"Fisher-Price of East
   Aurora, N.Y." → "Fisher-Price"`), DBA-clause extraction, balanced-parenthetical strip
   (`"CHRYSLER (FCA US, LLC)" → "CHRYSLER"`).
2. **Fuzzy clustering** — edit-distance over multi-token company names where the right
   model is token-set similarity, not character distance: `HONDA` ↔ `AMERICAN HONDA MOTOR
   CO` (subset), `TOYOTA MOTOR ENGINEERING & MANUFACTURING` ↔ `TOYOTA MOTOR CORPORATION`
   (reorder + shared boilerplate).
3. **Deterministic forced-merges** — FDA assigns each establishment a government FEI; a
   shared FEI is an authoritative same-firm signal that must override fuzzy scoring.

The warehouse is Neon Postgres (ADR 0005); transformations are dbt-core (ADR 0011). Where
should this resolution run? Two hard infrastructure constraints frame the answer:

- **Neon has no fuzzy-match extensions enabled.** `pg_trgm` / `fuzzystrmatch` are absent
  from all project code and there is no `CREATE EXTENSION` anywhere — by deliberate
  posture (keep the warehouse vanilla so local Postgres / DuckDB stay viable, per the
  project cost constraints).
- **Neon has no dbt-python runtime.** dbt-python models need in-warehouse Python
  (Snowpark / BigQuery / Databricks); Postgres has none. So RapidFuzz cannot run *inside*
  dbt at all.

| Option | Notes |
|---|---|
| **Python stage + dbt source (chosen)** | Tested `src/enrichment/` module (RapidFuzz `token_set_ratio` + union-find + FEI forced-merges), user-run `recalls resolve-firms` CLI writing `firm_crosswalk`, registered back as a dbt source. Deterministic set-based work stays in dbt-SQL. |
| **Pure dbt-SQL + pg_trgm/fuzzystrmatch** | Would keep everything in dbt (lineage, ordering, freshness for free) — but neither extension does token-set matching, and clustering is graph work. See "rejected" below. |
| **dbt-python model** | Best of both (algorithm + lineage) — but needs an in-warehouse Python runtime Neon does not have. Infeasible here. |
| **Pure Python (all transforms)** | Over-engineers the 99% deterministic, set-based work the warehouse does better; forfeits dbt lineage/tests for the parts that belong in SQL. |

## Decision

**Hybrid, with the seam drawn where SQL stops being the right tool.** Deterministic,
set-based transformation stays in dbt-SQL/staging. The genuinely algorithmic work — fuzzy
scoring, clustering, and FEI forced-merges — runs in a tested Python module driven by a
user-run CLI that writes a table registered back as a dbt source:

- `src/enrichment/firm_normalization.py` — pure cleaning (`clean_firm_name` = geo-suffix +
  DBA strip, applied cross-source; `extract_firm_dba` / `extract_paren_aliases` lift brand
  aliases). No parenthetical strip (see the 2026-06-04 amendment above).
- `src/enrichment/firm_resolution.py` — pure **tiered** resolution over a blocking index (see the
  2026-06-04 tiered-resolution amendment above): Tier 0 FDA current-FEI grouping (fan-out gated),
  Tier 1 identical-distinctive-set / `token_sort_ratio` name repair, Tier 2 optional `token_set_ratio`
  entity rollup with a place-word guard (`place_words.py`). Tier 2's residual is reviewed/tuned via
  `verify_fuzzy_clusters.sql`; Tier 0+1 are precision-safe and always on.
- `src/enrichment/crosswalk_writer.py` — the I/O boundary (pure `build_crosswalk_rows`
  separated from `resolve_firm_crosswalk`, mirroring the extractor `_parse_*` split).
- `recalls resolve-firms` (Typer CLI, USER-run) — reads the all-source distinct names from
  the `stg_*` views, truncate-and-reloads `firm_crosswalk`, stamps `resolver_version`.

The silver firm models LEFT JOIN `firm_crosswalk` (a dbt source) for an **additive**
`canonical_firm_id = coalesce(crosswalk.canonical_firm_id, md5(normalized_name))`. `firm_id`
stays `md5(normalized_name)`; fuzzy merges express only through the additive canonical.

The deterministic *cleaning* also lives in the Python module — not because SQL could not do
it, but to co-locate the whole `raw → clean → cluster → canonical` surface, and because the
geo-anchored vocabulary + blocklist logic is clean in Python and gnarly as `regexp_replace`
(consistent with ADR 0027's principle that deterministic normalization is a silver concern;
this scopes the *venue* to Python for the resolution surface specifically).

## Rejected: pure dbt-SQL with pg_trgm / fuzzystrmatch

On record because it is the obvious alternative and will be re-litigated. Enabling the
extensions makes a pure-SQL path *possible* but, for this problem, not *better*:

- **Neither extension does `token_set_ratio`.** `pg_trgm` is character-trigram — strong for
  typos, weak on the exact cases here: token reorder and subset (`HONDA` ⊂ `AMERICAN HONDA
  MOTOR CO` scores *low* on trigram `similarity()`, *high* on `token_set_ratio`).
  `fuzzystrmatch.levenshtein` is character edit-distance (same reorder/subset weakness) and
  is unindexable. Matching this data shape in SQL means reimplementing token-set scoring
  (tokenize → dedupe → sort → set-ops) — which buries the algorithm.
- **Clustering is still not set-based.** Turning pairwise scores into firm clusters (with
  the FEI must-link constraints) is graph connected-components → a recursive CTE doing
  label propagation, hard to unit-test, with the transitive-over-merge trap (A~B, B~C,
  A≁C) to guard by hand. The union-find + cycle guard is a tested ~50-line pure function in
  Python.
- **pg_trgm's real superpower — indexable fuzzy lookup — solves a bottleneck we don't
  have.** First-token blocking already cuts the candidate space from 434,225,715 pairs to
  204,143 (~2,127×, G0 2026-06-03); that is seconds in Python. A GIN trigram index buys
  nothing at this scale.
- **It reverses the vanilla-warehouse posture** (ADR 0005) and needs an extension-enabling
  migration that re-applies at the Phase 6a.5 prod cutover.

Net: worse on the dimension we most care about (precision on subset/reorder, with
precision-over-recall as the governing rule — a wrong merge is worse than a missed one) for
no scale benefit, at the cost of testability and portability. The clean *future* upgrade is
a warehouse with a real Python runtime (dbt-python), not SQL-fuzzy.

## Consequences

**Positive.** The right tool per layer; RapidFuzz confined to the residual the deterministic
layer + FEI edges leave; no `CREATE EXTENSION`, no warehouse Python dependency; the
algorithm is a tested pure module; and the `crosswalk-as-source` pattern is reusable for
future Python enrichment stages. (Portfolio: exercises dbt-SQL + a tested Python enrichment
module + the source-join seam + pure/IO separation + bulk-load.)

**Negative — the stage sits OUTSIDE the dbt DAG**, so what dbt would give for free is
re-supplied deliberately:

| dbt affordance forfeited | How it is re-supplied |
|---|---|
| **Lineage** — the DAG sees `firm_crosswalk` only as a leaf source, not its `stg_*` inputs | Registered as the dbt source `enrichment.firm_crosswalk` (testable, `ref()`-able); the upstream dependency is documented in the module + CLI docstrings and `architecture.md`. A documented seam, not an invisible one. |
| **Orchestration / ordering** — dbt won't sequence `staging → resolve-firms → firm` | Explicit run-order documented; **and** the `coalesce` makes the stage *additive* — a missing/stale/empty crosswalk degrades to "every firm is its own canonical" (no fuzzy merges), never broken correctness. The external stage is never load-bearing. |
| **Testing** | Pure logic → pytest (`tests/enrichment/`); the landed table → dbt source-tests (`firm_id` not_null+unique, `canonical_firm_id` not_null). |
| **Idempotency** | Truncate-and-reload inside one transaction; `resolver_version` stamped for auditable rebuilds. |
| **Freshness / staleness** | Bounded + non-breaking by the same `coalesce`; documented "re-run after large seeds." |
| **Join-key integrity** (the subtlest risk — a Python table JOINing back to dbt) | `firm_id = md5(upper(trim(name)))` computed in Python over the *same* string Postgres computes, reading the *same* `stg_*` views `firm.sql` reads → keys match by construction. |

The single most important safeguard is the `coalesce`: it makes the entire external stage
*optional*. Worst case is lost fuzzy merges, never lost correctness.

## Related

- ADR 0002 — firm dimension (declines a structured `firm_identifier` table; FEI/MIC stay
  attributes/edges, not identity — the premise this engine resolves *around*).
- ADR 0011 — dbt-core for transformations (this is the scoped, justified exception: SQL for
  set-based work, Python for the edit-distance residual).
- ADR 0027 — deterministic normalization is a silver concern (this scopes the *venue* of
  the resolution surface to Python).
- ADR 0005 — Neon storage tier (the runtime/extension constraint that forces the seam).
- Plan: `project_scope/phase-6b-execution-plan.md` §2 (engine spine) + the resolve-firms PRs.
