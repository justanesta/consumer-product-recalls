# 0038 — Gold-layer modeling and indexing strategy

**Status:** Accepted (ratified at the Phase 6e merge, #62) | Amended 2026-06-13 (serving-layer branch, ADR 0042)
**Date:** 2026-06-07

> **Amended 2026-06-08 (Phase 6f.1):** a first concrete gold consumer is now on the horizon — the
> project website's BI-esque visualizations — which is the "concrete star payoff becomes visible"
> condition Decision §1 named as the revisit trigger. The revisit is **gated on the website's data
> feed**: API-fed or direct-gold with a *fixed* chart set is already served by the `fct_*` aggregate
> marts (no star); a BI tool / semantic layer or user-driven cross-dimensional slicing is what would
> justify the star. The decision is deferred to Phase 8 framing (ADR 0024, API↔gold relationship);
> the narrative + the `dim_date` no-regret early piece live in `gold_design_notes.md` §"Deferred: a
> dimensional star schema", and the sequencing is tracked in `implementation_plan.md`. No code change.

> **Amended 2026-06-13 (serving-layer branch, ADR 0042):** the gold layer now uses `post_hook`s in
> three distinct classes — see §6 amendment note below.

## Context

The medallion gold layer (ADR 0004) is the serving/analytics layer: it feeds the planned Phase 8 API (`GET /recalls`, `/recalls/{source}/{id}`, `/products/search`, `/firms/{id}`), the dashboards, the landing pages, and the keyword search promised in the project vision. Until Phase 6e the layer was a single stub (`recalls_by_month`, a view) — there was no decision record for how gold should be shaped, named, materialized, searched, or indexed.

By Phase 6e the silver layer is complete and rich: facts (`recall_event`, `recall_product`, the `recall_event_firm` bridge, `recall_event_press_release`); a conformed `firm` dimension whose `firm_id` is already the 6b cross-source cluster id, with three per-source SCD-2 attribute sidecars (`firm_usda_attributes`, `firm_uscg_attributes`, `firm_fda_attributes`); and history (`recall_event_history`, `recall_lifecycle`, `recall_product_history`). Gold should *consume* this, not re-model it.

Three constraints bound the decision:

- **`pg_trgm` / `fuzzystrmatch` are not enabled on Neon** (ADR 0037 — firm fuzzy-resolution runs as a Python stage for exactly this reason). Gold cannot lean on trigram indexes for search.
- **dbt rebuilds silver/gold tables on every `dbt build`.** Any index on a dbt-materialized table must be re-created by dbt itself, not hand-managed.
- **No production query traffic exists yet.** Index selection is necessarily first-principles, not profile-driven.

The original Phase-6 plan also listed "silver/gold Alembic migrations" — that was misleading: silver and gold are 100% dbt-managed. Alembic governs only the bronze tables and the `enrichment.firm_crosswalk` table.

## Decision

1. **Gold takes two shapes, not a star schema (yet).**
   - **Denormalized serving marts** (`mart_*`): one wide "one-big-table" per API/landing-page consumer, joining a fact to its rollups so a reader needs one query.
   - **Aggregate marts** (`fct_*`): grain-reduced rollups for dashboards.
   - A full Kimball star (conformed `dim_date` / role-playing dims / `fct_` grain facts) is **deferred** — silver already supplies fact/dim/bridge, so a star would largely duplicate it. Revisit once the marts are in use and a concrete star payoff is visible.

2. **Naming convention.** `mart_` = wide denormalized serving (materialized `table`, indexed); `fct_` = grain-reducing aggregate (materialized `view`); `dim_` reserved for a future pure conformed dimension. Gold **reuses silver surrogate keys** (`recall_event_id`, `recall_product_id`, `firm_id`) verbatim — never re-keyed — to preserve lineage (`silver_design_notes.md`). The existing `recalls_by_month` is renamed `fct_recalls_by_month`.

3. **Materialization policy.** Serving + search marts → `table` (so they can be indexed and read fast). Aggregate marts → `view` (cheap to recompute, small result sets). Override per model in `config()` when a profile later says otherwise.

4. **Per-source rendering via one model.** Marts carry the `source` dimension; where an all-source total is also useful, a `GROUPING SETS` rollup emits an `'ALL'` row alongside the per-source rows, so one model serves both a per-source page (filter `source='CPSC'`) and the cross-source total (`source='ALL'`). Consumers filter deliberately.

5. **Keyword search = Postgres built-in FTS.** `mart_product_search` carries a stored `tsvector` (`to_tsvector('english', …)`) indexed with **GIN**; exact-identifier lookups (HIN, model, UPC) use btree. Trigram/fuzzy search is **rejected for now** (extension disabled, ADR 0037); revisit if typo-tolerant product search becomes a requirement. Firm-name fuzziness is already resolved upstream by the 6b Python clusterer, so the firm side needs no trigram.

6. **Indexing is declared where the object lives, and is first-principles.**
   - **Bronze** indexes stay Alembic-managed (19 today, each documented inline).
   - **Silver/gold** indexes are declared in dbt `config(indexes=[…])`, so dbt re-creates them on every table rebuild — no post-hooks, no Alembic. Gold indexes are co-located in each `mart_` model's config; silver indexes are added to each silver model's config.
   - Index selection covers FK/join columns, natural keys, the documented Phase-8 API filter predicates (`source`, `published_at`, `classification`, `firm.normalized_name`, product `hin`/`model`/`upc`), and the search GIN. dbt `unique`-test assertions on primary keys are backed by real Postgres unique indexes.
   - Because there is no traffic yet, these are first-principles. **Phase-7 follow-up (recorded here):** re-profile with `pg_stat_statements` once the API is live; drop unused indexes, add observed hot paths. dbt model contracts (Postgres-enforced PK/FK) are deferred as an optional later hardening.

> **Amended 2026-06-13 (ADR 0042):** the claim "no post-hooks" in §6 is superseded on the
> serving-layer branch. Three classes of `post_hook` are now in use:
>
> 1. **Expression KEYSET index** — `mart_recall_summary` declares a `post_hook` to create
>    `(published_at DESC, recall_event_id)` via raw DDL (`mart_recall_summary.sql:9-13`), because
>    `config(indexes=[…])` cannot express descending-column index specifications.
> 2. **ANALYZE** — all three serving marts (`mart_recall_summary`, `mart_product_search`,
>    `mart_firm_profile`) run `analyze {{ this }}` as a `post_hook` after each table rebuild so
>    the planner has current statistics immediately.
> 3. **Folder-level `recalls_readonly` grant** — `dbt_project.yml` applies
>    `+post-hook: "{{ grant_gold_readonly() }}"` to every gold model (`dbt_project.yml:30`,
>    `macros/grant_gold_readonly.sql`), re-granting `SELECT` to the public read-only API role
>    after each nightly drop+recreate. See ADR 0042 for the full serving-layer read contract.

7. **A companion index audit deliverable** (`documentation/index_audit.md`) records the bronze confirmation pass and the silver/gold additions in one table (layer → object → index → query pattern → verdict).

## Consequences

- Gold is consumer-shaped and index-backed; the Phase-8 API can read `mart_*` tables directly without re-joining silver.
- One decision record now governs naming, materialization, search, and indexing, so future readers don't re-derive them per model.
- First-principles indexing will likely create a few indexes production traffic never uses — accepted, and pruned in the Phase-7 profiling pass. Conversely, real hot paths may be missing until then.
- Deferring the star schema means no conformed `dim_date` / role-playing dimensions yet; aggregate marts compute date parts inline (`date_trunc`). Acceptable at this corpus size.
- FTS without trigram means product search is token/prefix-based, not typo-tolerant. The firm side is unaffected (upstream Python resolution).
- `GROUPING SETS` `'ALL'` rows require consumers to filter `source` deliberately; documented in `data_schemas.md` and each model's description.
- Index DDL travels with the dbt model config, so a `dbt run` of a single mart re-creates its indexes — no drift between the model and its indexes.
