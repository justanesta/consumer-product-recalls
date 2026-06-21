# 0038 — Gold-layer modeling and indexing strategy

**Status:** Accepted (ratified at the Phase 6e merge, #62) | Amended 2026-06-13 (serving-layer branch, ADR 0042) | Amended 2026-W25 (announce-date facts) | Amended 2026-W26 (announce-recency feed sort + config-index oscillation fix)
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

> **Amended 2026-W25 (`fix/announced-at-date-join`) — time-series facts bucket on the ANNOUNCE date,
> not the publish date.** The five date-grained facts (`fct_recalls_by_month` / `_week` / `_year`,
> `fct_recalls_monthly_trend`, `fct_units_recalled`) join `dim_date` on
> `coalesce(announced_at, published_at)::date` instead of `published_at::date`, and
> `mart_firm_profile.first_recall_at` / `last_recall_at` move to the same basis. **Rationale:**
> `published_at` is a last-published/last-modified watermark, and FDA's (`event_lmd`) is bulk-stamped
> ~2018-09 for the openFDA archive migration — so bucketing on it collapsed all pre-2018 FDA history
> (~29k events) into one month. `announced_at` is the TRUE, backfill-immune recall-initiation date
> (`recall_event.sql`), so it is the correct time-series basis. It is NULLABLE (~20 FDA events with no
> trustworthy initiation date), so the join coalesces to the non-null `published_at` — keeping the inner
> join **lossless** (the existing `assert_fct_recalls_by_month_reconciles` guard still passes;
> `_by_week` / `_by_year` reconcile guards were added). **Pagination/sort is deliberately NOT changed** [in
> W25 — later finished in §2026-W26 below, which repointed the feed to `event_date`]**:**
> the `mart_recall_summary` keyset + R2 index stay on `published_at DESC` — keyset pagination requires a
> non-null sort key, and surfacing recently *updated* recalls is the intended feed behavior. The two axes
> are kept separate (`announced_at` = "when did it happen" / analytics; `published_at` = "what's
> new/updated" / sort) and the mart exposes both. An optional future announce-recency feed (paginate on a
> materialized `coalesce(announced_at, published_at)`) is logged in `TODO.md` §Performance; quarantining
> the ~20 FDA nulls to force `announced_at` NOT NULL was **rejected** (deletes real recalls — 14 have a
> recoverable year — and reverses the `recover-rejected fda` recovery). **`published_at` fidelity note
> (describe it precisely everywhere):** it is a non-null recency FLOOR, not uniformly "last edited" —
> CPSC `last_publish_date` / FDA `event_lmd` / USDA `last_modified_date` / USCG `last_date` are genuine
> last-modified, but NHTSA's is `coalesce(DATEA, RCDATE)` where `DATEA` = "Record Creation Date"
> (`RCL.txt:46`; the NHTSA flat file carries no last-modified field). **No schema/wire change** (the facts
> expose `period`, not a raw date; the mart already exposes both dates) → no `gold_meta.schema_version`
> bump; only the served *values* shift on the next `dbt build` (the FDA Sept-2018 spike disappears, FDA
> history spreads across years).

> **Amended 2026-W26 (`fix/announced-index-sort-site`) — the `GET /recalls` feed now sorts by
> announce-recency, finishing what §2026-W25 deferred.** §2026-W25 deliberately left the feed keyset on
> `published_at DESC`; in production this surfaced long-dormant recalls that got one minor agency edit at
> the top of the feed (e.g. a 2000 recall re-published days ago outranking genuinely newer ones) and
> clustered FDA's ~2018-09 `event_lmd` pile-up mid-feed. **Change:** `mart_recall_summary` gains a stored
> non-null `event_date = coalesce(announced_at, published_at)` column (the SAME basis as the §2026-W25
> facts — consistent by construction), and the R2 keyset index + the `(source, …)` composite repoint from
> `(published_at DESC, recall_event_id)` → `(event_date DESC, recall_event_id)` / `(source, event_date)`.
> The recalls-api keyset cursor, `ORDER BY`, and seek `WHERE` retarget `event_date` (cursor kind `p`→`e`);
> the site relabels its date filters to announce semantics (the existing `announced_after`/`announced_before`
> API params). **Why coalesce, not `announced_at` directly:** keyset pagination needs a non-null, totally
> ordered sort key; `announced_at` is nullable (~20 FDA), so a raw-`announced_at` keyset would mis-order the
> NULLs and break the seek. `coalesce(announced_at, published_at)` is non-null by construction (the ~20 fall
> back to `published_at`, landing where they sort today) → **no quarantine, no data loss.** Quarantining the
> ~20 to force `announced_at NOT NULL` stays **rejected** (per §2026-W25: deletes real recalls, reverses the
> `recover-rejected fda` recovery and the ≥1940 precision guard). **Wire/contract change:** additive column,
> but the *default-sort meaning* changes ("newest first" = newest announced) → `gold_meta.schema_version`
> bumped **1 → 2** (default floor changed in `gold_meta.sql`); `published_at`/`announced_at` both stay
> exposed, and the `published_after`/`published_before` filters are retained. Cross-repo: recalls-api
> `0.1.1 → 0.2.0` + `openapi.json` regen; site date-filter relabel + `schema.d.ts` regen.

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
> 1. **Expression KEYSET index** — `mart_recall_summary` declares a `post_hook` to create the
>    `(<sort> DESC, recall_event_id)` keyset index via raw DDL (`mart_recall_summary.sql` config), because
>    `config(indexes=[…])` cannot express descending-column index specifications. (`<sort>` was
>    `published_at` here; **repointed to `event_date` in §2026-W26** — see the amendment above.)
> 2. **ANALYZE** — all three serving marts (`mart_recall_summary`, `mart_product_search`,
>    `mart_firm_profile`) run `analyze {{ this }}` as a `post_hook` after each table rebuild so
>    the planner has current statistics immediately.
> 3. **Folder-level `recalls_readonly` grant** — `dbt_project.yml` applies
>    `+post-hook: "{{ grant_gold_readonly() }}"` to every gold model (`dbt_project.yml:30`,
>    `macros/grant_gold_readonly.sql`), re-granting `SELECT` to the public read-only API role
>    after each nightly drop+recreate. See ADR 0042 for the full serving-layer read contract.

> **Amended 2026-W26 (`fix/announced-index-sort-site`) — `config(indexes)` OSCILLATES under dbt 1.11.x;
> ALL silver + gold table indexes moved to `config(meta.index_specs)` + a folder-level `rebuild_indexes()`
> post_hook.** §6's mechanism ("silver/gold indexes declared in `config(indexes=[…])`") and the 2026-06-15
> belief that hash-named `config(indexes)` GINs are *oscillation-immune* are **superseded repo-wide**. Root
> cause: dbt 1.11.x creates `config(indexes)` indexes on the `__dbt_tmp` relation via `create index if not
> exists "<hash>"` BEFORE the table swap; the hash (md5 of cols+relation+unique+type, with the stably-named
> `__dbt_tmp` relation) is identical across builds, so it collides with the old table's same-named index,
> the `IF NOT EXISTS` no-ops, and the index is dropped with the backup — it vanishes every other build.
> (The earlier "immune" claim held only while older dbt created indexes on the *final* relation post-swap.)
> **Fix (DRY, repo-wide):** every `table` model (3 serving marts + `dim_date` + `fct_recalls_by_geography`
> + 12 silver models incl. the firm sidecars) declares its indexes in
> `config(meta={'index_specs': [{suffix, cols, method?, unique?}]})`, and a single folder-level
> `+post-hook: "{{ rebuild_indexes() }}"` on the `silver` + `gold` folders (`dbt_project.yml`) reads that
> meta and DROP-THEN-CREATEs each index on the final `{{ this }}` (immune; `macros/rebuild_indexes.sql`).
> `config(indexes=[...])` is removed everywhere; `mart_recall_summary`'s keyset and
> `firm_fda_attributes`'s functional `(firm_fei_num::text)` index are now `meta.index_specs` specs too. New
> index = add to `meta.index_specs`; new model = auto-covered. `assert_gold_serving_indexes_present` gained
> `depends_on` refs (it had none — it ran ~node 9, before the marts rebuilt, validating the *previous*
> build's catalog → the false failure that surfaced this) and was widened to guard every load-bearing
> serving index. **A silver index-presence guard is not yet added (the gold guard covers the API surface);
> noted for a later hardening pass.**

7. **A companion index audit deliverable** (`documentation/index_audit.md`) records the bronze confirmation pass and the silver/gold additions in one table (layer → object → index → query pattern → verdict).

## Consequences

- Gold is consumer-shaped and index-backed; the Phase-8 API can read `mart_*` tables directly without re-joining silver.
- One decision record now governs naming, materialization, search, and indexing, so future readers don't re-derive them per model.
- First-principles indexing will likely create a few indexes production traffic never uses — accepted, and pruned in the Phase-7 profiling pass. Conversely, real hot paths may be missing until then.
- Deferring the star schema means no conformed `dim_date` / role-playing dimensions yet; aggregate marts compute date parts inline (`date_trunc`). Acceptable at this corpus size.
- FTS without trigram means product search is token/prefix-based, not typo-tolerant. The firm side is unaffected (upstream Python resolution).
- `GROUPING SETS` `'ALL'` rows require consumers to filter `source` deliberately; documented in `data_schemas.md` and each model's description.
- Index DDL travels with the dbt model config, so a `dbt run` of a single mart re-creates its indexes — no drift between the model and its indexes.
