# Gold design notes

The gold layer is the **consumer-shaped serving + analytics** layer (medallion ADR 0004), built from
the corrected silver layer in Phase 6e. **Policy** (naming, materialization, search, indexing) is
[ADR 0038](decisions/0038-gold-layer-modeling-and-indexing-strategy.md); **per-model contracts** are
in `dbt/models/gold/_gold.yml`. This doc is the narrative — what's in gold and why.

## Two shapes

Gold does **not** re-model silver into a formal Kimball star (a real `dim_date` / role-playing dims
were deferred — silver already supplies fact/dim/bridge). Instead it takes two shapes:

1. **Denormalized serving marts** (`mart_*`, materialized `table`, indexed) — a fact flattened with
   its dimensions + rollups into one wide row, so a consumer reads it in a single keyed query.
2. **Aggregate marts** (`fct_*`, materialized `view`) — facts pre-grouped to a coarser grain for
   dashboards.

Surrogate keys are **reused from silver verbatim** (`recall_event_id`, `recall_product_id`,
`firm_id`) — never re-keyed — to preserve lineage.

## Catalog

**Serving (feed the Phase 8 API):**
- `mart_recall_summary` — one row per recall, with a jsonb firm rollup, product rollup, lifecycle
  summary, and edit-history flags. Feeds `GET /recalls`.
- `mart_firm_profile` — one row per **canonical** firm (the 6b cross-source cluster), with recall
  stats + per-source SCD-2 sidecar attributes. Feeds `GET /firms/{id}`.
- `mart_product_search` — one row per product + a stored `tsvector` `search_vector`. Feeds
  `GET /products/search`. Search is **Postgres built-in FTS** (tsvector + GIN); `pg_trgm` is not
  enabled (ADR 0037), so no fuzzy/trigram search.

**Aggregates (dashboards):** `fct_recalls_by_week`/`_by_month`/`_by_year`, `fct_recalls_monthly_trend`
(rolling averages + YoY over a dense month spine), `fct_recalls_by_firm` (leaderboard), `fct_recalls_by_classification`,
`fct_recall_status` (active/inactive), `fct_recalls_by_geography`, `fct_units_recalled`. Most carry a
`source` dimension with an `'ALL'` rollup via `GROUPING SETS` (one model serves a per-source page and
the cross-source total).

## Design notes worth knowing

- **Geography has two lenses** (`fct_recalls_by_geography`). *Distribution* = where the product went
  (`recall_distribution_area`, FDA free-text parse + USDA states → `distribution_state_codes[]`;
  precision-over-recall). *Firm-location* = where the firm is registered (the SCD-2 sidecar `state`).
  They answer different questions and are **not** interchangeable. **Caveat:** firm-location inherits
  the **canonical** firm's address, which is often a corporate **HQ / FDA-FEI registration** (Walmart→AR,
  Target→MN), not where the product was made — and a name-merged firm can carry multiple FEIs across
  states. To be fleshed out in 6f (see `project_scope/phase-6-execution-plan.md` §6f exploration note).
- **Units are narrow and not cross-source comparable** (`fct_units_recalled`). Only NHTSA (vehicles)
  and USCG (boats) have a clean integer `unit_count`; CPSC/FDA/USDA are free-text (USDA = pounds).
  NHTSA's `recall_product` is the 7-tuple (many component rows per campaign) and `potaff` (an
  event-level count) repeats across them, so it is collapsed to **one per recall** before aggregating
  (a naive product-grain sum overcounts ~100×; `potaff` verified constant within a campno → the
  collapse is exact). USCG's `recall_product` is 1:1 with `recall_event`, so it never fans out — no
  explosion. `total_units` sums per-recall affected counts — a recall-**magnitude** measure, not unique
  vehicles (a vehicle recurs across recalls). Free-text value+unit parse (CPSC/FDA/USDA) is deferred to
  the units-enrichment TODO.
- **Indexing** (ADR 0038): silver/gold indexes are declared in dbt `config(indexes=[...])` so they
  re-create on each table rebuild. Two load-bearing specials: a **functional index on
  `firm_fda_attributes((firm_fei_num::text))`** (the firm→sidecar join casts FEI to text), and
  **`ANALYZE` post_hooks** on the firm-join-chain tables so a freshly-rebuilt table has fresh planner
  stats immediately (without them, an incremental rebuild fell back to seq-scans — `fct_recalls_by_geography`
  went 3s → 130s until ANALYZE was added).
- **Phase-7 follow-up:** re-profile indexes against real API traffic (`pg_stat_statements`); promote
  warn→error tests where stable; add the statistical/baseline tests that need production data.
