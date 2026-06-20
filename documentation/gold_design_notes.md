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
- `gold_meta` — one-row gold-layer rebuild stamp: `rebuilt_at` (dbt `run_started_at`, UTC, identical
  across every model in one `dbt build`) and `schema_version` (manual contract-version bump via
  `--vars '{gold_schema_version: "2"}'`). Read by the API to compute a layer-wide ETag / Last-Modified
  for conditional GET / 304. Covered by the `grant_gold_readonly` post-hook (ADR 0042 R4).

**Aggregates (dashboards):** `fct_recalls_by_week`/`_by_month`/`_by_year`, `fct_recalls_monthly_trend`
(rolling averages + YoY over a dense month spine), `fct_recalls_by_firm` (leaderboard), `fct_recalls_by_classification`,
`fct_recall_status` (active/inactive), `fct_recalls_by_geography` (US states), `fct_recalls_by_country`
(distribution countries, FDA+USDA; ISO-3166-1 alpha-2 with a derived `US` cell), `fct_units_recalled`.
Most carry a `source` dimension with an `'ALL'` rollup via `GROUPING SETS` (one model serves a
per-source page and the cross-source total).

## Serving-layer access control (R1/R4, ADR 0042)

Three components form the published read contract between the pipeline and the `recalls-api`:

- **`gold_meta`** (R4) — the one-row rebuild stamp described in the Catalog above. Its `rebuilt_at`
  and `schema_version` columns are the `_gold.yml` column contract; see `dbt/models/gold/gold_meta.sql`
  for the implementation.
- **`recalls_readonly` role** (R1) — a `NOLOGIN` SQL role provisioned out-of-band by migration
  `0034_recalls_readonly_role`. Gold-only `SELECT`; `default_transaction_read_only = on`; **not** a
  `neon_superuser` member. The role's DSN is exposed as `NEON_DATABASE_URL_RO`. It is activated
  out-of-band (the pipeline owns schema, the API owns the connection string).
- **`grant_gold_readonly` macro** — applied to every model under `dbt/models/gold/` via
  `dbt_project.yml +post-hook`. Re-grants `SELECT` to `recalls_readonly` after each nightly
  drop+recreate of the gold tables. Tolerant of the role being absent (dev/CI ephemeral branches) so
  those builds do not fail. See `dbt/macros/grant_gold_readonly.sql`.

Together these ensure the API's read path survives nightly rebuilds without any manual re-grant.
The full stability obligations (column contract, key recipes, source enum) are recorded in ADR 0042.

## Design notes worth knowing

- **Geography has two lenses** (`fct_recalls_by_geography`) that answer *different questions* — the
  metric is `count(distinct recall_event_id)` per `(geography_basis, source, state_code)`, with a
  `GROUPING SETS` `'ALL'`-source rollup. They are **not** interchangeable.
  - ***`distribution`*** = **where the recalled product went** — `recall_distribution_area.distribution_state_codes[]`
    (FDA free-text parse + USDA states; precision-over-recall). **FDA + USDA only**; CPSC/NHTSA/USCG
    carry no distribution field, so they contribute nothing. This is the clean *"where did the product
    go / who was potentially affected"* answer. **Countries (C12, 2026-06-09):** the sidecar now also
    carries `distribution_country_codes[]` (ISO-3166-1 alpha-2, parsed from the FDA international tail
    via the `country_iso` seed), parallel to the state array; its grain expanded to *≥1 state OR
    country*, so a country-only recall now gets a row with empty `state_codes[]`. The `distribution`
    lens here is **unchanged** — it unnests `distribution_state_codes[]` and is inert to country-only
    rows (an empty array unnests to nothing). The world-map companion **`fct_recalls_by_country` was
    built (C12 follow-on, 2026-06-09)**: foreign cells unnest `distribution_country_codes[]`, and the
    **`US` cell is *derived*** from `distribution_scope`(Nationwide/Regional) + `distribution_state_codes`
    — *not* stored in silver, which keeps `distribution_country_codes[]` a clean foreign-only
    international-presence array. Empirically US dominates (50,854 recalls vs ~4.5k for the next country,
    Canada); only ~273 recalls are truly non-US.
  - ***`firm_registration`*** (renamed from `firm_location`, C17 2026-06-09) = **where the responsible firm is *registered*** — `firm.observed_company_ids`
    → the per-source SCD-2 sidecar state (`firm_usda_attributes.state` /
    `firm_uscg_attributes.state` / `firm_fda_attributes.firm_state_cd`). It is **not** "all
    firms": only firms whose 6b canonical cluster carries an FSIS `establishment_number`, USCG `mic`, or
    FDA `firm_fei_num` — directly, or via a name-merge to one — appear; a pure CPSC/NHTSA firm (no
    structural id) contributes nothing.
  - **Caveats make `firm_registration` *not* a consumer-impact geography:** (1) **registration ≠ harm**
    — the FEI/establishment address is the firm's *registered* (often corporate-HQ) address (Walmart's
    FEI → AR/Bentonville, Target → MN), so "Walmart recall → AR" means *Walmart is registered in AR*,
    not that the product came from or affected AR; (2) **multi-counting (kept by design)** — a recall is
    counted in **every** state where any of its firms is registered, and a firm can carry facilities in
    multiple states (up to **7** observed), so per-state counts **sum to more than the distinct-recall
    total** (an *industry-footprint* reading: recall × firm-registered-state incidences, not distinct
    recalls per state). The C18 single-primary-state collapse was evaluated 2026-06-09 and **reverted** —
    65% of multi-state firms tie at ~1 registration/state, so it attributed an arbitrary state to **6.6%**
    of recalls (`scripts/sql/gold/inspect_firm_state_ties.sql`); (3) **coverage skew**
    — only the three sidecar-backed sources (+ name-merged CPSC/NHTSA) feed it; (4) **merge-sensitive**
    — a 6b over-merge attributes one firm's state to another's recalls.
  - **Use:** read `distribution` as "where the product went," `firm_registration` as "which states' firms
    get recalled" (an industry/regulatory lens) — **never** as where consumers were affected. Neither is
    "where the product was *made*" (no production-site field exists). **Done 2026-06-09 (C17):** the
    `geography_basis` value was renamed `firm_location → firm_registration` for honesty. **C18
    (single-primary-state collapse) was evaluated and reverted** — the multi-counting is a legitimate,
    documented industry-footprint property; collapsing it picked an arbitrary state for 6.6% of recalls
    (no uniform cross-source recency date exists to break the pervasive ~1-registration-per-state ties).
    Evidence: `scripts/sql/gold/inspect_firm_state_ties.sql` (the probe that surfaced the 65%
    ~1-registration/state tie rate and the 6.6% affected-recall figure driving the revert).
- **Units are narrow and not cross-source comparable** (`fct_units_recalled`, grain source ×
  `unit_category` × month). The **measure means different things per source** — NHTSA/USCG = vehicles/
  boats *potentially affected* (clean integer `unit_count`); FDA = quantity *distributed*; USDA =
  weight *recovered* — so always filter by source; no 'ALL' rollup. `unit_category`
  (count/weight/volume/grouping) keeps incommensurable units apart (1,000 cases ≠ 1,000 lbs).
  NHTSA's `recall_product` is the 7-tuple (many component rows per campaign) and `potaff` repeats
  across them, so it is collapsed to **one max per recall** (a naive product-grain sum overcounts
  ~100×; `potaff` verified constant within a campno → exact). USCG is 1:1 with `recall_event` (no
  fan-out). **FDA/USDA added 2026-06-09 (C13)** from the `recall_product` quantity parse,
  **basis-agnostic**: the recall-wide quantity repeats identically across the recall's product rows
  (e.g. "15,779,607 units" on all 14 rows of a recall), so `units = max(quantity_value)` per
  `(recall, category)` — a naive `SUM` over-counts by the product-row count. The v1 parser emits a
  value only for clean single-quantity strings and NULLs messy multi-product breakdowns (precision
  guards in `quantity.py`; messy tail → AI extractor v2, freetext-enrichment-backlog), so rows
  reaching here are clean. The basis-sum/GREATEST logic was replaced 2026-06-09. `total_units` sums
  per-recall magnitudes — a recall-**magnitude** measure, not unique items.
  **CPSC stays free-text-only** (no parse); the value/unit parse for it remains in the
  units-enrichment backlog.
- **Indexing** (ADR 0038): silver/gold indexes are declared in dbt `config(indexes=[...])` so they
  re-create on each table rebuild. Two load-bearing specials: a **functional index on
  `firm_fda_attributes((firm_fei_num::text))`** (the firm→sidecar join casts FEI to text), and
  **`ANALYZE` post_hooks** on the firm-join-chain tables so a freshly-rebuilt table has fresh planner
  stats immediately (without them, an incremental rebuild fell back to seq-scans — `fct_recalls_by_geography`
  went 3s → 130s until ANALYZE was added). Two additional gold serving-mart indexes added for the
  Phase 8 API (ADR 0042): **(1) `mart_recall_summary`** — an expression index on
  `(published_at DESC, recall_event_id)`, the R2 keyset-pagination anchor for `GET /recalls` cursor
  queries; declared via `post_hook` because column-list `config(indexes=[...])` cannot express
  expression indexes. **(2) `mart_product_search`** — a GIN index on `recall_product_upcs`, serving
  the R3 recall-level UPC containment query (`@> :upc`); declared via `config(indexes=[{columns:
  [recall_product_upcs], type: gin}])` (column-list config, not a post_hook). The per-product `upc`
  btree was **dropped (gold-audit G5, 2026-06-15)** — `upc` is 0% populated, so the btree was an empty index
rebuilt nightly for no benefit; the `upc` *column* stays as a placeholder. **Gold-audit G1 (2026-06-15)**
added two GIN indexes on `mart_recall_summary`'s `distribution_state_codes` / `distribution_country_codes`
so the recalls-api array filters (`@>` / `&&`) are index-backed instead of seq-scanning the recall mart.
- **Performance rewrites (2026-06-09, no schema change):** the two items below are pure query
  rewrites with no observable output change. Note: the `geography_basis` value rename
  (`firm_location → firm_registration`, C17) also touched `fct_recalls_by_geography` this session
  but is a separate **schema change** — documented in the `firm_registration` lens note above.
  - `mart_firm_profile` had a firm × product fan-out: the pre-rewrite CTE re-joined `recall_event_firm`
    and `recall_product` separately, so a multi-firm + multi-product NHTSA recall (e.g. a 139-component
    Takata campno) exploded to firms × products rows before `count(distinct)` — the root of the ~180s
    build time. Fixed by a single `materialized` CTE (`firm_recalls`) at the event grain (one row per
    firm × event × role), then counting products per event first (`event_products`), then summing over
    the firm's distinct events — exact, no double-count, and no fan-out.
  - `fct_recalls_by_geography` had a correlated cross-join-lateral subquery per `company_id` to look up
    the matching sidecar state (a seq-scan-per-row bottleneck). Rewritten to the same hash-join pattern
    as `mart_firm_profile.firm_attr_rows`: unnest `observed_company_ids` into `firm_company_ids`, then
    `LEFT JOIN` all three sidecars + `coalesce` the state — the disjoint id namespaces ensure each id
    matches at most one sidecar.
- **Phase-7 follow-up:** re-profile indexes against real API traffic (`pg_stat_statements`); promote
  warn→error tests where stable; add the statistical/baseline tests that need production data.

## Deferred: a dimensional star schema

Gold deliberately stops short of a formal Kimball star ([ADR 0038](decisions/0038-gold-layer-modeling-and-indexing-strategy.md) §1) —
silver already supplies fact/dim/bridge, so a star would largely duplicate it. The `dim_` prefix is
**reserved** (ADR 0038 §2) for the day a star earns its keep. This note records when that day arrives
and what gets built.

**The gating question is the website's data feed, not the star itself.** The project website will be
gold's first BI-esque consumer, and *how it reads the data* decides whether a star helps:

- **API-fed (Phase 8 FastAPI) with a fixed chart set** — the existing `fct_*` aggregate marts already
  *are* the dashboard layer (one query each: by week/month/year, monthly trend, by firm, by
  classification, by status, by geography, units). A star buys nothing; a missing chart is a new
  `fct_*` view or API endpoint, not a re-model. **No star.**
- **Direct-gold, fixed charts** — same conclusion; `fct_*` serves them.
- **A BI tool / semantic layer over gold** (Metabase, Superset, Cube, the dbt Semantic Layer) **or
  user-driven cross-dimensional slicing** (a pivot like "recalls by firm × classification × quarter"
  that no single `fct_*` pre-computes) — *this* is where a conformed star pays off: the tool
  auto-joins dims↔facts and every chart shares consistent slicers. **Build the star.**

So the prerequisite is not a coding task — it is **enumerating the website's charts**. A fixed/known
set → extend `fct_*`. An interactive/pivot set fed by a star-consuming tool → build the star.

**If built**, the star is mostly *promotions* of existing silver, not new modeling: `dim_firm`
(Type-2 — the SCD-2 snapshots are already the hard part), a generated `dim_date` (role-played as
announced/published), `dim_source` / `dim_classification` from the existing enums, and
`fct_recall_event` / `fct_recall_product` carrying measures + dim-FKs; `recall_event_firm` becomes a
factless bridge.

**The one no-regret early piece is `dim_date`** — a generated calendar that replaces the inline
`date_trunc` logic in the **five** date-grained `fct_*` models (by_month / by_year / by_week /
monthly_trend / units; the other four `fct_*` carry no `date_trunc`) and unlocks fiscal/holiday
calendars cheaply. **Built + wired 2026-06-09 (C10/C11):** `dim_date` (**1940-01-01** .. current-year+2,
dynamic; unique `date_day`) is built, and the five models join it on `coalesce(announced_at,
published_at)::date` (**2026-W25, fix/announced-at-date-join** — moved off `published_at::date` so the
time-series buckets on the TRUE announce date, not the publish watermark; FDA's `event_lmd` is
bulk-stamped ~2018-09 for the openFDA archive, which had piled all pre-2018 FDA history into one
month). `announced_at` is nullable (~20 FDA), so the coalesce floors to the non-null `published_at`,
keeping the join lossless (guarded by `assert_fct_recalls_by_{month,week,year}_reconciles`; the C11
wiring was originally verified byte-identical to the prior `date_trunc` form). The 1940 floor matches
`assert_recall_event_date_sanity`'s ERROR floor (which range-checks BOTH dates), so any sane recall is
guaranteed a `dim_date` row — INNER JOINs in the `fct_*` models can never silently drop one (ISSUE-7).
Pagination/sort is unaffected: the `mart_recall_summary` R2 keyset stays on `published_at DESC` (the
non-null freshness key). Column definitions (year/quarter/month/week/iso/dow/us_fiscal_year) are
in `documentation/data_schemas.md`. The full Kimball star stays deferred (above); `dim_date` was the
no-regret slice.

**When:** decide at Phase 8 framing — [ADR 0024](decisions/README.md) already owns "the relationship
between API endpoints and dbt gold views" — once the website's feed + chart inventory are known.
Sequencing is tracked in `project_scope/implementation_plan.md` (Architectural follow-ups). Not
before: building it speculatively risks a star no consumer queries.
