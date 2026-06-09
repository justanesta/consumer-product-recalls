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

- **Geography has two lenses** (`fct_recalls_by_geography`) that answer *different questions* — the
  metric is `count(distinct recall_event_id)` per `(geography_basis, source, state_code)`, with a
  `GROUPING SETS` `'ALL'`-source rollup. They are **not** interchangeable.
  - ***`distribution`*** = **where the recalled product went** — `recall_distribution_area.distribution_state_codes[]`
    (FDA free-text parse + USDA states; precision-over-recall). **FDA + USDA only**; CPSC/NHTSA/USCG
    carry no distribution field, so they contribute nothing. This is the clean *"where did the product
    go / who was potentially affected"* answer.
  - ***`firm_registration`*** (renamed from `firm_location`, C17 2026-06-09) = **where the responsible firm is *registered*** — `firm.observed_company_ids`
    → the per-source SCD-2 sidecar state (`firm_establishment_attributes.state` /
    `firm_manufacturer_attributes.state` / `firm_fda_attributes.firm_state_cd`). It is **not** "all
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

**The one no-regret early piece is `dim_date`** — a generated calendar that replaces the `date_trunc`
logic repeated inline across the nine `fct_*` models and unlocks fiscal/holiday calendars cheaply.
**Decided 2026-06-08:** it will be built pre-Phase-8 regardless of the star call (tracked in
`project_scope/implementation_plan.md`, Architectural follow-ups).

**When:** decide at Phase 8 framing — [ADR 0024](decisions/README.md) already owns "the relationship
between API endpoints and dbt gold views" — once the website's feed + chart inventory are known.
Sequencing is tracked in `project_scope/implementation_plan.md` (Architectural follow-ups). Not
before: building it speculatively risks a star no consumer queries.
