# Index audit (Phase 6e)

Companion deliverable to **ADR 0038** (gold-layer modeling + indexing strategy). Records the
state of indexing across all three layers after the Phase 6e index pass: bronze confirmed,
silver/gold added via dbt `config(indexes=[...])`. First-principles selection (FK/join columns,
natural keys, documented Phase-8 API filters, search) — **no production query traffic exists yet**,
so the Phase-7 follow-up (below) re-profiles against real traffic.

## Mechanism

- **Bronze** — Alembic-managed (`migrations/versions/`); indexes persist with the append-only tables.
- **Silver / gold** — declared in each model's `config(indexes=[...])`, so dbt drops + recreates them
  on every `dbt build` (the tables are rebuilt each run). Gold indexes are co-located in each
  `mart_` model. One expression index that the column-list config can't express is added via
  `post_hook` (see firm_fda_attributes below).

## Bronze — confirmed appropriate (no change)

19 indexes across 11 tables, each documented inline in its migration. Verdict: each maps to a live
access pattern; no gaps found. Pattern summary:

| Pattern | Columns | Tables | Purpose |
|---|---|---|---|
| Dedup (universal) | `(source_recall_id, extraction_timestamp DESC)` | all bronze | `BronzeLoader._fetch_existing_hashes()` latest-per-identity |
| Watermark | `last_publish_date` / `event_lmd` / `last_modified_date` | CPSC / FDA / USDA | incremental cursor (ADR 0010) |
| Join / grouping | `recall_event_id` (FDA), `campno` (NHTSA), `mic` (USCG) | per source | silver joins / analytical grouping |
| Composite identity | `(source_recall_id, langcode)`, `(source_recall_id, press_release_url, ts DESC)` | USDA, FDA PR | bilingual / M:1 dedup |
| Functional | `upper(establishment_name)` | USDA establishments | case-insensitive silver join |
| Filter | `status_regulated_est`, `state` | USDA est., USCG mfr/detail | "active only" / geographic |
| Crosswalk | `canonical_firm_id` | firm_crosswalk | cluster-member reverse lookup |

## Silver — added (Phase 6e)

All on `materialized='table'` models (the SCD-2 sidecars are current tables, indexable):

| Model | Indexes |
|---|---|
| `recall_event` | unique `recall_event_id`; `(source, source_recall_id)`; `(source, published_at)`; `classification` |
| `recall_product` | unique `recall_product_id`; `recall_event_id`; `upc`; `hin` |
| `firm` | unique `firm_id`; `normalized_name` |
| `recall_event_firm` | unique `(recall_event_id, firm_id, role)`; `recall_event_id`; `firm_id` |
| `recall_lifecycle` | unique `recall_event_id` |
| `recall_event_press_release` | unique `recall_event_press_release_id`; `recall_event_id` |
| `recall_event_establishment_resolution` | unique `recall_event_id` |
| `recall_distribution_area` | unique `recall_event_id`; **GIN** `distribution_state_codes` (array containment) |
| `firm_usda_attributes` | unique `establishment_id`; `state` |
| `firm_uscg_attributes` | unique `mic`; `state` |
| `firm_fda_attributes` | unique `firm_fei_num`; `firm_state_cd`; **functional** `(firm_fei_num::text)` (post_hook) |
| `uscg_mic_reassignment_years` | unique `mic` |

**Why the `firm_fei_num::text` functional index** (the load-bearing one): `firm.observed_company_ids`
stores the FEI as text, so the firm→sidecar join in `fct_recalls_by_geography` (firm-location lens)
and `mart_firm_profile` is `firm_fda_attributes.firm_fei_num::text = company_id`. A plain index on the
`bigint` `firm_fei_num` cannot serve that cast; the expression index can. This is the fix for the
~130s `fct_recalls_by_geography` build (correlated seq-scans of the sidecars → index scans).

## Gold — serving marts indexed; aggregates are views

| Model | Indexes |
|---|---|
| `mart_recall_summary` | unique `recall_event_id`; `(source, published_at)`; `is_active`; `classification` |
| `mart_firm_profile` | unique `firm_id`; `normalized_name` |
| `mart_product_search` | unique `recall_product_id`; `recall_event_id`; `hin`; `model`; `upc`; **GIN** `search_vector` (FTS) |
| `fct_recalls_by_geography` | `(geography_basis, source, state_code)`; `state_code` |
| `fct_recalls_by_*` (aggregates) | none — materialized as views (small, recomputed) |

## Phase-7 follow-up (recorded per ADR 0038)

Once the Phase-8 API generates traffic, re-profile with `pg_stat_statements`: drop indexes the
planner never uses, add observed hot paths, and reconsider promoting the heavier aggregate views to
indexed tables. Also evaluate promoting silver `unique`-test assertions / FK relationships to
Postgres-enforced constraints (dbt model contracts) for planner benefit.
