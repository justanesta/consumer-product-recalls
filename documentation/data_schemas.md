# Data schemas reference

A reader's index for "what does column X mean, where is it defined, and what does this domain term refer to?" The authoritative definitions for the schemas themselves live in code (`src/schemas/`) and dbt configuration (`dbt/models/`). This document is the glossary, the cross-reference, and the quick-lookup — not a copy of the schemas.

> **Status (2026-06-13, pre-go-live-validation branch):** Phase 7 surfaces documented (as before). This branch adds: `gold_meta` table (R4, ADR 0042); `mart_firm_profile` sidecar output columns renamed to `firm_{usda,uscg,fda}_attributes` (R5); `recalls_readonly` role (migration 0034, R1) — all reflected below. This file is the **single-home owner** for field-level definitions; design rationale and grain traps live in [`silver_design_notes.md`](silver_design_notes.md) and [`gold_design_notes.md`](gold_design_notes.md).

For:
- **System-level architecture** — see [`architecture.md`](architecture.md).
- **Per-source silver mapping decisions** (CPSC vs. FDA column unification, surrogate keys, null-filling) — see [`silver_design_notes.md`](silver_design_notes.md).
- **Why a particular schema choice was made** — see [`decisions/`](decisions/).

---

## Where each schema lives (authoritative sources)

The two-pronged validation surface ([ADR 0014](decisions/0014-schema-evolution-policy.md)) means each table has two authoritative artifacts: the Pydantic model that validates rows on the way in, and the dbt model + tests that validate them on the way out. Both should be consulted when reasoning about a column's contract.

### Bronze (one per source, insert-only)

| Table | Pydantic schema | Alembic migration | dbt source |
|---|---|---|---|
| `cpsc_recalls_bronze` | `src/schemas/cpsc.py` | `migrations/versions/0002_cpsc_bronze.py`, `0003_cpsc_sold_at_label.py` | `dbt/models/staging/_sources.yml` |
| `cpsc_recalls_rejected` | (same Pydantic; rejection routed via `BronzeLoader`) | same | (no dbt source — forensic only) |
| `fda_recalls_bronze` | `src/schemas/fda.py` | `migrations/versions/0004_fda_bronze.py` | `_sources.yml` |
| `fda_recalls_rejected` | (same) | same | (forensic only) |
| `usda_fsis_recalls_bronze` | `src/schemas/usda.py` | `migrations/versions/0005_usda_fsis_bronze.py` | `_sources.yml` |
| `usda_fsis_recalls_rejected` | (same) | same | (forensic only) |
| `usda_fsis_establishments_bronze` | `src/schemas/usda_establishment.py` | `migrations/versions/0006_*`, `0007_*`, `0008_*` | `_sources.yml` |
| `usda_fsis_establishments_rejected` | (same) | same | (forensic only) |
| `nhtsa_recalls_bronze` | `src/schemas/nhtsa.py` | `migrations/versions/0011_nhtsa_recalls_bronze.py` | `_sources.yml` |
| `nhtsa_recalls_rejected` | (same) | same | (forensic only) |
| `uscg_recalls_bronze` | `src/schemas/uscg.py` | `migrations/versions/0013_uscg_recalls_bronze.py` | `_sources.yml` |
| `uscg_recalls_rejected` | (same) | same | (forensic only) |
| `uscg_manufacturers_bronze` | `src/schemas/uscg_manufacturer.py` | `migrations/versions/0015_uscg_manufacturers_bronze.py` | `_sources.yml` |
| `uscg_manufacturers_rejected` | (same) | same | (forensic only) |
| `uscg_manufacturer_details_bronze` | `src/schemas/uscg_manufacturer_detail.py` | `migrations/versions/0017_uscg_manufacturer_details_bronze.py` | `_sources.yml` |
| `uscg_manufacturer_details_rejected` | (same) | same | (forensic only) |
| `fda_press_releases_bronze` | `src/schemas/fda.py` (`FdaPressReleaseRecord`) | `migrations/versions/0022_fda_press_releases_bronze.py` | `_sources.yml` |
| `fda_press_releases_rejected` | (same) | same | (forensic only) |

Bronze tables follow the [ADR 0027](decisions/0027-bronze-storage-forced-transforms-only.md) rule: only storage-forced transforms (date string → datetime, "True"/"False" → bool) happen at the Pydantic layer. Value-level normalization moves to silver staging.

`fda_press_releases_bronze` is **event-grain** (one recall event → several press releases; `source_recall_id` = RECALLEVENTID, joinable to `fda_recalls_bronze.recall_event_id`). Its dedup identity is the **full natural key** `(source_recall_id, press_release_url, press_release_type, press_release_issued_dt)` — the same URL recurs within an event under a different type/issue date (every column is an immutable fact, so all four are the key; the 2-tuple `(event, url)` mis-bucketed those and crashed the seed). See [`src/bronze/dedup_contracts.py`](../src/bronze/dedup_contracts.py).

### Pipeline state (shared across sources)

| Table | Migration | Purpose | Authoritative ADR |
|---|---|---|---|
| `source_watermarks` | `0001_baseline.py` (+ per-source seeds in `0008_seed_usda_establishments_watermark.py`) | Cursor state — where the next incremental query should start | [ADR 0020](decisions/0020-pipeline-state-tracking.md) |
| `extraction_runs` | `0001_baseline.py` (+ `change_type` column added by Phase 5b.2 per ADR 0027) | Run metadata — `started_at`, `status`, `records_extracted`, `records_inserted`, `change_type` | [ADR 0020](decisions/0020-pipeline-state-tracking.md), [ADR 0027](decisions/0027-bronze-storage-forced-transforms-only.md) |
| `extraction_run_identities` | `0027_extraction_run_identities.py` (Phase 6c) | Per-run **presence** manifest — one row per `(run_id, source, source_recall_id, langcode)` returned by a successful run; substrate for the `recall_lifecycle` silver dims. **USDA + NHTSA** (`DedupContract.writes_presence_manifest`; NHTSA keyed on `campno`, written only on full-enumerating deep-rescan — see [ADR 0026](decisions/0026-lifecycle-tracking-snapshot-presence-manifest.md) C16 amendment). Written in-txn by `Extractor._record_run`. | [ADR 0026](decisions/0026-lifecycle-tracking-snapshot-presence-manifest.md) |
| `deep_rescan_checkpoints` | `0030_deep_rescan_checkpoints.py` (FDA PR seed) | Durable resume cursor for **batched** deep-rescan sweeps — one row per `(source, change_type)`, opaque JSONB `cursor` + counters + `status`. The checkpointed seed co-commits it in the **same txn** as each batch's bronze load, so resume reads from the DB (empty events leave no bronze row to resume from). FDA PR seed cursor: `{"init_dt", "event_id"}`. | [`fda-press-release-seed-plan.md`](../project_scope/archive/fda-press-release-seed-plan.md) |

### Silver (unified across sources, dbt-managed)

| Model | dbt SQL | dbt tests / schema | Per-source mapping |
|---|---|---|---|
| `staging/stg_cpsc_recalls` | `dbt/models/staging/stg_cpsc_recalls.sql` | `stg_cpsc_recalls.yml` | n/a |
| `staging/stg_fda_recalls` | `stg_fda_recalls.sql` | `stg_fda_recalls.yml` | n/a |
| `staging/stg_usda_fsis_recalls` | `stg_usda_fsis_recalls.sql` | `stg_usda_fsis_recalls.yml` | n/a |
| `silver/recall_event` | `recall_event.sql` | `_silver.yml` | [`silver_design_notes.md`](silver_design_notes.md) |
| `silver/recall_product` | `recall_product.sql` | `_silver.yml` | NHTSA branch consumes `nhtsa_recall_product_snapshot` (7-tuple, `dbt_valid_to is null`) since the 6c.7 cutover ([ADR 0034](decisions/0034-nhtsa-silver-v15-migration.md)); other 4 sources unchanged. See [`silver_design_notes.md`](silver_design_notes.md) §12 |
| `silver/firm` | `firm.sql` | `_silver.yml` | [`silver_design_notes.md`](silver_design_notes.md) |
| `silver/recall_event_firm` | `recall_event_firm.sql` | `_silver.yml` | [`silver_design_notes.md`](silver_design_notes.md) |
| `silver/recall_event_history` | `recall_event_history.sql` (Phase 6c.1) | `_silver.yml` + `recall_event_history_unit_tests.yml` + `assert_recall_event_history_real_changes.sql` | LAG() over bronze, 5 sources, per [ADR 0022](decisions/0022-fda-history-endpoints-empty-snapshot-synthesis-for-all-sources.md) + [ADR 0027](decisions/0027-bronze-storage-forced-transforms-only.md); see [`silver_design_notes.md`](silver_design_notes.md) |
| `silver/recall_lifecycle` | `recall_lifecycle.sql` (Phase 6c.2) | `_silver.yml` + `recall_lifecycle_unit_tests.yml` | 1:1 with recall_event; bronze-derived first/last_seen + edit_count (all 5 sources) + manifest-derived is_currently_active / was_ever_retracted (**USDA + NHTSA**, per [ADR 0026](decisions/0026-lifecycle-tracking-snapshot-presence-manifest.md) amended C16; NHTSA manifest keyed on `campno`, written only on full-enumerating deep-rescan — see [ADR 0026](decisions/0026-lifecycle-tracking-snapshot-presence-manifest.md)); see [`silver_design_notes.md`](silver_design_notes.md) |
| `silver/recall_distribution_area` | `recall_distribution_area.sql` (Phase 6e; country cols added 2026-06-09 C12) | `_silver.yml` + `assert_distribution_area_has_geography.sql` + `assert_distribution_country_codes_valid_iso.sql` | Sidecar to `recall_event`; one row per recall with ≥1 parseable US state **or** ≥1 parseable foreign country (FULL OUTER — grain expanded from "≥1 state" in Phase 6e). FDA + USDA only. Columns: `distribution_state_codes text[]`, `n_distribution_states int`, `distribution_country_codes text[]`, `n_distribution_countries int`. See [new columns detail below](#recall_distribution_area-country-columns-c12). |
| `silver/firm_usda_attributes` | `firm_usda_attributes.sql` (Phase 6b; C19 rename from `firm_establishment_attributes`) | `_silver.yml` | USDA FSIS establishment attributes sidecar — latest-wins per `establishment_number`; SCD-2 history in `firm_usda_attributes_snapshot`. See [ADR 0031](decisions/0031-silver-row-fragmentation-strategy.md) + [ADR 0035](decisions/0035-cross-source-scd2-silver-dimensions.md). |
| `silver/firm_uscg_attributes` | `firm_uscg_attributes.sql` (Phase 6b; C19 rename from `firm_manufacturer_attributes`) | `_silver.yml` | USCG MIC attributes sidecar — latest-wins per `mic`; SCD-2 history in `firm_uscg_attributes_snapshot`. See [ADR 0031](decisions/0031-silver-row-fragmentation-strategy.md) + [ADR 0035](decisions/0035-cross-source-scd2-silver-dimensions.md). |
| `silver/firm_fda_attributes` | `firm_fda_attributes.sql` (Phase 6b) | `_silver.yml` | FDA establishment attributes sidecar — latest-wins per `firm_fei_num`; SCD-2 history in `firm_fda_attributes_snapshot`. See [ADR 0035](decisions/0035-cross-source-scd2-silver-dimensions.md). |
| `silver/uscg_mic_reassignment_years` | `uscg_mic_reassignment_years.sql` (Phase 6c.5) | `_silver.yml` | Per-MIC `current_holder_since_year` parsed from the detail-page `(OOB YYYY)` lineage; feeds the `recall_event_firm` as-of-build-year resolution ([ADR 0035](decisions/0035-cross-source-scd2-silver-dimensions.md) §5) |
| `staging/stg_nhtsa_recalls_current` | `stg_nhtsa_recalls_current.sql` (Phase 6c.6) | `stg_nhtsa_recalls_current.yml` | Distinct-on-7-tuple latest projection; single-homes the v1.5 `recall_product_id`; input to `nhtsa_recall_product_snapshot`; see [`silver_design_notes.md`](silver_design_notes.md) §12 |
| `silver/recall_product_history` | `recall_product_history.sql` (Phase 6c.6) | `_silver.yml` | Full product-grain version history (all snapshot versions + `is_current`); the audit peer of `recall_product`'s NHTSA branch (Policy C). See [`silver_design_notes.md`](silver_design_notes.md) §12 |

### Silver snapshots (SCD-2 history — `silver_snapshots` schema, dbt-managed, ADR 0035)

dbt snapshots (`strategy='check'`) bank attribute history per stable anchor. The three firm sidecars follow Policy C (the `dbt_valid_to is null` current view is the dim, the full table is the queryable peer history); the NHTSA `nhtsa_recall_product_snapshot` is the v1.5 **product-grain** track (not a firm sidecar — see §12). The `silver_snapshots` schema is exempt from ADR 0007 bronze-snapshot pruning. See [`silver_design_notes.md`](silver_design_notes.md) §10 (firm sidecars) + §12 (NHTSA product).

| Snapshot | dbt | Anchor | Feeds | Type |
|---|---|---|---|---|
| `firm_uscg_attributes_snapshot` | `dbt/snapshots/firm_uscg_attributes_snapshot.sql` | `mic` | `firm_uscg_attributes` | Type-2 **NEED** (MIC reassignment; 6b.5) |
| `firm_usda_attributes_snapshot` | `dbt/snapshots/firm_usda_attributes_snapshot.sql` | `establishment_number` | `firm_usda_attributes` | Type-2 **BENEFIT** (6c.4) |
| `firm_fda_attributes_snapshot` | `dbt/snapshots/firm_fda_attributes_snapshot.sql` | `firm_fei_num` | `firm_fda_attributes` | Type-2 **BENEFIT** (6c.4) |
| `nhtsa_recall_product_snapshot` | `dbt/snapshots/nhtsa_recall_product_snapshot.sql` | 7-tuple `recall_product_id` | `recall_product` (NHTSA branch, current view) + `recall_product_history` | Type-2 **product-grain** (NHTSA v1.5; [ADR 0033](decisions/0033-silver-row-versioning-via-scd-on-stable-anchor.md) + amendment / [ADR 0034](decisions/0034-nhtsa-silver-v15-migration.md), cutover 6c.7) |

(CPSC has no firm sidecar. NHTSA's row is product-grain, not a firm dim — the v1.5 track; since the 6c.7 cutover ([ADR 0034](decisions/0034-nhtsa-silver-v15-migration.md)) its current view **is** `recall_product`'s NHTSA branch, with `recall_product_history` as the audit peer.)

### Enrichment (Python-written, dbt source)

| Table | Written by | Alembic migration | dbt source |
|---|---|---|---|
| `firm_crosswalk` | `recalls resolve-firms` (`src/enrichment/crosswalk_writer.py`) — a *derived* table, truncate-and-reloaded; **not** Pydantic-validated (it is computed from silver, not ingested) | `0024_firm_crosswalk.py`, `0025_*_clean_name.py`, `0026_*_alternate_names.py` | `enrichment.firm_crosswalk` |
| `quantity_crosswalk` | `recalls parse-quantities` (`src/enrichment/quantity_crosswalk_writer.py`) — derived, truncate-and-reloaded; **not** Pydantic-validated. Populated with distinct FDA + USDA raw quantity strings → parsed fields. 0 rows until the CLI runs. | `0032_quantity_crosswalk.py` | `enrichment.quantity_crosswalk` |

The resolution *method* that populates it is in [`architecture.md`](architecture.md#srcenrichment--firm-resolution-stage-adr-0037); the *why* (a Python stage, not in-warehouse SQL) is [ADR 0037](decisions/0037-firm-resolution-python-stage-not-sql-fuzzy.md); the operator runbook is [`operations.md`](operations.md#firm-resolution-recalls-resolve-firms). Column contract = the DDL in the three migrations + the `build_crosswalk_rows` row dict in `crosswalk_writer.py`. Key columns: `firm_id` (PK, `md5(upper(trim(name)))`), `canonical_firm_id`, `canonical_name`, `clean_name`, `alternate_names` (jsonb), `match_confidence`, `match_score`, `resolver_version`, `resolved_at`. See the **Firm resolution** glossary section below for what each means.

`quantity_crosswalk` columns — PK `raw_quantity` (the raw text of `recall_product.number_of_units`, the join key); `quantity_value` (Numeric, nullable — NULL for unparseable / messy shapes); `quantity_unit` (text, nullable — canonical unit from the `quantity.py` taxonomy); `quantity_category` (text, nullable — `count` / `weight` / `volume` / `grouping`); `quantity_basis` (text, always present — `per_product` / `total_all_products` / `unknown`). See the [Quantity fields](#recall_product-quantity-fields-c13) section below for the full semantics.

### Gold (consumer-shaped, dbt-managed)

Built on the corrected silver layer (Phase 6e). Policy = [ADR 0038](decisions/0038-gold-layer-modeling-and-indexing-strategy.md);
narrative = [`gold_design_notes.md`](gold_design_notes.md); per-model contracts = `dbt/models/gold/_gold.yml`.

**Calendar dimension:**

| Model | Grain | Key columns |
|---|---|---|
| `dim_date` | one row per calendar day; `date_day` unique key | `date_day`, `year`, `quarter`, `month`, `month_name`, `iso_week`, `iso_day_of_week`, `day_name`, `day_of_year`, `iso_week_start`, `month_start`, `quarter_start`, `year_start`, `is_weekend`, `us_fiscal_year` (US fed. fiscal year: Oct-start, labelled by the calendar year it ends in). Spine: **1940-01-01** .. (current year + 2, dynamic). The 1940 floor matches `assert_recall_event_date_sanity`'s ERROR floor, so any sane `published_at` finds a row (no INNER JOIN drops). See [`gold_design_notes.md`](gold_design_notes.md). |

**Serving marts** (denormalized "one big table", materialized `table` + indexed — feed the Phase 8 API):

| Model | Grain | Feeds |
|---|---|---|
| `mart_recall_summary` | one row per recall_event | `GET /recalls` (list + detail) |
| `mart_firm_profile` | one row per canonical firm | `GET /firms/{id}` (cross-source rollup) |
| `mart_product_search` | one row per recall_product (+ FTS `search_vector`) | `GET /products/search` |
| `gold_meta` | one row (gold build stamp) | ETag / Last-Modified signal for the API; part of the ADR 0042 published contract. Columns: `rebuilt_at` (timestamptz) — dbt `run_started_at` UTC, identical across one `dbt build`; `schema_version` (text) — manually bumped contract version (default `"1"`), bump via `--vars`; breaking column / key / enum changes require a bump (ADR 0042). |

`mart_firm_profile` per-source sidecar attribute outputs: `firm_usda_attributes` (JSONB array of USDA establishment records), `firm_uscg_attributes` (JSONB array of USCG MIC records), `firm_fda_attributes` (JSONB array of FDA FEI records). Renamed R5 (from `establishment_attributes` / `manufacturer_attributes` / `fda_attributes`) on this branch. These names are part of the ADR 0042 published read contract.

`mart_recall_summary` array rollup columns: `product_names`, `models`, `hins`, and `firms` are coalesced to `'[]'::jsonb` so they are **never NULL** in the serving mart (`mart_recall_summary.sql` O1 coalesce). This is a load-bearing API contract (ADR 0042) — the API may rely on these columns always being a valid JSONB array, never NULL.

**Aggregate marts** (`fct_`, materialized `view` — feed dashboards):

| Model | Grain |
|---|---|
| `fct_recalls_by_week` / `_by_month` / `_by_year` | recall count per period × source (+ `'ALL'`); period from `dim_date.month_start` / `year_start` / `iso_week_start` |
| `fct_recalls_monthly_trend` | per source × month + rolling 3/12-mo avgs + YoY; period from `dim_date` |
| `fct_recalls_by_firm` | most-recalled-firm leaderboard (rank) |
| `fct_recalls_by_classification` | per source × classification / risk_level |
| `fct_recall_status` | active / inactive / unknown per source |
| `fct_recalls_by_geography` | per US state — `distribution` + `firm_registration` lenses; see [`gold_design_notes.md`](gold_design_notes.md) |
| `fct_recalls_by_country` | per country × source (FDA + USDA only); ISO-3166-1 alpha-2; foreign cells unnest `distribution_country_codes[]`, US cell derived — see [below](#fct_recalls_by_country-c12) |
| `fct_units_recalled` | per source × `unit_category` × month (from `dim_date`); NHTSA + USCG unit counts + FDA/USDA parsed quantities; grain: `max(quantity_value)` per (recall, category) — see [below](#fct_units_recalled) |

---

## Glossary

Domain-specific terms used across this project. When in doubt, this is the canonical definition.

### Records and granularity

- **Recall event.** A single regulatory action by a single agency. One row in `silver.recall_event`. Identified by `(source, source_recall_id)`. CPSC's `RecallNumber` (e.g. `"24-158"`), FDA's `RECALLEVENTID` (e.g. `"98724"`), USDA's `field_recall_number` (e.g. `"049-2024"`) are the source-side primary keys.
- **Recall product.** A single product-line within a recall event. One row in `silver.recall_product`. CPSC encodes products as a JSONB array per event (exploded in dbt); FDA emits one bronze row per `PRODUCTID` (already flat); USDA recall events do not split products at this stage. See [`silver_design_notes.md`](silver_design_notes.md) §1–2.
- **Firm.** A company involved in a recall in some role. Deduplicated by normalized name (`UPPER(TRIM(firm_name))`). See [ADR 0002](decisions/0002-unit-of-analysis-header-line-firm.md).
- **Role.** The relationship of a firm to a recall event. Allowed values: `manufacturer`, `importer`, `distributor`, `establishment`, `filer`. `establishment` = the recalling FDA-registered / USDA-FSIS facility (**FDA + USDA**); `filer` = NHTSA's recall *filer* (`mfgname`), split from `manufacturer` (`mfgtxt`, the product maker). `retailer` was **removed** — CPSC's retailer field is a free-text sales-channel narrative, not a firm entity, so it lives in `recall_event.sales_channel_narrative` (see [`silver_design_notes.md`](silver_design_notes.md) §3 + `cpsc/field_audit_2026_w22.md`). Per [ADR 0002](decisions/0002-unit-of-analysis-header-line-firm.md).
- **Event type.** Forward-compatibility column on `recall_event` (`event_type` defaults to `'RECALL'`). See [ADR 0003](decisions/0003-event-type-discriminator.md). Reserved for future non-recall regulatory actions (e.g., enforcement actions, market withdrawals).

### Firm resolution

Terms for the cross-source firm entity-resolution overlay (Phase 6b, [ADR 0037](decisions/0037-firm-resolution-python-stage-not-sql-fuzzy.md)). **Grain: one `firm` = one brand/name cluster** across the five sources; every structured id (FDA FEI, USDA `establishment_number`, USCG MIC, CPSC `company_id`) is an attribute (`observed_company_ids`), not a merge key. The *method* (blocking, document frequency) is in [`architecture.md`](architecture.md#srcenrichment--firm-resolution-stage-adr-0037); these are the schema-level terms.

- **`firm_id` (resolution key).** `md5(upper(trim(firm_name)))` — the global natural key of one *raw* firm name; the PK of `firm_crosswalk`. **Caution:** the silver `firm` model's output column *also* named `firm_id` carries the **canonical** id, not this raw key (see `canonical_firm_id`).
- **`canonical_firm_id`.** The id of a cluster's representative — every name that resolves to the same real-world company shares it. **Additive** ([ADR 0002](decisions/0002-unit-of-analysis-header-line-firm.md)): silver computes `coalesce(crosswalk.canonical_firm_id, md5(normalized_name))`, so a missing/empty crosswalk degrades to "each name is its own canonical." It is the grain of the silver `firm` dimension (emitted there as the `firm_id` column) and the firm key of the `recall_event_firm` bridge.
- **`match_confidence`.** The resolution path/quality (tier), stamped per `firm_crosswalk` row and carried onto each `recall_event_firm` bridge row. The accepted-values vocabulary is single-homed in `dbt/models/silver/_silver.yml` (severity `warn` until 6b.6). Firm-resolution values: `exact_name` (no merge), `geo_suffix_strip_exact` / `dba_extract_exact` (deterministic cleaning); **Tier 1** `name_variant_exact` (identical distinctive-token set) / `name_typo_high` (`token_sort_ratio` typo); **Tier 2** `rapidfuzz_rollup` (≥2 shared distinctive tokens — the reviewable rollup tier); `singleton` (unmerged); `fei_exact` (FDA-FEI group) is **deferred/opt-in** (`--fei-merge`, off — ADR 0037) and does not appear in the shipped crosswalk. USDA (`usda_*`) and USCG (`uscg_*`) values come from PRs 6b.2 / 6b.5.
- **`match_score`.** The RapidFuzz `token_set_ratio` (0–100) for a `rapidfuzz_*` merge; NULL for deterministic / FEI / unmerged rows. Lives on `firm_crosswalk` only — not projected into silver.
- **`alternate_names`.** JSONB array of brand / surface-form aliases for a firm — the DBA brand plus brand-bearing parentheticals (`extract_paren_aliases`), **captured instead of stripped** (ADR 0037). `firm.sql` flattens + de-dupes them per canonical firm; intended as a search/alias field.
- **`observed_names` / `observed_company_ids`.** On the silver `firm` row: JSONB arrays of every raw spelling, and every structured government ID (FDA FEI / FSIS establishment number / USCG MIC), that folded into the canonical firm — the audit trail of a merge.

### Pipeline mechanics

- **Watermark.** Per-source cursor recording the most recent successful extraction's reference timestamp. Used by the next incremental query as `WHERE last_modified_field >= <watermark>`. Stored in `source_watermarks`. **Advisory, not authoritative** — content-hash dedup is what actually prevents duplicates ([ADR 0020](decisions/0020-pipeline-state-tracking.md)).
- **Extraction run.** A single invocation of one source's extractor. One row in `extraction_runs` with `started_at`, `status`, `records_extracted`, `records_inserted`, `change_type`.
- **Content hash.** SHA-256 of the canonical (sorted-key, no-whitespace) JSON serialization of a bronze record's payload. Conditional-insert key — bronze never duplicates rows with the same hash. Defined in `src/bronze/hashing.py` per [ADR 0007](decisions/0007-lineage-via-bronze-snapshots-and-content-hashing.md).
- **Canonical record dict.** The output of `to_canonical()` on a Pydantic schema instance — the dict that gets hashed. Changes to canonicalization (Pydantic normalizers, hashing helpers) invalidate every prior bronze hash; this is treated as a schema migration per [ADR 0007](decisions/0007-lineage-via-bronze-snapshots-and-content-hashing.md) line 70 and [ADR 0027](decisions/0027-bronze-storage-forced-transforms-only.md).
- **Change type.** Column on `extraction_runs` distinguishing routine extraction from re-baseline waves. Allowed values: `routine` (default), `schema_rebaseline`, `hash_helper_rebaseline`, `historical_seed` (added by [ADR 0028](decisions/0028-backfill-historical-reextraction-semantics.md)), `etag_audit` (forces unconditional GET for `usda` / `usda_establishments` to verify ETag honesty; added 2026-05-10). The `recall_event_history` model filters out non-routine runs from edit detection. Single-home detail at [`cli.md`](cli.md) § change-types.
- **Deep rescan.** Re-fetch from a source over a wide window, ignoring the watermark. Per-source workflow at `.github/workflows/deep-rescan-<source>.yml`. Used for edit detection on weak-watermark sources and for one-time historical backfill. See [ADR 0010](decisions/0010-ingestion-cadence-and-github-actions-cron.md), [ADR 0023](decisions/0023-fda-deep-rescan-required-archive-migration-detected.md), [ADR 0028](decisions/0028-backfill-historical-reextraction-semantics.md).
- **Deep-rescan checkpoint.** Durable resume cursor for a *batched* deep-rescan sweep — `deep_rescan_checkpoints`, keyed `(source, change_type)`, written by the batched loader in the **same transaction** as each batch's bronze load so the cursor can never lead committed rows. Resume reads it from the DB rather than re-deriving from bronze (empty/no-row events leave nothing to resume from — the gap the old log-grep chunk script papered over). Introduced for the FDA press-release seed (`FdaPressReleaseCheckpointedSeedLoader`, recent-first by `recall_initiation_dt`); generalizes to any cursor-ordered sweep. See [`fda-press-release-seed-plan.md`](../project_scope/archive/fda-press-release-seed-plan.md).
- **R2 replay.** Re-run the bronze loader against raw payloads already in R2, without contacting the source. Used for schema-drift recovery, normalizer changes, hashing-helper updates. See [ADR 0028](decisions/0028-backfill-historical-reextraction-semantics.md) Mechanism B.
- **Re-baseline.** A wave of bronze inserts produced by an our-side change (Pydantic normalizer or hashing helper change) where the source was unchanged. Marked `change_type='schema_rebaseline'` or `'hash_helper_rebaseline'` to distinguish from real edits. See [ADR 0027](decisions/0027-bronze-storage-forced-transforms-only.md), `documentation/operations/re_baseline_playbook.md`.
- **Run identity (presence) manifest.** Per-run record of which `(source_recall_id, [langcode])` tuples were present in a successful run's response — stored in `extraction_run_identities` (migration 0027), written by `Extractor._record_run` in the **same transaction** as the `extraction_runs` row it FKs to. It is the one signal bronze cannot infer: a retraction (record absent upstream) produces zero new bronze rows, identical to "content unchanged, dedup skipped." Substrate for the `recall_lifecycle` silver dims (`is_currently_active`, `was_ever_retracted`). **USDA + NHTSA** (`DedupContract.writes_presence_manifest`; NHTSA manifest keyed on `campno`, written only on the full-enumerating deep-rescan, not the daily extract — per [ADR 0026](decisions/0026-lifecycle-tracking-snapshot-presence-manifest.md) amended C16). See [ADR 0026](decisions/0026-lifecycle-tracking-snapshot-presence-manifest.md).

### Failure routing

- **T0 — raw landing.** Cloudflare R2 bucket `<source>/<extraction_date>/<key>`. Every extracted byte lands here before validation. Immutable, retain-forever. See [ADR 0004](decisions/0004-four-layer-medallion-pipeline.md), [ADR 0005](decisions/0005-storage-tier-neon-and-r2.md).
- **T1 — rejected tables.** Per-source `<source>_recalls_rejected` Postgres tables. Records that fail Pydantic validation or business invariants are routed here with `failure_reason`, `failure_stage` (`validate` or `invariants`), and `raw_landing_path` pointing back to T0. See [ADR 0013](decisions/0013-error-handling-retries-idempotency-and-quarantine.md).
- **T2 — alert.** Structured warning log + workflow non-zero exit when rejection rate exceeds threshold. v1 alert surface is the GitHub Actions UI ([ADR 0029](decisions/0029-application-observability-and-alerting.md)).
- **Quarantine.** The act of routing a record to T1 (rejected table) instead of bronze. Schema violations and invariant failures both quarantine; the record's `failure_stage` distinguishes which path triggered it.

### Source-specific terms

- **Bilingual pair (USDA).** A USDA recall published in both English and Spanish. Each language is a separate row in the FSIS API response with the same `field_recall_number` and a different `langcode`. See [ADR 0006](decisions/0006-usda-bilingual-record-deduplication.md). **Empirical note:** ~13.3% of bilingual pairs do not update atomically — silver lifecycle logic must treat each language independently per [ADR 0026](decisions/0026-lifecycle-tracking-snapshot-presence-manifest.md).
- **Establishment (USDA).** An FSIS-regulated facility (`establishment_id` is the FSIS primary key). Distinct from "manufacturer" — an establishment is the *recalling* facility, often co-incident with the manufacturer but legally a separate role. Used in `recall_event_firm.role='establishment'`.
- **FEI number (FDA).** FDA Establishment Identifier (`firmfeinum`). A permanent id for a physical **establishment (facility)**, *not* a firm — one firm has many FEIs, and a firm's FEI is reassigned on ownership/operational change. It is therefore **temporal**: the recall feed gives the FEI *at the time of the recall* plus `firmsurvivingfei`/`firmsurvivingnam` (the **current** FEI/name if changed). **FEI is an attribute, not a firm-merge key** (ADR 0037): `firm` is name/brand grain, and FEI rides on `firm.observed_company_ids` + the `firm_fda_attributes` sidecar (latest-wins; SCD-2 history deferred to 6c, [ADR 0035](decisions/0035-cross-source-scd2-silver-dimensions.md)-class, like USCG MIC). The deferred Tier-0 `fei_resolve` (opt-in `--fei-merge`, off) groups by `current_fei = coalesce(firm_surviving_fei, firm_fei_num)` but is disabled because facilities change corporate hands, so FEI-merging chains unrelated firms; the `firm_fei_edges` model (surfacing `firm_fei_num`, `firm_surviving_fei`, `current_fei`, `current_name`) feeds it only when enabled. See [ADR 0002](decisions/0002-unit-of-analysis-header-line-firm.md), [ADR 0037](decisions/0037-firm-resolution-python-stage-not-sql-fuzzy.md).
- **Archive migration (CPSC, FDA).** Upstream re-processing where the agency touches old records wholesale, advancing their `LastPublishDate` / `eventlmd` without an editorial change. Inflates incremental-query result sets without producing real edits. See [ADR 0023](decisions/0023-fda-deep-rescan-required-archive-migration-detected.md), `documentation/cpsc/last_publish_date_semantics.md`.
- **Historical seed.** A one-time deep rescan over a multi-year window to populate records that the incremental strategy will never reach (e.g., CPSC's 20-year 2005–2024 gap). Marked `change_type='historical_seed'`. See [ADR 0028](decisions/0028-backfill-historical-reextraction-semantics.md) Mechanism A.

### Storage layers

- **Landing (T0).** Cloudflare R2. Raw immutable payloads.
- **Bronze.** Neon Postgres. Insert-only, per-source, content-hash-keyed dedup. Pydantic-validated.
- **Silver.** Neon Postgres. dbt-managed, rebuilt per transform run. Unified schema across sources.
- **Gold.** Neon Postgres. dbt-managed, denormalized for query shape. Feeds dashboards and Phase 8 FastAPI.

### Dev/prod isolation

- **`main` Neon branch.** Production database. Cron workflows write here.
- **`dev` Neon branch.** Local development database. Branched from `main` per [ADR 0005](decisions/0005-storage-tier-neon-and-r2.md).
- **`consumer-product-recalls` R2 bucket.** Production R2 bucket. Used by GitHub Actions.
- **`consumer-product-recalls-dev` R2 bucket.** Local development R2 bucket. R2 has no native branching, so dev/prod isolation is bucket-level with separate API tokens. See [ADR 0005](decisions/0005-storage-tier-neon-and-r2.md), [ADR 0016](decisions/0016-secrets-management.md).

---

## Quick: "I want to find..."

| Looking for | Look here |
|---|---|
| What columns does `cpsc_recalls_bronze` have? | `src/schemas/cpsc.py` (Pydantic), `migrations/versions/0002_cpsc_bronze.py` (Postgres DDL) |
| What's the surrogate-key formula for `recall_event_id`? | `dbt/models/silver/recall_event.sql` (`md5(source \|\| '\|' \|\| source_recall_id)`); summary in [`silver_design_notes.md`](silver_design_notes.md) §4 |
| What's the surrogate-key formula for `recall_product_id`? | CPSC: `md5('CPSC'\|\|'\|'\|\|source_recall_id\|\|'\|'\|\|product_ordinal::text)` — stable (event, ordinal) anchor (`recall_product.sql` cpsc_products CTE; ADR 0031 amendment 2026-06-13); FDA: `md5('FDA'\|\|'\|'\|\|source_recall_id)`; NHTSA: 7-tuple md5 single-homed in `stg_nhtsa_recalls_current` (ADR 0033/0034); USDA/USCG: `recall_product_id = recall_event_id` (1:1 grain). See [`silver_design_notes.md`](silver_design_notes.md) §2 + §4. |
| What does `change_type='schema_rebaseline'` mean? | This file's "Pipeline mechanics" glossary; full context in [ADR 0027](decisions/0027-bronze-storage-forced-transforms-only.md) and `documentation/operations/re_baseline_playbook.md` |
| Why does `firm` deduplicate by normalized name and not by company ID? | [ADR 0002](decisions/0002-unit-of-analysis-header-line-firm.md), [`silver_design_notes.md`](silver_design_notes.md) §4 |
| What goes in `_rejected` tables vs. bronze? | [ADR 0013](decisions/0013-error-handling-retries-idempotency-and-quarantine.md); fields documented in `migrations/versions/0001_baseline.py` |
| Where is the FDA `RECALLEVENTID` mapped to silver? | `dbt/models/staging/stg_fda_recalls.sql`, then [`silver_design_notes.md`](silver_design_notes.md) "Column mapping" table |
| What's the watermark column for source X? | `source_watermarks` row + the per-source extractor in `src/extractors/<source>.py` (look for `WATERMARK_FIELD`-style class constants) |
| What are the allowed values of `recall_event_firm.role`? | `dbt/models/silver/_silver.yml` (`accepted_values` test) — `['manufacturer', 'importer', 'distributor', 'establishment', 'filer']` (`retailer` removed → `sales_channel_narrative`; `filer` added for NHTSA) |
| What columns does `firm_crosswalk` have? | DDL in `migrations/versions/0024_firm_crosswalk.py` (+ `0025`/`0026`); the row dict in `src/enrichment/crosswalk_writer.py` (`build_crosswalk_rows`). Term-by-term in this file's "Firm resolution" glossary |
| What are the allowed `match_confidence` values? | `dbt/models/silver/_silver.yml` (`accepted_values`, severity `warn`); explained in the "Firm resolution" glossary above |
| How are firm name variants collapsed (the fuzzy resolver)? | [`architecture.md`](architecture.md#srcenrichment--firm-resolution-stage-adr-0037) firm-resolution section; method in `src/enrichment/firm_resolution.py`; operator runbook in [`operations.md`](operations.md#firm-resolution-recalls-resolve-firms) |
| What are the `quantity_*` fields on `recall_product`? | [Quantity fields section below](#recall_product-quantity-fields-c13) |
| What countries are in `distribution_country_codes`? | `dbt/seeds/country_iso.csv` (~135 ISO-3166-1 alpha-2 codes; excludes `US` and `Georgia`); [country columns section below](#recall_distribution_area-country-columns-c12) |
| Which `fct_*` models join `dim_date`? | The five date-grained aggregates: `fct_recalls_by_month`, `fct_recalls_by_year`, `fct_recalls_by_week`, `fct_recalls_monthly_trend`, `fct_units_recalled` (the C11 join scope). The other four `fct_*` carry no `date_trunc` — see [`gold_design_notes.md`](gold_design_notes.md) for the `dim_date` design and why those four stay date-free. |

---

## New and extended silver columns (Phase 7)

### `recall_event.terminated_year` (C6)

**Source:** USDA + USCG only (NULL for CPSC, FDA, NHTSA).

**Type:** `int`, nullable.

**Semantics:** the **year** in which the recall was closed / terminated. USDA uses a year-only `closed_year` field (post-2026-06 API change) when the precise `closed_date` is absent; `terminated_year` is `coalesce(closed_year::int, extract(year from closed_at)::int)`. USCG: `extract(year from case_close_date)::int`.

**Relation to `terminated_at`:** `terminated_at` carries the full `timestamptz` when available; `terminated_year` carries the year. When only year-grain data is available (USDA `closed_year`), `terminated_at` is NULL but `terminated_year` is set — **no fabricated date**. When a precise close date is present, both are populated (and `terminated_year = extract(year from terminated_at)`).

**Design note:** rationale in [`silver_design_notes.md`](silver_design_notes.md) (C6).

### `recall_event.reason_category_tokens` (C14)

**Source:** USDA only (NULL for all other sources).

**Type:** `jsonb` (array of text tokens), nullable.

**Semantics:** the USDA FSIS recall-reason string (`reason_category`) split on commas into an array of clean tokens. The 9 known taxonomy values are: `Import Violation`, `Insanitary Conditions`, `Misbranding`, `Mislabeling`, `Processing Defect`, `Produced Without Benefit of Inspection`, `Product Contamination`, `Unfit for Human Consumption`, `Unreported Allergens`. None contain internal commas (verified 2026-06-09), so the comma-split is lossless.

**Why jsonb (not `text[]`):** matches the bronze jsonb representation, supports containment filtering (`reason_category_tokens ? 'Unreported Allergens'`), and avoids the dbt-postgres unit-test `cast(null as ARRAY)` limitation.

**Raw value preserved:** `recall_event.reason_category` holds the original comma-joined text. Both columns are present.

**Guard:** `assert_usda_reason_tokens_in_taxonomy.sql` (severity warn) — flags any token not in the 9-value set (signals taxonomy drift or a comma-split bug).

### `recall_product.processing_categories` (C14)

**Source:** USDA only (NULL for all other sources).

**Type:** `jsonb` (array of text tokens), nullable.

**Semantics:** the USDA FSIS processing-category string (`type` / `processing`) split on commas into an array of clean tokens. The 10 known taxonomy values are: `Eggs/Egg Products`, `Fully Cooked - Not Shelf Stable`, `Heat Treated - Not Fully Cooked - Not Shelf Stable`, `Heat Treated - Shelf Stable`, `Not Heat Treated - Shelf Stable`, `Products with Secondary Inhibitors - Not Shelf Stable`, `Raw - Intact`, `Raw - Non Intact`, `Thermally Processed - Commercially Sterile`, `Unknown`. Tokens use ` - ` and `/` internally but never a comma (verified 2026-06-09).

**Raw value preserved:** `recall_product.type` holds the original comma-joined processing text for USDA.

**Guard:** `assert_usda_processing_tokens_in_taxonomy.sql` (severity warn) — flags any token outside the 10-value set.

### `recall_distribution_area` country columns (C12)

Added to the `recall_distribution_area` sidecar alongside the existing state columns.

| Column | Type | Semantics |
|---|---|---|
| `distribution_country_codes` | `text[]` | ISO-3166-1 alpha-2 codes for foreign countries in the recall's distribution. **Foreign-only** — `US` is intentionally absent (US distribution is authoritatively carried by `distribution_scope` + `distribution_state_codes`; duplicating `US` here would denormalize a fact already stored). Empty array `{}` when the recall has states but no foreign countries. |
| `n_distribution_countries` | `int` | Count of distinct foreign countries. |

**Seed:** `dbt/seeds/country_iso.csv` (~135 countries, ISO-3166-1 alpha-2; deliberately excludes `US` and `Georgia` — the latter to avoid a US-state collision; Georgia-the-country is effectively absent in FDA distribution data and the false-positive risk on US-state Georgia is real).

**Grain:** expanded from "≥1 US state" to "≥1 state OR ≥1 country" via a FULL OUTER join between `state_agg` and `country_agg`. A country-only recall gets a row with `distribution_state_codes = {}`.

**Parse precision:** country NAMES matched whole-word in the international tail only (FDA free text cut at the first international marker, so US place-names that share a country name — Mexico MO, Peru IN — sit in the domestic head and are never matched). Foreign 2-letter ISO codes are NOT matched (too ambiguous with USPS codes).

**Design rationale:** [`silver_design_notes.md`](silver_design_notes.md) (C12).

**Guards:**
- `assert_distribution_area_has_geography.sql` (error) — every row must have ≥1 state or ≥1 country.
- `assert_distribution_country_codes_valid_iso.sql` (warn) — every emitted code must be in the `country_iso` seed.

### `recall_product` quantity fields (C13)

Added to `recall_product` via a LEFT JOIN to `quantity_crosswalk` on `raw_quantity = number_of_units`.

| Column | Type | Semantics |
|---|---|---|
| `quantity_value` | `numeric`, nullable | Parsed numeric quantity. NULL for unparseable / messy shapes (lot ids, list indices, code prefixes, N/M container compounds, multi-product breakdowns). v1 precision-scoped: emits a value for clean single-quantity strings only (~70% value+unit, ~17% value-only; the messy ~10% deliberately NULLed). |
| `quantity_unit` | `text`, nullable | Canonical unit from the `quantity.py` taxonomy (e.g. `each`, `case`, `pound`, `liter`). NULL when `quantity_value` is NULL or no unit was parseable. |
| `quantity_category` | `text`, nullable | Incommensurability guard: `count` (individual items/containers), `weight` (mass units), `volume` (liquid units), `grouping` (lots/batches/components — not a retail count). NULL when no value parsed. Never sum across categories (`1,000 cases ≠ 1,000 lbs`). |
| `quantity_basis` | `text`, always present | `per_product` (the quantity is product-specific), `total_all_products` (a recall-wide total that repeats identically across all product rows for the same recall), `unknown`. FDA `product_distributed_quantity` is typically `total_all_products`. |

**v1 precision scope:** the messy ~10% (lot ids, list indices, code prefixes, N-over-M container compounds, multi-product breakdowns) is deliberately NULLed rather than parsed with a confident-but-wrong value. Reliably extracting that tail requires sentence-level reasoning; deferred to an LLM-extractor v2 — see [`freetext-enrichment-backlog.md`](../project_scope/freetext-enrichment-backlog.md).

**Grain trap:** `quantity_value` is a recall-grain field that repeats identically across all product rows for the same recall (the recall-wide total appears on every product). Consumers must use `max(quantity_value)` per `(recall_event_id, quantity_category)`, not `sum`. `fct_units_recalled` applies this correctly.

**Parser:** `src/enrichment/quantity.py` (pure, side-effect-free); populated by `recalls parse-quantities` via `src/enrichment/quantity_crosswalk_writer.py`.

**Design rationale (grain traps + precision scope):** [`silver_design_notes.md`](silver_design_notes.md) (C13).

---

## `fct_recalls_by_country` (C12)

**Grain:** one row per `(source, country_code)` (plus `'ALL'`-source rollup via `GROUPING SETS`).

**Sources:** FDA + USDA only. CPSC, NHTSA, USCG carry no distribution field and contribute nothing.

**Key columns:** `source` (text, `'ALL'` for the rollup), `country_code` (ISO-3166-1 alpha-2), `recall_count` (distinct recall_event_id count).

**The US cell is derived, not stored:**
- US distribution is authoritatively carried by `distribution_scope` (Nationwide/Regional) + `distribution_state_codes`; storing a literal `US` in silver would denormalize a fact already present.
- The gold view derives the US cell: a recall qualifies as US-distributed if `source = 'USDA'` (FSIS is US-only) OR `distribution_scope in ('Nationwide', 'Regional')` OR `cardinality(distribution_state_codes) > 0` OR `distribution_area_summary ~* '\ynationwide\y'` (catches international recalls that are also US-nationwide).
- Foreign cells unnest `distribution_country_codes[]` directly.

**Multi-valued geography:** a US+Canada recall counts once for `US` and once for `CA` — per-country counts sum to more than the distinct-recall total (an industry-reach reading, consistent with the state distribution lens). Empirically US dominates (~50,854 recalls vs. ~4,500 for Canada, the next highest).

**Design rationale:** [`gold_design_notes.md`](gold_design_notes.md).

---

## `fct_units_recalled`

**Grain:** one row per `(source, unit_category, period)` where `period = dim_date.month_start`.

**Key columns:** `source`, `unit_category`, `period`, `recalls_with_units`, `total_units`, `avg_units_per_recall`, `max_units`.

**The measure means different things per source** — always filter by source; there is deliberately no `'ALL'` rollup:
- `NHTSA` / `USCG`: vehicles / boats potentially affected (clean integer `unit_count`); `unit_category = 'count'`.
- `FDA`: quantity distributed (`recall_product.quantity_value`); `unit_category` from the quantity taxonomy.
- `USDA`: weight recovered (`qty_recovered`); `unit_category` typically `'weight'`.

**Grain traps (enforced in the model):**
- NHTSA `potaff` repeats across a campaign's component rows → collapsed to `max(unit_count)` per recall (verified constant within a campno — max is exact).
- USCG is 1:1 recall/product (no fan-out).
- FDA/USDA `quantity_value` is recall-grain (repeats identically across product rows) → `max(quantity_value)` per `(recall_event_id, unit_category)` (see [quantity field grain trap above](#recall_product-quantity-fields-c13)).

**`total_units`** sums per-recall magnitudes — a recall-magnitude measure, not unique items.

---

## dbt tests (Phase 7)

New singular tests added in Phase 7 (C12–C20). Tests in `dbt/tests/source_assumptions/` inherit `severity='warn'` from the directory config; others declare severity inline. Exception: `assert_cpsc_products_array_append_only.sql` is escalated to `severity=error` via an in-file `config()` override (ADR 0031 amendment 2026-06-13) — it now guards CPSC product identity, not just fragmentation.

| Test file | Severity | Purpose |
|---|---|---|
| `assert_quantity_parse_coverage.sql` | warn | Quantity parser must extract a value from ≥75% of non-empty FDA `product_distributed_quantity` rows. A drop below the floor signals a genuine API format change (not the intentional precision NULLs). Baseline ~87% (2026-06-09); floor deliberately conservative. |
| `assert_usda_multivalue_fields_are_arrays.sql` | error | Every USDA bronze multivalue field must be a JSONB **array** or NULL — never a bare scalar. Catches a migration-0028 replay miss or future API field that regresses to scalar. The 10 fields are migration-0028's `_ARRAY_FIELDS`. |
| `assert_usda_reason_tokens_in_taxonomy.sql` | warn | Every `reason_category_tokens` element must be in the 9-value FSIS reason taxonomy. A returned row is either taxonomy drift (extend the list) or a comma-split bug (a token that contains an internal comma — the correctness assumption). |
| `assert_usda_processing_tokens_in_taxonomy.sql` | warn | Every `processing_categories` element must be in the 10-value FSIS processing taxonomy. Same taxonomy-drift-vs-split-bug logic. |
| `assert_distribution_area_has_geography.sql` | error | Every `recall_distribution_area` row must have ≥1 state or ≥1 country. An empty-both-arrays row would be a FULL OUTER build bug. |
| `assert_distribution_country_codes_valid_iso.sql` | warn | Every `distribution_country_codes` element must be a known alpha-2 in `country_iso`. Green by construction (parse JOINs the seed); catches a future hand-edit or seed drift. |
| `assert_fda_nhtsa_have_firms.sql` (source_assumptions/) | warn | Every FDA and NHTSA `recall_event` must have ≥1 firm in `recall_event_firm`. Both sources always name a recalling firm; a firmless FDA/NHTSA recall is an extraction regression. Baseline 0. USDA/CPSC/USCG excluded (documented firmless baselines). |
| `assert_usda_multi_establishment_recalls.sql` (source_assumptions/) | warn | Counts USDA recalls with ≥2 establishments in the bronze `establishment` array. Baseline 4 (2026-06-09). A material increase signals that multi-establishment recalls have stopped being a rounding error and the grain-change resolution work is worth revisiting. |
