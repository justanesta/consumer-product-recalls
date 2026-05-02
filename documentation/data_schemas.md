# Data schemas reference

A reader's index for "what does column X mean, where is it defined, and what does this domain term refer to?" The authoritative definitions for the schemas themselves live in code (`src/schemas/`) and dbt configuration (`dbt/models/`). This document is the glossary, the cross-reference, and the quick-lookup — not a copy of the schemas.

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

Bronze tables follow the [ADR 0027](decisions/0027-bronze-storage-forced-transforms-only.md) rule: only storage-forced transforms (date string → datetime, "True"/"False" → bool) happen at the Pydantic layer. Value-level normalization moves to silver staging.

### Pipeline state (shared across sources)

| Table | Migration | Purpose | Authoritative ADR |
|---|---|---|---|
| `source_watermarks` | `0001_baseline.py` (+ per-source seeds in `0008_seed_usda_establishments_watermark.py`) | Cursor state — where the next incremental query should start | [ADR 0020](decisions/0020-pipeline-state-tracking.md) |
| `extraction_runs` | `0001_baseline.py` (+ `change_type` column added by Phase 5b.2 per ADR 0027) | Run metadata — `started_at`, `status`, `records_extracted`, `records_inserted`, `change_type` | [ADR 0020](decisions/0020-pipeline-state-tracking.md), [ADR 0027](decisions/0027-bronze-storage-forced-transforms-only.md) |
| `extraction_run_identities` | (Phase 6 — not yet shipped) | Per-run identity manifest for lifecycle dimensions | [ADR 0026](decisions/0026-lifecycle-tracking-snapshot-presence-manifest.md) |

### Silver (unified across sources, dbt-managed)

| Model | dbt SQL | dbt tests / schema | Per-source mapping |
|---|---|---|---|
| `staging/stg_cpsc_recalls` | `dbt/models/staging/stg_cpsc_recalls.sql` | `stg_cpsc_recalls.yml` | n/a |
| `staging/stg_fda_recalls` | `stg_fda_recalls.sql` | `stg_fda_recalls.yml` | n/a |
| `staging/stg_usda_fsis_recalls` | `stg_usda_fsis_recalls.sql` | `stg_usda_fsis_recalls.yml` | n/a |
| `silver/recall_event` | `recall_event.sql` | `_silver.yml` | [`silver_design_notes.md`](silver_design_notes.md) |
| `silver/recall_product` | `recall_product.sql` | `_silver.yml` | [`silver_design_notes.md`](silver_design_notes.md) |
| `silver/firm` | `firm.sql` | `_silver.yml` | [`silver_design_notes.md`](silver_design_notes.md) |
| `silver/recall_event_firm` | `recall_event_firm.sql` | `_silver.yml` | [`silver_design_notes.md`](silver_design_notes.md) |
| `silver/recall_event_history` | (Phase 6 — not yet shipped) | (TBD) | per [ADR 0022](decisions/0022-fda-history-endpoints-empty-snapshot-synthesis-for-all-sources.md) |

### Gold (denormalized, dbt-managed)

| Model | dbt SQL | Tests |
|---|---|---|
| `gold/recalls_by_month` | `dbt/models/gold/recalls_by_month.sql` | `_gold.yml` |
| (more gold views in Phase 6) | — | — |

---

## Glossary

Domain-specific terms used across this project. When in doubt, this is the canonical definition.

### Records and granularity

- **Recall event.** A single regulatory action by a single agency. One row in `silver.recall_event`. Identified by `(source, source_recall_id)`. CPSC's `RecallNumber` (e.g. `"24-158"`), FDA's `RECALLEVENTID` (e.g. `"98724"`), USDA's `field_recall_number` (e.g. `"049-2024"`) are the source-side primary keys.
- **Recall product.** A single product-line within a recall event. One row in `silver.recall_product`. CPSC encodes products as a JSONB array per event (exploded in dbt); FDA emits one bronze row per `PRODUCTID` (already flat); USDA recall events do not split products at this stage. See [`silver_design_notes.md`](silver_design_notes.md) §1–2.
- **Firm.** A company involved in a recall in some role. Deduplicated by normalized name (`UPPER(TRIM(firm_name))`). See [ADR 0002](decisions/0002-unit-of-analysis-header-line-firm.md).
- **Role.** The relationship of a firm to a recall event. Allowed values: `manufacturer`, `retailer`, `importer`, `distributor`, `establishment`. The `establishment` value is USDA-specific (FSIS-regulated facility). Per [ADR 0002](decisions/0002-unit-of-analysis-header-line-firm.md).
- **Event type.** Forward-compatibility column on `recall_event` (`event_type` defaults to `'RECALL'`). See [ADR 0003](decisions/0003-event-type-discriminator.md). Reserved for future non-recall regulatory actions (e.g., enforcement actions, market withdrawals).

### Pipeline mechanics

- **Watermark.** Per-source cursor recording the most recent successful extraction's reference timestamp. Used by the next incremental query as `WHERE last_modified_field >= <watermark>`. Stored in `source_watermarks`. **Advisory, not authoritative** — content-hash dedup is what actually prevents duplicates ([ADR 0020](decisions/0020-pipeline-state-tracking.md)).
- **Extraction run.** A single invocation of one source's extractor. One row in `extraction_runs` with `started_at`, `status`, `records_extracted`, `records_inserted`, `change_type`.
- **Content hash.** SHA-256 of the canonical (sorted-key, no-whitespace) JSON serialization of a bronze record's payload. Conditional-insert key — bronze never duplicates rows with the same hash. Defined in `src/bronze/hashing.py` per [ADR 0007](decisions/0007-lineage-via-bronze-snapshots-and-content-hashing.md).
- **Canonical record dict.** The output of `to_canonical()` on a Pydantic schema instance — the dict that gets hashed. Changes to canonicalization (Pydantic normalizers, hashing helpers) invalidate every prior bronze hash; this is treated as a schema migration per [ADR 0007](decisions/0007-lineage-via-bronze-snapshots-and-content-hashing.md) line 70 and [ADR 0027](decisions/0027-bronze-storage-forced-transforms-only.md).
- **Change type.** Column on `extraction_runs` distinguishing routine extraction from re-baseline waves. Allowed values: `routine` (default), `schema_rebaseline`, `hash_helper_rebaseline`, `historical_seed` (added by [ADR 0028](decisions/0028-backfill-historical-reextraction-semantics.md)). The `recall_event_history` model filters out non-routine runs from edit detection.
- **Deep rescan.** Re-fetch from a source over a wide window, ignoring the watermark. Per-source workflow at `.github/workflows/deep-rescan-<source>.yml`. Used for edit detection on weak-watermark sources and for one-time historical backfill. See [ADR 0010](decisions/0010-ingestion-cadence-and-github-actions-cron.md), [ADR 0023](decisions/0023-fda-deep-rescan-required-archive-migration-detected.md), [ADR 0028](decisions/0028-backfill-historical-reextraction-semantics.md).
- **R2 replay.** Re-run the bronze loader against raw payloads already in R2, without contacting the source. Used for schema-drift recovery, normalizer changes, hashing-helper updates. See [ADR 0028](decisions/0028-backfill-historical-reextraction-semantics.md) Mechanism B.
- **Re-baseline.** A wave of bronze inserts produced by an our-side change (Pydantic normalizer or hashing helper change) where the source was unchanged. Marked `change_type='schema_rebaseline'` or `'hash_helper_rebaseline'` to distinguish from real edits. See [ADR 0027](decisions/0027-bronze-storage-forced-transforms-only.md), `documentation/operations/re_baseline_playbook.md`.
- **Run identity manifest.** Per-run record of which `(source_recall_id, [identity-suffix])` tuples were present in the response. Substrate for silver lifecycle dimensions. USDA-only initially. See [ADR 0026](decisions/0026-lifecycle-tracking-snapshot-presence-manifest.md).

### Failure routing

- **T0 — raw landing.** Cloudflare R2 bucket `<source>/<extraction_date>/<key>`. Every extracted byte lands here before validation. Immutable, retain-forever. See [ADR 0004](decisions/0004-four-layer-medallion-pipeline.md), [ADR 0005](decisions/0005-storage-tier-neon-and-r2.md).
- **T1 — rejected tables.** Per-source `<source>_recalls_rejected` Postgres tables. Records that fail Pydantic validation or business invariants are routed here with `failure_reason`, `failure_stage` (`validate` or `invariants`), and `raw_landing_path` pointing back to T0. See [ADR 0013](decisions/0013-error-handling-retries-idempotency-and-quarantine.md).
- **T2 — alert.** Structured warning log + workflow non-zero exit when rejection rate exceeds threshold. v1 alert surface is the GitHub Actions UI ([ADR 0029](decisions/0029-application-observability-and-alerting.md)).
- **Quarantine.** The act of routing a record to T1 (rejected table) instead of bronze. Schema violations and invariant failures both quarantine; the record's `failure_stage` distinguishes which path triggered it.

### Source-specific terms

- **Bilingual pair (USDA).** A USDA recall published in both English and Spanish. Each language is a separate row in the FSIS API response with the same `field_recall_number` and a different `langcode`. See [ADR 0006](decisions/0006-usda-bilingual-record-deduplication.md). **Empirical note:** ~13.3% of bilingual pairs do not update atomically — silver lifecycle logic must treat each language independently per [ADR 0026](decisions/0026-lifecycle-tracking-snapshot-presence-manifest.md).
- **Establishment (USDA).** An FSIS-regulated facility (`establishment_id` is the FSIS primary key). Distinct from "manufacturer" — an establishment is the *recalling* facility, often co-incident with the manufacturer but legally a separate role. Used in `recall_event_firm.role='establishment'`.
- **FEI number (FDA).** FDA Establishment Identifier (`firmfeinum`). Globally unique facility ID across FDA-regulated firms; the strongest cross-source firm anchor available. See [ADR 0002](decisions/0002-unit-of-analysis-header-line-firm.md).
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
| What does `change_type='schema_rebaseline'` mean? | This file's "Pipeline mechanics" glossary; full context in [ADR 0027](decisions/0027-bronze-storage-forced-transforms-only.md) and `documentation/operations/re_baseline_playbook.md` |
| Why does `firm` deduplicate by normalized name and not by company ID? | [ADR 0002](decisions/0002-unit-of-analysis-header-line-firm.md), [`silver_design_notes.md`](silver_design_notes.md) §4 |
| What goes in `_rejected` tables vs. bronze? | [ADR 0013](decisions/0013-error-handling-retries-idempotency-and-quarantine.md); fields documented in `migrations/versions/0001_baseline.py` |
| Where is the FDA `RECALLEVENTID` mapped to silver? | `dbt/models/staging/stg_fda_recalls.sql`, then [`silver_design_notes.md`](silver_design_notes.md) "Column mapping" table |
| What's the watermark column for source X? | `source_watermarks` row + the per-source extractor in `src/extractors/<source>.py` (look for `WATERMARK_FIELD`-style class constants) |
| What are the allowed values of `recall_event_firm.role`? | `dbt/models/silver/_silver.yml` (`accepted_values` test) — currently `['manufacturer', 'retailer', 'importer', 'distributor', 'establishment']` |
