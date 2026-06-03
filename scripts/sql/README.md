# `scripts/sql/` — data investigation queries

This directory holds all data-investigation SQL for the project: bronze
shape probes, silver/gold spot-checks, schema-failure investigation,
join-coverage measurements, and per-source diagnostic batches.

## Why this directory exists

Per project convention (see `documentation/decisions/` and the per-source
findings docs):

- **Every investigation query is reviewable in git** before it touches the
  database.
- **Every query is `psql -f`-runnable** so the user can run it from the
  command line without retyping or fishing the SQL out of chat.
- **Findings docs reference query files by path**, not by inlined SQL — the
  docs interpret the results, the SQL files are the source-of-truth for the
  computation.

This means `psql -c "..."` ad-hoc queries are discouraged; if a query is
worth running once, it's worth committing. Findings docs that previously
inlined SQL (CPSC, FDA, USDA recall) reference the migrated files in this
tree.

## Layout

```
scripts/sql/
├── README.md                                (this file)
├── <source>/
│   ├── bronze/
│   │   └── <purpose>.sql
│   ├── silver/
│   │   └── <purpose>.sql
│   └── gold/
│       └── <purpose>.sql
└── cross_source/                            (when added)
    └── <layer>/
        └── <purpose>.sql
```

Sources mirror the project's source naming: `cpsc/`, `fda/`,
`usda_recalls/`, `usda_establishments/`, `nhtsa/`, `uscg/`. The
`cross_source/` directory is for queries spanning multiple sources (e.g.,
firm entity resolution probes once Phase 6 builds them).

## File contents

- **Lead with a comment block** documenting purpose, when to run, what each
  result column means, any inputs that might be tweaked.
- **Multiple queries per file are fine** when they form a logical
  diagnostic batch (e.g., the `explore_bronze_shape.sql` files run a full
  bronze characterization in one shot). Use `\echo '=== Qn: <title> ==='`
  separators between queries for readable terminal output.
- **One query per file** when the query is standalone or part of a larger
  decision-making process (e.g., `probe_recall_join_coverage.sql` in
  `usda_establishments/bronze/`).
- Use plain SQL — no shell variables. The user pastes the file path into
  `psql -f`.

## Running a query

```bash
set -a && . .env && set +a
PGPASSWORD="$NEON_PASSWORD" psql -h "$NEON_HOST" -U "$NEON_USER" -d "$NEON_DBNAME" \
  -f scripts/sql/<source>/<layer>/<query>.sql
```

The `-f` flag streams the file. `\echo` lines in the SQL print headers
between query result blocks so the output is paste-able into a findings doc
without manual demarcation.

## Index

### CPSC

| Path | Purpose | Source doc |
|---|---|---|
| `cpsc/bronze/explore_bronze_shape.sql` | 12-query batch covering cardinality, cadence, edit detection, dedup summary, null/JSONB rates, products-per-recall, hazard-type check, spike/gap analysis, extraction run history | `documentation/cpsc/first_extraction_findings.md` |
| `cpsc/bronze/inspect_array_field_population.sql` | 11-query nested-key batch (snake_case JSONB drill): CompanyID emptiness across 4 firm roles, Products[]/Hazards[]/Images[] field populated rates, RemedyOptions/ManufacturerCountries enum distributions, per-array element-count, Retailers[] Name length/distinctness signal | `documentation/cpsc/field_audit_2026_w22.md` §9 |
| `cpsc/bronze/inspect_firm_name_fragmentation.sql` | 6-query firm-dim fragmentation baseline (silver-remap W1): per-role firm footprint, Option-B retailer-removal reduction, Bug-2 suffix-strip collapse SIMULATION (read-only, conservative lower bound), strippable-suffix prevalence, recurring-firm rate current vs stripped, sample before→after collapses | `documentation/cpsc/field_audit_2026_w22.md` §9 + `documentation/audit/bronze_corpus_profile.md` §2/§3/§5 |

### FDA

| Path | Purpose | Source doc |
|---|---|---|
| `fda/bronze/explore_bronze_shape.sql` | 16-query batch covering cardinality, cadence, edit detection, dedup summary, multi-product event detail, deep-dive on a single recall, center/product/phase distributions, null rates, free-text quantity samples, gap analysis, extraction run history | `documentation/fda/first_extraction_findings.md` |
| `fda/bronze/inspect_field_population.sql` | 8-query silver-remap profiling batch: narrative length stats (Bug 1/2/3), enum domains (voluntary_type/classification/notification), termination_dt↔phase + null-recall_num↔NC relationships, distribution-area vs short-reason content samples | `documentation/fda/field_audit_2026_w22.md` §8 + `documentation/audit/bronze_corpus_profile.md` |
| `fda/bronze/profile_freetext_normalization.sql` | 6-query scoping batch sizing the normalization design for `product_distributed_quantity` + `distribution_area_summary_txt`: quantity pattern-coverage buckets + messy tail, distribution scope buckets + negation false-positive risk + Nationwide surface-form compression | normalization-tier decision in `cross_source_consolidation.md` (W2) |

### USDA recalls

| Path | Purpose | Source doc |
|---|---|---|
| `usda_recalls/bronze/explore_usda_bronze.sql` | First-extraction shape probe | `documentation/usda/first_extraction_findings.md` |
| `usda_recalls/bronze/inspect_field_population.sql` | 7-query silver-remap sibling (English-latest, nullif-based): silver-grain snapshot, risk_level×recall_classification crosstab (the derive proof), length stats for free-text lift fields, comma-separated multi-value prevalence + exploded-token enum SSOT (processing/recall_reason), all-time cadence | `documentation/usda/field_audit_2026_w22.md` §9 + `documentation/audit/bronze_corpus_profile.md` |
| `usda_recalls/bronze/verify_usda_first_extraction.sql` | Post-extraction verification queries | same |
| `usda_recalls/bronze/probe_recall_id_storage.sql` | Confirm source_recall_id storage shape (leading/trailing whitespace contamination); brackets-wrapped values + length + corpus-wide whitespace summary | — |
| `usda_recalls/bronze/diagnose_wave_field_drivers.sql` | For a given run, count which JSONB fields actually differ between new and prior bronze versions across ALL re-versioned rows. Authoritative answer to "is this wave driven by one field?" | — |
| `usda_recalls/bronze/classify_field_diffs_whitespace.sql` | Reusable. For a given run + JSONB field, classify each diff as whitespace_only vs real_content_change vs null_transition. Empirical justification for any hash-exclude proposal (ADR-0032-analog). Parameterized via -v run_id and -v field | — |
| `usda_recalls/bronze/list_inserted_recalls_per_run.sql` | For every USDA run with records_inserted > 0, list the (source_recall_id, langcode) pair(s) it inserted. Maps a "wave" date to specific recall IDs to drive subsequent diagnose_payload_drift_for_recall.sql or scripts/usda_recalls/inspect_raw_landing_for_recall.py probes. | — |

### USDA establishments

| Path | Purpose | Source doc |
|---|---|---|
| `usda_establishments/bronze/explore_bronze_shape.sql` | 8-query batch: cardinality, status enum exhaustiveness, per-field nullability, false-sentinel-as-text observations, JSONB array shapes, `latest_mpi_active_date` Finding G verification, state distribution, re-version pattern | `documentation/usda/establishment_first_extraction_findings.md` |
| `usda_establishments/bronze/inspect_join_key_and_dbas.sql` | 5-query silver-remap sibling (latest-per-id): establishment_number join-key profile (population/uniqueness/length, K), grant-prefix + multi-grant form breakdown, establishment_name uniqueness (why the key is the number), dbas fill + exact `N/A`/`None`/`''` placeholder counts (§7 element-filter sizing), size enum SSOT | `documentation/usda/field_audit_2026_w22.md` §9 + `documentation/audit/bronze_corpus_profile.md` |
| `usda_establishments/bronze/explore_rejected_failures.sql` | Diagnose `usda_fsis_establishments_rejected` after a failed extraction; Pydantic ValidationError prefix histogram, sample raw_record, schema-field mention counts | (used standalone during Phase 5b.2 first extraction) |
| `usda_establishments/bronze/probe_recall_join_coverage.sql` | 6-query batch measuring recall→establishment join coverage; name-only and DBA-fallback rates, per-distinct-name and per-record match counts, sample of unmatched names, multi-hit popularity | `documentation/usda/establishment_join_coverage.md` |
| `usda_establishments/bronze/list_status_flips.sql` | Enumerate establishments whose `status_regulated_est` changed in a given run (active ↔ Inactive); direction summary, per-establishment detail, geographic distribution. Phase 6 firm-resolution test-case generator | — |
| `usda_establishments/silver/verify_dbas_placeholder_strip.sql` | W4 Phase E green-build spot-check: confirms the `'N/A'`/`'None'`/`''` dbas element-strip landed (0 placeholder rows, DBA-less → NULL not `[]`, sample survivors) | — |

### NHTSA

Silver-remap-relevant subset; the full `nhtsa/bronze/` suite also carries ~18 identity/drift diagnostic scripts (the SCD reference implementation — `decompose_*`, `attribute_*`, `verify_*`, `investigate_*`).

| Path | Purpose | Source doc |
|---|---|---|
| `nhtsa/bronze/explore_bronze_shape.sql` | Cardinality / cadence / dedup / category distributions / null rates (rerun at corpus scale for the profile) | `documentation/nhtsa/field_audit_2026_w22.md` §9 |
| `nhtsa/bronze/inspect_field_population.sql` | Silver-remap field-population + enum/length profiling | `documentation/nhtsa/field_audit_2026_w22.md` §9 + `documentation/audit/bronze_corpus_profile.md` |
| `nhtsa/bronze/inspect_mfgname_vs_mfgtxt.sql` | filer (`mfgname`) vs manufacturer (`mfgtxt`) role-split evidence | same |
| `nhtsa/bronze/assert_eleven_tuple_identity_stable.sql` / `assert_nine_tuple_identity_stable.sql` | SCD fragmentation baselines (refreshes ADR 0031 / 0033 at corpus scale) | same |

### USCG recalls

| Path | Purpose | Source doc |
|---|---|---|
| `uscg/bronze/explore_first_extraction.sql` | 9-query batch: extraction summary, rejection breakdown, per-field NULL rates, disposition/manufacturer/opened-on/prefix distributions (primary corpus-scale field inspector) | `documentation/uscg/scraping_observations.md` |
| `uscg/bronze/inspect_field_population.sql` | 8-query silver-remap batch: severity enum, hin `N/A` sentinel, boat_type code list, narrative length cap, model_year format, mic↔company_name consistency | `documentation/uscg/field_audit_2026_w22.md` §9 |
| `uscg/bronze/diagnose_rejections.sql` | Pydantic + invariant quarantine analysis | same |

### USCG manufacturers

| Path | Purpose | Source doc |
|---|---|---|
| `uscg_manufacturers/bronze/explore_bronze_shape.sql` | 7-query silver-remap batch: cardinality + reassignment edit-versions (the temporal-SCD anchor), MIC format/sub-namespace breakdown, multi-sentinel missing rates, state distribution (incl. Canadian), address-truncation cliff, reassignment detail, recall→directory MIC coverage | `documentation/uscg/field_audit_2026_w22.md` §9 + `documentation/audit/bronze_corpus_profile.md` |
| `uscg_manufacturers/bronze/diagnose_short_circuit_miss.sql` | Short-circuit gate diagnosis; isolates the 2 reassignment inserts (AXY/COP) | `documentation/uscg/manufacturer_scraping_observations.md` §M |

### USCG manufacturer details

| Path | Purpose | Source doc |
|---|---|---|
| `uscg_manufacturer_details/bronze/explore_bronze_shape.sql` | 7-query silver-remap batch (with a seed-state guard): cardinality + edit-versions, listing-coverage join, succession-lineage fill (the SCD-2 inputs), `(OOB)`-year parseability, `date_modified` change signal, status enum, `type` run-on fill | `documentation/uscg/field_audit_2026_w22.md` §9 + `documentation/audit/bronze_corpus_profile.md` |
| `uscg_manufacturer_details/bronze/preflight_check.sql` | Pre-seed readiness check (listing work-list size, detail cold-start = 0, watermark) | `documentation/uscg/manufacturer_scraping_observations.md` §M |

### cross_source

| Path | Purpose | Source doc |
|---|---|---|
| `cross_source/scd_monitors/assert_classification_stable.sql` | Measure the recall-classification/severity **amendment rate** across content-hash edit-versions (FDA/USDA/USCG), to validate the `classification` SCD-type designation. Measure-forward (reads ~0 until incrementals re-bank history). | `documentation/audit/scd_field_designations.md` §4 |
| `cross_source/scd_monitors/assert_lifecycle_stable.sql` | Measure the recall **lifecycle/status transition rate** across edit-versions (FDA `phase_txt` / USDA `recall_type` / USCG `disposition`), validating the `lifecycle_status` Type-2-BENEFIT designation. Measure-forward. | `documentation/audit/scd_field_designations.md` §4 |
| `cross_source/scd_monitors/assert_mic_holder_stable.sql` | USCG-only — detect **MIC reassignment** (the firm anchor pointing to a different company over time). Q1 dynamic (edit-versions, measure-forward); **Q2–Q4 static** (the detail `past_company_*` lineage — returns real data now: the corpus-wide + recall-exposed misattribution surface). | `documentation/audit/scd_field_designations.md` §4 |

(Optional `cross_source/bronze/profile_grain_and_keys.sql` grain roll-up still pending — see `silver-field-remap-plan.md` W1.)

## When to add a new file

- The user (or an analysis branch) needs a non-trivial query against any
  bronze, silver, or gold table.
- A query is being run more than once — even if it's "just" a spot-check —
  commit it.
- A finding in a documentation file would otherwise inline the SQL.

## When NOT to add a new file

- Single-line cardinality probes (`select count(*) from foo`) embedded in
  the workflow of a larger analysis. Promote to a file once the query
  starts to grow or once the result becomes evidence in a doc.
- Production queries called by application code — those live with the code
  (e.g., `src/extractors/<source>.py` for extractor-side queries, dbt
  models for transformation queries).
