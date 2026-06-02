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
| `usda_recalls/bronze/verify_usda_first_extraction.sql` | Post-extraction verification queries | same |
| `usda_recalls/bronze/probe_recall_id_storage.sql` | Confirm source_recall_id storage shape (leading/trailing whitespace contamination); brackets-wrapped values + length + corpus-wide whitespace summary | — |
| `usda_recalls/bronze/diagnose_wave_field_drivers.sql` | For a given run, count which JSONB fields actually differ between new and prior bronze versions across ALL re-versioned rows. Authoritative answer to "is this wave driven by one field?" | — |
| `usda_recalls/bronze/classify_field_diffs_whitespace.sql` | Reusable. For a given run + JSONB field, classify each diff as whitespace_only vs real_content_change vs null_transition. Empirical justification for any hash-exclude proposal (ADR-0032-analog). Parameterized via -v run_id and -v field | — |
| `usda_recalls/bronze/list_inserted_recalls_per_run.sql` | For every USDA run with records_inserted > 0, list the (source_recall_id, langcode) pair(s) it inserted. Maps a "wave" date to specific recall IDs to drive subsequent diagnose_payload_drift_for_recall.sql or scripts/usda_recalls/inspect_raw_landing_for_recall.py probes. | — |

### USDA establishments

| Path | Purpose | Source doc |
|---|---|---|
| `usda_establishments/bronze/explore_bronze_shape.sql` | 8-query batch: cardinality, status enum exhaustiveness, per-field nullability, false-sentinel-as-text observations, JSONB array shapes, `latest_mpi_active_date` Finding G verification, state distribution, re-version pattern | `documentation/usda/establishment_first_extraction_findings.md` |
| `usda_establishments/bronze/explore_rejected_failures.sql` | Diagnose `usda_fsis_establishments_rejected` after a failed extraction; Pydantic ValidationError prefix histogram, sample raw_record, schema-field mention counts | (used standalone during Phase 5b.2 first extraction) |
| `usda_establishments/bronze/probe_recall_join_coverage.sql` | 6-query batch measuring recall→establishment join coverage; name-only and DBA-fallback rates, per-distinct-name and per-record match counts, sample of unmatched names, multi-hit popularity | `documentation/usda/establishment_join_coverage.md` |
| `usda_establishments/bronze/list_status_flips.sql` | Enumerate establishments whose `status_regulated_est` changed in a given run (active ↔ Inactive); direction summary, per-establishment detail, geographic distribution. Phase 6 firm-resolution test-case generator | — |

### NHTSA, USCG, cross_source

Pending. These directories are created when their respective sources / silver
models land.

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
