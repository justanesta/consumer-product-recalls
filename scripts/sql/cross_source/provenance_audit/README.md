# Silver/Gold provenance audit — query catalog

Runnable, **read-only** verification queries from the 2026-W25 data-provenance audit. They check that the
*served* Silver/Gold shapes (types, enum cardinality, nullability, grain, empty-string cleanliness) match
the ADR contract + the docs.

**Findings, verdicts, and the prioritized fix plan are single-homed in**
`documentation/audit/silver_gold_provenance_audit_2026_w25.md`. This directory is just the executable half.

## Run

```bash
# per file, against the target Neon branch (psql reads PG* env / .pgpass)
psql -f scripts/sql/cross_source/provenance_audit/10_staging_audit.sql
```

Order: `10`→`41` by medallion layer; `90` = coverage-gap queries (event_type, key-recipe, crosswalk vocab,
distribution conflation). Each block states the **CONFORMS** result — any deviation is a finding.

## Notes

- **Schemas:** staging views = `staging`; silver = `silver`; gold = `gold`; dbt seeds = `public`
  (`public.us_state_abbr.abbr`, `public.country_iso.alpha2`).
- **Exclude from the `''` probe:** date columns (`bgman`/`endman`/`in_business`/`out_of_business`),
  `status_regulated_est` (`''` is a meaningful value), and the history tables
  (`recall_event_history.old_value/new_value`, `recall_product_history` NHTSA cols — `''` by design).
- `90_…` blocks tagged `[CONFIRM NAMES]` reference objects the audit did not statically verify (bronze
  tables, `firm_crosswalk`) — confirm names against the catalog before running, or skip.
