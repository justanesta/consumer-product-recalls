# 0007 — Lineage tracking via bronze snapshots + content hashing

- **Status:** Accepted
- **Date:** 2026-04-16

## Context

Data governance, lineage, and audit are stated project goals (`CLAUDE.md`). The platform should be able to answer "when did this value change, and what was it before?" for any value in silver, regardless of which source it came from.

The five in-scope sources have asymmetric capabilities for change tracking:

- **FDA** exposes native field-level history endpoints (`/search/productHistory/{productid}` and `/search/eventproducthistory/{eventid}`) returning `(fieldname, oldvalue, newvalue, eventlmd)` rows.
- **CPSC, USDA, NHTSA, USCG** expose no history endpoints. Any change tracking must be derived by comparing successive ingestions.

Two architectural options were considered:

- **Per-source `*_history` tables populated at ingest time** by computing per-field diffs against the previous bronze row. Mirrors FDA's pattern across all sources but pushes diff complexity into every ingest path.
- **Bronze as a snapshot store; history derived as a query concern.** Bronze tables retain every ingestion as a separate row keyed by `(source_recall_id, extraction_timestamp)`; history is computed on demand using window functions. Simpler ingest, deferred complexity.

The second option is cheaper to implement and lets ingestion stay focused on landing data correctly. The trade-off is bronze storage growth — without mitigation, NHTSA's weekly 14 MB flat file becomes ~728 MB/year of mostly-duplicate data.

## Decision

Bronze tables are snapshot stores, not "current state" stores. A unified `recall_event_history` view in silver synthesizes lineage from bronze.

**Bronze layer rules (apply to every source):**

- Bronze rows are keyed by `(source_recall_id, extraction_timestamp)`, not just `source_recall_id`.
- Each bronze loader computes a SHA-256 hash of the canonical record content (deterministically serialized, key-sorted JSON) and stores it as a column.
- Insertion is conditional: skip if the new hash matches the most recent existing hash for that `source_recall_id`. This dedupes "no real change" snapshots automatically — solves the NHTSA full-refresh problem.
- Snapshot retention policy: daily snapshots retained for 90 days, monthly snapshots retained for 1 year, then pruned. Managed by a scheduled retention job.

**Silver-layer history view:**

- For **FDA**: a dedicated bronze table mirrors `productHistory` and `eventproducthistory` directly (field-level granularity from the source).
- For **CPSC, USDA, NHTSA, USCG**: history is derived via `LAG()` window functions over bronze snapshots, restricted to a configurable allowlist of consumer-meaningful fields (`status`, `classification`, `hazard_short`, `remedy`, `units_affected`, `terminated_at`).
- Both feed a unified `recall_event_history` view that downstream consumers query, so the source-asymmetry is hidden from the API and dashboards.

## Consequences

- One consistent lineage story across all five sources, exposed via one silver view.
- Silent upstream agency revisions are detected and surfaced rather than silently overwriting.
- Bronze storage stays bounded by content-hash dedup + tiered retention.
- Diff complexity for non-FDA sources is contained in one place (the silver view definition), not spread across every ingest path.
- Bronze loaders gain modest complexity: deterministic JSON serialization, hash computation, conditional insert.
- The field-level allowlist for non-FDA history is a config value, not a schema change — easy to expand later without re-ingestion.
- Retention policy is enforced by a scheduled job; a future ADR may revisit retention durations as the platform's audit needs evolve.
- "Why did this value change?" becomes answerable at any point in time within retention — directly demonstrable as a portfolio capability.
