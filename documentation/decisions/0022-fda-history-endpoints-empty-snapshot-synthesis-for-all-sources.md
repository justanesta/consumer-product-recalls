# 0022 — FDA history endpoints are empty; snapshot synthesis applies to all five sources

- **Status:** Accepted
- **Date:** 2026-04-26
- **Supersedes:** [ADR 0007](0007-lineage-via-bronze-snapshots-and-content-hashing.md) (partially — the bronze snapshot strategy and content-hashing mechanism are unchanged; only the FDA-specific history-table path is revised)

## Context

ADR 0007 split lineage derivation by source capability:

- **FDA** — use native field-level history endpoints (`/search/productHistory/{productid}` and `/search/eventproducthistory/{eventid}`) as the primary lineage source, backed by dedicated bronze tables (`fda_product_history_bronze`, `fda_event_product_history_bronze`).
- **CPSC, USDA, NHTSA, USCG** — synthesize history via `LAG()` window functions over bronze snapshots.

This split was based on FDA iRES API documentation, which describes the history endpoints as returning `(fieldname, oldvalue, newvalue, eventlmd)` rows for each audited field change.

Phase 5a FDA Bruno exploration (branch `feature/fda-exploration-extractor`, finding L in `documentation/fda/api_observations.md`) tested the history endpoints empirically across four recall events representing distinct lifecycle states:

| Event | RecallNum | State |
|---|---|---|
| 98815 | 2026-xxx | Ongoing (2026) |
| 98279 | 2026-xxx | Terminated (2026) |
| 98286 | 2026-xxx | Terminated (2026) |
| 25159 | 2002-xxx | Archive-migrated (record re-touched 2026) |

All four returned `RESULTCOUNT: 0` from both `/search/productHistory/{productid}` and `/search/eventproducthistory/{eventid}`. Tested across multiple product IDs under each event. The endpoints exist and respond correctly (HTTP 200, FDA success code 400) — they are simply not populated.

The assumption in ADR 0007 that FDA's history endpoints provide a reliable, populated lineage source is empirically false. The original architectural split — FDA as a native-history special case, the other four as snapshot-synthesis sources — does not hold.

## Decision

FDA uses bronze-snapshot synthesis for lineage derivation, identical to CPSC, USDA, NHTSA, and USCG. There is no longer a source-asymmetric lineage path.

**What changes from ADR 0007:**

- The dedicated `fda_product_history_bronze` and `fda_event_product_history_bronze` tables are **retained** in the schema. They are cheap to maintain, the endpoints may eventually be populated by FDA, and having the tables in place means a future change to populate them does not require a schema migration on top of a code change. However, they are **not the primary lineage mechanism**.
- The silver `recall_event_history` view uses `LAG()` window functions over FDA's main bronze snapshot table, exactly as it does for the other four sources.
- The unified `recall_event_history` view is still the single surface that downstream consumers query. The source-asymmetry it was designed to hide no longer exists, which simplifies the view's implementation.

**What does not change:**

- The bronze snapshot store model (rows keyed by `(source_recall_id, extraction_timestamp)`, content-hash conditional insert, tiered retention) is unchanged.
- The content-hashing implementation in `src/bronze/hashing.py` is unchanged.
- The `LAG()`-based history derivation for CPSC, USDA, NHTSA, USCG is unchanged.

## Consequences

- Silver `recall_event_history` view becomes simpler: one uniform synthesis path (`LAG()` over snapshots) rather than a `UNION` of a native-history path and a snapshot-derived path.
- FDA history coverage is bounded by the bronze snapshot retention policy (90 days daily, 1 year monthly) rather than the full audit log the native endpoints would have provided — acceptable given the endpoints are empty.
- If FDA ever populates the history endpoints, the bronze tables are already in place and the view can be extended without a schema migration. File a new ADR at that point to re-introduce the native-history path.
- No additional implementation complexity: the FDA extractor follows the same snapshot pattern as CPSC's Phase 3 extractor.
