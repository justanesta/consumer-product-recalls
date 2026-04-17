# 0004 — Four-layer medallion pipeline (object-storage landing → Postgres bronze / silver / gold)

- **Status:** Accepted
- **Date:** 2026-04-16

## Context

The pipeline must move heterogeneous data from five sources (CPSC, FDA, USDA, NHTSA, USCG — see ADR 0001) into a unified, queryable shape (see ADR 0002) under a near-zero cost ceiling. Several constraints shape the architecture:

- Harmonization rules will be iterated many times; re-extracting raw data on every iteration is wasteful and, for FDA, risks rate-limit exhaustion. For USCG, re-scraping is brittle.
- Data governance, lineage, and audit are stated project goals.
- Storage costs matter: a "fully typed bronze table per source" pattern in Postgres is appropriate at enterprise scale but burns DB free-tier capacity on rarely-queried raw payloads.
- Object storage is cheap, durable, and well-suited to raw-payload archival; relational storage is appropriate for queryable structured data.

Three alternatives were rejected:

- **Single-pass ETL (extract → transform → load directly to silver):** ties harmonization to extraction, forces re-extraction on every transformation iteration, and discards audit trail.
- **Fully typed bronze for raw payloads in Postgres:** consumes free-tier DB capacity on data rarely queried directly. Enterprise pattern, wrong scale.
- **Pure object-storage solution (everything in object storage, queried via DuckDB):** rejected because the project also needs interactive transactional reads for the API and the consumer-facing dashboard.

## Decision

A four-layer pipeline:

| Layer | Storage | Contents | Purpose |
|---|---|---|---|
| **0 — Landing** | Object storage | Raw API responses, scraped HTML, and flat-file snapshots, written as JSON / JSONL / HTML / TSV and partitioned by `source/extraction_date/` | Cheap, durable, replayable. The audit trail. |
| **1 — Bronze** | Postgres | One typed table per source (e.g. `cpsc_recalls_bronze`, `fda_food_enforcement_bronze`, `nhtsa_recalls_bronze`), mirroring API record shape with light type coercion via Pydantic | Schema-on-load. Queryable raw, source-specific analysis. |
| **2 — Silver** | Postgres | Unified `recall_event`, `recall_product`, `firm`, `recall_event_firm`, plus extension tables (per ADR 0002) | The harmonized model. |
| **3 — Gold** | Postgres views and materialized views | Pre-aggregated dashboards, denormalized search indexes | Fast reads for the API and frontend. |

The specific object-store and Postgres providers are deferred to ADR 0005 pending Phase 2 volume validation.

## Consequences

- Re-running harmonization without re-extracting is possible: rebuild silver from bronze.
- Every value in silver can be traced to a bronze row to a raw file in landing storage — full end-to-end lineage.
- Bronze Pydantic models become the per-source schema-on-load contract. Pydantic is already a project library standard (see `CLAUDE.md`).
- Silver becomes the SQL-transformation domain. The choice between dbt-core, plain SQL via SQLAlchemy, or another transformation framework is deferred to a future ADR in Phase 3.
- More tables and transformations than a single-pass pipeline. The overhead is real but proportional at five sources, and each layer demonstrates a distinct DE skill (extraction, schema validation, harmonization, serving optimization) — aligned with the portfolio goal.
