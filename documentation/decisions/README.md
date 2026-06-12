# Architecture Decision Records

Every non-trivial design decision in this project is captured as an ADR using Michael Nygard's template: **Status / Date / Context / Decision / Consequences**.

ADRs are **immutable once accepted** — if a decision changes, a new ADR is written that supersedes the old one. The old ADR remains in place with its Status updated. This preserves the historical record of how thinking evolved, which is part of the portfolio narrative.

A new ADR is written when someone reading the code six months later would ask "why did they do it this way and not the obvious alternative?" Trivial choices (variable naming, lint config) don't get ADRs; substantive tradeoffs do.

> For how docs *other than ADRs* are organized (plans, findings, TODO, branch sequencing), see [`documentation/documentation_model.md`](../documentation_model.md). This README is the authority on ADRs and on the next-free ADR number.

---

## By topic

### Scope and data model

- [0001 — Sources in scope](0001-sources-in-scope.md) — CPSC, FDA, USDA, NHTSA, USCG in scope; EPA deferred; FAA cut
- [0002 — Unit of analysis: header / line / firm](0002-unit-of-analysis-header-line-firm.md) — `recall_event` + `recall_product` + `firm` tables
- [0003 — `event_type` discriminator](0003-event-type-discriminator.md) — cheap forward-compat for non-recall regulatory actions
- [0006 — USDA bilingual record deduplication](0006-usda-bilingual-record-deduplication.md) — collapse EN/ES records with Spanish summary in `summary_alt_lang`
- [0009 — NHTSA: line-level granularity in silver](0009-nhtsa-line-level-granularity-in-silver.md) — preserve per-(make,model,year,component) rows

### Architecture and storage

- [0004 — Four-layer medallion pipeline](0004-four-layer-medallion-pipeline.md) — landing → bronze → silver → gold
- [0005 — Storage tier](0005-storage-tier-neon-and-r2.md) — Neon Postgres free + Cloudflare R2 free
- [0007 — Lineage via bronze snapshots + content hashing](0007-lineage-via-bronze-snapshots-and-content-hashing.md) — unified history derivable across all sources *(partially superseded by 0022)*
- [0008 — NHTSA: flat file primary, JSON API for live vehicle lookup](0008-nhtsa-flat-file-primary-api-for-vehicle-lookup.md)
- [0022 — FDA history endpoints empty; snapshot synthesis for all sources](0022-fda-history-endpoints-empty-snapshot-synthesis-for-all-sources.md) — supersedes ADR 0007's FDA-specific history path
- [0026 — Lifecycle tracking via per-run snapshot-presence manifest](0026-lifecycle-tracking-snapshot-presence-manifest.md) — closes bronze's retraction gap; USDA+NHTSA (campno key; deep-rescan-only write for NHTSA), separate `extraction_run_identities` table
- [0027 — Bronze keeps storage-forced transforms only; value-level normalization moves to silver](0027-bronze-storage-forced-transforms-only.md) — bronze hashes change iff the source changed
- [0030 — NHTSA bronze identity: composite tuple + within-batch dedup](0030-nhtsa-bronze-identity-composite-tuple-and-within-batch-dedup.md) — 11-tuple identity; `RECORD_ID` excluded from content hash
- [0031 — Silver-row fragmentation strategy](0031-silver-row-fragmentation-strategy.md) — per-source surrogate keys, drift detection, reconciliation tiers
- [0032 — USDA establishment `latest_mpi_active_date` hash exclusion (upstream heartbeat fields)](0032-usda-establishment-heartbeat-field-hash-exclusion.md) — third category of `hash_exclude_fields` use; suppresses weekly ~7k phantom re-versions
- [0033 — Silver row versioning via SCD on stable anchor](0033-silver-row-versioning-via-scd-on-stable-anchor.md) — NHTSA `recall_product` 6-tuple stable anchor + Type-1 latest-wins + Type-2 dbt snapshot; framework generalizes cross-source
- [0034 — NHTSA silver v1.5 — Layer 3 migration cutover](0034-nhtsa-silver-v15-migration.md) *(Accepted — cutover done 6c.7; design in `project_scope/archive/silver_v15_migration_plan.md`)*
- [0035 — Cross-source SCD-2 for silver dimensions](0035-cross-source-scd2-silver-dimensions.md) — cross-source policy = Policy C (latest-wins current view + first-class snapshot history) via dbt snapshot `strategy='check'` on a stable anchor; only the USCG `firm_uscg_attributes` instance (the measured Type-2 NEED, 221/718 OOB-recycled / 365 prior) is built in 6b.5, FDA/CPSC/USDA deferred *(Accepted — ratified at the #59 merge)*
- [0036 — Cross-source canonical silver column naming](0036-cross-source-canonical-silver-naming.md) *(Accepted — ratified at the #58 merge)*
- [0038 — Gold-layer modeling and indexing strategy](0038-gold-layer-modeling-and-indexing-strategy.md) — `mart_`/`fct_` shapes (Kimball star deferred), serving=table+indexed / aggregate=view, Postgres FTS search, dbt `config(indexes=)` mechanism, Phase-7 profiling follow-up *(Accepted — ratified at the #62 merge)*
- [0041 — NHTSA bronze dedup-lookup: set-based staging-table JOIN](0041-nhtsa-bronze-dedup-lookup-staging-join.md) — replaces the ~45-chunk `IN`-loop with one `pg_temp` staging JOIN for large batches (fetch-only restructure; dedup decision byte-identical), ~17 min → ~1 min; clarifies ADR 0030

### Pipeline, extraction, and transformation

- [0010 — Ingestion cadence and orchestration via GitHub Actions cron](0010-ingestion-cadence-and-github-actions-cron.md) *(partially superseded by 0023; amended 2026-05-01 for CPSC + USDA findings, 2026-06-02 for Phase-7 deep-rescan cadence + GHA hardening)*
- [0011 — Transformation framework: dbt-core](0011-transformation-framework-dbt-core.md)
- [0023 — FDA deep rescan required; archive migration detected](0023-fda-deep-rescan-required-archive-migration-detected.md) — supersedes ADR 0010's FDA no-rescan exemption
- [0012 — Extractor pattern: custom ABC + per-source subclasses](0012-extractor-pattern-custom-abc-and-per-source-subclasses.md) — adopts patterns from NYC DCP's `dcpy` without the dependency *(amended 2026-05-01: multi-response-shape pattern note)*
- [0013 — Error handling: retries, idempotency, and quarantine](0013-error-handling-retries-idempotency-and-quarantine.md) *(amended 2026-05-01: FDA HTML-redirect throttling)*
- [0014 — Schema evolution policy](0014-schema-evolution-policy.md) — `extra='forbid'` + `strict=True` + required-by-default
- [0020 — Pipeline state tracking](0020-pipeline-state-tracking.md) — Neon watermark + run-metadata tables; Prefect Cloud as future overlay; Dagster deferred
- [0028 — Backfill and historical re-extraction semantics](0028-backfill-historical-reextraction-semantics.md) — three named mechanisms (deep rescan, R2 replay, manifest backfill) with idempotency and silver-layer rules
- [0037 — Firm resolution runs as a Python stage, not in-warehouse SQL/pg_trgm](0037-firm-resolution-python-stage-not-sql-fuzzy.md) — scoped exception to ADR 0011; RapidFuzz clustering + FEI forced-merges in `src/enrichment/` → `firm_crosswalk` dbt source; rejects pg_trgm/fuzzystrmatch with reasoning

### Serving and presentation (built as separate repos post-go-live)

- [0024 — Serving-layer API design](0024-serving-layer-api-design.md) — FastAPI + SQLAlchemy-Core-async over the gold marts; four endpoints; keyset pagination; no star (`fct_*` is the dashboard layer)
- [0025 — API deployment target](0025-api-deployment-target.md) — Fly.io (Render fallback; Cloudflare Workers rejected — Pyodide/WASM can't run asyncpg)
- [0039 — Frontend framework](0039-frontend-framework.md) — Astro (islands) on a free static host

### Tooling and development

- [0015 — Testing strategy](0015-testing-strategy.md) — unit / integration (VCR) / e2e pyramid + dbt tests
- [0016 — Secrets management](0016-secrets-management.md) — GitHub Actions secrets + `.env` + `pydantic-settings` + optional direnv *(amended 2026-05-01: bot-manager fingerprinting)*
- [0017 — Package management via uv](0017-package-management-via-uv.md)
- [0018 — CI posture](0018-ci-posture.md) — workflow triggers, dbt orchestration, pre-commit, branch protection
- [0019 — License: MIT](0019-license-mit.md)
- [0021 — Structured logging with structlog](0021-structured-logging.md) — correlation-ID propagation via contextvars, stdlib bridge, JSON to stdout
- [0029 — Application observability and alerting: v1 stance and upgrade triggers](0029-application-observability-and-alerting.md) — formalizes the v1 deferral with named upgrade triggers
- [0040 — Dedicated data-quality framework: Soda Core](0040-data-quality-framework-soda-core.md) — bronze freshness/anomaly/schema checks complementing dbt's transform-layer tests (scoped to not duplicate dbt)

---

## By ADR number

1. [Sources in scope](0001-sources-in-scope.md)
2. [Unit of analysis: header / line / firm](0002-unit-of-analysis-header-line-firm.md)
3. [`event_type` discriminator](0003-event-type-discriminator.md)
4. [Four-layer medallion pipeline](0004-four-layer-medallion-pipeline.md)
5. [Storage tier: Neon + R2](0005-storage-tier-neon-and-r2.md)
6. [USDA bilingual record deduplication](0006-usda-bilingual-record-deduplication.md)
7. [Lineage via bronze snapshots + content hashing](0007-lineage-via-bronze-snapshots-and-content-hashing.md)
8. [NHTSA: flat file primary, JSON API for live lookup](0008-nhtsa-flat-file-primary-api-for-vehicle-lookup.md)
9. [NHTSA: line-level granularity in silver](0009-nhtsa-line-level-granularity-in-silver.md)
10. [Ingestion cadence and GitHub Actions cron](0010-ingestion-cadence-and-github-actions-cron.md)
11. [Transformation framework: dbt-core](0011-transformation-framework-dbt-core.md)
12. [Extractor pattern: custom ABC + per-source subclasses](0012-extractor-pattern-custom-abc-and-per-source-subclasses.md)
13. [Error handling: retries, idempotency, and quarantine](0013-error-handling-retries-idempotency-and-quarantine.md)
14. [Schema evolution policy](0014-schema-evolution-policy.md)
15. [Testing strategy](0015-testing-strategy.md)
16. [Secrets management](0016-secrets-management.md)
17. [Package management via uv](0017-package-management-via-uv.md)
18. [CI posture](0018-ci-posture.md)
19. [License: MIT](0019-license-mit.md)
20. [Pipeline state tracking](0020-pipeline-state-tracking.md)
21. [Structured logging with structlog](0021-structured-logging.md)
22. [FDA history endpoints empty; snapshot synthesis for all sources](0022-fda-history-endpoints-empty-snapshot-synthesis-for-all-sources.md)
23. [FDA deep rescan required; archive migration detected](0023-fda-deep-rescan-required-archive-migration-detected.md)
24. [Serving-layer API design](0024-serving-layer-api-design.md)
25. [API deployment target](0025-api-deployment-target.md)
26. [Lifecycle tracking via per-run snapshot-presence manifest](0026-lifecycle-tracking-snapshot-presence-manifest.md)
27. [Bronze keeps storage-forced transforms only; value-level normalization moves to silver](0027-bronze-storage-forced-transforms-only.md)
28. [Backfill and historical re-extraction semantics](0028-backfill-historical-reextraction-semantics.md)
29. [Application observability and alerting: v1 stance and upgrade triggers](0029-application-observability-and-alerting.md)
30. [NHTSA bronze identity: composite tuple + within-batch dedup](0030-nhtsa-bronze-identity-composite-tuple-and-within-batch-dedup.md)
31. [Silver-row fragmentation strategy: per-source surrogate keys, drift detection, reconciliation tiers](0031-silver-row-fragmentation-strategy.md)
32. [USDA establishment `latest_mpi_active_date` hash exclusion (upstream heartbeat fields)](0032-usda-establishment-heartbeat-field-hash-exclusion.md)
33. [Silver row versioning via SCD on stable anchor](0033-silver-row-versioning-via-scd-on-stable-anchor.md)
34. [NHTSA silver v1.5 — Layer 3 migration cutover](0034-nhtsa-silver-v15-migration.md)
35. [Cross-source SCD-2 for silver dimensions](0035-cross-source-scd2-silver-dimensions.md)
36. [Cross-source canonical silver column naming](0036-cross-source-canonical-silver-naming.md)
37. [Firm resolution runs as a Python stage, not in-warehouse SQL/pg_trgm](0037-firm-resolution-python-stage-not-sql-fuzzy.md)
38. [Gold-layer modeling and indexing strategy](0038-gold-layer-modeling-and-indexing-strategy.md)
39. [Frontend framework](0039-frontend-framework.md)
40. [Dedicated data-quality framework: Soda Core](0040-data-quality-framework-soda-core.md)
41. [NHTSA bronze dedup-lookup: set-based staging-table JOIN](0041-nhtsa-bronze-dedup-lookup-staging-join.md)

---

## Writing new ADRs

When adding a new ADR:

1. Pick the next sequential number. Current state: **0001–0041 all filed and Accepted.** 0024 (serving-layer API design), 0025 (API deployment target), 0039 (frontend framework), and 0040 (data-quality framework — Soda Core) were filed 2026-06-09 on the phase-7 branch; 0034/0035/0036/0038 were ratified Accepted at their merges (#60 / #59 / #58 / #62); 0041 (NHTSA bronze dedup-lookup staging JOIN) was filed 2026-06-12 on `feature/pre-go-live-validation`. The serving/presentation ADRs (0024/0025/0039) govern the **separate** API + website repos built after go-live (there is no in-repo Phase 8/9). **The next free number is 0042.** (This line is the single source of truth for the next number — plan docs must not reserve numbers independently.)
2. File name: `NNNN-kebab-case-title.md`.
3. Use the standard template (see any existing ADR as a model).
4. Add an entry under the appropriate topic above **and** in the numeric index.
5. If the new ADR supersedes a previous one, update the superseded ADR's Status line to `Superseded by ADR NNNN` and add a link.
6. If the new ADR amends rather than supersedes (e.g., adds an "Implementation notes" section after empirical findings), update the original ADR's Status line to note the amendment date and reference the section, rather than filing a separate ADR. Use supersession when a core decision changes; use amendment when the original decision stands but needs refinement.
