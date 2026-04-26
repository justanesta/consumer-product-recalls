# Architecture Decision Records

Every non-trivial design decision in this project is captured as an ADR using Michael Nygard's template: **Status / Date / Context / Decision / Consequences**.

ADRs are **immutable once accepted** — if a decision changes, a new ADR is written that supersedes the old one. The old ADR remains in place with its Status updated. This preserves the historical record of how thinking evolved, which is part of the portfolio narrative.

A new ADR is written when someone reading the code six months later would ask "why did they do it this way and not the obvious alternative?" Trivial choices (variable naming, lint config) don't get ADRs; substantive tradeoffs do.

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

### Pipeline, extraction, and transformation

- [0010 — Ingestion cadence and orchestration via GitHub Actions cron](0010-ingestion-cadence-and-github-actions-cron.md) *(partially superseded by 0023)*
- [0011 — Transformation framework: dbt-core](0011-transformation-framework-dbt-core.md)
- [0023 — FDA deep rescan required; archive migration detected](0023-fda-deep-rescan-required-archive-migration-detected.md) — supersedes ADR 0010's FDA no-rescan exemption
- [0012 — Extractor pattern: custom ABC + per-source subclasses](0012-extractor-pattern-custom-abc-and-per-source-subclasses.md) — adopts patterns from NYC DCP's `dcpy` without the dependency
- [0013 — Error handling: retries, idempotency, and quarantine](0013-error-handling-retries-idempotency-and-quarantine.md)
- [0014 — Schema evolution policy](0014-schema-evolution-policy.md) — `extra='forbid'` + `strict=True` + required-by-default
- [0020 — Pipeline state tracking](0020-pipeline-state-tracking.md) — Neon watermark + run-metadata tables; Prefect Cloud as future overlay; Dagster deferred

### Tooling and development

- [0015 — Testing strategy](0015-testing-strategy.md) — unit / integration (VCR) / e2e pyramid + dbt tests
- [0016 — Secrets management](0016-secrets-management.md) — GitHub Actions secrets + `.env` + `pydantic-settings` + optional direnv
- [0017 — Package management via uv](0017-package-management-via-uv.md)
- [0018 — CI posture](0018-ci-posture.md) — workflow triggers, dbt orchestration, pre-commit, branch protection
- [0019 — License: MIT](0019-license-mit.md)
- [0021 — Structured logging with structlog](0021-structured-logging.md) — correlation-ID propagation via contextvars, stdlib bridge, JSON to stdout

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

---

## Writing new ADRs

When adding a new ADR:

1. Pick the next sequential number (0022, 0023, ...).
2. File name: `NNNN-kebab-case-title.md`.
3. Use the standard template (see any existing ADR as a model).
4. Add an entry under the appropriate topic above **and** in the numeric index.
5. If the new ADR supersedes a previous one, update the superseded ADR's Status line to `Superseded by ADR NNNN` and add a link.
