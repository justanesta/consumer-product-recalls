# 0023 — FDA requires a deep-rescan workflow; archive migration invalidates the no-rescan assumption

- **Status:** Accepted
- **Date:** 2026-04-26
- **Supersedes:** [ADR 0010](0010-ingestion-cadence-and-github-actions-cron.md) (partially — the GitHub Actions orchestration choice, per-source cadences for CPSC/USDA/NHTSA/USCG, and the deep-rescan mechanism are unchanged; only FDA's "no rescan needed" exemption is revised)

## Context

ADR 0010 granted FDA an explicit exemption from the deep-rescan workflow required for CPSC and USDA:

> **FDA — None needed.** `eventlmddt` explicitly advances on edits per agency docs.

The rationale was that FDA's own documentation describes `eventlmddt` (now correctly `EVENTLMD` per ADR 0007 revision) as advancing on every edit, making a rescan redundant: the daily incremental query on `eventlmd >= yesterday` would catch all changes automatically.

Phase 5a FDA Bruno exploration (finding M in `documentation/fda/api_observations.md`) revealed an active archive migration pattern that invalidates this assumption.

**What was observed:**

A 90-day window query against the iRES enforcement report list endpoint returned recall events dated from 2002 through 2019 intermixed with 2025–2026 records. These are not new recalls — they are old records whose `EVENTLMD` was bumped wholesale as part of an ongoing FDA data migration effort that is re-touching legacy records. The daily incremental extractor, which queries `eventlmd >= yesterday`, surfaces these migrated records correctly on the day they are re-touched. But if the migration script processes a batch of 2,000 records in a single night, those records appear in tomorrow's incremental window and are ingested; records processed on a day the extractor did not run (e.g., a GH Actions flake, a holiday weekend) would be silently missed.

This is functionally identical to the silent-edit pattern that drove deep-rescan workflows for CPSC and USDA in ADR 0010. Content-hash dedup (ADR 0007) means the rescan cost scales with the number of genuinely-changed records, not the rescan window size.

**Corrected column name:** ADR 0010 refers to `eventlmddt`. The live API column is `EVENTLMD` (no `dt` suffix), per ADR 0007's 2026-04-26 revision and finding H in `documentation/fda/api_observations.md`. All references in this ADR use the correct name.

## Decision

FDA receives a weekly deep-rescan workflow matching the CPSC and USDA posture in ADR 0010.

**Updated deep-rescan table (replaces ADR 0010's table for the FDA row):**

| Source | Primary (daily) | Deep rescan | Rationale |
|---|---|---|---|
| CPSC | `LastPublishDate >= yesterday` | Weekly full rescan of last 90 days | Catches silent edits within 7 days |
| **FDA** | **`eventlmd >= yesterday`** | **Weekly full rescan of last 90 days** | **Archive migration re-touches old records; daily incremental may miss a batch on flake days** |
| USDA | `field_last_modified_date >= yesterday` | Weekly full rescan of last 90 days | Guards against documented-vs-actual gap |
| NHTSA | Weekly full flat file | N/A | Content hashing handles all dedup |
| USCG | Weekly full scrape | N/A | Same |

**Implementation:**

- Workflow file: `.github/workflows/deep-rescan-fda.yml`, scheduled Sunday 05:00 UTC (offset from CPSC/USDA Sunday 04:00 UTC to avoid runner contention).
- The rescan calls a **separate method or extractor class** from `FdaExtractor.extract()` — the same split established for CPSC in Phase 3. The historical path must handle arbitrarily large result sets; `FdaExtractor.extract()` includes a response-count guard (`_MAX_INCREMENTAL_RECORDS`) that would fire immediately on a full 90-day window. The rescan path has no count guard.
- `workflow_dispatch` trigger is added alongside the cron so the rescan can be triggered manually after a GH Actions outage or a known large migration batch.
- Content-hash dedup (ADR 0007) absorbs the volume: unchanged records are conditional-insert no-ops.

**What does not change from ADR 0010:**

- GitHub Actions as the orchestrator.
- FDA daily incremental cadence (`eventlmd >= yesterday`).
- CPSC, USDA, NHTSA, USCG cadences and rescan postures.
- The empirical-verification escape hatch: if post-Phase-5a monitoring shows FDA's archive migration has concluded and `EVENTLMD` advances only on genuine content edits, this ADR can be re-opened to relax or remove the rescan.

## Consequences

- FDA's extraction posture now matches CPSC and USDA: daily incremental + weekly rescan. One fewer special case in the codebase.
- A second FDA workflow file is required (`.github/workflows/deep-rescan-fda.yml`), adding one GitHub Actions workflow to Phase 5a's deliverables.
- The rescan is cheap in practice: FDA's full dataset is ~9,800 records; content-hash dedup means only genuinely-migrated records produce inserts. Estimated runtime is well within GitHub Actions' per-job limits.
- If FDA's archive migration concludes, the rescan workflow can be disabled without a code change (disable the cron trigger; keep `workflow_dispatch` for manual backfills).
