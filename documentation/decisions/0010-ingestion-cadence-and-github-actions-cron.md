# 0010 — Ingestion cadence and orchestration via GitHub Actions cron

- **Status:** Accepted
- **Date:** 2026-04-16

## Context

Each in-scope source has its own publication rhythm and update profile:

| Source | Publication rhythm |
|---|---|
| CPSC | New recalls posted multiple times per week |
| FDA | Weekly enforcement reports + daily product/event updates |
| USDA | Weekly publication, occasionally daily during outbreaks |
| NHTSA | Flat file refreshed daily, but slow-changing in practice |
| USCG | Low volume, ~monthly cadence |

Several orchestration patterns were considered:

- **Linux cron + bare scripts on a VM.** Cheapest in dollar terms but requires VM hosting (free-tier Oracle Cloud or similar) and self-managed observability. Adds infrastructure burden that doesn't pay portfolio dividends.
- **GitHub Actions scheduled workflows.** Free for public repos with no minute cap; private repos get 2000 minutes/month. Git-native logging and re-run UI. Secrets handled by GitHub. No external infrastructure.
- **Prefect Cloud free tier.** Managed orchestration with DAG visualization, built-in retries, observability. Adds an external dependency and a learning surface that doesn't earn its keep at v1's complexity.
- **Airflow / Dagster.** Heavyweight; require their own hosting; overkill for daily cron at this scale.

At v1's scale (5 sources, ~15K records/year ingested, no cross-source DAG dependencies until silver/gold) managed orchestration adds complexity without commensurate value.

## Decision

- **Orchestrator:** GitHub Actions scheduled workflows (cron syntax in `.github/workflows/`), one workflow per source extractor.
- **Repository visibility:** public. Secondary benefit beyond portfolio reasons — public repos have unlimited Actions minutes, while private repos are capped at 2000/month (which v1 would approach).
- **Per-source cadence:**

| Source | Cadence | Strategy |
|---|---|---|
| CPSC | daily | Incremental query on `LastPublishDate >= yesterday` |
| FDA | daily | Incremental query on `eventlmd >= yesterday` |
| USDA | daily | Filter on `field_last_modified_date >= yesterday`; cheap when nothing changed |
| NHTSA | weekly | Full flat file download per ADR 0008, content-hash dedup per ADR 0007 |
| USCG | weekly | HTML scrape with rate limiting and robots.txt respect |

- **Workflow isolation:** each source has its own workflow file. A USCG scraping outage does not block CPSC ingestion.
- **Runtime environment:** `ubuntu-latest`, dependencies installed via `uv` (or pip), execute the per-source extractor + bronze loader.
- **Secrets:** Neon connection string, R2 credentials, FDA API key live in GitHub Actions repository secrets.
- **Silver and gold transformation orchestration is out of scope for this ADR** and will be addressed in Phase 3 — it depends on the choice of transformation framework (dbt-core vs. plain SQL vs. other), which is itself a future ADR.

### Deep rescans — catching silent edits on weak-timestamp sources

Incremental cadence above assumes each source's last-modified timestamp advances when an existing recall is edited in place. This is explicitly documented only for FDA (`eventlmddt` and `productlmd`, with field-level history endpoints as additional evidence). For CPSC (`LastPublishDate`) and USDA (`field_last_modified_date`), agency documentation is silent or ambiguous on whether those timestamps advance on edits. A silent-edit failure mode — fields change but the timestamp does not — would cause the incremental extractor to miss the update entirely.

To guard against this, CPSC and USDA get a **secondary deep-rescan workflow** in addition to their daily incremental cron:

| Source | Primary (daily) | Deep rescan | Rationale |
|---|---|---|---|
| CPSC | `LastPublishDate >= yesterday` | Weekly full rescan of last 90 days | Catches silent edits within 7 days |
| FDA | `eventlmd >= yesterday` | None needed | `eventlmddt` explicitly advances on edits per agency docs |
| USDA | `field_last_modified_date >= yesterday` | Weekly full rescan of last 90 days | Guards against documented-vs-actual gap until Phase 5b empirical verification; may relax or remove if verified reliable |
| NHTSA | Weekly full flat file | N/A — the weekly operation is already a full rescan | Content hashing per ADR 0007 handles all dedup |
| USCG | Weekly full scrape | N/A — the weekly operation is already a full rescan | Same |

Deep rescans exploit the content hashing defined in ADR 0007: the rescan pulls records ignoring the watermark, and every row whose canonical content is unchanged since the prior bronze insert becomes a no-op conditional insert. Cost scales with the number of actually-edited records, not with the rescan window size.

**Rescan workflow files:** one per affected source, `.github/workflows/deep-rescan-<source>.yml`, scheduled for weekends (e.g., Sunday 04:00 UTC) to avoid colliding with daily extraction workflows or the Monday morning transform window.

**Empirical-verification escape hatch:** if Phase 3 (CPSC) or Phase 5b (USDA) empirical verification confirms that the relevant timestamp reliably advances on edits — by observing a known-edited recall across extraction runs, or by modifying a test record where the agency permits — this ADR is re-opened to relax or remove the deep rescan for the verified source. Until then, the rescans stand as a defense-in-depth correctness measure.

## Consequences

- Zero infrastructure cost at v1 scale. Public repo means unlimited Actions minutes.
- All pipeline runs logged in the GitHub UI with runtime, status, and per-step output. No separate observability platform needed initially.
- Re-running a failed extraction is a one-click action in the GitHub UI; manual triggering supported via `workflow_dispatch`.
- Per-source workflow isolation means failures are localized — no global pipeline failure mode.
- USDA daily polling is cheap when nothing has changed (single API call returns empty filtered result), so daily cadence costs almost nothing relative to weekly.
- Re-evaluation triggers for moving off GitHub Actions: (a) any individual workflow runtime exceeds 60 minutes consistently, (b) cross-source DAG dependencies need explicit modeling, (c) sub-hourly cadence is required (cron in GH Actions is not guaranteed to fire on time at high frequency).
- Public repo is a hard requirement for the unlimited-minutes math; if it ever needs to go private, the orchestration choice gets revisited (likely toward self-hosted runner on Oracle Cloud Always Free).
