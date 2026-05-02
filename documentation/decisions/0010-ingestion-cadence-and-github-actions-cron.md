# 0010 — Ingestion cadence and orchestration via GitHub Actions cron

- **Status:** Accepted; partially superseded by [ADR 0023](0023-fda-deep-rescan-required-archive-migration-detected.md); amended 2026-05-01 (CPSC + USDA empirical findings — see "Revision note" at end)
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
| CPSC | daily | Incremental query on `LastPublishDate >= yesterday` (publication-time only — does NOT advance on edits; deep rescan is the edit-detection mechanism — see Revision note) |
| FDA | daily | Incremental query on `eventlmd >= yesterday` |
| USDA | daily | **Full-dump on every run** (`field_last_modified_date` is not a server-side filter — see Revision note); content-hash dedup makes re-runs cheap |
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
| CPSC | `LastPublishDate >= yesterday` | Weekly full rescan of last 90 days — **mandatory** | `LastPublishDate` is publication-time only; edit detection depends on the rescan. See Revision note. |
| FDA | `eventlmd >= yesterday` | ~~None needed~~ **Weekly rescan added — see ADR 0023** | Archive migration re-touches old records; daily incremental may miss a batch on flake days |
| USDA | Full-dump on every run | N/A — the daily operation is already a full snapshot | Server-side date filter does not exist (Phase 5b Finding D); content-hash dedup handles idempotency |
| NHTSA | Weekly full flat file | N/A — the weekly operation is already a full rescan | Content hashing per ADR 0007 handles all dedup |
| USCG | Weekly full scrape | N/A — the weekly operation is already a full rescan | Same |

Deep rescans exploit the content hashing defined in ADR 0007: the rescan pulls records ignoring the watermark, and every row whose canonical content is unchanged since the prior bronze insert becomes a no-op conditional insert. Cost scales with the number of actually-edited records, not with the rescan window size.

**Rescan workflow files:** one per affected source, `.github/workflows/deep-rescan-<source>.yml`, scheduled for weekends (e.g., Sunday 04:00 UTC) to avoid colliding with daily extraction workflows or the Monday morning transform window.

**No escape hatch — both empirical verifications closed.** The original ADR allowed for relaxing or removing rescans if Phase 3 (CPSC) or Phase 5b (USDA) verification proved the timestamps reliable. Both verifications closed in the opposite direction (see Revision note). The deep rescan is now the **primary edit-detection mechanism for CPSC**, not a defense-in-depth net. USDA's deep-rescan section above no longer applies — every USDA run is already a full snapshot.

## Consequences

- Zero infrastructure cost at v1 scale. Public repo means unlimited Actions minutes.
- All pipeline runs logged in the GitHub UI with runtime, status, and per-step output. No separate observability platform needed initially.
- Re-running a failed extraction is a one-click action in the GitHub UI; manual triggering supported via `workflow_dispatch`.
- Per-source workflow isolation means failures are localized — no global pipeline failure mode.
- USDA daily polling is cheap when nothing has changed (single API call returns empty filtered result), so daily cadence costs almost nothing relative to weekly.
- Re-evaluation triggers for moving off GitHub Actions: (a) any individual workflow runtime exceeds 60 minutes consistently, (b) cross-source DAG dependencies need explicit modeling, (c) sub-hourly cadence is required (cron in GH Actions is not guaranteed to fire on time at high frequency).
- Public repo is a hard requirement for the unlimited-minutes math; if it ever needs to go private, the orchestration choice gets revisited (likely toward self-hosted runner on Oracle Cloud Always Free).

---

## Revision note — 2026-05-01 (CPSC + USDA empirical findings)

Two pieces of empirical evidence collected during Phases 3 and 5b invalidate the original "deep rescan is a relaxable defense-in-depth net" framing. Both are closed; this revision updates the per-source cadence table and the deep-rescan section to match observed reality.

### CPSC `LastPublishDate` does NOT advance on edits

Phase 3 first-extraction analysis of 1,193 bronze records over 365 days (`documentation/cpsc/last_publish_date_semantics.md`) shows a **bimodal gap distribution** with zero records between 8 days and 5 years. Edits to already-published recalls do not bump `LastPublishDate`. The only mid-life advances observed are the 709 archive-migration records (25-year gaps), which are an upstream re-processing artifact, not edits in the editorial sense.

Consequence: the daily `LastPublishDate >= yesterday` query is a **publication-time cursor only**. Detecting genuine edits to already-published CPSC recalls requires the weekly deep rescan. The rescan is no longer optional — it is the primary edit-detection mechanism for CPSC.

There is also a 20-year (2005–2024) historical gap in CPSC bronze that the incremental strategy will not reach until the upstream archive migration completes (estimated years away at ~2–3 records/day). A one-time deep rescan with `LastPublishDateStart=2005-01-01` is required before Phase 7 cron go-live to populate this. See ADR 0028 (backfill semantics) and `documentation/cpsc/last_publish_date_semantics.md` Section 3.

### USDA `field_last_modified_date` is not a server-side filter

Phase 5b first-extraction probing (`documentation/usda/recall_api_observations.md` Finding D, `documentation/usda/first_extraction_findings.md`) confirms that both naming variants — `field_last_modified_date` and `field_last_modified_date_value` — are silently ignored by the FSIS API and return the full 2,001-record dataset. There is no working incremental cursor on the recall API.

Consequence: USDA is a **full-dump source** on every run, like NHTSA and USCG. Daily cadence is still cheap (~1.6 MB compressed payload, ETag conditional-GET considered but disabled in production due to unreliable Akamai CDN behavior — see Finding N). Content-hash dedup (ADR 0007) handles idempotency. The "deep rescan" concept does not apply — every run is already a full snapshot.

The original deep-rescan-usda.yml workflow exists from Phase 5b but its operational role collapses to "the same thing as the daily incremental run" — kept for symmetry and for one-off operator triggering, but contributes no additional coverage.

### What this changes downstream

- **`implementation_plan.md`** Phase 7 line 500 ("relaxable if empirical verification shows...") — wording corrected by the same realignment that produced this revision.
- **CPSC historical backfill** is added as a pre-Phase-7 blocker in the implementation plan, formalized by ADR 0028.
- **USDA daily cadence vs. weekly cadence question** — daily is fine; the bandwidth difference between daily and weekly is small at 1.6 MB/run, and daily preserves a tighter audit trail of the (source_recall_id, langcode) presence set per ADR 0026.
