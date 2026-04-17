# 0013 — Error handling: retries, idempotency, and quarantine

- **Status:** Accepted
- **Date:** 2026-04-16

## Context

The pipeline ingests from five heterogeneous sources (ADR 0001) via the Extractor ABC (ADR 0012). Three distinct classes of failure need different routing:

- **Transient failures** — network timeouts, 5xx responses, rate limits. Likely to succeed on retry.
- **Schema violations** — Pydantic `ValidationError`, type mismatches. Retry is pointless; human intervention required.
- **Business invariant violations** — cross-record or semantic checks Pydantic can't model (e.g., a USDA Spanish record with no English counterpart within 24 hours). Record is structurally valid but wrong for our purposes.

Sources of non-idempotency to neutralize:

- Duplicate bronze inserts (handled by content hashing per ADR 0007 — conditional insert).
- Concurrent runs racing on the same source.
- Partial state when an extraction step fails mid-pipeline.

Three design options for failure handling were considered:

- **Fail the entire run on any error.** Rejected — one bad CPSC record shouldn't block 499 good ones.
- **Skip bad records silently with a counter.** Rejected — unacceptable for data quality.
- **Quarantine-by-tier + selective retry.** Accepted.

A dead-letter queue (Kafka-style) was considered and rejected as overkill for a batch pipeline at this scale.

## Decision

### Idempotency

- Bronze inserts are content-hash-conditional per ADR 0007. Re-running an extractor never duplicates rows.
- GitHub Actions workflows use `concurrency: group: extract-${source}, cancel-in-progress: false`. In-flight runs complete; new runs queue.
- Raw landing to R2 (ADR 0004) happens before any validation, so raw data is preserved regardless of what fails downstream.

### Retry ladder

Implemented via the `tenacity` library as decorators on ABC lifecycle methods. Policy: **only retry failures that could plausibly succeed on retry.**

| Failure class | Signal | Strategy |
|---|---|---|
| Transient network / 5xx / timeout / DNS | 500–504, connection error | Exponential backoff with jitter, max 5 attempts |
| Rate limited | 429 | Backoff per `Retry-After` header; counts as 1 attempt |
| Auth failure | 401, 403 | Fail fast, alert, stop workflow |
| Schema violation | Pydantic `ValidationError` | Fail fast, quarantine record, continue workflow |
| Business invariant violation | Application-defined check | Quarantine record, continue workflow |

Retry decorators applied only to:

- `extract()` — fetches from source; most failure-prone stage.
- `load_bronze()` — Postgres writes can fail on transient connection issues.
- `land_raw()` — separate retry profile tuned to R2's behavior.

Never retried:

- `validate()` — pure function; retry would fail identically.
- `check_invariants()` — same reason.

### Quarantine architecture (three tiers)

| Tier | Where | Triggers | Purpose |
|---|---|---|---|
| **T0 — Raw landing** | R2 (ADR 0004) | Every extraction, unconditionally | Raw preserved before validation. If anything downstream fails, raw is recoverable. |
| **T1 — `_rejected` bronze tables** | Postgres | Pydantic validation OR business invariant check fails | One per source (`cpsc_recalls_rejected`, `fda_enforcement_rejected`, etc.). Columns: `source_recall_id`, `raw_record JSONB`, `failure_reason TEXT`, `failure_stage TEXT`, `rejected_at TIMESTAMP`, `raw_landing_path TEXT`. |
| **T2 — Alert** | Structured log + workflow status | Any T1 insert | Structured warning log emitted. Workflow exits non-zero only if rejections exceed 5% of batch (tunable per source). |

dbt does not touch `_rejected` tables — they are inspection/debug surfaces, not transformation inputs.

### Extractor lifecycle refinement (extends ADR 0012)

ADR 0012 defined a 4-step lifecycle. This ADR refines validation into two sub-steps and makes the failure-routing explicit:

1. `extract()` — fetch from source (retried).
2. `land_raw()` — persist to R2 T0 (retried).
3. `validate()` — Pydantic structural check. Failure → T1 `_rejected` with `failure_stage='validate'`.
4. `check_invariants()` — cross-record / business logic. Failure → T1 `_rejected` with `failure_stage='invariants'`.
5. `load_bronze()` — content-hash + conditional insert (retried).

### Business invariant checks (v1 starter list)

Implemented in `check_invariants()` between Pydantic validation and bronze insert:

- **USDA only:** If a Spanish record has no English counterpart within 24 hours of ingestion, quarantine the Spanish record (per ADR 0006 edge case).
- **All sources:** If `published_at` is in the future or more than 70 years in the past, quarantine. Sanity guard.
- **All sources:** If `source_recall_id` is null or empty, quarantine. Can't dedupe without it.

## Consequences

- All classes of extraction failure have explicit, named routes.
- Rejected records are SQL-inspectable via per-source `_rejected` tables; debugging a reject is straightforward.
- Idempotency is table-stakes: every run is safely re-runnable whether triggered manually or by workflow retry.
- Content hashing from ADR 0007 does double duty — it also makes quarantine-then-reprocess idempotent after a schema fix.
- `tenacity` is a widely-used MIT-licensed Python library; swappable if it proves insufficient.
- The `check_invariants()` refinement adds one method to the Extractor ABC from ADR 0012 — minor scope expansion of the base class.

### Open for revision as real-world API behavior surfaces

These parameters are placeholders, not settled science. Explicit review triggers once API fixtures and production usage patterns emerge:

- **Retry counts and backoff curves.** Per-source calibration likely needed. FDA's rate-limit regime, CPSC's peak-hour 5xx behavior, NHTSA's download reliability are unknown until observed.
- **5% rejection threshold.** A placeholder — should be per-source and based on observed reject rates. USDA's bilingual-timing quirk may push rejects above 5% during normal operation without indicating a real problem.
- **Business invariant checklist.** The three v1 checks are a starting point. Expect additions as real-world data reveals patterns that shouldn't enter bronze. Future brainstorming is anticipated during fixture-building and early production.
- **Retry library choice.** `tenacity` is fine for now. If fixture work reveals ergonomic gaps, `backoff` or a custom decorator is an easy swap — the ABC lifecycle boundary is the clean migration seam.
