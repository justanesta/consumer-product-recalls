# 0020 — Pipeline state tracking via Neon watermark tables with optional Prefect Cloud overlay

- **Status:** Accepted
- **Date:** 2026-04-17

## Context

The pipeline needs to answer several distinct state-shaped questions at runtime:

- "What's the newest CPSC recall I've already seen?" (incremental extraction)
- "Did yesterday's USDA workflow succeed?" (operational visibility)
- "Has this exact raw payload already been processed?" (idempotency)
- "When did CPSC bronze last successfully refresh?" (freshness)
- "Which record in gold traces back to which raw payload in R2?" (lineage)

These map to three separable concerns:

| Concern | Example question | Where it lives today |
|---|---|---|
| **Domain state** (source-specific cursors, ETags, last-seen IDs) | "What's my bookmark for CPSC?" | Nowhere yet — this ADR fills the gap |
| **Run metadata** (did this invocation succeed, how many records, how long) | "Did yesterday's FDA run work?" | GitHub Actions UI + logs only; not SQL-queryable |
| **Idempotency + lineage** | "Seen this payload before?" / "Where did this row come from?" | Already solved — content hashing (ADR 0007), raw in R2 (ADR 0004), `_rejected` tables (ADR 0013) |

Without explicit domain state, extractors must either re-fetch full history every run (wasteful) or rely on brittle heuristics ("yesterday's date") that break on a single missed run. Without SQL-queryable run metadata, "did the USDA pipeline run this week?" requires clicking through the GitHub Actions UI rather than a dashboard query.

Four places state could live — considered honestly:

- **State files committed back to the repo** (workflow writes via `GITHUB_TOKEN`). Gives a free git audit trail but introduces race conditions when two extractors merge on the same day, noisy commit history, and a non-transactional gap between the bronze load and the state commit. Fixable with lockfile workflows, but the coupling fights the rest of the architecture.
- **State files in R2.** Simpler than repo commit-back; R2 is already in the stack; conditional writes via `If-Match` give optimistic concurrency. A legitimate choice. The tradeoff is weaker transactional correctness with the bronze load — state lives in a different storage system than the data, so a two-writer failure mode exists (mitigated but not eliminated by content-hash idempotency).
- **Neon tables.** Transactional with the bronze load, SQL-queryable for monitoring, free tier already in use. Strongest correctness. Small schema surface.
- **Orchestrator-managed state** (Prefect / Dagster / Airflow). Covers run metadata excellently; does not cover domain state (source cursors are not an orchestrator concern — they're domain logic). Adds hosting and a tooling commitment that exceeds v1's complexity budget.

Orchestrator options specifically, given ADR 0010 already chose GitHub Actions:

- **Dagster** is a strong conceptual fit — its asset-based model maps cleanly onto bronze/silver/gold tables and `@dbt_assets` integrates with ADR 0011. However, it requires either Dagster+ Serverless (another managed service) or self-hosting (a VM), and restructuring extraction code around the asset model. High portfolio value, but a large v1 commitment.
- **Prefect Cloud (free tier)** is a lighter lift — workers can execute inside GitHub Actions, so Prefect becomes a metadata/UI layer over GA rather than a compute replacement. Adds observability without forcing a compute migration.
- **Airflow (MWAA / Composer)** is overkill for five pipelines and is not free.

## Decision

### v1: Neon watermark tables + GitHub Actions (per ADR 0010) unchanged

Two tables, created by the same migration tooling that manages bronze/silver/gold schemas.

**`source_watermarks`** — one row per source. Holds domain state that extractors read before fetching and write after a successful bronze load.

```sql
CREATE TABLE source_watermarks (
  source TEXT PRIMARY KEY,                -- 'cpsc' | 'fda' | 'usda' | 'nhtsa' | 'uscg'
  last_successful_run_at TIMESTAMPTZ,     -- when the last successful run finished
  last_seen_published_at TIMESTAMPTZ,     -- highest source-native publication timestamp ingested
  last_seen_source_id TEXT,               -- most-recent source_recall_id seen (useful for cursor-style APIs)
  last_etag TEXT,                         -- If-None-Match value for sources that support conditional GETs
  last_record_count INTEGER,              -- records returned on last successful run (sanity context)
  notes JSONB,                            -- per-source structured metadata (e.g., FDA pagination cursor state)
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

**`extraction_runs`** — one row per workflow invocation. Written at run start with `status='running'`, updated at run end with terminal status and counts.

```sql
CREATE TABLE extraction_runs (
  run_id UUID PRIMARY KEY,
  source TEXT NOT NULL,
  started_at TIMESTAMPTZ NOT NULL,
  finished_at TIMESTAMPTZ,
  status TEXT NOT NULL,                   -- 'running' | 'succeeded' | 'failed' | 'partial'
  records_fetched INTEGER,
  records_loaded INTEGER,
  records_rejected INTEGER,               -- correlates with _rejected tables per ADR 0013
  github_run_url TEXT,                    -- deep link back to the GHA run for inspection
  error_message TEXT,
  CONSTRAINT extraction_runs_source_fk FOREIGN KEY (source) REFERENCES source_watermarks(source)
);

CREATE INDEX extraction_runs_source_started_idx
  ON extraction_runs (source, started_at DESC);
```

### Transactional coupling with bronze load

At the end of the extractor lifecycle (ADR 0012 / ADR 0013's `load_bronze()`), the extractor writes bronze rows, the `source_watermarks` update, and the terminal `extraction_runs` update **in a single Postgres transaction**. If the bronze insert succeeds but the watermark update fails, the transaction rolls back and content-hash idempotency (ADR 0007) guarantees the next run reprocesses the same window without duplication.

### Future path: Prefect Cloud free tier as an optional overlay

Adopting Prefect Cloud later is a **low-friction addition, not a replacement**:

- Compute stays on GitHub Actions runners (ADR 0010 unchanged). Prefect workers run from inside the existing GA workflows.
- `extraction_runs` rows become redundant with Prefect's flow-run state — optionally retired at that point, or kept as a secondary audit surface.
- `source_watermarks` stays put. Prefect does not track source-specific cursors; that is domain state, not pipeline state.

The migration seam is clean: the work is wrapping extractor functions as `@flow` / `@task` and registering a Prefect deployment, not rewriting state storage. This is the explicit reason to pick Neon tables over R2 state files — it removes the "do we need to migrate state storage too?" question from a future orchestrator decision.

### Deferred: Dagster

Dagster's asset model is a strong fit for the medallion architecture and dbt, and would carry meaningful portfolio value. It is deferred rather than rejected because adopting it at v1 means:

- Hosting Dagster (Dagster+ Serverless free tier or self-hosted on Fly.io / a small VM).
- Restructuring extraction code as `@asset` definitions instead of per-source modules.
- An early architectural bet before v1 has shaken out its real operational patterns.

A future ADR may adopt Dagster if v1 operational experience demonstrates the asset-based model would meaningfully improve the pipeline. The watermark + Prefect-optional path decided here does not close that door.

### Rejected: state files (repo or R2)

- **Repo-committed state files** — rejected for commit-back race conditions and lack of transactional coupling with the bronze load.
- **R2 state files** — a legitimate alternative. Rejected in favor of Neon because (a) transactional correctness is free when watermarks live in the same database as bronze, (b) SQL queryability for monitoring is free, and (c) no additional migration step is needed when Prefect is adopted later.

### Rejected: Airflow

Hosting cost and operational weight exceed v1 complexity. Re-evaluate only if cross-source DAG dependencies become complex enough to require modeling beyond dbt's `ref()`.

## Consequences

- Domain state (cursors, ETags) and run metadata (success/failure, counts, durations) are both durable, SQL-queryable, and transactional with the bronze load — correctness is strongest under concurrent/partial-failure scenarios.
- Operational queries in `operations.md` can answer "did the CPSC extractor run today?" or "what's the last-seen FDA event timestamp?" with a single SQL statement.
- Re-ingestion per ADR 0014 still reads raw from R2 and relies on content hashing for idempotency — watermarks are a performance concern, not a correctness one. A re-ingest can clobber or ignore watermarks without data-correctness risk.
- `source_freshness:` assertions in dbt (ADR 0015) can compare `source_watermarks.last_successful_run_at` against expected cadence — a cleaner freshness signal than `max(published_at)` of source records.
- The `github_run_url` column gives a one-click hop from a failed-run SQL query to the actual GitHub Actions log — fast operational debugging.
- Adopting Prefect Cloud later requires no domain-state migration — only wrapping functions as flows. The deferred-Dagster door stays open as well.
- Small schema surface: two tables, one foreign key. Migration effort is negligible.

### Open for revision

- **Table schema details.** The column list above is a v1 starting point. Per-source needs may surface columns that are not obvious yet — e.g., NHTSA's flat-file download URL state, or USDA's bilingual-pairing timing buffer. Add columns as needed; migrations stay small.
- **Run-row lifecycle.** Writing a `status='running'` row at start and updating it at end is the simple approach. If GA runs get killed mid-execution often enough to leave stale `running` rows, add a janitor query or switch to end-only writes.
- **Adoption of Prefect Cloud.** Revisit once v1 is stable. Triggers would be: wanting better flow-run observability than the GitHub Actions UI provides, or needing retries/observability features GA does not express cleanly.
- **Adoption of Dagster.** Revisit after v1 if asset-based lineage + dbt integration would meaningfully improve the pipeline. A separate ADR at that time.
