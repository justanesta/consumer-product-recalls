# Architecture

System-level overview of the consumer-product-recalls ETL pipeline. Covers the four-layer medallion structure, end-to-end data flow, the components that implement each layer, and the load-bearing invariants that hold across them.

This is the reader's-entry-point document. For:
- **Per-source silver mapping decisions** (column unification, surrogate keys, null-filling) — see [`silver_design_notes.md`](silver_design_notes.md).
- **Schema reference** (table-by-table column types, business keys, glossary) — see [`data_schemas.md`](data_schemas.md).
- **Local development** (setup, running tests, debugging) — see [`development.md`](development.md).
- **Command cheat sheet** (uv, recalls CLI, alembic, ruff, pyright, pytest, dbt, bru, R2, neonctl, psql, gh) — see [`commands.md`](commands.md).
- **Production operations** (monitoring queries, secret rotation, re-ingestion procedures) — see [`operations.md`](operations.md).
- **Why a particular choice was made** — see [`decisions/`](decisions/) (Architecture Decision Records).

---

## The four-layer medallion

The pipeline is built around the medallion architecture defined in [ADR 0004](decisions/0004-four-layer-medallion-pipeline.md):

```
   ┌──────────────┐    ┌──────────────┐    ┌──────────────┐    ┌──────────────┐
   │   Landing    │ →  │    Bronze    │ →  │    Silver    │ →  │     Gold     │
   │              │    │              │    │              │    │              │
   │ raw payloads │    │  validated,  │    │   unified    │    │ pre-aggregated│
   │  on R2,      │    │  per-source, │    │   schema     │    │  views,       │
   │  immutable   │    │  insert-only │    │   across     │    │  search       │
   │              │    │              │    │   sources    │    │  indexes      │
   └──────────────┘    └──────────────┘    └──────────────┘    └──────────────┘
       (R2)               (Postgres)          (Postgres)          (Postgres)
```

Each layer has a different audience and a different mutability story.

| Layer | Storage | Mutability | Audience | Schema |
|---|---|---|---|---|
| **Landing (T0)** | Cloudflare R2 | Append-only; immutable | Operators (forensic), re-ingest CLI | Source-native (raw JSON / TSV / HTML) |
| **Bronze** | Neon Postgres | Insert-only; content-hash-keyed dedup | Operators, dbt | Per-source Pydantic-validated tables |
| **Silver** | Neon Postgres | Rebuilt by dbt every transform run | Operators, dashboards, gold consumers | Unified across all sources (`recall_event` / `recall_product` / `firm` / `recall_event_firm`) |
| **Gold** | Neon Postgres | Rebuilt by dbt; views/materializations | Dashboards, FastAPI serving layer (Phase 8) | Denormalized, query-shape-driven |

The boundaries are not arbitrary — each one is enforced by a different mechanism:

- **Source → Landing** is enforced by the extractor's `land_raw()` step ([ADR 0012](decisions/0012-extractor-pattern-custom-abc-and-per-source-subclasses.md)). Every byte fetched is persisted to R2 before validation. If anything downstream fails, raw is recoverable.
- **Landing → Bronze** is enforced by Pydantic strict validation ([ADR 0014](decisions/0014-schema-evolution-policy.md)) and content-hash-conditional inserts ([ADR 0007](decisions/0007-lineage-via-bronze-snapshots-and-content-hashing.md)). Records that fail structural or business-invariant checks route to per-source `_rejected` tables ([ADR 0013](decisions/0013-error-handling-retries-idempotency-and-quarantine.md)) — they never enter bronze proper.
- **Bronze → Silver** is enforced by dbt models with generic and singular tests ([ADR 0011](decisions/0011-transformation-framework-dbt-core.md), [ADR 0015](decisions/0015-testing-strategy.md)). dbt does not touch `_rejected` tables — they are forensic surfaces, not transformation inputs.
- **Silver → Gold** is enforced by dbt's view/materialization machinery. Gold is a query-shape projection of silver; no new business logic is introduced.

---

Succinct mental model of data transformations on each layer:
- **Landing** = byte-for-byte what is given from source that lands in R2 buckets
- **Bronze** = parsed and typed source values within column-type constraints (preserves source-verbatim where the column type allows).                                                        
- **Staging (pre-Silver)** = source-shape normalization (empty strings, sentinels, encoding, dedup, bilingual filter). Per-source cleanup, no cross-source thinking.
- **Silver** = cross-source unification (surrogate keys, unions, firm dedup, role assignment). The first layer where "give me all recall events across CPSC + FDA + USDA" is one query.       
- **Gold** = aggregates/reports for dashboards (e.g. recalls_by_month). 

## End-to-end data flow

```
                                     ┌─────────────────────────────────────────┐
                                     │  GitHub Actions cron schedule (ADR 0010) │
                                     │  • daily: extract-cpsc, extract-fda,    │
                                     │           extract-usda                  │
                                     │  • weekly: extract-nhtsa, extract-uscg, │
                                     │            deep-rescan-cpsc/fda         │
                                     └────────────────┬────────────────────────┘
                                                      │
                                                      │ workflow_dispatch /
                                                      │ schedule trigger
                                                      ▼
              ┌──────────────────────────────────────────────────────────────────┐
              │                      Extractor (per source)                     │
              │                                                                 │
              │   ┌──────────┐   ┌──────────┐   ┌──────────┐   ┌─────────────┐  │
              │   │ extract  │ → │land_raw  │ → │ validate │ → │check_invar. │  │
              │   │ (live)   │   │  (R2)    │   │(Pydantic)│   │ (business)  │  │
              │   └────┬─────┘   └────┬─────┘   └────┬─────┘   └──────┬──────┘  │
              │        │              │               │                │         │
              │        │              │               ▼                ▼         │
              │        │              │           ┌────────────────────────┐    │
              │        │              │           │   _rejected tables     │    │
              │        │              │           │ (per-source forensic)  │    │
              │        │              │           └────────────────────────┘    │
              │        │              │                                          │
              │        │              ▼                                          │
              │        │     ┌──────────────────┐                                │
              │        │     │ Cloudflare R2    │   (T0 — raw, immutable)        │
              │        │     │ <source>/<date>/ │                                │
              │        │     └──────────────────┘                                │
              │        │                                                          │
              │        ▼                                                          │
              │   ┌──────────────────┐                                            │
              │   │  load_bronze     │   (content-hash conditional insert)        │
              │   └────┬─────────────┘                                            │
              └────────┼─────────────────────────────────────────────────────────┘
                       │
                       ▼
              ┌────────────────────────────────────┐
              │  Bronze (Postgres)                 │
              │  • <source>_recalls_bronze         │
              │  • <source>_recalls_rejected       │
              │  • source_watermarks               │
              │  • extraction_runs                 │
              └─────────────┬──────────────────────┘
                            │
                            │ scheduled: dbt build (Phase 7 transform workflow)
                            ▼
              ┌────────────────────────────────────┐
              │  Silver (Postgres, dbt-managed)    │
              │  • staging: stg_<source>_*         │
              │  • silver:  recall_event,          │
              │             recall_product, firm,  │
              │             recall_event_firm,     │
              │             recall_event_history   │
              │  • dbt tests: not_null, unique,    │
              │    accepted_values, relationships  │
              └─────────────┬──────────────────────┘
                            │
                            │ same dbt run, downstream models
                            ▼
              ┌────────────────────────────────────┐
              │  Gold (Postgres, dbt-managed)      │
              │  • aggregate views                 │
              │  • search-index materializations   │
              │  • feeds Phase 8 FastAPI layer     │
              └────────────────────────────────────┘
```

Three things happen per extraction run that the diagram above abbreviates:

1. **Run metadata is recorded.** `extraction_runs` gets a row with `source`, `started_at`, `status`, `records_extracted`, `records_inserted`, and `change_type` (per [ADR 0027](decisions/0027-bronze-storage-forced-transforms-only.md), distinguishing routine runs from re-baselines).
2. **Watermarks advance** for sources that have one. `source_watermarks.last_extracted_at` is updated after a successful run ([ADR 0020](decisions/0020-pipeline-state-tracking.md)). Watermarks are advisory cursors — they tell the extractor where to start its next incremental query, but the bronze content-hash dedup is what actually prevents duplicates.
3. **Logs are emitted to stdout in JSON** via `structlog`, with a `run_id` correlation ID that ties together every log line from a single extraction ([ADR 0021](decisions/0021-structured-logging.md)).

---

## Components

### `src/extractors/` — the extraction layer

| File | Role |
|---|---|
| `_base.py` | `Extractor` ABC — defines the 5-step lifecycle (`extract`, `land_raw`, `validate`, `check_invariants`, `load_bronze`) shared by every source |
| `_rest_api.py` | `RestApiExtractor` — operation-type subclass for JSON REST sources (CPSC, FDA, USDA) |
| `_flat_file.py` | `FlatFileExtractor` — operation-type subclass for tab-delimited downloads (NHTSA, Phase 5c) |
| `_html_scraping.py` | `HtmlScrapingExtractor` — operation-type subclass for HTML scraping (USCG, Phase 5d) |
| `cpsc.py` / `fda.py` / `usda.py` / `usda_establishment.py` | Per-source concrete subclasses |

The hierarchy is **two layers deep**: `Extractor` (ABC) → operation-type subclass (`RestApiExtractor`, etc.) → per-source concrete subclass. This was deliberate per [ADR 0012](decisions/0012-extractor-pattern-custom-abc-and-per-source-subclasses.md): `Extractor` defines the lifecycle contract, the operation-type subclasses encode shape-specific concerns (pagination loops for REST, ZIP unpacking for flat files, BeautifulSoup parsing for scraping), and concrete subclasses encode source-specific quirks (auth headers, watermark column names, response-shape multiplexing).

### `src/landing/` — raw payload landing

| File | Role |
|---|---|
| `r2.py` | R2 client wrapper — writes raw extracted bytes to `<source>/<extraction_date>/<key>` keys |

R2 is the immutable substrate. Every extraction's raw payload lands here before validation, and stays forever (current retention policy: keep). This is the substrate for ADR 0014 schema-drift recovery and ADR 0028 backfill mechanisms B and C.

### `src/bronze/` — bronze loading and shared mechanisms

| File | Role |
|---|---|
| `loader.py` | `BronzeLoader` — content-hash conditional insert + quarantine routing |
| `hashing.py` | Canonical-serialization + SHA-256 helpers (per ADR 0007 — changes here are treated as schema migrations) |
| `retry.py` | `tenacity`-decorated retry policies, applied to lifecycle methods that contact external services |
| `invariants.py` | Cross-record / business-logic checks (e.g., USDA bilingual orphan, date sanity, null-ID guard) |

The bronze layer's job is "what arrived, what we kept, what we rejected, why" — it is the audit boundary between "bytes from the source" and "data we've taken responsibility for."

### `src/schemas/` — Pydantic bronze contracts

One file per source, each with `ConfigDict(extra='forbid', strict=True)` per [ADR 0014](decisions/0014-schema-evolution-policy.md). Required-by-default fields catch silent renames; `extra='forbid'` catches silent additions. Every drift event surfaces loud at the boundary, not silently downstream.

Per [ADR 0027](decisions/0027-bronze-storage-forced-transforms-only.md), schemas perform only storage-forced transforms (date string → datetime for `TIMESTAMPTZ`, `"True"`/`"False"` → bool for `BOOLEAN`). Value-level normalization (empty-string → null, whitespace strip, false-sentinel handling) lives in silver staging models, not here.

### `src/cli/` — Typer CLI dispatch

`recalls extract <source>`, `recalls re-ingest`, debug subcommands. Thin dispatch over the Extractor ABC and bronze loader; no business logic. Per [ADR 0012](decisions/0012-extractor-pattern-custom-abc-and-per-source-subclasses.md) Implementation notes.

The CLI currently instantiates extractors with hardcoded constructor kwargs — the YAML-driven dispatch promised in ADR 0012 is filed as a Phase 6/7 architectural follow-up. See `project_scope/implementation_plan.md`.

### `src/config/` — settings + structured logging

| File | Role |
|---|---|
| `settings.py` | `pydantic-settings` `Settings` model — loads `.env`, fails loud on missing required values, marks credentials as `SecretStr` |
| `logging.py` | `structlog` configuration with `run_id` contextvar, stdlib bridge for third-party libraries |

### `migrations/versions/` — Alembic migrations

Per-source bronze + rejected tables, the shared `extraction_runs` and `source_watermarks` state tables, and `extraction_runs.change_type` (added in Phase 5b.2 per ADR 0027). Migrations are forward-only — there is no `downgrade()` body that does anything meaningful, by convention.

### `dbt/models/` — silver and gold transformations

| Subdirectory | Role |
|---|---|
| `staging/stg_<source>_*.sql` | Per-source views over bronze with type casting, latest-version dedup, value-level normalization |
| `silver/recall_event.sql` etc. | Unified cross-source models — one row per recall event regardless of source |
| `gold/recalls_by_month.sql` etc. | Aggregate views and search materializations |

Generic dbt tests (`not_null`, `unique`, `accepted_values`, `relationships`) and singular tests (orphan detection, source count baselines) are configured per [ADR 0015](decisions/0015-testing-strategy.md).

### `tests/` — pytest

Organized into `unit/`, `integration/`, and `e2e/` per [ADR 0015](decisions/0015-testing-strategy.md). Integration tests use VCR cassettes for replayable network scenarios; `respx` is the accepted pattern for hand-constructed error-path mocks.

---

## Load-bearing invariants

Five properties hold across the entire pipeline. Each is enforced by a specific mechanism, not by convention. If any of them break silently, multiple downstream guarantees break with it.

### 1. Idempotency

Re-running any extractor (or any backfill mechanism per [ADR 0028](decisions/0028-backfill-historical-reextraction-semantics.md)) over the same window produces no duplicate bronze rows.

**Enforcement:** content-hash conditional insert in `BronzeLoader.load()`. The hash is computed via `src/bronze/hashing.py`'s canonical serialization, and any insert whose hash already exists for the same `(source_recall_id, [identity-suffix])` becomes a no-op.

**Consequences:** workflows can be safely retried. Cron overlap with ad-hoc deep-rescan runs is safe. Schema-drift recovery via R2 replay is safe. None of these need cross-coordination.

### 2. Schema-drift visibility

Every meaningful change in source response shape — added field, removed field, renamed field, type change — surfaces loudly at the bronze boundary, not silently downstream.

**Enforcement:** Pydantic `extra='forbid'` + `strict=True` + required-by-default ([ADR 0014](decisions/0014-schema-evolution-policy.md)). Added field → forbid error. Renamed field → missing-required error. Type change → strict-mode validation error. All three route to `<source>_recalls_rejected` with `failure_stage='validate'`.

**Consequences:** silver layer never has to second-guess what bronze means. The trade-off is that schema drift causes ingestion to halt for the affected source — operators need to amend the schema and re-ingest from R2 (per [ADR 0014](decisions/0014-schema-evolution-policy.md), [ADR 0028](decisions/0028-backfill-historical-reextraction-semantics.md) Mechanism B). This is the right trade-off because silent drift is worse than loud halt.

### 3. Watermarking + content-hash dedup composes correctly

The incremental cursor is advisory; the dedup is authoritative. A watermark that misses an edit, a deep rescan over a too-wide window, a clock-skew event — none of these duplicate data.

**Enforcement:** `source_watermarks` is updated after a successful run ([ADR 0020](decisions/0020-pipeline-state-tracking.md)) but the bronze loader does not rely on it for correctness. Content-hash dedup is the actual guard.

**Consequences:** weak-watermark sources (CPSC, USDA — see [ADR 0010](decisions/0010-ingestion-cadence-and-github-actions-cron.md) revision note) are handled by deep-rescan workflows that ignore the watermark and re-fetch wider windows. Bronze stays correct.

### 4. Raw payloads survive every failure mode

Anything fetched from a source is persisted to R2 before validation, so any downstream failure is recoverable.

**Enforcement:** `Extractor.land_raw()` is the second lifecycle step; nothing in steps 3–5 can prevent it from running. Network failures in step 1 fail loud (no raw to land); failures in steps 3–5 leave raw intact.

**Consequences:** R2 is the substrate for [ADR 0014](decisions/0014-schema-evolution-policy.md) re-ingest, [ADR 0028](decisions/0028-backfill-historical-reextraction-semantics.md) Mechanism B (R2 replay), and [ADR 0028](decisions/0028-backfill-historical-reextraction-semantics.md) Mechanism C (manifest backfill from raw). R2 retention is load-bearing.

### 5. Failure routes are named and queryable

Every class of failure has a documented destination. Schema-violating records go to `<source>_recalls_rejected`. Transient network failures are retried per `tenacity` policy. Auth failures fail loud, no retry. Throttling has source-specific detection (see ADR 0013 amendment for FDA's HTML-redirect throttling). Bot-manager fingerprinting has its own surface (see ADR 0016 amendment for USDA's Akamai gating).

**Enforcement:** `tenacity`-decorated lifecycle methods (`src/bronze/retry.py`); `_rejected` tables ([ADR 0013](decisions/0013-error-handling-retries-idempotency-and-quarantine.md)); structured-log fields on failure events ([ADR 0021](decisions/0021-structured-logging.md)).

**Consequences:** "what failed and why" is one SQL query away. Operators don't need to read GHA logs to diagnose data-shape problems; the rejected tables hold the record + reason + raw R2 path.

---

## What's not in v1

These are deliberate omissions, each documented in an ADR or in `project_scope/implementation_plan.md` "Out of scope":

- **Frontend dashboard** — Phase 9, deferred. Phase 8 ships a FastAPI serving layer; downstream rendering is a separate decision.
- **Application monitoring beyond GHA UI** — formalized in [ADR 0029](decisions/0029-application-observability-and-alerting.md) with named upgrade triggers. v1 = GHA UI + structured logs + SQL queries from operations.md.
- **EPA integration** — deferred per [ADR 0001](decisions/0001-sources-in-scope.md). Re-evaluate when v1 ships.
- **Statistical drift detection** — needs baseline data; v2 effort per [ADR 0015](decisions/0015-testing-strategy.md).
- **Silver-layer interpretation of source-side deletions** — bronze captures the *signal* via [ADR 0026](decisions/0026-lifecycle-tracking-snapshot-presence-manifest.md)'s manifest, but silver in v1 reports `is_currently_active` only. Modeling deletion as a first-class lifecycle event is v2.
- **Authenticated API tier** — public read-only is sufficient for v1.

---

## Reading order for new contributors

1. `README.md` (repo root) — what the project does and how to run it.
2. This file (`architecture.md`) — system shape.
3. [`decisions/README.md`](decisions/README.md) — index of every architectural decision.
4. [`development.md`](development.md) — how to set up locally and run things.
5. [`commands.md`](commands.md) — quick-reference cheat sheet, kept open while you work.
6. [`data_schemas.md`](data_schemas.md) — when you need to know what a column means.
7. [`silver_design_notes.md`](silver_design_notes.md) — when you're adding a new source's silver mapping.
8. [`operations.md`](operations.md) — when you're operating in production.
