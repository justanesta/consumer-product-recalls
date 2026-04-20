# Data Engineering Project Decision Guide

A reference for the decision points that arise when starting a data engineering project from scratch — the tools, infrastructure choices, and underlying concepts you need to understand to make them well. Organized by the order decisions tend to surface.

---

## 1. Scope and Source Selection

### Decision: Which data sources to ingest?

Before writing any code, define what "in scope" means and commit to it explicitly.

**Concepts to understand:**
- **Semantic consistency** — is each source actually measuring the same thing? Sources that look similar may conflate different event types (e.g., a registration cancellation is not the same as a safety recall). Mixed semantics corrupt aggregations.
- **Engineering feasibility per source** — sources expose data through different interfaces (REST APIs, bulk flat files, HTML pages). Each interface type carries different complexity, brittleness, and maintenance cost.
- **Portfolio breadth vs. coherence tradeoff** — more distinct source types (API, flat file, scrape) demonstrate more skills, but each type that differs from the others adds a distinct maintenance surface.

**Questions to ask:**
- Is the source semantically consistent with the others, or does it require a different event taxonomy?
- Is there a machine-readable interface (API, flat file), or does ingestion require HTML scraping?
- What is the publication cadence? How are updates to existing records handled?
- What is the historical volume and expected annual growth?

**Outcome of this decision:** defines which sources get extractors, which get deferred (with a clear condition for reopening), and which are cut.

---

## 2. Data Model and Unit of Analysis

### Decision: What is the row? What is the schema?

The most consequential early decision. Gets harder to change after data flows.

**Concepts to understand:**
- **Unit of analysis** — the entity that one row in your primary table represents. A single source may have multiple natural granularities (event-level vs. product-level vs. component-level).
- **1:N relationships** — most real-world entities have a "header" with multiple child "lines" (e.g., one recall campaign with many affected vehicle models). Flattening to pure event level loses detail; pure line level denormalizes header fields across N rows (expensive to update).
- **Header / line / dimension normalization** — a pattern that separates the recall event (header), affected products (lines), and shared entities like firms (dimensions) into separate tables linked by foreign keys. Balances query flexibility with update efficiency.
- **JSONB for sparse fields** — when lines from different sources have different fields, putting source-specific attributes into a JSONB column avoids wide sparse tables while preserving queryability.
- **Entity resolution** — when the same real-world entity (e.g., a manufacturer) appears across multiple sources with variant names, reconciling them into a canonical identity is a real data engineering problem requiring fuzzy matching.

**Questions to ask:**
- What is the consumer's canonical query? (e.g., "is my vehicle recalled?" vs. "how many recalls this quarter?") — the unit of analysis should make that query trivially simple.
- What is the natural granularity of each source's API response?
- Which fields are shared across all sources? Which are source-specific?
- How often do records update? If a recall is revised, how many rows must change?
- Do cross-source rollups (e.g., by firm) need to be possible?

**Schema forward-compatibility:**
- **Discriminator columns** (e.g., `event_type TEXT DEFAULT 'RECALL'`) add near-zero cost today and allow future event categories to share the table without schema migration.
- **JSONB extension columns** (`source_specific_attrs`, `summary_alt_lang`) accommodate long tails of per-source or per-language fields without proliferating columns.

---

## 3. Pipeline Architecture

### Decision: How many layers? ETL or ELT? What lives where?

**Concepts to understand:**
- **ETL (Extract-Transform-Load)** — data is transformed before loading into the destination. Simple, but ties transformation to extraction; re-running transformations requires re-extracting.
- **ELT (Extract-Load-Transform)** — raw data is loaded first, then transformed in the destination. Enables re-running transformations without re-hitting the source. Better for warehouse-centric architectures.
- **Medallion / multi-layer architecture** — a staging pattern with named layers at different levels of curation:
  - **Landing (raw)** — byte-for-byte what the source sent. Immutable. Used for audit, replay, and re-ingestion.
  - **Bronze** — lightly typed version of the source's native shape. Source-specific, queryable, schema-on-load.
  - **Silver** — harmonized, validated, business-model-aligned data across all sources.
  - **Gold** — pre-aggregated, denormalized views and materialized views for serving layer performance.
- **Object storage vs. relational storage** — object storage (S3, R2) is cheap and durable for raw blobs but requires compute to query. Relational databases are queryable but more expensive per GB. Raw landing belongs in object storage; typed structured data belongs in a database.
- **Data lineage** — the ability to trace a value in gold back through silver, bronze, to the exact raw payload that produced it. Full lineage requires that raw data is preserved and that bronze records are linked to landing files.

**Questions to ask:**
- Do you need to re-run transformations without re-hitting sources? (Almost always yes for APIs with rate limits.)
- What data do you need queryable interactively vs. just preservable for audit?
- What storage tier is appropriate for each layer given volume and query patterns?
- Do you need batch or streaming? (Batch is almost always the right starting point for recall-type data.)

---

## 4. Storage Provider Selection

### Decision: Where does data live?

**Concepts to understand:**
- **Free-tier headroom rule** — target <50% of a free tier at launch to give yourself multi-year growth runway before hitting a cost trigger.
- **Egress costs** — some object storage providers charge per GB downloaded. For a consumer-facing app that serves raw artifacts, zero-egress pricing (Cloudflare R2) can matter.
- **Serverless Postgres** — providers like Neon offer Postgres with instant branching (DB clones), which is highly useful for testing strategies. Cold starts are the trade-off.
- **Migration paths** — evaluate providers not just on their free tier, but on what the upgrade path costs and how difficult migration would be if you exceed the free tier.

**Relevant trade-offs:**

| Concern | Options |
|---|---|
| Postgres (structured) | Neon (3 GB free, branching), Supabase (500 MB), Render (1 GB), Fly.io (1 GB) |
| Object storage (blobs) | Cloudflare R2 (10 GB, zero egress), Backblaze B2 (10 GB, egress fees) |

---

## 5. Extraction Design

### Decision: How are extractors structured?

**Concepts to understand:**
- **Abstract Base Class (ABC)** — a Python pattern where a base class defines a required interface (via `abstractmethod`) that all subclasses must implement, or Python refuses to instantiate them. Enforces consistency across per-source implementations.
- **ABC + Pydantic double inheritance** — combining ABC (interface contract) and Pydantic BaseModel (config validation at construction time) so that a connector *is* its own validated config object. Wrong config types fail immediately at construction, not at first use.
- **Declarative config (YAML-per-source)** — separating what to extract (URLs, auth, cadence, filter params) from how to extract it (code). Makes adding sources a YAML file rather than a Python class. Makes credential rotation a config diff, not a code change.
- **Extraction lifecycle stages** — a clean separation of steps:
  1. `extract()` — fetch raw bytes/records from source
  2. `land_raw()` — write raw payload to object storage
  3. `validate()` — parse via Pydantic; structural contract check
  4. `check_invariants()` — business logic checks Pydantic can't express
  5. `load_bronze()` — content-hash conditional insert into database

**Questions to ask:**
- Which sources use REST APIs? Which use flat files? Which require scraping?
- What auth patterns do each source use (API key in header, OAuth, no auth)?
- Should you use an extraction framework (Singer, Airbyte, Meltano), or build a custom ABC? (Frameworks only earn their keep if pre-built connectors exist for your sources.)

---

## 6. Content Hashing and Idempotency

### Decision: How do you handle duplicate ingestion and track changes?

**Concepts to understand:**
- **Idempotency** — a pipeline operation is idempotent if running it multiple times produces the same result as running it once. Essential for safe retries and re-ingestion.
- **Content hashing** — computing a deterministic hash (SHA-256) of a record's canonical content, then using that hash to skip inserts where nothing has changed. Requires deterministic serialization: sorted keys, no whitespace variation, consistent handling of nulls, timestamps normalized to UTC, floats rounded.
- **Snapshot store vs. current-state store** — bronze tables can be "current state only" (upsert in place) or "append-only snapshots" (every ingestion is a new row). Snapshot stores enable lineage and change history derivation via window functions (`LAG()`), at the cost of storage growth.
- **Conditional insert** — only insert a new row if the content hash differs from the most recent existing hash for that `source_recall_id`. Collapses no-change re-ingestions to no-ops.

**Canonical serialization rules (must be locked down to avoid hash instability):**
- Sort keys at every nesting level
- No whitespace in output
- Strip null values (absent vs. null must be treated equivalently)
- Normalize all timestamps to UTC ISO-8601 before hashing
- Round floats to a fixed precision
- Handle `datetime`, `Decimal`, `UUID` via `default=str`

---

## 7. Error Handling

### Decision: What happens when something goes wrong?

**Concepts to understand:**
- **Failure taxonomy** — the three categories that need different routing:
  - *Transient failures* (network errors, 5xx, timeouts) — retry
  - *Schema violations* (Pydantic ValidationError) — quarantine record, continue pipeline
  - *Business invariant violations* (semantically wrong but structurally valid) — quarantine record, continue pipeline
- **Exponential backoff with jitter** — the standard retry strategy for transient failures. Exponential spacing reduces thundering-herd load on the source; jitter prevents synchronized retry waves from multiple concurrent clients.
- **Retry-After header** — sources that rate-limit (HTTP 429) often specify how long to wait. Honoring this is more robust than exponential backoff alone.
- **Fail-fast for auth errors** — 401/403 errors cannot be resolved by retry. They should fail the workflow immediately and alert.
- **Quarantine architecture (tiered):**
  - *T0* — raw landing in object storage happens unconditionally, before any validation. Always preserved.
  - *T1* — a `_rejected` table per source in the database with structured fields: raw record, failure reason, failure stage, timestamp.
  - *T2* — structured log warning or workflow-exit signal when rejections exceed a threshold.
- **Dead letter queue** — a Kafka-style pattern for routing failed records to a retry queue. Usually overkill for batch pipelines at small scale.
- **Rejection threshold** — a configurable percentage of batch records that can fail before the workflow itself exits with a failure status (vs. partial success).

**Tenacity** is the standard Python library for declarative retry policies (exponential backoff, jitter, specific exception filtering).

---

## 8. Schema Evolution

### Decision: How do you handle upstream API changes?

**Concepts to understand:**
- **Structural drift** — a source adds, renames, removes, or retypes a field. Requires updating the Pydantic model and potentially re-ingesting from raw landing.
- **Value-level semantic drift** — same schema, but new valid values appear in an enum-like field (e.g., a new recall classification). Pydantic's `str` type won't catch this; requires test assertions on known values.
- **`extra='forbid'`** — Pydantic config that raises `ValidationError` on any field not declared in the model. Catches both additions (new unknown field appears) and renames (new-name present → forbid error; old-name missing → required-field error).
- **`strict=True`** — Pydantic config that disables type coercion. Catches source-side type changes.
- **Required-by-default stance** — fields without `Optional` and without a default catch silent renames because the old name disappearing → missing-required-field error. Only use `Optional[T] = None` when the source explicitly documents a field as nullable.
- **Re-ingestion** — when a schema model changes, affected records in the database may need to be re-validated and re-loaded. Because raw landing is preserved in object storage, re-ingestion reads from R2 (not the source API), making it a local operation.

**Division of responsibility:**
- Pydantic = structural contracts at load time (field presence, types, unknown fields)
- dbt tests = semantic contracts at transformation time (valid values, referential integrity, business rules)

---

## 9. Transformation Framework

### Decision: How are bronze → silver → gold transformations expressed?

**Concepts to understand:**
- **dbt-core** — an open-source SQL transformation framework using Jinja-templated SQL models. Key capabilities:
  - `ref()` — creates a DAG of model dependencies, enabling ordered execution and lineage tracking
  - `source()` — declares raw tables as explicitly managed inputs with freshness assertions
  - Generic tests (`not_null`, `unique`, `accepted_values`, `relationships`) declared in YAML
  - Singular tests — custom SQL assertions for cross-model invariants
  - `dbt docs generate` — produces a static lineage graph site
  - Incremental models and snapshots — for efficient re-processing and SCD (Slowly Changing Dimension) patterns
- **Staging layer** — lightweight 1:1 views over bronze tables that handle type casting and column renaming before silver models consume them
- **Data contract division** — the clean separation between what Pydantic validates (load-time structure) and what dbt tests validate (post-transform semantics). Neither should duplicate the other's work.
- **SQLMesh** — a newer dbt alternative with stronger incremental model semantics and virtual environments. Less industry recognition than dbt.

**dbt test posture for silver models:**
- `not_null` on all primary and foreign keys
- `unique` on natural composite keys (e.g., `(source, source_recall_id)`)
- `accepted_values` on every enum-like column
- `relationships` for every foreign key
- Singular tests: orphan detection, date sanity, value sanity, baseline volume guards

---

## 10. Orchestration

### Decision: What triggers pipeline runs and in what order?

**Concepts to understand:**
- **Cron scheduling** — time-based job triggering. Simple, sufficient for most data pipelines whose sources have daily or weekly cadence.
- **Workflow isolation** — when each source has its own workflow file, a failure in one source does not block others. Tight coupling between sources is a common failure mode in monolithic pipelines.
- **Incremental extraction** — querying only records modified since the last run (using a watermark/bookmark). Requires that source timestamps reliably advance on updates, which is often undocumented and must be empirically verified.
- **Deep rescans** — periodic full-history polls as a defense against sources whose modification timestamps don't reliably advance on record edits. Combined with content hashing, a deep rescan of N records produces O(actually changed records) new bronze rows.
- **Pipeline state (watermarks)** — a database table holding per-source cursors (last-seen timestamp, last-seen ID, ETags) that extractors read before fetching and update after successful load. Required for idiomatic incremental extraction.
- **Run metadata tables** — a table recording per-invocation status (succeeded/failed/partial), record counts, duration, and a link back to the CI run. Makes "did CPSC run today?" a SQL query rather than a UI hunt.
- **Transactional coupling** — watermark updates should happen in the same database transaction as the bronze load. If the bronze insert succeeds but the watermark update fails, the transaction rolls back; the next run reprocesses the same window idempotently.
- **`workflow_dispatch`** — a CI trigger that allows manual invocation of any workflow, essential for debugging, re-ingestion, and one-off operations.

**Orchestrator options:**

| Tool | Fit |
|---|---|
| GitHub Actions cron | Free (public repos), git-native logging, sufficient for independent per-source workflows |
| Prefect Cloud (free tier) | Adds DAG UI and flow-run metadata over GA without replacing compute |
| Dagster | Asset-based model maps naturally to medallion layers; requires hosting; high portfolio value |
| Airflow | Heavyweight, requires hosting, overkill for small-scale batch |
| Linux cron | Free but no observability, no git-native UI, extra infra burden |

**Re-evaluation triggers for outgrowing GitHub Actions cron:**
- Any workflow consistently exceeds 60 minutes
- Cross-source DAG dependencies need explicit modeling
- Sub-hourly cadence is required

---

## 11. Pipeline State Tracking

### Decision: Where does pipeline state live?

Three categories of state with different homes:

| State type | What it answers | Best home |
|---|---|---|
| Domain state (source cursors, ETags) | "What's my CPSC watermark?" | Same database as bronze (transactional coupling) |
| Run metadata (success/failure, counts) | "Did FDA run yesterday?" | Same database (SQL-queryable) or orchestrator |
| Idempotency + lineage | "Seen this payload? Where did this row come from?" | Content hashing (ADR 0007) + raw in object storage |

**State storage options:**
- **Neon tables** — transactional with bronze load, SQL-queryable, free tier already in use. Strongest correctness.
- **R2 state files** — simpler than repo commit-back; conditional write with `If-Match` for concurrency. Weaker transactional coupling with bronze.
- **Repo-committed state files** — free git audit trail but introduces commit-back race conditions and a non-transactional gap.
- **Orchestrator-managed state** — covers run metadata well; does not cover domain cursors (those are domain logic).

---

## 12. Testing Strategy

### Decision: How is the pipeline tested?

**Concepts to understand:**
- **Test pyramid** — more unit tests (fast, isolated), fewer integration tests (slower, touching real systems), minimal end-to-end tests (slowest, full pipeline).
- **API fixtures / VCR cassettes** — recorded HTTP interactions (request + response pairs) stored as YAML files and replayed in tests. Provide determinism, speed, and safety (no quota burn, no credentials needed in CI). Also function as a schema-drift archive: cassette diffs show exactly what changed in a source API response.
- **`pytest-vcr` (VCR.py)** — the library that records and replays HTTP cassettes in pytest. `before_record_request` hooks strip credentials from cassettes before committing.
- **`respx` / `responses`** — mock HTTP libraries for unit tests where hand-constructed responses are more appropriate than captured responses (specific error codes, pagination edge cases, malformed payloads).
- **Database portability** — integration tests should consume a single `test_db_url` fixture whose implementation is swappable between Neon branches, local Postgres, and testcontainers. Tests should never call Neon-specific APIs directly.
- **Neon branching for CI** — Neon's instant DB clone feature (branches) allows each CI run to create a pristine database, run tests against it, and delete it on teardown — all within the free tier.
- **Coverage targets** — a coverage floor (e.g., ≥85% line coverage on `src/`) enforced in CI catches gaps. Exclusions are explicit and reviewed.
- **dbt test posture** — generic tests on every model plus singular tests for cross-model invariants. Source freshness assertions catch stale bronze.

**Integration test scenarios per extractor (VCR-backed):**
- Happy path (single page and multi-page)
- Empty result
- Pagination termination
- 429 rate limit (retry ladder)
- 500 transient error (backoff + retry)
- 401 auth failure (fail fast)
- Malformed record (routes to rejected table; others continue)
- Content-hash dedup (second run produces no new rows)

---

## 13. Secrets Management

### Decision: How are credentials stored and accessed?

**Concepts to understand:**
- **`pydantic-settings`** — loads environment variables and `.env` files into a typed Pydantic model at process start. `extra='forbid'` means a missing required credential raises `ValidationError` at boot, not at first use.
- **`SecretStr`** — Pydantic's type for credential fields. `repr()` renders as `**********`, preventing accidental logging.
- **`.env` + `.env.example`** — the `.env` file is gitignored; `.env.example` is committed with placeholder values and comments pointing at where each credential is obtained.
- **`direnv`** — a shell tool that auto-injects environment variables when `cd`-ing into a directory. Can auto-activate venvs and source from password-manager CLIs for developers who prefer not to keep plaintext credentials on disk.
- **Production secrets** — for CI-driven pipelines, CI provider secrets (GitHub Actions repository secrets) are the production source of truth. Never bake credentials into workflow files or committed config.
- **Credential leakage surfaces** — two specific leak paths for pipeline projects: VCR cassettes (recorded HTTP responses may include auth headers) and log output (HTTP libraries may log full request headers). Both require explicit defense.
- **`gitleaks` / `detect-secrets`** — pre-commit hooks that scan staged diffs for credential patterns (high-entropy strings, key formats, JWTs).
- **Rotation cadence** — establish a periodic rotation schedule (e.g., 90 days) and document the runbook before you need it. Automating a reminder (a scheduled issue) prevents forgetting.

---

## 14. Package Management

### Decision: How are Python dependencies managed?

**Concepts to understand:**
- **Lockfile** — a file (e.g., `uv.lock`) that pins the exact resolved version of every direct and transitive dependency. `uv sync --frozen` in CI enforces that exactly what's in the lockfile is installed — no silent drift between machines.
- **`uv`** — a Rust-based Python package/venv/version manager that replaces `pip` + `pip-tools` + `pyenv` + `virtualenv` with a single tool. 10–100× faster than pip. Uses `pyproject.toml` as the source of truth.
- **`pyproject.toml`** — the modern Python packaging standard for declaring dependencies, tool config (ruff, pyright, pytest, coverage), and project metadata in one file.
- **`uv run <command>`** — runs a command in the managed venv without requiring explicit venv activation. Canonical pattern for CI and scripts.

---

## 15. Code Quality and CI

### Decision: What gates code quality, and when do they run?

**Concepts to understand:**
- **Linting** — static analysis for style, common mistakes, and code quality. `ruff` is the modern fast Python linter (replaces flake8, isort, and more).
- **Type checking** — static analysis that verifies type annotations are correct and complete. `pyright` is the standard tool for strict Python type checking.
- **Pre-commit hooks** — scripts that run automatically before a `git commit`. Catch issues before they reach CI. `pre-commit` (the library) manages hook installation and invocation.
- **Defense in depth** — run the same hooks in CI (`pre-commit run --all-files` on every PR). Developers who skip `pre-commit install` are still caught at CI time.
- **Branch protection** — GitHub settings that prevent direct pushes to `main`, require status checks to pass before merge, and prevent force-pushes. The CI posture is only meaningful if main can't be bypassed.
- **Workflow triggers** — distinct events (PR, push to main, cron, manual dispatch) should trigger different job sets. Production ingestion crons should run independently of PR checks to avoid mutual interference.

**Recommended pre-commit hook set for a Python data pipeline:**

| Hook | What it catches |
|---|---|
| `ruff` + `ruff-format` | Lint violations, formatting drift |
| `pyright` | Type errors |
| `gitleaks` | Credentials in diffs |
| Custom cassette scrub verifier | Auth headers in committed VCR cassettes |
| Custom Pydantic strict verifier | Bronze models missing `extra='forbid'`/`strict=True` |
| `uv lock --check` | Lockfile out of sync with `pyproject.toml` |

---

## 16. Structured Logging

### Decision: How are pipeline runs observed?

**Concepts to understand:**
- **Structured logging** — log output as JSON key/value pairs rather than free-text strings. Enables filtering, aggregation, and dashboard queries on log fields without parsing.
- **Correlation ID** — a UUID generated at the start of each pipeline run and attached to every log line from that run. With a correlation ID, filtering "all logs from last Tuesday's failed FDA run" is a single JSON-filter query.
- **`contextvars`** — Python's mechanism for context-local state (similar to thread-local storage but async-safe). `structlog.contextvars` allows binding `run_id` and `source` once at run start and having all subsequent log calls inherit it automatically.
- **Stdlib bridge** — third-party libraries (SQLAlchemy, httpx, dbt, tenacity) log through Python's stdlib `logging` module. A bridge (e.g., `structlog.stdlib.ProcessorFormatter`) routes their output through the same structured pipeline so all logs appear in one stream.
- **`structlog`** — the standard Python structured logging library. Processor-chain architecture, context-local bindings, full stdlib-logging bridge, TTY-aware dev rendering.
- **Log shipping** — sending JSON log output to an aggregation platform (Grafana Loki, Datadog, BetterStack). At v1 scale, stdout captured by CI is sufficient; the JSON format is compatible with any future shipper.
- **Log levels** — consistent conventions: retries → `warning`; quarantine inserts → `warning`; auth failures → `error`; high rejection rate → `error`.

---

## 17. Data Source-Specific Patterns

### Bilingual / duplicate records
When a source publishes the same record in multiple languages as separate API rows (e.g., USDA's English + Spanish records), naive ingestion double-counts. The canonical solution: collapse to one row per canonical record ID at the silver layer, attach alternate-language content in a JSONB column (e.g., `summary_alt_lang: {"es": "..."}`).

### Bulk file vs. API
When a source offers both a bulk flat file and a record-level API, use the bulk file for ingestion and the API for live lookup in the serving layer. Enumerating all possible query parameters to reconstruct what a bulk file provides atomically is operationally wasteful and brittle.

### Scraping brittle sources
HTML scraping requires: rate limiting and `robots.txt` respect, raw HTML archival to landing storage (so schema drift in the page structure can be detected by diffing archives), and explicit schema-drift alarms when the page structure changes.

### Line-level vs. event-level granularity
Preserve native source granularity in bronze. Silver-layer roll-up decisions should be driven by the canonical consumer query. If the question is "is my Honda Civic recalled?", line-level rows with indexed `(brand, model, model_year)` columns beat unpacking JSONB arrays in every query.

---

## 18. Documentation and Architecture Records

### Decision: How is architectural thinking preserved?

**Concepts to understand:**
- **ADR (Architecture Decision Record)** — a short document capturing: context (why this decision needed to be made), decision (what was chosen), and consequences (trade-offs accepted). ADRs are immutable once accepted; superseded decisions get a new ADR, not an edit.
- **When to write an ADR** — whenever someone reading the code six months later would ask "why did they do it this way instead of the obvious alternative?" Trivial choices (variable naming, lint config) don't need ADRs; substantive trade-offs do.
- **ADR as portfolio artifact** — a record of the reasoning process is as valuable for demonstrating engineering judgment as the code itself.
- **Lineage graph as documentation** — dbt's `dbt docs generate` produces a deployable static site with a full model lineage graph. Publishing it automatically on every main push (to Cloudflare Pages, Vercel, etc.) keeps docs in sync with code.
- **ERDs and DAG diagrams** — Entity-Relationship Diagrams for the silver schema and DAG diagrams for the pipeline are worth maintaining as `.drawio` XML files (git-diffable, round-trippable) with exported SVG for embedding in documentation.
- **Avoiding documentation duplication** — a separate `architecture.md` that summarizes what ADRs already cover will diverge from the ADRs as they evolve. Prefer a Mermaid diagram in `README.md` for at-a-glance structure and an ADR index for the structured deep dive.

---

## Decision Checklist for New Projects

Use this as a starting checklist when standing up a new data engineering project:

**Scope**
- [ ] Sources defined and semantically validated
- [ ] Interface type per source (API / flat file / scrape) determined
- [ ] Historical volume and annual growth estimated per source
- [ ] Update/edit behavior documented per source

**Data model**
- [ ] Unit of analysis chosen for each layer
- [ ] Header/line/dimension normalization decision made
- [ ] JSONB strategy for sparse/source-specific fields decided
- [ ] Discriminator columns added for forward compatibility

**Architecture**
- [ ] Layer count and medallion shape decided
- [ ] Object storage vs. relational storage split decided
- [ ] ETL vs. ELT decision made
- [ ] Storage providers selected with headroom projections

**Extraction**
- [ ] Extractor ABC designed with consistent lifecycle
- [ ] Declarative config (YAML-per-source) for source definitions
- [ ] Auth patterns documented per source

**Resilience**
- [ ] Content hashing and idempotency implemented
- [ ] Error taxonomy defined (transient / schema / invariant)
- [ ] Quarantine architecture (T0/T1/T2) defined
- [ ] Retry policy library chosen and configured
- [ ] Schema evolution policy documented
- [ ] Re-ingestion procedure designed

**Orchestration**
- [ ] Scheduling mechanism chosen
- [ ] Workflow isolation enforced (per-source)
- [ ] Incremental extraction strategy designed
- [ ] Deep rescan strategy for sources with unreliable timestamps
- [ ] Watermark tables and run metadata tables designed
- [ ] Transactional coupling between bronze load and watermark update

**Transformation**
- [ ] Transformation framework chosen (dbt-core, plain SQL, etc.)
- [ ] Staging / silver / gold layer models defined
- [ ] dbt test posture defined for each model type

**Testing**
- [ ] Test pyramid defined (unit / integration / e2e)
- [ ] API fixture strategy (VCR cassettes, respx)
- [ ] Integration database strategy (Neon branches, testcontainers, local Postgres)
- [ ] Coverage floor set
- [ ] dbt tests written for silver models

**Operations**
- [ ] Secrets management approach chosen
- [ ] `.env.example` committed
- [ ] Structured logging configured with correlation IDs
- [ ] Pre-commit hooks installed
- [ ] Branch protection on main configured
- [ ] CI workflow triggers defined
- [ ] Credential rotation runbook written
- [ ] Re-ingestion runbook written

**Documentation**
- [ ] ADR template adopted and first ADR written
- [ ] ADR index maintained
- [ ] README with Mermaid pipeline diagram
- [ ] ERD for silver schema
- [ ] Development, operations, and data schema docs

---

## Key Concept Glossary

| Term | Brief definition |
|---|---|
| ADR | Architecture Decision Record — an immutable log of a design decision and its trade-offs |
| Bronze | Typed but source-native layer; mirrors API shape with light coercion |
| Content hash | SHA-256 of a deterministically serialized record, used to skip unchanged re-inserts |
| Correlation ID | A UUID attached to all log lines from one pipeline run for cross-line filtering |
| Dead letter queue | A queue for failed records pending manual review or retry; Kafka-style |
| Deep rescan | A full-history poll on a periodic schedule to catch edits missed by incremental extraction |
| Discriminator column | A column (e.g., `event_type`) that distinguishes subtypes in a shared table |
| dbt | Data Build Tool — SQL transformation framework with DAG, tests, and lineage |
| ELT | Extract-Load-Transform — raw load first, transform in destination warehouse |
| Entity resolution | Matching and deduplicating records that refer to the same real-world entity |
| ETL | Extract-Transform-Load — transform before loading into destination |
| `extra='forbid'` | Pydantic config that raises on unknown fields; catches API additions and renames |
| Free-tier headroom | Target <50% of a free tier at launch for multi-year growth runway |
| Gold | Pre-aggregated views and materialized views for serving-layer performance |
| Idempotency | A pipeline is idempotent if running it multiple times is equivalent to running it once |
| Landing | Raw byte-for-byte object storage layer; immutable audit trail |
| Lineage | The ability to trace a value in gold back to the raw payload that produced it |
| Medallion architecture | A bronze/silver/gold layering pattern for staged data curation |
| Quarantine | Routing failed records to a `_rejected` table rather than dropping them silently |
| Silver | Harmonized, validated, business-model-aligned data across all sources |
| Structured logging | Logging as JSON key/value pairs rather than free text |
| Unit of analysis | The entity that one row in the primary table represents |
| VCR cassette | A recorded HTTP interaction (request + response) replayed in tests for determinism |
| Watermark | A persisted cursor (last-seen timestamp or ID) enabling incremental extraction |
| `uv` | A Rust-based Python package/venv/version manager; replaces pip, pyenv, virtualenv |
