# Implementation Plan

This plan sequences the implementation of the decisions captured in ADRs 0001–0019. Each phase produces something deployable and testable; later phases build on earlier ones.

## Philosophy

- **Vertical slice first, then horizontal expansion.** Build one source end-to-end (bronze → silver → gold → dbt tests → CI) before cloning the pattern. A vertical slice stress-tests the architecture; horizontal expansion confirms it generalizes.
- **Ship working code with tests.** Every phase ends with green tests and a green CI run. No "implementation in progress for weeks" branches.
- **Defer what can be deferred.** Don't build frontends, statistical drift detection, or optional polish until the core pipeline is real. Premature scope creep kills portfolio projects.
- **Follow the ADRs.** They are the spec. If implementation reveals an ADR was wrong, update the ADR (or supersede it) before changing code.

---

## Phase 1 — Project scaffolding

**Goal:** a buildable, testable, deployable skeleton.

**Deliverables:**

- `pyproject.toml` with uv-managed dependencies (per ADR 0017)
- `.python-version` pinning Python 3.12
- `src/` directory structure per ADR 0012 (`extractors/`, `schemas/`, `bronze/`, `landing/`, `config/`)
- `tests/` skeleton per ADR 0015 (`unit/`, `integration/`, `e2e/`, `fixtures/cassettes/`, `conftest.py`)
- `dbt/` directory initialized with `dbt init` (per ADR 0011)
- `.pre-commit-config.yaml` per ADR 0018 (six hooks)
- `scripts/check_pydantic_strict.py` and `scripts/verify_cassette_scrub.py` (custom hooks)
- `.env.example` per ADR 0016
- `.envrc` template per ADR 0016 (uv-compatible `PATH_add .venv/bin` pattern)
- `alembic.ini` + `migrations/` for Postgres schema migrations
- Minimal GitHub Actions CI workflow: PR checks only (ruff, pyright, pytest on empty suite)
- `src/config/settings.py` with `pydantic-settings` `Settings` class (per ADR 0016)

**Quality gates:**

- `uv sync` completes cleanly
- `pre-commit run --all-files` runs clean
- `uv run pytest` passes (empty suite)
- PR-check CI workflow runs green on a trivial commit
- Branch protection on `main` configured (per ADR 0018)

---

## Phase 2 — Core infrastructure

**Goal:** the shared code that every extractor depends on.

**Deliverables:**

- `src/extractors/_base.py` — `Extractor` ABC with the 5-step lifecycle from ADR 0013 (extract → land_raw → validate → check_invariants → load_bronze)
- Three operation-type subclasses: `RestApiExtractor`, `FlatFileExtractor`, `HtmlScrapingExtractor` (concrete extractors inherit from one of these)
- `src/landing/r2.py` — R2 client wrapper for raw payload landing (per ADR 0004)
- `src/bronze/loader.py` — bronze loader with content hashing (ADR 0007) and quarantine routing (ADR 0013)
- `src/bronze/retry.py` — retry decorators via `tenacity` scoped to the lifecycle methods per ADR 0013
- `src/bronze/invariants.py` — the three starter business invariant checks (USDA bilingual, date sanity, null ID) per ADR 0013
- `src/config/logging.py` — structured JSON logging setup
- Alembic baseline migration: creates `_rejected` table shape and shared conventions
- Unit tests for every infrastructure component (per ADR 0015)

**Quality gates:**

- Unit test coverage of infrastructure: 100% (it's small and critical)
- `check_pydantic_strict` hook passes on any schemas declared so far
- Content hash is stable and deterministic across repeated runs
- Retry logic verified with mocked transient failures

---

## Phase 3 — First vertical slice: CPSC end-to-end

**Goal:** prove the architecture works against the simplest source before building four more.

CPSC is chosen first because it has no auth, clean nested JSON, and a stable event-level shape — minimum source-specific complexity. Any ABC flaws surface here cheaply.

**Deliverables:**

- `src/schemas/cpsc.py` — Pydantic bronze model with `ConfigDict(extra='forbid', strict=True)` per ADR 0014
- `src/extractors/cpsc.py` — `CpscExtractor(RestApiExtractor)` with CPSC-specific pagination, filter construction, and `LastPublishDate` incremental logic
- `config/sources/cpsc.yaml` — declarative config per ADR 0012
- Alembic migration: `cpsc_recalls_bronze` + `cpsc_recalls_rejected` tables
- VCR cassettes for 9 integration scenarios per ADR 0015 (happy path, multi-page, empty, rate limit, 5xx, etc.)
- Unit tests for CPSC Pydantic schema and parser logic
- Integration tests consuming the cassettes
- `.github/workflows/extract-cpsc.yml` with `workflow_dispatch` trigger (not yet on cron)
- First live extraction run, producing real bronze rows

**Quality gates:**

- All 9 integration scenarios pass
- Re-running the extractor produces no duplicate bronze rows (idempotency)
- Malformed-record scenario routes correctly to `cpsc_recalls_rejected`
- `workflow_dispatch` produces a successful run end-to-end
- Content hashes for a given record are stable across runs

---

## Phase 4 — Silver foundation from CPSC alone

**Goal:** prove the dbt transformation pipeline works end-to-end against one source before scaling to five.

**Deliverables:**

- dbt project initialized with `profiles.yml` pointing at Neon (per ADR 0005)
- `models/staging/stg_cpsc_recalls.sql` — view over CPSC bronze with type casting
- `models/silver/recall_event.sql` — initial silver model populated from CPSC staging only
- `models/silver/recall_product.sql` — populated from CPSC's nested Products collection
- `models/silver/firm.sql` — initial firm table (unresolved names from CPSC)
- `models/silver/recall_event_firm.sql` — M:N between events and firms with role
- Generic dbt tests per ADR 0015 on every silver model (not_null, unique, accepted_values, relationships)
- Two singular tests: orphan detection, per-source count baseline
- `models/gold/recalls_by_month.sql` — first gold view for dashboards
- `source freshness:` assertion on `cpsc_recalls_bronze`

**Quality gates:**

- `dbt build` succeeds (compile + run + test)
- All generic and singular tests pass
- Silver content spot-check via SQL confirms correct values
- Source freshness warning when bronze is older than 48h

---

## Phase 5 — Remaining extractors

**Goal:** complete source coverage.

Built in order of increasing complexity so earlier lessons inform later sources:

**5a. FDA iRES** (auth + signature cache-busting)

- Pydantic schema, extractor, YAML config, Alembic migration
- Handle Authorization-User/Key headers per ADR 0012
- Handle `signature=` cache-busting parameter
- `eventlmd` incremental logic
- 9 VCR scenarios + cron workflow

**5b. USDA FSIS** (bilingual dedup)

- Schema, extractor, YAML config, migration
- Bilingual edge case handled in `check_invariants()` per ADR 0006 + ADR 0013
- 9 VCR scenarios + cron workflow

**5c. NHTSA flat-file** (ZIP + tab-delimited + schema evolution)

- Flat-file extractor per ADR 0008
- Pydantic schema for 29-field tab-delimited row
- Schema-drift detection on unexpected fields (NHTSA has added fields before)
- Weekly cron workflow
- Large bronze table; test with realistic row counts

**5d. USCG scraping** (brittle source)

- `UscgScrapingExtractor(HtmlScrapingExtractor)` using BeautifulSoup
- Raw HTML archival to R2 (polite-scraper behavior)
- Schema drift on HTML structure changes raises `ValidationError`
- Weekly cron workflow

**Quality gates per source:**

- 9 integration scenarios pass
- Rejected records route correctly
- Source freshness assertion configured appropriately
- Real API / file / scrape extraction works end-to-end

---

## Phase 6 — Full silver + gold materialization

**Goal:** unified data model across all five sources.

**Deliverables:**

- Silver `recall_event`, `recall_product`, `firm`, `recall_event_firm` fed from all five sources' staging models
- Firm entity resolution: FDA's `firmfeinum` as the anchor per ADR 0002; fuzzy-match (RapidFuzz) across sources for non-FDA firms
- Full dbt test suite per ADR 0015 (60–80 generic tests + 5 singular + freshness)
- Gold: aggregate views for dashboards, denormalized search index
- `recall_event_history` view per ADR 0007 — FDA from native history tables + snapshot diffs for other four sources
- `scripts/re_ingest.py` — re-ingest CLI per ADR 0014 for schema-drift recovery
- Alembic migrations for all silver and gold tables

**Quality gates:**

- All dbt tests pass
- Firm resolution works on demonstrable cross-source examples (Honda, Tyson, etc.)
- Re-ingest command is idempotent (verified via repeat runs)
- History captures a simulated schema-drift event in an e2e test

---

## Phase 7 — Production CI and orchestration

**Goal:** production-grade automation.

**Deliverables:**

- All five per-source extract workflows on cron per ADR 0010 cadences
- Transform workflow (`dbt build` + `dbt test`) on time-shifted cron per ADR 0018
- Full PR-check workflow matching ADR 0018 (ruff, pyright, pytest unit + integration, dbt parse, 1–2 e2e smoke)
- Neon branching via the Neon API for integration-test DBs (per ADR 0015); `test_db_url` fixture in `conftest.py`
- `dbt docs generate` deploys to Cloudflare Pages on every main push
- Quarterly secret-rotation reminder workflow per ADR 0016
- Production secrets populated in GitHub Actions repo secrets

**Quality gates:**

- First full end-to-end production run (cron extracts + transform + docs deploy) succeeds
- Dashboards (once built) show real data
- PR-check pipeline runs under 10 minutes

---

## Phase 8 — Serving layer (FastAPI)

**Goal:** public API for recall data. Foundation for any frontend.

**Deliverables:**

- FastAPI project scaffolding in `src/api/`
- REST endpoints:
  - `GET /recalls` — list with filters (source, classification, date range, firm)
  - `GET /recalls/{source}/{recall_id}` — detail with products, firms, history
  - `GET /products/search` — by UPC / VIN / model for "is my product recalled?"
  - `GET /firms/{id}` — cross-source firm rollup
- OpenAPI spec auto-generated at `/openapi.json`
- API fixtures for testing (analogous to VCR cassettes for extractors)
- Deployment to Fly.io or Render free tier
- Read-only connection to Neon Postgres

**Quality gates:**

- API endpoints respond correctly against live silver/gold
- OpenAPI spec validates
- Response times acceptable for a personal-scale API

---

## Phase 9 — Frontend (optional for v1)

**Goal:** consumer-facing dashboard.

Deferred as a separate decision — depends on framework choice (Observable Framework, React+Recharts, SvelteKit), which deserves its own ADR when we get here. Not a v1 blocker; the project is complete and demonstrable with Phase 8.

**Candidate deliverables (to be scoped at that time):**

- Framework ADR (0020+)
- Dashboard MVP showing recall counts, classifications, firm rollups
- "Is my product recalled?" search UI
- Deployment to Cloudflare Pages or Vercel free tier

---

## Out of scope for v1

- **EPA integration** — deferred per ADR 0001
- **Statistical drift detection** — needs baseline data; add in v2 per ADR 0015
- **draw.io diagrams** — separate walkthrough (tracked in `TODO.md`)
- **Monitoring / alerting beyond GitHub Actions UI** — add if/when pipeline noise warrants
- **Authenticated API tier** — public read-only is sufficient for v1

---

## Tracking progress

Progress is tracked in this plan by checking off phase deliverables as they ship. A phase is not "done" until all its quality gates are green. New ADRs filed during implementation are linked from the relevant phase.

When implementation starts, use Claude Code's plan-mode feature or a TodoList per phase to track task-level progress within a phase.
