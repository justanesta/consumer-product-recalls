# Implementation Plan

This plan sequences the implementation of the decisions captured in the ADRs (`documentation/decisions/`, indexed in its README — the single authority on ADR numbering). Each phase produces something deployable and testable; later phases build on earlier ones. For the documentation conventions this plan follows (its role as the thin master index, the findings-vs-plan split, etc.), see `documentation/documentation_model.md`.

## Philosophy

- **Vertical slice first, then horizontal expansion.** Build one source end-to-end (bronze → silver → gold → dbt tests → CI) before cloning the pattern. A vertical slice stress-tests the architecture; horizontal expansion confirms it generalizes.
- **Ship working code with tests.** Every phase ends with green tests and a green CI run. No "implementation in progress for weeks" branches.
- **Defer what can be deferred.** Don't build frontends, statistical drift detection, or optional polish until the core pipeline is real. Premature scope creep kills portfolio projects.
- **Follow the ADRs.** They are the spec. If implementation reveals an ADR was wrong, update the ADR (or supersede it) before changing code.

---

## Phase 1 — Project scaffolding

**Goal:** a buildable, testable, deployable skeleton.

**Prerequisites:**

- Neon project provisioned with `main` and `dev` branches per ADR 0005's Neon branch conventions
- Cloudflare R2 buckets provisioned per ADR 0005, **one per environment** (R2 has no native branching, so dev/prod isolation is bucket-level): `consumer-product-recalls-dev` used by local `.env`, `consumer-product-recalls` used by GitHub Actions. Use separate per-bucket API tokens so a leaked dev token cannot reach the prod bucket.
- GitHub Actions repository secrets populated with `NEON_DATABASE_URL` (pointing at `main`), `R2_ACCOUNT_ID`, `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY`, `R2_BUCKET_NAME` (set to the prod bucket name) per ADR 0016 (FDA credentials follow in Phase 5a)
- Repository is public per ADR 0010 (unlocks unlimited GitHub Actions minutes) and ADR 0018 (branch protection relies on it)

**Deliverables:**

- `pyproject.toml` with uv-managed dependencies (per ADR 0017)
- `.python-version` pinning Python 3.12
- `src/` directory structure per ADR 0012 (`extractors/`, `schemas/`, `bronze/`, `landing/`, `config/`) plus `src/cli/` — Typer-based CLI entrypoint per ADR 0012 Implementation notes
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
- `src/extractors/_rest_api.py` — `RestApiExtractor` operation-type subclass (concrete extractors for CPSC in Phase 3 and FDA in Phase 5a inherit from this). The other two operation-type subclasses (`FlatFileExtractor`, `HtmlScrapingExtractor`) are **deferred to first use** per the "vertical slice first, then horizontal expansion" philosophy stated above — each is built in the phase that first needs it (Phase 5c and Phase 5d respectively), so its shape is informed by a real source rather than speculative design. Tracked as deliverables of those phases.
- `src/landing/r2.py` — R2 client wrapper for raw payload landing (per ADR 0004)
- `src/bronze/loader.py` — bronze loader with content hashing (ADR 0007) and quarantine routing (ADR 0013)
- `src/bronze/hashing.py` — canonical serialization + SHA-256 helper per ADR 0007 Implementation notes
- `src/bronze/retry.py` — retry decorators via `tenacity` scoped to the lifecycle methods per ADR 0013
- `src/bronze/invariants.py` — the three starter business invariant checks (USDA bilingual, date sanity, null ID) per ADR 0013
- `src/config/logging.py` — `structlog` configuration with `run_id` contextvar binding per ADR 0021, stdlib-logging bridge for third-party libraries (SQLAlchemy, httpx, tenacity, dbt)
- Alembic baseline migration: `_rejected` table shape, `source_watermarks` and `extraction_runs` per ADR 0020, and shared conventions
- Unit tests for every infrastructure component (per ADR 0015)

**Quality gates:**

- Unit test coverage of infrastructure: 100% (it's small and critical)
- `check_pydantic_strict` hook passes on any schemas declared so far
- Content hash is stable and deterministic across repeated runs — verified by round-trip determinism unit tests per ADR 0007 Implementation notes
- Retry logic verified with mocked transient failures

---

## Phase 3 — First vertical slice: CPSC end-to-end

**Goal:** prove the architecture works against the simplest source before building four more.

CPSC is chosen first because it has no auth, clean nested JSON, and a stable event-level shape — minimum source-specific complexity. Any ABC flaws surface here cheaply.

**Deliverables:**

- `src/schemas/cpsc.py` — Pydantic bronze model with `ConfigDict(extra='forbid', strict=True)` per ADR 0014
- `src/extractors/cpsc.py` — `CpscExtractor(RestApiExtractor)` with CPSC-specific filter construction and `LastPublishDate` incremental logic. (The CPSC API returns all matching records in one response — no pagination loop, which simplifies the extractor relative to other Phase 5 sources.)
- `config/sources/cpsc.yaml` — declarative config per ADR 0012
- Alembic migration: `cpsc_recalls_bronze` + `cpsc_recalls_rejected` tables
- VCR cassettes covering ADR 0015's integration matrix, tuned to CPSC's no-pagination shape. Recording strategy per scenario:
  - **Live-recorded** via `pytest --record-mode=rewrite`: happy path recent, happy path wide window, happy path narrow window, empty result. (Pagination-specific scenarios from ADR 0015 — single-page vs multi-page vs partial-last-page — do not apply to CPSC; those matter for paginated sources like FDA iRES in Phase 5a. See the Phase 5 standing requirement for the per-source shape guidance.)
  - **Live-recorded with a deliberately-bad credential**: 401 auth failure (applies to sources with auth; CPSC has none so 401 isn't produced for CPSC)
  - **Hand-constructed via `respx` (or hand-edited from a 200 cassette)**: 429 rate limit, 500 transient, malformed record in response — the live API won't return these on demand. Per ADR 0015, `respx` is the accepted pattern for explicit hand-constructed mock responses
  - **Shared with happy-path cassette**: content-hash dedup scenario reuses a happy-path cassette twice and asserts bronze row count does not grow — no separate cassette needed
- `bruno/cpsc/` — Bruno collection covering CPSC API endpoints; `.bru` request files are plain text and git-tracked. Includes an `environments/dev.bru` file that references credentials via `{{variables}}` rather than hardcoding them. Serves as living API documentation alongside the extractor. (Retroactively created at end of Phase 4 before Phase 5 begins.)
- Unit tests for CPSC Pydantic schema and parser logic
- Integration tests consuming the cassettes
- `.github/workflows/extract-cpsc.yml` with `workflow_dispatch` trigger (not yet on cron)
- `.github/workflows/deep-rescan-cpsc.yml` with `workflow_dispatch` trigger per ADR 0010's deep-rescan addendum (not yet on cron; cron turns on in Phase 7). The workflow calls a **separate method or extractor class** — not `CpscExtractor.extract()` — because the historical-seed code path has no incremental count guard and must handle arbitrarily large result sets. `CpscExtractor.extract()` is the incremental path only; it guards against unexpectedly large responses (`_MAX_INCREMENTAL_RECORDS = 500`) which would fire immediately if used for a full historical pull. See the Phase 5 standing requirement for how this split generalizes to all five sources.
- First live extraction run, producing real bronze rows
- **Empirical verification of `LastPublishDate` update semantics:** identify a recall that has been edited by CPSC since first publication (status change, remedy update, recalled-product count revision) and confirm by extraction whether `LastPublishDate` advanced at the edit. Document findings in `documentation/cpsc/`. **Closed 2026-05-01:** verification confirmed `LastPublishDate` does NOT advance on edits (bimodal gap distribution over 1,193 records, zero records between 8 days and 5 years). The deep-rescan workflow is now the **primary edit-detection mechanism** for CPSC, not an optional defense-in-depth net. ADR 0010 amended to reflect this. See `documentation/cpsc/last_publish_date_semantics.md`.

**Quality gates:**

- All integration scenarios pass (the per-source scenario count is tuned to the source's API shape; for CPSC this is 8 cassettes — 4 live + 4 hand-constructed — because pagination-specific scenarios and 401 auth don't apply)
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

Built in order of increasing complexity so earlier lessons inform later sources.

---

### Standing architectural requirement: incremental vs. historical load paths

Every source has two distinct code paths that must not be conflated:

- **Incremental path** (`<Source>Extractor.extract()`) — uses the watermark cursor (e.g. `LastPublishDateStart`, `eventlmd`, file modification date) to fetch only records changed since the last run. This path includes a response-count guard that raises `TransientExtractionError` if the result set exceeds a source-specific ceiling (e.g. `_MAX_INCREMENTAL_RECORDS = 500` for CPSC). The guard prevents a silently-ignored cursor parameter from loading the full database undetected.
- **Historical load path** (`deep-rescan-<source>.yml` workflow) — fetches all records in a date range for initial seeding or gap backfill. This path calls a **separate method or extractor class**, never `<Source>Extractor.extract()`, because it must handle arbitrarily large result sets and the incremental count guard would immediately fire. The historical path has no count guard.

This split was established for CPSC in Phase 3 (CPSC API behavior confirmed: an invalid or missing `LastPublishDateStart` parameter returns the full ~9,700-record dataset silently). Apply the same pattern for each source in Phase 5: FDA iRES, USDA FSIS, NHTSA, and USCG each need both an incremental extractor with a source-appropriate count guard and a separate historical load path without one.

---

### Standing architectural requirement: verify identity stability empirically before trusting source field descriptions

For every source, the bronze `identity_fields` choice is load-bearing for ADR 0007 dedup. A field that *claims* to uniquely identify a row may turn out to be a regen-time counter, a within-file-only identifier, or a coarser-grain identifier than the TSV row grain. Documentation wording can mislead — verify empirically before committing.

The minimum verification, before locking `identity_fields` for any new source:

- **Cross-regeneration stability.** Pull two regenerations of the source's data at least 24 hours apart. Pin a known logical row (e.g., a specific recall × make × model × year) in both regenerations using fields you trust as content-stable, and compare the candidate identity fields. Identity fields must be byte-identical for the same logical row across regenerations. Set-equality joins are safer than tuple joins for fan-out cases (avoid the cartesian-blowup that polluted Phase 5c's first attempt).
- **Within-snapshot row-uniqueness.** `count(*)` vs `count(distinct (identity_tuple))` against a single regeneration's load. A deficit means the candidate tuple is too coarse and the source emits multiple rows for the same identity — diagnose the residual collisions with a column-by-column distinct-count diagnostic to find the missing dimension or confirm byte-duplicates.
- **Byte-level fidelity.** Compare bronze rows to the raw source file for at least one non-trivial collision set, using the `extraction_runs.response_inner_content_sha256` (flat-file sources) or `response_body_sha256` (REST sources) capture as the byte-equivalence anchor. This rules out the extractor's parsing/decompression layer as a source of false differentiation or false collapse before any architectural conclusion is drawn.

This requirement was established in Phase 5c after NHTSA's `RECORD_ID` (RCL.txt field 1, documented as *"Running Sequence Number, Which Uniquely Identifies The Record"*) turned out to be a regen-time row counter. The original schema's docstring read the "uniquely identifies" qualifier as a stability claim, ignoring the "Running Sequence Number" prefix; verifying empirically would have caught this in Step 3 instead of after a 132,135-row bronze pollution. See ADR 0030 and `documentation/nhtsa/flat_file_observations.md` Findings K and L for the full evidence trail.

The same lesson applies to NHTSA's `RCL_CMPT_ID` (component-grain, repeats across rows within a recall) and to NHTSA's `(campno, make, model, year, compname)` 5-tuple (too coarse — fan-out across part numbers and across NHTSA-internal duplicates). Apply this verification to USCG (Phase 5d) and any future flat-file or HTML-scrape source. The diagnostic SQL pattern under `scripts/sql/nhtsa/bronze/` is reusable: per-source `verify_natural_key_candidate.sql` + `find_row_differentiator.sql` + `verify_<n>_tuple_identity.sql` was the sequence that resolved Phase 5c.

---

### Per-source workflow

Each sub-phase replicates the Phase 3 → Phase 4 pattern for its source: build and run the extractor first, then design cassettes from real evidence, then establish the silver layer before moving on. The five steps are ordered — each informs the next, and none should be skipped or resequenced.

**Step 1 — Source exploration**

For REST API sources (FDA, USDA): Bruno collection in `bruno/<source>/` with an `environments/dev.bru` file referencing credentials via `{{variables}}` — never hardcoded in `.bru` request files. Commit the collection alongside the extractor; `.bru` files are plain text and diff cleanly in git. Use `bru run bruno/<source>/` for quick scripted smoke tests from the terminal. The collection informs which cassette scenarios are worth recording and serves as living API documentation.

For flat-file and HTML sources (NHTSA, USCG): direct inspection of the download URL and response shape before writing the extractor. Document the observed format, update cadence, and any schema-drift history in `documentation/<source>/` before writing any code.

**Step 2 — Schema, extractor, YAML config, and Alembic migration**

Deliverables common to every source:

- `src/schemas/<source>.py` — Pydantic bronze model with `ConfigDict(extra='forbid', strict=True)` per ADR 0014
- `src/extractors/<source>.py` — incremental extractor + deep-rescan subclass
- `config/sources/<source>.yaml` — declarative config per ADR 0012
- Alembic migration: `<source>_recalls_bronze` + `<source>_recalls_rejected` tables
- `.github/workflows/extract-<source>.yml` with `workflow_dispatch`
- `.github/workflows/deep-rescan-<source>.yml` with `workflow_dispatch` per ADR 0010

**Step 3 — First extraction run and bronze data documentation**

Run the extractor against the live source and query the resulting bronze table to surface publication patterns, gap distributions, and any data shape surprises — the same analysis done for CPSC in `documentation/cpsc/last_publish_date_semantics.md`. Key questions to answer for each source: Does the incremental cursor field reliably advance on genuine edits? Are there batch/migration events that flood the watermark? What is the publication cadence and are there historical gaps in the database? Document findings in `documentation/<source>/`. These findings directly inform which cassette scenarios are worth recording in Step 4 and whether deep-rescan workflows can be relaxed or must be treated as the primary historical-load mechanism.

**Step 4 — Cassette suite design and recording**

Design and record **live-recorded** VCR cassettes after the first extraction, not before — real data surfaces schema surprises that hand-crafted mocks hide. Hand-constructed-via-`respx` cassettes for error paths (401/429/500/malformed-record) can land alongside, since those scenarios won't be served on demand by the live API. Phase 3 followed this pattern — live cassettes for happy paths, `respx` for error paths — and it generalizes to all five sources. The "after first extraction" guidance here applies to the live-recorded set; the error-path mocks are not gated by it.

**The scenarios recorded must be tuned to the source's actual API shape** — there is no universal 4-cassette matrix. The lists below are **starting heuristics, not prescriptions**:

- For paginated APIs (e.g., FDA iRES): single-page, multi-page, partial last page, empty.
- For non-paginated APIs (e.g., CPSC — one GET returns everything): recent, wide window, narrow window, empty. (Pagination-specific scenarios don't apply and recording them is busywork.)
- For flat-file downloads (e.g., NHTSA ZIP): one representative archive plus an intentionally-malformed variant. The "page" concept doesn't apply.
- For HTML scrapes (e.g., USCG): current-page HTML plus a structurally-drifted variant to exercise the scraper's failure mode.

**Trim the suite based on what the API actually does at the source's data volume**, not on what the projected matrix above suggests. The projected scenarios above were drafted before any source's empirical investigation; the real cassette suite must be designed against findings from Steps 1–3. Concrete example from FDA (Phase 5a): the projected matrix called for `single_page`, `multi_page`, and `partial_last_page` cassettes, but with FDA's measured ~20 records/day and `PAGE_SIZE=5000`, no realistic window paginated — all three cassettes ended up testing the same single-iteration code path, so two were deleted post-recording. See `documentation/fda/api_observations.md` finding O for the full reasoning. Apply the same critical-evaluation step to USDA/NHTSA/USCG: record what the matrix suggests, then audit each cassette's HTTP-call count, response shape, and code path against the others; delete or merge anything redundant before committing.

CPSC cassette recording revealed four schema bugs that hand-crafted respx mocks had hidden: a missing `SoldAtLabel` field, a missing `Caption` sub-field on images, a wrong alias casing (`InConjunctions` vs `Inconjunctions`), and a datetime string format difference. Treat cassette failures as schema bugs to fix, not test failures to skip.

**Step 5 — Silver dbt models**

Per-source silver pass before moving on to the next source:

- `models/staging/stg_<source>_recalls.sql` — staging view over the new bronze table with type casting and field normalization
- Extend `models/silver/recall_event.sql`, `recall_product.sql`, `firm.sql`, and `recall_event_firm.sql` to incorporate the new source's staging model
- dbt generic tests on the new staging model (not_null, unique, accepted_values, relationships)
- `source freshness:` assertion on the new bronze table

Phase 6 handles the work that genuinely requires all five sources to be present — firm entity resolution across sources, the `recall_event_history` snapshot model, gold aggregates, and the full dbt test suite.

---

### 5a. FDA iRES (auth + signature cache-busting) ✓

**Step 1 — Bruno exploration** ✓
- `bruno/fda/` — Bruno collection covering iRES endpoints (enforcement report list, single event detail, product history); `environments/dev.bru` stores `FDA_AUTHORIZATION_USER` and `FDA_AUTHORIZATION_KEY` as `{{variables}}`

**Step 2 — Schema, extractor, migration** ✓
- `FDA_AUTHORIZATION_USER` and `FDA_AUTHORIZATION_KEY` added to GitHub Actions repository secrets and local `.env` per ADR 0016
- Pydantic schema, extractor, YAML config, Alembic migration
- Handle Authorization-User/Key headers per ADR 0012
- Handle `signature=` cache-busting parameter — extractor injects a unique value (e.g. `int(time.time())` or `uuid.uuid4()`) into every request URL because the iRES server caches by full URL including `signature`. Without this, a 401 from a bad credential is cached and returned even after the credential is fixed; stale 200s also leak across rapid retries. The pattern is documented in `bruno/fda/lookup/get_product_types.yml` (the `docs:` block enumerates the four iRES quirks).
- `eventlmd` incremental logic
- **Pre-bronze ADR revisions (per `documentation/fda/api_observations.md` findings H, L, M) — completed 2026-04-26:**
  - **ADR 0007 textual correction (done):** dropped the `dt` suffix from `eventlmddt` / `productlmddt` references — actual API columns are `EVENTLMD` and `PRODUCTLMD`. Edited ADR 0007 in place with a revision note.
  - **ADR 0022 (filed):** supersedes ADR 0007's FDA-specific history path. FDA's native field-history endpoints are universally empty; FDA uses bronze-snapshot synthesis like the other four sources. See `documentation/decisions/0022-fda-history-endpoints-empty-snapshot-synthesis-for-all-sources.md`.
  - **ADR 0023 (filed):** supersedes ADR 0010's FDA no-rescan exemption. Archive migration re-touches old records wholesale; FDA needs a weekly `deep-rescan-fda.yml` workflow matching CPSC/USDA posture. See `documentation/decisions/0023-fda-deep-rescan-required-archive-migration-detected.md`.

**Step 3 — First extraction and bronze findings** ✓
- **API identity check:** confirmed `iRES_enforcement_reports_api_usage_documentation.pdf` and `enforcement_report_api_definitions.pdf` describe the same API (2026-04-26).
- **Empirical verification of `eventlmddt` edit semantics:** confirm via the documented `productHistory` / `eventproducthistory` endpoints that edits produce an advanced `eventlmddt` and corresponding history rows. FDA docs claim this explicitly; the check is to trust-but-verify before relying on it in production.

**Step 4 — Cassettes** ✓
- 4 live-recorded cassettes + hand-constructed error-path tests (see `tests/integration/test_fda_live_cassettes.py`)
- **Custom VCR request matcher required for FDA**: cassettes must match on path + method + filtered query params, with `signature` excluded from the match (or stripped before comparison). Without this, every replay attempt fails because the recorded `signature` value will never match the timestamp/UUID generated at replay time. Implemented in `tests/integration/test_fda_live_cassettes.py` via module-level `vcr_config` override with `filter_query_parameters: ["signature"]`.

**Step 5 — Silver** ✓ (shipped 2026-05-09; FDA `description` field-mapping correction deferred to the `feature/silver-field-remap` PR per `archive/phase-6-execution-plan.md` § Sequencing Constraints)
- `models/staging/stg_fda_recalls.sql` + extend silver models to incorporate FDA

---

### 5b. USDA FSIS (bilingual dedup)

**Step 1 — Bruno exploration** ✓
- `bruno/usda/` — Bruno collection covering FSIS recall endpoints; `environments/dev.bru` for auth parameters (none required — unauthenticated public API)

**Step 2 — Schema, extractor, migration** ✓
- Pydantic schema, extractor, YAML config, Alembic migration
- Bilingual edge case handled in `check_invariants()` per ADR 0006 + ADR 0013 — Spanish records without an English sibling are quarantined
- `.github/workflows/deep-rescan-usda.yml` with `workflow_dispatch` trigger per ADR 0010
- ETag conditional-GET optimization implemented but disabled by default (`etag_enabled=False`) pending multi-day reliability evidence — see Finding N in `documentation/usda/recall_api_observations.md`
- Browser-like UA + Accept headers required to pass Akamai Bot Manager — see Finding O

**Step 3 — First extraction and bronze findings** ✓
- **Empirical verification of `field_last_modified_date`:** field exists and is stored in bronze, but cannot be used as a server-side filter (both naming variants silently ignored — Finding D). 42.2% of records have no value (Finding C). Full-dump extraction is the only viable strategy. Document any findings about whether the field reliably advances on edits in `documentation/usda/`.

**Step 4 — Cassettes** ✓
- 2 live-recorded cassettes + 7 hand-constructed tests (see `tests/integration/test_usda_live_cassettes.py`)
- No custom VCR matcher needed — USDA has no auth headers or cache-busting params

**Step 5 — Silver** ✓
- `models/staging/stg_usda_fsis_recalls.sql` filters `langcode='English'` (EN-primary, ES dropped from silver but retained in bronze for audit) — minimal interpretation of the original "EN as primary, ES as companion" plan. Bilingual JSONB companion sidecar deferred until a downstream consumer needs it.
- Silver models extended: `recall_event`, `recall_product`, `firm`, `recall_event_firm` all gained a `usda_*` CTE. `published_at` coalesces `last_modified_date` → `recall_date` (last_modified_date is 42% null per Finding D). USDA `establishment` flows into `firm.sql` with **role='establishment'** (new role value) and into `recall_event_firm` accordingly. `_silver.yml` `accepted_values` extended on both `source` (`['CPSC','FDA','USDA']`) and `role` (added `'establishment'`).
- `recall_product` emits one row per USDA recall event (recall_product_id = recall_event_id) — `product_items` is unstructured per ADR 0002 deferral.
- USDA singular floor test added at `dbt/tests/assert_usda_row_count_sane.sql` (floor: 1,000 events).

---

### 5b.2. USDA FSIS Establishment Listing API — recall enrichment ✓ (shipped — extractor + bronze + establishment silver join landed; sub-steps below)

Functionally a sixth source. The FSIS Establishment Listing API
(`/fsis/api/establishments/v/1`, 7,945 records, weekly Mon/Tue cadence, no
auth, no pagination; ETag presence under re-investigation — Finding A
originally claimed absent, but 2026-05-03 production capture observed
`etag` and `last-modified` populated, see the "USDA recall ETag
re-evaluation" follow-up below — see `documentation/usda/establishment_api_observations.md`)
provides demographic + geolocation data for FSIS-regulated establishments.
Pre-extraction Bruno exploration is complete (collection in
`bruno/usda/establishment_exploration/`). Steps mirror the standard 5-step
per-source workflow:

1. Bruno exploration — done.
2. Schema (`src/schemas/usda_establishment.py` with `false`-sentinel handling
   for `geolocation` / `county` per Finding C and array-whitespace stripping
   for `activities` / `dbas`), extractor (`UsdaEstablishmentExtractor`),
   `config/sources/usda_establishments.yaml`, Alembic migration
   (`usda_fsis_establishments_bronze` + rejected table), `extract-usda-establishments.yml`
   workflow with `workflow_dispatch` and weekly cron.
3. First extraction + bronze findings: measure overlap between recall
   `establishment` and establishment `establishment_name` / `dbas`. The
   coverage gap from Finding F (1:1 join confirmed on a single record) needs
   broad-spectrum verification before committing the silver join shape. Probe:
   ```sql
   with recall_names as (
       select distinct upper(trim(establishment)) as nrm
       from stg_usda_fsis_recalls
       where establishment is not null and trim(establishment) <> ''
   ),
   est_names as (
       select distinct upper(trim(establishment_name)) as nrm
       from stg_usda_fsis_establishments
   )
   select count(*) as total, count(est_names.nrm) as matched
   from recall_names left join est_names using (nrm);
   ```
   Document in `documentation/usda/establishment_join_coverage.md`.
4. Cassettes: one full-dump cassette + one quoted-name-filter cassette;
   pagination scenarios don't apply.

**Step 4.5 — Bronze normalization refactor (ADR 0027) — gates Phase 5c.**
Before writing the establishment silver staging model in Step 5, refactor
the affected bronze schemas (FDA, USDA recall, USDA establishment — CPSC
already conformant per audit) per ADR 0027 — bronze keeps storage-forced
transforms only, value-level normalization moves to silver staging. Doing
this between Steps 4 and 5 means the establishment silver staging model is
written once with the new pattern rather than rewritten afterward, and
NHTSA/USCG inherit the corrected pattern from day one.

Same PR also lands two supporting artifacts required by the production
re-baseline playbook (`documentation/operations/re_baseline_playbook.md`):

- Alembic migration adding `extraction_runs.change_type TEXT NOT NULL DEFAULT 'routine'` (allowed values: `routine`, `schema_rebaseline`, `hash_helper_rebaseline`).
- CLI flag `recalls extract <source> --change-type=<value>` in `src/cli/main.py`, defaulting to `routine`. The first re-extract per refactored source uses `--change-type=schema_rebaseline` to mark the wave.

Expected re-baseline waves: FDA (medium), USDA recall (medium), USDA
establishment (small ~14% second wave). CPSC: none. Acceptable on dev;
the production-side gates (PR template, CI guard) land in Phase 7 before
cron turn-on. This is the only refactor in the plan that gates a downstream
phase — positioned here precisely to prevent the inconsistency from
propagating to NHTSA and USCG.

5. Silver: `stg_usda_fsis_establishments` staging view; extend `firm.sql` to
   populate `observed_company_ids` for USDA rows with the FSIS
   `establishment_id` (matched on normalized name; **HTML-entity decode the
   recall side first** — per `establishment_join_coverage.md`, the recall API
   returns names with `&#039;` and `&amp;` while the establishment API
   returns plain text, accounting for ~80% of unmatched names; fixing this
   takes the per-distinct-name match rate from 82.85% → ~97%). Skip DBA
   fallback at the staging-join layer (probe Q3 confirmed zero additional
   matches). Defer fuzzy matching to Phase 6 firm entity resolution. Optional:
   add a `firm_establishment_attributes` silver dim for address/geolocation/FIPS.

Best landed before or alongside Phase 6 firm entity resolution work — the
Establishment ID is the strongest cross-source FSIS firm anchor (analogous to
FDA's FEI per ADR 0002).

---

### 5c. NHTSA flat-file (ZIP + tab-delimited + schema evolution)

> **Schema follows ADR 0027** — bronze does storage-forced transforms only;
> value-level normalization (empty-string sentinels, whitespace, etc.) lives
> in `stg_nhtsa_recalls.sql`, not in `src/schemas/nhtsa.py`.

**Step 1 — Source exploration** ✓
- Direct inspection of the NHTSA recall ZIP download URL before writing the extractor. Key questions: How often does NHTSA release a new ZIP vs update an existing one? Does the file modification date reliably reflect content changes or just re-packaging? Document in `documentation/nhtsa/`.

**Step 2 — Schema, extractor, migration** ✓ (revision landed per ADR 0030, 2026-05-08)
- `src/extractors/_flat_file.py` — `FlatFileExtractor` operation-type subclass of the `Extractor` ABC (deferred from Phase 2 to its first use here). Shape is informed by NHTSA: ZIP download → stream-decompress → row-by-row parse → bronze load. Unit-tested in isolation before `NhtsaExtractor` lands on top of it.
- `NhtsaExtractor(FlatFileExtractor)` per ADR 0008
- Pydantic schema for 29-field tab-delimited row
- Schema-drift detection on unexpected fields (NHTSA has added fields before)
- Weekly cron workflow
- Large bronze table; test with realistic row counts
- **Post-bronze identity-and-dedup revision (per ADR 0030 / Findings K and L) — landed 2026-05-08:** Step 3 first-extraction analysis surfaced two source-shape findings the original schema/extractor mishandled: NHTSA's RECORD_ID is a regen-time row counter (Finding K), and the TSV ships byte-duplicate rows (Finding L). Implementation went through two iterations — initial 7-tuple ADR proposal on 2026-05-07, widened to 11-tuple on 2026-05-08 after `scripts/nhtsa/tsv_analysis/identity_search.py` surfaced an 822-anomaly residue across the full POST_2010 corpus that the bronze-narrow analysis missed. Final shape:
  - **Identity tuple:** `BronzeLoader` instantiation in `load_bronze()` uses the 11-tuple `("campno", "maketxt", "modeltxt", "yeartxt", "compname", "rcl_cmpt_id", "mfr_comp_ptno", "mfr_comp_desc", "mfr_comp_name", "endman", "bgman")`. Row-unique on POST_2010 modulo 987 byte-duplicate groups (handled by within_batch_dedup); harmless on PRE_2010 (4 added fields are constant-empty for all pre-2010 rows).
  - **Hash exclusion:** `hash_exclude_fields=frozenset({"source_recall_id"})`. RECORD_ID's regen-time instability doesn't pollute content_hash; `source_recall_id` stays on bronze rows for audit/lineage but isn't load-bearing for dedup.
  - **Within-batch dedup:** `within_batch_dedup=True`. New `BronzeLoader._dedup_within_batch()` method collapses `(identity, hash)` duplicates within a batch and raises `WithinBatchIdentityCollisionError` on same-identity-different-hash (defensive).
  - **Allow null identity:** `allow_null_identity=True`. Four of the 11 identity fields (`bgman`, `endman`, `mfr_comp_desc`, `mfr_comp_name`) are legitimately empty for many rows; the flag treats `None`/`""` as a valid identity bucket rather than raising the safety check that protects CPSC/FDA/USDA from missing-required-field bugs.
  - **Loader-side SQL fixes** (generic, in `src/bronze/loader.py`): (a) text-canonical IN comparison via `_identity_text_expr` so empty-string identity values bind cleanly into TIMESTAMPTZ parameter slots (`bgman`/`endman` would otherwise return `DataError: invalid input syntax for type timestamp with time zone: ""`); (b) chunked existing-hash lookup at ~5,400 keys per query so the 11×65k composite IN clause stays under Postgres' bind-parameter ceiling and planner memory (~723k params would otherwise return `OperationalError`).
  - Schema and extractor docstrings rewritten to reflect the 11-tuple and reference ADR 0030.
  - Bronze-table cleanup before re-extracting on dev: `truncate table nhtsa_recalls_bronze, nhtsa_recalls_rejected`. The 132,135 polluted rows from the original May 5 + May 7 loads (under the broken `source_recall_id`-as-identity scheme) discarded; `extraction_runs` history retained for audit.
  - Tests verify byte-duplicate collapse, cross-regen dedup-on-rerun (no new insert when re-extracting the same data), the defensive `WithinBatchIdentityCollisionError`, the `allow_null_identity` flag's accept-empty + raise-on-empty-when-off behavior, and the full 11-tuple + flag set asserted on the NHTSA extractor's BronzeLoader call.
  - **Empirical end-to-end validation:** post-implementation, `recalls extract nhtsa --since 2024-01-01` lands 65,732 rows on a fresh extract; a re-run inserts 0 duplicates of the existing 19 May-2026 rows; `--since 2023-12-01` adds only 6,343 net-new December 2023 rows on top. `scripts/sql/nhtsa/bronze/verify_eleven_tuple_row_unique.sql` returns `excess_rows = 0`, confirming the 11-tuple is row-unique on what landed.

**Step 3 — First extraction and bronze findings** ✓
- After first extraction, document publication cadence, whether the modification date watermark is reliable, and any schema surprises in `documentation/nhtsa/`. Findings K and L (2026-05-07) are the bronze-shape surprises and are documented in `documentation/nhtsa/flat_file_observations.md`; their architectural response is ADR 0030 and is slotted into Step 2 above. Watermark-reliability verdict (Finding H sub-question) and publication-cadence characterization remain open and close as a side-effect of post-revision daily extracts logging `inner_content_sha256` to `extraction_runs` (per Finding J's mandate).

**Step 4 — Cassettes** ✓
- **One** live-recorded happy-path cassette (`test_happy_path_full_dump.yaml`) for the incremental POST_2010 path. Deliberately deviates from CPSC/USDA precedent: real S3 response headers + a synthetic 1.8 KB body (the existing `tests/fixtures/nhtsa/sample_recalls.zip`) rather than the live ~14 MB body. The continuous schema-evolution archive role is already filled by `scripts/nhtsa/probe_watermarks.sh` + `documentation/nhtsa/watermark_probes.jsonl`, which captures HEAD + body SHA-256 across all 15 NHTSA URLs daily — more comprehensive than any single cassette. The cassette here covers test-time HTTP-client integration only. Recording + hand-edit procedure documented in the test file's docstring and `tests/fixtures/cassettes/nhtsa/README.md`. The original "intentionally-malformed variant" is dropped as redundant — malformed-zip handling is fully covered by unit tests against the same fixture (`tests/extractors/test_nhtsa_extractor.py`). Error paths (429, 500) are hand-constructed via `respx`-style patching in the integration test, mirroring `tests/integration/test_fda_live_cassettes.py:195-219`. ADR 0014 + ADR 0015 cassette framing applied per ADR 0031's "Tier 2 detection" / "complementary archive" pattern.

**Step 5 — Silver** ✓
- `dbt/models/staging/stg_nhtsa_recalls.sql` (new) + `dbt/models/staging/stg_nhtsa_recalls.yml` (new) — latest-per-11-tuple projection over bronze. NHTSA source added to `dbt/models/staging/_sources.yml`.
- `dbt/models/silver/recall_event.sql` (modified) — `nhtsa_events` CTE; `source_recall_id = campno`, `recall_event_id = md5('NHTSA' || '|' || campno)`. DISTINCT ON (campno) collapses many-rows-per-recall to event grain.
- `dbt/models/silver/recall_product.sql` (modified) — `nhtsa_products` CTE; `recall_product_id = md5(11-tuple)` per ADR 0031 option 3b. Each batch is its own product row; v1 fragmentation rate ~0.0004%/day documented as a known limitation.
- `dbt/models/silver/firm.sql` (modified) — `nhtsa_normalized` CTE for `mfgname` → manufacturer firms (`company_id` NULL — no NHTSA analog to FDA's firmfeinum).
- `dbt/models/silver/recall_event_firm.sql` (modified) — NHTSA bridge for (campno, mfgname) → firm via normalized name match.
- `dbt/models/silver/_silver.yml` (modified) — added `'NHTSA'` to `accepted_values` for `recall_event.source` and `recall_product.source`.
- New ADR 0031 (`documentation/decisions/0031-silver-row-fragmentation-strategy.md`) — three-tier framing (prevention / detection / reconciliation) for silver-row fragmentation across all sources; per-source surrogate + fragmentation profile; Phase 6 reconciliation thresholds. Forward-references ADR 0002 for firm-level RapidFuzz; clarifies that ADR 0002 is firm-scope only and product-level fragmentation is this ADR's scope.
- ADR 0030 amended with cross-reference to ADR 0031 (silver companion).
- `documentation/nhtsa/incremental_delta_findings.md` Section G updated with full-corpus 9-tuple validation result, cross-corpus drift evidence (AC DELCO maketxt normalization, 1 case in 240k rows over 1 day = ~0.0004%/day), and the option 3b silver decision.
- New SQL `scripts/sql/nhtsa/_pipeline/inner_content_cadence.sql` — focused Finding H Q1 closure mechanism querying `extraction_runs.response_inner_content_sha256` for day-over-day transitions.

---

### 5d. USCG scraping (brittle source)

> **Schema follows ADR 0027** — bronze does storage-forced transforms only;
> value-level normalization lives in `stg_uscg_recalls.sql`, not in
> `src/schemas/uscg.py`.

**Step 1 — Source exploration** ✓ landed 2026-05-16 on branch `feature/uscg-exploration-schema-extractor-migration`
- Direct inspection of the USCG target HTML before writing the scraper. Findings A-N captured in `documentation/uscg/scraping_observations.md`: static HTML / table-based listing / 71 paginated pages × ~25 rows = 1,763 records / 6 listing fields + 13 details fields / two date formats (`YYYY-MM-DD` listing, `M/D/YYYY` details) / no `robots.txt` / no `Last-Modified` / no `ETag` / details-page render byte-stable across consecutive fetches / pagination boundary returns empty placeholder row. Two findings deferred to Step 1.5 (corpus-wide `source_recall_id` uniqueness, year-prefix invariant) — surface as a one-shot script after Step 2's extractor lands.

**Step 2 — Schema, extractor, migration** ✓ landed 2026-05-16 on branch `feature/uscg-exploration-schema-extractor-migration`
- `src/extractors/_html_scraping.py` — `HtmlScrapingExtractor` ABC promoted from `_base.py` stub. Polite-scraper throttling (sleep-before-fetch, `scrape_delay_seconds=1.0` default), per-fetch tenacity retry budget (3 attempts × short backoff so a transient 503 mid-walk doesn't restart the full 1,834-fetch walk), single-NDJSON-per-run R2 artifact via `land_raw`, page-0-only forensic capture. Unit-tested in isolation via `tests/extractors/test_html_scraping_base.py` with a minimal `_TestSubclass` fixture stubbing the abstract methods.
- `UscgScrapingExtractor(HtmlScrapingExtractor)` — concrete subclass with BeautifulSoup (`lxml` backend) parsing of both listing + details. Pagination boundary detected via empty-`id` query parameter (Finding L). Drift fences: listing-page table headers validated against `expected_columns` (`config/sources/uscg.yaml`), details-page labels validated against `_DETAILS_LABEL_MAP`; both raise `TransientExtractionError` on mismatch. USCG-specific year-prefix invariant added (`source_recall_id[:2]` vs `opened_on.year % 100`). `UscgDeepRescanLoader` kept for symmetry with NHTSA/FDA/USDA — same fetches, skips `_touch_freshness`.
- Raw HTML archival to R2: single NDJSON-per-run artifact, every fetched page's body+envelope serialized as one line, `application/x-ndjson` content-type. Re-ingest reads one R2 file. Migration 0013 adds `uscg_recalls_bronze` + `uscg_recalls_rejected`. `pyproject.toml` adds `beautifulsoup4>=4.12,<5` + `lxml>=5.0,<6` (project version stays at 0.6.0 on the branch — bumped on the merge-to-main PR per the parallel-branch version-coordination convention).
- Schema drift on HTML structure changes raises `TransientExtractionError` at parse time (drift fence aborts the run cleanly) AND `ValidationError` at Pydantic time via `ConfigDict(extra='forbid', strict=True)` (catches additive drift not surfaced by the parser).
- Weekly cron workflow — deferred to Step 5d post-Step-4 (cassette suite) per the user's branch sequencing.

**Step 3 — First extraction and bronze findings** (partially landed 2026-05-17 on the same branch)
- ✓ Ran `recalls deep-rescan uscg --change-type=historical_seed` — 1,763 fetched, 1,512 bronze inserts on first run, 251 quarantined (14.2% rejection rate). Run aborted on threshold but bronze + rejected rows persisted (transaction commits before threshold check).
- ✓ Investigated rejections via `scripts/sql/uscg/bronze/explore_first_extraction.sql` + `diagnose_rejections.sql` + R2 inspection via `scripts/uscg/inspect_landing_ndjson.py`. Six findings landed in `scraping_observations.md`:
  - **Finding G (replaced)**: year-prefix invariant falsified across ≥4 distinct mechanisms (fiscal year, prefix=year−1, multi-year re-issues, Unix-epoch sentinel). Invariant removed from `src/extractors/uscg.py`.
  - **Finding O**: listing-side Unix-epoch sentinel — USCG renders `1970-01-01` literally in the Opened On column when no date is known, while the details page leaves Case Open Date empty. Same logical semantic, two encodings; silver `stg_uscg_recalls.sql` will map `1970-01-01` → NULL.
  - **Finding P**: `company_name` corpus-nullable (33/1763 ≈ 1.9% empty, mostly pre-2005 historical entries). Schema + migration updated to allow nulls.
  - **Finding Q (minor)**: listing pages contain occasional non-UTF-8 bytes; production parser handles via BeautifulSoup's encoding auto-detect; forensic inspector hardened with `errors="replace"`.
  - **Finding R** (silver-layer): `disposition` value case-inconsistency — `Closed`/`Open`/`CLOSED`/`OPEN` all observed (1476/190/95/2 split). Bronze stores verbatim per ADR 0027; silver staging normalizes.
  - **Finding S** (Phase 6 entity-resolution implication): 23 bronze rows have BOTH `mic`=NULL AND `company_name`=NULL → no firm anchor at all. Three silver treatments enumerated; recommend option 3 (soft-fail with `firm_id=NULL`). Decision deferred to Phase 6 silver landing.
- ✓ Step 1.5 corpus probe partially resolved: year-prefix invariant probe folded into Step 3's findings above (Finding G replaced). `source_recall_id` uniqueness probe still deferred.
- Re-extraction pending after Step 3 fixes — predicted outcome: 0% rejection rate, run completes with `status="success"`, ~1,763 bronze inserts.
- Re-evaluate items #4 and #9 (see "Architectural follow-ups" below) — still deferred; pragmatic-capture path for `extraction_runs.response_*` columns held up cleanly (`response_etag` + `response_last_modified` correctly NULL per Finding K; no operational pain from sparsity yet).

**Step 4 — Cassettes** (done 2026-05-18)
- ✓ Cassette recording means capturing the real scraped HTML structure (not a hand-crafted fixture), since HTML schema drift is the primary failure mode. Record current-page HTML + a structurally-drifted variant to exercise the scraper's failure path.

**Step 5 — Silver** (done 2026-05-18)
- ✓ `models/staging/stg_uscg_recalls.sql` + extend silver models to incorporate USCG

**Step 6 — USCG Finding J short-circuit enhancement** (done 2026-05-18) ✓

**Goal:** drop USCG steady-state run cost from ~36 min (the current always-full-fetch design) to ~3 sec (one HTTP request + one DB lookup), enabling a cadence shift from weekly → daily aligned with the other sources. Without this, USCG dominates total pipeline runtime by ~90%.

**Algorithm — two-part short-circuit check, applied at the top of `UscgScrapingExtractor.extract()`:**

1. **Count check**: fetch page 0 only. Parse `Records Found: NNNN`. Compare against the value from the last successful run.
2. **Listing-row check**: every recall ID visible in page 0's data rows already exists in `uscg_recalls_bronze` (single `WHERE source_recall_id = ANY(:ids)` lookup, ~25 IDs).

If BOTH pass → short-circuit. `extract()` returns empty list. `_touch_freshness()` still updates `last_successful_extract_at` so monitoring sees the run as successful. `_record_run()` writes a `status="success"` row tagged with the short-circuit (new `change_type` value `"records_count_skip"` or boolean flag column on `extraction_runs`).

If EITHER fails → fall through to the existing full 1,834-fetch walk. Correctness preserved either way.

**Where the "previous count" lives**: either a new `last_records_count` field on `source_watermarks` (schema migration), OR derived at query-time from the most recent `extraction_runs` row's metadata (no migration, slightly more SQL each run). Pick the simpler one at implementation time.

**Failure modes + mitigations:**

- **Details-only edit on an existing recall**: USCG amends a details-page field (e.g., `Disposition: Open → Closed`) without touching the listing row. Neither short-circuit signal catches it. Mitigation: weekly operator-triggered `recalls deep-rescan uscg --change-type=schema_rebaseline` as a periodic safety net — runs the full walk, catches anything the short-circuit missed.
- **Stale count + reshuffled listing**: USCG removes recall X and adds recall Y on the same run. Total count unchanged. New ID Y appears on page 0 → listing-row check fails → short-circuit correctly skipped. Caught by check (2).
- **Listing reorders without content changes**: page 0's recall IDs might shift between runs due to sort order. The listing-row check tolerates this (set membership, not order).

**New observability surface needed:**
- `extraction_runs` should distinguish short-circuited runs from full-walk-with-0-inserts runs. Either: (a) new `change_type` value `"records_count_skip"`, (b) new boolean column `was_short_circuited`, or (c) a new `extraction_runs_skip_reason` text column. Pick at implementation time.
- Dashboards should be able to answer "when did USCG last do a full walk?" — important for confirming the safety-net cadence is being honored.

**Cadence change enabled:**
- Move USCG cron from weekly → daily. Steady-state cost: ~21 sec/week (7 days × ~3 sec). vs current ~36 min/week.
- Schedule weekly deep-walk (cron-triggered `recalls deep-rescan uscg --change-type=schema_rebaseline`) as the safety net.

**Pre-conditions:** Step 4 (cassettes) + Step 5 (silver staging) land first so the short-circuit can be exercised against recorded flows + verified with downstream silver tests. Order: Step 4 → Step 5 → Step 6.

**Implementation surfaces (touch list):**
- `src/extractors/uscg.py`: new `_short_circuit_eligible()` helper; modify `extract()` to call it before the walk.
- `src/extractors/uscg.py` or `_record_run`: persist the new short-circuit signal to `extraction_runs`.
- New Alembic migration: `last_records_count` column on `source_watermarks` (if option (a)) or new column on `extraction_runs` (if option (c)).
- `src/cli/main.py`: update `_LOOKBACK_NO_OP_MESSAGES["uscg"]` text to reflect short-circuit behavior.
- `tests/extractors/test_uscg_extractor.py`: new test class for short-circuit eligibility + the fall-through path.
- `documentation/uscg/scraping_observations.md` Finding J: add a Step-N postscript documenting the empirical short-circuit hit-rate after a few weeks of daily runs.

**Step 7 — USCG manufacturer-directory ingestion** (done 2026-05-30, branch `feature/phase-5d-uscg-manufacturers`) ✓

Sibling non-recall source `uscg_manufacturers` ingested via `HtmlScrapingExtractor` from `https://uscgboating.org/content/manufacturers-identification.php` (16,263 manufacturer records, 651 pages × ~25 rows, listing-only extraction per `manufacturer_scraping_observations.md` Finding C). Mirrors the USDA `recalls + establishments` design pattern: separate extractor + schema + bronze + staging + silver attributes table (`firm_manufacturer_attributes`), plus a directory LEFT JOIN in `firm.sql` USCG branch (and `recall_event_firm.sql` USCG branch in lockstep) that rescues 5 of the §3 Bug 3 mic-only-no-name rows and provides general canonical-name enrichment for ~18 additional USCG firms. Cross-source recall→directory coverage: 99.44% (Phase 6b firm-resolution gains a richer anchor). Plan: `project_scope/archive/phase-5d-uscg-manufacturers.md`. Empirical observations: `documentation/uscg/manufacturer_scraping_observations.md`. Audit fold-in: `documentation/uscg/field_audit_2026_w22.md` §3 Bug 3 update + §6 update.

**Follow-up (2026-05-30) — MIC reassignment is real; listing-only extraction does not capture it.** The first incremental run surfaced two MIC reassignments (`AXY`, `COP`) and resolved `manufacturer_scraping_observations.md` Open Question #3 (see new §M): the MIC is a stable regulatory anchor (= first 3 chars of a hull's HIN per 33 CFR 181) but its company/address attributes change as USCG recycles the code from an out-of-business builder to a new one. Two consequences for this source:

- **Listing-only (Path A) discards the lineage.** The detail page (`manufacturers-identification-detail.php?id=N`, confirmed to answer a direct GET) carries `Past Company 1–3 (OOB year)`, `In Business`, `Parent MIC`, `DBA`, and **`Date Modified`** — none captured by migration 0015. A **Path B detail-enrichment pass** (or a `Date Modified`-cursored incremental) would capture the source-native succession history and a far better change signal than re-walking + re-hashing 16k listing rows. Tracked as a candidate in the Phase 6 cross-source SCD-2 item below.
- **Reassignment-rate probe — RUN 2026-05-30 (decisive: build Path B).** Recall-directed (`--recalled-only`, 714/718 recalled MICs) found **51.1% (365) with a prior company and 28.7% (205) `(OOB)`-recycled** — vs 28.1%/17.3% for a random 1,000-MIC sample, i.e. recalled MICs are ~1.8× more reassigned (recall and reassignment correlate, not independent). Affected-MIC list: `data/exploratory/uscg_manufacturers/recalled_reassigned_mics.json`; full numbers + caveats in `manufacturer_scraping_observations.md` §M.6. As-originally-scoped — Goal: decide whether Path B + a time-aware recall↔manufacturer join is worth building. Method: random-sample ~1,000 detail-page ids in `[1, 16263]` (≈±3% at 95% CI; ~18 min at the 1 s polite throttle), parse `Past Company 1–3` / `OOB` / `In Business` / `Date Modified` / `Parent MIC`, and report (a) % of records with ≥1 Past Company, (b) OOB-year distribution (how recent are reassignments), (c) % with a `Parent MIC` (corporate succession vs pure code recycling). **Bridge metric:** join the sampled reassigned MICs against `uscg_recalls_bronze.mic` to size the *actual* misattribution surface in our own corpus. Decision gate: if a material fraction of recalled MICs are reassigned, build Path B + SCD-2 + the HIN-build-date join; if negligible, document as a known minor limitation and keep the "current MIC holder" silver semantic. Suggested home: `scripts/uscg/probe_mic_reassignment_rate.py` (mirrors `scripts/uscg/inspect_landing_ndjson.py`; polite throttle + response cache so re-runs don't re-fetch).
- **Branch scope when this is built.** The detail-capture feature branch (`feature/uscg-manufacturers-detail-addition`) is **bronze-capture only**: Alembic migration (`uscg_manufacturer_details_bronze` + rejected table), `UscgManufacturerDetailExtractor` (promotes the validated probe parser into a production `_parse_details_page` with a raise-on-unknown-label drift fence) + `UscgManufacturerDetailRecord` schema, CLI/registry/workflow wiring, cassette + unit tests, and a thin `stg_uscg_manufacturer_details.sql`. The **SCD-2 `firm_manufacturer_attributes` + the HIN-build-date time-aware join stay in the Phase 6 cross-source SCD-2 item** (below), NOT this branch — bundling them would un-focus the branch and collide with Phase 6b on `firm.sql`. That SCD-2 model then **seeds historical intervals from the source-native `Past Company (OOB year)` + `In Business` lineage and extends them forward with our own bronze snapshots**. Full work breakdown: `project_scope/archive/phase-5d-uscg-manufacturers-detail.md` (written 2026-05-30; bronze-capture **shipped & merged in PR #42**, 2026-05-30 — the SCD-2 `firm_manufacturer_attributes` silver half + the HIN-build-date join stay deferred to the Phase 6 cross-source SCD-2 item / ADR 0035).

---

### Quality gates per source

- All integration scenarios pass (live cassettes + hand-constructed error paths)
- Rejected records route correctly to `<source>_recalls_rejected`
- Source freshness assertion configured on the bronze table
- Real API / file / scrape extraction works end-to-end
- Silver staging model passes dbt generic tests

---

## Architectural follow-ups

Cross-cutting work targeted at specific upcoming phases. Each item is gated to a phase rather than free-floating; the table below keeps the relationships visible.

| Item | Gated to | Status |
|---|---|---|
| ADR 0012 source-config loader and registry | **Phase 6** (preferred) or Phase 7 prerequisite | Implemented 2026-05-10 — Wave 2 MVP; per-environment overlay layering deferred as Phase 7 prerequisite, see "Per-environment YAML overlays" section below |
| ADR 0026 manifest implementation | **Phase 6** (USDA-only initially per accepted ADR) | **Implemented** — `extraction_run_identities` table + per-run `BronzeLoader.load()` insert + `recall_lifecycle` model shipped in Phase 6; manifest backfill via PR #61. See ADR 0026 + `silver_design_notes.md` §9. |
| ADR 0027 bronze storage-forced transforms refactor | **Phase 5b.2 Step 4.5** (already on critical path) | Cross-referenced from §5b.2 |
| `source_watermarks` seeding fix | **Phase 7 prerequisite** | **Closed 2026-06-09 — no-action at v1 scale.** Diagnostic-logging fix shipped 2026-05-09 (since consolidated into `_base._record_run` by #48). The FK-vs-CHECK constraint redesign stays Option (c) status-quo: zero cost for the frozen 9-source set, decidable on data only if a 10th source is contemplated. No go-live blocker. (Original deferral rationale + reopen condition below.) |
| FDA firm role reconciliation | **Phase 6 prerequisite** (firm entity resolution) | Implemented 2026-05-09 — see section below |
| Shared annotated types and invariants audit | **Phase 5c prerequisite** | Resolved 2026-05-01 — documented negative result; see section below |
| USDA recall ETag re-evaluation | **Phase 7 prerequisite** | Implemented 2026-05-09 — `etag_enabled=True` per Finding P; see section below |
| USDA establishment ETag enablement | **Phase 7 prerequisite** (gate-paired with USDA recall) | Implemented 2026-05-09 — `etag_enabled=True` per Finding A revision; see section below |
| `extraction_runs` source-specific column sparsity | **Phase 7 prerequisite** (was gated on USCG forensics) | **Closed 2026-06-09 — no-action.** USCG's pragmatic-capture path (2026-05-16) resolved the original blocker and the `response_inner_content_sha256` NULL-for-REST-rows sparsity has caused **zero** operational pain; the wide-table status quo stands. Revisit only if a real query-writing cost appears (Approach-2 extension tables are the recorded direction). No go-live blocker. |
| Quarantine-recovery CLI (`recalls recover-rejected`) | **`feature/quarantine-recovery-tool`** — shipped 2026-06-01 | **Implemented** 2026-06-01 (PR #45, v0.10.0) — `src/bronze/recovery.py` + `recalls recover-rejected`; FDA one-off retired. Owning doc: `project_scope/quarantine-recovery-tooling-plan.md`. See section below. |
| `src/` soundness consolidation (dedup-contract SSOT + DRY) | **`refactor/src-soundness-consolidation`** | Code complete 2026-06-01 — fixes the reachable NHTSA deep-rescan dedup bug (ADR 0030 amended); ~440 lines of duplication removed. NHTSA `main` data remediation pending. Owning doc: `project_scope/archive/src-consolidation-plan.md`; findings: `documentation/audit/src_soundness_audit.md`. |
| Deep-rescan reliability & workload (Phase-7 GHA readiness) | **`docs/deep-rescan-reliability-audit`** (graduation; impl tiers TBD) | **Phase 7 prerequisite.** Audit complete 2026-06-02 — workload (NHTSA O(corpus) ~21-min no-op) + reliability (Neon mid-txn drops; `pool_pre_ping`-only; USCG-detail exceeds the 6h cap) findings, with an adversarially-verified tiered fix ladder. Owning doc: `project_scope/deep-rescan-reliability-plan.md`; findings: `documentation/audit/deep_rescan_reliability_audit.md`. |
| Firm-attribute sidecar source-uniform renaming | **phase-7 plan C19** | **Implemented 2026-06-09** (verified on prod; old snapshot tables orphaned → operator DROPs). Surfaced 2026-06-08 (Phase 6f.1 schema review). The three SCD-2 firm sidecars + their snapshots use three inconsistent naming schemes (by-role vs by-source; the USCG snapshot doesn't mirror its view). Align to source-uniform `firm_{usda,uscg,fda}_attributes` + matching `_snapshot`s. **Zero-cost window:** snapshots bank 0 edit-versions today, so the rename is a code-only find-replace; once Phase 7 cron banks SCD-2 history it becomes a data migration. See section below. |
| Gold dimensional star schema (vs. extending `fct_*`) | **Phase 8 framing** (ADR 0024 / website data-feed) | Pending — deferred per ADR 0038 §1; `dim_` prefix reserved (§2). Revisit trigger now live: the project website is gold's first BI-esque consumer (surfaced 2026-06-08, Phase 6f.1). **Gating fork:** API- or direct-gold-fed with a *fixed* chart set → the existing `fct_*` aggregate marts already serve it (no star); a BI tool / semantic layer or user-driven cross-dimensional slicing → build the star. `dim_date` is a no-regret early DRY win. Narrative: `gold_design_notes.md` §"Deferred: a dimensional star schema". |
| Gold `dim_date` calendar dimension | **Pre-Phase-8 (no-regret)** | **Decided 2026-06-08** — build regardless of the star call (the star itself stays Phase-8-gated, row above). A generated calendar replacing the inline `date_trunc` in the **five** `fct_*` models that carry it (`fct_recalls_by_month`, `fct_recalls_by_year`, `fct_recalls_by_week`, `fct_recalls_monthly_trend`, `fct_units_recalled`); unlocks fiscal/holiday calendars. See `decisions/README.md` for ADR status; see `gold_design_notes.md` §"Deferred: a dimensional star schema". |
| Firm-coverage monitor (recalls with 0 firms) | **post-6f (cheap dbt singular test)** | Proposed 2026-06-08 (Phase 6f.1). **FDA + NHTSA must have ≥1 firm per recall** — a firmless FDA/NHTSA recall = a firm-extraction regression (hard invariant, 0 baseline). USDA/CPSC/USCG carry *documented* firmless baselines: USDA ~426 (`no_establishment_field`, ~35% — `usda/establishment_join_coverage.md`); CPSC ~37 (retailer-only / non-recall announcements); USCG ~9 (Finding-S null anchor). Monitor = `severity=warn` test in `dbt/tests/source_assumptions/`; alert on FDA/NHTSA going non-zero or material baseline drift. Probe: `scripts/sql/cross_source/silver/inspect_firmless_recalls.sql`. |

### ADR 0012 implementation: source-config loader and registry — Wave 2, landed 2026-05-10

The `config/sources/*.yaml` files were filed as Phase 1 deliverables but were not loaded by any code path until Wave 2. CLI dispatch instantiated extractors with hardcoded constructor kwargs, so YAML edits had no runtime effect. Affects all five sources equally.

Originally surfaced during Phase 5b USDA extraction when an `etag_enabled: false` YAML edit had no effect on the running extractor.

**Acceptance criteria met (2026-05-10):** editing `config/sources/usda.yaml` to set `etag_enabled: false` takes effect on the next `recalls extract usda` run with no code change. The CLI dispatch in both `extract` and `deep-rescan` commands now resolves a source name to an extractor class via the static dicts `src.config.source_registry.EXTRACTOR_BY_SOURCE_NAME` and `DEEP_RESCAN_BY_SOURCE_NAME`, then materializes constructor kwargs from `config/sources/<source>.yaml` via `src.config.source_loader.load_source_config`. The `usda.yaml` and `usda_establishments.yaml` "this file is NOT loaded by any code path" header comments are gone — the YAML files are now the live kill switch (and `usda_establishment.yaml` was renamed to `usda_establishments.yaml` so the filename matches the canonical source name in `extraction_runs.source`). As a side effect, the `config/sources/fda.yaml`'s `timeout_seconds: 60.0` declaration now takes effect at runtime (prior runtime used the parent-class default of 30s). See ADR 0012's "Implementation notes — source-config loader and registry (Wave 2, landed 2026-05-10)" section for the full implementation summary.

#### Per-environment YAML overlays — Phase 7 prerequisite (deferred from Wave 2)

ADR 0012 mentions per-env overlays (dev vs. prod) as a possible benefit. Wave 2's MVP is single-file-per-source: the loader reads exactly one `config/sources/<source>.yaml`. Layered overlays — `<source>.<env>.yaml` merging or replacing into the base — were deferred deliberately to keep Wave 2 scoped to the loader/registry refactor.

The current `Settings()` env-var indirection covers the legitimate per-env knobs today (DB URL, R2 keys, FDA creds); URL/timeout/etag values don't differ across dev and prod yet. When the first real env divergence appears (or before production cron turns on, whichever comes first), the overlay layer needs to land. Open design questions to resolve at that point:

- **Overlay precedence** — does `<source>.<env>.yaml` replace the entire block, or merge field-by-field?
- **Env-name source** — env var (e.g., `RECALLS_ENV=dev`)? hostname? CI flag?
- **Schema-mismatch behavior** — strict failure if overlay declares an unknown field, or warn-and-fall-back?

**Hard deadline: Phase 7 cron turn-on.** Cron-driven runs in production with the same code path as dev increases the silent-drift risk; before that, divergence is operator-visible.

### ADR 0026 implementation: per-run snapshot-presence manifest

Tracked in `documentation/decisions/0026-lifecycle-tracking-snapshot-presence-manifest.md`. Promoted to Accepted 2026-05-01 with USDA-only initial scope, Option A (separate `extraction_run_identities` table) representation, and Phase 6 timing.

Lands in Phase 6 alongside the silver `recall_event_history` model. Bronze-side change is the new table + a per-run insert in `BronzeLoader.load()`; silver-side change is the `recall_lifecycle.sql` model deriving `first_seen_at`, `last_seen_at`, `is_currently_active`, `was_ever_retracted`, `edit_count` columns.

**Within-Phase-6 ordering:** `recall_lifecycle.sql` depends on `recall_event_history.sql` — the lifecycle model reads from history rows, not vice versa. Order Phase 6 silver work as `recall_event_history` first → `recall_lifecycle` second. Both ride the same dbt build run once they exist.

Manifest backfill from historical R2 payloads is covered by ADR 0028 Mechanism C (`scripts/backfill_manifest.py`).

### ADR 0027 implementation: bronze does storage-forced transforms only

Tracked in `documentation/decisions/0027-bronze-storage-forced-transforms-only.md`. Promoted to Accepted 2026-05-01.

**Not a free-floating follow-up** — placed on the critical path as Phase 5b.2 Step 4.5 (see §5b.2 above), gating Phase 5c so NHTSA and USCG inherit the corrected pattern from day one. Listed here as a cross-reference.

### `source_watermarks` seeding for new sources — Phase 7 prerequisite

Migration 0001 hardcodes a five-source list (`cpsc/fda/usda/nhtsa/uscg`) and seeds `source_watermarks` with one row per source. `extraction_runs.source` is a FK to that table, so any new source needs a one-row seed migration before its `_record_run` call can succeed (otherwise the FK insert fails silently inside the broad except — surfaced during Phase 5b.2 first extraction when `usda_establishments` warning'd `extraction_run.record_failed` while bronze loaded normally).

Three options under consideration:
- **(a)** Drop the FK in favor of a CHECK constraint listing valid sources, updated as sources are added.
- **(b)** Drop the constraint entirely and let the application enforce the source enum.
- **(c)** Status quo — one seed migration per new source (like migration 0008 was for `usda_establishments`, like the migration that would be required for a hypothetical sixth source).

Options (a) and (b) avoid the per-new-source seed-migration ritual. Option (c) accepts the ritual but adds zero new design surface; for a project that may stay at five sources indefinitely, the ritual cost is bounded. Also: replicate the diagnostic-logging fix from `src/extractors/usda_establishment.py::_record_run` (capture exception `type` + `message` instead of swallowing) across `cpsc.py`, `fda.py`, `usda.py`. The current swallowing mode predates the fix and would mask similar failures on the older extractors.

Lands before Phase 7 cron turn-on so `extraction_runs` write-failures during cron are loud, not silent.

**Diagnostic-logging fix Implemented 2026-05-09.** `cpsc.py:331-345`, `fda.py:477-491`, `usda.py:523-537` now mirror `usda_establishment.py:529-544` exactly: `except Exception as exc:` captures the exception, the warning includes `error=str(exc)` and `error_type=type(exc).__name__`, and a comment explains why diagnostic fields matter for FK constraint violations. Tests `test_db_error_does_not_propagate` (cpsc, fda) and `test_db_failure_is_swallowed_and_logged` (usda) extended to assert the new fields are emitted via `structlog.testing.capture_logs()`. Migration 0008 (`migrations/versions/0008_seed_usda_establishments_watermark.py`) had already addressed the `usda_establishments` seed gap that surfaced this bug, so the diagnostic-logging fix is the only remaining piece of this section's near-term work.

**Constraint redesign (FK vs. CHECK vs. no-constraint) — deferred.** The original framing was "tackle after USCG so the source enum stabilizes." Two facts now make the upstream-dependency framing moot: (1) USCG was always in migration 0001's hardcoded `_SOURCES` list (a watermark row exists for it even though no extractor does), AND (2) USCG is now indefinitely deferred (USCG website down 2026-05-09 — **SUPERSEDED: USCG returned 2026-05-15; see the 2026-05-16 update below**). The source enum is therefore as stable as it'll get — the redesign now lacks **urgency**, not **prerequisites**.

**Update 2026-05-16 (USCG reactivation, Phase 5d Steps 1 & 2 landed):** USCG is back online and the bronze extractor + migration 0013 land in this branch. The pre-seed argument holds — USCG required NO new seed migration because migration 0001's hardcoded `_SOURCES` already covered it. So the reactivation neither triggers nor defers this work: the cost of (c) status quo remains zero for the v1 five-source set. Re-evaluate at Phase 5d Step 3 IF either (a) the first formal USCG extraction surfaces an operational pain not captured by the diagnostic-logging fix, or (b) a sixth source is contemplated. Otherwise the original Phase 7 cron-prep reopen condition stands.

**Reopen condition:** revisit during Phase 7 cron-prep when the operational pain (or lack thereof) of the per-new-source seed-migration ritual is visible. Adding a sixth source would be the natural trigger — at that point the cost of (c) status quo (one more seed migration) becomes empirically comparable to (a) or (b) the one-time constraint redesign, and the choice is decidable on data rather than speculation.

### FDA firm role reconciliation — Phase 6 prerequisite

`firm.sql` and `recall_event_firm.sql` label FDA's `firm_legal_nam` with `role='manufacturer'`, but semantically that field is the *recalling establishment* (analogous to USDA's `establishment` which uses `role='establishment'`). Relabel FDA's role to `'establishment'` to align cross-source firm rollups. Touches the `accepted_values` enum on `recall_event_firm.role` and downstream queries that filter by role.

Lands in Phase 6 alongside firm entity resolution work — the resolution logic across CPSC, FDA, and USDA is cleaner if all three agree on the role vocabulary first.

**Implemented 2026-05-09.** Relabeled in `dbt/models/silver/firm.sql:53-65` (`fda_normalized` CTE) and `dbt/models/silver/recall_event_firm.sql:43-54` (`fda_event_firms` CTE). The `accepted_values` test at `dbt/models/silver/_silver.yml:111-113` already permitted `'establishment'` (USDA had been using it), so no test-enum update was needed. Code-scan over `dbt/`, `scripts/sql/`, `src/` for hardcoded `'manufacturer'` filters surfaced no FDA-context occurrences that would silently miss FDA after the flip — the only `'manufacturer'` literal role-assignments in dbt are correctly scoped to CPSC's `manufacturers` JSONB array and NHTSA's `mfgname`. Stale documentation comments updated in `firm.sql:6-11`, `recall_event_firm.sql:6-11`, and `documentation/silver_design_notes.md:62`.

### USDA recall ETag re-evaluation — Phase 7 prerequisite

`UsdaExtractor.etag_enabled` was set to `False` during Phase 5b based on Finding N in `documentation/usda/recall_api_observations.md` — Akamai's CDN response was inconsistent enough that conditional-GET (`If-None-Match`) sometimes returned `200` with a full body even when the underlying data was unchanged. That observation predated dialing in the browser-like request fingerprint (Firefox/Linux UA + matching `Accept` / `Accept-Language` / `Accept-Encoding` headers per ADR 0016 amendment). With a stable fingerprint the bot-manager scoring path is more deterministic; the caching tier may now be deterministic too.

**Establishment API status reversed (2026-05-03).** Finding A originally claimed the establishment endpoint returns no `etag` header. The first production extraction with the response-capture columns (migration 0010) directly contradicted this: `etag` and `last-modified` populated on every successful run, identical shape to the recall endpoint. Likely cause: Finding A's Bruno probe sent default headers; Akamai's bot-manager appears to route browser-fingerprinted requests through a different cache tier (same dynamic Finding O documents on the recall side). A/B verification request committed at `bruno/usda/establishment_exploration/get_all_establishments_with_browser_headers.yml`. Finding A pending update after A/B confirms. **Net:** the establishment API is now in scope for the same viability study.

**Mechanism — automated capture (implemented 2026-05-03, supersedes the original manual-logging procedure).** Migration 0010 added five columns to `extraction_runs`:

- `response_status_code`, `response_etag`, `response_last_modified` — promoted forensic columns
- `response_body_sha256` — ground-truth oracle for "did the data change?" (byte-exact, covers inserts/updates/deletes)
- `response_headers` (JSONB) — full headers for retroactive cache-layer fingerprinting (X-Cache, Age, Server, Via)

Universal across REST API sources (cpsc/fda/usda/usda_establishments today; future sources inheriting from `RestApiExtractor` get capture for free). NHTSA and USCG inherit from `FlatFileExtractor` / `HtmlScrapingExtractor` respectively and would need a parallel capture path if the same forensic study is wanted there — out of scope for this prerequisite. Every `RestApiExtractor` populates the columns via `_capture_response()` on every successful fetch (paginated sources capture only the first page). The `etag_viability.sql` script at `scripts/sql/_pipeline/etag_viability.sql` reads from these columns and produces the green-light decision via 5 numbered queries (transition verdict, format inspection, origin-vs-CDN, intra-day stability, summary recommendation).

**Procedure:**

1. **Accumulate data.** Run the daily extractor (manual `gh workflow run extract-usda.yml` or `recalls extract usda` locally) for **at least 14 days, including ≥1 day with a real upstream update**. Multi-runs-per-day count toward the transition tally and add intra-day stability evidence — encouraged. No code change needed; capture is universal-on by default.
2. **Inspect verdicts continuously.** `psql -f scripts/sql/_pipeline/etag_viability.sql` (defaults to `usda`; pass `-v src=usda_establishments` to study the establishment endpoint with the same machinery). Watch query 1 for any row tagged `SUSPECT: false-304` — that's a disqualifying observation regardless of how clean the rest looks.
3. **Decision rule** (query 5 produces the recommendation directly):
   - `false_304_count = 0` for ≥7 transitions including a real-update day → safe to flip `etag_enabled=True`.
   - `false_304_count > 0` ever → leave disabled. Period. The full-dump + bronze content-hash pattern (ADR 0007) already handles dedup correctly; ETag would add risk without commensurate value.
   - `false_200_count > 0` only → safe to enable. You'll over-fetch occasionally; bronze hash absorbs it.
4. **Document the result** as a "Finding P" addendum to `documentation/usda/recall_api_observations.md` for the recall API, and update Finding A in `documentation/usda/establishment_api_observations.md` for the establishment API. Both record the empirical disposition regardless of which way the decision goes.

Best landed before Phase 7 cron turn-on so the daily bandwidth profile is settled before recurring runs accumulate. Cost of re-evaluation is now near-zero (the capture path runs on every extract automatically; no log-field addition or manual per-request capture needed); cost of leaving it ambiguous through cron is recurring ~1.6 MB / day per affected source on idle days that could have been 304s.

**Implemented 2026-05-09.** `etag_viability.sql -v src=usda` produced the SAFE-TO-ENABLE verdict over 7 transitions (`false_304_count=0`, `false_200_count=5`, one real-update day on 2026-05-05). `UsdaExtractor.etag_enabled` flipped to `True` at `src/extractors/usda.py:173`; `UsdaDeepRescanLoader` retains its explicit `False` override per its docstring. YAML at `config/sources/usda.yaml:36` updated for forward-consistency. Test `test_usda_extractor_etag_enabled_by_default` updated. Empirical disposition documented as Finding P in `documentation/usda/recall_api_observations.md`.

### USDA establishment ETag enablement — Phase 7 prerequisite

The establishment endpoint emits `ETag` and `Last-Modified` under browser fingerprint (Finding A revision 2026-05-03 + A/B verification at `bruno/usda/establishment_exploration/get_all_establishments_with_browser_headers.yml`). The capture path (migration 0010) collects per-run ETag observations alongside the recall endpoint's data; both share the same `etag_viability.sql` machinery.

**Code scaffolded 2026-05-03 with `etag_enabled=False` default.** `UsdaEstablishmentExtractor` now has 1:1 mirrors of `UsdaExtractor`'s `_fetch`, `_read_etag_state`, `_update_watermark_state`, `_touch_freshness`, and `_guard_etag_contradiction` methods (with "Mirrors UsdaExtractor.<method>; keep in sync" comments). The 304 lifecycle (land_raw skip → load_bronze touch_freshness) and the contradiction-guard test cases (`test_not_modified_304` / `test_etag_contradiction_guard`) parallel the recall side. Currently the extractor still issues a plain GET on every run because the flag defaults OFF — no behavior change vs. pre-scaffolding.

**Remaining work to enable** (gated on viability):

1. Verify `etag_viability.sql -v src=usda_establishments` shows the green-light verdict from query 5 (`false_304_count = 0` over ≥7 transitions including a real-update day; 14+ days of capture preferred).
2. Flip `etag_enabled=True` — either via constructor kwarg in `src/cli/main.py` or by changing the class default in `src/extractors/usda_establishment.py:UsdaEstablishmentExtractor`.
3. Optionally: do a 1-2 day dev smoke first by setting `etag_enabled=True` on a feature branch, running `recalls extract usda_establishments` repeatedly, and observing `If-None-Match` going out + 304s in `extraction_runs.response_status_code`.

**Decision rule:** the two endpoints share Akamai infrastructure and may exhibit identical ETag reliability, but do not assume so without evidence — they get studied independently. A `false_304_count > 0` for either source is disqualifying for that source regardless of how the other behaves.

Best landed alongside the recall ETag flip if both pass viability simultaneously, or independently if one passes and the other doesn't. Cost of leaving disabled through cron is ~810 KB / day downloads on idle days that could have been 304s. Lands before Phase 7 cron turn-on so the daily bandwidth profile is settled before recurring runs accumulate.

**Implemented 2026-05-09.** `etag_viability.sql -v src=usda_establishments` produced the SAFE-TO-ENABLE verdict over 7 transitions (`false_304_count=0`, `false_200_count=3`, one real-update day on 2026-05-06 inserting 7,176 records). `UsdaEstablishmentExtractor.etag_enabled` flipped to `True` at `src/extractors/usda_establishment.py:180`. YAML at `config/sources/usda_establishments.yaml:28` updated for forward-consistency. Test `test_no_conditional_headers_when_etag_disabled` reworked from "default-state" assertion to "explicit-disable" assertion (sets `etag_enabled=False` before sanity check). Empirical disposition documented in `documentation/usda/establishment_api_observations.md` Finding A revision 2026-05-09.

### `extraction_runs` source-specific column sparsity — Phase 7 prerequisite

`extraction_runs` is shaping up as a wide table where some forensic columns apply to only a subset of sources. Migration 0010 added five universal columns (`response_status_code`, `response_etag`, `response_last_modified`, `response_body_sha256`, `response_headers`) populated by every `RestApiExtractor` and `FlatFileExtractor`. Migration 0011 added `response_inner_content_sha256` populated only by flat-file sources (NHTSA today; USCG-adjacent later if its scrape produces an archive shape). The sparsity is structural: REST sources have no wrapper to decompress, so the column will be NULL for all CPSC / FDA / USDA / USDA establishment rows in perpetuity (~99% of `extraction_runs` rows by volume long-term).

Original framing assumed USCG (Phase 5d) would land and provide the second source-of-design-evidence — HTML-scraping forensics (HTTP cache headers, scrape-delay metadata, polite-scraper retry counts) that the split design needed alongside flat-file evidence from NHTSA. With USCG since returned (2026-05-15) and on the pragmatic-capture path (see the 2026-05-16 update below), that original blocker is resolved, but no urgency replaced it. Two paths now:

- **(a) Wait indefinitely** for USCG to come back. Pure status quo: 1 column (`response_inner_content_sha256`) is NULL for all REST-source rows; minor schema noise; no operational cost. If USCG never returns, this is the permanent state at zero cost.
- **(b) Proceed with NHTSA-only flat-file design now** (Approach 2 from the alternatives below). Build `extraction_runs_flat_file_forensics`, move `response_inner_content_sha256` to it. Accept that USCG-when-it-returns will need a separate `extraction_runs_html_forensics` migration.

**Recommendation: path (a) wait.** Pending state has zero day-to-day cost. Path (b) trades a minor cosmetic improvement for committing to a design before the second source-of-design-evidence (USCG) is available. If USCG never returns, path (b) was unnecessary work; if USCG returns, path (b) was premature commitment.

**Design alternatives to evaluate when this lands:**

1. **Status quo — wide table with NULLable source-specific columns.** Simple; one query joins everything. Cost: column count grows monotonically as sources add quirks; semantics not self-documenting from the schema alone.
2. **Per-source-type extension tables — `extraction_runs` keeps universal columns; `extraction_runs_flat_file_forensics` (1:1 to runs) holds flat-file-only columns; `extraction_runs_html_forensics` for HTML scraping.** Clean separation; only applicable columns per row. Cost: forensic queries need joins; one migration per new operation type.
3. **JSONB `source_specific_forensics` blob.** Zero migration churn for new fields. Cost: indexing degrades; no type constraints; analytical SQL becomes verbose.
4. **Materialized per-source views over the wide table.** Zero schema change; ergonomic for ad-hoc query. Cost: doesn't fix the underlying width — just hides it.

**Recommended direction (subject to revision when the data lands):** Approach 2 — per-source-type extension tables. Scales naturally as new operation types arrive; FK 1:1 to `extraction_runs.id` makes joins predictable; matches the existing `Extractor → RestApiExtractor / FlatFileExtractor / HtmlScrapingExtractor` operation-type hierarchy in `src/extractors/_base.py` and `_flat_file.py`. The split would be roughly: universal columns (status, ETag, Last-Modified, response body SHA, full headers) stay on `extraction_runs`; `response_inner_content_sha256` and any other future flat-file-only columns move to `extraction_runs_flat_file_forensics`.

**Acceptance criteria:** forensic queries that span all sources still work via a single LEFT JOIN per source-type table. No NULL sentinel from missing source-type rows is mistaken for "not captured" — captured-but-not-applicable distinguishes from genuinely missing.

**Reopen condition:** revisit if (1) Phase 7 cron-prep surfaces a real operational cost from the sparsity — e.g., a query that's hard to write because the column is NULL-for-some-rows — or (2) USCG returns and forensics shape becomes known. After cron is on, restructuring the table requires data migration in addition to DDL, so weigh that cost in any future revisit.

**Update 2026-05-16 (USCG reactivation, Phase 5d Steps 1 & 2 landed):** USCG returned 2026-05-15 — trigger (2) above has fired. Phase 5d Step 2 chose the **pragmatic capture path**: `UscgScrapingExtractor._record_run` populates the existing migration 0010 columns (`response_status_code`, `response_etag`, `response_last_modified`, `response_body_sha256`, `response_headers`) from the page-0 listing fetch, and leaves `response_inner_content_sha256` NULL (HTML has no wrapper/inner distinction). USCG's actual response shape (Finding K — no `Last-Modified`, no `ETag`, `Cache-Control: no-store`) means three of those five columns persist as NULL even when capture succeeds — that's correct semantics, not a missed capture. **Phase 5d Step 3 deliverable:** validate that the pragmatic-capture columns suffice operationally across ≥3 real extractions before committing to or against the Approach-2 (per-source-type extension tables) full redesign. If Step 3 surfaces HTML-specific forensic needs not covered by the existing columns (e.g., per-page retry counts, total NDJSON bytes uploaded, table-header signature for fast drift detection), THAT is the trigger for the redesign — not USCG-just-being-here.

### Shared annotated types and invariants audit — Phase 5c prerequisite

**Status: Resolved 2026-05-01 with a documented negative result.**

Three sources (CPSC, FDA, USDA recall, USDA establishment) have shipped Pydantic schemas and bronze invariants in isolation. The audit looked at `src/schemas/cpsc.py`, `fda.py`, `usda.py`, `usda_establishment.py`, and `src/bronze/invariants.py` for shared patterns worth extracting before NHTSA (Phase 5c) and USCG (Phase 5d) land.

**Audit conducted on the post-ADR-0027 codebase** (after value-level normalizers were dropped from bronze schemas in the same PR that filed this resolution). Conducting it after the refactor was deliberate — pre-refactor, the schemas had repeating `_normalize_str` / `_FdaNullableStr` / `_UsdaNullableStr` / `_FsisNullableStr` patterns that *did* look extractable. Those patterns no longer exist; bronze nullable-text fields are now plain `str | None`.

#### What's left in the schemas (post-ADR 0027)

| Source | Storage-forced validators | Annotated types |
|---|---|---|
| CPSC | `_coerce_date_string_to_utc_datetime` (calls `_parse_cpsc_date`) | None — only the date validator |
| FDA | `_to_int`, `_to_nullable_int`, `_to_str`, `_parse_fda_date`, `_parse_nullable_fda_date` | `_FdaInt`, `_FdaNullableInt`, `_FdaStrId`, `_FdaDate`, `_FdaNullableDate` |
| USDA recall | `_to_bool`, `_to_nullable_bool`, `_parse_usda_date`, `_parse_nullable_usda_date` | `_UsdaBool`, `_UsdaNullableBool`, `_UsdaDate`, `_UsdaNullableDate` |
| USDA establishment | `_coerce_false_to_text`, reuses `_parse_usda_date` / `_parse_nullable_usda_date` from `usda.py` | `_FsisFalseAsTextStr`, `_UsdaDate`, `_UsdaNullableDate` |

#### Patterns evaluated for extraction

1. **Nullable-parser wrapper.** Each source's `_parse_nullable_<source>_date` is structurally identical: `if v is None or v == "": return None; return _parse_<source>_date(v)`. Could be replaced with a `make_nullable(parser)` higher-order function. **Verdict: rejected.** Adds indirection for ~10 LOC saved across three sources; the explicit per-source named function is more readable.
2. **Date format parsing.** Different formats per source (FDA `MM/DD/YYYY`, USDA `YYYY-MM-DD`, CPSC `YYYY-MM-DD[THH:MM:SS]`). **Not extractable** — format is the source-specific quirk that requires the validator in the first place.
3. **Boolean string-to-bool, int coercion, false-sentinel coercion.** Each appears in only one source. **Not extractable.**
4. **Cross-source business invariants.** Already centralized: `check_null_source_id`, `check_date_sanity` in `src/bronze/invariants.py` are reused across CPSC/FDA/USDA extractors today. `check_usda_bilingual_pairing` is correctly USDA-specific. **No further extraction needed.**

#### Discipline for new sources (NHTSA Phase 5c, USCG Phase 5d, future)

When implementing a new source's Pydantic schema:

- Follow ADR 0027: only storage-forced validators (date string → datetime for `TIMESTAMPTZ`, "True"/"False" → bool for `BOOLEAN`, int coercion for `INTEGER`, etc.). Value-level normalization (empty string → null, whitespace strip, casing) belongs in silver staging models, not bronze schemas.
- Name validators per-source (e.g., `_parse_nhtsa_date`, not `_parse_date`) so each source's quirks remain readable in isolation. Do not preemptively create a "shared schemas" module — three sources of evidence have shown that the source-specific quirks dominate the would-be shared shape.
- For cross-source invariants, add to `src/bronze/invariants.py` and reuse from the new extractor's `check_invariants()` method. `check_null_source_id` and `check_date_sanity` are likely applicable to any source.
- For source-specific invariants (analogous to `check_usda_bilingual_pairing`), keep them in `src/bronze/invariants.py` if they're parameterizable across hypothetical-future similar sources, OR keep them in the source's extractor module if they're fundamentally one-of-a-kind.

If a fourth source's schema reveals a pattern that meaningfully repeats across three or more sources, file a follow-up to revisit this audit and extract at that point. The bar for adding a shared module is "evidence from three sources that the abstraction is real," not "two sources happen to have similar-looking code."

### Quarantine-recovery CLI (`recalls recover-rejected`) — implemented 2026-06-01 (PR #45, `feature/quarantine-recovery-tool`)

**Implemented 2026-06-01** (PR #45, v0.10.0, branch `feature/quarantine-recovery-tool`) — owning doc: `project_scope/archive/quarantine-recovery-tooling-plan.md`. Source-agnostic `recalls recover-rejected <source>` reads the uniform `<source>_rejected` table, reconstructs records (datetime-field coercion derived by Pydantic introspection — verified for FDA/NHTSA/CPSC) and loads them via `BronzeLoader.load` directly (no watermark mutation), dispatched through `RECOVERY_CONFIG_BY_SOURCE_NAME` in `src/bronze/recovery.py`; the FDA one-off `scripts/fda/recover_rejected_invariant_records.py` was retired (census SQL kept). **Distinct from — and complementary to — the planned `scripts/re_ingest.py`** (ADR 0014): re-ingest replays R2 raw bytes and *re-runs* `check_invariants()` (so it would re-reject the typo rows); recovery *bypasses* the invariant on purpose, human-in-the-loop (census-first, `--dry-run`, non-destructive), and does **not** change the invariant. First real use: reclaimed the 24 `check_date_sanity` rows (source dropped-century typo `2013→0013` on `recall_initiation_dt`) quarantined by the 2026-06-01 FDA full-corpus seed.

### Firm-attribute sidecar source-uniform renaming — Phase 7 prerequisite (surfaced 2026-06-08, Phase 6f.1)

**Implemented 2026-06-09 (phase-7 plan C19)** — done on `feature/phase-7-production-plus-todos` inside the zero-cost window (snapshots held 0 edit-versions). `firm_establishment_attributes → firm_usda_attributes`, `firm_manufacturer_attributes → firm_uscg_attributes`, + snapshots → `firm_{usda,uscg}_attributes_snapshot` (FDA already uniform). All dbt refs + docs + operator scripts updated; verified on prod (downstream `dbt build` PASS=75; snapshot re-run `INSERT 0 0`; new snapshots 7979/16260 all-current — 0 history lost). **Operator cleanup:** `DROP TABLE silver_snapshots.firm_establishment_attributes_snapshot;` + `… uscg_manufacturer_attributes_snapshot;` (orphaned by the rename). The original Pending spec follows as history.

Phase 6f.1's schema walk surfaced three different naming schemes across the per-source SCD-2 firm sidecars and their snapshots:

| Source | Current view | Snapshot | Scheme |
|---|---|---|---|
| USDA (FSIS) | `firm_establishment_attributes` | `firm_establishment_attributes_snapshot` | by-role (view + snapshot agree) |
| USCG | `firm_manufacturer_attributes` | `uscg_manufacturer_attributes_snapshot` | by-role view, **by-source snapshot (mismatch)** |
| FDA | `firm_fda_attributes` | `firm_fda_attributes_snapshot` | by-source (view + snapshot agree) |

**Decision:** align to a source-uniform scheme — `firm_usda_attributes` / `firm_uscg_attributes` / `firm_fda_attributes` and matching `_snapshot`s. Each sidecar is 1:1 with one source on a source-specific government anchor (`establishment_number` / `mic` / `firm_fei_num`), and FDA already sets the by-source precedent. (Counter-argument considered: the role nouns "establishment"/"manufacturer" carry domain meaning that "usda"/"uscg" hide — but FDA firms are *also* establishments, so the role scheme can't be made uniform without a source qualifier anyway.)

**Why pre-Phase-7 — the zero-cost window.** Every snapshot is forward-banking 0 edit-versions today (`_snapshots.yml`), so the SCD-2 history tables hold only the current state. Right now a rename is a pure code find-replace + a clean rebuild that loses nothing. Once Phase 7 cron starts banking real SCD-2 history, renaming a snapshot orphans its accumulated history and becomes a data migration. Cheapest done now.

**Scope (verified reference counts, excl. `target/`).** Rename 3 current-view models + 3 snapshots; update `ref()`s in the gold marts (`mart_firm_profile`, `fct_recalls_by_geography`), the silver consumers (`recall_event_firm`, `recall_event_establishment_resolution`, `uscg_mic_reassignment_years`), the three current-view bodies, the 3 sidecar tests, `_silver.yml` / `_snapshots.yml`, and the docs (`database_overview.md`, `data_schemas.md`, `silver_design_notes.md`, `gold_design_notes.md`). Mechanical; no logic change. Verify the snapshots re-bank 0 versions on rebuild (idempotency gate) after the rename.

---

## Phase 6 — Full silver + gold materialization

**Goal:** unified data model across all five sources.

**Status: ✅ Complete — 6a → 6f all landed (closed out 2026-06-08).** Built per the archived master `project_scope/archive/phase-6-execution-plan.md` + per-stage execution plans (6b/6c/6d archived; 6e merged #62; **6f diagrams + docs-sync** archived as `project_scope/archive/phase-6f-execution-plan.md`). Diagrams are now **Mermaid inline** in the docs (no draw.io). Open follow-ups (`dim_date`/star schema, the SCD-2 sidecar rename, the firm-coverage monitor) are tracked under *Architectural follow-ups* above.

**Deliverables:**

- Silver `recall_event`, `recall_product`, `firm`, `recall_event_firm` fed from all five sources' staging models
- **Silver field remap (the "(a) PR")** — audit-driven correction of cross-source silver field mappings + canonical column naming + dbt tests, grounded in full-corpus bronze profiling. Owned by `project_scope/silver-field-remap-plan.md`; audit artifacts `documentation/audit/{bronze_corpus_profile,cross_source_consolidation}.md`; canonical-naming decision ADR 0036. Precedes 6b/6c per `archive/phase-6-execution-plan.md` § Sequencing Constraints.
- **Silver field capture-expansion (the "(b) PR")** — the FDA work the (a) remap deferred: (A) `firm_fda_attributes` sidecar + posted-date silver lift (fields already in bronze via migration 0019), (C) new Tier-3 per-event press-release extractor + bronze child table. Tier-2 excluded (no net-new content, `api_observations.md` K0.3); `code_information` parse deferred to `freetext-enrichment-backlog.md`. Owned by `project_scope/silver-field-capture-expansion-plan.md`; candidate parking lot `documentation/audit/capture_expansion_backlog.md` § FDA.
- Firm entity resolution: FDA's `firmfeinum` as the anchor per ADR 0002; fuzzy-match (RapidFuzz) across sources for non-FDA firms
- Full dbt test suite per ADR 0015 (60–80 generic tests + 5 singular + freshness)
- Gold: aggregate views for dashboards, denormalized search index
- **Execution of the history / lifecycle / cross-source SCD-2 deliverables below is owned by `project_scope//archive/phase-6c-execution-plan.md`** (commits 6c.0–6c.8; it also absorbs NHTSA v1.5 Layer 2/3 from `archive/silver_v15_migration_plan.md`). The descriptions here remain the master spec; that doc is the how.
- `recall_event_history` silver dbt model per ADR 0022 — uniform `LAG()` window function over bronze snapshot tables for all five sources (CPSC, FDA, USDA, NHTSA, USCG); no source-asymmetric path. Model partitions by `(source, source_recall_id)`, orders by `extraction_timestamp`, and emits one row per changed field per snapshot interval. **Joins to `extraction_runs.change_type` and excludes rows from non-routine runs** (`schema_rebaseline`, `hash_helper_rebaseline`) from edit detection so parser-driven re-version waves don't synthesize false-edit events — see ADR 0027 + `documentation/operations/re_baseline_playbook.md`. **Per-field whitespace normalization before LAG comparison.** Empirically observed on USDA recalls (2026-05-15 wave, Finding Q in `documentation/usda/recall_api_observations.md`): cosmetic upstream whitespace edits (e.g., 10 leading newlines collapsed to 1 on `company_media_contact`) drive ~1024 phantom bronze re-versions per wave despite 100% byte-identical actual content. To prevent the model from synthesizing 1024 phantom edit events per such wave, compare values via `regexp_replace(value, '\s+', ' ', 'g')` then `trim()` rather than raw equality, on text fields prone to whitespace churn (USDA: `company_media_contact`, `summary`, press-release fields; audit other sources for similar fields). This places the cosmetic-noise filter in silver where its design trade-off is visible, rather than at the bronze hash level where it would lose real isolated edits (rationale: `company_media_contact` and analogous "media contact" fields on other sources are Phase 6 candidates for surfacing to silver/gold/API and a hash-exclude would prevent capturing isolated real updates). FDA's native history endpoints (`/search/productHistory/{productid}` and `/search/eventproducthistory/{eventid}`) were confirmed empty across all tested lifecycle states in Phase 5a; if they ever start populating, file a new ADR and add: (a) an Alembic migration for `fda_product_history_bronze` and `fda_event_product_history_bronze`, (b) an extraction path for those tables, and (c) a `UNION` branch in this model to merge native-history rows with the snapshot-derived rows. Until then those tables do not exist.
- `extraction_run_identities` table — Alembic migration; one row per `(run_id, source_recall_id)` per ADR 0026, populated by a per-run insert in `BronzeLoader.load()`. USDA-only initial scope per the accepted ADR. Bronze-side half of the lifecycle-tracking work. See § ADR 0026 implementation above for full rationale.
- `dbt/models/silver/recall_lifecycle.sql` — silver-side half of ADR 0026; derives `first_seen_at`, `last_seen_at`, `is_currently_active`, `was_ever_retracted`, `edit_count`. **Within Phase 6, lands AFTER `recall_event_history`** — `recall_lifecycle` reads from history rows, not vice versa.
- **Cross-source SCD-2 strategy for silver dimensions.** `recall_event_history` and `recall_lifecycle` cover recall-event-level history, but silver dimensions (`firm`, `firm_establishment_attributes`, `firm_manufacturer_attributes`, `recall_product`) are currently materialized as `table` and re-built on every `dbt build` with **no attribute history preserved**. The 2026-05-15 USDA establishments run made this gap concrete: 13 establishments flipped `status_regulated_est` from `''` to `'Inactive'` (see `documentation/usda/establishment_api_observations.md` Finding G addendum), and after the next transform their prior status is unrecoverable from silver/gold alone. Decide on a uniform strategy across sources: (a) dbt snapshots in `dbt/snapshots/` with `strategy='check'` over a chosen attribute set per dim, OR (b) a per-dim `silver/<dim>_history.sql` model that derives history from bronze via `LAG()` (analogous to `recall_event_history`'s pattern), OR (c) defer entirely and route as-of-date queries to bronze. Decision-forcing analytical use cases: "what was the firm's regulatory status as of recall X's publication date", "did this firm change address between recalls", "when did this product's category last change", "which manufacturer held this MIC when the recalled boat was built" (USCG MIC reassignment — confirmed 2026-05-30, `AXY`/`COP`; the disambiguator is the boat build-date encoded in the HIN, not the recall date). File as an ADR before implementing, since the choice ripples across all four sources' silver layers and the as-of-date query surface affects gold model design. Sequence: file the ADR early in Phase 6 (before `recall_event_history` lands, since the implementation may share `LAG()` plumbing); land the implementation alongside or after firm entity resolution, since firm resolution stabilizes the dim grain. **USCG manufacturers adds a second concrete instance and a twist:** the 2026-05-30 `AXY`/`COP` MIC reassignments (see `documentation/uscg/manufacturer_scraping_observations.md` §M) are whole-entity succession under a stable anchor (`mic`), not single-field drift — hence `firm_manufacturer_attributes` is in the dim list above. Uniquely among our sources, the USCG source **publishes its own succession history** (detail-page `Past Company 1–3 (OOB year)` + `Date Modified`), which predates our observation window by decades — so the SCD-2 model for this dim **seeds historical intervals from the source-native lineage (`Past Company (OOB year)` + `In Business`, captured via the Path B detail-enrichment pass — Step 7 follow-up above) and extends them forward with our own bronze snapshots** (forward-only from 2026-05-30). The bronze-side Path B capture is its own feature branch (`feature/uscg-manufacturers-detail-addition`, bronze-only); this SCD-2 silver model and the HIN-build-date join are the Phase 6 half, deliberately kept off that branch to avoid colliding with Phase 6b on `firm.sql`. The decision-forcing query here is as-of-*build-date* (HIN chars 9–12; recalls bronze carries `hin` and `model_year`), not as-of-recall-date — see the cross-source application section in `project_scope/archive/silver_v15_migration_plan.md`. **v1 join treatment — flag-as-time-sensitive (not precise resolution).** The recall-directed probe (2026-05-30, §M.6) sized the surface at **205 recalled MICs (28.7%) `(OOB)`-recycled and 365 (51.1%) with any prior holder** — material, not edge-case. But the source's reassignment dates are largely unusable for a precise build-date lookup (only ~13 of 205 carry a parseable `(OOB YYYY)` year; `In Business` is contaminated by record-touches). So v1 silver attributes to the current MIC holder **but flags any recall whose MIC has a `Past Company` as "manufacturer attribution time-sensitive"** rather than silently misattributing; precise HIN-build-date resolution is a later refinement where the dates permit. The affected-MIC list is the probe artifact `data/exploratory/uscg_manufacturers/recalled_reassigned_mics.json`.

  **Cross-cutting sequence.** The branch ordering for the snapshot/SCD work relative to 6a.5, the silver remap, and 6b is **owned by `project_scope/branch_sequencing_strategy.md` (dependency graph) and `archive/phase-6-execution-plan.md` (§ Sequencing Constraints)** — see there; don't restate. In brief: **6a → 6a.5 → `feature/silver-field-remap` → {NHTSA v1.5 Layer 2 (snapshot baseline must run post-6a.5 on full-corpus bronze), 6b, 6c} → NHTSA Layer 3 cutover (coordinated with 6c/6b).** The one implementation-scoped detail that belongs here is the per-source SCD **build priority** (Open Q#4): **NHTSA (acute fragmentation; first) → CPSC (firm-array ordinality-shift hazard) → USCG (confirmed MIC reassignment + source-native lineage); FDA/USDA low-priority (stable natural keys / 1:1 grain).**

  **Sub-decision the SCD-2 ADR must resolve — value-selection policy for erasable text fields.** Distinct from the storage-architecture choice (a/b/c above), the ADR must also specify *how silver's current-state view selects a value when the source has erased a previously-populated text field mid-lifecycle*. Three policies, in increasing order of analytical fidelity and implementation complexity:

  - **Policy A — naive latest wins.** Silver `current_value = bronze_latest_snapshot.value`, full stop. If FSIS erases `field_establishment` from `"Richelieu Foods, Inc."` to `""`, silver carries `""` and downstream joins against `usda_establishments` go silently lossy. Simplest. Worst for analytical fidelity. Defensible only if value-erasure is provably rare and inconsequential — which our current evidence contradicts.
  - **Policy B — latest-non-empty wins (per-field).** Silver carries the last populated value forward; an explicit `<field>_is_inferred_from_history` boolean (or analogous flag) marks rows where the live value was overridden. Preserves join capability for the common case (FSIS-side data degradation). Wrong for the case where FSIS *intentionally* cleared the field (e.g., legal redaction, firm misattribution correction). Requires per-field policy declaration in the silver model (not all text fields should be subject to non-empty preservation — `summary` certainly shouldn't be).
  - **Policy C — latest wins, history is a first-class peer model.** Silver `current_value = bronze_latest_snapshot.value` (same as Policy A), AND a parallel `silver/<dim>_attribute_history.sql` model exposes prior populated values as queryable rows. Downstream consumers that need join resilience use the history model; downstream consumers that want the source-of-truth current state read silver-current. Cleanest separation of concerns: silver represents "what FSIS says is true today," history represents "what FSIS has ever said." No policy decisions baked into silver-current; no inferred-value confusion in downstream joins; the as-of-date query surface composes naturally on top of history.

  **Leading candidate as of 2026-05-17: Policy C.** Rationale: (1) matches the project's existing bronze/silver division of labor — bronze is the passive ledger, silver is the opinionated current-state view; (2) avoids baking policy decisions into silver-current that we don't yet have enough evidence to make confidently (the user's instinct in the 2026-05-17 thread); (3) parallels the existing `recall_event_history` design (history is a peer model, not metadata on the current table); (4) keeps Policy B available as a layered optimization on a per-consumer basis later, once join-coverage measurement quantifies the loss from naive latest. Tradeoff is plumbing cost: every dim that has erasable text fields needs a peer history model. That cost is bounded (the same `LAG()` pattern is reused) and aligns with sub-decision (b) above if (b) is chosen.

  **Motivating empirical evidence for the value-selection sub-decision:**

  - USDA recalls (2026-05-17): `PHA-04302026-01` / English exhibited upstream-FSIS clearing of `field_establishment` (`"Richelieu Foods, Inc."` → `""`) and `field_company_media_contact` (populated HTML block → `""`) between snap 1 (2026-05-01) and snap 2 (2026-05-02), with values remaining empty in snap 3 (2026-05-17). Verification script: `scripts/usda_recalls/inspect_raw_landing_for_recall.py` confirms the erasure originates upstream at the API, not in the bronze Pydantic schema (which preserves `""` verbatim per ADR 0027). Full diagnostic write-up: `documentation/usda/bilingual_and_lmd_findings.md` PHA-04302026-01 subsection.
  - USDA establishments (2026-05-15): 13 establishments flipped `status_regulated_est` from `''` to `'Inactive'` (the original motivating case for the sub-decision (a/b/c) framing above). Same class of mid-lifecycle attribute change; different field type (status enum vs free-text join key).
  - Cross-source extrapolation: any source that publishes editorially-maintained records (CPSC, FDA, NHTSA recall narratives) is subject to the same risk. The ADR should specify Policy C as the cross-source default and audit each source's per-field stability profile (an extension of Finding C / its analogues) to identify which fields warrant inclusion in the per-dim history model.

  **Implementation hooks once the ADR lands:**

  - If Policy C: file `dbt/models/silver/<dim>_attribute_history.sql` for each dim with erasable text fields. Schema: `(natural_key, attribute_name, value, first_seen_at, last_seen_at, is_currently_active)`. One row per distinct value per natural_key per contiguous run of snapshots. Sourced from bronze via `LAG()`, reusing the `recall_event_history` plumbing.
  - Independently of storage architecture: add a "value-erasure stability profile" subsection to each source's findings doc enumerating which text fields have been observed to clear mid-lifecycle. Seed with the USDA observations above; extend per source as evidence accumulates.
  - Phase 6 quality gate addition: "downstream join against an erasable text field falls back to history when current-snapshot is empty" — verifiable via an e2e test that re-creates the PHA-04302026-01 erasure pattern in test fixtures and asserts the `usda_establishments` join resolves to the pre-erasure firm.
  - **dbt singular test — continuous join-key erasure warning.** File `dbt/tests/source_assumptions/assert_no_join_key_erasure_<source>.sql` for each source that has join-key text fields prone to erasure, alongside the existing U2/U3 wrappers (`severity=warn` to match convention). Test logic: for each `(natural_key)` in bronze, fail (warn) if a join-key text field is populated in any prior snapshot AND empty (`''` or `NULL`) in the current snapshot. USDA recalls seed fields: `establishment`, `company_media_contact`. USDA establishments seed fields: TBD pending erasure observations there. The test surfaces every new erasure event during the next `dbt build` after extraction — proactive monitoring layer that pairs with the silver `<dim>_attribute_history.sql` model (history captures the value; the test surfaces the event). Without this test, erasures land silently in bronze and surface only when someone runs `inspect_raw_landing_for_recall.py` against a specific recall. Tracker query shape:

    ```sql
    -- per source, per declared erasure-prone field
    with snaps as (
        select source_recall_id, langcode, extraction_timestamp,
               <field> as value,
               row_number() over (
                 partition by source_recall_id, langcode
                 order by extraction_timestamp desc
               ) as rn
        from {{ ref('<bronze_table>') }}
    )
    select source_recall_id, langcode
    from snaps cur
    where cur.rn = 1
      and (cur.value is null or cur.value = '')
      and exists (
          select 1 from snaps prior
          where prior.source_recall_id = cur.source_recall_id
            and prior.langcode = cur.langcode
            and prior.rn > 1
            and prior.value is not null and prior.value <> ''
      )
    ```

    Generalize via dbt's per-column test config (one test per erasure-prone field, listed in `dbt_project.yml` or in a vars block) so adding a new field-to-monitor is a one-line config change rather than a new test file.
- `scripts/backfill_manifest.py` — historical R2 payload replay per ADR 0028 Mechanism C, used to populate `extraction_run_identities` for runs predating the table's existence.
- `scripts/re_ingest.py` — re-ingest CLI per ADR 0014 for schema-drift recovery
- Alembic migrations for all silver and gold tables
- ✅ Silver+gold ERD set + written overview → `documentation/database_overview.md` (Mermaid, Phase 6f.1) — supersedes the originally-planned draw.io ERD.

**Quality gates:**

- All dbt tests pass
- Firm resolution works on demonstrable cross-source examples (Honda, Tyson, etc.)
- Re-ingest command is idempotent (verified via repeat runs)
- History captures a simulated schema-drift event in an e2e test

---

## Phase 7 — Production CI and orchestration

**Goal:** production-grade automation.

**Deliverables:**

- All five per-source extract workflows on cron per ADR 0010 cadences (note: USDA is full-dump on every run per ADR 0010 revision note — no incremental filter exists)
- CPSC deep-rescan workflow on weekly cron per ADR 0010's deep-rescan addendum — **mandatory**, not optional, because CPSC's `LastPublishDate` does not advance on edits (verification closed 2026-05-01). FDA deep-rescan also on weekly cron per ADR 0023. USDA's daily run is already a full snapshot, so a separate "deep rescan" workflow would be redundant — the dispatch-only `deep-rescan-usda.yml` is retained for operator convenience but contributes no additional coverage.
- **Pre-cron blocker — CPSC historical seeding (per ADR 0028):** before turning on weekly cron, run `deep-rescan-cpsc.yml` once with `--change-type=historical_seed` to populate the historical gap currently missing from bronze. The deep-rescan path uses CPSC's `1970-01-01` floor (no `LastPublishDateStart` needed — that 2005 figure was stale), so a single dispatch returns the full ~9.8k-record corpus. The gap exists because the CPSC archive migration cadence (~2–3 records/day) will not reach the backfill point for years on its own. Documented in `documentation/cpsc/last_publish_date_semantics.md` Section 3 and ADR 0028 Mechanism A.
- **Pre-cron blocker — deep-rescan validation across all sources:** before turning on weekly cron AND before any production historical-seed run, exercise each source's `deep-rescan-<source>.yml` workflow at least once on the dev Neon branch. The deep-rescan code path is structurally distinct from the incremental path (separate method or extractor class per the Phase 5 standing requirement), has no count guard, and is exercised less frequently — silent breakage typically surfaces only on a real seed run, which is expensive to redo. Validation criteria per source: (a) workflow_dispatch run completes successfully; (b) bronze rows land with `change_type=historical_seed` recorded in `extraction_runs`; (c) `source_watermarks` is NOT advanced by the deep-rescan path (incremental owns the watermark exclusively); (d) row counts match expectations from each source's empirical findings (CPSC ~9k + 20-year backfill, FDA ~current API total, USDA ~2k full dump, NHTSA ~322k via PRE_2010 + POST_2010, USCG TBD). Note: USDA's deep-rescan is functionally identical to its incremental path (full dump every run) so this is largely formality for that source; for CPSC / FDA / NHTSA / USCG the validation is load-bearing. Document the validation result per source in `documentation/<source>/` for the audit trail.
- Transform workflow (`dbt build` + `dbt test`) on time-shifted cron per ADR 0018
- Full PR-check workflow matching ADR 0018 (ruff, pyright, pytest unit + integration, dbt parse, 1–2 e2e smoke)
- Neon branching via the Neon API for integration-test DBs (per ADR 0015); `test_db_url` fixture in `conftest.py`
- `dbt docs generate` deploys to Cloudflare Pages on every main push
- Quarterly secret-rotation reminder workflow per ADR 0016
- Startup-check in every cron workflow that validates all required secrets are present before invoking extraction code (fail fast with a clear message rather than a `KeyError` mid-run)
- **Database-level mutation guard on `*_rejected` tables.** ADR 0013 designs the per-source rejected tables as append-only audit trail (schema-drift forensics, re-ingest source per ADR 0014, data-loss accounting). Enforce that as a Postgres invariant in production rather than relying on operator discipline: revoke `TRUNCATE`, `DELETE`, and `UPDATE` on every `*_rejected` table from the production application role, leaving only `INSERT` and `SELECT`. The migration role retains DDL rights so future Alembic migrations still work. Dev branches keep full privileges (truncating is fine when iterating on a buggy schema). Filed during Phase 5b.2 first extraction (2026-05-01) — context: 7,945 records were rejected on a missed `city` field; the temptation to truncate before the fix-and-retry highlighted the need for a structural guard in prod.
- **Re-baseline gate for bronze-shape PRs (ADR 0027).** Add `.github/PULL_REQUEST_TEMPLATE.md` with a "Does this change the bronze canonical dict?" checkbox, and a CI workflow `.github/workflows/re-baseline-check.yml` that fails any PR touching `src/schemas/*.py` or `src/bronze/hashing.py` whose body lacks a `RE-BASELINE: yes|no` line. Operator-side procedure documented at `documentation/operations/re_baseline_playbook.md`. Lands before cron turn-on so the first production schema PR hits the gate.
- **Investigate dedicated data-quality framework (Soda Core or Great Expectations).** Today, data-quality assertions are hand-rolled SQL (`scripts/sql/<source>/<layer>/assert_*.sql`) plus the three starter business invariants in `src/bronze/invariants.py` plus dbt tests on silver. A dedicated DQ framework would give: (a) a uniform place to express cross-source assertions like NHTSA's 11-tuple identity stability check at `scripts/sql/nhtsa/bronze/assert_eleven_tuple_identity_stable.sql` (motivation: `documentation/nhtsa/incremental_delta_findings.md` Section D — detect identity-field drift that ADR 0030's dedup cannot catch); (b) standardized result reporting / alerting on assertion failures (vs. eyeballing `psql -f ...` output); (c) anomaly-detection patterns (row-count deltas, null-rate drift, freshness SLAs) that today require ad-hoc scripts. Decision criteria: pick **Soda Core** if the assertions stay close to SQL and we want a low-ceremony YAML config; pick **Great Expectations** if we want richer Python-expressible expectations and HTML data-docs. Sequence: cheap assertions stay as `.sql` files until there are ≥10 of them or the first production-impact incident demands centralized alerting, whichever comes first.
- **Data-quality assertion maturation — severity escalation + fixture-backed monitor tests (deferred here from Phase 6c, 2026-06-06).** Phase 6c shipped the silver history/SCD layer with a set of `severity=warn` monitors and deliberately left two maturation steps to the DQ-framework migration above, because both need capabilities the current hand-rolled-SQL + dbt-test setup lacks. (Cross-refs: `project_scope//archive/phase-6c-execution-plan.md` 6c.8 as-built; `documentation/architecture.md` testing strategy; the `severity` policy comment in `dbt/dbt_project.yml`. This bullet is their single home as forward work.)

  - **(a) Threshold-aware `warn → error` escalation.** `dbt/dbt_project.yml` holds every `tests/source_assumptions/*` monitor at `severity=warn` (several have known non-zero baselines — e.g. the ~13.3% USDA bilingual non-atomicity per ADR 0026, the FDA null-country and nullable `announced_at` warns). The 6c monitors split into two classes that need *different* treatment, which only a threshold-aware framework expresses cleanly — a blanket dbt `error` cannot:
    - *Forward-measuring drift monitors* — `assert_classification_stable`, `assert_lifecycle_stable`, `assert_mic_holder_stable`, `assert_no_join_key_erasure_usda` (all 6c.3), plus the pre-existing `assert_nhtsa_eleven_tuple_identity_stable`. These are **designed to fire** when upstream drift/erasure occurs; promoting them to a binary `error` would fail the nightly build on the first *expected* event. They want anomaly-style alerting (notify, don't block) or a rate threshold (`error` only above N events per window), i.e. the Soda/GE "warn vs fail threshold" primitive.
    - *Zero-baseline structural invariants* — `assert_pre_2008_seven_tuple_unique` (6c.6; 0 rows on the full corpus) and the snapshot `(<anchor>, dbt_valid_from)` uniqueness combination tests. A violation here is a real anchor bug, so these are the cleanest first candidates to go straight to `error`. They are held at `warn` now only to keep the `source_assumptions` dir uniform until the migration handles severity systematically — do NOT special-case one test's severity before then (inconsistent dir = future confusion).
  - **(b) Fixture-backed monitor-fire tests.** A monitor that returns 0 on clean live data is not *proven* to fire on a violation. Proving it — e.g. that `assert_no_join_key_erasure_usda` catches the PHA-04302026-01 establishment-erasure pattern (the event it was built from) — needs a **fixture-DB harness the project does not have**: `tests/integration/` replays HTTP cassettes through *extractors → bronze*, but nothing loads synthetic bronze rows and runs dbt models / singular tests against them. A hand-crafted SQL "demo" that re-runs copied monitor SQL on copied data is **circular (false coverage)**, so 6c.8 deliberately did not build one; and the real positive case can't be replayed because the Phase-6a.5 re-seed collapsed bronze to current-state, erasing the pre-erasure version. The natural home is a Neon-branch-backed dbt-fixture harness (ADR 0015's `test_db_url`) or GE/Soda fixtures, where each monitor asserts **both** the clean (0) and planted-violation (≥1) cases. The same harness retires the one remaining v1.5 coverage gap: proving the dbt-snapshot SCD-2 mechanism **versions** a `check_col` edit end-to-end (today that rests on dbt-core's external guarantee per ADR 0033 + the 6c.7 live-cutover evidence — `recall_product` 320,303, idempotent re-snapshot, `characterize_v15_*` — rather than an automated fixture test).

**Quality gates:**

- First full end-to-end production run (cron extracts + transform + docs deploy) succeeds
- Dashboards (once built) show real data
- PR-check pipeline runs under 10 minutes

---

## Phase 8 — Serving layer (FastAPI)

**Goal:** public API for recall data. Foundation for any frontend.

**Prerequisites:**

- **ADR 0024 — Serving-layer API design** filed and accepted. Covers endpoint shapes, response schemas, pagination, rate-limit posture, auth posture (public read-only per the project vision), OpenAPI generation strategy, and the relationship between API endpoints and dbt gold views.
- **ADR 0025 — API deployment target** filed and accepted. Evaluates Fly.io vs. Render vs. Cloudflare Workers free tiers against cold-start behavior, Python runtime compatibility, read-only Neon connection patterns (from `main` per ADR 0005), and GitHub Actions CI/CD integration.

Rationale for two ADRs rather than one: API design and deployment target are separable concerns, and deployment constraints sometimes drive design choices (e.g., Cloudflare Workers' Python limitations would reshape endpoint design). Keeping them separate also matches this project's pattern of narrow, single-decision ADRs.

*(ADRs 0022 and 0023 were used for FDA revision ADRs filed in Phase 5a. ADRs 0026–0029 were filed during the 2026-05-01 architecture realignment — see `documentation/decisions/README.md` for the index.)*

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

- Framework ADR — **ADR 0039** (filed on the phase-7 branch per the master plan; see `documentation/decisions/README.md`, the SSOT for ADR numbers — 0024/0025 are the API design + deployment-target ADRs; 0034/0035/0036/0038 ratified Accepted)
- Dashboard MVP showing recall counts, classifications, firm rollups
- "Is my product recalled?" search UI
- Deployment to Cloudflare Pages or Vercel free tier

---

## Out of scope for v1

- **EPA integration** — deferred per ADR 0001
- **Statistical drift detection** — needs baseline data; add in v2 per ADR 0015
- **draw.io diagrams** — separate walkthrough (tracked in `TODO.md`)
- **Monitoring / alerting beyond GitHub Actions UI** — formalized as ADR 0029 with named upgrade triggers; add when one fires
- **Authenticated API tier** — public read-only is sufficient for v1
- **Silver-layer interpretation of source-side deletions/retractions** — bronze captures the *signal* (record absent from a snapshot) via ADR 0026's manifest, but silver in v1 reports `is_currently_active` only. Modeling deletion as a first-class lifecycle event in silver/gold (e.g., "this recall was withdrawn on date X" rather than "this recall stopped appearing in the response on date X") is a v2 effort. The signal exists; the interpretation is deferred.

---

## Tracking progress

Progress is tracked in this plan by checking off phase deliverables as they ship. A phase is not "done" until all its quality gates are green. New ADRs filed during implementation are linked from the relevant phase.

When implementation starts, use Claude Code's plan-mode feature or a TodoList per phase to track task-level progress within a phase.
