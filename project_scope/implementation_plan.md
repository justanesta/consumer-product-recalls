# Implementation Plan

This plan sequences the implementation of the decisions captured in ADRs 0001–0021. Each phase produces something deployable and testable; later phases build on earlier ones.

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

**Step 5 — Silver** (pending)
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

### 5b.2. USDA FSIS Establishment Listing API — recall enrichment (pending)

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
   `config/sources/usda_establishment.yaml`, Alembic migration
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

**Step 2 — Schema, extractor, migration** (pending)
- `src/extractors/_flat_file.py` — `FlatFileExtractor` operation-type subclass of the `Extractor` ABC (deferred from Phase 2 to its first use here). Shape is informed by NHTSA: ZIP download → stream-decompress → row-by-row parse → bronze load. Unit-tested in isolation before `NhtsaExtractor` lands on top of it.
- `NhtsaExtractor(FlatFileExtractor)` per ADR 0008
- Pydantic schema for 29-field tab-delimited row
- Schema-drift detection on unexpected fields (NHTSA has added fields before)
- Weekly cron workflow
- Large bronze table; test with realistic row counts

**Step 3 — First extraction and bronze findings** (pending)
- After first extraction, document publication cadence, whether the modification date watermark is reliable, and any schema surprises in `documentation/nhtsa/`.

**Step 4 — Cassettes** (pending)
- One representative archive cassette + intentionally-malformed variant. The "page" concept doesn't apply to flat-file downloads; pagination-specific scenarios are busywork here.

**Step 5 — Silver** (pending)
- `models/staging/stg_nhtsa_recalls.sql` + extend silver models to incorporate NHTSA

---

### 5d. USCG scraping (brittle source)

> **Schema follows ADR 0027** — bronze does storage-forced transforms only;
> value-level normalization lives in `stg_uscg_recalls.sql`, not in
> `src/schemas/uscg.py`.

**Step 1 — Source exploration** (pending)
- Direct inspection of the USCG target HTML before writing the scraper. Document the observed HTML structure, publication frequency, and whether historical records are accessible via pagination or only the current page. Document in `documentation/uscg/`.

**Step 2 — Schema, extractor, migration** (pending)
- `src/extractors/_html_scraping.py` — `HtmlScrapingExtractor` operation-type subclass of the `Extractor` ABC (deferred from Phase 2 to its first use here). Shape is informed by USCG: polite-scraper throttling → fetch HTML → archive raw to R2 → BeautifulSoup parse → bronze load. Unit-tested in isolation before `UscgScrapingExtractor` lands on top of it.
- `UscgScrapingExtractor(HtmlScrapingExtractor)` using BeautifulSoup
- Raw HTML archival to R2 (polite-scraper behavior)
- Schema drift on HTML structure changes raises `ValidationError`
- Weekly cron workflow

**Step 3 — First extraction and bronze findings** (pending)
- After first extraction, document the observed publication cadence, pagination behavior, and any HTML structure surprises in `documentation/uscg/`.

**Step 4 — Cassettes** (pending)
- Cassette recording means capturing the real scraped HTML structure (not a hand-crafted fixture), since HTML schema drift is the primary failure mode. Record current-page HTML + a structurally-drifted variant to exercise the scraper's failure path.

**Step 5 — Silver** (pending)
- `models/staging/stg_uscg_recalls.sql` + extend silver models to incorporate USCG

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
| ADR 0012 source-config loader and registry | **Phase 6** (preferred) or Phase 7 prerequisite | Pending |
| ADR 0026 manifest implementation | **Phase 6** (USDA-only initially per accepted ADR) | Pending |
| ADR 0027 bronze storage-forced transforms refactor | **Phase 5b.2 Step 4.5** (already on critical path) | Cross-referenced from §5b.2 |
| `source_watermarks` seeding fix | **Phase 7 prerequisite** | Pending |
| FDA firm role reconciliation | **Phase 6 prerequisite** (firm entity resolution) | Pending |
| Shared annotated types and invariants audit | **Phase 5c prerequisite** | Resolved 2026-05-01 — documented negative result; see section below |
| USDA recall ETag re-evaluation | **Phase 7 prerequisite** | Pending |
| USDA establishment ETag enablement | **Phase 7 prerequisite** (gate-paired with USDA recall) | Code scaffolded 2026-05-03 (`etag_enabled=False`); awaiting viability gate |

### ADR 0012 implementation: source-config loader and registry

The `config/sources/*.yaml` files were filed as Phase 1 deliverables, but the loader, Pydantic-discriminated-union dispatch, and registry described in ADR 0012 were never implemented. CLI dispatch in `src/cli/main.py` instantiates extractors with hardcoded constructor kwargs, so YAML edits have no runtime effect. Affects all five sources equally.

Surfaced during Phase 5b USDA extraction when an `etag_enabled: false` YAML edit had no effect on the running extractor; see detour L3 in `documentation/usda/first_extraction_findings.md` and the header comment in `config/sources/usda.yaml`. ADR 0012 amended 2026-05-01 to document the deferral explicitly.

**Acceptance criteria:** editing `config/sources/usda.yaml` to set `etag_enabled: true` takes effect on the next extractor run without a code change. CLI invokes a registry lookup keyed on the source's `extractor_type` discriminator. Per-environment overlays (dev vs. prod) are clean.

Best landed in Phase 6 alongside silver work — Phase 6 already touches the extractor configuration surface for cross-source firm resolution and benefits from the cleaner config story. Hard deadline is Phase 7 cron turn-on, after which silent YAML drift between expected and actual config behavior compounds.

### ADR 0026 implementation: per-run snapshot-presence manifest

Tracked in `documentation/decisions/0026-lifecycle-tracking-snapshot-presence-manifest.md`. Promoted to Accepted 2026-05-01 with USDA-only initial scope, Option A (separate `extraction_run_identities` table) representation, and Phase 6 timing.

Lands in Phase 6 alongside the silver `recall_event_history` model. Bronze-side change is the new table + a per-run insert in `BronzeLoader.load()`; silver-side change is the `recall_lifecycle.sql` model deriving `first_seen_at`, `last_seen_at`, `is_currently_active`, `was_ever_retracted`, `edit_count` columns.

Manifest backfill from historical R2 payloads is covered by ADR 0028 Mechanism C (`scripts/backfill_manifest.py`).

### ADR 0027 implementation: bronze does storage-forced transforms only

Tracked in `documentation/decisions/0027-bronze-storage-forced-transforms-only.md`. Promoted to Accepted 2026-05-01.

**Not a free-floating follow-up** — placed on the critical path as Phase 5b.2 Step 4.5 (see §5b.2 above), gating Phase 5c so NHTSA and USCG inherit the corrected pattern from day one. Listed here as a cross-reference.

### `source_watermarks` seeding for new sources — Phase 7 prerequisite

Migration 0001 hardcodes a five-source list (`cpsc/fda/usda/nhtsa/uscg`) and seeds `source_watermarks` with one row per source. `extraction_runs.source` is a FK to that table, so any new source needs a one-row seed migration before its `_record_run` call can succeed (otherwise the FK insert fails silently inside the broad except — surfaced during Phase 5b.2 first extraction when `usda_establishments` warning'd `extraction_run.record_failed` while bronze loaded normally).

Two cleaner long-term options:
- **(a)** Drop the FK in favor of a CHECK constraint listing valid sources, updated as sources are added.
- **(b)** Drop the constraint entirely and let the application enforce the source enum.

Either avoids the per-new-source seed-migration ritual. Also: replicate the diagnostic-logging fix from `src/extractors/usda_establishment.py::_record_run` (capture exception `type` + `message` instead of swallowing) across `cpsc.py`, `fda.py`, `usda.py`. The current swallowing mode predates the fix and would mask similar failures on the older extractors.

Lands before Phase 7 cron turn-on so `extraction_runs` write-failures during cron are loud, not silent.

### FDA firm role reconciliation — Phase 6 prerequisite

`firm.sql` and `recall_event_firm.sql` label FDA's `firm_legal_nam` with `role='manufacturer'`, but semantically that field is the *recalling establishment* (analogous to USDA's `establishment` which uses `role='establishment'`). Relabel FDA's role to `'establishment'` to align cross-source firm rollups. Touches the `accepted_values` enum on `recall_event_firm.role` and downstream queries that filter by role.

Lands in Phase 6 alongside firm entity resolution work — the resolution logic across CPSC, FDA, and USDA is cleaner if all three agree on the role vocabulary first.

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

### USDA establishment ETag enablement — Phase 7 prerequisite

The establishment endpoint emits `ETag` and `Last-Modified` under browser fingerprint (Finding A revision 2026-05-03 + A/B verification at `bruno/usda/establishment_exploration/get_all_establishments_with_browser_headers.yml`). The capture path (migration 0010) collects per-run ETag observations alongside the recall endpoint's data; both share the same `etag_viability.sql` machinery.

**Code scaffolded 2026-05-03 with `etag_enabled=False` default.** `UsdaEstablishmentExtractor` now has 1:1 mirrors of `UsdaExtractor`'s `_fetch`, `_read_etag_state`, `_update_watermark_state`, `_touch_freshness`, and `_guard_etag_contradiction` methods (with "Mirrors UsdaExtractor.<method>; keep in sync" comments). The 304 lifecycle (land_raw skip → load_bronze touch_freshness) and the contradiction-guard test cases (`test_not_modified_304` / `test_etag_contradiction_guard`) parallel the recall side. Currently the extractor still issues a plain GET on every run because the flag defaults OFF — no behavior change vs. pre-scaffolding.

**Remaining work to enable** (gated on viability):

1. Verify `etag_viability.sql -v src=usda_establishments` shows the green-light verdict from query 5 (`false_304_count = 0` over ≥7 transitions including a real-update day; 14+ days of capture preferred).
2. Flip `etag_enabled=True` — either via constructor kwarg in `src/cli/main.py` or by changing the class default in `src/extractors/usda_establishment.py:UsdaEstablishmentExtractor`.
3. Optionally: do a 1-2 day dev smoke first by setting `etag_enabled=True` on a feature branch, running `recalls extract usda_establishments` repeatedly, and observing `If-None-Match` going out + 304s in `extraction_runs.response_status_code`.

**Decision rule:** the two endpoints share Akamai infrastructure and may exhibit identical ETag reliability, but do not assume so without evidence — they get studied independently. A `false_304_count > 0` for either source is disqualifying for that source regardless of how the other behaves.

Best landed alongside the recall ETag flip if both pass viability simultaneously, or independently if one passes and the other doesn't. Cost of leaving disabled through cron is ~810 KB / day downloads on idle days that could have been 304s. Lands before Phase 7 cron turn-on so the daily bandwidth profile is settled before recurring runs accumulate.

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

---

## Phase 6 — Full silver + gold materialization

**Goal:** unified data model across all five sources.

**Deliverables:**

- Silver `recall_event`, `recall_product`, `firm`, `recall_event_firm` fed from all five sources' staging models
- Firm entity resolution: FDA's `firmfeinum` as the anchor per ADR 0002; fuzzy-match (RapidFuzz) across sources for non-FDA firms
- Full dbt test suite per ADR 0015 (60–80 generic tests + 5 singular + freshness)
- Gold: aggregate views for dashboards, denormalized search index
- `recall_event_history` silver dbt model per ADR 0022 — uniform `LAG()` window function over bronze snapshot tables for all five sources (CPSC, FDA, USDA, NHTSA, USCG); no source-asymmetric path. Model partitions by `(source, source_recall_id)`, orders by `extraction_timestamp`, and emits one row per changed field per snapshot interval. **Joins to `extraction_runs.change_type` and excludes rows from non-routine runs** (`schema_rebaseline`, `hash_helper_rebaseline`) from edit detection so parser-driven re-version waves don't synthesize false-edit events — see ADR 0027 + `documentation/operations/re_baseline_playbook.md`. FDA's native history endpoints (`/search/productHistory/{productid}` and `/search/eventproducthistory/{eventid}`) were confirmed empty across all tested lifecycle states in Phase 5a; if they ever start populating, file a new ADR and add: (a) an Alembic migration for `fda_product_history_bronze` and `fda_event_product_history_bronze`, (b) an extraction path for those tables, and (c) a `UNION` branch in this model to merge native-history rows with the snapshot-derived rows. Until then those tables do not exist.
- `scripts/re_ingest.py` — re-ingest CLI per ADR 0014 for schema-drift recovery
- Alembic migrations for all silver and gold tables
- Create final column-level ERD in `documentation/diagrams/` for silver postgres DB.

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
- **Pre-cron blocker — CPSC historical seeding (per ADR 0028):** before turning on weekly cron, run `deep-rescan-cpsc.yml` once with `LastPublishDateStart=2005-01-01` and `--change-type=historical_seed` to populate the 20-year (2005–2024) gap currently missing from bronze. This gap exists because the CPSC archive migration cadence (~2–3 records/day) will not reach the 2024 backfill point for years on its own. Documented in `documentation/cpsc/last_publish_date_semantics.md` Section 3 and ADR 0028 Mechanism A.
- Transform workflow (`dbt build` + `dbt test`) on time-shifted cron per ADR 0018
- Full PR-check workflow matching ADR 0018 (ruff, pyright, pytest unit + integration, dbt parse, 1–2 e2e smoke)
- Neon branching via the Neon API for integration-test DBs (per ADR 0015); `test_db_url` fixture in `conftest.py`
- `dbt docs generate` deploys to Cloudflare Pages on every main push
- Quarterly secret-rotation reminder workflow per ADR 0016
- Startup-check in every cron workflow that validates all required secrets are present before invoking extraction code (fail fast with a clear message rather than a `KeyError` mid-run)
- **Database-level mutation guard on `*_rejected` tables.** ADR 0013 designs the per-source rejected tables as append-only audit trail (schema-drift forensics, re-ingest source per ADR 0014, data-loss accounting). Enforce that as a Postgres invariant in production rather than relying on operator discipline: revoke `TRUNCATE`, `DELETE`, and `UPDATE` on every `*_rejected` table from the production application role, leaving only `INSERT` and `SELECT`. The migration role retains DDL rights so future Alembic migrations still work. Dev branches keep full privileges (truncating is fine when iterating on a buggy schema). Filed during Phase 5b.2 first extraction (2026-05-01) — context: 7,945 records were rejected on a missed `city` field; the temptation to truncate before the fix-and-retry highlighted the need for a structural guard in prod.
- **Re-baseline gate for bronze-shape PRs (ADR 0027).** Add `.github/PULL_REQUEST_TEMPLATE.md` with a "Does this change the bronze canonical dict?" checkbox, and a CI workflow `.github/workflows/re-baseline-check.yml` that fails any PR touching `src/schemas/*.py` or `src/bronze/hashing.py` whose body lacks a `RE-BASELINE: yes|no` line. Operator-side procedure documented at `documentation/operations/re_baseline_playbook.md`. Lands before cron turn-on so the first production schema PR hits the gate.

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

- Framework ADR (0030+ — 0024 and 0025 are reserved for Phase 8's API design and deployment-target ADRs; 0026–0029 are filed)
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
