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
- **Empirical verification of `LastPublishDate` update semantics:** identify a recall that has been edited by CPSC since first publication (status change, remedy update, recalled-product count revision) and confirm by extraction whether `LastPublishDate` advanced at the edit. Document findings in a short note in `documentation/cpsc/`. If the timestamp reliably advances, file a follow-up to re-open ADR 0010 and relax the CPSC deep rescan; if not, the deep-rescan workflow stands as designed.

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

Built in order of increasing complexity so earlier lessons inform later sources:

**Standing requirement for all four sources in Phase 5:**

**Incremental extractor vs. historical load path.** Every source has two distinct code paths that must not be conflated:

- **Incremental path** (`<Source>Extractor.extract()`) — uses the watermark cursor (e.g. `LastPublishDateStart`, `eventlmd`, file modification date) to fetch only records changed since the last run. This path includes a response-count guard that raises `TransientExtractionError` if the result set exceeds a source-specific ceiling (e.g. `_MAX_INCREMENTAL_RECORDS = 500` for CPSC). The guard prevents a silently-ignored cursor parameter from loading the full database undetected.
- **Historical load path** (`deep-rescan-<source>.yml` workflow) — fetches all records in a date range for initial seeding or gap backfill. This path calls a **separate method or extractor class**, never `<Source>Extractor.extract()`, because it must handle arbitrarily large result sets and the incremental count guard would immediately fire. The historical path has no count guard.

This split was established for CPSC in Phase 3 (CPSC API behavior confirmed: an invalid or missing `LastPublishDateStart` parameter returns the full ~9,700-record dataset silently). Apply the same pattern for each source in Phase 5: FDA iRES, USDA FSIS, NHTSA, and USCG each need both an incremental extractor with a source-appropriate count guard and a separate historical load path without one.

Phase 3 established a three-part empirical process for CPSC that must be repeated for each source:

0. **Bruno API exploration (REST API sources only — FDA, USDA).** Before writing the schema or extractor, use Bruno to interactively explore the source's endpoints. Build the collection in `bruno/<source>/` with an `environments/dev.bru` file referencing credentials via `{{variables}}` — never hardcoded in `.bru` request files. Commit the collection alongside the extractor; `.bru` files are plain text and diff cleanly in git. Use `bru run bruno/<source>/` for quick scripted smoke tests from the terminal. The collection informs which cassette scenarios are worth recording and serves as living API documentation. Not applicable to NHTSA (flat file) or USCG (HTML scrape).

1. **Live cassette recording.** After the schema and extractor are written, record a set of live VCR cassettes against the real source before committing. **The scenarios recorded must be tuned to the source's actual API shape** — there is no universal 4-cassette matrix. Use whichever combination meaningfully exercises the extractor's code paths:
   - For paginated APIs (e.g., FDA iRES): single-page, multi-page, partial last page, empty.
   - For non-paginated APIs (e.g., CPSC — one GET returns everything): recent, wide window, narrow window, empty. (Pagination-specific scenarios don't apply and recording them is busywork.)
   - For flat-file downloads (e.g., NHTSA ZIP): one representative archive plus an intentionally-malformed variant. The "page" concept doesn't apply.
   - For HTML scrapes (e.g., USCG): current-page HTML plus a structurally-drifted variant to exercise the scraper's failure mode.

   CPSC cassette recording revealed four schema bugs that hand-crafted respx mocks had hidden: a missing `SoldAtLabel` field, a missing `Caption` sub-field on images, a wrong alias casing (`InConjunctions` vs `Inconjunctions`), and a datetime string format difference. Treat cassette failures as schema bugs to fix, not test failures to skip.

2. **API data exploration.** After the first live extraction run, query the bronze table to surface publication patterns, gap distributions, and any data shape surprises — the same analysis done for CPSC in `documentation/cpsc/last_publish_date_semantics.md`. Key questions to answer for each source: Does the incremental cursor field reliably advance on genuine edits? Are there batch/migration events that flood the watermark? What is the publication cadence and are there historical gaps in the database? Document findings in the corresponding `documentation/<source>/` directory. These findings directly inform whether deep-rescan workflows can be relaxed or must be treated as the primary historical-load mechanism.

**5a. FDA iRES** (auth + signature cache-busting)

- `FDA_AUTHORIZATION_USER` and `FDA_AUTHORIZATION_KEY` added to GitHub Actions repository secrets and local `.env` per ADR 0016
- Pydantic schema, extractor, YAML config, Alembic migration
- Handle Authorization-User/Key headers per ADR 0012
- Handle `signature=` cache-busting parameter — extractor injects a unique value (e.g. `int(time.time())` or `uuid.uuid4()`) into every request URL because the iRES server caches by full URL including `signature`. Without this, a 401 from a bad credential is cached and returned even after the credential is fixed; stale 200s also leak across rapid retries. The pattern is documented in `bruno/fda/lookup/get_product_types.yml` (the `docs:` block enumerates the four iRES quirks).
- `eventlmd` incremental logic
- `bruno/fda/` — Bruno collection covering iRES endpoints (enforcement report list, single event detail, product history); `environments/dev.bru` stores `FDA_AUTHORIZATION_USER` and `FDA_AUTHORIZATION_KEY` as `{{variables}}`
- 9 VCR scenarios + cron workflow. **Custom VCR request matcher required for FDA**: cassettes must match on path + method + filtered query params, with `signature` excluded from the match (or stripped before comparison). Without this, every replay attempt fails because the recorded `signature` value will never match the timestamp/UUID generated at replay time. Implement once in `tests/conftest.py` (or wherever the VCR fixture is configured) as a custom matcher that produces a query-string variant with `signature` removed; apply it to FDA cassettes only — CPSC/USDA/NHTSA/USCG cassettes use VCR's default matchers. Reference: signature cache-busting rationale and observed behavior documented in `bruno/fda/lookup/get_product_types.yml`.
- **API identity check:** confirm whether `iRES_enforcement_reports_api_usage_documentation.pdf` and `enforcement_report_api_definitions.pdf` describe the same API (the agent audit on update semantics treated them as separate; likely the same). Document the resolution in `documentation/fda/` so future readers aren't confused. **UPDATE 2026-04-26: THIS HAS BEEN CONFIRMED, THESE TWO DOCUMENTS REFERENCE THE SAME API.**
- **Empirical verification of `eventlmddt` edit semantics:** confirm via the documented `productHistory` / `eventproducthistory` endpoints that edits produce an advanced `eventlmddt` and corresponding history rows. FDA docs claim this explicitly; the check is to trust-but-verify before relying on it in production.
- **Pre-bronze ADR revisions (per `documentation/fda/api_observations.md` findings H, L, M):**
  - **ADR 0007 textual correction:** drop the `dt` suffix from `eventlmddt` / `productlmddt` references — the actual API columns are `EVENTLMD` and `PRODUCTLMD`. Edit ADR 0007 in place with a brief revision note.
  - **ADR 0007 architectural revision:** FDA's native field-history endpoints (`/search/producthistory/{productid}`, `/search/eventproducthistory/{eventid}`) are sparsely populated across every event tested — 2026 ongoing (98815), 2026 terminated (98279, 98286), and 2002 archive-migration (25159) all returned `RESULTCOUNT: 0`. The original ADR 0007 split — "FDA gets native field-level history, the other four sources synthesize from bronze snapshots" — is empirically false. FDA must use bronze-snapshot synthesis like the other four sources. File a superseding ADR (or substantial amendment) before the silver `recall_event_history` view is implemented in Phase 6; the schema for `fda_product_history_bronze` and `fda_event_product_history_bronze` should still be created (cheap, mostly empty) since the endpoints may eventually populate, but they cannot be the primary lineage path.
  - **ADR 0010 architectural revision:** ADR 0010 states FDA needs no deep rescan because `eventlmddt` reliably advances on edits per agency docs. Finding M empirically shows FDA actively re-touches old recall records and bumps `EVENTLMD` wholesale (records from 2002–2019 surfaced in a "90-day window" query), which is functionally the same archive-migration silent-edit pattern that drove the deep-rescan workflows for CPSC and USDA. File a superseding ADR (or amendment) to add a weekly `deep-rescan-fda.yml` workflow matching CPSC's and USDA's deep-rescan posture; content-hash dedup (ADR 0007) handles the volume cost-effectively.
  - **Numbering note:** ADRs 0022 and 0023 are reserved for Phase 8 (API design + deployment target). New ADRs from these revisions would start at 0024, bumping Phase 9's framework ADR by one. Alternative: amend ADR 0007 and ADR 0010 in place rather than superseding, to avoid the numbering cascade — pick based on how much you value preserving the original-vs-current distinction.

**5b. USDA FSIS** (bilingual dedup)

- Schema, extractor, YAML config, migration
- Bilingual edge case handled in `check_invariants()` per ADR 0006 + ADR 0013
- `bruno/usda/` — Bruno collection covering FSIS recall endpoints; `environments/dev.bru` for any auth parameters
- 9 VCR scenarios + cron workflow
- `.github/workflows/deep-rescan-usda.yml` with `workflow_dispatch` trigger per ADR 0010's deep-rescan addendum
- **Empirical verification of `field_last_modified_date`:** confirm the field exists in USDA FSIS API responses (the agent audit did not find it documented in the PDF, but ADR 0010 relies on it — this is the priority unknown to resolve). If present, confirm via a known-edited recall that the field advances on edits. Document findings in `documentation/usda/`. If the field does not exist or is unreliable, the USDA deep-rescan workflow becomes the primary extraction mechanism rather than a safety net.

**5c. NHTSA flat-file** (ZIP + tab-delimited + schema evolution)

- `src/extractors/_flat_file.py` — `FlatFileExtractor` operation-type subclass of the `Extractor` ABC (deferred from Phase 2 to its first use here). Shape is informed by NHTSA: ZIP download → stream-decompress → row-by-row parse → bronze load. Unit-tested in isolation before `NhtsaExtractor` lands on top of it.
- `NhtsaExtractor(FlatFileExtractor)` per ADR 0008
- Pydantic schema for 29-field tab-delimited row
- Schema-drift detection on unexpected fields (NHTSA has added fields before)
- Weekly cron workflow
- Large bronze table; test with realistic row counts
- **Live cassette recording + data exploration** per the standing requirement above. For NHTSA, the exploration should specifically address: how often does NHTSA release a new ZIP vs update an existing one, and does the file change date reliably reflect content changes or just re-packaging? Document in `documentation/nhtsa/`.

**5d. USCG scraping** (brittle source)

- `src/extractors/_html_scraping.py` — `HtmlScrapingExtractor` operation-type subclass of the `Extractor` ABC (deferred from Phase 2 to its first use here). Shape is informed by USCG: polite-scraper throttling → fetch HTML → archive raw to R2 → BeautifulSoup parse → bronze load. Unit-tested in isolation before `UscgScrapingExtractor` lands on top of it.
- `UscgScrapingExtractor(HtmlScrapingExtractor)` using BeautifulSoup
- Raw HTML archival to R2 (polite-scraper behavior)
- Schema drift on HTML structure changes raises `ValidationError`
- Weekly cron workflow
- **Live cassette recording + data exploration** per the standing requirement above. For USCG, cassette recording means capturing the real scraped HTML structure (not a hand-crafted fixture), since HTML schema drift is the primary failure mode. The exploration should document the observed HTML structure, publication frequency, and whether historical records are accessible via pagination or only the current page. Document in `documentation/uscg/`.

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

- All five per-source extract workflows on cron per ADR 0010 cadences
- CPSC and USDA deep-rescan workflows on weekly cron per ADR 0010's deep-rescan addendum (relaxable if Phase 3 / 5b empirical verification shows their timestamps reliably advance on edits)
- Transform workflow (`dbt build` + `dbt test`) on time-shifted cron per ADR 0018
- Full PR-check workflow matching ADR 0018 (ruff, pyright, pytest unit + integration, dbt parse, 1–2 e2e smoke)
- Neon branching via the Neon API for integration-test DBs (per ADR 0015); `test_db_url` fixture in `conftest.py`
- `dbt docs generate` deploys to Cloudflare Pages on every main push
- Quarterly secret-rotation reminder workflow per ADR 0016
- Startup-check in every cron workflow that validates all required secrets are present before invoking extraction code (fail fast with a clear message rather than a `KeyError` mid-run)

**Quality gates:**

- First full end-to-end production run (cron extracts + transform + docs deploy) succeeds
- Dashboards (once built) show real data
- PR-check pipeline runs under 10 minutes

---

## Phase 8 — Serving layer (FastAPI)

**Goal:** public API for recall data. Foundation for any frontend.

**Prerequisites:**

- **ADR 0022 — Serving-layer API design** filed and accepted. Covers endpoint shapes, response schemas, pagination, rate-limit posture, auth posture (public read-only per the project vision), OpenAPI generation strategy, and the relationship between API endpoints and dbt gold views.
- **ADR 0023 — API deployment target** filed and accepted. Evaluates Fly.io vs. Render vs. Cloudflare Workers free tiers against cold-start behavior, Python runtime compatibility, read-only Neon connection patterns (from `main` per ADR 0005), and GitHub Actions CI/CD integration.

Rationale for two ADRs rather than one: API design and deployment target are separable concerns, and deployment constraints sometimes drive design choices (e.g., Cloudflare Workers' Python limitations would reshape endpoint design). Keeping them separate also matches this project's pattern of narrow, single-decision ADRs.

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

- Framework ADR (0024+ — 0022 and 0023 reserved for Phase 8's API design and deployment-target ADRs)
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
