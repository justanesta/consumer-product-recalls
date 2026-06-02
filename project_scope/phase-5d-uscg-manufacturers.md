# Plan: USCG Manufacturers Directory Ingestion (Phase 5d Step 7 follow-up)

- **Status:** ✅ Complete — shipped in **PR #40** (2026-05-30), branch `feature/phase-5d-uscg-manufacturers`. As-built record: `documentation/uscg/manufacturer_scraping_observations.md`; see `implementation_plan.md` Phase 5d Step 7. The promised standalone `uscg_manufacturers_directory_plan.md` was **never split out** — the six-step plan below stayed inline and the work landed directly from it, so **this document IS the directory-ingestion plan.** Archive to `project_scope/archive/` once the Phase 6b SCD-2 follow-on (ADR 0035) lands.
- **Open questions (bottom of doc): RESOLVED** — Q1→§B, Q2→§C + the detail-capture plan, Q3→foreign-MIC finding, Q4→status field/§M, Q5→cadence finding, all in `documentation/uscg/manufacturer_scraping_observations.md`.

## Context

During the Phase 6a USCG audit (2026-05-29), `https://uscgboating.org/content/manufacturers-identification.php` was surfaced as a sibling page to the recalls endpoint already in pipeline. It's a paginated HTML directory of **16,263 USCG-registered boat manufacturers** with MIC + Company + Address + City + State per row — roughly **10× the recall corpus**. Currently uncaptured.

**Why this matters now (between Phase 6a and Phase 6a.5):**

1. **Resolves the §3 Bug 3 soft-fail residue.** The USCG audit's Bug 3 fix accepts that ~33 recall rows lose firm dim entry under Option 3 soft-fail (23 NULL/NULL + 10 mic-only-no-name). Most of those 10 mic-only-no-name rows have their `mic` in the directory paired with a `company_name`. A `LEFT JOIN stg_uscg_manufacturers ON mic = mic` pattern (mirroring USDA's recall ↔ establishment join in `firm.sql:75-86`) backfills the name. **Number of rescued rows is empirical; framing it as "0 to 10" is honest until measured.**
2. **Phase 6b cross-source firm rollup needs richer firm metadata.** The empirical USCG run showed firms like Mercury Marine (36 rows) appearing with NULL MIC in recall data — but Mercury Marine has its own assigned MIC in the directory. Directory data canonicalizes the firm identity for fuzzy-match work.
3. **Geographic enrichment** — currently USCG recalls carry zero firm-address data. The directory has address/city/state. Useful for landing pages.
4. **Sequencing constraint per user:** "after this branch, before Phase 6a.5". Two operational reasons:
   - **Phase 6a.5 covers historical backfill for CPSC + NHTSA + FDA** (per `project_scope/phase-6-execution-plan.md` lines 72-93). USDA + USCG are explicitly out of scope because both are at full corpus already. By doing the USCG manufacturers initial seed as Step 3 of this work, that source is also at full corpus before Phase 6a.5 — no need to retroactively expand 6a.5 scope, no need for a separate later seed event.
   - **Phase 6b cross-source firm work** (RapidFuzz + per-source name cleaning) wants the directory data as a join input. If 6b starts before the directory lands, the firm.sql USCG branch + Phase 6b NHTSA-style name cleaning have to be revised when the directory arrives.
5. **Reopens Phase 5d**, not Phase 6. Phase 5d Steps 1-6 closed 2026-05-18 covering USCG recalls. This is the same source-family, same extractor pattern, deferred until the recall pipeline shipped.

**Architectural precedent:** Identical to how USDA has two sources (`usda` recalls + `usda_establishments` directory). The two-table firm pattern (`firm` cross-source dim + `firm_establishment_attributes` USDA-only sibling) is already established. This work creates the USCG analog: `firm_manufacturer_attributes`.

**Output deliverable (as-built):** The six-step plan stayed **inline in this document** — the separate `uscg_manufacturers_directory_plan.md` was never split out (unnecessary once the work landed directly from here). A reference was inserted into `project_scope/implementation_plan.md` Phase 5d Step 7. ✓

## Recommended approach

**Mirror the USCG recalls pipeline structurally + the USDA establishments integration semantically.** The HTML scraping infrastructure already exists (`HtmlScrapingExtractor` base class per `src/extractors/_html_scraping.py:99-308`); the two-table firm pattern is already established (`firm_establishment_attributes.sql:1-37`). This work composes existing primitives, no new architecture.

**Six-step plan, mirroring Phase 5d Steps 1-6 cadence:**

1. **Step 1 — Scraping observations probe.** Manual `curl` against the directory + 2-3 specific pages. Empirically establish: per-row field shape (confirm MIC vs row-ID question — initial fetch showed values like `101`, `102`, `103` which may be row numbers, not MICs; verify in HTML source); pagination model (`?pageNum_manufacturers=N` confirmed); "Records Found: 16263" short-circuit signal confirmed; header inventory (no ETag/Last-Modified per recall-page Finding K — expected); is there a per-manufacturer details page or just the listing? Document as `documentation/uscg/manufacturer_scraping_observations.md` parallel to `scraping_observations.md`.
2. **Step 2 — Extractor + schema + bronze table.** New `src/extractors/uscg_manufacturer.py` reusing `HtmlScrapingExtractor`. New `src/schemas/uscg_manufacturer.py` with `UscgManufacturerRecord` Pydantic model. New `config/sources/uscg_manufacturers.yaml`. Alembic migration `0015_uscg_manufacturers_bronze.py` creates `uscg_manufacturers_bronze` + `uscg_manufacturers_rejected` + `source_watermarks` seed row. Source registry entries in `src/config/source_registry.py:59-79`.
3. **Step 3 — First extraction + bronze findings.** Run `recalls deep-rescan uscg_manufacturers --change-type=historical_seed`. Document corpus findings (analogous to Findings A-S of the recalls scraping observations). Surface any data-quality patterns (MIC format consistency, address-field formats, etc.).
4. **Step 4 — Cassettes.** Live-record cassettes mirroring `tests/fixtures/cassettes/uscg/` pattern: `test_real_manufacturer_listing_page_parses.yaml`, `test_pagination_boundary.yaml`, `test_response_metadata_captured.yaml`. Unit tests for parser + integration tests against cassettes.
5. **Step 5 — Silver landing.** `dbt/models/staging/stg_uscg_manufacturers.sql` (latest-per-mic projection). `dbt/models/silver/firm_manufacturer_attributes.sql` (USCG-only sibling to `firm` dim, keyed on `mic` analogous to USDA's `firm_establishment_attributes.establishment_id`). **Update `dbt/models/silver/firm.sql` USCG branch** to LEFT JOIN `stg_uscg_manufacturers` for enrichment — pattern from `firm.sql:75-86` USDA branch.
6. **Step 6 — Audit doc update.** Fold findings into `documentation/uscg/field_audit_2026_w22.md` §3 Bug 3 (final mic-only-no-name rescue count) + §6 firm-relationship deep dive (now with directory enrichment) + new §1c entry for the directory shape.

**Key design choices (locked, not optional):**

- **Source type: `html_scraping`** (not `rest_api`). Same as USCG recalls.
- **Source name: `uscg_manufacturers`** mirrors `usda_establishments` naming.
- **Identity: `mic`** as the natural key (3-char alpha per HIN spec; the `111` outlier per audit §5 stays in bronze and quarantines or normalizes at silver per the audit decision).
- **Short-circuit pattern: same as recalls.** `Records Found: NNNN` total → `source_watermarks.last_records_count` per migration 0014 pattern. The directory updates infrequently; short-circuit hit rate will be high.
- **Bronze identity dedup: `identity_fields=("source_recall_id",)` where `source_recall_id = mic`.** No `within_batch_dedup`, no `allow_null_identity`. Mirrors USCG recall extractor (`uscg.py` BronzeLoader config).
- **Polite-scraper defaults:** `scrape_delay_seconds=1` (same as recalls). ~651 pages × 1 sec = ~11 min full walk; acceptable for one-time seed + daily/weekly incremental.
- **firm.sql LEFT JOIN approach (not bronze-side merge).** Per USDA precedent decision; directory is enrichment, not source-of-truth identity.

## Critical files

**To create (Step 2):**
- `config/sources/uscg_manufacturers.yaml` — HTML scraping source config
- `src/extractors/uscg_manufacturer.py` — `UscgManufacturerExtractor(HtmlScrapingExtractor[UscgManufacturerRecord])` + `UscgManufacturerDeepRescanLoader`. Mirror `src/extractors/uscg.py:273-861` structurally.
- `src/schemas/uscg_manufacturer.py` — `UscgManufacturerRecord` Pydantic model with `extra='forbid'` + `strict=True`. Fields TBD per Step 1 empirical observation; minimum: source_recall_id (=mic), company_name, address, city, state.
- `migrations/versions/0015_uscg_manufacturers_bronze.py` — mirrors `0006_usda_fsis_establishments_bronze.py` shape: bronze table + rejected table + 3 indexes ((source_recall_id, extraction_timestamp DESC), upper(company_name), state).
- `migrations/versions/0016_seed_uscg_manufacturers_watermark.py` — seed `source_watermarks` row for the new source.

**To create (Step 4):**
- `tests/fixtures/uscg/sample_manufacturer_listing_page.html` — hand-saved sample
- `tests/fixtures/cassettes/uscg/test_real_manufacturer_listing_page_parses.yaml`
- `tests/fixtures/cassettes/uscg/test_manufacturer_pagination_boundary_returns_empty.yaml`
- `tests/extractors/test_uscg_manufacturer_extractor.py`
- `tests/integration/test_uscg_manufacturer_live_cassettes.py`

**To create (Step 5):**
- `dbt/models/staging/stg_uscg_manufacturers.sql` + `.yml`
- `dbt/models/silver/firm_manufacturer_attributes.sql` + add to `_silver.yml`

**To modify (Step 2):**
- `src/config/source_registry.py:59-79` — register both extractor classes in `EXTRACTOR_BY_SOURCE_NAME` and `DEEP_RESCAN_BY_SOURCE_NAME`

**To modify (Step 5):**
- `dbt/models/silver/firm.sql` — update USCG branch (lines 99-110) to LEFT JOIN `stg_uscg_manufacturers` for the 10 mic-only-no-name row rescue + general enrichment

**To create (Step 1 + Step 3 + Step 6):**
- `documentation/uscg/manufacturer_scraping_observations.md` — Findings A onwards mirroring `scraping_observations.md`
- Update `documentation/uscg/field_audit_2026_w22.md` §3 Bug 3 with final mic-only-no-name rescue count + §6 firm-relationship update

**Final deliverable (as-built):**
- ~~`project_scope/uscg_manufacturers_directory_plan.md`~~ — never created; this document is the plan, and the work shipped directly from it (PR #40).
- Reference inserted into `project_scope/implementation_plan.md` Phase 5d Step 7. ✓

## Existing primitives to reuse

| Component | Existing file | Reuse pattern |
|---|---|---|
| HTML scraping base | `src/extractors/_html_scraping.py:99-308` | Inherit `HtmlScrapingExtractor[T]`; implement `_parse_listing_page()` for the manufacturer table. Throttling, R2 archiving, polite UA, Retry-After handling all free |
| BronzeLoader integration | `src/bronze/loader.py:100-117` | Same `(identity_fields, content_hash, hash_exclude_fields)` mechanism; just configure for `source_recall_id=mic` |
| Source registry | `src/config/source_registry.py:59-79` | Add 2 entries to existing dicts |
| Two-table firm pattern | `dbt/models/silver/firm_establishment_attributes.sql:1-37` | Structural mirror: `firm_manufacturer_attributes.sql` with same shape, USCG-specific columns |
| firm.sql LEFT JOIN enrichment | `dbt/models/silver/firm.sql:75-86` USDA branch | Update USCG branch (lines 99-110) with analogous LEFT JOIN |
| Short-circuit | `src/extractors/uscg.py:354-356` + migration 0014 | Reuse `source_watermarks.last_records_count` + `extraction_runs.was_short_circuited` columns; no new schema |
| Cassette philosophy | `tests/fixtures/cassettes/uscg/README.md` | Real-content cassettes; hand-saved listing samples |

## Sequencing

**Position in the project's phase timeline:**

```
[current branch: feature/phase-6a-foundation-audit]
   ├── Phase 6a audits complete (5 sources)
   ├── Phase 6a.5 historical-backfill plan in place
   ├── Phase 6 plan + audit doc updates landed
   └── Merge to main

[next branch: feature/phase-5d-step7-uscg-manufacturers] ← THIS WORK
   ├── Step 1: scraping observations probe (~2 hours)
   ├── Step 2: extractor + schema + migration (~1 day)
   ├── Step 3: first extraction + corpus findings doc (~half day)
   ├── Step 4: cassettes + tests (~half day)
   ├── Step 5: silver landing + firm.sql update (~half day)
   ├── Step 6: audit doc fold-in (~1 hour)
   └── Merge to main

[then: Phase 6a.5 historical-backfill]
   └── Now also includes USCG manufacturers in any sizing decisions
        (16,263 records × ~200 bytes ≈ ~3 MB bronze — negligible vs FDA/NHTSA)

[then: Phase 6b firm normalization]
   └── Now has directory data to enrich the LEFT JOIN
```

**Total work estimate:** ~3 days end-to-end. Same scale as Phase 5d Steps 1-3 for USCG recalls.

**Hard sequencing constraints:**
- Step 1 precedes Step 2 (empirical observations drive the schema)
- Step 2 precedes Step 3 (extractor must exist before first extraction)
- Step 5 precedes Step 6 (silver fold-in needs landing tables in place)
- All of Steps 1-6 precede Phase 6a.5 (per user direction)
- Steps 5-6 incorporate audit doc Bug 3 + §6 final rescue counts — close the loop on the USCG audit findings

## Verification

**Per Phase 5d Step pattern:**

- **Step 1**: `curl -A "..." https://uscgboating.org/content/manufacturers-identification.php` + `curl ?pageNum_manufacturers=10` + `curl ?pageNum_manufacturers=651` (pagination boundary). Confirm HTML shape, fields, headers, total count. Hand-save 1-2 representative HTML files as fixtures.

- **Step 2**: `pytest tests/extractors/test_uscg_manufacturer_extractor.py` (unit tests pass against deterministic fixtures). `recalls extract uscg_manufacturers --dry-run` (CLI dispatch wired correctly).

- **Step 3**: `recalls deep-rescan uscg_manufacturers --change-type=historical_seed` lands bronze rows. Run `select count(*) from uscg_manufacturers_bronze` — expect ~16,263. Run quarantine rate query — expect <5%.

- **Step 4**: `pytest tests/integration/test_uscg_manufacturer_live_cassettes.py` against committed cassettes (offline-safe).

- **Step 5**: `dbt build --select stg_uscg_manufacturers firm_manufacturer_attributes firm` succeeds. Query `select count(*) from firm_manufacturer_attributes` ≈ 16,263. Query `select count(distinct firm_id) from firm where firm_id in (select firm_id from recall_event_firm where source = 'USCG')` — compare to pre-this-work baseline; expect ~10 additional firm rows recovered (the Bug 3 mic-only-no-name rescue).

- **Step 6**: Audit doc §3 Bug 3 has empirical rescue count populated. `documentation/uscg/manufacturer_scraping_observations.md` exists with Findings A-N+ shape.

- **End-to-end smoke**: `recalls extract uscg_manufacturers` (incremental, post-historical-seed) hits the Finding J short-circuit on the next run (no records changed). Confirms `_should_short_circuit` short-circuit count matches recalls behavior.

## Open questions for the user — RESOLVED (Step 1 empirical work)

All five were answered during the Step 1 source inspection — resolutions are in `documentation/uscg/manufacturer_scraping_observations.md` (mapping in the Status banner at the top of this doc). Retained below for provenance:

1. **Are the visible values `101, 102, 103` in the directory page the actual MIC values or row indices?** Cannot tell from the WebFetch summary alone — the recall data's MIC is 2-4 chars alpha (`YDV`, `NLP`, `123`) per Finding A. Step 1's HTML source inspection resolves this.
2. **Per-manufacturer details page?** USCG recalls have a two-tier model (listing + details). Does the manufacturer directory have details pages or is it listing-only? Step 1 confirms.
3. **Country field?** The visible columns are MIC + Company + Address + City + State. No country. Are foreign-flagged manufacturers in scope? (USCG-issued MICs are US-only; foreign manufacturers selling boats in the US need a separate registration.) Step 1 confirms or surfaces.
4. **Active/inactive status?** USDA establishments have `status_regulated_est`. USCG directory may have an analog. Step 1 confirms.
5. **Update cadence?** USDA establishments updates ~weekly (per Finding A). What's USCG's? Probe with two fetches spaced 1 day apart.
