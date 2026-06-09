# Phase 5d Step 7 (detail) — USCG manufacturer detail-page capture (Path B, bronze-only)

- **Status:** ✅ Complete — bronze-capture shipped and **merged to `main` in PR #42** (commit `ffe9536`, 2026-05-30). The §11 SCD-2 silver half stays deferred to Phase 6 / ADR 0035. (Alembic migration, `dbt build`, live extraction, and live-cassette recording were user-run — see §10 + §14.) Archive to `project_scope/archive/` once the ADR 0035 SCD-2 follow-on lands.
- **Scope class:** **bronze-capture only** (per `project_scope/implementation_plan.md:490`). The SCD-2 silver model and the recall→manufacturer time-aware join are **out of scope** on this branch and deferred to Phase 6 (see §11).
- **Branch:** `feature/uscg-manufacturers-detail-addition`.
- **Parent plan:** `project_scope/implementation_plan.md` Phase 5d Step 7 follow-up (lines 482–490).
- **Sibling plan:** `project_scope/phase-5d-uscg-manufacturers.md` (the listing-only Step 7 plan this mirrors).
- **Deferred-half home (Phase 6):** `implementation_plan.md:707` (cross-source SCD-2 item) + `project_scope/archive/silver_v15_migration_plan.md:229–247` (cross-source application section).
- **Sequencing doc:** `project_scope/branch_sequencing_strategy.md` (updated alongside this doc to register the branch).
- **Empirical source-of-truth:** `documentation/uscg/manufacturer_scraping_observations.md` §M (M.1–M.6) + `documentation/uscg/field_audit_2026_w22.md`.
- **Sunset:** ✅ branch merged (#42); remaining trigger — the §11 SCD-2 design migrates into Phase 6 / ADR 0035, then archive this doc.

---

## 1. Goal + scope boundary

**Goal.** Capture the USCG manufacturer **detail-page** payload into bronze so the source-native succession history and richer firm attributes are durably recorded — feeding the eventual SCD-2 firm dimension and the time-sensitive recall→manufacturer join, **neither of which is built on this branch**. The listing-only extractor (Step 7, PR #40) captured 5 fields; the detail page (`manufacturers-identification-detail.php?id=N`, confirmed to answer a direct GET — `manufacturer_scraping_observations.md` §M.2) carries ~20: `company`, `dba`, `parent_company`, `parent_mic`, `past_company_1/2/3` (with `(OOB year)` lineage), `address` (full, untruncated), `city`, `state`, `zip`, `country`, `phone`, `fax`, `status`, `company_official`, `in_business`, `out_of_business`, `date_modified`, `type`, `additional_address`.

### In scope (this branch)

1. Migration `0017_uscg_manufacturer_details_bronze.py` — bronze + rejected table + indexes (§4).
2. Migration `0018_seed_uscg_manufacturer_details_watermark.py` — `source_watermarks` seed row (mirrors `0016`).
3. `src/schemas/uscg_manufacturer_detail.py` — `UscgManufacturerDetailRecord` (§5).
4. `src/extractors/uscg_manufacturer_detail.py` — `UscgManufacturerDetailExtractor` + `UscgManufacturerDetailDeepRescanLoader` (§6).
5. `config/sources/uscg_manufacturer_details.yaml` (§8).
6. `src/config/source_registry.py` — 2 imports + 2 dict entries (§8).
7. `src/cli/main.py` — no-op message entries for the new source **and** the already-registered `uscg_manufacturers` (currently missing — §8).
8. `.github/workflows/extract-uscg-manufacturers-detail.yml` + `deep-rescan-uscg-manufacturers-detail.yml` (dispatch-only; cron is Phase 7 — §8).
9. `dbt/models/staging/stg_uscg_manufacturer_details.sql` + `.yml` — thin latest-per-MIC staging only (§9).
10. Unit tests + HTML fixture (shipped). **Live integration cassettes: deferred to user recording** — can't be recorded offline (§10).

### Explicitly deferred to Phase 6 (NOT this branch), with named home

| Deferred item | Home |
|---|---|
| SCD-2 `firm_manufacturer_attributes` (snapshot) | `implementation_plan.md:707`; `archive/silver_v15_migration_plan.md:243`; new **ADR 0035** |
| HIN-build-date as-of-build-date join | `implementation_plan.md:707`; `archive/silver_v15_migration_plan.md:245`; Phase 6+ |
| `recall_event_firm` flag-as-time-sensitive column + `match_confidence` | Phase 6b (bundled to avoid `firm.sql` collision) |
| The cross-source SCD-2 **ADR 0035** | Filed early in Phase 6 (0034 reserved for NHTSA Layer-3 — `archive/silver_v15_migration_plan.md:128`) |

**Why the split.** Bundling the silver half here would "un-focus the branch and collide with Phase 6b on `firm.sql`" (`implementation_plan.md:490`). The USCG firm join lives at `firm.sql` `uscg_normalized` (line 103+) and `recall_event_firm.sql` `uscg_event_firms` (line 89+), which Phase 6b edits and which must stay in **lockstep** (the lockstep comment lives at `recall_event_firm.sql:22` and `:98`; `firm.sql` carries no explicit lockstep warning). This branch touches none of those files.

---

## 2. Why detail capture (the decision gate — resolved)

The decision gate is **resolved, not open**. The recall-directed probe (`scripts/uscg/probe_mic_reassignment_rate.py --recalled-only`, 714/718 recalled MICs, 2026-05-30) found **51.1% (365) with a prior company and 28.7% (205) `(OOB)`-recycled**, vs 28.1%/17.3% for a random 1,000-MIC sample — recalled MICs are **~1.8× more reassigned** (`manufacturer_scraping_observations.md` §M.6; `implementation_plan.md:489`). Two confirmed successions: `AXY` (ARMY SURPLUS → SOSA PERFORMANCE BOATS) and `COP` (CONSER/COPALIS → COPALO INC), §M.1.

Listing-only extraction discards the succession lineage and `Date Modified`; the current "latest-per-MIC" silver semantic always resolves to the **current** holder, misattributing recalls on boats built before a succession (§M.5). The affected-MIC artifact (recalled-only run) is `data/exploratory/uscg_manufacturers/recalled_reassigned_mics.json` (365 records). *(Provenance note: the random-1,000 default run wrote `reassigned_mics.json`; the 205/365 figures are the `--recalled-only` run written to `recalled_reassigned_mics.json` via `--out`.)*

---

## 3. Architecture decisions

- **D1 — Separate detail bronze table; do NOT widen the listing table.** New `uscg_manufacturer_details_bronze` keyed on `source_recall_id = MIC` (joinable to `uscg_manufacturers_bronze` on `source_recall_id`). Rationale: different fetch grain (one GET per `id`, not per listing page), different `content_hash` field set, different cadence; migration `0015` is closed.
- **D2 — Separate enrichment extractor; do NOT walk details inside the listing extractor.** `UscgManufacturerDetailExtractor` is a **distinct source** (own `_SOURCE` name, own YAML, own registry keys). Its `extract()` sources a work-list of `(source_recall_id, uscg_directory_id, detail_url)` tuples from `uscg_manufacturers_bronze` (latest-per-MIC) and fetches one detail page per MIC. This differs from both the listing extractor (walks ~651 listing pages; `_parse_details_page` deliberately raises `NotImplementedError` — `uscg_manufacturer.py:550-568`) and `UscgScrapingExtractor` (which walks listing + details in one pass for the much smaller recalls corpus).
- **D3 — Two-tier cadence (design only; cron is Phase 7).** Tier-1 (incremental default): re-walk only MICs whose listing row changed since the last detail run. Tier-2 (deep-rescan): full ~16.3k-row sweep (~4.5 h at the 1 s throttle). The watermark column the Tier-1 cursor needs is an open question (§16).
- **D4 — The `Date Modified` reality.** `Date Modified` is the change signal but is **detail-page-only**, so it cannot reduce the **fetch** cost (you must GET the page to read it) — only the **load** cost (content-hash dedup skips the insert). The only fetch-cost lever is D3's Tier-1 listing-delta cursor.

### Conceptual shape, runtimes & cadence (the two-tier model in numbers)

One logical entity (a manufacturer, keyed by `mic`); two extraction sources for two field-slices behind two URL shapes (D1/D2). Concrete fetch/runtime/cadence profile (≈1 s polite throttle):

| Operation | Fetches | Runtime | Cadence |
|---|---|---|---|
| `uscg_manufacturers` extract — short-circuit HIT (nothing changed) | 1 (page 0) + 1 DB lookup | ~3 s | daily |
| `uscg_manufacturers` extract — short-circuit MISS (a change) | ~651 listing pages | ~15 min | only on a change-day (directory is slowly-changing) |
| `uscg_manufacturers` deep-rescan (full seed) | ~651 pages | ~15 min | one-off / weekly safety net |
| **`uscg_manufacturer_details` extract — FIRST run** (detail table empty) | ~16,263 | **~4.5 h** | one-off (initial seed) |
| `uscg_manufacturer_details` extract — Tier-1 incremental (steady state) | only MICs whose listing row changed (e.g. 2) | seconds | daily / after-listing |
| `uscg_manufacturer_details` deep-rescan — Tier-2 full sweep | ~16,263 | ~4.5 h | periodic (e.g. quarterly) |

**Tier-1 vs Tier-2 coverage (the blind spot — load-bearing).** Tier-1 re-queues a MIC only when its **listing** row changes (`company`/`address`/`city`/`state`). The high-value event — a **reassignment** — changes the company name (a listing field), so Tier-1 catches it. Tier-1 is **blind to detail-only edits** (a `status` flip with no company/address change; `phone`/`DBA`/`Out of Business`/`Parent MIC` updates; an appended `Past Company`; a bare `Date Modified` bump) because the listing row's timestamp doesn't move. There is **no cheap per-MIC signal** for those (`Date Modified` lives on the detail page you would have to fetch), so the **Tier-2 full sweep is the only mechanism** that catches them — content-hash dedup makes the *load* a no-op; the ~4.5 h is purely fetch cost. Tier-2 is therefore **necessary for completeness but its cadence is relaxable** (quarterly, even annually): reassignments already ride Tier-1, so Tier-2 only keeps the lower-frequency detail-only fields current. Same blind-spot shape as the listing source's short-circuit, which the weekly deep-rescan backstops.

---

## 4. Bronze migration

- **`migrations/versions/0017_uscg_manufacturer_details_bronze.py`** (next free number after `0016`).
- Table `uscg_manufacturer_details_bronze`: standard bronze columns (`id`, `source_recall_id` = MIC `NOT NULL`, `content_hash`, `extraction_timestamp`, `raw_landing_path`) + detail payload (all nullable except identity):
  - **TEXT:** `company`, `dba`, `parent_company`, `parent_mic`, `past_company_1`, `past_company_2`, `past_company_3`, `address`, `city`, `state`, `zip`, `country`, `phone`, `fax`, `status`, `company_official`, `type`, `additional_address`.
  - **TIMESTAMPTZ** (coerced at the schema layer, §5): `in_business`, `out_of_business`, `date_modified`.
- **`zip` is TEXT** — must store 9-digit hyphen-free strings (e.g. `561640126`) and Canadian 6-char postal codes (e.g. `V8L3S1`). No numeric/length constraint.
- Rejected table `uscg_manufacturer_details_rejected` via the same `rejected_table_columns()` helper as `0015`.
- Index `(source_recall_id, extraction_timestamp DESC)` — supports `BronzeLoader._fetch_existing_hashes()` + latest-per-MIC staging (mirrors `0015`). An optional `date_modified` index is deferred (only needed if a date-cursor lands).
- **No DB `CHECK` on `status`.** Observed values `{In Business, Inactive, Federal or State Agency}`; a new value must be surfaced by the extractor drift fence (§6), not a DB constraint that would error the load.

---

## 5. Pydantic schema

- **`src/schemas/uscg_manufacturer_detail.py`**, `UscgManufacturerDetailRecord`, `ConfigDict(extra='forbid', strict=True)` (ADR 0014 project standard, matching `uscg_manufacturer.py`'s schema).
- Field set = the 20 fields from the **validated probe `_LABEL_MAP`** (`scripts/uscg/probe_mic_reassignment_rate.py:91-114`): `mic`→`source_recall_id` (via `validation_alias`), `company`→`company_name`, then `dba`, `parent_company`, `parent_mic`, `past_company_1/2/3`, `address`, `city`, `state`, `zip`, `country`, `phone`, `fax`, `status`, `company_official`, `in_business`, `out_of_business`, `date_modified`, `type`, `additional_address`.
- Date fields (`in_business`, `out_of_business`, `date_modified`) use a `BeforeValidator` wrapping a nullable `M/D/YYYY` parser — **reuse the recalls-details precedent** `_UscgNullableDetailsDate` / `_parse_nullable_uscg_details_date` (`src/schemas/uscg.py:83-104`); the manufacturer detail page uses the same `M/D/YYYY` format.
- **Documented quirks (module docstring + field comments), all preserved verbatim at bronze per ADR 0027:**
  - `company_official` sentinel `'-, -'` ("no official recorded"). Normalized at staging.
  - `zip` TEXT (9-digit + Canadian — §4).
  - **`in_business` is contaminated** by record-touch dates on active large manufacturers (MERCURY / VOLVO PENTA / CATERPILLAR show `in_business ≈ date_modified ≈ 2025/2026`; defunct `4WN` shows a real `1972`). Never treat as a "founded" signal in isolation (§M.6).
  - `address` is **full and untruncated** — the payoff vs the listing's ~30-char truncation (Finding F.1).
  - `past_company_N` ∈ {`COMPANY (OOB YYYY)`, `COMPANY (OOB)`, bare `COMPANY`}. Only ~13 of 205 recycled MICs carry a parseable year.
  - **`out_of_business`** (top-level) = the **current** holder is defunct (the SCD `valid_to` for the current interval). **`Past Company (OOB)`** = a **prior** holder ceased and the MIC was recycled. **Do not conflate** (§M.6).
  - `status` TEXT, 3 observed values; no `Literal` at bronze.
  - `type` is a `<br/>`-concatenated run-on string (a verbal vessel-type taxonomy; verbatim at bronze, normalization deferred to silver — see §16).
  - The detail page's `Comments` block is HTML-commented-out and must **not** be in the label map; if it ever goes live the drift fence (§6) surfaces it.

> **Learning-exercise hook** (per the standing reminder): promoting a probe parser to production + adding a RAISE-on-unknown-label drift fence is a genuinely novel pattern for this project. Flag a 10–15 min exercise when this lands; do not auto-run.

---

## 6. Detail extractor

- **`src/extractors/uscg_manufacturer_detail.py`**.
- `UscgManufacturerDetailExtractor(HtmlScrapingExtractor[UscgManufacturerDetailRecord])` — reuse the base for `_fetch_page` (3-attempt tenacity retry), `_archive_page`, `land_raw` (single NDJSON-per-run gzip → R2), `_capture_response_metadata`, `_throttle` (1 s), and the polite honest UA (`src/extractors/_html_scraping.py`).
- **`_parse_details_page` — promote the probe parser to production.** Lift `parse_detail` + `_value_for_label` (Strategy 1 only) + `_LABEL_MAP` from `scripts/uscg/probe_mic_reassignment_rate.py` into a module-level `_DETAIL_LABEL_MAP` + `_normalize_label` (mirroring `uscg.py:224-253`). **Critical change probe → production:** replace the probe's soft "record unknown label and continue" with **RAISE `TransientExtractionError` on any unknown bolded label** — exactly the recalls drift fence (`uscg.py:629-634`). DOM pattern (verified against the cached `655.html`/`27.html`): 5-cell rows `[left-label][left-value][&nbsp; spacer][right-label][right-value]`; the value is the label cell's **immediate** next-sibling `<td>` — do **not** skip an empty value cell to the spacer (that is the empty-cell-bleed bug that produced `parent_company == "Parent MIC:"`). Ignore the `<h2>COMPANY</h2>` page title so the `<strong>`/`<b>` scan doesn't pick it up.
- **`extract()` work-list sourcing:** query `uscg_manufacturers_bronze` for the latest-per-MIC `(source_recall_id, uscg_directory_id, detail_url)` tuples. `detail_url` is already absolutized to `https://uscgboating.org/content/manufacturers-identification-detail.php?id=N` (`uscg_manufacturer.py:526-527`), so it can be fetched directly. (Contract wrinkle: `HtmlScrapingExtractor` requires a `start_url`; repurpose it as the canonical base URL — open question §16.)
- **Walk ceiling:** `_MAX_DETAIL_ROWS ≈ 30_000` (~2× the 16.3k corpus), analogous to the listing extractor's `_MAX_PAGES = 2000` guard.
- **Deep-rescan sibling:** `UscgManufacturerDetailDeepRescanLoader` — replicate the three-override pattern from `UscgManufacturerDeepRescanLoader` (`uscg_manufacturer.py:715-754`): override `_should_short_circuit` → always `False`; override `load_bronze` to skip `_touch_freshness` + `_update_records_count`. Deep-rescan = full 16.3k walk (Tier-2); incremental = Tier-1 listing-delta.
- **`load_bronze` config:** `BronzeLoader(identity_fields=("source_recall_id",), hash_exclude_fields=frozenset({"detail_url", "uscg_directory_id"}))` — mirrors `uscg_manufacturer.py:362-367`. Identity is MIC, **not** `uscg_directory_id` (page-offset-deterministic, hash-excluded).

---

## 7. content_hash policy

- **Exclude** `detail_url` (URL-scheme-rewrite defense) and `uscg_directory_id` (page-offset-deterministic) — same as the listing table.
- **INCLUDE `date_modified`.** It IS the Path B change signal; excluding it would defeat the branch's purpose. Inclusion is correct by design (§M.6; `implementation_plan.md:490`).
- **Bulk re-touch risk + handling.** If USCG bulk-touches `Date Modified` across many rows (a render/admin sweep, not real content change), every touched row re-hashes and re-versions. Manage this with `change_type='schema_rebaseline'` on deliberate re-baselines (NOT by hash-excluding `date_modified`), so the wave is excluded from the eventual `recall_event_history` edit-detection (ADR 0027 + `documentation/operations/re_baseline_playbook.md`; `implementation_plan.md:704`).

---

## 8. CLI / registry / workflow wiring

- **`src/config/source_registry.py`:** add `"uscg_manufacturer_details": UscgManufacturerDetailExtractor` to `EXTRACTOR_BY_SOURCE_NAME` and `"uscg_manufacturer_details": UscgManufacturerDetailDeepRescanLoader` to `DEEP_RESCAN_BY_SOURCE_NAME`, + 2 imports. Generic dispatch handles `html_scraping` sources — no source-specific CLI branch is needed (`cli/main.py:186-187, 267-268`).
- **`src/cli/main.py`:** add entries to `_LOOKBACK_NO_OP_MESSAGES` (line 52) and `_DEEP_RESCAN_NO_DATE_WINDOW_MESSAGES` (line 67) for **both** `uscg_manufacturer_details` **and** `uscg_manufacturers` — the latter is currently **missing** (only `uscg` is present), so `recalls extract uscg_manufacturers --lookback-days N` does not get the informative no-op notice. Fixing the `uscg_manufacturers` omission is a small adjacent correctness fix; call it out in the PR body.
- **`--limit N` (new on this branch):** `recalls extract uscg_manufacturer_details --limit N` caps the work-list to the first N MICs (by `uscg_directory_id`). Two uses: (1) **cheap dev validation** — a handful of pages exercises fetch → R2 → bronze → dbt end-to-end in seconds, without the ~4.5 h full sweep; (2) **chunked / resumable seeding** — repeated capped runs march through the corpus in `uscg_directory_id` order because the Tier-1 incremental cursor + content-hash dedup skip already-loaded MICs each pass (a workaround for the monolithic, load-at-end run, which has no mid-run checkpoint). Gated to `uscg_manufacturer_details` only (`_WORK_LIST_LIMIT_SOURCES`); other sources emit an ignored-notice. The cap is applied **after** the `_MAX_DETAIL_ROWS` blow-up guard so a capped run still benefits from the safety net (`uscg_manufacturer_detail.py`: `work_list_limit` field + the `extract()` slice; `cli/main.py`: the `--limit` option + dispatch).
- **`config/sources/uscg_manufacturer_details.yaml`:** `source_type: html_scraping`, `start_url` = the base detail URL, `scrape_delay_seconds: 1`, `timeout_seconds`, `rate_limit_rps` (consumed by `HtmlScrapingSourceConfig.to_extractor_kwargs`).
- **Workflows (dispatch-only; cron is Phase 7):** `.github/workflows/extract-uscg-manufacturers-detail.yml` + `deep-rescan-uscg-manufacturers-detail.yml`, modeled on `extract-usda.yml` / `deep-rescan-usda.yml` (secrets `NEON_DATABASE_URL`, `R2_ACCOUNT_ID`, `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY`, `R2_BUCKET_NAME`; `recalls extract|deep-rescan <source>`).
  - **Adjacent gap (NOT fixed here):** there are currently **no** USCG GitHub workflows at all (no `extract-uscg.yml`, `extract-uscg-manufacturers.yml`, `deep-rescan-uscg*.yml`). This branch adds only the **2 detail** workflows; the missing listing/recall USCG workflows are tracked separately (not in scope).

---

## 9. Staging model + dbt tests

- **`dbt/models/staging/stg_uscg_manufacturer_details.sql`** — latest-per-MIC via `row_number() OVER (PARTITION BY source_recall_id ORDER BY extraction_timestamp DESC) = 1`, mirroring `stg_uscg_manufacturers.sql`.
- Sentinel coercion mirroring `stg_uscg_manufacturers.sql` extended to all detail fields: `CASE WHEN x IN ('-','UNK','') THEN NULL` for text; **add `'-, -'`** to the `company_official` coercion; `nullif(trim(type),'')`; `nullif(trim(additional_address),'')`.
- Inline comment carrying the **"do not conflate `out_of_business` (current) vs `past_company_N (OOB)` (prior holder)"** warning (§5).
- `.yml` tests (follow `stg_uscg_manufacturers.yml` shape): `source_recall_id`/`mic` not_null + unique; `content_hash` not_null; `extraction_timestamp` not_null; optional `status` `accepted_values` (normalized form).
- **No silver model on this branch.** `firm_manufacturer_attributes_detail` / the SCD-2 dim is the deferred Phase 6 work (§11).

---

## 10. Cassettes + unit tests + integration tests

> **Shipped vs deferred (per the review, M3).** The HTML fixture + unit tests below are committed and green — they cover the parser, the empty-cell-bleed regression, the drift fence, and schema validation **offline**. The **live integration cassettes are a user-recorded follow-up**: they can't be recorded in the build (no live network), so per the standard project pattern they're recorded on the first extraction run via the command at the end of this section. Until then the live integration test file is not committed and the unit layer is the shipped coverage.

- **HTML fixture:** `tests/fixtures/uscg/sample_manufacturer_details_page.html`, promoted from a real page — prefer **id=655 (AXY)** (the probe-test shape, contains a blank `Parent Company` cell immediately followed by `Parent MIC`, and a `Past Company` with `(OOB YYYY)`).
- **Unit tests:** `tests/extractors/test_uscg_manufacturer_detail_extractor.py`, using the `_make_extractor()` pattern from `test_uscg_manufacturer_extractor.py` (`monkeypatch.setenv`, `MagicMock(spec=sa.Engine)`, patch `create_engine` + the module's `R2LandingClient`, `scrape_delay_seconds=0.0`). Mandatory tests:
  - `test_blank_parent_company_does_not_bleed_parent_mic_label` — assert `parsed['parent_company']` is empty and **not** the literal `'Parent MIC:'` (empty-cell-bleed regression; cf. `tests/scripts/test_probe_mic_reassignment_rate.py:101-106`).
  - `test_drift_fence_raises_on_unknown_label` — inject `<strong>Risk Category:</strong>`, assert `pytest.raises(TransientExtractionError)` (cf. `test_uscg_extractor.py`). **This is the behavioral difference from the probe** (probe records unknown labels; production raises).
  - Pydantic happy-path `model_validate()` + an `extra='forbid'` rejection test.
- **Integration cassettes:** new dir `tests/fixtures/cassettes/uscg_manufacturer_details/`; module-scoped `vcr_cassette_dir`; autouse `skip_if_no_cassette` (per `test_uscg_manufacturer_live_cassettes.py`).
  - `test_real_details_page_parses.yaml` (id=655) — assert key set, MIC value, `parent_company != 'Parent MIC:'`.
  - **Drifted-variant coverage as a hand-constructed `respx` mock (no YAML)** — drifted pages can't be recorded live; matches the existing respx error-path pattern. The unit-layer drift-fence test is primary; the respx one is the integration-layer confirmation.
  - Optional bounded lifecycle cassette (3 listing-source rows + a small detail sample), `max_pages`-bounded, `_should_short_circuit=False`.
- **Recording:** `pytest --vcr-record=all tests/integration/test_uscg_manufacturer_detail_live_cassettes.py`.

---

## 11. Deferred to Phase 6 — SCD-2 + flag-as-time-sensitive join (designed here, built there)

> This section is a **spec for the Phase 6 implementer / ADR 0035**, not a build item for this branch. Nothing in §11 ships on `feature/uscg-manufacturers-detail-addition`.

- **Mechanism (recommended):** SCD on the stable anchor `mic` (1-tuple) + Type-1 latest-wins attributes (`company_name`, `address`, `city`, `state`, + the Path B detail fields) + **Type-2 via dbt snapshot `strategy='check'`** — option (a) of `implementation_plan.md:707`, matching ADR 0033's NHTSA pattern. Reject the `LAG()` model and the "defer entirely" options.
- **Snapshot config:** `dbt/snapshots/uscg_manufacturer_attributes_snapshot.sql`; `unique_key = mic` **alone** (a reassignment is a *new version of the same anchor*; `mic+company` would wrongly fork lineage); `check_cols = [company_name, address, city, state, …]`; `target_schema = silver_snapshots`; driven from `stg_uscg_manufacturer_details` (latest-per-MIC).
- **MIC casing:** the snapshot/anchor must normalize casing — the 7 lowercase recall MICs (`cec, blb, kis, lbb, ser, vky, zep`) join via `upper(trim())` in `firm.sql` / `recall_event_firm.sql`; without normalization they miss the SCD table.
- **Hybrid seed (the USCG twist NHTSA lacks):** forward-only from 2026-05-30 (our snapshots) **plus** a one-time backfill of historical intervals from the source-native lineage (`Past Company (OOB year)` + `In Business`). Undated `Past Company` entries get **open-ended low-confidence intervals** (`archive/silver_v15_migration_plan.md:243`).
- **Flag-as-time-sensitive join (v1 treatment):** because dates are mostly unusable (~13/205 parseable OOB years; `In Business` contaminated), v1 attributes to the **current** MIC holder but **flags** any recall whose MIC has a `Past Company` as "manufacturer attribution time-sensitive" (`implementation_plan.md:707`; `archive/silver_v15_migration_plan.md:245`).
  - **Mechanism:** a `match_confidence` value on `recall_event_firm` (parallel to the USDA/CPSC `match_confidence` from Phase 6b — `archive/phase-6-execution-plan.md:213, 252`): e.g. `mic_unambiguous` / `mic_time_sensitive_unresolved` / `mic_build_date_resolved`. v1 sets `mic_time_sensitive_unresolved` for any MIC in `recalled_reassigned_mics.json`.
  - **Lockstep constraint (load-bearing):** any change to `firm.sql` `uscg_normalized` (line 103+) must be replicated to `recall_event_firm.sql` `uscg_event_firms` (line 89+; lockstep comment at `recall_event_firm.sql:22` and `:98` — `firm.sql` carries none) — a break orphans `firm_id`s. **Recommend bundling the SCD-2 + flag work into the single Phase 6b PR** that already edits both files, to eliminate rebase-conflict risk (this is the same reason it is kept off the current branch).
- **As-of-build-date (Phase 6+, NOT Phase 6b):** correct attribution joins on the **boat build date** (HIN chars 9–12; recalls staging carries `hin` + `model_year` — `stg_uscg_recalls.sql`), not the recall date. ~52.8% of USCG recalls have a populated HIN (`field_audit_2026_w22.md §4`). A **later refinement**; OOB dates are too sparse for v1.
- **ADR 0035 (new) — required.** `implementation_plan.md:707` says "File as an ADR before implementing." 0034 is reserved for NHTSA Layer-3 (`archive/silver_v15_migration_plan.md:128`). ADR 0035 covers all four SCD-2 dims + the value-selection Policy (leaning Policy C — `implementation_plan.md:715`) + the USCG-specific whole-entity-succession-under-a-stable-anchor + the as-of-build-date surface NHTSA lacks. **Also:** fill ADR 0031's TBD USCG per-source row, and exempt the `silver_snapshots` table from ADR 0007's bronze-snapshot pruning policy.
- **Retired/sentinel MICs** (`111`, `999`, `777`, `N/A`, mic-only-no-name): no directory row — omit from the SCD dim, document as known non-joinable orphans in `_silver.yml`.

---

## 12. Serving-layer fit (FastAPI + the four frontend page types)

Every captured field earns its place in the final product (FastAPI per Phase 8 / ADR 0024 *not yet filed*; frontend per Phase 9). This doc supplies USCG-field input to ADR 0024; it does not pre-empt it.

- **Individual recall page** (`GET /recalls/{source}/{recall_id}`): `company`, `dba`, full `address`/`city`/`state`/`zip`, `phone`, `fax`, `company_official`, `status` → the firm-demographics block. `match_confidence == mic_time_sensitive_unresolved` → a banner/footnote ("Manufacturer attribution time-sensitive — this MIC was reassigned; the current holder may not have built the recalled boat"), surfacing the ambiguity instead of silently misattributing. `past_company_1/2/3` + `out_of_business` → an optional "MIC history" disclosure.
- **Firm rollup** (`GET /firms/{id}`): `dba` + `parent_company`/`parent_mic` feed observed-names / corporate-succession surfacing; `status` feeds current firm status; full `address` feeds firm location.
- **Per-source summary / data-viz pages** (Phase 9): `status` → active vs out-of-business cohort counts; `state`/`zip` (now **first-class** on the detail table, unlike the truncated listing address) → geographic distribution, joining the same surface USDA already has; the recycled-MIC cohort (`recalled_reassigned_mics.json` / `match_confidence`) → a USCG-distinctive reassignment/recycling stat (the kind of breadth-demonstrating analytic the portfolio goal favors).
- **Combined landing page:** firm-lifecycle stats (active vs OOB) and geographic hotspots roll up from the same `status` + `state`/`zip` fields.
- **Gold note (cross-reference, not a build item):** gold is currently understocked (`recalls_by_month.sql` only); the per-source firm-stats + geographic views are Phase 6e gold work. This branch's contribution is the **field availability** (esp. `state`/`zip` and `status`) that unblocks them.

---

## 13. Sequencing + branch placement

- **Position:** after Phase 5d Step 7 (listing-only, PR #40, landed) and **before/independent of Phase 6a**. The user's Phase 6a.5 ordering ("after this branch") still holds.
- **Independent of Phase 6a.** Bronze-only scope (migration + schema + extractor + thin staging) does not touch Phase 6a's silver field-mapping edits or `silver_design_notes.md`. Orthogonal file scopes → can land before or in parallel with 6a without collision.
- **No Phase 6b collision.** No `firm.sql` / `recall_event_firm.sql` / silver-dim edits on this branch (the SCD-2 + flag work that touches those is deferred — §11). This is the explicit reason for the split (`implementation_plan.md:490`).
- **`branch_sequencing_strategy.md` registration.** That doc previously omitted this branch from its workstreams table, dependency graph, and conflict playbook; it is updated alongside this plan to add the branch (bronze-only, off `main`, post-#41, low conflict risk — new files + bronze/staging only).

---

## 14. Quality gates (branch-merge checklist)

- [x] `recalls extract uscg_manufacturer_details` lands bronze rows; quarantine rate < 5%. **Integration-validated on the Neon dev branch 2026-05-30** via `--limit 50`: 50 fetched / 50 loaded / 0 rejected (rejection_rate 0.0); `dbt build --select stg_uscg_manufacturer_details` green (view + 6 generic tests PASS). Full ~16.3k seed + `deep-rescan` Tier-2 sweep run at the Phase 6a.5 production cutover (§13) — not on this dev branch.
- [ ] Drift fence raises on an unknown bolded label (unit test green).
- [ ] Empty-cell-bleed regression green (`parent_company` never `'Parent MIC:'`).
- [ ] `stg_uscg_manufacturer_details` passes dbt generic tests; `dbt build` green.
- [ ] `ruff` + `pyright` clean; `extra='forbid'` enforced on the schema.
- [ ] Unit tests + HTML fixture committed and green. **Live integration cassettes are a user-recorded follow-up** (recorded on first extraction; can't be recorded offline — see §10).
- [ ] CLI no-op messages present for both `uscg_manufacturers` and `uscg_manufacturer_details` (no `KeyError` on `--lookback-days` / date-window flags).
- [ ] `pyproject.toml` minor version bump (a meaningful chunk lands).
- [ ] **Scope guard:** NO silver SCD-2 / `firm.sql` / `recall_event_firm.sql` / snapshot edits present in the diff.

---

## 15. Risks

| Risk | Mitigation |
|---|---|
| Bulk `Date Modified` re-touch → re-version wave | `change_type='schema_rebaseline'` (§7); excluded from history edit-detection |
| Full-walk cost (~4.5 h at 1 s) | Tier-1 listing-delta cursor for incremental; full walk only on deep-rescan |
| Source HTML drift | RAISE-on-unknown-label fence — fail fast, don't absorb |
| `start_url` contract mismatch (work-list comes from bronze, not pagination) | Repurpose/ignore `start_url`; open question §16 |
| `Past Company` cardinality > 3 | Schema fixes 3 columns; a 4th label trips the drift fence |
| `type` run-on / truncation artifact | Captured verbatim; normalization deferred to silver |

---

## 16. Open questions (inferences to confirm before/while implementing)

1. **Source name string:** ✅ **Resolved — `uscg_manufacturer_details`** (shipped; migration 0017, YAML, `source_registry.py`, CLI no-op messages, and the R2 prefix all agree).
2. **`start_url` repurposing:** ✅ **Resolved — `start_url` is the canonical base detail URL** (required by `HtmlScrapingExtractor`); per-row detail URLs come from the bronze work-list's `detail_url`, not from paginating `start_url`.
3. **Tier-1 cursor watermark:** ✅ **Resolved — no new column.** The listing-delta cursor compares each MIC's latest listing `extraction_timestamp` vs its latest detail `extraction_timestamp` (`_build_work_list`); no `source_watermarks` column needed.
4. **`type` normalization:** the verbal vessel-type run-on vs the recalls `boat_type` code gap (`field_audit_2026_w22.md §8`) — confirm shared semantics; defer normalization to silver regardless.
5. **`Past Company` max cardinality:** observed ≤2–3; 4+ unconfirmed (drift fence is the safety net).
6. **`match_confidence` vocabulary alignment:** keep the USCG values consistent with the USDA/CPSC `match_confidence` enum Phase 6b defines (single shared column, source-specific values).

---

## 17. References

- `project_scope/implementation_plan.md:482–490` (Step 7 follow-up + branch scope), `:704` (`recall_event_history` / re-baseline), `:707` (cross-source SCD-2 item + flag-as-time-sensitive), `:715` (Policy C), Phase 8/9 (FastAPI/frontend).
- `project_scope/archive/silver_v15_migration_plan.md:128` (ADR 0034 reservation), `:229–247` (cross-source application).
- `project_scope/archive/phase-6-execution-plan.md` (Phase 6b firm resolution + `match_confidence`; sequencing constraints).
- `project_scope/phase-5d-uscg-manufacturers.md` (sibling listing-only plan); `project_scope/branch_sequencing_strategy.md`.
- `documentation/uscg/manufacturer_scraping_observations.md` §M (M.1–M.6); `documentation/uscg/field_audit_2026_w22.md` §3/§4/§6/§8.
- ADRs: 0007 (bronze snapshot pruning — `silver_snapshots` exemption), 0014 (schema policy), 0027 (bronze storage-forced), 0031 (USCG TBD per-source row), 0033 (SCD-on-stable-anchor + ADR 0034 reservation), **0035 (to be filed — cross-source SCD-2)**.
- Code: `src/extractors/uscg_manufacturer.py:362-367,526-527,550-568,715-754`; `src/extractors/uscg.py:224-253,605-660,629-634`; `src/extractors/_html_scraping.py`; `src/schemas/uscg.py:83-104`; `src/config/source_registry.py`; `src/cli/main.py:52-77,186-187,267-268`; `scripts/uscg/probe_mic_reassignment_rate.py:91-204`.
- dbt: `dbt/models/staging/stg_uscg_manufacturers.sql`; `dbt/models/silver/firm.sql:103+` (`uscg_normalized`); `dbt/models/silver/recall_event_firm.sql:89+` (`uscg_event_firms`); `dbt/models/silver/firm_manufacturer_attributes.sql`.
- Migrations: `0015_uscg_manufacturers_bronze.py`, `0016_seed_uscg_manufacturers_watermark.py`; new `0017`/`0018`.
- Artifacts: `data/exploratory/uscg_manufacturers/recalled_reassigned_mics.json` (recalled-only, 365 affected MICs); `data/exploratory/uscg_manufacturers/detail_probe_cache/{655,27}.html`.
