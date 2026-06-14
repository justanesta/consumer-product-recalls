# Capture-expansion backlog — the (b) PR

- **Status:** Open 2026-05-28

> **Update 2026-06-07:** Primary FDA scope (Tier-1 firm address lift + Tier-3 press releases) executed and merged via Phases 6b–6e (PRs #59–#62). Original tracking branch superseded and deletable. CPSC/NHTSA/USCG capture-expansion sections below remain open stubs pending their respective audits.

- **Scope:** Single cross-source parking lot for fields documented in API docs but not currently captured at bronze. Accumulates source-by-source as Phase 6a audits run. Drives one consolidated capture-expansion PR once all five sources are audited.
- **Methodology:** `documentation/audit/methodology.md`
- **Driver doc references:** `documentation/<source>/field_audit_<period>.md` per source

## Why one PR not five

The (b) PR is a single coherent unit because:

1. **Cross-source column-naming alignment happens once**, against the union of fields (e.g., FDA `firmcitynam` + CPSC `manufacturer_city` + USDA `establishment_city` should converge to one column name like `firm.city`).
2. **Bronze schema migrations bundle naturally** — one Alembic revision adds columns across multiple bronze tables in one transaction.
3. **`displaycolumns` (or equivalent) expansions per source each carry their own engineering tax** (FDA page-size penalty, NHTSA TSV-position invariant, CPSC array-shape changes); a single review window covers them together and lets us decide tax-vs-utility consistently.
4. **One deep-rescan suffices** to backfill historical bronze across all sources at once, rather than five separate backfills.

## FDA

Driver: `documentation/fda/field_audit_2026_w22.md` §7. Architecture context: §5b (iRES endpoint architecture) + §8 R2 validation findings + `documentation/fda/api_observations.md` Findings K0, K0.1, K0.2.

The FDA iRES API has a **three-tier endpoint architecture** documented by FDA and confirmed via this project's Bruno exploration (2026-04-26) and production extractor (2026-04-27 onward). The 2026-05-29 capture-expansion probe sweep empirically validated the tier-1 column list against the iRES Usage PDF's authoritative 33-column bulk POST datagroup, and resolved two prior misdiagnoses:

- **Finding K0** — bulk POST `displaycolumns` is restricted to the 33 columns enumerated in `iRES_enforcement_reports_api_usage_documentation.pdf` page 7. Fields outside the list (all `*short` and `*indicator` variants, `productlmd`, `pressreleaseurl`/`pressreleaseissuedt`/`pressreleasetype`, `createdt`) return STATUSCODE 406 — they are lookup-endpoint-only columns.
- **Finding K0.2** — POSTs whose `sort` column is not in `displaycolumns` return HTTP 204 No Content at the Akamai edge (not 406 from the FDA app). This had been previously misdiagnosed as "Akamai per-IP scoring" / "shape-variance bot detection"; the actual rule is purely structural and is enforced at parse time by `scripts/fda/audit/probe_displaycolumns.py --sort` pre-flight.

### FDA Tier 1 — `POST /recalls/` bulk (stable shape, low cost)

Add to `src/extractors/fda.py`'s `_DISPLAY_COLUMNS` constant. Bumps the field count from 21 → 32 but stays at one POST per page. No new request volume.

All fields below confirmed in the 33-column bulk POST datagroup (PDF page 7) AND empirically validated against a 100-record probe window starting `eventlmdfrom=05/01/2026` on 2026-05-29 (see `documentation/fda/field_audit_2026_w22.md` §7a for per-field population rates).

| Field | Priority | Population (2026-05-29 probe) | Landing-page utility |
|---|---|---|---|
| `firmcitynam` | MEDIUM | 100% | Firm city |
| `firmcountrynam` | MEDIUM | 100% | Firm country (3 distinct in window; needed to discriminate non-US firms) |
| `firmline1adr` | MEDIUM | 100% | Firm address line 1 |
| `firmline2adr` | MEDIUM | 0% (probe window) | Firm address line 2; ship anyway for schema future-proofing |
| `firmpostalcd` | MEDIUM | 97% | Firm postal code (ZIP+4 sometimes) |
| `firmstatecd` | MEDIUM | 97% | **Newly surfaced 2026-05-29** — 2-letter state code |
| `firmstateprvncnam` | MEDIUM | 97% | Full state name; pairs with `firmstatecd` (co-varies perfectly — same NULLs, same per-state counts). Capture both as denormalized lookup. |
| `firmsurvivingnam`, `firmsurvivingfei` | MEDIUM | 15% each | Current firm name / FEI if changed since the recall — critical for firm-dim continuity in Phase 6b |
| `postedinternetdt` | LOW | 84% | "First posted to internet" date — distinct from `eventlmd` (sample: posted 05/07/2025, lmd 05/28/2026). Definitions PDF notes blank for recalls prior to 2022-10-25. |
| `codeinformation` | HIGH | not yet corpus-measured | Lot/serial numbers, expiration dates, etc. Max length **205,424 chars** in the 2026-05-29 100-record window; **full-bronze max 8,867,432 chars** (2026-06-03 probe). Accepting the page-size penalty (5000 → 2500) is the §6 decision 5 in field_audit_2026_w22.md. Sizing impact for Phase 6a.5: revised FDA historical seed estimate to 1.5-3 GB. **Silver treatment = parse into a `recall_product_code` child table, deferred post-6b → `project_scope/freetext-enrichment-backlog.md`.** |

### FDA Tier 2 — Per-product GET (one request per unique `productid`)

> **RESOLVED 2026-06-03 → SKIP all of Tier-2.** Empirically settled by `scripts/fda/audit/probe_tier2_shorts.py` (random sample of 60): the `*short` fields are whitespace-normalized truncations of full fields already in bronze (`content ⊆ full` for every populated short), and the `*indicator`s are presence flags (`'true' ⟺ short present`) — zero net-new content for ~134K Akamai-paced GETs. Evidence: `documentation/fda/api_observations.md` **K0.3**. Decision homed in `project_scope/silver-field-capture-expansion-plan.md`. The table below is retained as the original candidate list.

Reference Bruno requests: `bruno/fda/lookup/get_code_info.yml`, `bruno/fda/lookup/get_event_products.yml`. Pattern: `GET /recalls/product/{productid}` per product (and `GET /search/codeinfo/{productid}` as an alternative path for `codeinformation` if not captured via bulk POST).

Daily delta sizing (per `field_audit_2026_w22.md` §8 R2 inspection): ~150-450 products/day in the active window → ~150-450 extra GETs/day. Deep-rescan: 134K products → ~134K GETs across a single rescan run. Significant but bounded; need rate-limiting + courteous pacing.

| Field | Priority | Endpoint | Note |
|---|---|---|---|
| `productdescriptionshort` | ~~MEDIUM~~ **SKIP** | `GET /recalls/product/{productid}` | **Settled 2026-06-03 (K0.3): whitespace-normalized truncation of `productdescriptiontxt` (content ⊆ full 8/8), ~13% populated.** Bug 2 `product_name` → derive in silver (`regexp_replace`+truncate), not fetch. |
| `recallreasonshort` | ~~LOW~~ **SKIP** | `GET /recalls/product/{productid}` | **Settled 2026-06-03 (K0.3): whitespace-normalized truncation of `productshortreasontxt` (content ⊆ full 7/7).** |
| `codeinfoshort` | ~~LOW~~ **SKIP** | `GET /recalls/product/{productid}` | **Settled 2026-06-03 (K0.3): word-boundary truncation of `codeinformation` (content ⊆ full 5/5).** |
| `productlmd` | LOW | `GET /recalls/product/{productid}` | Empirically null on every probed surface per Finding K0.1; capture would carry zero information. Listed for completeness; recommend SKIP per K0.1 closure. |

### FDA Tier 3 — Per-event GET (one request per unique `recalleventid`)

Reference Bruno requests: `bruno/fda/lookup/get_press_release_urls.yml`. Pattern: `GET /recalls/event/{eventid}` or `GET /search/pressreleaseurls/?eventid={eventid}` per event.

Daily delta sizing: ~50-180 unique events/day → ~50-180 extra GETs/day per per-event endpoint hit. Deep-rescan: ~25K events → ~25K GETs per endpoint.

| Field | Priority | Endpoint | Note |
|---|---|---|---|
| `pressreleaseurl` | HIGH | `GET /search/pressreleaseurls/?eventid={eventid}` | External authoritative link. M:1 (multiple press releases per event possible). 4 columns: `recalleventid, pressreleasetype, pressreleaseissuedt, pressreleaseurl`. Bulk POST returns STATUSCODE 406 (confirmed 2026-05-29). |
| `pressreleaseissuedt` | HIGH | Same | Pairs with URL. |
| `pressreleasetype` | MEDIUM | Same | Context per Definitions PDF: "State, Firm, or FDA". |
| `distributionpatternshort` | LOW | `GET /recalls/event/{eventid}` | Truncated UI variant of `distributionareasummarytxt`; could derive in silver instead. |
| `createdt` | LOW | `GET /recalls/event/{eventid}` | "Date that recall was first posted" per Definitions PDF. Likely distinct from `postedinternetdt` but unclear; lookup-endpoint probe would clarify. |

### Architecture decision — three sub-options for the (b) PR

| Option | What ships | Engineering cost |
|---|---|---|
| **B1 — Tier 1 only** | Firm address + survivors + posted date | Cheap — extend `_DISPLAY_COLUMNS`, no new extractor pattern |
| **B2 — Tier 1 + Tier 2** | Add `codeinformation` (the highest-utility tier-2 field) | New per-product GET loop + dedup by productid + retry/pace logic |
| **B3 — All three tiers** | Press releases, all shorts, createdt, full state name | Two new GET loops; significantly higher request volume; needs Akamai accommodation (static IP whitelist or careful pacing) |

Defer the choice to cross-source consolidation. CPSC's per-recall detail-page enrichment and USCG's already-built per-detail-page scraping are similar architectures — decide them together so we standardize the per-record-enrichment pattern across sources.

**RESOLVED 2026-06-03 — scope ≈ B1 + Tier-3 (no B2).** Tier-2 is excluded (settled SKIP, K0.3). Note the B2 row's framing is moot: `codeinformation` is a Tier-1 / bulk-POST field (already captured via migration 0019), not a Tier-2 add. The (b) PR is **(A) Tier-1 firm/posted-date silver lift + (C) Tier-3 press releases**; `code_information`'s silver *parse* into a `recall_product_code` child table is deferred post-6b to `project_scope/freetext-enrichment-backlog.md`. Execution owned by `project_scope/silver-field-capture-expansion-plan.md`.

### Skipped (not eligible for capture)

- `fieldname`, `newvalue`, `oldvalue` — value-tracking audit-history endpoint only; out of scope for current data model
- `productdescriptionindicator`, `distributionpatternindicator`, `recallreasonindicator`, `codeinfoindicator` — UI expansion flags ("show more…" toggles), not content (empirically confirmed 2026-06-03, K0.3: `indicator='true' ⟺ short present`, 0 exceptions / 60 sampled)

## CPSC

(To populate when CPSC audit runs.)

## USDA recalls + USDA establishments

Driver: `documentation/usda/field_audit_2026_w22.md` §8 + §9. Architecture context: USDA's pattern is bulk-fetch-only (no per-record-enrichment endpoints) and both APIs return everything they offer on every fetch.

### Zero capture-expansion adds for either source

**Both APIs are fully captured at bronze.** The audit (2026-05-28, validated against 6072 English recall records + 7970 establishment records) confirmed no documented field is missing from `src/schemas/usda.py` or `src/schemas/usda_establishment.py`. All work for USDA happens in:

- **(a) silver-remap PR**: Bug 1 fix (`recall_product.type ← processing`) + 11 lifts from JSONB to first-class columns (see `documentation/usda/field_audit_2026_w22.md` §4 + §7)
- **Silver derive logic**: `recall_event.risk_level` derived from `recall_classification` via CASE WHEN (1:1 correlation per R2 validation)
- **Silver element-level filter**: `firm_usda_attributes.dbas` strips 'N/A' and 'None' placeholder element values

### Items deferred to Phase 6/7 (not (b) PR scope)

| Item | Reason for deferral |
|---|---|
| `field_product_items` structured parsing | Phase 6/7 enrichment workstream — extract embedded UPCs, lot codes, FSIS establishment numbers, dates from free text. Out of foundation-audit scope |
| `field_company_media_contact` structured parsing | Multi-line text with embedded name/phone/email. Same Phase 6/7 enrichment slot |
| `dbas` array-shape vs flattened-text resolution for cross-source firm dim | Decide at cross-source consolidation when CPSC's per-array firm structure is in scope |

### Skipped (not eligible)

- `field_press_release` and `field_en_press_release` — 99.9%/100% empty per R2 validation. Kept in bronze for shape parity, no silver lift |

## NHTSA

(To populate when NHTSA audit runs.)

## USCG

(To populate when USCG audit runs.)

## Cross-source engineering tax summary

| Source | Tax of any expansion | Mitigation / decision |
|---|---|---|
| FDA | `codeinformation` cuts bulk POST page size 5000→2500 — doubles request count on deep rescans | Pay it; daily impact negligible. 2026-05-28 |
| CPSC | TBD when audited | |
| USDA recalls | TBD | |
| USDA establishments | TBD | |
| NHTSA | TBD; suspected TSV column-position invariants need careful schema-drift handling | |
| USCG | TBD; HTML-scraped, no API displaycolumns concept | |

## Workflow for the (b) PR (when ready)

1. Confirm each source's audit doc is up to date and §7 capture-expansion items are categorized.
2. Run probe scripts per source to verify proposed-add fields actually populate (HIGH at minimum; MEDIUM if cheap).
3. Bundle on `feature/silver-field-capture-expansion`:
   - One Alembic migration per source with new columns
   - Pydantic schema updates per source
   - Extractor `displaycolumns` / equivalent expansions per source
   - Staging projection updates per source
   - Silver column lifts (or new columns) per source
4. Backfill historical bronze via `recalls deep-rescan <source> --change-type=schema_rebaseline` per source.
5. `dbt build` to verify silver populates the new columns end-to-end.
6. Land — one PR.

## Workflow for the (a) PR (silver remap only — runs first, after all audits but before (b))

Sibling PR. Operates only on `dbt/models/silver/*.sql` (and possibly `dbt/models/staging/*.sql` for column projection). Uses fields already in bronze. No extraction change, no migration, no backfill. Branch: `feature/silver-field-remap`.

The split exists so (a) can land quickly with cross-source-aligned column naming, and (b) carries the heavier-touch extraction expansion without blocking the user-visible silver corrections.
