# Capture-expansion backlog — the (b) PR

- **Status:** Open 2026-05-28
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

Driver: `documentation/fda/field_audit_2026_w22.md` §7. Architecture context: §5b (iRES endpoint architecture) + §8 R2 validation findings.

The FDA iRES API has a **three-tier endpoint architecture** that this project's Bruno exploration (2026-04-26) and the production extractor (2026-04-27 onward) already established as the canonical pattern. The 2026-05-28 audit confirmed via probe failures that mixing tiers in a single bulk request fails with STATUSCODE 406 + Akamai-side blocking. The tiers below mirror what `bruno/fda/` proved works.

### FDA Tier 1 — `POST /recalls/` bulk (stable shape, low cost)

Add to `src/extractors/fda.py`'s `_DISPLAY_COLUMNS` constant. Bumps the field count from 21 → ~30 but stays at one POST per page. No new request volume.

| Field | Priority | Landing-page utility |
|---|---|---|
| `firmcitynam`, `firmcountrynam`, `firmline1adr`, `firmline2adr`, `firmpostalcd`, `firmstatecd` | MEDIUM | Firm address (2-letter state code; the full state name is tier 3) |
| `firmsurvivingnam`, `firmsurvivingfei` | MEDIUM | Current firm name / FEI if changed since the recall |
| `postedinternetdt` | LOW | Probably redundant with `enforcement_report_dt`; defer or skip |

### FDA Tier 2 — Per-product GET (one request per unique `productid`)

Reference Bruno requests: `bruno/fda/lookup/get_code_info.yml`, `bruno/fda/lookup/get_event_products.yml`. Pattern: `GET /search/codeinfo/{productid}` or `GET /recalls/product/{productid}` per product.

Daily delta sizing (per `field_audit_2026_w22.md` §8 R2 inspection): ~150-450 products/day in the active window → ~150-450 extra GETs/day. Deep-rescan: 134K products → ~134K GETs across a single rescan run. Significant but bounded; need rate-limiting + courteous pacing to stay below Akamai's per-IP threshold.

| Field | Priority | Endpoint | Note |
|---|---|---|---|
| `codeinformation` | HIGH | `GET /search/codeinfo/{productid}` | Per Bruno docstring (line 109-111): the bulk-POST inclusion of this field is architecturally avoided. The dedicated endpoint is the established path. Returns 2 columns: `productid, codeinformation` |
| `productdescriptionshort` | MEDIUM | `GET /recalls/product/{productid}` | Could be candidate for `recall_product.product_name` |
| `recallreasonshort` | LOW | `GET /recalls/product/{productid}` | Could derive in silver instead |
| `codeinfoshort` | LOW | `GET /recalls/product/{productid}` | Could derive in silver instead |
| `productlmd` | LOW | `GET /recalls/product/{productid}` | Granularity proxy vs `eventlmd`; usually redundant |

### FDA Tier 3 — Per-event GET (one request per unique `recalleventid`)

Reference Bruno requests: `bruno/fda/lookup/get_press_release_urls.yml`. Pattern: `GET /recalls/event/{eventid}` or `GET /search/pressreleaseurls/{eventid}` per event.

Daily delta sizing: ~50-180 unique events/day → ~50-180 extra GETs/day per per-event endpoint hit. Deep-rescan: ~25K events → ~25K GETs per endpoint.

| Field | Priority | Endpoint | Note |
|---|---|---|---|
| `pressreleaseurl` | HIGH | `GET /search/pressreleaseurls/{eventid}` | External authoritative link. M:1 (multiple press releases per event possible). 4 columns: `recalleventid, pressreleasetype, pressreleaseissuedt, pressreleaseurl` |
| `pressreleaseissuedt` | HIGH | Same | Pairs with URL |
| `pressreleasetype` | MEDIUM | Same | Context (State / Firm / FDA) |
| `distributionpatternshort` | LOW | `GET /recalls/event/{eventid}` | Could derive in silver instead |
| `firmstateprvncnam` (full state name) | LOW | `GET /recalls/event/{eventid}` | `firmstatecd` from tier 1 is the cheaper substitute |
| `createdt` | LOW | `GET /recalls/event/{eventid}` | Probably redundant with `enforcement_report_dt` |

### Architecture decision — three sub-options for the (b) PR

| Option | What ships | Engineering cost |
|---|---|---|
| **B1 — Tier 1 only** | Firm address + survivors + posted date | Cheap — extend `_DISPLAY_COLUMNS`, no new extractor pattern |
| **B2 — Tier 1 + Tier 2** | Add `codeinformation` (the highest-utility tier-2 field) | New per-product GET loop + dedup by productid + retry/pace logic |
| **B3 — All three tiers** | Press releases, all shorts, createdt, full state name | Two new GET loops; significantly higher request volume; needs Akamai accommodation (static IP whitelist or careful pacing) |

Defer the choice to cross-source consolidation. CPSC's per-recall detail-page enrichment and USCG's already-built per-detail-page scraping are similar architectures — decide them together so we standardize the per-record-enrichment pattern across sources.

### Skipped (not eligible for capture)

- `fieldname`, `newvalue`, `oldvalue` — value-tracking audit-history endpoint only; out of scope for current data model
- `productdescriptionindicator`, `distributionpatternindicator`, `recallreasonindicator`, `codeinfoindicator` — UI expansion flags ("show more…" toggles), not content

## CPSC

(To populate when CPSC audit runs.)

## USDA recalls

(To populate when USDA recalls audit runs.)

## USDA establishments

(To populate when USDA establishments audit runs.)

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
