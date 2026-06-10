# USDA field audit — 2026 W22

- **Status:** In progress 2026-05-28
- **Scope:** USDA FSIS Recall API + Establishment Listing API ("MPI Directory") — every documented field vs. what we capture, what silver does with it, what's missing, and the structural firm-relationship question
- **Methodology:** `documentation/audit/methodology.md`
- **Companions:**
  - Capture-expansion backlog: `documentation/audit/capture_expansion_backlog.md`
  - Existing USDA findings: `documentation/usda/recall_api_observations.md`, `documentation/usda/establishment_api_observations.md`, `documentation/usda/establishment_join_coverage.md`, `documentation/usda/bilingual_and_lmd_findings.md`
  - Source PDFs: `documentation/usda/usda_fsis_recall_api_documentation.pdf`, `documentation/usda/usda_fsis_establishment_listing_api_data_documentation.pdf`

## 1. API field universe — two endpoints, two field sets

USDA is the only source so far with **two separate APIs** that must be joined client-side. The recall API has the recall narrative + a free-text firm name; the Establishment Listing API has the structured firm identifier infrastructure (FSIS grant numbers, addresses, sizes, activities, DBAs).

### 1a. Recall API — `GET https://www.fsis.usda.gov/fsis/api/recall/v/1`

Full universe per `usda_fsis_recall_api_documentation.pdf`:

| Field | Definition | Notable |
|---|---|---|
| `field_recall_number` | Recall number (e.g. `040-2022`) | Identity. |
| `field_title` | Recall title | Often includes firm name + product summary |
| `field_recall_date` | Recall announcement date (YYYY-MM-DD) | Required |
| `field_last_modified_date` | Last modified (YYYY-MM-DD) | 42% NULL per Finding D; unreliable as a freshness signal |
| `field_closed_date` | Closed date | 8.4% NULL per Finding D |
| `field_closed_year` | Closed year | String, can be empty |
| `field_year` | Recall year | String |
| `field_active_notice` | True / False | 9.4% NULL per Phase 5b first-extraction Finding C addendum |
| `field_archive_recall` | True / False | Always populated |
| `field_related_to_outbreak` | True / False / "" | 25% empty |
| `field_has_spanish` | True / False | Bilingual marker |
| `langcode` | English / Spanish | Strict enum |
| `field_recall_classification` | Class I / II / III / Public Health Alert | Structured class |
| `field_risk_level` | "High - Class I" / "Low - Class II" / "Marginal - Class III" / "Medium - Class I" / "Public Health Alert" | Severity — overlaps with `field_recall_classification` (see §5) |
| `field_recall_reason` | Free text label: "Product Contamination", "Misbranding, Unreported Allergens", etc. | Maps to filter `field_recall_reason_id` (Insanitary Conditions=17, Misbranding=13, etc.) |
| `field_recall_type` | "Recall" or "Public Health Alert" (per the API) | Action category, not product type |
| `field_processing` | "Fully Cooked - Not Shelf Stable", "Heat Treated - Shelf Stable", "Raw - Intact", etc. | FSIS processing category — closest analog to FDA's `PRODUCTTYPESHORT` |
| `field_states` | Comma-separated state list ("Arizona, California, Colorado, Utah, Washington") | Distribution geography |
| `field_establishment` | **Free-text firm name** ("Foster Farms", "Family Traditions Meat Company, Inc.") | No establishment number on the recall side — must be joined to Establishment Listing API |
| `field_product_items` | Free-text product description with embedded UPC, lot codes, and FSIS establishment numbers | Currently used as `recall_product.product_description` |
| `field_summary` | HTML-encoded recall narrative (paragraph length) | Currently mapped to `recall_event.description` |
| `field_distro_list` | Distribution list | Often empty |
| `field_qty_recovered` | Quantity recovered free text | Mapped to `recall_product.number_of_units` |
| `field_labels` | Labels filename (e.g. `Recall-040-2022-label.pdf`) | Currently in JSONB |
| `field_media_contact` | Free-text media contact name | Currently in JSONB |
| `field_company_media_contact` | Multi-line free-text — embedded name, title, phone, email | Currently in JSONB |
| `field_press_release` | Press release URL/text | 99.9% empty (Finding C); excluded from content hash |
| `field_en_press_release` | English press release URL/text | 100% empty (Finding C); excluded from content hash |
| `field_recall_url` | Recall detail URL | **Undocumented in PDF** (Finding H) but consistently returned |

Filter parameters (from Appendix A): `field_states_id`, `field_archive_recall`, `field_closed_date_value`, `field_closed_year_id`, `field_processing_id`, `field_product_items_value`, `field_recall_classification_id`, `field_recall_number`, `field_recall_reason_id`, `field_recall_type_id`, `field_related_to_outbreak`, `field_risk_level_id`, `field_summary_value`, `field_translation_language`, `field_year_id`. **Per Findings A-G in `recall_api_observations.md`, filters don't actually reduce the response — the API returns the full corpus regardless. Watermark/cursor concepts don't apply for USDA; the pattern is fetch-full + content-hash-dedup.**

### 1b. Establishment Listing API — `GET https://www.fsis.usda.gov/fsis/api/establishments/v/1`

Per `usda_fsis_establishment_listing_api_data_documentation.pdf`. This is the **MPI (Meat, Poultry, Egg Inspection) Directory** — every FSIS-regulated establishment.

| Field | Definition | Notable |
|---|---|---|
| `establishment_id` | Unique FSIS identifier (integer-as-string) | Identity. Other FSIS datasets may capitalize: `EstablishmentID` |
| `establishment_number` | Grant number with prefix: M=Meat, P=Poultry, I=Imports, G=Eggs, V=Voluntary | Can have suffix letters (M1234A ≠ M1234B), or `+`-joined multiple grants (M234+P567). **THIS is the number embedded in `field_product_items` of the recall API** |
| `establishment_name` | Name on the FSIS grant of inspection | Join key for recall ↔ establishment |
| `dbas` | "Doing Business As" — alternate legal names (JSON array) | Critical for firm-name normalization — establishment may be known commercially under a DBA |
| `address`, `city`, `state`, `zip` | Physical address | All populated 96-100% per Finding D |
| `phone` | Main phone | 3.9% empty |
| `duns_number` | Dun & Bradstreet identifier | 85.5% empty |
| `district` | FSIS district | Optional admin metadata |
| `circuit` | FSIS circuit | Sometimes TA-suffix (Talmage-Aiken state-inspected) |
| `size` | Large (≥500 emp) / Small (10-499) / Very Small (<10 or <$2.5M) | Business class |
| `county`, `fips_code` | County + FIPS code | `county` uses JSON `false` sentinel for missing (Finding C) |
| `latitude`, `longitude` (collapsed into `geolocation` in bronze) | Lat/lon coordinates | Same `false` sentinel pattern |
| `grant_date` | Most recent Grant of Inspection date | Optional |
| `activities` | Inspection-activity types: Slaughter, Processing, Egg Product, Imported Product, Identification, Certification - Cysticercus, Certification - Export, Food Inspection, Off-Premise Freezing, Technical Animal Fats, AMS RTE Canada EV Program (JSON array) | Multi-valued |
| `latest_mpi_active_date` | Latest MPI active date | 100% populated even on inactive (Finding G); required |
| `status_regulated_est` | "" (active MPI) or "Inactive" | Two-value enum kept open in schema |

## 2. Current bronze capture — both sources

### 2a. `usda_fsis_recalls_bronze` — 25 fields

Per `src/schemas/usda.py`. Bronze drops the `field_` prefix; columns include: `source_recall_id`, `langcode`, `title`, `recall_date`, `recall_type`, `recall_classification`, `archive_recall`, `has_spanish`, `active_notice`, `last_modified_date`, `closed_date`, `related_to_outbreak`, `closed_year`, `year`, `risk_level`, `recall_reason`, `processing`, `states`, `establishment`, `labels`, `qty_recovered`, `summary`, `product_items`, `distro_list`, `media_contact`, `company_media_contact`, `recall_url`, `en_press_release`, `press_release`. **`field_recall_url` captured despite being undocumented**, per Finding H.

### 2b. `usda_fsis_establishments_bronze` — 20 fields

Per `src/schemas/usda_establishment.py`. Columns: `source_recall_id` (= `establishment_id`), `establishment_name`, `establishment_number`, `address`, `city`, `state`, `zip`, `latest_mpi_active_date`, `status_regulated_est`, `activities`, `dbas`, `phone`, `duns_number`, `fips_code`, `county`, `geolocation`, `grant_date`, `size`, `district`, `circuit`. **All establishment-side fields captured at bronze.**

## 3. Mismappings (silver-only — fixable in the (a) PR)

Two confirmed mismappings + one near-mismapping that's a labeling concern.

### Bug 1 — `recall_product.type` is wrongly `recall_type`

`dbt/models/silver/recall_product.sql:92`:

```sql
recall_type                                   as type,
```

`field_recall_type` per the API is either "Recall" or "Public Health Alert" — it's the **action category**, not a product type. FDA's analogous `recall_product.type` column maps to `producttypeshort` (Devices / Food / Drugs / etc. — the commodity). USDA's structurally equivalent field is `field_processing` (Fully Cooked, Heat Treated, Raw - Intact, etc. — the FSIS processing category, ~10 enum values).

**Fix:** `recall_product.type ← processing` for USDA. Reassign `recall_type` as a §4 lift target (`recall_event.lifecycle_status`) — it's a useful 3-value lifecycle signal (0% NULL per R2 validation) rather than a drop candidate. *Note from R2 validation: `field_processing` is multi-valued comma-separated for ~3% of records ("Raw - Intact, Raw - Non Intact"). Silver will carry the comma-separated form; structured array parsing is Phase 6/7.*

### Bug 2 — `recall_event` source_payload_raw buries the structured classification

The `recall_event.source_payload_raw` JSONB for USDA at `recall_event.sql:106-119` includes `risk_level` and `recall_reason` as raw strings. But these are **structured classifications** that belong as first-class silver columns. Today landing-page consumers have to JSON-parse to surface them.

**Fix (part of §4 lifts):** lift `risk_level` and `recall_reason` out of JSONB to dedicated columns. See §4.

### Bug 3 — `recall_product.recall_product_id` equals `recall_event.recall_event_id` for USDA

`dbt/models/silver/recall_product.sql:85-86`:

```sql
md5('USDA' || '|' || source_recall_id)        as recall_product_id,
md5('USDA' || '|' || source_recall_id)        as recall_event_id,
```

By design (per `recall_product.sql:7-9` comment): USDA's `product_items` is free-text, so we emit one product row per recall event with `recall_product_id = recall_event_id`. **This is documented and intentional** — not a bug per se, but worth flagging because: (a) on landing pages, consumers can't tell from the ID whether this is a real product or a deferred-parse stub, (b) the silver shape masks the fact that USDA recalls often involve multiple distinct SKUs embedded in `product_items` text. Parsing this is out of scope for the (a) PR (it's a Phase 6/7 enrichment task) but worth noting in the audit.

## 4. Underused captures — lift from JSONB to structured columns

Already in bronze, currently buried in `recall_event.source_payload_raw` JSONB at `recall_event.sql:106-119`. All zero-cost silver lifts (except where R2 validation showed redundancy — see `risk_level`).

**Updated post-R2 validation (2026-05-28).** See §7 for decisions; §9 for the empirical findings that drove each row.

| Bronze field | Proposed silver column | Notes |
|---|---|---|
| `recall_classification` | `recall_event.classification` | 4-value enum: Class I (67.8%), Class II (15.4%), Public Health Alert (13.3%), Class III (3.5%) per R2 validation. Already partly mapped at the recall-classification level; lift formalizes it. Cross-source-alignable with FDA's `centerclassificationtypetxt` |
| `recall_type` *(NEW lift post-R2)* | `recall_event.lifecycle_status` | 3-value enum per R2: Closed Recall (86.4%) / Public Health Alert (13.3%) / Active Recall (0.3%). PDF said 2 values; actual API returns 3. Better lifecycle signal than `field_active_notice` (0% NULL vs 7.9%). Was Bug 1's misuse — reassign here |
| `recall_reason` | `recall_event.recall_reason` | Multi-valued comma-separated string per R2 (26 distinct values vs 9 PDF taxonomy). Cross-source-alignable with FDA's `voluntarytypetxt` + classification fields. Structured array parse deferred to Phase 6/7 |
| ~~`risk_level`~~ | ~~`recall_event.risk_level`~~ — **DERIVED instead** | **Removed as a lift per R2 validation: perfectly 1:1 with `recall_classification`** (4115/935/215/807 cardinalities match exactly). Compute in silver via `CASE WHEN classification='Class I' THEN 'High - Class I' …` if the human-facing label is needed |
| `processing` | `recall_product.type` *(per Bug 1)* | FSIS processing category. Multi-valued comma-separated per R2 (20 distinct vs 10 PDF). Closest USDA analog to FDA's `PRODUCTTYPESHORT` |
| `states` | `recall_event.distribution_states` | Comma-separated state list (28.4% NULL per R2). Mix of structured ("Florida, Maryland, …") and unstructured ("Nationwide", "Midwest") values |
| `related_to_outbreak` | `recall_event.related_to_outbreak` | Boolean (21% NULL per R2 — kept as nullable bool) |
| `archive_recall` | `recall_event.archived` | Boolean (0% NULL — 86% True per R2 — tracks `recall_type='Closed Recall'` closely) |
| `closed_at` | `recall_event.closed_at` | Trivial lift; already in staging as a date |
| `labels` | `recall_product.label_artifact_name` | PDF artifact filename like `Recall-040-2022-label.pdf` (11.1% NULL per R2). Landing-page consumers can build a hyperlink |
| `distro_list` | `recall_product.distribution_list_artifact_name` | **Corrected from `recall_event.distribution_list_text` post-R2 validation.** This is also a PDF filename (`RC-005-2026-Retail-List.pdf`), not narrative content. 79.7% NULL. Parallel shape to `labels` |
| `company_media_contact` | `recall_event.firm_contact_block_text` | Multi-line text with embedded name/phone/email (44.7% NULL per R2). Structured parse deferred to Phase 6/7 |
| `recall_url` | `recall_event.url` | **Already mapped.** 100% populated, 1:1 with recall_number per R2 — reliable despite being undocumented (Finding H) |

## 5. Field-naming gotchas

| USDA field | What the name suggests | What the data confirms |
|---|---|---|
| `field_recall_classification` | Severity classification | Class I / II / III / Public Health Alert — structured 4-value enum |
| `field_risk_level` | Different severity dimension | "High - Class I" / "Low - Class II" / "Marginal - Class III" / "Public Health Alert" — **empirically 1:1 correlated with `field_recall_classification` per 2026-05-28 R2 validation** (cardinalities match exactly: 4115 / 935 / 215 / 807). Don't lift as a separate column; derive in silver via CASE WHEN |
| `field_recall_type` | Type of product | **3 values** per R2 validation: "Closed Recall" (86.4%), "Public Health Alert" (13.3%), "Active Recall" (0.3%). PDF said 2. **Action / lifecycle category**, not product type (see Bug 1). Useful as a lifecycle proxy (0% NULL) — added as a §4 lift target |
| `field_processing` | Single-value enum | **Multi-valued comma-separated** per R2 — 20 distinct values vs PDF's 10-value taxonomy. Entries like "Raw - Intact, Raw - Non Intact" are real. Silver preserves the comma-separated form; structured array parse deferred to Phase 6/7 |
| `field_recall_reason` | Free-text "why this happened" / single selection | **Multi-valued comma-separated** per R2 — 26 distinct values vs PDF's 9. "Misbranding, Unreported Allergens" is one real value (2 reasons in one recall). Silver preserves the comma-separated form |
| `field_distro_list` | Distribution narrative | **PDF artifact filename** like `RC-005-2026-Retail-List.pdf`, not narrative content. 79.7% NULL per R2. Parallel naming/structure to `field_labels` |
| `field_establishment` | FSIS establishment with a number | **Free-text firm name only**, no number on this side. The actual FSIS number lives in the Establishment Listing API. 35.1% NULL per R2 — caps effective firm-join coverage at ~63% per record |
| `establishment_number` (establishment API) | Sequential establishment counter | **Grant-prefixed string** (M1234, P5678+G123) where prefix encodes grant type. Multiple establishments can share a numerical part across different grant types. 100% unique per R2 |
| `establishment_name` (establishment API) | Unique per-establishment name | **NOT unique** — 6885 distinct names for 7970 records per R2 (~14% name-shared establishments under multi-grant). Use `establishment_number` as the canonical join key, not name |
| `size` (establishment API) | 3-value business class per PDF | **4 distinct values** per R2: Very Small (41.7%), Small (38.9%), **"N / A" (10.2%, undocumented)**, Large (6.6%). 2.6% NULL |
| `dbas` element values | All alternate business names | Per R2, includes placeholder strings: 'N/A' (94 occurrences) and 'None' (15). Silver should filter both to null at element-level. Empty result lists → null. See §7 decision #7 |
| `status_regulated_est` | Bool / status enum | Two-value enum: `""` (90.1%) means active MPI, `"Inactive"` (9.9%) means inactive. The 90.1% "NULL" framing in the bronze schema docstring is misleading — `""` is the active sentinel, not a missing value |
| `field_summary` (HTML-encoded) | A plain-text summary | **HTML-encoded narrative** including `<` for `<`, `>` for `>`, `&` for `&`, etc. Embedded HTML markup (`<p>`, `<strong>`) and Spanish characters when `has_spanish=True`. Silver currently passes verbatim; landing-page rendering needs entity decode + HTML sanitization |

## 6. The firm-relationship question (structural — biggest decision in this audit)

USDA is the only source so far where the firm identifier infrastructure lives on a **separate API** from the recall payload. The current silver design handles this with two tables:

- **`firm`** (cross-source dim, keyed on `normalized_name = md5(upper(trim(raw_name)))`). For USDA, contributing fields are `establishment` from the recall side as `raw_name`, joined to the establishment API by `upper(trim(establishment_name))` to populate `company_id = establishment_number`.
- **`firm_usda_attributes`** (USDA-only table, keyed on `establishment_number`). Houses the rich metadata that has no cross-source analog: address, county, FIPS, geolocation, MPI active date, grant date, status, size, district, circuit, activities (JSONB), dbas (JSONB).

The join from recall side to establishment side is `upper(trim(r.establishment)) = upper(trim(e.establishment_name))` — per `establishment_join_coverage.md`, this hits **~97% of distinct recall names** (lifted from 82.85% by HTML-entity-decoding `&#039;` → `'` and `&amp;` → `&` on the recall side in staging).

### Three options for the firm architecture

| Option | Description | Pros | Cons |
|---|---|---|---|
| **A — Status quo, formalized** | Keep `firm_usda_attributes` as USDA-only. Lift cross-source firm attributes that DO exist in other recall payloads (FDA's `firmcitynam`, `firmstateprvncnam` etc., CPSC's per-array `address` fields) into `firm` as cross-source columns where populated, NULL elsewhere. The "rich" USDA-only attributes (DUNS, FIPS, MPI grant date, activities, dbas, lat/lon) stay in `firm_usda_attributes`. | Preserves cross-source firm anchor without forcing USDA-shape onto sources that don't have analogous data. Explicit table name signals scope. Smallest blast radius. | Discovery problem: landing-page consumers need to know to join `firm` → `firm_usda_attributes` via `firm.company_id`. The 3% of USDA firms with NULL establishment_number appear in `firm` but not `firm_usda_attributes`. |
| **B — Generalize to `firm_attributes`** | Rename `firm_usda_attributes` → `firm_attributes`. Add columns for FDA address fields, CPSC structured firm fields, USCG mic, etc. Most columns NULL except for the source that contributes them. | Single discovery point for firm metadata. Future-source-friendly. | Wide-but-sparse table. Adding a new source means adding columns. Most landing-page consumers only care about firm metadata for one source at a time, so the cross-source union may not actually help them. |
| **C — Merge into `firm`** | Add USDA-only columns (and FDA address columns, etc.) directly to `firm`. Each row carries every source's attributes; consumers filter by source where they need to. | Cleanest discovery — one table, one join. | `firm` becomes USDA-heavy. The cross-source firm anchor purpose (one row per normalized name with multiple `observed_company_ids` from multiple sources) gets diluted by per-source enrichment. Conceptually awkward when one firm appears in multiple sources with different attribute shapes. |

### My recommendation: Option A — confirmed by user 2026-05-28

Reasoning:
1. The cross-source firm anchor design in `firm.sql` is genuinely valuable — it lets us deduplicate a firm that appears under both USDA and FDA (food-safety crossover is real; e.g., a meat processor may have both an FDA enforcement and a USDA recall).
2. The rich USDA-only attributes (FSIS grant numbers, MPI lifecycle dates, activities, DBAs) are *intrinsically USDA-specific* — they describe FSIS-regulated establishment lifecycle, which has no analog in CPSC consumer-product recalls, FDA non-meat recalls, NHTSA vehicle recalls, or USCG boat recalls. Forcing them into a cross-source table is dishonest about scope.
3. The discovery problem is real but cheaper to solve with documentation than with restructuring. A `_silver.yml` schema description + the audit doc's existence resolves it. For the FastAPI surface this project is building (per `project_scope/implementation_plan.md`), REST sub-resources (e.g., `GET /firms/{id}/usda-establishment`) naturally abstract the two-table backing.
4. The 3%-of-USDA-firms-with-NULL-establishment_number problem is a join-coverage issue (Phase 6b RapidFuzz territory), not an architecture issue. Improving the join doesn't require changing tables.

**What does change in Option A:** add cross-source firm-address columns to `firm` (`firm.city`, `firm.state`, `firm.country`, `firm.line1`, `firm.line2`, `firm.postal_code`) populated from FDA's tier-1 (b)-PR additions and CPSC's per-array address fields if they exist, NULL for sources that have no address. USDA's establishment-listing address stays in `firm_usda_attributes` where it joins by `establishment_number` rather than by name, because that's the more reliable USDA join.

### Empirical findings from R2 validation (2026-05-28)

See §9 for the full table. Headlines that shape this section:

- **Effective per-record FSIS-establishment-number coverage in silver: ~63%** (not ~97% as `establishment_join_coverage.md` reads when scanned alone). Breakdown:
  - 35.1% of USDA recalls have NULL `field_establishment` (recall-side missingness)
  - Of the 64.9% with populated names, ~97% per-distinct-name join hit rate
  - 64.9% × 97% ≈ 63% net
- **`establishment_number` is 100% unique** across all 7970 establishments — confirms Option A's choice of canonical key
- **`establishment_name` is only 86.4% unique** (6885 distinct / 7970 records) — same-business multi-grant duplicates exist
- **DBAs marginal at best for closing the join gap**: only 32.3% of establishments have populated DBAs, with ~2% of element values being literal 'N/A'/'None' placeholders. Doesn't address the dominant 35.1% recall-side NULL
- **Phase 6b RapidFuzz** could target both sides of the gap, but the recall-side NULL is fundamental — net coverage ceiling for any name-based join is ~64.9%, regardless of fuzzy-matching quality
- **Per-recall disambiguation when name fans out to multiple establishments** (~14% of establishments share names with at least one other) is a separate Phase 6b workstream. Detail in `project_scope/archive/phase-6-execution-plan.md` § Phase 6b → "USDA recall-to-establishment disambiguation" — signal hierarchy (1: `field_product_items` embedded establishment number, 2: `field_states` ∩ establishment state, 3: `field_processing` ∩ activities, 4: combined, 5: `LatestMPIActiveDate` proximity), `match_confidence` column on `recall_event_firm`, precision-over-recall principle (NULL beats wrong association).

- **Empirical sub-classification of duplicate-name groups** (via `scripts/sql/usda_establishments/bronze/inspect_duplicate_names.sql`, run 2026-05-28): 456 distinct names have duplicates, partitioned into:
  - `multi_grant_same_state` (276 groups, 600 records, avg 2.17/group) — same physical facility with multiple FSIS grant types (M+P+V at one address). **Architectural reframing in Phase 6b**: treat (name, city, state) as one facility holding N grant numbers rather than N facilities to pick between. Drops 60% of duplicate-name groups out of the per-recall disambiguation problem.
  - `multi_state` (103 groups, 248 records, avg 2.41/group) — different businesses with identical legal names across states. `field_states` ∩ `establishment.state` is the dominant signal; ~80% resolvable.
  - `mixed` (77 groups, 693 records, avg 9.00/group, max 59) — multi-location chains spanning states + grants. **Dominant by record count (45%)**. Resolution is highly dependent on Phase 6/7 `field_product_items` extraction quality.

- **Cold-storage vs producer firm attribution (new architectural finding 2026-05-28).** The top 5 largest duplicate-name groups in `mixed` are all cold-storage operators — Lineage Logistics, LLC (59 establishments), Lineage Logistics PFS, LLC (38), Americold Logistics (32 + 31), Pilgrim's Pride (31, also a producer). Cold-storage facilities don't typically *produce* recalled products — they *store* products from other producers. When `field_establishment` is a cold-storage operator, the firm-of-interest for landing-page rendering is the *producer*, not the storage facility. This is **not** a name-fan-out issue — it's a wrong-firm-attribution issue. Resolution path: Phase 6/7 `field_product_items` structured-parse workstream becomes the producer-extraction surface in addition to the establishment-number-extraction surface. Detail also in `project_scope/archive/phase-6-execution-plan.md` § Phase 6b.

## 7. Decisions locked in (confirmed 2026-05-28)

All confirmed by user in conversation 2026-05-28 — decisions 1-4 during initial audit-doc review, decisions 5-6 after R2 validation discussion, decision 7 added post-R2-empirical findings.

1. **`recall_product.type ← processing` for USDA** (was `recall_type`). Cross-source semantic = "what kind of product is this?" → FSIS processing category fits. *Note from R2 validation: `field_processing` is multi-valued comma-separated for ~3% of records (e.g., "Raw - Intact, Raw - Non Intact"); silver preserves the comma-separated form. Structured array parsing deferred to Phase 6/7.*
2. **Lift to first-class silver columns:**
   - `recall_classification` → `recall_event.classification` (4-value enum)
   - `recall_type` → `recall_event.lifecycle_status` *(new, post-R2-validation: 3-value enum, 0% NULL — better lifecycle signal than `active_notice` which is 7.9% NULL)*
   - `recall_reason` → `recall_event.recall_reason` (multi-valued comma-separated string per R2)
   - `risk_level` → **derived in silver** from `recall_classification` *(post-R2: 1:1 correlation makes lifting redundant; CASE WHEN maps cleanly: Class I → "High - Class I", Class II → "Low - Class II", Class III → "Marginal - Class III", Public Health Alert → "Public Health Alert")*
   - `states` → `recall_event.distribution_states`
   - `related_to_outbreak` → `recall_event.related_to_outbreak`
   - `archive_recall` → `recall_event.archived`
   - `closed_at` → `recall_event.closed_at`
   - `labels` → `recall_product.label_artifact_name` (PDF artifact filename)
   - `distro_list` → `recall_product.distribution_list_artifact_name` *(post-R2: corrected name — this is a PDF filename like `RC-005-2026-Retail-List.pdf`, not narrative content)*
   - `company_media_contact` → `recall_event.firm_contact_block_text`

   Cross-source column names (and possible renames like `description` → `recall_reason` or similar) deferred to cross-source consolidation.
3. **Firm architecture: Option A.** Keep `firm_usda_attributes` as USDA-only; add cross-source firm address columns to `firm` when FDA's tier-1 capture-expansion (b)-PR fields land. Document the two-table join pattern in `dbt/models/silver/_silver.yml`. *Empirically confirmed (§6): `establishment_number` 100% unique is the right canonical key; the recall-side `field_establishment` 35.1% NULL caps effective coverage at ~63% per record, which Option B/C wouldn't help with anyway.*
4. **Defer USDA `field_product_items` structured parsing to Phase 6/7.** It's a separate enrichment workstream (extract embedded UPCs, lot codes, FSIS numbers, dates from the free text), not foundation-audit scope.
5. **Bilingual handling unchanged.** English-only filter at staging is right; Spanish siblings remain in bronze for audit. No silver change.
6. **`field_summary` HTML-encoded narrative passed verbatim to `recall_event.description` is acceptable for silver.** Landing-page rendering responsibility for entity decode + HTML sanitization; silver shouldn't strip markup since some of it (paragraph breaks, emphasis) carries semantic value.
7. **`dbas` element-level placeholder filtering in silver.** *(New, post-R2.)* Silver's `firm_usda_attributes.dbas` should filter the literal element values `'N/A'` (94 occurrences, ~1.5% of all dbas elements) and `'None'` (15) to null at element-level before re-aggregating. Empty resulting lists become null. Preserves the cardinality-signal value of real DBAs while dropping placeholder noise.

## 8. Capture-expansion items deferred to backlog

Logged in `documentation/audit/capture_expansion_backlog.md` § USDA (to be populated). Summary:

- **HIGH — none.** Bronze already captures everything the two USDA APIs offer in their respective endpoint surfaces. No capture gap.
- **MEDIUM — none.** Same reason.
- **LOW — `field_product_items` structured parsing.** Extract embedded UPCs, lot codes, FSIS establishment numbers, dates as structured product attributes. Defer to Phase 6/7 enrichment workstream.
- **SKIP — `field_press_release` and `field_en_press_release`.** 99.9-100% empty (Finding C); kept in bronze for shape parity but no silver lift needed.

USDA's audit is unusual in that the (b) capture-expansion PR has **zero adds** — both APIs are fully captured. The only structural gain is silver remap (Section 3) + JSONB lifts (Section 4). This is honest evidence that audit work has compounding payoffs: the audit identifies that USDA is the simplest source's tier and FDA's three-tier complexity is genuinely FDA-specific.

## 9. R2 validation status

USDA is also Akamai-fronted (per `documentation/usda/recall_api_observations.md` Finding O — the `refresh-user-agents` weekly workflow exists specifically to keep our UA fresh against Akamai bot detection). The `data/user_agents.json` hookup auto-rotates Chrome/Firefox UAs weekly so the production extractor stays unblocked.

### Completed 2026-05-28

- ✅ **`scripts/usda_recalls/audit/inspect_landed_payloads.py`** built and run against **6072 English-only records** across 5 snapshots (2026-05-01, 05-08, 05-15, 05-21, 05-28). 1216 distinct recalls in the corpus. Run uses `--limit-per-date 1` since USDA returns full corpus every fetch.
- ✅ **`scripts/usda_establishments/audit/inspect_landed_payloads.py`** built and run against **7970 records** (one snapshot, 2026-05-15). Full MPI Directory snapshot. List-handling fix in `_lib.py` lets `dbas` and `activities` surface real distinct-count + per-element distribution.
- ✅ **No Akamai issues** during audit runs — `data/user_agents.json` UA rotation kept the production extractor under Akamai's threshold. Confirms the existing weekly-refresh design is sufficient for routine extraction; audit probes piggy-back on it.
- ✅ **(b) PR for USDA confirmed as zero adds** per §8: both APIs fully captured at bronze.

### Headline empirical findings (folded into §§1a, 1b, 3-7 above)

**Recall side (6072 English records):**

| Finding | Impact |
|---|---|
| `field_recall_classification` ↔ `field_risk_level` are **perfectly 1:1** (4115 Class I / 935 Class II / 215 Class III / 807 Public Health Alert match exactly) | §4 lift changed: lift `recall_classification`, **derive `risk_level`** in silver |
| `field_recall_type` returns **3 values** (PDF said 2): Closed Recall (86.4%) / Public Health Alert (13.3%) / Active Recall (0.3%) | §1a corrected. §4 adds it as a lift target — useful lifecycle proxy (0% NULL vs `field_active_notice` 7.9% NULL) |
| `field_processing` is **multi-valued comma-separated** (20 distinct values vs 10 in the PDF taxonomy; entries like "Raw - Intact, Raw - Non Intact" are real) | Bug 1 fix accepts comma-separated form; structured parse deferred to Phase 6/7 |
| `field_recall_reason` is **multi-valued comma-separated** (26 distinct values vs 9 in the PDF taxonomy) | Same — silver preserves comma-separated form |
| `field_establishment` is **35.1% NULL** per record (2129 / 6072) | §6 effective firm-join coverage drops to **~63%** per record (the 97% in `establishment_join_coverage.md` is per-distinct-populated-name, not per-record) |
| `field_distro_list` is a **PDF filename** (`RC-005-2026-Retail-List.pdf`), not narrative | §4 lift renamed `recall_event.distribution_list_text` → `recall_product.distribution_list_artifact_name` (parallel to `labels`) |
| `field_recall_url` 100% populated, 1216 distinct (1:1 with `recall_number`) | Confirms current silver `recall_event.url` mapping is reliable |
| `field_active_notice` 7.9% NULL | Within Finding C addendum's 9.4% baseline; no change needed |
| `field_company_media_contact` 44.7% NULL | Lift to silver acknowledged as 44.7% sparse |

**Establishment side (7970 records):**

| Finding | Impact |
|---|---|
| `size` has **4 values** (PDF said 3): Very Small (41.7%), Small (38.9%), **"N / A" (10.2%, undocumented)**, Large (6.6%) | §1b updated; worth a Finding addendum in `establishment_api_observations.md` |
| `establishment_name` is **86.4% unique** (6885 / 7970) — ~1085 establishments share names | §6 confirms `establishment_number` (100% unique) is the right canonical key for `firm_usda_attributes` |
| `establishment_number` is **100% populated and 100% unique**; max length 31 chars (multi-grant `+`-joined forms like `M46712+P46712` are common) | Confirms Option A's choice of `establishment_number` as the primary key |
| `dbas` **67.7% NULL** (empty `[]`) — only 32.3% of establishments have DBAs | §6 DBA join potential is marginal (covers a small slice of the ~3% per-distinct-name gap, doesn't address the 35.1% recall-side NULL) |
| `dbas` populated records: 4146 distinct strings across 6340 elements ≈ 2.5 DBAs per populated record | Useful but noisy |
| `dbas` placeholders: **'N/A' (94 records) and 'None' (15 records)** appear as element values | §7 new decision #7: silver filters these to null at element level |
| `dbas` leading-whitespace duplicates (' Tyson' vs 'Tyson') already handled by staging trim | No change |
| `activities` is **always multi-valued**: 18,676 elements / 7,935 records ≈ 2.4 activities per establishment | Confirms JSON-array shape is essential |
| `activities` 43 distinct raw → ~20 canonical after trim. Top: Meat Processing (5515), Poultry Processing (4315), Certification - Export (1420) | Useful for cross-cutting "what kind of establishment" classification |
| `status_regulated_est` 90.1% "" (active), 9.9% "Inactive" | The 90.1% "NULL" framing is misleading — it's the active sentinel; current `_silver.yml` description should clarify |
| `address`, `city`, `state`, `zip`, `LatestMPIActiveDate`, `grant_date`, `establishment_id`, `establishment_number`, `establishment_name` all 100% populated | Solid foundation for `firm_usda_attributes` Option A |
| `district` has format inconsistency: `'5'` (100 records) coexists with `'05'` (1830 records) — same district, two formats | Data-quality note for future Phase 6/7 normalization |
| `county` 1.5%, `geolocation` 1.2% null after treating JSON-`false` sentinel as null per `_is_null` | Matches Finding C; no change |

### Corpus-scale re-validation (2026-06-02 — full-corpus seed)

The 2026-05-31 re-seed consolidated bronze to a single snapshot: **2,005 recall rows (1,216 English silver grain + 789 Spanish), 0 edit-versions**; **7,979 establishments** (1 version each). Unlike FDA/CPSC, USDA was already at full corpus in the 2026-05-28 R2 run (1,216 distinct recalls) — the re-seed mainly removed multi-snapshot edit-stacking (6,072 rows → 2,005 single-version). Re-running the four scripts + the two new siblings (`usda_recalls/.../inspect_field_population.sql`, `usda_establishments/.../inspect_join_key_and_dbas.sql`) **confirmed the R2 findings and added three load-bearing results.** Feeds `documentation/audit/bronze_corpus_profile.md` §1–§6.

**The `''`-sentinel correction (the headline).** `explore_usda_bronze.sql` Q19 counts SQL NULL only and reads **0%** for `establishment`/`states`/`distro_list`/`product_items`/`qty_recovered`/`summary`/`company_media_contact`. But USDA preserves `''` as the missing sentinel (ADR 0027). The nullif-based silver-accurate emptiness — what staging actually sees — is very different:

| Field | Q19 (SQL-NULL) | nullif (silver-accurate) |
|---|---|---|
| `establishment` | 0% | **35.0%** (probe Q4 no_establishment_field) |
| `distro_list` | 0% | **79.7%** |
| `product_items` | 0% | **40.5%** |
| `company_media_contact` | 0% | **44.7%** |
| `states` | 0% | **28.4%** |
| `qty_recovered` | 0% | **14.1%** |
| `recall_reason` | 0% | **1.2%** |
| `summary` | 0% | 0.0% (genuinely populated) |

The establishment side has the same artifact: `duns_number` reads 0% SQL-NULL but is ~85% `''`; `county`/`geolocation` use a `'false'` text sentinel (122 / 94 records). **Any NOT-NULL decision must use the nullif figures, not Q19.**

**The risk_level derive — confirmed 1:1 at corpus scale (Q2).** Class I → High - Class I (823), Class II → Low - Class II (187), Class III → Marginal - Class III (43), Public Health Alert → Public Health Alert (163); `risk_levels_per_classification` = **1 on every row**. The §4/§7 decision to **derive** `risk_level` in silver (CASE WHEN on `recall_classification`), not lift it, is locked.

**Exploded tokens recover the PDF taxonomy (Q4–Q6).** The comma-separated multi-value fields explode cleanly to the documented base sets: `processing` → **10 tokens** (from 20 raw combinations; 2.0% multivalued), `recall_reason` → **9 tokens** (from 26 raw combinations; **30.3% multivalued**) — matching the PDF's 10/9 exactly. The W5 `accepted_values` tests **must run on exploded tokens**: testing raw `recall_reason` would false-fail ~30% of rows. Token SSOTs (with %) are in `bronze_corpus_profile.md` §4.

**Establishment join key (Q1–Q3, new sibling).** `establishment_number` is **100% populated + 100% unique** (7,979/7,979) — the empirical basis for Option A keying `firm_usda_attributes` on it; `establishment_name` is only **86.1% unique** (1,110 shared), confirming the name can't be the key. New shape result: **67.1% of numbers are '+'-joined multi-grant composites** (`M46712+P46712`; prefixes M 81% / V 11% / P 4% / I 3% / G 1%, none outside M/P/I/G/V). Implication: the deferred `product_items` embedded-number match (6b Signal 1) must **split the composite** to match a single embedded grant.

**DBA fallback adds zero join coverage (probe Q2 == Q3 == 82.91%)** — empirically confirms "DBAs marginal." The §7 element-filter removes exactly **94 `'N/A'` + 15 `'None'`** placeholder elements (0 empty-string) of 6,350 total; real-DBA fill is 32.4% (2,585 establishments).

**Confirmed.** `size` 4 values + `''` (Very Small 41.7% / Small 38.9% / **N / A 10.1% undocumented** / Large 6.6% / `''` 2.6%); `status_regulated_est` `''` active 90.0% / Inactive 10.0%; recall→establishment per-record coverage **55.0% on bronze** (→ ~63% post-staging-decode, gated by the 35.0% empty `establishment`); `qty_recovered` dominated by `"0 pounds"` recovered-nothing recalls (note a whitespace-variant duplicate — `"0 pounds"` ×235 and ×17 — for the enrichment Tier-0 trim).

**SCD signal.** 0 edit-versions in the single snapshot (NEED low, snapshot caveat). BENEFIT candidates: `status_regulated_est` active↔Inactive flips (10% Inactive) and the **105/789 bilingual pairs whose EN/ES `last_modified_date` diverge** (FSIS updates languages independently) — both need cross-snapshot history to measure; recorded as hypothesis per the inference-vs-observation discipline.

## References

- `src/schemas/usda.py` — recall bronze Pydantic contract
- `src/schemas/usda_establishment.py` — establishment bronze Pydantic contract
- `dbt/models/staging/stg_usda_fsis_recalls.sql` — recall staging (HTML-entity decode + bilingual filter)
- `dbt/models/staging/stg_usda_fsis_establishments.sql` — establishment staging
- `dbt/models/silver/recall_event.sql:89-124` — USDA → recall_event mapping
- `dbt/models/silver/recall_product.sql:83-102` — USDA → recall_product mapping (where Bug 1 lives)
- `dbt/models/silver/firm.sql:75-86` — USDA → firm with the name-based join
- `dbt/models/silver/recall_event_firm.sql:65-73` — USDA → recall_event_firm
- `dbt/models/silver/firm_usda_attributes.sql` — USDA-only enrichment table (subject of the §6 architectural question)
- `documentation/usda/recall_api_observations.md` — recall API findings A-G (filter behavior, 25 fields, bilingual, etc.), Finding O (Akamai)
- `documentation/usda/establishment_api_observations.md` — establishment API findings A-G
- `documentation/usda/establishment_join_coverage.md` — the ~97% join coverage analysis
- `documentation/usda/bilingual_and_lmd_findings.md` — bilingual + last-modified-date findings
