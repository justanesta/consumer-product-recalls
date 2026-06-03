# NHTSA field audit — 2026 W22

- **Status:** In progress 2026-05-29
- **Scope:** NHTSA FLAT_RCL flat-file recall data (PRE_2010 + POST_2010 archives) — every documented field vs. what we capture, what silver does with it, and what's missing
- **Methodology:** `documentation/audit/methodology.md`
- **Companions:**
  - Existing NHTSA findings: `documentation/nhtsa/flat_file_observations.md` (Findings A-L), `documentation/nhtsa/incremental_delta_findings.md` (Sections A-M, including Pierce + Nissan events)
  - ADRs: 0030 (bronze 11-tuple identity), 0031 (silver fragmentation strategy), 0033 (silver v1.5 SCD migration — Layer 1 docs only as of 2026-05-15)
  - Existing toolkit (referenced, not rebuilt): 18 SQL scripts under `scripts/sql/nhtsa/bronze/` + 8 TSV-analysis Python scripts under `scripts/nhtsa/tsv_analysis/`
  - Source PDF / TXT: `documentation/nhtsa/RCL.txt` (canonical field reference), `documentation/nhtsa/nhtsa_datasets_and_apis.pdf` (broader datasets context), `documentation/nhtsa/RCL_Annual_Rpts.txt`, `documentation/nhtsa/RCL_Qtrly_Rpts.txt` (sibling-dataset references)

NHTSA is the project's only **flat-file** source (no REST API; tab-delimited TSV inside a daily-regenerated ZIP wrapper). The audit shape mirrors FDA/USDA/CPSC, but several sections are NHTSA-specific:

- §1 documents two archives (PRE_2010 + POST_2010) with identical schema except for 4 fields that PRE_2010 leaves empty.
- §3 deals with two real source-to-target column-flip bugs plus one informational cross-source gap.
- §6 (firm) is a cleaner architectural question than CPSC's because NHTSA has **no structured firm identifier at all** — just a free-text manufacturer name, with a load-bearing distinction between "filer" and "manufacturer of items" that current silver collapses.
- §9 references the existing NHTSA toolkit (assertion + diagnose + decompose + watermark probe + TSV-analysis Python suite) instead of building new inspect scripts.

## 1. API field universe (per RCL.txt — 29 fields, 5-drift history since 2007)

### 1a. The single source — two flat-file archives

NHTSA exposes the recall data as two ZIPs containing one tab-delimited TSV each (no header row, YYYYMMDD dates, max record length 17,108 chars):

| Archive | URL | Records | Field-population profile |
|---|---|---|---|
| `FLAT_RCL_POST_2010.zip` (incremental + deep-rescan) | `https://static.nhtsa.gov/odi/ffdd/rcl/FLAT_RCL_POST_2010.zip` | ~240k records (per ADR 0030 + extractor docstring) | All 29 fields populated as documented |
| `FLAT_RCL_PRE_2010.zip` (deep-rescan only) | `https://static.nhtsa.gov/odi/ffdd/rcl/FLAT_RCL_PRE_2010.zip` | TBD (Phase 6a.5 sizing) | 4 fields constant-empty: `mfr_comp_desc`, `mfr_comp_name`, `endman`, `bgman` (per ADR 0030's "Null-rate caveat" — these fields were added 2020 and aren't backfilled into pre-2010 archive) |

Both archives share the 29-field schema documented in `RCL.txt`. `config/sources/nhtsa.yaml:9-16` lists POST_2010 as `file_url` (incremental path) and PRE_2010 in `historical_seed_urls` (deep-rescan only). The deep-rescan path (`NhtsaDeepRescanLoader`, `src/extractors/nhtsa.py:565`) iterates both.

**No watermark, no cursor, no conditional GET.** Per Findings A-C + J: ETag is content-MD5 (usable as a fingerprint but not as an If-None-Match short-circuit because the ZIP wrapper bytes are non-deterministic across daily re-archive jobs); Last-Modified is daily re-stamped regardless of content change; no server-side filter. Every fetch is a full dump. Content-hash dedup (ADR 0007) over the decompressed inner TSV is the change-detection mechanism. The daily watermark probe (`scripts/nhtsa/probe_watermarks.sh` → `documentation/nhtsa/watermark_probes.jsonl`) is the schema-evolution archive (analogous to the cassette-archive role for the other 3 sources).

### 1b. The 29 fields (per RCL.txt)

Names are uppercase as in RCL.txt; bronze captures them via `validation_alias` against lowercase forms produced by the extractor (`src/schemas/nhtsa.py:48-145`).

**Identifiers + campaign-level:**

| # | Field | Type/Size | Description (per RCL.txt) |
|---|---|---|---|
| 1 | `RECORD_ID` | NUMBER(9) | **Running Sequence Number** — explicitly a counter, not a per-row natural key (per ADR 0030 + Finding K). NHTSA regenerates this on every file build |
| 2 | `CAMPNO` | CHAR(12) | NHTSA Recall Number (e.g., `24V930000`) — stable upstream |
| 3 | `MAKETXT` | CHAR(25) | Vehicle/Equipment Make |
| 4 | `MODELTXT` | CHAR(256) | Vehicle/Equipment Model |
| 5 | `YEARTXT` | CHAR(4) | Vehicle/Equipment Model Year, `9999` if Unknown/N/A |
| 6 | `MFGCAMPNO` | CHAR(20) | Manufacturer Campaign Number |
| 7 | `COMPNAME` | CHAR(256) | Component Description |
| 11 | `RCLTYPECD` | CHAR(4) | **Vehicle, Equipment, Child Restraint, Or Tire** — the cross-source-aligned product category. Stored as `rcltype` in bronze (CD suffix dropped) |
| 12 | `POTAFF` | NUMBER(9) | Potential Number of Units Affected |
| 18 | `RPNO` | CHAR(3) | Regulation Part Number |
| 19 | `FMVSS` | CHAR(3) | Federal Motor Vehicle Safety Standard Number. **Narrowed to CHAR(3) May 2025** (Finding F) |
| 24 | `RCL_CMPT_ID` | CHAR(27) | Number That Uniquely Identifies A Recalled Component **within a recall** (added March 2008) |

**Firm fields — TWO distinct semantic concepts:**

| # | Field | Type/Size | Description |
|---|---|---|---|
| 8 | `MFGNAME` | CHAR(40) | **Manufacturer that filed Part 573 Defect/Noncompliance Report** — the *filer* (legally responsible entity for the recall) |
| 15 | `MFGTXT` | CHAR(40) | **Manufacturers of Recalled Vehicles/Equipment/Child Restraint/Tires** — the actual *manufacturer of recalled items*. Distinct from MFGNAME |
| 25 | `MFR_COMP_NAME` | CHAR(50) | Manufacturer-Supplied Component Name (added March 2020) |
| 26 | `MFR_COMP_DESC` | CHAR(200) | Manufacturer-Supplied Component Description (added March 2020) |
| 27 | `MFR_COMP_PTNO` | CHAR(100) | Manufacturer-Supplied Component Part Number (added March 2020) |

**Dates (all YYYYMMDD strings in source, parsed to UTC midnight per `_parse_nhtsa_date`):**

| # | Field | Description |
|---|---|---|
| 9 | `BGMAN` | Begin Date of Manufacturing |
| 10 | `ENDMAN` | End Date of Manufacturing |
| 13 | `ODATE` | Date Manufacturer Notified Owners. **Sentinel `19010101` = unknown** (Finding H) — bronze preserves; silver staging maps to NULL |
| 16 | `RCDATE` | Part 573 Defect/Noncompliance Report Received Date — cleanest "when did the recall happen" signal per Finding H Q2 |
| 17 | `DATEA` | Record Creation Date |

**Recall provenance + advisory:**

| # | Field | Description |
|---|---|---|
| 14 | `INFLUENCED_BY` | Recall Initiated By (`MFR`, `OVSC`, `ODI`) — voluntary vs FDA-requested analog |
| 23 | `NOTES` | Recall Notes (added September 2007) |
| 28 | `DO_NOT_DRIVE` | Consumer Advisory: Do Not Drive (`Yes`/`No`, added May 2025) — bronze coerces to bool |
| 29 | `PARK_OUTSIDE` | Consumer Advisory: Park Outside (`Yes`/`No`, added May 2025) — bronze coerces to bool |

**Free-text narrative (CHAR(2000-6000); HTML markup preserved per Finding E + ADR 0027):**

| # | Field | Description |
|---|---|---|
| 20 | `DESC_DEFECT` | Defect Summary (CHAR(6000), widened May 2025) |
| 21 | `CONEQUENCE_DEFECT` | Consequence Summary. **Note: typo in source** (CONSEQUENCE misspelled) — bronze preserves as `conequence_defect`. See §5 |
| 22 | `CORRECTIVE_ACTION` | Corrective Summary (CHAR(6000), widened May 2025) |

### 1c. Schema-drift history (RCL.txt Change log + Finding F)

The TSV format has gained fields 5 times since 2007 (RCL.txt itself documents this). Each drift event is captured in bronze via nullable defaults so historical archives parse without spurious quarantine; `extra='forbid'` + `strict=True` still catches a future 30th column:

| Date | Drift event | Affected fields |
|---|---|---|
| Sept 2007 | Field 23 added | `NOTES` |
| Sept 2007 | File extension `.lst` → `.txt` | (transport-level) |
| March 2008 | Field 24 added | `RCL_CMPT_ID` |
| March 2020 | Fields 25-27 added | `MFR_COMP_NAME`, `MFR_COMP_DESC`, `MFR_COMP_PTNO` |
| May 2025 | Field 19 narrowed to CHAR(3); fields 20+22 widened to CHAR(6000); fields 28-29 added; all descriptions rewritten | `FMVSS`, `DESC_DEFECT`, `CORRECTIVE_ACTION`, `DO_NOT_DRIVE`, `PARK_OUTSIDE` |

## 2. Current bronze capture (`nhtsa_recalls_bronze` — 29 fields, all captured)

Per `src/schemas/nhtsa.py`. Pydantic snake_case field names match the extractor's lowercase RCL.txt keys (no PascalCase aliasing concern — this is a CPSC-specific gotcha; NHTSA's wire format is uppercase but the extractor lower-cases at parse time). All 29 RCL.txt fields land in bronze; nullable for the drift-added subset.

| Bronze column | RCL.txt field |
|---|---|
| `source_recall_id` | `RECORD_ID` (counter — audit-only, NOT load-bearing per ADR 0030; excluded from content_hash via `hash_exclude_fields`) |
| `campno` | `CAMPNO` |
| `maketxt` | `MAKETXT` |
| `modeltxt` | `MODELTXT` |
| `yeartxt` | `YEARTXT` (CHAR(4) preserved as string — silver casts as needed) |
| `mfgcampno` | `MFGCAMPNO` (nullable) |
| `compname` | `COMPNAME` |
| `mfgname` | `MFGNAME` |
| `rcltype` | `RCLTYPECD` (CD suffix dropped) |
| `potaff` | `POTAFF` (preserved as string — silver casts to int if needed) |
| `mfgtxt` | `MFGTXT` |
| `rcdate` | `RCDATE` (nullable — 5/81,714 PRE_2010 records empty per Finding H Q2) |
| `desc_defect` | `DESC_DEFECT` |
| `conequence_defect` | `CONEQUENCE_DEFECT` (typo preserved) |
| `corrective_action` | `CORRECTIVE_ACTION` |
| `bgman`, `endman`, `odate`, `datea` | dates (all nullable) |
| `influenced_by`, `rpno`, `fmvss` | strings (all nullable) |
| `notes` | `NOTES` (nullable — drift-added 2007) |
| `rcl_cmpt_id` | `RCL_CMPT_ID` (nullable — drift-added 2008) |
| `mfr_comp_name`, `mfr_comp_desc`, `mfr_comp_ptno` | strings (all nullable — drift-added 2020) |
| `do_not_drive`, `park_outside` | bools (Yes/No→bool, nullable — drift-added May 2025) |

**Bronze captures everything RCL.txt documents.** No capture gaps. Storage-forced transforms (per ADR 0027) only — date parsing, bool coercion, FMVSS CHAR(3) constraint. Value-level normalization (sentinel-date → NULL, HTML markup stripping) deferred to staging.

**Bronze identity (per ADR 0030):** 11-tuple = `(campno, maketxt, modeltxt, yeartxt, compname, rcl_cmpt_id, mfr_comp_ptno, mfr_comp_desc, mfr_comp_name, endman, bgman)`. `content_hash` excludes `source_recall_id`. Within-batch dedup handles the 987 byte-duplicate collision groups NHTSA ships in POST_2010. `allow_null_identity=True` accepts empty values as identity-bucket components (load-bearing for the 4 drift-added fields in PRE_2010 + for equipment/tire/child-seat recalls where `bgman`/`endman` are legitimately empty).

**Staging (`stg_nhtsa_recalls.sql`)** — latest-per-11-tuple projection via `row_number() over partition by 11-tuple order by extraction_timestamp desc`. Preserves all 29 fields. No column-level transformation other than passing values through.

## 3. Mismappings (silver — fixable in the (a) PR)

Three findings. **Bug 1** is a clean cross-source-aligned column-flip; **Bug 2** is a semantic-capture finding that splits one role into two; **Bug 3** is informational and reflects a cross-source schema gap, not a NHTSA-specific bug.

### Bug 1 — `recall_product.type = NULL` should be `rcltype`

`dbt/models/silver/recall_product.sql:104-123`:

```sql
nhtsa_products as (
    select
        ...
        compname                                      as product_name,
        mfr_comp_desc                                 as product_description,
        modeltxt                                      as model,
        cast(null as text)                            as type,        -- ← Bug 1
        ...
```

`recall_product.type` is silver's cross-source product-category column. FDA maps `producttypeshort` here (Devices/Food/Drugs/Cosmetics/Veterinary/Biologics); USDA maps `processing` here (per the USDA §3 Bug 1 fix); CPSC maps `products[].Type` here. NHTSA leaves it NULL — but `RCLTYPECD` per RCL.txt is *exactly* the right semantic fit: **Vehicle, Equipment, Child Restraint, Or Tire**. Four-value structured enum, cross-source-alignable, populated 100% (per `explore_bronze_shape.sql` Q10).

**Fix:** `recall_product.type ← rcltype` for NHTSA. The enum values (V/E/T/C per the source; expanded short names: `Vehicle`/`Equipment`/`Tire`/`Child Restraint`) join the cross-source enum surface alongside FDA's `producttypeshort` and USDA's `processing`.

### Bug 2 — `firm` + `recall_event_firm` collapse the filer/manufacturer distinction

`dbt/models/silver/firm.sql:88-97` and `recall_event_firm.sql:75-83`:

```sql
nhtsa_normalized as (
    select distinct
        'manufacturer'              as role,
        mfgname                     as raw_name,
        upper(trim(mfgname))        as normalized_name,
        cast(null as text)          as company_id
    from {{ ref('stg_nhtsa_recalls') }}
    where mfgname is not null
      and trim(mfgname) <> ''
),
```

**The fix uses `mfgname` (RCL.txt field 8) as the manufacturer, but `mfgname` is documented as the Part 573 *filer* — the entity that legally submitted the defect/noncompliance report.** The actual manufacturer of recalled items lives in `mfgtxt` (field 15: "Manufacturers of Recalled Vehicles/Equipment/Child Restraint/Tires").

These can differ. Parent companies file Part 573 reports for subsidiary products. Multi-brand corporations (e.g., a single corporate entity filing for several model lines manufactured by different divisions) put one name in MFGNAME and a different one in MFGTXT.

Current silver:
- `firm` carries the filer-named entity with role='manufacturer' — semantically wrong (it's actually the *filer*)
- `mfgtxt` (the real manufacturer) lives in `recall_product.source_specific_attrs` JSONB (per `recall_product.sql:127`) — invisible to the firm dim
- A cross-source query for "all recalls by manufacturer X" against `recall_event_firm` returns recalls where X *filed* the report, not necessarily where X *made* the affected items

**Fix candidates:**

| Option | Description | Pros | Cons |
|---|---|---|---|
| **A — Status quo, relabel only** | Change `role='manufacturer'` → `role='filer'` for NHTSA's `mfgname` contribution. Add `mfgtxt` as a second `recall_event_firm` row with `role='manufacturer'`. | Honest semantic split. Two distinct concepts get two distinct roles. | Doubles `recall_event_firm` row count for NHTSA recalls. Cross-source firm-rollup queries need to filter by role to compare like-with-like. |
| **B — Coalesce as one role** | Continue using only one (whichever — `mfgname` or `mfgtxt`) as `role='manufacturer'`; drop the other from the firm dim. | Single row per recall in `recall_event_firm`. Simpler queries. | Loses one of the two distinct concepts. Wrong-firm-attribution issue (analog to USDA's cold-storage finding) if we keep the filer and drop the manufacturer. |
| **C — Status quo (do nothing)** | Continue current behavior — `mfgname` as manufacturer. `mfgtxt` stays in JSONB. | No change. | The cross-source firm rollup is semantically wrong for NHTSA. |

**Recommendation: Option A — confirmed by user 2026-05-29.** Both concepts are real; the FastAPI surface should expose both. The `recall_event_firm.role` enum gains `'filer'` (alongside existing `'manufacturer'`, `'retailer'`, `'importer'`, `'distributor'`, `'establishment'`). Cross-source consumers filter by role.

**Empirical confirmation — 2026-05-29 run of `inspect_mfgname_vs_mfgtxt.sql` (74,604 rows, 2,549 campaigns):**

| Question | Finding | Decisive for Option A? |
|---|---|---|
| Q1: Match rate | 83.50% exact, 83.51% normalized. **16.5% genuinely differ.** | Yes — significant semantic split |
| Q3: Substring relationship | **98.4% completely disjoint** when they differ (only 202 of 12,309 differing rows have one as substring of the other) | **Decisive — mismatches are genuinely distinct entities, not casing/formatting variants** |
| Q4: Per-campno mfgname variation | **0%** (0 of 2,549 campaigns) — mfgname is strictly per-recall | Yes — confirms filer is recall-level |
| Q4: Per-campno mfgtxt variation | **1.26%** (32 of 2,549 campaigns), max 5 distinct mfgtxt per campaign | **Decisive — manufacturer is row-level; collapsing into per-campaign loses information** |
| Q7: Cross-source firm dim impact | 384 distinct filers + 374 distinct manufacturers, **85.78% overlap, 24 net-new firms** | Modest blast-radius expansion |
| Q9: Population rates | Both 100% populated (0 NULL/empty) | No nullability handling needed in `firm.sql` |

**Dominant mismatch patterns (Q2 + Q5):**

| Pattern | Example | Rows |
|---|---|---|
| **Engineering subsidiary files for corporate parent** | `Toyota Motor Engineering & Manufacturing` → `Toyota Motor Corporation` | 10,651 (single biggest case) |
| **Upfitter/coachbuilder files for OEM-made base vehicles** | `Rollx Vans` → `Chrysler (FCA US, LLC)`/`Ford Motor Company`/`Toyota Motor Corporation` (campaign 25V876000: 5 distinct OEMs under one Rollx filing) | ~120 across multiple campaigns |
| **Cross-brand contract manufacturing / JV** | `Toyota Motor Engineering & Manufacturing` → `Subaru of America, Inc.` (campaign 25V744000: 7,140 rows of Toyota-built Subarus) | 252 + 7,140 |
| **Tier-1 supplier files for vehicle OEM** | `Volvo Trucks North America` → `Bendix Commercial Vehicle Systems, LLC` | scattered |
| **Corporate-hierarchy disambiguation** | `Chrysler (FCA US, LLC) (Stellantis)` → `Chrysler (FCA US, LLC)` (Stellantis suffix stripped) | 97 |
| **Distributor/import relationship** | `ABC BUS Inc` → `Van Hool N.V.` | 322 |

These patterns confirm both Bug 2 framing AND the Phase 6b NHTSA name-cleaning patterns sketched in §6 — corporate-form suffixes, parent-subsidiary hierarchies, regional qualifiers, and contract-manufacturing relationships are all real. The empirical evidence justifies the role split (Option A); it also gives Phase 6b name-cleaning concrete patterns to handle.

### Bug 3 (informational) — `recall_product.model = modeltxt` is the vehicle model, not the part model

`recall_product.sql:117`:

```sql
modeltxt                                      as model,
```

`MODELTXT` per RCL.txt is "Vehicle/Equipment Model" — the model of the *thing being recalled* (vehicle or equipment), not a manufacturer-supplied part model number. Cross-source comparison:

- **CPSC** `recall_product.model ← products[].Model` — manufacturer's product model (e.g., a stroller model number)
- **FDA** `recall_product.model = NULL` — no analog
- **USDA** `recall_product.model = NULL` — no analog
- **NHTSA** `recall_product.model = modeltxt` — vehicle model name (e.g., "Pacifica", "F-150")

This isn't strictly wrong — for NHTSA, the "product" IS a vehicle, and `modeltxt` IS its model. But the cross-source semantic is inconsistent. The FastAPI consumer querying `recall_product.model` across all 4 sources gets product model strings for CPSC, NULL for FDA/USDA, and vehicle model strings for NHTSA. Worth documenting in `_silver.yml` so consumers know the per-source semantics.

**No code change recommended.** This is a cross-source schema gap to surface in documentation, not a bug to fix in the (a) PR.

## 4. Underused captures — lift from `source_payload_raw` JSONB to first-class silver columns

NHTSA bundles 13+ fields into `recall_event.source_payload_raw` JSONB at `recall_event.sql:143-156` plus more in `recall_product.source_specific_attrs` at `recall_product.sql:124-135`. For the landing-page + FastAPI surface, most of these are core user-facing content or regulatory provenance and should be promoted.

| Bronze field | Current placement | Proposed silver column | Why lift |
|---|---|---|---|
| `corrective_action` | `recall_event.source_payload_raw.corrective_action` | `recall_event.corrective_action` (text) | **Landing-page critical.** Free-text "what consumers should do." Cross-source-alignable with CPSC's `remedies[].Name` and USDA's `recall_reason` remedy content |
| `conequence_defect` | `recall_event.source_payload_raw.conequence_defect` | `recall_event.consequence_of_defect` (text) | **Landing-page critical.** Defect consequences narrative — what happens if the defect occurs. Note the silver column name **fixes the source typo** (consequence, not "conequence") — bronze preserves the typo, silver presents the corrected form |
| `notes` | `recall_event.source_payload_raw.notes` | `recall_event.notes` (text) | Recall notes — sometimes regulatory context, sometimes additional consumer-facing detail. Nullable (drift-added 2007). **38.11% empty per Q1**; populated rows have ~162-char median length (tightly clustered — likely boilerplate templates). Useful but sparse |
| `mfgcampno` | `recall_event.source_payload_raw.mfgcampno` | `recall_event.manufacturer_campaign_id` (text) | Manufacturer's own recall number (distinct from `campno` which is NHTSA-assigned). Useful for cross-referencing to manufacturer's recall website. Cross-source-alignable with FDA's `recall_num` and USDA's `field_recall_number` (though those are source-assigned, not manufacturer-assigned). **38.53% empty per Q1** — sparse but high-value when present |
| `influenced_by` | `recall_event.source_payload_raw.influenced_by` | `recall_event.recall_initiator` (text — `MFR`/`ODI`/`OVSC`/`ISSUE_INVGSTN`) | **Cross-source-alignable** with FDA's `voluntarytypetxt`. **2026-05-29 Q3 corpus distribution:** MFR 87.28% (65,112 rows — firm-initiated), ODI 12.16% (Office of Defects Investigation), OVSC 0.56% (Office of Vehicle Safety Compliance), **ISSUE_INVGSTN 0.01% (4 rows — undocumented in RCL.txt, surface in §5 as new finding).** 0% NULL. Same dominant-voluntary pattern as FDA's 87% Firm Initiated |
| ~~`rpno`~~ | `recall_event.source_payload_raw.rpno` | **DROP — documented-empty-by-source** *(post-2026-05-29 SQL Q1 confirmation: 100% empty across 74,604 rows)* | Originally proposed as `recall_event.regulation_part_number`. Empirically confirmed 100% empty in current bronze. Keep capture in bronze for shape parity; drop from silver `source_payload_raw`. Per Q1 the field has NEVER been populated in the 2023-12-01 → 2026-05-27 window. Phase 6a.5 historical seed may surface a pre-2024 cohort where it was populated; revisit then |
| `fmvss` | `recall_event.source_payload_raw.fmvss` (+ `recall_product.source_specific_attrs.fmvss`) | `recall_event.fmvss_standard` (text — CHAR(3) post-May-2025) | FMVSS standard reference (e.g., "208" for occupant crash protection). Searchable regulatory facet. **48.30% empty per Q1**; ~52% populated with 55 distinct CHAR(3) values per `explore_bronze_shape.sql` Q13 — sparse but high-cardinality enough to be useful as a filter facet |
| `do_not_drive` | already drives `recall_event.status` synth | `recall_event.do_not_drive_advisory` (bool) — keep as separate column alongside `status` | Currently the synthetic `status` field uses `do_not_drive` + `park_outside` to derive `'do_not_drive'`/`'park_outside'`/NULL. The boolean signal should ALSO be a first-class column so consumers can query `WHERE do_not_drive_advisory IS true` without parsing the synth. **2026-05-29 Q4: 506 true (0.68%), 74,098 false (99.32%), 0 NULL** — NHTSA back-filled "No" to all pre-May-2025 historical records when fields were drift-added |
| `park_outside` | same | `recall_event.park_outside_advisory` (bool) | Same. **2026-05-29 Q4: 424 true (0.57%), 74,180 false (99.43%), 0 NULL** |
| `odate` | `recall_event.source_payload_raw.odate` | `recall_event.notified_owners_at` (timestamptz) | Date manufacturer notified owners. Cross-source-alignable with FDA's `initialfirmnotificationtxt` (FDA stores the method; NHTSA stores the date). Useful for landing-page rendering ("Owners notified on...") |
| `potaff` | `recall_event.source_payload_raw.potaff` (+ `recall_product.number_of_units = potaff`) | already in `recall_product.number_of_units` — no lift; **consider also as `recall_event.potential_units_affected`** for event-level aggregation | Cross-source-alignable with CPSC's `products[].NumberOfUnits`, FDA's `productdistributedquantity`, USDA's `qty_recovered`. Already at product level; potentially useful at event level too (sum across products) but might be redundant |
| `mfgtxt` | `recall_event.source_payload_raw.mfgtxt` (+ `recall_product.source_specific_attrs.mfgtxt`) | feeds new `recall_event_firm` row with `role='manufacturer'` per §3 Bug 2 Option A | The semantic-capture lift |
| `rcltype` | currently NULL'd in `recall_product.type` (per Bug 1) | `recall_product.type ← rcltype` | Per Bug 1 |
| `datea` | (not in either JSONB) | already used for `recall_event.published_at = coalesce(datea, rcdate)`; consider exposing as `recall_event.recorded_at` separately | Date NHTSA recorded the receipt. Useful for "when did NHTSA capture this" vs `rcdate` ("when did the Part 573 arrive"). Optional lift |
| Product-level: `bgman`, `endman` | `recall_product.source_specific_attrs.{bgman,endman}` | already documented in ADR 0033 as candidates for SCD Type 1 columns on `recall_product` — not lifted here pending silver v1.5 decision | Defer — overlaps with the in-flight v1.5 migration |
| Product-level: `mfr_comp_name`, `mfr_comp_ptno` | already in `recall_product.source_specific_attrs` | leave in source_specific_attrs (manufacturer-supplied component metadata; low landing-page value) | OK as-is |
| `rcl_cmpt_id` | already in `recall_product.source_specific_attrs` | leave in source_specific_attrs (opaque NHTSA component identifier) | OK as-is |

**Cross-source naming alignment notes (forward-looking, deferred to cross-source consolidation):**

| Cross-source column | CPSC | FDA | USDA | NHTSA |
|---|---|---|---|---|
| `recall_event.description` (defect narrative) | `description` | `productshortreasontxt` (per FDA Bug 1 fix) | `summary` | `desc_defect` |
| `recall_event.corrective_action` / `remedy_text` | `remedies[].Name` | (FDA has none) | (USDA has `recall_reason` partial) | `corrective_action` |
| `recall_event.consequence_of_defect` | (CPSC has none) | (FDA has none) | (USDA has none) | `conequence_defect` |
| `recall_event.recall_initiator` (voluntary vs mandated) | (CPSC has none) | `voluntarytypetxt` | `recall_type` (per USDA Bug 1 fix moves this to lifecycle_status) | `influenced_by` |
| `recall_product.type` | `products[].Type` | `producttypeshort` | `processing` (per USDA Bug 1 fix) | `rcltype` (per Bug 1 here) |
| `recall_event.classification` | (none) | `centerclassificationtypetxt` | `recall_classification` | (no severity classification; `do_not_drive`/`park_outside` are advisory flags but not the same shape) |
| `recall_event.manufacturer_countries` | `manufacturer_countries[]` | (FDA has firm address but not specifically country-of-manufacture) | (USDA has establishment address) | (NHTSA has no country-of-manufacture; recalls cover US-distributed vehicles) |

The cross-source naming pass after all 4 source audits land is where these get final names.

## 5. Field-naming gotchas

| Surface | Gotcha | Notes |
|---|---|---|
| **Source typo** | `CONEQUENCE_DEFECT` (field 21) — should be CONSEQUENCE | Bronze preserves the typo as `conequence_defect`. Silver §4 lift presents the corrected `consequence_of_defect` column name (silver-layer rename pattern; bronze stays source-faithful per ADR 0027) |
| RCL.txt name vs bronze name | `RCLTYPECD` (field 11) → `rcltype` | CD suffix dropped in schema field name. Minor; documented in schema comments |
| `RECORD_ID` is a counter, not a primary key | Per ADR 0030 + Finding K | RCL.txt's "Running Sequence Number, Which Uniquely Identifies The Record" language is true within a single TSV only — regenerated per file build. Use the 11-tuple, not `source_recall_id`, for joins |
| TSV ships byte-duplicate rows | ~0.7% of POST_2010 corpus per Finding L (987 collision groups across ~240k rows) | Handled by `within_batch_dedup=True` in BronzeLoader. Audit doesn't see this — bronze materializes one row per logical fact |
| Date sentinels | `ODATE 19010101` = unknown; `YEARTXT 9999` = unknown | Bronze preserves; staging maps `ODATE = 1901-01-01` to NULL per ADR 0027 (Finding H). **2026-05-29 Q5 corpus frequencies:** `odate=1901-01-01` 0 records in current bronze (no sentinel hits yet); `odate IS NULL` 4,787 (6.42%); **`yeartxt='9999'` 20,485 records (27.46%) — much higher than expected.** Recommend silver staging also map `yeartxt='9999'` → NULL (parallel to `odate` sentinel handling); currently `stg_nhtsa_recalls.sql` passes the sentinel through verbatim |
| `INFLUENCED_BY` undocumented value | RCL.txt documents MFR/OVSC/ODI as the enum; **2026-05-29 Q3 surfaces a 4th value: `ISSUE_INVGSTN` (4 records, 0.01%)** | Not in RCL.txt; not in `flat_file_observations.md`. New finding. Worth: (a) email NHTSA OII to ask about the value's semantics; (b) silver `accepted_values` test should include all 4 observed values at `severity=warn`; (c) Phase 6a.5 post-seed re-run may surface additional undocumented values from older cohort |
| Empty string vs NULL in bronze | Drift-added optional text fields (`notes`, `mfr_comp_*`, `mfgcampno`, `rpno`, `fmvss`) stored as **empty string `""`, not NULL**, when source omits them | Per ADR 0027 storage-forced transforms in bronze; staging is responsible for empty-string→NULL normalization. `explore_bronze_shape.sql` Q9 counts `is not null` (shows 100% for drift-added fields), while `inspect_field_population.sql` Q1 counts `null OR empty` (shows actual sparsity: notes 38% empty, mfgcampno 38% empty, rpno 100% empty). The bronze docstring frames these as "nullable" but they're predominantly empty-string at the bronze layer. Document in `silver_design_notes.md` so future readers don't confuse "100% non-NULL" with "100% populated" |
| `MAKETXT` casing vs `MFGNAME`/`MFGTXT` casing | **`maketxt` is uppercase** in source ("TOYOTA", "FORD", "MERCEDES-BENZ"), but `mfgname`/`mfgtxt` are mixed-case ("Toyota Motor Engineering & Manufacturing", "Chrysler (FCA US, LLC) (Stellantis)") | Different source-side normalization conventions. `firm.sql`'s `upper(trim())` already handles both. But the casing-difference matters for landing-page rendering: vehicles want title-case ("Toyota") while filer/manufacturer names should preserve source mixed-case. Worth documenting in `_silver.yml` |
| `FMVSS` width | Narrowed from larger to CHAR(3) in May 2025 (Finding F) | Schema enforces `max_length=3`; longer values quarantine. Pre-2025 records that had longer FMVSS values (if any) would have already landed before the narrowing |
| `MFGNAME` vs `MFGTXT` | Field 8 is the **filer**; field 15 is the actual **manufacturer** of recalled items. Distinct semantics | See §3 Bug 2 |
| `DO_NOT_DRIVE` / `PARK_OUTSIDE` values | Strings `"Yes"`/`"No"` in source (not booleans on the wire) | Bronze `_to_bool` validator coerces. Distinct from USDA's `"True"`/`"False"` strings (per Pydantic validator docstring) |
| PRE_2010 archive — 4 fields constant-empty | `mfr_comp_desc`, `mfr_comp_name`, `endman`, `bgman` per ADR 0030 + Finding G | The 11-tuple is over-specified on PRE_2010 (those 4 fields all `""`); harmless because empty values are valid identity-bucket components per `allow_null_identity=True` |
| TSV file shape | Tab-delimited, no header row, YYYYMMDD dates, max record length 17,108 chars per RCL.txt | Distinct from CPSC/FDA/USDA's JSON shape; flat file extractor handles |
| ZIP wrapper bytes non-deterministic | Per Finding J | Conditional GET unavailable for ZIP wrappers; etag_enabled=false in config. Inner-TSV SHA is the actual content fingerprint (`response_inner_content_sha256` column on `extraction_runs`) |
| ETag is content-MD5 | Per Finding A | Usable as a content fingerprint for audit, but Finding B (Last-Modified daily re-stamped) + Finding J (ZIP non-determinism) mean Conditional GET short-circuit doesn't work |
| Last-Modified is daily-stamped | Per Finding B | Daily re-stamp regardless of content change; not a usable cursor signal |
| Embedded HTML in narrative fields | `desc_defect`, `conequence_defect`, `corrective_action`, `notes` carry embedded `<a>` anchor tags per Finding E | Bronze preserves verbatim per ADR 0027; landing-page rendering must HTML-sanitize |
| No watermark | Full dump every fetch | Content-hash dedup is the change-detection mechanism. No `default_lookback_days` concept |
| Cassette philosophy | Real S3 response headers + hand-built 1.8 KB ZIP body (not the live ~14 MB body) | Per `tests/fixtures/cassettes/nhtsa/README.md`. Distinct from CPSC/FDA/USDA cassettes which commit real bodies. The watermark probe is the schema-archive role |

## 6. The firm-relationship question — no structured identifier (structural finding)

NHTSA is the only audited source with **zero structured firm identifiers**:

- No FEI (the FDA analog)
- No FSIS establishment_number (the USDA analog)
- No MIC (the USCG analog, if/when USCG returns)
- CPSC has CompanyID but it's empirically always empty (per CPSC §3 Bug 3) — also no real structured ID, just like NHTSA

Both `MFGNAME` (filer, field 8) and `MFGTXT` (manufacturer of items, field 15) are free-text CHAR(40) strings with no associated structured identifier. The Phase 6b cross-source firm reconciliation against `firm_fei_num` per ADR 0002 + FDA `firm_legal_nam` won't get any anchor signal from NHTSA — the join lever is name-only, RapidFuzz fuzzy matching against FDA/USDA strings, with all the AC DELCO / ACDELCO class fragmentation NHTSA exhibits (per ADR 0031:84).

### NHTSA's specific firm patterns (informing Phase 6b name normalization)

Cassette + corpus samples surface several patterns that the Phase 6b CPSC suffix-strip work should also handle for NHTSA `mfgname` and `mfgtxt`:

- **Corporate-form suffixes** — "Ford Motor Company", "General Motors LLC", "Honda Motor Co., Ltd.", "Mercedes-Benz USA, LLC"
- **Regional qualifiers** — "Toyota Motor North America, Inc.", "Volkswagen Group of America, Inc.", "BMW of North America, LLC"
- **Parent-subsidiary patterns** — "Stellantis (FCA US LLC)", "Chrysler Group LLC" (historic), "Daimler Trucks North America LLC"
- **Casing/spacing drift** — "AC DELCO" ↔ "ACDELCO" (the canonical ADR 0031:84 example)
- **Acronym vs full-name** — "GM" vs "General Motors" vs "GENERAL MOTORS LLC"

This matches the §6 + Phase 6b plan from the CPSC audit + USDA audit — strip suffix patterns + RapidFuzz fuzzy matching. The same `firm.alternate_names` JSONB column populated from CPSC DBA extraction + USDA `dbas` array would also house NHTSA's alternate corporate forms (e.g., resolve "AC DELCO" + "ACDELCO" to one firm with `alternate_names: ["AC Delco", "ACDelco"]`).

### Cross-reference to USDA / CPSC

| Source | Structured ID | Fragmentation mode | Phase 6b approach |
|---|---|---|---|
| CPSC | None (CompanyID empty) | Suffix-strip ("of <X>", "dba <Y>") + RapidFuzz | Per CPSC audit §6 Option B |
| USDA | `establishment_number` (100% populated per USDA §6) | Per-recall disambiguation via 5-signal hierarchy | Per USDA audit §6 |
| FDA | `firm_fei_num` (mostly populated) | Cross-source anchor; FDA firms are well-identified | RapidFuzz handles within-FDA + cross-source |
| **NHTSA** | **None at all** | **Pure name-matching; corporate-form suffixes + casing drift** | **Same suffix-strip + RapidFuzz as CPSC, plus the §3 Bug 2 filer-vs-manufacturer role split** |

Updated Phase 6b plan should add NHTSA-specific suffix patterns to the CPSC name-cleaning regex once Phase 6a.5 historical seed lands (corpus-scale evidence on NHTSA suffix variants).

## 7. Decisions locked in (confirmed 2026-05-29)

All items below confirmed by user in conversation 2026-05-29. Bug 2's implementation scope contingent on the `inspect_mfgname_vs_mfgtxt.sql` empirical run (see §3 Bug 2 + §9).

1. **Bug 1 — `recall_product.type ← rcltype` for NHTSA.** Cross-source semantic enum (`Vehicle`/`Equipment`/`Tire`/`Child Restraint`). Reversing the silver NULL'd column to use the populated source field.
2. **Bug 2 — Option A: filer/manufacturer role split (empirically confirmed 2026-05-29).** `recall_event_firm.role` enum gains `'filer'`. NHTSA contributes:
   - `mfgname` → `recall_event_firm` row with `role='filer'`
   - `mfgtxt` → `recall_event_firm` row with `role='manufacturer'`
   - Both via `firm.sql` cross-source dim
   - When `mfgname = mfgtxt` (83.5% of rows per 2026-05-29 SQL Q1), the firm contributes two distinct `recall_event_firm` rows but one `firm` dim row (deduped by normalized_name)
   - When they differ (16.5%), the role split honors the semantic distinction — 98.4% of differences are completely-disjoint entities (Q3), not casing variants. Patterns: corporate-subsidiary filings (Toyota E&M for Toyota Corp — 10,651 rows), upfitter filings for OEM-made base vehicles (Rollx Vans for Chrysler/Ford/Toyota), cross-brand contract manufacturing (Toyota built cars for Subaru — 7,140 rows in campaign 25V744000), tier-1 supplier filings (Bendix for Volvo Trucks)
   - **Implementation blast radius:** +24 net-new firms in cross-source dim (408 combined vs 384 mfgname-only per Q7); ~1.26% of NHTSA campaigns produce multi-manufacturer fan-out under Option A (per Q4)
3. **Bug 3 — documented gap, no code change.** `recall_product.model = modeltxt` is vehicle model for NHTSA; cross-source consumers know per-source semantics differ. Note in `_silver.yml`.
4. **§4 lifts to first-class silver columns** (post-2026-05-29 empirical refinement — 9 lifts confirmed, 1 dropped):
   - `corrective_action` → `recall_event.corrective_action` (0% empty — always populated)
   - `conequence_defect` → `recall_event.consequence_of_defect` *(silver-layer typo-correction in column name)* (0% empty)
   - `notes` → `recall_event.notes` (38.11% empty — sparse but lift retains landing-page value)
   - `mfgcampno` → `recall_event.manufacturer_campaign_id` (38.53% empty — sparse but high-value cross-reference when present)
   - `influenced_by` → `recall_event.recall_initiator` (0% empty; 4-value enum including undocumented `ISSUE_INVGSTN` per §5)
   - ~~`rpno` → `recall_event.regulation_part_number`~~ **CANCELLED — documented-empty-by-source per 2026-05-29 Q1 (100% empty across 74,604 rows).** Keep capture in bronze; drop from silver source_payload_raw. Revisit post-Phase 6a.5 if pre-2024 cohort has populated values
   - `fmvss` → `recall_event.fmvss_standard` (48.30% empty — moderate population; ~52% populated with 55 distinct CHAR(3) values)
   - `do_not_drive` → `recall_event.do_not_drive_advisory` (0% NULL; 0.68% true) (alongside `status` synth which keeps using both)
   - `park_outside` → `recall_event.park_outside_advisory` (0% NULL; 0.57% true)
   - `odate` → `recall_event.notified_owners_at` (6.42% NULL; 0 sentinel hits)
   - `mfgtxt` → flows through Bug 2 Option A; not a separate column
   - Cross-source column names deferred to cross-source consolidation
5. **§3 / §4 — defer to silver v1.5 (ADR 0033) for batch-window columns.** `bgman` and `endman` placement on `recall_product` (currently in `source_specific_attrs`) overlaps with the in-flight v1.5 SCD migration. Don't lift here; let v1.5 resolve.
6. **Documented-empty by source (no lift):** `RECORD_ID` (counter, not load-bearing per ADR 0030); **`rpno` (100% empty per Q1 — confirmed)**; the 4 PRE_2010-empty fields (`mfr_comp_desc`, `mfr_comp_name`, `endman`, `bgman`) on PRE_2010 records specifically.

9. **NEW post-empirical decision — silver staging sentinel mapping for `yeartxt`.** Per Q5 2026-05-29: `yeartxt='9999'` is 27.46% of corpus (20,485 records). Currently `stg_nhtsa_recalls.sql` passes the sentinel through verbatim — downstream consumers see "model year 9999" which is the documented-unknown sentinel, not a real year. Add `case when yeartxt = '9999' then null else yeartxt end as model_year` to staging (parallel to `odate` sentinel handling per ADR 0027). Pre-existing behavior is technically correct (preserves source bytes) but operationally confusing for landing-page rendering and aggregation queries.
7. **HTML markup handling unchanged.** `desc_defect`, `conequence_defect`, `corrective_action`, `notes` carry embedded HTML; bronze + silver pass verbatim; landing-page rendering sanitizes. Same as USDA §7 #6.
8. **Cassette philosophy unchanged.** Real headers + hand-built fixture body. Watermark probe is the schema archive role.

## 8. Capture-expansion items deferred to backlog

Logged in `documentation/audit/capture_expansion_backlog.md` § NHTSA (to be populated). Summary:

- **HIGH — none.** Bronze fully captures all 29 RCL.txt fields. Schema-drift guard (`extra='forbid'`) catches a future 30th column. No capture gap.
- **MEDIUM — none.** Same reason.
- **LOW — none.** Same reason.
- **SKIP — sibling NHTSA datasets.** Per `documentation/nhtsa/nhtsa_datasets_and_apis.pdf` (300 KB, broader NHTSA datasets context), NHTSA exposes many additional datasets: `RCL_Annual_Rpts.txt` + `RCL_Qtrly_Rpts.txt` (whose RCL.txt-sibling field references we have under `documentation/nhtsa/`), the `recalls.json` API, SaferCar API, Complaints, Investigations, etc. These are scope-expansion candidates (different data shapes — recall annual summary stats, complaints, investigations); none replace or enrich the FLAT_RCL data we already capture. Defer to Phase 7+ scope-expansion review if FastAPI consumers request these features.
- **SKIP — `recalls.json` API as alternative source.** NHTSA also exposes the recall data via a REST JSON API. Same data, different format. The flat-file path is more reliable for bulk ingest (paginated JSON would multiply request counts dramatically); no reason to switch.

**Like USDA and CPSC, NHTSA's (b) capture-expansion PR has zero adds.** All 29 documented fields land in bronze. The audit's gains are §3 + §4 lifts (silver schema enrichment) + §6 firm-relationship Option A + §5 documented gotchas.

## 9. R2 validation status — reference existing toolkit, no new scripts needed

NHTSA's toolkit predates this audit. The audit's role is to *reference* what already exists, not duplicate it.

### Existing toolkit (per `scripts/sql/nhtsa/bronze/` + `scripts/nhtsa/tsv_analysis/`)

**Audit-specific additions (this audit):**

| Script | Purpose | Audit §9 use |
|---|---|---|
| `scripts/sql/nhtsa/bronze/inspect_mfgname_vs_mfgtxt.sql` | 9-query batch comparing `mfgname` (Part 573 filer) vs `mfgtxt` (manufacturer of items): exact + normalized match rate, top mismatched pairs, substring-relationship analysis, per-campno variation, length/cardinality comparison, cross-source firm-rollup volume impact, qualitative samples, NULL rates | **Run 2026-05-29 against 74,604-row bronze** — empirical results folded into §3 Bug 2 confirm Option A (83.5% match, 98.4% completely-disjoint when differ, 0% per-campno mfgname variation vs 1.26% per-campno mfgtxt variation). Re-run post-Phase 6a.5 against full corpus to surface additional pre-2024 patterns. |
| `scripts/sql/nhtsa/bronze/inspect_field_population.sql` | 6-query batch covering the gaps that `explore_bronze_shape.sql` Q1-Q16 doesn't: (Q1) per-field NULL/empty rate for all 29 fields, (Q2) narrative field length distributions (desc_defect, conequence_defect, corrective_action, notes), (Q3) `influenced_by` enum distribution (MFR/OVSC/ODI), (Q4) `do_not_drive`/`park_outside` true/false/null breakdown, (Q5) sentinel frequencies (`yeartxt='9999'`, `odate='1901-01-01'`), (Q6) `potaff` distribution buckets | **Pending run.** Mirrors `inspect_array_field_population.sql` for CPSC + corpus-scale `inspect_landed_payloads.py` for FDA/USDA. Results validate §4 lift NULL rates, §3 Bug 1 rcltype distribution (via `explore_bronze_shape.sql` Q10), §5 sentinel documentation, and recall_event.status synth assumptions in `recall_event.sql:137-141`. |

### SQL findings — 2026-05-29 run (74,604 records, --since=2023-12-01 slice)

Run sequence: `explore_bronze_shape.sql` Q1-Q16 + `inspect_field_population.sql` Q1-Q6.

**Corpus shape (`explore_bronze_shape.sql` Q1-Q7):**

- 74,604 rows / 2,549 distinct campaigns / 72,175 distinct RECORD_IDs / 29.27 avg rows per campaign
- Date range: 2023-12-01 → 2026-05-27 (matches `--since=2023-12-01` extraction window)
- Yearly cadence: 2023 (108 campaigns from Dec only), 2024 (1,073), 2025 (997), 2026 (371 YTD)
- Top fan-out: campaign 24T014000 with 19,321 rows (Takata-class tire recall, single year, 2 makes)
- Burst days: 2024-12-09 with 21,208 rows (the Takata event); 2025-10-30 with 7,216 rows (Toyota-built Subarus campaign 25V744000)

**Confirmations (no audit doc change needed):**

- ✅ **§3 Bug 1 confirmed at corpus scale.** Q10 rcltype distribution exactly matches RCL.txt's 4-value enum: V 72.5% (54,098 / 2,271 campaigns), T 26.1% (19,445 / 37 campaigns — huge per-campaign fan-out on tire recalls), E 1.3% (988 / 228), C 0.1% (73 / 13). No surprise values.
- ✅ **§5 RECORD_ID counter behavior** — Q1 shows 72,175 distinct RECORD_IDs for 74,604 rows (97% distinct), with multi-hash clusters in Q4 (e.g., source_recall_id 321732 has 4 distinct content_hashes) — exactly the per-build regeneration pattern documented in ADR 0030 + Finding K.
- ✅ **§5 ADR 0030 bgman/endman population profile** — Q1 shows bgman 68.89% empty, endman 68.97% empty, matching the prediction that V recalls have manufacturing dates while E/T/C recalls don't.
- ✅ **§5 ADR 0030 drift-added field nullability** — Q9 shows 100% population by year (notes, rcl_cmpt_id, mfr_comp_name, do_not_drive) BUT this counts `is not null`. Q1 of `inspect_field_population.sql` (counting NULL OR empty) reveals true sparsity — see "New findings" below.
- ✅ **FMVSS Finding F (May 2025 CHAR(3) narrowing) at corpus scale** — Q13 shows 38,568 populated rows with 55 distinct CHAR(3) values; 36,036 empty rows (matches §1 Q1 fmvss 48.30% empty). No regression-to-wider values observed.
- ✅ **§4 do_not_drive / park_outside synth assumptions hold** — Q4 confirms 0.68% true / 0.57% true respectively. recall_event.status synth produces NULL for ~99% of records (expected).
- ✅ **`influenced_by` cross-source alignment with FDA voluntarytypetxt validated** — Q3 shows MFR 87.28% / ODI 12.16% / OVSC 0.56%, mirroring FDA's 87% Firm Initiated / 12-13% other.

**New findings refining §1, §4, §5, §7:**

- 🆕 **`rpno` is 100% empty across 74,604 rows** (Q1). §4 lift cancelled (now in §7 documented-empty list). §1b row updated. Phase 6a.5 historical seed may surface pre-2024 rpno-populated records; revisit then.
- 🆕 **`influenced_by` has a 4th undocumented value: `ISSUE_INVGSTN`** (4 records, 0.01%). Not in RCL.txt. New §5 row added. Worth emailing NHTSA OII to ask about semantics.
- 🆕 **`yeartxt = '9999'` sentinel is 27.46%** of corpus (20,485 records). Much higher than expected. New §7 decision #9: silver staging should map sentinel → NULL parallel to `odate` handling.
- 🆕 **`mfgcampno` 38.53% empty** — sparse but valuable when present; lift retained with NULL-rate noted.
- 🆕 **`notes` 38.11% empty** with tightly-clustered ~162-char median length (likely boilerplate templates) — lift retained.
- 🆕 **`do_not_drive` / `park_outside` 100% populated since 2023-12** despite being drift-added May 2025. NHTSA backfilled "No" to all historical records when fields were added. Operationally clean — no NULL handling needed in §4 lifts.
- 🆕 **Empty string vs NULL distinction in bronze for optional text fields** — bronze stores `""` (not NULL) when source omits. Per ADR 0027 staging does empty→NULL. New §5 row added.
- 🆕 **`maketxt` is uppercase ("TOYOTA"), but `mfgname`/`mfgtxt` are mixed-case ("Toyota Motor Engineering & Manufacturing")** — different source-side normalization conventions. New §5 row added.
- 🆕 **Narrative field lengths well-bounded.** desc_defect max 794 (avg 398); corrective_action max 1330 (avg 284); conequence_defect max 424 (avg 108); notes max 1815 (avg 164 — tightly clustered around 162 indicating boilerplate). All fit comfortably in `text` columns. No truncation concerns. (Contrast CPSC's Description max 5,983 chars.)
- 🆕 **`potaff` distribution well-spread.** No NULLs or 0 values. Q6: 1-99 5%, 100-999 11%, 1K-9.9K 13%, 10K-99K 14%, 100K-999K 40% (dominant), 1M-9.9M 17%. Max 4.4M in current bronze. Phase 6a.5 seed likely surfaces 10M+ Takata-class events from older cohort.

**Pre-existing toolkit bug surfaced (not audit-fix scope):**

- ⚠️ **`explore_bronze_shape.sql` Q14** — references `rejection_reason` column which doesn't exist; should be `failure_reason` (per CPSC + FDA extractor conventions, see `src/extractors/cpsc.py:67-77` table definition). Pre-existing bug, surfaces only on quarantine inspection. One-line fix; outside this audit's scope. File separately.

### Phase 6a.5 post-seed re-validation

After NHTSA historical seed in Phase 6a.5, re-run both scripts. Expected deltas:
- Bronze row count: 74,604 → ~~440k (PRE_2010 + POST_2010 combined per Phase 6a.5 plan)
- `rpno` may surface non-empty values in pre-2024 cohort (currently 100% empty)
- `influenced_by` may surface additional undocumented values
- `bgman`/`endman` empty rate may increase (older PRE_2010 archive has more constant-empty rows)
- `yeartxt='9999'` rate may increase (older recalls more likely to have unknown model year)
- `do_not_drive` / `park_outside` may have actual NULL values for older records if NHTSA's backfill wasn't comprehensive
- `potaff` max may surface 10M+ Takata-class events


**Pre-existing bronze SQL (Tier 2 detection + diagnostics):**

| Script | Purpose | Audit §9 use |
|---|---|---|
| `explore_bronze_shape.sql` | 16-query batch covering row counts, cadence, fan-out, edit detection, drift-added field population, rcltype/yeartxt/make distributions, FMVSS length, extraction_runs history | **Primary corpus-scale field-rate inspector.** Equivalent to FDA/USDA/CPSC's `inspect_landed_payloads.py` for NHTSA. Run after Phase 6a.5 historical seed |
| `assert_eleven_tuple_identity_stable.sql` | Tier 2: cross-run drift on 10 non-anchor identity fields. Per-path-value-set semantics (post-2026-05-12 refactor). Severity=warn | Continuous monitoring; not run as part of audit |
| `assert_nine_tuple_identity_stable.sql` | Tier 2: cross-run drift on 8 silver-canonical fields (drops bgman/endman). Originally for the v1.5 migration evaluation; sunset 2026-05-15 per ADR 0031 / 0033 but still runs as a routine monitoring signal | Continuous monitoring |
| `decompose_eleven_tuple_drift.sql` | Diagnostic: splits Tier 2 drift into structural-multi-batch (silver-correct) vs real_drift (the assertion numerator) | Investigate drift events; produces the per-field decomposition table in ADR 0031 |
| `decompose_nine_tuple_drift.sql` | Same as above, for 9-tuple | Same |
| `attribute_mfgcampno_shifts_by_campno.sql` | Attribute analysis: when `mfgcampno` shifts within a campno across runs | Drift triage |
| `attribute_rcdate_shifts_by_campno.sql` | Same for `rcdate` (per Section M.2 — 2026-05-25 rcdate corrections) | Drift triage |
| `attribute_structural_drift_by_campno.sql` | Same for the structural-multi-batch class | Drift triage |
| `diagnose_full_reinsert.sql` | Diagnoses the original RECORD_ID-as-identity bug from ADR 0030 era | Pre-ADR-0030 history; reference only |
| `diagnose_null_regression.sql` | Diagnoses populated→NULL field transitions (e.g., Mack 26V261000 H1 cluster per ADR 0031 / Section H.4) | Drift triage |
| `find_row_differentiator.sql` | What differs between rows in a same-tuple collision set | Drift triage |
| `investigate_residual_collisions.sql` | The 477-row residue investigation that drove ADR 0030's 7-tuple → 11-tuple widening | Historical reference |
| `investigate_tire_collision.sql` | Same for tire-specific subset | Historical reference |
| `verify_eleven_tuple_row_unique.sql` | Validates the 11-tuple is row-unique post-extraction. `excess_rows = 0` is the pass criterion | Post-extraction sanity check; pass after Phase 6a.5 seed |
| `verify_natural_key_candidate.sql` | Validates candidate identity tuples | Historical reference |
| `verify_six_tuple_identity.sql` | Validates the 6-tuple grain (ADR 0033 v1.5 candidate) | v1.5 design reference |

**TSV-analysis Python (`scripts/nhtsa/tsv_analysis/`):**

| Script | Purpose |
|---|---|
| `_lib.py` | TSV streaming + SHA-256 + field name → index mapping |
| `inspect_archive_row.py` | Byte-level row inspection for H1/H2 verdicts on drift events (the canonical "is bronze right?" tool) |
| `identity_search.py` | Iterative identity-tuple widening (load-bearing for ADR 0030's 11-tuple decision) |
| `uniqueness_at_tuple.py` | Single-tuple uniqueness check for any candidate identity |
| `find_differentiator.py` | Column-by-column distinct-count analysis with optional row filter |
| `cross_corpus_stability.py` | Full-corpus cross-TSV-capture drift detection (caught the AC DELCO `maketxt` normalization) |
| `diff_inner_tsv.py` | Inner-TSV diff between two archives |

**Operational tooling:**

| Script | Purpose |
|---|---|
| `scripts/nhtsa/probe_watermarks.sh` | Daily HTTP HEAD + body SHA capture against 15 NHTSA URLs |
| `scripts/nhtsa/probe_sentinels.sh` | Probes for sentinel value behavior |
| `scripts/nhtsa/download_archives.sh` | Bulk archive download utility |
| `scripts/nhtsa/verify_collisions_raw_tsv.sh` | Cross-references bronze duplicate rows against raw TSV |
| `documentation/nhtsa/watermark_probes.jsonl` | Committed daily-probe archive (Findings A-C empirical record) |

### Existing audit doc coverage

`documentation/nhtsa/flat_file_observations.md` (Findings A-L) covers:
- Finding A: ETag is content-MD5
- Finding B: Last-Modified watermark reliability (daily re-stamped)
- Finding C: x-amz-version-id behavior
- Finding D: year-band URL pattern
- Finding E: TSV column count + encoding + embedded HTML
- Finding F: documented schema-drift history
- Finding G: header inventory + CDN/cache layer
- Finding H: update cadence + historical coverage (sentinel dates)
- Finding I: format heterogeneity (TSV historical vs CSV recent — not currently extracted)
- Finding J: ZIP wrapper bytes non-deterministic
- Finding K: RECORD_ID per-build regeneration
- Finding L: TSV byte-duplicate rows

`documentation/nhtsa/incremental_delta_findings.md` (Sections A-M) covers:
- A: dedup architecture functioning
- B: net-new vs amendment split
- C: RECORD_ID per-build counter
- D: which fields drive amendments
- E: amendment backdating window
- F: bursty distribution
- G: structural multiplicities in 11-tuple identity
- H: 2026-05-12 assertion refactor + updated daily-delta sample (including Mack 26V261000 H1 case)
- I: 2026-05-13 Nissan CUBE 26V230000 NULL-regression
- J: 2026-05-15 healthy weekly amendment baseline
- K: 2026-05-15 Pierce ARROW XT family 26V217000 mfr_comp_desc population event (the Stop criterion #1 firing for v1.5 migration)
- L: 2026-05-16 post-Pierce stability
- M: 2026-05-19 → -25 amendment series + rcdate corrections + methodology refinements

**Together these cover everything an FDA/USDA/CPSC-style audit `inspect_landed_payloads.py` would surface, plus considerably more.**

### Phase 6a.5 post-seed re-validation

Per `project_scope/phase-6-execution-plan.md` § Phase 6a.5, after the NHTSA historical seed completes:

- [ ] Re-run `explore_bronze_shape.sql` Q1-Q16 against full PRE_2010 + POST_2010 corpus. Expected: row counts roughly double (depending on PRE_2010 size); rcltype distribution shifts (more Vehicle vs Equipment proportionally on older recalls); FMVSS length distribution may surface pre-May-2025 wider values; per-year rcdate distribution shows the 1979 bulk-load event per Finding H
- [ ] Re-run `assert_eleven_tuple_identity_stable.sql` post-seed. Expected: warn-count delta documented; per §6a.5 quality gate, "any new drift groups triaged"
- [ ] Re-run `verify_eleven_tuple_row_unique.sql`. Expected: `excess_rows = 0` (validates within_batch_dedup handled PRE_2010 too)
- [ ] **NEW corpus-scale finding to surface:** PRE_2010 rcltype distribution. Pre-2008 records may show a wider distribution of `rcltype` values than recent years — relevant for Bug 1 fix scope
- [ ] **NEW corpus-scale finding to surface:** PRE_2010 firm-name distribution. Older recalls may have more historical corporate forms (defunct manufacturers, acquired-and-renamed entities) — relevant for §6 Phase 6b name-cleaning patterns

### What's still pending (out of scope for this audit, deferred to Phase 6a.5)

- [ ] Corpus-scale top-level field-rate inspect (Phase 6a.5 re-run of `explore_bronze_shape.sql`)
- [ ] Corpus-scale firm-name analysis to inform Phase 6b NHTSA-specific suffix patterns
- [ ] Cross-corpus stability run post-seed (`cross_corpus_stability.py`) — likely surfaces additional AC DELCO-class drift events given the wider time window
- [ ] Capture-expansion review for `recalls.json` API + `RCL_Annual_Rpts` if FastAPI consumers request these features

### Corpus-scale re-validation (2026-06-02 — silver-field-remap W1, 321,592 rows)

Closes most of the "Phase 6a.5 post-seed re-validation" checklist above (explore Q1–Q16 + the 11-tuple assert + inspect + mfgname_vs_mfgtxt re-run). The full FLAT_RCL seed + the 2026-06-02 incremental put bronze at **321,592 rows / 30,045 campaigns / 321,425 distinct records** — the 74,604 above was a `--since` slice; the full corpus is ~4.3× and makes NHTSA the **largest** bronze table. The incremental inserted **167 changed rows**, so unlike the single-shot FDA/CPSC/USDA seeds NHTSA carries **real cross-run edit-versions** — which makes the identity assert a measurement, not a hypothesis. Feeds `documentation/audit/bronze_corpus_profile.md` §1–§6.

**11-tuple identity STABLE at corpus scale (the SCD anchor — ADR 0033 holds).** `assert_eleven_tuple_identity_stable.sql`: the natural-key core (`compname`/`maketxt`/`modeltxt`/`yeartxt`/`rcl_cmpt_id`/`mfr_comp_name`) = **0 drift**; the only non-zero is `mfr_comp_ptno` (7 groups), and the samples are supplier part-number supersession (NOVA BUS window part numbers across 2 landing paths) — the documented structural-multi-batch class (silver-correct, not fragmenting). The 11-tuple surrogate key does **not** fragment on the 167 real edit-versions.

**The `''`-sentinel correction (load-bearing).** `explore_bronze_shape.sql` Q9 reports the drift-added fields as 100% populated every year back to 1966 — a column-presence artifact (the flat file backfills `''`, not absence). The nullif-based `inspect_field_population.sql` Q1 is silver-accurate: `rpno` **94.5%** empty (→ §4 drop confirmed), `fmvss` 74.3%, `mfr_comp_ptno` 48.4% / `mfr_comp_desc` 48.4% / `mfr_comp_name` 47.3% (the 2020-drift component fields), `mfgcampno` 39.2%, `notes` 8.5%, `conequence_defect` 5.5%, `odate` 4.1%, `desc_defect` 2.6%, `corrective_action` 2.5%. NHTSA joins FDA + USDA in the must-nullif club.

**Bug 1 — `rcltype` enum at corpus scale (6 values):** V 87.3% (280,679) / T 6.9% (22,282) / E 5.3% (16,978) / C 0.3% (1,117) / **I 0.1% (393)** / **X 0.04% (143)**. The audit's V/E/T/C mapping is right; `I`/`X` are rare + undocumented → `accepted_values` warn. `yeartxt = 9999` sentinel 9.5% (→ NULL).

**Bug 2 — filer/manufacturer split strongly validated:** `mfgname` (filer) ≠ `mfgtxt` (manufacturer) on **38.0% of rows exact / 30.3% normalized**; of the differing rows **95.9% are completely disjoint** strings (not casing) — genuine supplier/equipment-filed recalls (campaign 09E012000: filer `SABERSPORT` affecting **19 distinct vehicle manufacturers**; Honeywell affecting GM). `mfgname` is constant per campaign (0 campaigns vary); `mfgtxt` varies up to 19/campaign. NHTSA contributes **3,940** distinct firms under the two-role split (3,569 filers + 2,836 manufacturers, 2,465 in both). The two-row (filer + manufacturer) emission is confirmed correct.

**§4 lift sizing:** `desc_defect` (→ recall_reason) avg 392 / max 1,982 chars, 97.4% populated; `corrective_action` avg 278 / max 1,678; `conequence_defect` avg 130 / max 759 (typo-fix → `consequence_of_defect`); `notes` avg 195 / max 1,875. `influenced_by`: MFR 82.8% / ODI 14.4% / OVSC 2.9% / ISSUE_INVGSTN (2 rows) → warn. `do_not_drive` 0.62% true / `park_outside` 0.37% true (clean booleans). `number_of_units ← potaff` is a **clean integer** (0% empty, 0–32M) — no Tier-0 parse, unlike FDA/USDA free-text quantity.

## References

- `src/extractors/nhtsa.py` — incremental extractor (`NhtsaExtractor`) + historical loader (`NhtsaDeepRescanLoader`). 11-tuple identity config at `load_bronze()`
- `src/extractors/_flat_file.py` — flat-file base class
- `src/schemas/nhtsa.py` — Pydantic bronze contract (29 fields, BeforeValidators for date/bool coercion, drift-aware nullables)
- `config/sources/nhtsa.yaml` — source registry: POST_2010 as `file_url`, PRE_2010 as `historical_seed_urls[0]`
- `dbt/models/staging/stg_nhtsa_recalls.sql` — latest-per-11-tuple projection
- `dbt/models/silver/recall_event.sql:126-163` — NHTSA → recall_event mapping
- `dbt/models/silver/recall_product.sql:104-137` — NHTSA → recall_product mapping (where Bug 1 + Bug 3 live)
- `dbt/models/silver/firm.sql:88-97` — NHTSA → firm mapping (where Bug 2 lives)
- `dbt/models/silver/recall_event_firm.sql:75-83` — NHTSA → recall_event_firm mapping (same)
- `documentation/decisions/0030-nhtsa-bronze-identity-composite-tuple-and-within-batch-dedup.md` — 11-tuple bronze identity (load-bearing)
- `documentation/decisions/0031-silver-row-fragmentation-strategy.md` — silver fragmentation framework + Tier 2 detection
- `documentation/decisions/0033-silver-row-versioning-via-scd-on-stable-anchor.md` — silver v1.5 SCD migration (Layer 1 docs only as of 2026-05-15)
- `documentation/nhtsa/RCL.txt` — canonical field reference
- `documentation/nhtsa/flat_file_observations.md` — Findings A-L (Phase 5c first-extraction empirical record)
- `documentation/nhtsa/incremental_delta_findings.md` — Sections A-M (Pierce + Nissan events, daily-delta samples)
- `documentation/nhtsa/watermark_probes.jsonl` — daily HTTP probe archive
- `documentation/nhtsa/nhtsa_datasets_and_apis.pdf` — broader NHTSA datasets context (§8 SKIP reference)
- `tests/fixtures/cassettes/nhtsa/README.md` — cassette philosophy (real headers + hand-built body)
