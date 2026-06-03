# Bronze corpus profile — cross-source shape evidence

- **Status:** Active (append-only) — started 2026-06-02. **All eight bronze tables profiled — W1 corpus profiling COMPLETE** (FDA + CPSC + USDA recalls + USDA establishments + NHTSA + USCG recalls + USCG manufacturers + USCG mfr details).
- **Scope:** the empirical *shape* of the full-corpus bronze — per-column population, cardinality, enum domains, length, grain/business-key, and identity-fragmentation — across all eight bronze tables. This is the evidence base the silver remap (`project_scope/silver-field-remap-plan.md`) builds its NOT NULL / `accepted_values` / length-sizing / SCD-grain decisions on.
- **Single-home boundary:** this doc owns the **cross-source shape matrices + the `accepted_values` value-set catalogue**. It does **not** own the semantic field→canonical-column *mapping* (that is `cross_source_consolidation.md`) nor the per-source narrative findings (those stay in each `documentation/<source>/field_audit_2026_w22.md`; this doc points at them for per-value counts rather than restating).
- **Methodology:** `documentation/audit/methodology.md`. Query files: `scripts/sql/<source>/<layer>/*.sql`.

> **CAVEAT (load-bearing — see `methodology.md` line ~29).** This is population / shape / enum / fragmentation **evidence for silver build decisions**. It is **not** a re-derivation of which fields to capture — that field-selection audit is Phase 6a, done (the eight `field_audit_2026_w22.md` docs). A field that is **empty in bronze describes the capture state, never a recommendation to stop capturing it.**

---

## 1. Corpus snapshot

| Source | Bronze table | Rows | Distinct business key | Seed provenance | Profiled |
|---|---|---|---|---|---|
| FDA | `fda_recalls_bronze` | 134,461 | ~134,450 products / 50,509 events | Phase 6a.5 full-corpus seed (re-seed 2026-06-02) | 2026-06-02 |
| CPSC | `cpsc_recalls_bronze` | 9,828 | 9,828 recalls / 11,836 products | full-corpus deep-rescan seed (2026-05-31) | 2026-06-02 |
| USDA recalls | `usda_fsis_recalls_bronze` | 2,005 | 1,216 English (silver grain) + 789 Spanish | full snapshot each fetch (re-seed 2026-05-31) | 2026-06-02 |
| USDA establishments | `usda_fsis_establishments_bronze` | 7,979 | 7,979 establishments | full MPI snapshot (2026-05-31) | 2026-06-02 |
| NHTSA | `nhtsa_recalls_bronze` | 321,592 | 30,045 campaigns / 321,425 records | full FLAT_RCL seed + incremental (2026-05-31 → 06-02) | 2026-06-02 |
| USCG recalls | `uscg_recalls_bronze` | 1,763 | 1,763 recalls | full corpus historical seed (2026-05-31) | 2026-06-02 |
| USCG manufacturers | `uscg_manufacturers_bronze` | 16,263 | 16,263 MICs | directory listing seed (re-seeded, 0 edit-versions) | 2026-06-02 |
| USCG mfr details | `uscg_manufacturer_details_bronze` | 16,263 | 16,263 MICs | Path B detail seed (1:1 listing coverage) | 2026-06-02 |

## 2. Grain & business-key matrix

| Source | Declared grain | Business key | Rows | Distinct key | Edit-versions (rows − distinct) | Notes |
|---|---|---|---|---|---|---|
| FDA | one row per **product** | `source_recall_id` (PRODUCTID) | 134,461 | ~134,450 | ~11 | aggregates to 50,509 events (`recall_event` DISTINCT ON event_id); max fan-out 470 products/event (event 70452). Near-1:1 product:row in the fresh seed — see §5 caveat. |
| CPSC | one row per **recall** | `recall_id` (= `source_recall_id`) | 9,828 | 9,828 | 0 (single-shot seed) | 1:1 recall:row, 0 edit-versions in this seed. Also 1:N to **products**: 11,836 product elements, 91.7% single, **max 57** — multi-product now real (falsifies the C2 *always-length-1* vacuity; ordinal product surrogate key is load-bearing). |
| USDA recalls | one row per **(recall, langcode)** | `(source_recall_id, langcode)` | 2,005 | 2,005 | 0 (single-shot seed) | Silver is English-only latest-per-id → **1,216** recall_events. `recall_product` is **1 free-text stub/recall** (`recall_product_id = recall_event_id`, audit Bug 3); `product_items` 40.5% empty. |
| USDA establishments | one row per **establishment** | `establishment_number` (`establishment_id` = `source_recall_id`) | 7,979 | 7,979 | 0 (single MPI snapshot) | `establishment_number` **100% populated + 100% unique** — the canonical firm-join key (Option A). `establishment_name` only 86.1% unique (1,110 shared). |
| NHTSA | one row per **campaign × affected product** | 11-tuple natural key → `record_id` | 321,592 | 321,425 | **167 (measured, cross-run)** | 30,045 campaigns, ~10.7 rows/campaign, max fan-out **19,321** (24T014000 tire). The 11-tuple identity is MEASURED STABLE (assert: natural-key core 0 drift; only `mfr_comp_ptno` drifts, 7 groups = supplier supersession, silver-correct). |
| USCG recalls | one row per **recall** | `source_recall_id` | 1,763 | 1,763 | 0 (single-shot seed) | Firm anchor is `mic` (6.8% null) + `company_name` (1.9% null); `mic`↔company 83% 1:1. |
| USCG manufacturers | one row per **MIC** | `mic` (= `source_recall_id`) | 16,263 | 16,263 | 0 (re-seeded snapshot) | `mic` is a **temporal-SCD anchor** (reassignment measured §M: AXY/COP) — but the re-seed **wiped the reassignment edit-versions**; the recycle signal is now static in the detail lineage, not in edit-versions. |
| USCG mfr details | one row per **MIC** | `mic` (= `source_recall_id`) | 16,263 | 16,263 | 0 (re-seeded snapshot) | Path B detail seed; **1:1 listing coverage, 0 orphans**. Carries the succession lineage (`past_company_*`, `out_of_business`, `date_modified`) that drives the eventual SCD-2 firm dim. |

## 3. Population matrix — silver-relevant columns (corpus empty/NULL %)

`''` is counted as empty (FDA preserves both `null` and `''`; silver staging normalizes via `nullif`). Counts live in the per-source audit; this is the at-a-glance NOT-NULL driver.

### FDA → `recall_event`
| Silver column | FDA bronze source | Empty % | Silver nullability |
|---|---|---|---|
| `recall_reason` | `product_short_reason_txt` | 0.1% | near-NOT NULL (warn-tripwire) |
| `distribution_area_summary` | `distribution_area_summary_txt` | 0.1% | nullable |
| `classification` | `center_classification_type_txt` | 0.0% | NOT NULL |
| `status` | `phase_txt` | 0.0% | NOT NULL |
| `recall_initiator` | `voluntary_type_txt` (normalized) | ~0% (9 rows) | nullable |
| `notification_method` | `initial_firm_notification_txt` | 25.8% | nullable |
| `announced_at` | `recall_initiation_dt` | 0.0% | NOT NULL |
| `published_at` | `event_lmd` | ~0.15% (197) | nullable (archive tail; see §5) |
| `terminated_at` | `termination_dt` | 17.7% | nullable |

### FDA → `recall_product` / firm
| Silver column | FDA bronze source | Empty % | Silver nullability |
|---|---|---|---|
| `product_name` | `product_description_txt` | 0.0% | NOT NULL |
| `product_description` | `product_description_txt` | 0.0% | NOT NULL |
| `number_of_units` | `product_distributed_quantity` | 8.1% | nullable, **TEXT** (free-text) |
| firm name | `firm_legal_nam` | 0.0% | NOT NULL (avg len 25, max 102) |
| firm identifier | `firm_fei_num` | 0.1% | nullable |

### CPSC → `recall_event`
JSONB arrays count `null`/`[]` as empty. A near-uniform **~18.3% empty floor** runs across remedies / injuries / images / retailers / consumer_contact — an archival **skeleton cohort** (led by the 1,597-record 2014-05-23 migration spike) carrying only core scalars, not the collections. That floor — not 0% — is the realistic NULL rate for every CPSC collection lift.
| Silver column | CPSC bronze source | Empty % | Silver nullability |
|---|---|---|---|
| `recall_reason` | `description` | 0.0% | NOT NULL |
| `consumer_contact` | `consumer_contact` | 18.5% | nullable (lift; skeleton floor) |
| `sales_channel_narrative` | `retailers[]` (Option B) | 18.5% | nullable (lift) |
| `remedies` | `remedies[]` | 18.3% | nullable (lift) |
| `injuries` | `injuries[]` | 18.3% | nullable (lift) |
| `images` | `images[]` | 18.2% | nullable (lift) |
| `manufacturer_countries` | `manufacturer_countries[]` | 17.5% | nullable (lift) |
| `remedy_options` | `remedy_options[]` | 51.5% | nullable (lift) |
| `coordinated_recall_urls` | `in_conjunctions[]` | 87.0% | nullable (lift) |
| `product_upcs` | `product_upcs[]` | 95.4% | nullable (lift; sparse) |
| `announced_at` | `recall_date` | 0.0% | NOT NULL |
| `published_at` | `last_publish_date` | 0.0% | NOT NULL |

### CPSC → `recall_product`
| Silver column | CPSC bronze source | Empty % | Silver nullability |
|---|---|---|---|
| `product_name` | `products[].name` | 3.3% | **warn-tripwire** (sample said 0%; corpus 3.3% empty) |
| `product_description` | `products[].description` | 100% | documented-empty-by-source |
| `model` | `products[].model` | 100% | documented-empty-by-source |
| `type` | `products[].type` | 40.4% | nullable |
| `category_id` | `products[].category_id` | 40.4% | nullable |
| `number_of_units` | `products[].number_of_units` | 32.2% | nullable, **TEXT** (sample said 0.2%; corpus 32.2% empty) |

### USDA recalls → `recall_event` / `recall_product`
USDA preserves `''` as the missing sentinel (ADR 0027); silver staging is **English-only, latest-per-id, nullif**. **`explore_usda_bronze.sql` Q19 counts SQL NULL only and reads 0% for the `''`-sentinel fields — misleading; the nullif figures below are silver-accurate** (side-by-side in `field_audit_2026_w22.md` §9).
| Silver column | USDA bronze source | Empty % | Silver nullability |
|---|---|---|---|
| `description` | `summary` (HTML narrative) | 0.0% | NOT NULL (avg 3,603 / max 16,452 chars) |
| `recall_reason` | `recall_reason` (enum, exploded) | 1.2% | near-NOT NULL (warn) |
| `classification` | `recall_classification` | 0.0% | NOT NULL |
| `lifecycle_status` | `recall_type` | 0.0% | NOT NULL |
| `risk_level` | **derived from `classification`** (1:1, Q2) | — | derived, not lifted |
| `distribution_states` | `states` | 28.4% | nullable |
| `related_to_outbreak` | `related_to_outbreak` | 25.0% | nullable bool |
| `archived` | `archive_recall` | 0.0% | NOT NULL bool |
| `closed_at` | `closed_date` | 8.6% | nullable |
| `firm_contact_block_text` | `company_media_contact` | 44.7% | nullable (lift) |
| `url` | `recall_url` | 0.0% | NOT NULL |
| `product_description` | `product_items` | 40.5% | nullable (1 free-text stub/recall) |
| `type` | `processing` (exploded) | 1.2% | nullable |
| `number_of_units` | `qty_recovered` | 14.1% | nullable, **TEXT** |
| `label_artifact_name` | `labels` | 11.1%* | nullable (filename) |
| `distribution_list_artifact_name` | `distro_list` | 79.7% | nullable (filename, avg 42 chars) |

*`labels` 11.1% per the 2026-05-28 R2 Python inspector — not re-measured via nullif this wave.

### USDA establishments → `firm_establishment_attributes`
| Silver column | USDA bronze source | Empty % | Silver nullability |
|---|---|---|---|
| `establishment_number` (PK / `firm.company_id`) | `establishment_number` | 0.0% (**100% unique**) | NOT NULL |
| `status` | `status_regulated_est` | `''` active 90.0% / Inactive 10.0% | NOT NULL (incl `''`) |
| `size` | `size` | 2.6% `''` | nullable |
| `dbas` (JSONB, placeholder-filtered) | `dbas` | 67.6% empty `[]` | nullable |
| `duns_number` | `duns_number` | ~85% `''` (R2) | nullable |
| address / city / state / zip / `latest_mpi_active_date` / `grant_date` | (same) | 0.0% | NOT NULL |
| `county` / `geolocation` | (same) | `'false'` text sentinel: 122 / 94 records | nullable |

### NHTSA → `recall_event` / `recall_product` / firm
NHTSA's FlatFileExtractor stores `''` for fields the flat file leaves blank (drift-added columns are backfilled empty on old records), so the legacy `explore_bronze_shape.sql` Q9 reads **100% "populated" by column-presence — misleading**; the nullif-based rates below (`inspect_field_population.sql` Q1) are silver-accurate. Joins the FDA + USDA `''`-sentinel club.
| Silver column | NHTSA bronze source | Empty % | Silver nullability |
|---|---|---|---|
| `recall_reason` | `desc_defect` | 2.6% | near-NOT NULL (warn; avg 392 / max 1,982 chars) |
| `corrective_action` | `corrective_action` | 2.5% | nullable (lift) |
| `consequence_of_defect` | `conequence_defect` (typo-fixed) | 5.5% | nullable (lift) |
| `notes` | `notes` | 8.5% | nullable (lift) |
| (recall-initiator) | `influenced_by` | 0.0% | NOT NULL (enum) |
| `mfgcampno` | `mfgcampno` | 39.2% | nullable (lift) |
| `fmvss` | `fmvss` | 74.3% | nullable (lift; 3-digit code, 75 distinct) |
| `do_not_drive` | `do_not_drive` | 0.0% (0.62% true) | NOT NULL bool |
| `park_outside` | `park_outside` | 0.0% (0.37% true) | NOT NULL bool |
| `announced_at` | `rcdate` | 0.0% | NOT NULL |
| (owner-notified) | `odate` | 4.1% + 7 `1901-01-01` sentinel | nullable |
| ~~`rpno`~~ | `rpno` | 94.5% | **drop (§4 documented-empty)** |
| `type` | `rcltype` (Bug 1) | 0.0% | NOT NULL (enum) |
| `product_name` | `compname` | 0.0% | NOT NULL |
| `model` | `modeltxt` | 0.0% | nullable |
| `model_year` | `yeartxt` | 0.0% (9999 sentinel 9.5% → NULL) | nullable |
| `number_of_units` | `potaff` | 0.0% | NOT NULL, **clean INTEGER** (0–32M; unlike FDA/USDA free-text) |
| firm (filer) | `mfgname` | 0.0% | NOT NULL |
| firm (manufacturer) | `mfgtxt` | 0.0% | NOT NULL |

### USCG recalls → `recall_event` / `recall_product` / firm
Missing scalars are genuine SQL NULL (**not** `''` — USCG is the HTML-scraper exception, like CPSC; `explore` Q5 `IS NULL` matches the nullif counts). Traps are NAMED sentinels: `'N/A'` (hin), `'9999'` (model_year), `'1970-01-01'` (opened_on), `0`≡`00` (boat_type), disposition/severity case-folding. **Narrative cap: `problem_1`/`problem_2` are source-capped at 25 chars** (snippets, not full defect text — landing pages link out).
| Silver column | USCG bronze source | Empty % | Silver nullability |
|---|---|---|---|
| `recall_reason` | `coalesce(problem_1, problem_2)` | ~6% (both null) | nullable (**25-char cap**) |
| `severity` | `severity` (→ `upper`) | 23.1% | nullable; {H,L,M,S} |
| `disposition` | `disposition` (→ `lower`) | 0.0% | NOT NULL; {open,closed} |
| `terminated_at` | `case_close_date` | 36.1% | nullable |
| `campaign_started_at` | `campaign_open_date` | 4.0% | nullable |
| `campaign_ended_at` | `campaign_close_date` | 19.1% | nullable |
| `last_editorial_date` | `last_date` | 6.1% | nullable |
| `type` | `boat_type` (numeric code, lookup gap) | 35.7% | nullable |
| `hin` | `hin` (`'N/A'`→NULL) | 47.2% | nullable (UPC analog) |
| `model_year` | `model_year` (`9999`→NULL) | 32.4% | nullable |
| `number_of_units` | `units` (→ integer) | 1.3% | nullable INTEGER |
| `product_name` | `model_name` | 6.8% | nullable |
| `model` | — (Bug 1: **NULL**) | — | n/a |
| firm raw_name | `company_name` (Bug 3) | 1.9% | NOT NULL |
| firm company_id | `mic` | 6.8% | nullable |

### USCG manufacturers / details → `firm_manufacturer_attributes` (per-MIC dim)
Both tables re-seeded to single snapshots (16,263 each, **0 edit-versions** — the §M reassignments are not in live bronze; the recycle signal lives statically in the detail lineage). Detail seed is complete (1:1 listing coverage, 0 orphans). Fill % below uses nullif(`''`) only — named sentinels slightly inflate.
| Silver column | USCG bronze source | Fill % | Notes |
|---|---|---|---|
| `mic` (PK / `firm.company_id`) | listing `source_recall_id` | 100% | natural key; 99.40% `^[A-Z]{3}$`, 0.03% lowercase drift |
| `company_name` | listing `company_name` | 99.98% | 0.02% missing (UNK/-/'') |
| `status` | detail `status` | 32.4% | {In Business 29.5%, Inactive 2.5%, Fed/State 0.4%}; **`''` 67.6% ≈ the OOB/defunct set** |
| `out_of_business` | detail `out_of_business` | 67.6% | **directory is mostly defunct firms** |
| `in_business` | detail `in_business` | 95.1% | contaminated by record-touch dates (§M.6) — not a founding date |
| `dba` | detail `dba` | 30.9% | alternate-name signal |
| `past_company_1/2/3` | detail | 20.6% / 9.7% / 10.3% | the MIC-recycle signal (SCD-2 input) |
| `date_modified` | detail `date_modified` | 96.0% | Path B change oracle; **⚠ future-date outlier (2031-02-19)** |
| `address` | listing (~29-char cap) / detail (full) | 99.76% | listing truncates at 29 (274 rows clipped); full address detail-only |

## 4. Enum-domain catalogue — the `accepted_values` SSOT

Value **sets** for the dbt `accepted_values` tests (cross-source union per canonical column once consolidation runs). Per-value **counts** live in each source's `field_audit_2026_w22.md` §8.

| Canonical silver column | Source | Value set | Test posture |
|---|---|---|---|
| `recall_event.classification` | FDA | `1`, `2`, `3`, `NC` | error |
| `recall_event.status` | FDA | `Terminated`, `Ongoing`, `Completed` | error |
| `recall_event.recall_initiator` | FDA (post-normalize) | `Firm Initiated`, `FDA Requested`, `FDA Mandated` | warn |
| `recall_event.notification_method` | FDA | `Letter`, `Combination`, `Telephone`, `E-Mail`, `Press Release`, `FAX`, `Other`, `Visit` | **warn** (corpus surfaced FAX + Visit beyond the audit's 6-value assumption) |
| `recall_product.type` | FDA (`product_type_short`) | `Food`, `Devices`, `Drugs`, `Biologics`, `Veterinary`, `Cosmetics`, `Tobacco`, `Food and Cosmetics` | warn |
| `recall_event.remedy_options[]` | CPSC | `Refund`, `Repair`, `Replace`, `New Instructions`, `Dispose`, `Label`, `No Remedy Available`, `Inspect` | **warn** (corpus surfaced 4 values beyond the sample's 4; plus an `R` typo + a remedy-narrative-in-the-Option-slot data-entry outlier) |
| `recall_product.type` | CPSC (`products[].type`) | open category set, 40.4% empty | — (no test) |
| *(manufacturer-country)* | CPSC `manufacturer_countries[]` | China 52.6% / United States 17.9% / Taiwan 6.0% / Mexico 2.8% / … 30+ | warn — `United Stateso` typo persists (silver normalize) |
| `recall_event.classification` | USDA | `Class I`, `Class II`, `Class III`, `Public Health Alert` | error (4 values, stable) |
| `recall_event.lifecycle_status` | USDA (`recall_type`) | `Closed Recall`, `Public Health Alert`, `Active Recall` | warn (PDF said 2; corpus 3) |
| `recall_event.recall_reason` | USDA (`recall_reason`, **exploded**) | 9 tokens: `Product Contamination`, `Misbranding`, `Unreported Allergens`, `Produced Without Benefit of Inspection`, `Import Violation`, `Processing Defect`, `Mislabeling`, `Unfit for Human Consumption`, `Insanitary Conditions` | warn — **test exploded tokens** (30.3% comma-multivalued) |
| `recall_product.type` | USDA (`processing`, **exploded**) | 10 tokens: `Fully Cooked - Not Shelf Stable` … `Heat Treated - Shelf Stable`, `Unknown`, `Thermally Processed - Commercially Sterile`, `Not Heat Treated - Shelf Stable`, `Eggs/Egg Products` | warn — **test exploded tokens** (2.0% comma-multivalued) |
| `firm_establishment_attributes.size` | USDA | `Very Small`, `Small`, `Large`, `N / A`, `''` | warn — `N / A` (10.1%, 808 rows) undocumented in PDF |
| `firm_establishment_attributes.status` | USDA (`status_regulated_est`) | `''` (active, 90.0%), `Inactive` (10.0%) | accepted_values incl `''` |
| `recall_product.type` | NHTSA (`rcltype`) | `V` (87.3%), `T` (6.9%), `E` (5.3%), `C` (0.3%), `I` (0.1%), `X` (0.04%) | warn (`I`/`X` rare, undocumented) |
| `recall_event.influenced_by` | NHTSA | `MFR` (82.8%), `ODI` (14.4%), `OVSC` (2.9%), `ISSUE_INVGSTN` (2 rows) | warn |
| *(`recall_event.risk_level` — NOT an enum column: derived 1:1 from `classification`, Q2 proof)* | USDA | — | — |
| `recall_event.severity` | USCG | `H` (38.2%), `L` (34.9%), `M` (1.4%), `S` (0.1%) | warn — 37 lowercase (`upper()`) + `1` outlier; 23.1% NULL |
| `recall_event.disposition` | USCG | `open`, `closed` (post-`lower()`) | error (4 raw case-forms — `Closed`/`Open`/`CLOSED`/`OPEN` — collapse) |
| `recall_product.type` | USCG (`boat_type`) | 25 numeric codes (`11`/`12`/`13`/`17`/… ; `0`≡`00`) | — no test (lookup-table gap; semantics unknown, USCG OII ask) |
| `firm_manufacturer_attributes.status` | USCG (detail) | `In Business`, `Inactive`, `Federal or State Agency`, `''` | warn (`''` = 67.6%, the defunct/OOB set) |
| *(cross-source severity/classification alignment — FDA 1/2/3/NC vs USDA Class I/II/III vs USCG severity H/L/M/S)* | — | pending consolidation (W2) | — |

**Methodology note proven here:** the 447-record sample reported `voluntary_type_txt` = 2 values and `initial_firm_notification_txt` = 6 values; the full corpus surfaced **`FDA Requested`/`FDA Mandated`** and **`FAX`/`Visit`** respectively. CPSC repeats the lesson: the 2026-05-29 sample reported `RemedyOption` = 4 values; the full corpus surfaced **8** (added `New Instructions` / `Label` / `No Remedy Available` / `Inspect`). Hardcoding `accepted_values` from the sample would have produced false-failing tests — the catalogue must come from corpus profiling. **USDA adds a second discipline — exploded tokens, not raw values:** `processing` and `recall_reason` are comma-joined multi-value, so the raw distinct sets (20 / 26) are combination-inflated; splitting on `,` and trimming recovers the documented base taxonomies (**10 / 9**, matching the PDF). The `accepted_values` test must run on the exploded tokens — testing raw `recall_reason` would false-fail its 30.3% multivalued rows.

## 5. Identity-fragmentation summary (SCD-applicability input — W3)

> This table is the per-**source** edit-version shape. The per-**field** SCD-type designations (e.g. `classification`/`severity` is a **Type-2-BENEFIT** field that silver currently Type-1-drops into bronze-only) + the monitors that validate them live in the living catalogue **`scd_field_designations.md`**. Note `classification` amendments are *suspected but unmeasured* (the re-seeds wiped versions) — `cross_source/scd_monitors/assert_classification_stable.sql` measures the rate as incrementals re-bank history.

| Source | Edit-versions observed | NEED (fragmentation) | BENEFIT (attribute history) | Status |
|---|---|---|---|---|
| FDA | ~11 over 134,450 products in the fresh seed → near-1:1 | **low signal** | TBD (phase/classification edits over time) | hypothesis — **not yet measured for long-term edit-rate** |
| CPSC | 0 in the single-shot 2026-05-31 seed (1:1 recall:row) | **low** at the recall key; **high in the firm dim** (retailer narrative + M/I/D suffix fragmentation) | TBD (description/remedy edits over time) | hypothesis — snapshot only; one edit *was* observed pre-seed (recall `00015`, 2026-05-08) |
| USDA recalls | 0 in the single-shot 2026-05-31 seed (1:1 (recall,langcode):row) | **low** — 1:1 grain; the firm join is gated by 35% recall-side empty, not fragmentation | `status_regulated_est` flips + **105/789 bilingual pairs update EN/ES `last_modified` independently** | hypothesis — snapshot; flips need cross-snapshot history |
| USDA establishments | 0 (single MPI snapshot) | n/a — `establishment_number` 100% unique is the firm key | active↔Inactive flips (10.0% Inactive) are the BENEFIT signal | hypothesis — snapshot |
| NHTSA | **167 (MEASURED — the 2026-06-02 incremental gave real cross-run data)** | **low** — 11-tuple identity-core 0 drift (assert); only `mfr_comp_ptno` drifts (7 groups = supplier supersession, silver-correct) | SCD-2 already adopted (ADR 0033) | **MEASURED** — the one source with cross-run edit-versions in fresh bronze; cite ADR 0033, not a snapshot |
| USCG recalls | 0 in the single-shot seed (1:1 recall:row) | **low** at the recall key — the SCD axis is the firm anchor (`mic`), not the recall | n/a at the recall grain | hypothesis — snapshot |
| USCG manufacturers / details | **0 in the re-seeded snapshots** (the AXY/COP reassignment edit-versions were wiped) | **`mic` is a temporal-SCD anchor** — reassignment MEASURED (§M: AXY/COP detail-page lineage; §M.6 probe: 28.7% of recalled MICs recycled) | **high** — succession lineage (`past_company` 20.6%, `out_of_business` 67.6%, `date_modified` 96%) | **MEASURED** (§M / ADR 0035) — but the *live listing snapshot shows 0 edit-versions*; the recycle signal is **static in the detail lineage**, not in edit-versions. Only 10% of OOB entries carry a parseable year → time-aware join is flag-only. |

> **FDA caveat (observation vs inference):** a single-shot seed shows near-1:1 product:row *now*, but cannot reveal how often the same PRODUCTID is re-extracted with changed content over time. The SCD-NEED verdict must weigh `scripts/sql/fda/bronze/assert_productid_stable.sql` + the daily-incremental history, not this snapshot alone. Recorded as a hypothesis, per the distinguish-inference-from-observation discipline.

> **CPSC firm fragmentation (the firm-dim NEED, measured):** the recall *key* is stable, but CPSC's contribution to the cross-source firm dim is heavily fragmented. **Option B** (drop the `retailers[]` role) removes **7,947 firm rows = 44.2%** of CPSC's 17,974-name footprint, with **zero overlap** with the manufacturer/importer/distributor names (clean removal — `net_firms_removed` == `retailer_only_distinct_names`). **Bug 2** (deferred to 6b): **62.8%** of the 14,444 M/I/D names carry a strippable `, of <geo>`/`dba` suffix; the conservative comma-strip *simulation* collapses **5.7% (576)** within the current corpus (e.g. three `3M Company, of {St. Paul, Minnesota / Saint Paul, Minnesota / St. Paul, Minn.}` → one) and raises the recurring-firm share 16.2% → 18.1%. Full 6b normalization (entity-token + space-`dba` + parenthetical + cross-source fuzzy) collapses strictly more. `company_id` is 100% empty across all **22,463** firm-role elements (Bug 3) — CPSC adds nothing to `observed_company_ids`.

> **USDA recall→establishment firm join (measured):** the firm key (`establishment_number`) is 100% unique, but the *recall-side* join is gated. On bronze (HTML-encoded), the name match is **82.91% per distinct populated name / 55.0% per record**; staging HTML-entity-decode (`&#039;`→`'`, `&amp;`→`&`) lifts per-name to ~97% (audit), so silver effective per-record coverage is **~63%**, capped by the **35.0% of recalls with an empty `establishment`** (recall-side missingness, not match quality). The DBA fallback adds **0** (probe Q2 == Q3). `establishment_number` is **67.1% '+'-joined multi-grant composites** (`M46712+P46712`) — the deferred `product_items` embedded-number extraction (6b Signal 1) must split the composite to match a single embedded grant.

## 6. Cross-source relationship / cardinality notes

- FDA recall→product fan-out: 1→N, mean ~2.66, max 470 (event 70452).
- CPSC recall→product fan-out: 1→N, 11,836 products / 9,828 recalls (mean ~1.20), max **57**. The C2 *always-length-1* array-stability assumption (`array_stability_findings.md`) is **falsified** at corpus scale — the ordinal-based product surrogate key is now load-bearing and `assert_products_array_append_only.sql` guards real data.
- CPSC firm cross-role overlap: 734 names appear in >1 of manufacturer/importer/distributor; retailers share **0** names with M/I/D (why Option B's 7,947-row removal is exactly the retailer-only set).
- CPSC archival skeleton cohort: ~18.3% of records (led by the 1,597-record 2014-05-23 migration spike) carry only core scalars — the uniform empty floor seen across remedies/injuries/images/retailers/consumer_contact in §3.
- USDA recall→establishment join: 82.91% per distinct name / 55.0% per record on bronze; ~97% / ~63% post-staging HTML-decode (audit); DBA fallback adds 0; gated by 35.0% recall-side empty `establishment`.
- USDA `establishment_number` shape: 100% unique, **67.1% '+'-joined multi-grant** (M…+P…); prefixes M 81% / V 11% / P 4% / I 3% / G 1%, none outside M/P/I/G/V — the product_items embedded-number match (6b) must split the composite.
- USDA `''`/`'false'` sentinel (cross-cutting): the existing null-rate queries (`explore_usda_bronze.sql` Q19; establishments `explore_bronze_shape.sql` Q3) count SQL NULL only and read **0%** for `''`-sentinel fields (`establishment`/`states`/`distro_list`/`duns_number`/…); the nullif / `'false'`-aware figures in §3 are the silver-accurate emptiness.
- USDA cadence is low-volume: 1,216 English recalls over 2014→2026 (~50–160/yr), no pre-2014 archival tail (API starts 2014); the recent-window weekday-gap query is uninformative for a ~1–2/week source.
- NHTSA recall→product fan-out: 1→N, 321,592 rows / 30,045 campaigns (mean ~10.7), max **19,321** (24T014000 tire mega-recall). Recent mega-campaigns drive the row count (2021: 44,166 rows / 1,094 campaigns).
- NHTSA filer/manufacturer split (W4 Phase D, validated): `mfgname` (filer) ≠ `mfgtxt` (manufacturer) on ~30% of rows normalized (38% exact), **95.9% disjoint** when differing — supplier/equipment-filed recalls (SABERSPORT files affecting 19 vehicle makes; Honeywell files affecting GM). `mfgname` is constant per campaign (1 filer); `mfgtxt` varies (max 19/campaign). NHTSA contributes **3,940** distinct firms under the two-role split (2,465 appear in both roles).
- NHTSA `number_of_units ← potaff` is a **clean integer** (0% empty, 0–32M) — the only source without free-text quantity, so no Tier-0 parse (contrast FDA/USDA pounds-tails).
- NHTSA `''`-sentinel (cross-cutting, 3rd source): the FlatFileExtractor backfills drift-added columns as `''` on old records, so `explore_bronze_shape.sql` Q9 reads 100% by column-presence — the nullif-based `inspect_field_population.sql` Q1 is silver-accurate (`mfr_comp_name`/`desc`/`ptno` ~47–48% empty, not 0%). Same `''` club as FDA + USDA.
- **The `''`-club is {FDA, USDA, NHTSA}; {CPSC, USCG} use genuine SQL NULL** for missing scalars (measured: USCG `explore` Q5 `IS NULL` == the nullif counts). USCG's traps are *named* sentinels (`N/A`/`9999`/`1970-01-01`/`UNK`/`-`/`0`≡`00`), not bare `''` — already stripped in the inspect scripts. The discipline (nullif + named-sentinel strip before reading a rate) applies to all four non-CPSC sources.
- USCG recall→directory MIC coverage: **714/718 (99.44%)**; 4 orphans (`111`/`999`/`777`/`N/A`) are retired codes + sentinels. Firm-rollup quality: **83% of MICs → 1 company, 91% of companies → 1 MIC** (Phase 6b handles the ~10–17% suffix/reassignment variants).
- **USCG MIC directory is mostly defunct:** 67.6% of the 16,263 MICs are out-of-business (top-level OOB); `status` `''` (67.6%) ≈ the OOB set; only 29.5% `'In Business'`. An accumulating historical registry since the 1970s.
- USCG narratives are **25-char source-capped** (`problem_1`/`problem_2` max 25) — snippets, not full defect text; cross-source contrast: CPSC max 5,983, NHTSA `desc_defect` 1,982, USCG 25. FastAPI landing pages link out to the USCG details URL.

## 7. Free-text normalization scoping (feeds the Tier model + the enrichment backlog)

FDA, 2026-06-02 (`scripts/sql/fda/bronze/profile_freetext_normalization.sql`):

- **`product_distributed_quantity` → `recall_product.number_of_units`:** 66% cleanly numeric (9.4% pure integer + 56.6% integer+unit); 20.9% messy (weights `total pounds`, multi-figure `(globally);(US)`, cross-product totals — same shape as USDA `qty_recovered`); 2.4% sentinel; 8.1% empty; 56,967 distinct normalized forms. **Decision:** Tier-0 cleanup (sentinel→NULL + whitespace/CR collapse) in staging, keep **TEXT**. Tier-2 `quantity_value`+`quantity_unit` parse → deferred (`project_scope/freetext-enrichment-backlog.md`).
- **`distribution_area_summary_txt` → `distribution_area_summary` + derived `distribution_scope`:** 31.4% Nationwide + 15.9% Worldwide/all-states = 47% national/intl; 33% regional (17.4% single-region + 15.4% state-code lists); 20% narrative/mid; **negation negligible (4 rows)**; 9,800 distinct surface forms collapse into the Nationwide flag (57,070 rows); embedded `\r` confirmed. **Decision:** Tier-0 cleanup + **Tier-1 silver derive** `distribution_scope ∈ {Nationwide, International, Regional, Unspecified}` — **no negation guard** (data shows ~0 true negations; a guard misclassifies real-nationwide rows, e.g. "No product was distributed to government accounts. The product was distributed nationwide"). Tier-2 `distribution_states[]`/`distribution_countries[]` → deferred (enrichment backlog).

---

*Per-source detail: `documentation/<source>/field_audit_2026_w22.md`. Semantic mapping: `documentation/audit/cross_source_consolidation.md` (W2). SCD verdict: this doc §5 → ADR 0035.*
