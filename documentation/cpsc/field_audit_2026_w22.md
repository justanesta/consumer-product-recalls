# CPSC field audit — 2026 W22

- **Status:** In progress 2026-05-29
- **Scope:** CPSC SaferProducts Recall Retrieval Web Services — every documented field vs. what we capture, what silver does with it, and what's missing
- **Methodology:** `documentation/audit/methodology.md`
- **Companions:**
  - Capture-expansion backlog: `documentation/audit/capture_expansion_backlog.md`
  - Validation scripts: `scripts/cpsc/audit/inspect_landed_payloads.py` (top-level fields), `scripts/sql/cpsc/bronze/inspect_array_field_population.sql` (nested-array fields)
  - Source PDF: `documentation/cpsc/cpsc_recalls_retrieval_web_services_programmers_guide_v1_4.pdf` (Sept 2018, last reuploaded 2024-10-17)
  - Existing CPSC findings: `documentation/cpsc/first_extraction_findings.md`, `documentation/cpsc/last_publish_date_semantics.md`, `documentation/cpsc/array_stability_findings.md`

## 1. API field universe (per the CPSC Recall Retrieval Web Services Programmer's Guide v1.4 — September 2018)

The PDF documents one endpoint with case-insensitive substring (wildcard) search across any of the listed filter parameters. Response is XML by default; `?format=json` returns the same shape as JSON. There is no pagination — a single GET returns all matching records (per `src/extractors/cpsc.py:98` + `last_publish_date_semantics.md` cassette inventory).

### 1a. Single-valued fields per record

| Field | Definition (PDF p. 2) | Notable |
|---|---|---|
| `RecallID` | Numerical identifier (integer) | Stable across edits — used as `recall_id` in bronze |
| `RecallNumber` | CPSC-assigned recall number (string, e.g. `26425`) | Identity — `source_recall_id` |
| `RecallDate` | Original announcement date | Two formats observed: `YYYY-MM-DD` (older), `YYYY-MM-DDTHH:MM:SS` (newer) — both handled by `_parse_cpsc_date` |
| `Description` | Free-text description of the recall + product | **The actual recall narrative** — not a distribution-area summary like FDA's similarly-named field |
| `URL` | CPSC.gov detail URL | Always populated |
| `Title` | Recall title | Always populated |
| `ConsumerContact` | Free-text contact block (phone, email, web) | Landing-page critical |
| `LastPublishDate` | Last publish / re-process date | Used as the incremental cursor (`LastPublishDateStart` filter). Per `last_publish_date_semantics.md`, advances on (re-)publication; **does not advance on content edits** — deep-rescan is the safeguard |

### 1b. Collections (per PDF pp. 2-3 + C# class layout pp. 6-7)

| Collection | Element shape (PDF) | Notable |
|---|---|---|
| `Products` | `Product { Name, Description, Model, Type, CategoryID, NumberOfUnits }` | Per `first_extraction_findings.md` Section A — always exactly 1 element across the corpus (1,422-record confirmation 2026-05-29 SQL Q3). **Per-field populated rates (2026-05-29 SQL Q2):** `Name` 100%, `NumberOfUnits` 99.8%, **`Type` 58.8% populated** (cassette had falsely suggested empty), **`CategoryID` 58.8% populated** (same), `Description` **100% empty**, `Model` **100% empty**. Cassette-based assumption that all 4 of `Description/Model/Type/CategoryID` were empty was wrong on `Type`/`CategoryID`. Per `array_stability_findings.md` (2026-05-08) the C2/C3 append-only + name/model-stability assumptions are unfalsified but vacuously so at array length 1. |
| `Inconjunctions` | `Inconjunction { URL }` | **Spelled with lowercase 'c'** in the JSON key (a known gotcha documented in the schema; PDF p. 2 uses the same odd casing). URL to a coordinated cross-jurisdiction recall (e.g., Canadian Recalls and Safety Alerts). |
| `Images` | `Image { URL }` per the PDF; **API actually returns `Image { URL, Caption }`** (positive drift since 2018) | The PDF predates `Caption`. Bronze schema correctly captures both. **2026-05-29 SQL Q5: both URL and Caption 100% populated across 3,170 image elements.** Every image has alt-text-ready caption content — clean landing-page input with no NULL handling needed. |
| `Injuries` | `Injury { Name }` | Free-text narrative (e.g., `"None reported"` or a multi-sentence injury count). Single field per element. |
| `Manufacturers` | `Manufacturer { Name, CompanyID }` (mapped to C# `RecallFirm`) | Per cassette: `Name` often includes "of <country>" suffix (e.g., `"ZOLIQUEX, of China"`). `CompanyID` empirically always `""`. |
| `Retailers` | `Retailer { Name, CompanyID }` (mapped to C# `RecallFirm`) | **`Name` is a sales-channel narrative, not a firm name.** PDF's own example (p. 4): `"Babies R US and other retail stores nationwide and Albeebaby.com, Amazon.com, Dmartstores.com, Medbroad.com and other online retailers from May 2015 through November 2016 for about $180."` Includes outlets + window + price. `CompanyID` empirically always `""`. |
| `Importers` | `Importer { Name, CompanyID }` (mapped to C# `RecallFirm`) | Per cassette: `Name` often includes "of <city, state>" (e.g., `"Apex Gaming PCs Inc., of Houston, Texas"`). `CompanyID` empirically always `""`. |
| `Distributors` | `Distributor { Name, CompanyID }` (mapped to C# `RecallFirm`) | Per cassette: often "dba <name>, of <country>" (e.g., `"Cheyouhang Technology Shenzhen Co., Ltd., dba ZOLIQUEX, of China"`). `CompanyID` empirically always `""`. |
| `ManufacturerCountries` | `ManufacturerCountry { Country }` | Country-of-manufacture name (free text). Often single-element. **2026-05-29 SQL Q7 — 1,088 elements, 30+ distinct countries.** Top values: China 61.4% (668), United States 15.5% (169), Taiwan 4.3%, Canada 1.8%, Vietnam 1.7%, Thailand/India 1.3%, Japan/Italy 1.2%, Mexico/Hong Kong 1.0%. **Data quality finding:** `"United Stateso"` typo on 2 records. Worth a silver normalization step if FastAPI exposes country as a search facet. |
| `ProductUPCs` | `ProductUPC { UPC }` | Per `first_extraction_findings.md` Section F: documented as "always `[]` in JSON" based on the 1,191-row snapshot. **R2 validation 2026-05-29 refines this: 97.3% empty (1,331 of 1,368 records), 2.7% populated** with up to 20 UPCs per recall. Finding F was directionally right but not absolute — JSON does carry UPCs for ~3% of recalls. The RecallDelimited endpoint may still be denser (per Bruno UPC probe, RecallDelimited had UPCs for a record where JSON didn't). |
| `Hazards` | `Hazard { Name, HazardType, HazardTypeID }` | `Name` populated 99.8%; `HazardType` + `HazardTypeID` **always empty** per Finding G (also confirmed by Bruno `search_by_hazard.yml`). **Exact corpus confirmation 2026-05-29 SQL Q4: across 1,420 hazard elements — `name` 100% populated, `hazard_type` 100% empty (0 distinct values), `hazard_type_id` 100% empty (0 distinct values).** |
| `Remedies` | `Remedy { Name }` | Always populated. Free-text remedy narrative (refund/repair/replace instructions). |
| `RemedyOptions` | `RemedyOption { Option }` | Short categorical enum: `"Refund"`, `"Repair"`, `"Replace"`. Multi-valued when the recall offers multiple remedies. **2026-05-29 SQL Q6 enum distribution across 613 elements:** `Refund` 65.1% (399), `Repair` 17.5% (107), `Replace` 17.0% (104), `Dispose` 0.2% (1 — new value not seen in cassette), plus 2 data-quality outliers: `"R"` 0.2% (1 — likely typo for Refund or Repair) and 1 record where the `Option` field carries a full multi-line remedy narrative paragraph instead of a tag. Top 3 cover 99.5%. |

### 1c. Search parameters (PDF p. 1)

Case-insensitive wildcard/substring match on any combination:

`RecallID`, `RecallNumber`, `RecallDateStart`, `RecallDateEnd`, `LastPublishDateStart`, `LastPublishDateEnd`, `RecallURL`, `RecallTitle`, `ConsumerContact`, `RecallDescription`, `ProductName`, `InconjunctionURL`, `ImageURL`, `Injury`, `Manufacturer`, `Retailer`, `Importer`, `Distributor`, `ManufacturerCountry`, `UPC`, `Hazard`, `Remedy`, `RemedyOption`.

Plus the `format` non-field parameter (`XML` or `JSON`; default `XML`).

**Known broken params per Bruno + `first_extraction_findings.md`:**
- `Hazard=` targets `Hazards[].HazardType` (always empty) — returns 0 results for any non-empty query.
- `UPC=` targets the JSON `ProductUPCs[]` (always empty) — returns 0 even for UPCs that appear in RecallDelimited.

**Naming asymmetry:** several filter params have a `Recall*` prefix that the response field drops — `RecallTitle`→`Title`, `RecallDescription`→`Description`, `RecallURL`→`URL`. Filter-vs-field naming diverges; documented for FastAPI search-endpoint design.

### 1d. Additional endpoint — `RecallDelimited` (PDF p. 8)

Same root with `/RestWebServices/RecallDelimited`. Collections are pipe-delimited quote-escaped strings instead of XML/JSON arrays. **Same data, different format** — the JSON endpoint we use gives strictly richer structure. The Bruno `search_by_upc.yml` discovery that `RecallDelimited` returns UPCs that JSON doesn't is the one gap; weighing whether to add it is in §8.

### 1e. `SoldAtLabel` — positive drift since 2018 PDF

Not in the PDF field list, but consistently returned in the JSON response (always `null` per Finding F across the 1,193-row corpus). Bronze captures it (migration 0003) to prevent extraction rejection via the schema's `extra="forbid"` config. **Documented-empty-by-source** — not a capture gap.

## 2. Current bronze capture (`cpsc_recalls_bronze` — 21 fields)

Per `src/schemas/cpsc.py` + `src/extractors/cpsc.py:36-65`. Pydantic snake_case via `validation_alias` against the PascalCase API keys.

### 2a. Scalars (8)

| Bronze column | API field |
|---|---|
| `source_recall_id` | `RecallNumber` |
| `recall_id` | `RecallID` |
| `recall_date` | `RecallDate` |
| `last_publish_date` | `LastPublishDate` |
| `title` | `Title` |
| `description` | `Description` |
| `url` | `URL` |
| `consumer_contact` | `ConsumerContact` |
| `sold_at_label` | `SoldAtLabel` (positive drift — not in PDF) |

### 2b. Collections — 13 JSONB columns (pass-through, full element structure preserved)

| Bronze column | API field |
|---|---|
| `products` | `Products` |
| `manufacturers` | `Manufacturers` |
| `retailers` | `Retailers` |
| `importers` | `Importers` |
| `distributors` | `Distributors` |
| `manufacturer_countries` | `ManufacturerCountries` |
| `product_upcs` | `ProductUPCs` |
| `hazards` | `Hazards` |
| `remedies` | `Remedies` |
| `remedy_options` | `RemedyOptions` |
| `in_conjunctions` | `Inconjunctions` (lowercase 'c' alias) |
| `images` | `Images` (Caption captured despite PDF omission) |
| `injuries` | `Injuries` |

**Bronze captures everything the PDF documents + `SoldAtLabel` + `Image.Caption` (both positive drift). No capture gaps from the PDF surface. Staging (`stg_cpsc_recalls.sql`) is a latest-per-recall view with zero column-level transformation.**

## 3. Mismappings (silver — fix in the (a) PR)

**Unlike FDA, CPSC has no source-to-target column-flip bugs in `recall_event` or `recall_product` silver.** `description ← description`, `title ← title`, `url ← url`, all the `products[].*` projections are semantically correct. Cross-checking each silver column against the PDF definition produces a clean bill of health on the event and product tables.

The three findings below are **structural / informational** — they live in the firm models (`firm.sql`, `recall_event_firm.sql`) and they degrade firm-dimension quality without being a column-flip the way FDA's Bugs 1/2/3 were. Listed here so they aren't lost; resolution path is in §6.

### Bug 1 — `Retailers[].Name` is a sales-channel narrative, not a firm name (pollutes firm dim)

**Quantitative confirmation at corpus scale (2026-05-29):** `Retailers[].Name` is empirically 3× longer than the other 3 firm-role names and almost never repeats — confirming the data is narrative, not firm-name.

| Role | Elements | Avg length | Max length | Distinct names | % distinct |
|---|---|---|---|---|---|
| Manufacturers | 674 | 44 | 141 | 588 | 87.2% |
| Importers | 518 | 46 | 141 | 472 | 91.1% |
| Distributors | 430 | 49 | 127 | 405 | 94.2% |
| **Retailers** | **1,416** | **135** | **733** | **1,398** | **98.7%** |

A firm dim populated from a column where 98.7% of values are unique strings of ~135 chars is structurally wrong. Sample narratives from Q11 confirm: `"Home Interiors' direct sales associates exclusively sold the recalled tea lights, from September 2002 through November 2002, for about $5 per box."`, `"Online at dupray.com and in stores and online at Home Depot, Lowe's, Macy's, Walmart, Amazon.com and other retailers nationwide from April 2018 through December 2025 for about $150."`

`dbt/models/silver/firm.sql:36-39` and `recall_event_firm.sql:30-33` both treat each `Retailers[]` element as a firm with `role='retailer'`. But the **PDF's own example** on page 4 shows what `Retailer.Name` actually contains:

```
Babies R US and other retail stores nationwide and Albeebaby.com,
Amazon.com, Dmartstores.com, Medbroad.com and other online retailers
from May 2015 through November 2016 for about $180.
```

The cassette confirms the same shape across all sampled recalls — `Retailers[].Name` carries "where sold + when sold + price range" narrative, not a single retail-firm legal name. Current silver:

- `firm.normalized_name` becomes the entire narrative `UPPER(TRIM(...))`, so every unique sales-window narrative produces a distinct firm row.
- "Amazon" (probably the most-co-mentioned retailer in CPSC recalls) **never collapses** to a single firm — it appears as a substring of many different narratives.
- `recall_event_firm` carries thousands of single-use firm_ids on the retailer side.

**Fix path (proposed; see §6 + §7):** filter `Retailers[]` out of the `firm.sql` + `recall_event_firm.sql` exploded sets; promote the narrative content to a first-class `recall_event.sales_channel_narrative` JSONB or text[] column (one element per `Retailers[]` element).

### Bug 2 — `Manufacturers[]`, `Importers[]`, `Distributors[]` Names carry suffixes that fragment otherwise-identical firms

Per cassette samples:

| Role | Example `Name` value | Issue |
|---|---|---|
| Manufacturer | `"ZOLIQUEX, of China"` | Geography appended |
| Importer | `"Apex Gaming PCs Inc., of Houston, Texas"` | Geography appended |
| Importer | `"Aria Child Inc. of Dedham, Mass."` (PDF p. 4 example) | Geography appended |
| Distributor | `"Cheyouhang Technology Shenzhen Co., Ltd., dba ZOLIQUEX, of China"` | DBA + geography |
| Distributor | `"Jiangxi Runfuyuan Biotechnology Co., Ltd dba Agiiman, of China"` | DBA + geography |
| Distributor | `"Shenzhen Maikeer Industrial Co., Ltd., doing business as MalkerDirect, of China"` | **Full-form `"doing business as"`** (alongside abbreviated `"dba"`) — R2 finding 2026-05-29 |
| Importer | `"Lingqingxiangjiankangkejigufenyouxiangongsi (Lingqingxiang Health Technology Co., Ltd.) dba Hepo Care Medical Equipment Online Ltd., of China"` | DBA + parenthetical translation + geography — extreme fragmentation case |

`firm.sql` keys on `UPPER(TRIM(name))` — these strings each become distinct firms. A future recall of the same `ZOLIQUEX` brand by a Manufacturers[]={`"ZOLIQUEX"`} (without the country suffix) would not collapse with `"ZOLIQUEX, of China"`. Same for DBA-only variants.

**Quantitative confirmation (Q10 2026-05-29):** Manufacturer/Importer/Distributor name distinctness rates are 87-94% — firms barely collapse even within a single role. For comparison, a firm dim with well-normalized names would show distinctness much lower than the unique-count, because real-world firms repeat across recalls. The current 87-94% rate is consistent with suffix patterns + DBA variants fragmenting otherwise-identical firms.

**Fix path:** CPSC-specific name normalization before `firm.sql` does its `UPPER(TRIM())`. Strip `, of <location>` and `, (dba|doing business as) <name>` suffixes; extract DBAs into a candidate-names JSONB on the firm. This is **Phase 6b RapidFuzz-adjacent** work — listed here so it's not silently lost when Phase 6b plans cross-source firm rollup.

### Bug 3 (informational) — `CompanyID` is empirically dead across all 4 firm arrays

**Confirmed at exact corpus scale (2026-05-29 SQL Q1):** every `CompanyID` value is empty across all 4 roles — **0 distinct non-empty values across 3,038 firm-role elements**.

| Role | Elements | empty_companyid | pct_empty | distinct non-empty |
|---|---|---|---|---|
| Manufacturer | 674 | 674 | **100.00%** | 0 |
| Retailer | 1,416 | 1,416 | **100.00%** | 0 |
| Importer | 518 | 518 | **100.00%** | 0 |
| Distributor | 430 | 430 | **100.00%** | 0 |

**CPSC contributes no values to `firm.observed_company_ids`** — that JSONB column on CPSC-only firms will always be `null` (per the `filter (where company_id is not null)` clause). Cross-source firm-id rollup gets zero signal from CPSC.

This stands in contrast to:
- FDA `firm_fei_num` (FEI populated, usable as a stable identifier)
- USDA `establishment_number` (FSIS grant number, 100% unique and populated)
- USCG `mic` (Manufacturer Industry Code, populated for the firm anchor cases)

If we ever build a `firm_identifier` table anchoring cross-source rollup, **only FDA + USDA + USCG contribute**. CPSC adds nothing. Empirical R2 validation in §9 should re-confirm the always-empty observation against the broader corpus.

## 4. Underused captures — lift from JSONB / source_payload_raw to first-class silver columns

CPSC bundles **10 fields** into `recall_event.source_payload_raw` JSONB at `recall_event.sql:38-49`. For the landing-page + FastAPI surface the project is building (per `project_scope/implementation_plan.md`), most of these are core user-facing content and should be promoted to first-class silver columns.

| Bronze field | Current placement | Proposed silver column | Why lift |
|---|---|---|---|
| `remedies` (JSONB `[{Name}]`) | `recall_event.source_payload_raw.remedies` | `recall_event.remedies` (JSONB) | **Landing-page critical.** Free-text remedy narrative — what the consumer should do (refund process, repair instructions, replacement coordination). Multi-element when multiple remedies offered. |
| `remedy_options` (JSONB `[{Option}]`) | `recall_event.source_payload_raw.remedy_options` | `recall_event.remedy_options` (text[] or JSONB) | **Cross-source-alignable enum.** Short tags: `Refund`, `Repair`, `Replace`. Easily filterable/aggregatable; "show me all recalls offering refunds." |
| `injuries` (JSONB `[{Name}]`) | `recall_event.source_payload_raw.injuries` | `recall_event.injuries` (JSONB) | **Landing-page critical.** Per Finding F always populated (0% NULL); often `"None reported"` (categorical), occasionally a multi-sentence narrative with counts ("5 reports of stitches, 71 reports of unexpected folding, 12 minor bumps, 1 fractured wrist..."). Cross-source proxy for severity. |
| `images` (JSONB `[{URL, Caption}]`) | `recall_event.source_payload_raw.images` | `recall_event.images` (JSONB) | **Landing-page critical.** Product photos on cpsc.gov S3. Caption identifies which view (front, top, packaging). Per cassette commonly 1-5 elements per recall. |
| `consumer_contact` (text scalar) | `recall_event.source_payload_raw.consumer_contact` | `recall_event.consumer_contact` (text) | **Landing-page critical.** Free-text contact block: company name + phone + hours + email + website + recall-page-on-firm's-site. The "how do I get my refund" answer. |
| `manufacturer_countries` (JSONB `[{Country}]`) | `recall_event.source_payload_raw.manufacturer_countries` | `recall_event.manufacturer_countries` (text[] or JSONB) | **Searchable provenance.** "Show me all China-manufactured recalls." Per Finding F 28% NULL but valuable when present. Multi-valued when product manufactured across multiple countries. |
| `in_conjunctions` (JSONB `[{URL}]`) | `recall_event.source_payload_raw.in_conjunctions` | `recall_event.coordinated_recall_urls` (text[] or JSONB) | **Cross-jurisdiction signal.** Links to companion recalls (Canada `recalls-rappels.canada.ca`, sometimes EU). Per cassette typically 0-1 elements. Useful for cross-border landing pages. |
| `recall_id` (int scalar) | `recall_event.source_payload_raw.recall_id` | `recall_event.cpsc_recall_internal_id` (int) — *or drop* | Already promoted via `source_recall_id = RecallNumber`. `RecallID` (the integer) is CPSC's internal sequence; keep available for API joins to `cpsc.gov` slug if useful, otherwise drop from the JSONB. |
| `product_upcs` (JSONB `[{UPC}]`) | `recall_event.source_payload_raw.product_upcs` | `recall_event.product_upcs` (JSONB) *(revised post-R2 2026-05-29)* | **R2 validation refined the Finding F "always empty" claim.** Actual rate: 97.3% empty, 2.7% populated with 1-20 UPCs per recall. Sparse but a real signal — valuable for the FastAPI product-search surface ("find recalls by UPC"). Worth lifting despite the sparsity. |
| `sold_at_label` (text scalar) | `recall_event.source_payload_raw.sold_at_label` | **Drop (documented-empty-by-source)** | Per Finding F: 100% NULL across 1,193 records. Keep capture in bronze; drop from silver. |
| `hazards[].HazardType`, `[].HazardTypeID` | already in `recall_event.hazards` JSONB | Document as documented-empty | Per Finding G: 100% empty across 1,191 hazard-bearing records. Useful for `_silver.yml` so consumers don't try to query them. |
| `products[].CategoryID` | already in `recall_product.category_id` | Validate empirically | Per cassette: all `""` across sampled records. R2 validation (§9) should report the actual NULL rate. May be documented-empty-by-source too. |
| `products[].Type` | already in `recall_product.type` | Validate empirically | Per cassette: all `""`. Same as above. |

**Cross-source naming alignment notes:**

| Cross-source silver column | CPSC source | FDA source | USDA source |
|---|---|---|---|
| `recall_event.description` (defect/hazard narrative) | `description` | (Bug 1) `productshortreasontxt` | `summary` |
| `recall_event.classification` | (NULL — CPSC has none) | `centerclassificationtypetxt` | `recall_classification` |
| `recall_event.status` / `lifecycle_status` | (NULL — CPSC has none) | `phasetxt` | `recall_type` (per USDA §4) |
| `recall_event.hazards` (structured hazard JSONB) | `hazards` array | (NULL — FDA reason lives in product description) | (NULL — USDA reason in `recall_reason`) |
| `recall_event.related_to_outbreak` | (NULL — CPSC has none) | (NULL — FDA has none in bulk) | `related_to_outbreak` |
| `recall_event.distribution_states` | **embedded in `Retailers[]` narrative** (no structured field) | `distributionareasummarytxt` (per FDA §3 fix) | `states` |
| `recall_product.product_name` | `products[].Name` | (FDA Bug 2 — `productdescriptiontxt` is misnamed; pending `productdescriptionshort`) | `title` |
| `recall_product.product_description` | `products[].Description` (often empty) | (FDA Bug 3 fix) `productdescriptiontxt` | `product_items` |
| `recall_product.type` | `products[].Type` (often empty) | `producttypeshort` | (USDA Bug 1 fix) `processing` |
| `recall_product.number_of_units` | `products[].NumberOfUnits` (free text) | `productdistributedquantity` (free text) | `qty_recovered` (free text) |

The Retailer→distribution_states observation is worth highlighting: CPSC bundles geographic distribution into the same narrative-string that contains retailer names, prices, and date ranges. There is no standalone `distribution_states` analog. If we want a structured CPSC distribution-area field we'd need NER over the Retailer narrative — Phase 6/7 enrichment territory.

## 5. Field-naming gotchas

| Surface | Gotcha | Notes |
|---|---|---|
| **Bronze JSONB key casing** | API returns PascalCase (`Name`, `CompanyID`, `HazardType`); **bronze stores snake_case** (`name`, `company_id`, `hazard_type`) | `src/bronze/loader.py:329, 390` serializes via `model_dump(mode="json")` without `by_alias=True`, so Pydantic outputs field names (snake_case), not the `validation_alias` (PascalCase). The R2 landed payload is the raw API response (PascalCase); bronze JSONB is the Pydantic-validated form (snake_case). Production silver code already follows this — see `dbt/models/silver/recall_product.sql:32-37` (`prod.value ->> 'name'`) and `firm.sql:53-55` (`firm_json ->> 'company_id'`). **SQL drilling into bronze JSONB MUST use snake_case keys** — using PascalCase returns NULL silently and produces 100%-empty false positives. Diagnosed 2026-05-29 when the first run of `scripts/sql/cpsc/bronze/inspect_array_field_population.sql` returned 100% empty on every nested-key query. **Cross-script audit performed 2026-05-29:** swept all CPSC bronze SQL files for the same pattern. Two affected files fixed: (1) `scripts/sql/cpsc/bronze/inspect_array_field_population.sql` (8 queries: Q1, Q2, Q4, Q5, Q6, Q7, Q10, Q11); (2) `scripts/sql/cpsc/bronze/explore_bronze_shape.sql` Q9 (`hazards->0->>'HazardType'` → `hazards->0->>'hazard_type'`). The Q9 result had coincidentally aligned with Finding G's "always empty" conclusion so the bug had been silent. The two assert scripts (`assert_name_model_normalization_stable.sql`, `assert_products_array_append_only.sql`) + their dbt singular-test counterparts already used snake_case correctly. The rebaseline scripts don't drill into nested keys. CPSC-only bug (the only source with list-of-dict JSONB columns at bronze). |
| API key spelling | **`Inconjunctions`** (lowercase 'c') | Documented in `bruno/cpsc/incremental_extraction/get_recalls_by_last_publish_date.yml` and the bronze schema. Both PDF and JSON key use this spelling. |
| PDF drift | PDF p. 2 says `Image { URL }` — actual JSON returns `Image { URL, Caption }` | Caption was added since 2018; bronze schema captures both. |
| PDF drift | PDF field list has no `SoldAtLabel` — actual JSON always returns `"SoldAtLabel": null` | Capture added via migration 0003 to prevent `extra="forbid"` rejection. |
| PDF semantics | `productshortreasontxt` — wait, that's FDA | (Cross-reference: FDA's misleading "short" field is in `documentation/fda/field_audit_2026_w22.md` §5.) |
| Filter-vs-field naming | `RecallTitle`, `RecallDescription`, `RecallURL` filter params return `Title`, `Description`, `URL` fields | Asymmetric. Document for the FastAPI search-endpoint surface. |
| C# class reuse | PDF C# example (p. 6) maps `Manufacturers`, `Retailers`, `Importers`, `Distributors` all to one class: `List<RecallFirm>` | PDF's *intent* is "these 4 collections all hold firm-like records." Empirically (cassette + Bug 1) this is wrong for Retailers — the data shape is sales-channel narrative, not firm. |
| Always-empty by source | `Hazards[].HazardType`, `Hazards[].HazardTypeID` | 100% empty per Finding G + Bruno hazard probe. Don't use as filters. |
| Mostly-empty by source (JSON endpoint) | `ProductUPCs[]` | **97.3% empty per R2 validation 2026-05-29** (refines Finding F's "always empty" claim — was directionally right but not absolute). 2.7% of recalls carry 1-20 UPCs in JSON. The `RecallDelimited` endpoint may be denser per Bruno probe. |
| Always-empty by source | `SoldAtLabel` | 100% NULL across 1,193-row corpus. |
| `CompanyID` empirically dead | All 4 firm arrays' `CompanyID` field is `""` across cassette samples | Per Bug 3 — R2 validation should reconfirm against the broader corpus. |
| `RecallNumber` matching | Substring match (per Bruno `probe_recall_number_wildcard.yml` 2026-04-24) | "264" matches "26425" (prefix), "24264" (mid), "07264" (suffix). Not anchored. |
| Date format | Two formats coexist: `YYYY-MM-DD` (older records) and `YYYY-MM-DDTHH:MM:SS` (newer) — both timezone-naive | Handled by `_parse_cpsc_date` in `src/schemas/cpsc.py:88-101`. |
| `LastPublishDate` edit semantics | Does NOT advance on content edits per `last_publish_date_semantics.md` Finding 1 + `array_stability_findings.md` recall-`00015` observation | Deep-rescan is the mandatory safeguard. Not an audit fix; codified policy. |
| `LastPublishDateStart` boundary | Routine extraction window is wider than strict `> watermark` — opportunistically catches same-day re-publishes (per `array_stability_findings.md`) | Defensive behavior; documented. |

## 6. The firm-relationship question — CPSC's per-array firm structure (structural — biggest CPSC decision)

CPSC is the only source with **four parallel firm-role arrays** (Manufacturers, Retailers, Importers, Distributors) on every recall, plus a per-element `CompanyID` slot that was intended as a structured firm identifier but is empirically always empty.

Combined with §3's findings, the current `firm.sql` + `recall_event_firm.sql` design has three quality issues for CPSC-only firms:

1. **Retailer narratives pollute the firm dim** (§3 Bug 1). Each unique sales-window narrative becomes its own firm row.
2. **Manufacturer/Importer/Distributor names fragment** (§3 Bug 2). Geography + DBA suffixes split otherwise-identical firms.
3. **No identifier anchor** (§3 Bug 3). `CompanyID` always empty → `observed_company_ids` will always be NULL for CPSC-only firms → no cross-source identifier rollup possible.

### Three options for the CPSC firm architecture

| Option | Description | Pros | Cons |
|---|---|---|---|
| **A — Status quo** | Keep all four arrays feeding `firm.sql` + `recall_event_firm.sql` as-is. Accept retailer narratives and geography-suffixed names as a known firm-dim quality issue. Document in `_silver.yml`. | Smallest change. No new silver columns. Future RapidFuzz work in Phase 6b can address fragmentation. | Firm dim continues to carry thousands of single-use retailer-narrative rows. Cross-source rollup with FDA/USDA establishments breaks because CPSC manufacturer names with country suffixes never match the FDA legal-name normalization. The user's landing-page goal sees "Amazon" as 100+ different firms. |
| **B — Filter Retailers out + lift to recall_event** *(recommended)* | Remove the Retailers[] branch from `firm.sql` + `recall_event_firm.sql` exploded sets. Add `recall_event.sales_channel_narrative` (JSONB or text[]) populated from `retailers[]` array elements. Keep Manufacturers/Importers/Distributors in firm dim. | Fixes the worst pollution. Sales-channel narrative becomes a structured first-class field (good for landing pages: "Sold at: Amazon.com from Aug 2023 through Feb 2026 for ~$43"). Firm dim quality improves immediately. | Manufacturer/Importer/Distributor fragmentation (Bug 2) remains. Net `recall_event_firm` row count drops materially for CPSC. |
| **C — B + CPSC-specific name normalization** | Option B plus a CPSC-specific name-cleaning step before `firm.sql` normalize: strip `, of <location>`, strip `, dba <name>`, strip trailing company-type tokens (`Inc.`, `Ltd`, `LLC`). Capture stripped DBA names into a `firm.alternate_names` JSONB. | Most thorough fix. Best chance of cross-source firm collapse ("ZOLIQUEX" finally matches across recalls regardless of country suffix). DBAs preserved as alternate-name candidates for the Phase 6b RapidFuzz workstream. | Most invasive — CPSC-specific transform in cross-source silver. Risks over-stripping legitimate firm-name suffixes ("US LLC" is part of some legal names). Pre-RapidFuzz this is reinventing fuzzy matching by hand. |

### Recommendation: Option B in the (a) PR, defer Option C to Phase 6b RapidFuzz

Reasoning:

1. **Option B is a clean structural fix** that doesn't depend on any subjective normalization choices. Retailers[] empirically don't fit the firm contract; lifting them to `sales_channel_narrative` matches both the data shape and the landing-page user goal.
2. **Option C's name cleaning belongs with RapidFuzz**, not as a bespoke CPSC silver transform. The Phase 6b plan already calls for cross-source fuzzy matching with FDA `firm_fei_num` as anchor (per ADR 0002 + `project_scope/archive/phase-6-execution-plan.md`). CPSC name-stripping is conceptually the same problem as USDA's `multi_state` duplicate-name resolution — both want to collapse "this firm under different surface forms." Solve them together.
3. **The `CompanyID`-always-empty observation is just empirical truth**; no architectural response needed. The `firm_identifier` table sketched in §6 of the Phase 6 execution plan is FDA + USDA + USCG only — CPSC adds nothing to it. Document and move on.

**What changes in the (a) PR under Option B:**

- `dbt/models/silver/firm.sql:36-39` — remove the `cpsc_firms` `retailer` branch.
- `dbt/models/silver/recall_event_firm.sql:30-33` — remove the `cpsc_firms` `retailer` branch.
- `dbt/models/silver/recall_event.sql:25-54` — add `recall_event.sales_channel_narrative` JSONB column for CPSC; populate from `retailers` array. NULL for FDA/USDA/NHTSA/USCG.
- `_silver.yml` — drop `retailer` from `recall_event_firm.role` accepted_values; add column description for `sales_channel_narrative`.

This is a focused 3-file change; risk-bounded.

### Status — Option B confirmed by user 2026-05-29

Bug 2's name-cleaning work explicitly bundled with the broader Phase 6b RapidFuzz workstream (per user: "bundled in the rapidfuzz stage 6b with FDA firms and other information that needs to be extracted/manipulated with rapidfuzz before settling in silver"). See `project_scope/archive/phase-6-execution-plan.md` § Phase 6b → "CPSC firm-name normalization (Phase 6a audit follow-on)".

## 7. Decisions locked in (confirmed 2026-05-29)

All items below confirmed by user in conversation 2026-05-29 — alongside §6 Option B and the Phase 6b bundling of name-cleaning with the RapidFuzz workstream.

1. **§3 Bug 1 fix path — adopt Option B in §6.** Remove `Retailers[]` from `firm.sql` + `recall_event_firm.sql`; lift to `recall_event.sales_channel_narrative` JSONB.
2. **§3 Bug 2 — bundled into the Phase 6b RapidFuzz workstream.** Manufacturer/Importer/Distributor name fragmentation ("of <location>", "dba <name>" suffix patterns) handled by a pre-silver name-normalization stage in Phase 6b alongside FDA firm normalization and the USDA disambiguation work. Detail in `project_scope/archive/phase-6-execution-plan.md` § Phase 6b → "CPSC firm-name normalization".
3. **§3 Bug 3 — documented-empty.** `CPSC.CompanyID` always empty in JSON; `observed_company_ids` JSONB is permanently NULL for CPSC-only firms. Note in `_silver.yml` description. No code change.
4. **§4 lifts to first-class silver columns:**
   - `remedies` → `recall_event.remedies` (JSONB)
   - `remedy_options` → `recall_event.remedy_options` (text[] or JSONB)
   - `injuries` → `recall_event.injuries` (JSONB)
   - `images` → `recall_event.images` (JSONB)
   - `consumer_contact` → `recall_event.consumer_contact` (text)
   - `manufacturer_countries` → `recall_event.manufacturer_countries` (text[] or JSONB)
   - `in_conjunctions` → `recall_event.coordinated_recall_urls` (text[] or JSONB)
   - `retailers` → `recall_event.sales_channel_narrative` (JSONB) — per §6 Option B
   - Cross-source column names (and possible renames) deferred to cross-source consolidation.
5. **§4 — `product_upcs` lifted (revised post-R2 2026-05-29), `sold_at_label` dropped:**
   - `product_upcs` → `recall_event.product_upcs` (JSONB). **R2 validation revised this from "drop" to "lift"** — 2.7% of recalls carry 1-20 UPCs in JSON (not the always-empty Finding F suggested). Sparse but real signal; valuable for the FastAPI product-search surface.
   - `sold_at_label` — confirmed 100% NULL across 1,368-record corpus per R2. Documented-empty-by-source; drop from silver `source_payload_raw`.
6. **§5 — `Hazards[].HazardType` and `Hazards[].HazardTypeID` documented-empty.** Keep in bronze `hazards` JSONB; document in `_silver.yml`. Don't surface as silver filter columns. Don't propose `Hazard=` as a FastAPI filter param.
7. **`products[].Type` and `products[].CategoryID` — KEEP in silver (decision reversed post-SQL 2026-05-29).** Cassette had falsely suggested both empty; 2026-05-29 SQL Q2 confirmed **both are populated for 58.8% of recalls** (596 of 1,422). They carry real categorization signal and stay as `recall_product.type` + `recall_product.category_id` columns. Separately: `products[].Description` and `products[].Model` ARE confirmed 100% empty across the corpus. Document those two as "documented-empty-by-source for CPSC" in `_silver.yml`; the CPSC branch of `recall_product` can either keep them as silently-blank for cross-source schema parity OR explicit-NULL them. Recommend silent-blank (zero code change) since downstream consumers can filter `WHERE product_description != ''` if needed.
8. **Cross-source product-category column.** CPSC's `products[].Type` (when populated) is the closest analog to FDA's `producttypeshort` (Devices/Food/Drugs/Cosmetics) and USDA's `processing` (Heat Treated/Raw etc.). Cross-source naming alignment deferred to cross-source consolidation pass after NHTSA + USCG audits.

## 8. Capture-expansion items deferred to backlog

Logged in `documentation/audit/capture_expansion_backlog.md` § CPSC (to be populated).

**Summary by priority:**

- **HIGH — none.** Bronze fully captures the CPSC JSON endpoint surface (including positive drift on `SoldAtLabel` + `Image.Caption`). No capture gap.
- **MEDIUM — none.** Same reason.
- **LOW — UPCs via `RecallDelimited` endpoint.** Per Bruno `search_by_upc.yml` and `additional retrieval service` (PDF p. 8): the `RecallDelimited` variant returns UPCs that the JSON endpoint doesn't populate. Adding a second-endpoint fetch (similar in spirit to FDA's `/recalls/event/{id}` enrichment) would surface UPCs for products that have them. **Cost:** doubles request count per extraction window; introduces pipe-delimited parsing. **Value:** UPC-based product search ("show me recalls of UPC 822843828494") on the FastAPI surface. **Verdict:** defer pending Phase 8 search-endpoint scope. Not a foundation-audit item.
- **SKIP — `RecallDelimited` as a JSON replacement.** Same data, less structured format. No reason to switch the primary extractor.
- **SKIP — search-parameter filters as extractor knobs.** `RecallTitle=`, `Hazard=`, etc. are FastAPI/search-layer concerns, not extraction concerns. The extractor uses only `LastPublishDateStart` (incremental) + `RecallDateStart` (deep rescan), which is correct.
- **SKIP — `format=xml`.** JSON gives strictly more structured shape than XML for the same data.

**Like USDA, CPSC's (b) capture-expansion PR has zero adds.** The JSON endpoint is fully covered. The audit's structural gains are §3 (firm-dim cleanup), §4 (JSONB lifts), and §6 (Option B firm architecture).

## 9. R2 validation status

CPSC has **no Akamai gating** (per `last_publish_date_semantics.md` cache-validator finding: server emits `Cache-Control: no-cache` but no bot-detection headers; per `src/extractors/cpsc.py` no UA spoofing or cookie handling needed). Per `array_stability_findings.md`, current bronze sits at 1,360 rows / 1,357 distinct recalls as of 2026-05-08; the corpus has likely grown since.

### Corpus-scale re-validation (2026-06-02 — full-corpus seed, 9,828 rows)

The full-corpus deep-rescan seed (extraction run 2026-05-31: 9,828 extracted / 9,828 inserted / 0 rejected; `last_publish_date` 1975-04-07 → 2026-05-29) is **6.9× the 1,422-record May slice**. Re-running the three CPSC bronze scripts at this scale (`explore_bronze_shape.sql`, `inspect_array_field_population.sql`, the new `inspect_firm_name_fragmentation.sql`) confirmed most prior findings and **corrected several the small sample got wrong**. Feeds `documentation/audit/bronze_corpus_profile.md` §1/§2/§3/§4/§5/§6.

**Structural changes (load-bearing):**

- 🆕 **Multi-product CPSC recalls are now REAL — the C2 "always length 1" assumption is falsified.** 11,836 product elements across 9,828 recalls: 91.7% length 1, but **8.3% have >1 product, up to 57** (`explore` Q8 / `array` Q3). Every prior corpus (1,193 / 1,357 / 1,422) was universally single-product, making the `array_stability_findings.md` C2/C3 append-only assumptions *vacuous*. They are no longer vacuous: the ordinal-based silver product surrogate key is load-bearing and `assert_products_array_append_only.sql` now guards real data. **Follow-up: revisit `array_stability_findings.md` C2/C3 status (currently "vacuously holds").**
- 🆕 **~18.3% archival-skeleton cohort.** `remedies` (18.3%), `injuries` (18.3%), `images` (18.2%), `retailers` (18.5%), `consumer_contact` (18.5% null), `manufacturer_countries` (17.5%) all empty at a near-uniform ~18.3% floor — an archival cohort (led by the 1,597-record 2014-05-23 migration spike, `explore` Q10) carrying only core scalars (`title`/`description`/`url`/`recall_date`/`products`, all ~0% empty). This floor is the realistic NULL rate for **every** collection lift; the rich-collection lifts are ~81.7% populated, not ~99%.

**Corrections to sample-based figures:**

| Field | May sample | Full corpus (9,828) | Note |
|---|---|---|---|
| `products[].number_of_units` empty | 0.2% | **32.2%** | sample was recent-only; corpus (incl. archival + secondary products) is 1/3 empty → silver TEXT, nullable |
| `products[].name` empty | 0.0% | **3.3%** | can't be hard NOT NULL → warn-tripwire |
| `products[].type` / `.category_id` empty | 41.2% | 40.4% | confirmed (~59.6% populated) |
| `Image.Caption` empty | 0.0% | **5.1%** | landing-page alt-text needs a ~5% fallback after all |
| `RemedyOption` enum | 4 values | **8 values** | added `New Instructions` (1.05%), `Label` (0.15%), `No Remedy Available` (0.10%), `Inspect` (0.08%); top 3 still 97.95%; `R` typo + narrative-paragraph outlier persist → warn |
| `ManufacturerCountries` top | China 61.4% | China 52.6%, US 17.9% | distribution shifted; `United Stateso` typo persists |

**Confirmed at corpus scale:**

- ✅ `CompanyID` **100% empty across all 22,463 firm-role elements** (Bug 3) — 8,202 manufacturer + 8,015 retailer + 4,101 importer + 2,145 distributor (`array` Q1).
- ✅ `Hazards[].hazard_type` + `hazard_type_id` 100% empty across 9,710 elements; `name` 100% populated (Finding G).
- ✅ `sold_at_label` 100% NULL; `products[].description` + `products[].model` 100% empty (documented-empty-by-source).
- ✅ **Bug 1 (retailer narrative) worse at scale:** retailer `Name` avg length **141** (vs 26–45 for M/I/D), max **1,454**, **99.2% distinct** across 8,015 elements (`array` Q10).
- ✅ **Edit detection: 0 multi-hash recalls** (`explore` Q4/Q5) — 1:1 recall_id:row in this single-shot seed (one edit *was* observed pre-seed: recall `00015`, 2026-05-08). SCD NEED at the recall key = low; snapshot caveat applies (can't measure long-term edit rate).

**Firm fragmentation baseline (new `inspect_firm_name_fragmentation.sql`):**

- **§6 Option B quantified:** dropping `retailers[]` from the firm dim removes **7,947 firm rows = 44.2%** of CPSC's 17,974-name footprint, with **zero overlap** with M/I/D names (`net_firms_removed` == `retailer_only_distinct_names` == 7,947). Cleanest possible backing for Option B — it touches only the retailer-narrative rows.
- **§3 Bug 2 magnitude (deferred to 6b):** **62.8%** of the 14,444 M/I/D names carry a strippable `, of <geo>`/`dba` suffix (`comma_strippable_total` 9,074). The conservative comma-anchored strip collapses **5.7% (576 firms)** *within the current corpus* — most fragmentation is **latent** (a unique name with a geo suffix stays unique until the same firm recurs with a different spelling, e.g. the three `3M Company, of {St. Paul, Minnesota / Saint Paul, Minnesota / St. Paul, Minn.}` rows → one `3M COMPANY`). Recurring-firm share rises 16.2% → 18.1%. Space-prefixed `dba` (595 names) + parentheticals (501) are residual headroom the simulation deliberately leaves for full 6b normalization.

### Toolkit built 2026-05-29 — ready to run

- ✅ **`scripts/cpsc/audit/_lib.py`** — mirror of FDA + USDA `_lib.py`. `DEFAULT_CACHE_DIR = data/exploratory/cpsc/` (gitignored). Extended `summarize_field` with a list-of-dict path: reports element_count distribution + element samples for the 13 dict-element JSONB arrays (Manufacturers, Retailers, Hazards, etc.), skips distinct-count where elements aren't hashable.
- ✅ **`scripts/cpsc/audit/inspect_landed_payloads.py`** — three source modes (`--raw-landing-path`, `--local-path`, `--date YYYY-MM-DD`). CPSC payload shape is a flat JSON array with PascalCase keys (same as USDA). No `--langcode` (English only). No `--limit-per-date` (CPSC is incremental — daily payloads carry only the watermark window, not the full corpus).
- ✅ **`scripts/sql/cpsc/bronze/inspect_array_field_population.sql`** — 11-query batch for nested-key validation that the Python inspect can't drill into. Closes the §9 gaps the top-level summary can't answer (CompanyID across 4 firm roles, Products[].Type/CategoryID, Hazards[].HazardType/HazardTypeID, Images[].Caption, RemedyOptions[].Option enum, ManufacturerCountries[].Country distribution, per-array element-count, Retailers[].Name length signal). Sibling to the pre-existing `explore_bronze_shape.sql` which covers top-level scalar + array NULL rates.

### Python inspect findings — 2026-05-29 run (1,368 records across 4 daily payloads)

Run: `python scripts/cpsc/audit/inspect_landed_payloads.py --date 2026-05-02 2026-05-09 2026-05-16 2026-05-21 ...`. Resolved 4 of 5 dates (2025-05-29 was a typo — that date has no run). 1,368 records spanning the dominant 2026-05-02 backfill (1,336 extracted, 145 inserted post-content-hash-dedup) + three smaller daily runs.

**Confirmations (no doc change needed):**

- ✅ `SoldAtLabel` 100% NULL — Finding F confirmed at 1,368-record corpus scale.
- ✅ `Products` always length=1 (1,368 of 1,368) — C2 array-stability assumption remains vacuous; multi-product CPSC recalls still not observed.
- ✅ `Hazards` 99.85% length=1 (1,366 of 1,368), 0.15% empty (2 records). Finding F's 0.2% empty estimate confirmed.
- ✅ `Remedies` 100% length=1, populated. Lift to `recall_event.remedies` will be broadly populated.
- ✅ `Injuries` 99.93% length=1, plus 1 record (0.07%) with length=2 — edge case worth noting; multi-injury narrative recalls are possible.
- ✅ `Retailers` 99.4% populated (0.6% empty). Sample narratives confirm §3 Bug 1 at corpus scale: `"Online at Walmart.com from November 2025 through January 2026 for about $100."`, `"Online at Acer.com, Amazon.com and in stores at BrandMart from between June 2023 and February 2026 for between $245 and $70."` (multi-outlet + window + price-range).
- ✅ `Manufacturers` 54.8% empty (749 of 1,368). Finding F's 56% estimate confirmed within 1.2pp. Sample names confirm §3 Bug 2: `"Mobility Source Medical Technology Co., Ltd. of China"`, `"Alliance Chemical, of Taylor, Texas"`, `"Bayer HealthCare LLC, of Whippany, New Jersey"`.
- ✅ `ManufacturerCountries` 26.0% empty (Finding F: 28%). Multi-country recalls exist: 29 records with 2 countries, 7 with 3.
- ✅ `CompanyID="" ` visible in every firm-role array sample (Manufacturers, Importers, Distributors, Retailers). Exact 100%-empty rate awaits the SQL Q1 confirmation.
- ✅ `Image.Caption` visibly populated in samples (positive drift confirmed). Exact populated rate awaits SQL Q5.

**New findings refining earlier observations:**

- 🆕 **`ProductUPCs` is NOT always empty in JSON.** 97.3% empty (1,331 of 1,368), 2.7% populated with 1-20 UPCs per recall (max 20). Contradicts Finding F's "always `[]`" and the cassette observation. **§7 item 5 + §4 lift table updated**: lift `product_upcs` to `recall_event.product_upcs` JSONB instead of dropping it. Sparse but a real signal for FastAPI UPC-search.
- 🆕 **`"doing business as"` full form alongside `"dba"`.** R2 sample: `"Shenzhen Maikeer Industrial Co., Ltd., doing business as MalkerDirect, of China"`. The §3 Bug 2 + Phase 6b CPSC normalization regex needs to handle both forms (not just `"dba"`). Plan update applied to `project_scope/archive/phase-6-execution-plan.md` § Phase 6b → "CPSC firm-name normalization".
- 🆕 **`Inconjunctions` 93.7% empty.** Only 6.3% of recalls (86 records) have coordinated cross-jurisdiction links; mostly Canada (`recalls-rappels.canada.ca`). Lift to `recall_event.coordinated_recall_urls` still valuable but for a small slice — `_silver.yml` description should set expectation.
- 🆕 **`RemedyOptions` 58.6% empty (801 of 1,368) despite `Remedies` being 100% populated.** The structured Option enum (Refund/Repair/Replace) is sparse — most recalls carry only the free-text `Remedies[].Name` narrative, not the categorical tag. Lift to `recall_event.remedy_options` (text[] or JSONB) still useful as a filter when present; exact enum distribution awaits SQL Q6.
- 🆕 **`Importers` 64.2% empty, `Distributors` 72.2% empty.** Both sparser than Manufacturers (54.8%). Distributors carry the highest-cardinality elements per record — max length=8 distributors per recall observed.
- 🆕 **`Description` 1 record NULL (0.1%).** First observed NULL Description across CPSC bronze. Edge case — landing-page rendering for that single recall must handle NULL gracefully.
- 🆕 **`Description` max length 5,983 chars, `Title` max 268 chars, `ConsumerContact` max 757 chars.** Useful sizing inputs for FastAPI response-shaping and DB column-width decisions (current `text` typing handles these fine).
- 🆕 **`URL` 1,367 distinct of 1,368.** One recall pair shares a detail URL — likely a CPSC editorial collapse (two recall numbers pointing at one detail page) or a re-publish artifact. Out of scope for the audit; noted for future investigation.

**Per-array element-count distribution (key arrays):**

| Array | Empty % | Populated len=1 | Populated len=2 | Populated len≥3 | Max len |
|---|---|---|---|---|---|
| `Products` | 0% | 1,368 (100%) | — | — | 1 |
| `Remedies` | 0% | 1,368 (100%) | — | — | 1 |
| `Injuries` | 0% | 1,367 (99.9%) | 1 (0.07%) | — | 2 |
| `Hazards` | 0.15% | 1,366 (99.85%) | — | — | 1 |
| `Retailers` | 0.6% | 1,356 (99.1%) | 4 (0.3%) | — | 2 |
| `Images` | 2.6% | 595 (43.5%) | 350 (25.6%) | 388 (28.4%) | 28 |
| `ManufacturerCountries` | 26.0% | 977 (71.4%) | 29 (2.1%) | 7 (0.5%) | 3 |
| `Manufacturers` | 54.8% | 604 (44.2%) | 8 (0.6%) | 7 (0.5%) | 5 |
| `RemedyOptions` | 58.6% | 541 (39.5%) | 24 (1.8%) | 2 (0.1%) | 3 |
| `Importers` | 64.2% | 484 (35.4%) | 6 (0.4%) | — | 2 |
| `Distributors` | 72.2% | 359 (26.2%) | 15 (1.1%) | 6 (0.4%) | 8 |
| `Inconjunctions` | 93.7% | 84 (6.1%) | 2 (0.1%) | — | 2 |
| `ProductUPCs` | 97.3% | 24 (1.8%) | 5 (0.4%) | 8 (0.6%) | 20 |

### SQL nested-array findings — 2026-05-29 run (1,422 bronze records)

First run on 2026-05-29 surfaced a key-casing bug: queries used PascalCase JSONB keys but bronze stores snake_case (per the §5 "Bronze JSONB key casing" gotcha — diagnosed and fixed alongside the analogous bug in `explore_bronze_shape.sql` Q9). After the fix, the second run produced these findings.

**Headline confirmations:**

- ✅ **§3 Bug 3 definitively confirmed.** Q1: 100% empty CompanyID across 3,038 firm-role elements (674 Manufacturer, 1,416 Retailer, 518 Importer, 430 Distributor). **0 distinct non-empty values across any role.**
- ✅ **§3 Bug 1 quantitatively damning.** Q10: Retailer name avg length 135 chars (3× the M/I/D avg of 44-49), max 733 chars, **98.7% distinct** (1,398 distinct of 1,416). Q11 samples confirm narrative content with outlets + windows + prices.
- ✅ **§3 Bug 2 worse than expected.** Q10: Manufacturer/Importer/Distributor distinctness rates 87-94% — firms barely collapse even within a single role. Phase 6b name normalization is critical, not nice-to-have.
- ✅ **Finding G exact corpus confirmation.** Q4: `Hazards[].hazard_type` + `hazard_type_id` 100% empty across 1,420 hazard elements. `Hazards[].name` 100% populated.
- ✅ **`Images.Caption` 100% populated.** Q5: 3,170 elements, both `url` + `caption` 100% populated. Positive PDF drift confirmed; landing pages can use Caption as alt text without fallback.
- ✅ **Products always length=1.** Q3: 1,422/1,422 records. C2 array-stability assumption remains vacuous; ordinal-based silver surrogate key still safe.

**New empirical findings refining §1, §4, §7:**

- 🆕 **`Products[].Type` is 58.8% populated.** Q2: total 1,422 elements, `type` 41.2% empty → 58.8% populated. Cassette had falsely suggested 100% empty (3 of 3 samples). §7 item 7 reversed: keep `recall_product.type` in silver for CPSC.
- 🆕 **`Products[].CategoryID` is 58.8% populated.** Same rate as `type` (likely correlated — populated for the same recalls). §7 item 7 also reverses for `category_id`.
- 🆕 **`Products[].Description` is 100% empty + `Products[].Model` is 100% empty.** Per Q2. These do NOT carry signal for CPSC. `recall_product.product_description` and `recall_product.model` will be silently blank for every CPSC product. Document in `_silver.yml`.
- 🆕 **`RemedyOptions.Option` enum: 4 legitimate values (Refund 65.1%, Repair 17.5%, Replace 17.0%, Dispose 0.2%), 99.5% in top 3.** Q6 across 613 elements. Plus 2 data-quality outliers: `"R"` (1 record, likely typo) and a full multi-line narrative paragraph (1 record, CPSC data-entry error).
- 🆕 **`ManufacturerCountries.Country`: 30+ distinct values, China dominates (61.4%), United States (15.5%) second.** Q7 across 1,088 elements. **Data quality finding:** `"United Stateso"` typo on 2 records — country normalization worth doing in silver if FastAPI exposes country as a search facet.

**Data quality flags (worth tracking; not blocking):**

| Finding | Volume | Source | Action |
|---|---|---|---|
| `"United Stateso"` typo in `ManufacturerCountries` | 2 records | CPSC data entry | Silver `case when country='United Stateso' then 'United States' else country end` normalization or warn-test |
| `"R"` value in `RemedyOptions.Option` | 1 record | CPSC data entry (likely typo for Refund or Repair) | Silver `accepted_values` warn-test surfaces it |
| Full narrative paragraph in `RemedyOptions.Option` | 1 record | CPSC data entry error (Option got remedy-text instead of enum) | Silver `accepted_values` warn-test surfaces it; investigate the affected `RecallNumber` if it persists |
| `URL` duplicate (1,367 distinct of 1,368) | 1 pair | CPSC editorial collapse or republish artifact | Out-of-scope; flagged for future investigation |
| `Description` NULL | 1 record | First-ever-observed CPSC NULL description | Landing-page renderers must handle NULL; not silver-fix territory |

**§3/§4/§7 update propagation done in this commit:**

- §3 Bug 1 — added quantitative confirmation table (98.7% distinct, avg 135, max 733)
- §3 Bug 2 — added 87-94% distinct M/I/D quantitative finding
- §3 Bug 3 — added exact corpus confirmation table
- §1b — `Products`, `Images`, `Hazards`, `RemedyOptions`, `ManufacturerCountries` rows updated with SQL Q2-Q7 figures
- §7 item 7 — REVERSED: keep `Type` + `CategoryID` (both 58.8% populated); document `Description` + `Model` as empty-by-source
- §5 — new "Bronze JSONB key casing" gotcha row explaining the snake_case convention discovered during the SQL diagnosis
  - Confirm `CompanyID` 100% empty across all 4 firm arrays at corpus scale
  - Confirm `Products[].Type` + `Products[].CategoryID` populated rates (decision input for §7 item 7)
  - Confirm `Hazards[].HazardType` + `Hazards[].HazardTypeID` 100% empty at corpus scale
  - Confirm `Inconjunctions` populated rate (capture-expansion HIGH/MEDIUM/LOW input if it's higher than the cassette suggests)
  - Confirm `Images.Caption` populated rate (currently observed populated in cassette; gauge consistency)
  - Per-array element-count distributions for Manufacturers/Retailers/Importers/Distributors — confirms the "almost always single-element" cassette observation at corpus scale
  - `RemedyOptions` enum cardinality — confirms Refund/Repair/Replace is the full set
  - `ManufacturerCountries` element-count distribution — single-country dominates per cassette; confirm
  - Multi-product corpus check — per Finding A all 1,193 recalls had `Products` length=1; confirm this still holds (relevant to C2 array-stability assumption)

### Headline empirical findings to date (from `first_extraction_findings.md` Section F + cassette inspection)

| Finding | Source | Impact |
|---|---|---|
| `Products` always length=1 across 1,193 records | Finding A (2026-04-17) + `array_stability_findings.md` (2026-05-08, 1,357 distinct recalls) | C2 array-append-only assumption vacuously holds; revisit when multi-product recalls land |
| `Manufacturers[]` 56% empty | Finding F | High enough that "no manufacturer named" is a primary CPSC pattern — landing pages need to handle gracefully |
| `Retailers[]` 0.7% empty (almost always populated) | Finding F | Most recalls carry sales-channel narrative — §6 Option B's `sales_channel_narrative` is broadly populated |
| `Hazards` 0.2% empty (almost always populated) | Finding F | Hazard-narrative landing-page rendering is safe |
| `Hazards[].HazardType` 100% empty (across 1,191 hazard-bearing rows) | Finding G | Don't filter by; don't expose in FastAPI |
| `ProductUPCs` always `[]` in JSON endpoint | Finding F + Bruno UPC probe | UPCs available only via `RecallDelimited` — §8 LOW capture-expansion item |
| `SoldAtLabel` 100% NULL | Finding F | Documented-empty-by-source; drop from silver |
| Edit detection: first observed CPSC edit on 2026-05-08 (recall `00015`) | `array_stability_findings.md` | A copy-edit on `retailers[0].name` ("$125 to $175" → "$125 and $175"); confirms edits exist but don't advance `LastPublishDate`. Deep-rescan remains load-bearing |
| `LastPublishDate` semantics — bimodal | `last_publish_date_semantics.md` | 478 of 1,193 records have `recall_date == last_publish_date` (or 1-day gap); 709 of 1,193 are 25-year-old archive-migration re-processings |
| 2005-2024 gap in current bronze | `last_publish_date_semantics.md` | Archive migration is processing pre-2004 records at ~2-3/day; one-time deep-rescan with multi-year lookback is the only way to load 2005-2024 |

## References

- `src/extractors/cpsc.py` — incremental extractor (no auth, no pagination, watermark-based; 5-step lifecycle)
- `src/schemas/cpsc.py` — Pydantic bronze contract (21 columns, 14 of which are pass-through JSONB)
- `config/sources/cpsc.yaml` — source registry entry
- `dbt/models/staging/stg_cpsc_recalls.sql` — latest-per-recall view, zero column transformation
- `dbt/models/silver/recall_event.sql:25-54` — CPSC → recall_event mapping (where §4 lifts will land)
- `dbt/models/silver/recall_product.sql:26-59` — CPSC → recall_product mapping
- `dbt/models/silver/firm.sql:32-59` — CPSC → firm mapping (where §3 Bug 1 + Bug 2 live)
- `dbt/models/silver/recall_event_firm.sql:25-51` — CPSC → recall_event_firm mapping (where §3 Bug 1 will be fixed under Option B)
- `documentation/cpsc/first_extraction_findings.md` — corpus shape, Findings A-J (1,193 records, Apr 2026)
- `documentation/cpsc/last_publish_date_semantics.md` — incremental cursor analysis + archive-migration finding
- `documentation/cpsc/array_stability_findings.md` — C2/C3 assumption monitoring + first observed CPSC edit
- `documentation/silver_design_notes.md:16-91` — CPSC/FDA column-mapping reference (predates this audit; will be expanded in cross-source consolidation)
- `bruno/cpsc/` — request collection covering incremental, deep-rescan, data exploration, lookup, alternative-format
- `tests/fixtures/cassettes/cpsc/` — 4 scenario cassettes (happy_path_recent, _wide_window, _narrow_window, empty_result)
