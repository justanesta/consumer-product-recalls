# FDA field audit — 2026 W22

- **Status:** In progress 2026-05-28
- **Scope:** FDA iRES enforcement recall fields — every documented column vs. what we capture, what silver does with it, and what's missing
- **Methodology:** `documentation/audit/methodology.md`
- **Companions:**
  - Capture-expansion backlog: `documentation/audit/capture_expansion_backlog.md`
  - Validation scripts: `scripts/fda/audit/inspect_landed_payloads.py`, `scripts/fda/audit/probe_displaycolumns.py`
  - Source PDFs: `documentation/fda/enforcement_report_api_definitions.pdf`, `documentation/fda/iRES_enforcement_reports_api_usage_documentation.pdf`

## 1. API field universe (per the FDA Enforcement Report API definitions PDF — 48 columns)

Names are lowercase as in the PDF; bronze captures them via `validation_alias` against the uppercase forms returned by the API (Finding D in `api_observations.md`).

### Identifiers + event-level

| Field | Definition |
|---|---|
| `productid` | Numerical designation assigned by FDA to a specific recall product |
| `recalleventid` | Numerical designation assigned by FDA to a specific recall event |
| `recallnum` | System-generated, numeric designation assigned by FDA to a specific, classified recalled product |
| `centercd` | Name of the FDA Center that classifies the recall (CDRH, CDER, CFSAN, etc.) |
| `producttypeshort` | Commodity of the recalled product |
| `phasetxt` | Status (On-going / Completed / Terminated) |
| `centerclassificationtypetxt` | Classification (I / II / III) — health-hazard severity |
| `voluntarytypetxt` | Voluntary (firm volition) vs FDA-requested |

### Dates

| Field | Definition |
|---|---|
| `eventlmd` | Event last modified date |
| `productlmd` | Product last modified date (**not available via bulk POST** per Finding K0; documented but not returnable through this endpoint) |
| `recallinitiationdt` | Date firm began notifying public/consignees |
| `determinationdt` | Date FDA determined firm's action meets recall definition |
| `centerclassificationdt` | Date FDA classified the recalled products (I/II/III) |
| `enforcementreportdt` | Date FDA issued the weekly enforcement report |
| `terminationdt` | Date FDA terminated the recall |
| `createdt` | Date recall was first posted |
| `postedinternetdt` | Date first posted to Enforcement Report (blank for pre-2022-10-25 recalls) |
| `pressreleaseissuedt` | Date press release was posted to FDA's site |

### Firm (recalling-firm at time of recall)

| Field | Definition |
|---|---|
| `firmlegalnam` | Name of the recalling firm at the time of the recall |
| `firmfeinum` | FDA Establishment Identifier (FEI) of the recalling firm at the time of the recall |
| `firmcitynam`, `firmstateprvncnam`, `firmcountrynam`, `firmline1adr`, `firmline2adr`, `firmpostalcd` | Recalling firm address components at the time of the recall |
| `firmsurvivingnam`, `firmsurvivingfei` | Current firm name/FEI if changed since the recall |

### Product description / reason / code-info — three full+indicator+short triples

| Field | Definition |
|---|---|
| `productdescriptiontxt` | **Brief description of the product** |
| `productdescriptionshort` | Abbreviated product description for search results / pop-ups |
| `productdescriptionindicator` | UI flag — whether a "more…" expander should render |
| `productshortreasontxt` | **Information describing how the product is defective** — column-full-text label is "Reason for Recall (Full Text)"; field name is misleading (see §5) |
| `recallreasonshort` | Abbreviated Reason for Recall |
| `recallreasonindicator` | UI flag |
| `distributionareasummarytxt` | **General area of initial distribution** (states, countries, territories) — subsequent re-distribution by consignees may not be included |
| `distributionpatternshort` | Abbreviated Distribution Pattern |
| `distributionpatternindicator` | UI flag |
| `codeinformation` | **Lot and/or serial numbers, product numbers, expiration dates, sell or use by dates** on the product or its labeling |
| `codeinfoshort` | Abbreviated Code Information |
| `codeinfoindicator` | UI flag |

### Other

| Field | Definition |
|---|---|
| `productdistributedquantity` | Amount of product subject to recall |
| `initialfirmnotificationtxt` | Method(s) the firm used to notify public/consignees |
| `pressreleasetype` | Press release type (State / Firm / FDA) |
| `pressreleaseurl` | URL of press release(s) on FDA's website |

### Audit-history endpoint only (not bulk POST)

| Field | Definition |
|---|---|
| `fieldname`, `newvalue`, `oldvalue` | Value-tracking endpoint fields. Not applicable to `/recalls/` bulk POST |

## 2. Current bronze capture (33 fields as of Phase 6a.5; originally 22)

`_DISPLAY_COLUMNS` in `src/extractors/fda.py` requests 32 fields; `RID` is response-injected (query-position counter, Finding F) for 33 total. Schema: `src/schemas/fda.py`. Staging projection: `dbt/models/staging/stg_fda_recalls.sql`.

> **Phase 6a.5 update (2026-05-31):** the original capture was 21 requested + RID = 22. The 11 §7a SHIP fields were added to the bulk-POST capture (migration `0019`) **before** the historical seed so the one-time ~134k-record FDA pull lands everything silver + Phase 6b need — R2 replay can't recover un-requested columns, so capturing later would force a second Akamai-risky re-pull. Adding `codeinformation` dropped `_PAGE_SIZE` 5000 → 2500 (§6 Decision 5). Silver mapping/naming for the new fields is still deferred to the (b) PR. The bronze-column ↔ API-field table below covers the original 22; the 11 added fields are listed in §7a.
>
> **Full-corpus seed strategy (2026-05-31, api_observations.md Finding P + migration 0020):** the historical seed pulls the **no-window `filter:"[]"` full corpus** (not an `eventlmd` date window), because a window silently misses 197 null-`EVENTLMD` un-edited records (Finding H). Migration `0020` drops `NOT NULL` on the four formerly-"core" fields (`event_lmd`, `center_cd`, `product_type_short`, `firm_legal_nam`) so the no-window seed lands those rows instead of silently quarantining them — the "core identifiers never null" assumption (api_observations.md:374) was an inference the windowing masked, now falsified. Design + run procedure: `project_scope/archive/fda-historical-seed-plan.md`.

Bronze column → API field:

| Bronze column | API field |
|---|---|
| `source_recall_id` | `productid` |
| `recall_event_id` | `recalleventid` |
| `rid` | `RID` (response-only; excluded from `content_hash` per `fda.py:264`) |
| `center_cd` | `centercd` |
| `product_type_short` | `producttypeshort` |
| `event_lmd` | `eventlmd` |
| `firm_legal_nam` | `firmlegalnam` |
| `firm_fei_num` | `firmfeinum` |
| `recall_num` | `recallnum` |
| `phase_txt` | `phasetxt` |
| `center_classification_type_txt` | `centerclassificationtypetxt` |
| `recall_initiation_dt` | `recallinitiationdt` |
| `center_classification_dt` | `centerclassificationdt` |
| `termination_dt` | `terminationdt` |
| `enforcement_report_dt` | `enforcementreportdt` |
| `determination_dt` | `determinationdt` |
| `initial_firm_notification_txt` | `initialfirmnotificationtxt` |
| `distribution_area_summary_txt` | `distributionareasummarytxt` |
| `voluntary_type_txt` | `voluntarytypetxt` |
| `product_description_txt` | `productdescriptiontxt` |
| `product_short_reason_txt` | `productshortreasontxt` |
| `product_distributed_quantity` | `productdistributedquantity` |

## 3. Mismappings (silver-only — fixable in the (a) PR)

All three are visible by reading `dbt/models/silver/recall_event.sql` + `recall_product.sql` and cross-referencing the PDF column definitions. None require re-extraction.

### Bug 1 — `recall_event.description` is wrongly the distribution area

`dbt/models/silver/recall_event.sql:65`:

```sql
distribution_area_summary_txt    as description,
```

`distributionareasummarytxt` per the PDF is "General area of initial distribution such as states, countries, or territories" — it's *where the product went*, not what the recall is about. Cross-source pattern: `recall_event.description` carries the defect/hazard narrative for CPSC (`description`), USDA (`summary`), NHTSA (`desc_defect`), and USCG (`coalesce(problem_1, problem_2)`). FDA's matching field is `productshortreasontxt` (despite the misleading "short" in the name — see §5).

**Fix:** `recall_event.description ← product_short_reason_txt` for FDA. Move `distribution_area_summary_txt` to its own new column (provisionally `recall_event.distribution_area_summary` pending cross-source consolidation).

### Bug 2 — `recall_product.product_name` is wrongly the product description

`dbt/models/silver/recall_product.sql:67`:

```sql
product_description_txt          as product_name,
```

`productdescriptiontxt` per the PDF is "Brief description of the product" — it *is* the product description, not a name. FDA's bulk POST API has no canonical product-name field. Cassette evidence (Philips MRI recall, `recall_event_id=98779`): `PRODUCTDESCRIPTIONTXT` reads `"Philips SmartPath to dStream for XR and 3.0T with MR Elastography (MRE). \r\n\r\n1. Model Number (REF): 781270. \r\n\r\n2. Model Number (REF): 782113. ..."` — paragraph-length description with embedded model numbers.

**Provisional fix (the (a) PR):** Either leave `product_name` NULL for FDA (sparse but honest) or derive in silver as `LEFT(product_description_txt, 120)`. The fuller fix is in (b): add `productdescriptionshort` to the capture and use FDA's own truncation as `product_name`.

### Bug 3 — `recall_product.product_description` is wrongly the recall reason

`dbt/models/silver/recall_product.sql:68`:

```sql
product_short_reason_txt         as product_description,
```

`productshortreasontxt` per the PDF (and cassette confirmation) is the defect/reason narrative — what's *wrong* with the product, not what the product *is*. Same Philips example: `PRODUCTSHORTREASONTXT` reads `"The potential for stiffness value errors when a specific range of image reconstruction parameters is used in combination with Resoundant's algorithm, leading to the reconstruction voxel size settings in the default MRE scan protocol displaying too small."` — defect narrative.

**Fix:** `recall_product.product_description ← product_description_txt` for FDA (the actual product description).

## 4. Underused captures (in JSONB, should be lifted)

Already in bronze, currently buried in `recall_event.source_payload_raw` JSONB at `dbt/models/silver/recall_event.sql:70-81`:

| Bronze field | Proposed silver column | Why lift |
|---|---|---|
| `center_classification_dt` | `recall_event.classified_at` | Recall timeline — when FDA assigned Class I/II/III |
| `determination_dt` | `recall_event.determined_at` | Earliest known date in the recall flow |
| `termination_dt` | `recall_event.terminated_at` | Answers "is this still active?" on landing pages |
| `enforcement_report_dt` | `recall_event.enforcement_reported_at` | Official enforcement-report date |
| `initial_firm_notification_txt` | `recall_event.notification_method` | Landing-page narrative — how the firm notified consumers |
| `voluntary_type_txt` | `recall_event.voluntary_type` | Cross-source-alignable with USDA's voluntary indicator; user-facing context. **R2 validation found 2 distinct values for the same semantic** (`'Firm Initiated'` and `'Voluntary: Firm Initiated'`) — silver normalization required, see §8 |

These are zero-cost silver lifts — no extraction change required. Part of the (a) PR. Column names are provisional pending cross-source consolidation.

## 5. Field-naming gotchas

| FDA field | What the name suggests | What the PDF + cassette confirm it is |
|---|---|---|
| `productshortreasontxt` | A short / abbreviated version of "reason for recall" | The **Full Text** of the Reason for Recall — paragraph-length defect narrative. The "short" in the field name is misleading. The actual abbreviated form is `recallreasonshort` |

When proposing remappings, treat the column-full-text label + the definition as authoritative, not the field name.

## 5b. iRES endpoint architecture (load-bearing for the (b) PR scope)

Discovered 2026-05-28 via STATUSCODE 406 ("The payload displaycolumns does not match with the datagroup") when probing `pressreleaseurl`. Per `iRES_enforcement_reports_api_usage_documentation.pdf` page 7, the iRES API has **multiple endpoints with disjoint displaycolumn sets** — what FDA calls "datagroups." The bulk POST only accepts a subset of the 48 columns the API defines.

| Endpoint | Field set |
|---|---|
| `POST /recalls/` (what we use) | Documented bulk-POST displaycolumns: `productid, recalleventid, producttypeshort, firmcitynam, firmcountrynam, firmline1adr, firmline2adr, firmstatecd, firmpostalcd, phasetxt, recallinitiationdt, firmlegalnam, voluntarytypetxt, distributionareasummarytxt, terminationdt, initialfirmnotificationtxt, centerclassificationtypetxt, enforcementreportdt, firmfeinum, firmsurvivingnam, firmsurvivingfei, eventlmd, productdescriptiontxt, productshortreasontxt, recallnum, productdistributedquantity, determinationdt, postedinternetdt, codeinformation`. **Note FDA's docs are incomplete** — production successfully uses `centercd` and `centerclassificationdt`, neither of which is in this published list. |
| `GET /recalls/event/{eventid}` | `centercd, createdt, firmstateprvncnam` (full state name, not the 2-letter `firmstatecd`), `distributionpatternshort, distributionpatternindicator`, others |
| `GET /recalls/product/{productid}` | `productdescriptionshort, recallreasonshort, codeinfoshort, productlmd, productdescriptionindicator, recallreasonindicator, codeinfoindicator` |
| `GET /recalls/eventproducts/{eventid}` | Same as `product/{productid}` but batched by event |
| `GET /search/codeinfo/{productid}` | Just `codeinformation` (alternative path to bulk's codeinformation column) |
| `GET /search/pressreleaseurls/{eventid}` | `pressreleaseurl, pressreleaseissuedt, pressreleasetype` |
| `GET /search/productHistory/{productid}` | `fieldname, newvalue, oldvalue, productlmd` (audit-history) |

**Implication for the (b) PR:** several capture-expansion items I categorized in §7 require a *different fetch pattern* than our current bulk POST. Adding them means either (a) accepting the multi-endpoint enrichment cost (one GET per unique event_id and/or product_id), or (b) scoping them out for FDA. The decision lives in cross-source consolidation since other sources may have analogous fetch-pattern questions (CPSC paginated list + per-recall detail page is similar in spirit).

Re-categorized backlog reflects this in `documentation/audit/capture_expansion_backlog.md` § FDA.

## 6. Decisions locked in (2026-05-28 conversation)

1. **`recall_event.description` ← `productshortreasontxt` for FDA.** Cross-source semantic match. Final column name (whether to rename `description` → `recall_reason` / `summary`) deferred to cross-source consolidation.
2. **`recall_product.product_description` ← `productdescriptiontxt` for FDA.** Reverses Bug 3.
3. **`recall_product.product_name` for FDA** — provisional decision deferred until §7's `productdescriptionshort` arrives via (b); in (a) either NULL or `LEFT(product_description_txt, 120)`. Tentatively favor adding `productdescriptionshort` to displaycolumns and mapping it as `product_name` in (b).
4. **(b) capture-expansion PR delayed until all five sources audited.** Single-pass column-naming alignment across CPSC + FDA + USDA + NHTSA + USCG so we don't rename twice.
5. **Pay the `codeinformation` page-size tax.** Adding it to `displaycolumns` cuts page size from 5000 → 2500 (`src/extractors/fda.py:115`). Daily incremental impact negligible (~20-300 records/day); deep-rescan doubles request count, accepted as cost of having landing-page-critical lot/serial info.
6. **Methodology: API docs → R2 → bronze → staging → silver, in that order** (codified in `documentation/audit/methodology.md`).

## 7. Capture-expansion items deferred to backlog

Logged in `documentation/audit/capture_expansion_backlog.md` § FDA. Original priority groupings + empirical verdicts from the 2026-05-29 probe sweep:

### 7a. Bulk POST capture-expandable — SHIPPED in Phase 6a.5 (migration 0019, 2026-05-31)

> **Status update:** these 11 fields were originally slated for the (b) PR but were pulled forward into the **Phase 6a.5 bronze capture** (migration `0019`, `_DISPLAY_COLUMNS` + `src/schemas/fda.py`) so the one-time historical seed captures them — see §2. Bronze columns: `code_information`, `firm_city_nam`, `firm_country_nam`, `firm_line1_adr`, `firm_line2_adr`, `firm_postal_cd`, `firm_state_cd`, `firm_state_prvnc_nam`, `firm_surviving_nam`, `firm_surviving_fei` (BigInteger), `posted_internet_dt` (TIMESTAMPTZ). Their **silver mapping/naming remains deferred to the (b) PR** for cross-source alignment — only the bronze capture moved.

Confirmed in the 33-column bulk POST datagroup per `iRES_enforcement_reports_api_usage_documentation.pdf` page 7 and empirically validated via `scripts/fda/audit/probe_displaycolumns.py` against a 100-record window starting `eventlmdfrom=05/01/2026`.

| Field | Population rate (probe window) | Empirical notes | Verdict |
|---|---|---|---|
| `codeinformation` | not yet measured at corpus scale | Max length **205,424 chars** in a single record. Page-size penalty 5000 → 2500 (already accepted per §6 decision 5). Phase 6a.5 sizing impact noted in plan. | **SHIP** |
| `firmcitynam` | 100% | 39 distinct in 100 records. | **SHIP** |
| `firmcountrynam` | 100% | 3 distinct (United States 97, Netherlands 2, Switzerland 1). Required to discriminate non-US firms (state fields are NULL for them). | **SHIP** |
| `firmline1adr` | 100% | 42 distinct, 10-30 chars. | **SHIP** |
| `firmline2adr` | 0% (in this window) | All 100 NULL. Documented nullable per `api_observations.md:348`. Ship for schema future-proofing — zero storage cost on NULL. | **SHIP** |
| `firmpostalcd` | 97% | Mixed 5-digit ZIP and ZIP+4 (max 10 chars). The 3 NULL rows are the 3 international firms. | **SHIP** |
| `firmstatecd` | 97% | **Newly surfaced** — was not previously known to exist. 2-letter state code, 19 distinct. Co-varies perfectly with `firmstateprvncnam` (same NULLs, same per-state counts). | **SHIP** |
| `firmstateprvncnam` | 97% | Full state name, 19 distinct. Pairs with `firmstatecd`. | **SHIP** (both — denormalized lookup) |
| `firmsurvivingnam` | 15% | 3 distinct in 100 records (e.g. "ZimVie US Corp LLC" 13 rows). Populates iff firm renamed after recall. Critical for firm-dim continuity in Phase 6b. | **SHIP** |
| `firmsurvivingfei` | 15% | Paired with `firmsurvivingnam` — same row-level NULL alignment. | **SHIP** |
| `postedinternetdt` | 84% | `MM/DD/YYYY`, distinct from `eventlmd` (sample: posted `05/07/2025`, lmd `05/28/2026`). Definitions PDF: may be blank for recalls prior to 2022-10-25; the 16% NULL in this 2026 window is unexpectedly high — corpus rate likely lower than the PDF implies. | **SHIP** |

### 7b. Lookup-endpoint only — defer to enrichment pass

Each requested column returned **STATUSCODE 406** on bulk POST (or is documented as lookup-only per `api_observations.md` Finding K0). Capture would require a per-event or per-product GET pass after the bulk POST, same architectural pattern as the K0.1 `PRODUCTLMD` deferral.

| Field | Bulk POST status | Lookup endpoint | Notes |
|---|---|---|---|
| `pressreleaseurl` | 406 (per probe 2026-05-29) | `GET /search/pressreleaseurls/?eventid={id}` | Returns `RESULT.COLUMNS = [RECALLEVENTID, PRESSRELEASETYPE, PRESSRELEASEISSUEDT, PRESSRELEASEURL]`. Bruno collection at `bruno/fda/lookup/get_press_release_urls.yml`. |
| `pressreleaseissuedt` | 406 (same probe) | Same endpoint | — |
| `pressreleasetype` | 406 (same probe) | Same endpoint | Values per Definitions PDF: "State, Firm, or FDA". |
| `productdescriptionshort` | Not in 33-column list | `GET /recalls/event/{eventid}` or `/recalls/product/{productid}` | Truncated UI variant of `productdescriptiontxt`. May be derivable in silver via `LEFT(productdescriptiontxt, N)` — defer the "derive vs fetch" decision until the cross-source `product_name` strategy is set. |
| `recallreasonshort` | Not in 33-column list | Same lookup endpoints | Truncated UI variant of `productshortreasontxt`. Same derive-vs-fetch decision as above. |
| `distributionpatternshort` | Not in 33-column list | Same lookup endpoints | Truncated variant of `distributionareasummarytxt`. |
| `codeinfoshort` | Not in 33-column list | Same lookup endpoints | Truncated variant of `codeinformation`. |
| `createdt` | Not in 33-column list | Lookup endpoints | Definitions PDF: "Date that recall was first posted." Likely distinct from `postedinternetdt` but unclear; lookup-endpoint probe would clarify. |

### 7c. Permanently skipped

- `fieldname` / `newvalue` / `oldvalue` — history-endpoint columns; the history endpoints are empirically empty per Finding L.
- `*indicator` fields (`productdescriptionindicator`, `recallreasonindicator`, `codeinfoindicator`, `distributionpatternindicator`) — UI metadata, not content. Per Definitions PDF: "Indicator associated with [the field] that identifies when the truncated version is displayed on the search results and event/product details pop-up screen and where an option to expand to view the full text is given (i.e., 'more...')."
- `productlmd` — empirically null on every probed surface per Finding K0.1.
- `rid` — per-page positional index (sample: 1, 34, 3 across distinct rows), not record-stable. Capturing would force a `content_hash` churn on every refetch.

## 8. R2 validation status

Cassette inspection (`tests/fixtures/cassettes/fda/test_happy_path_single_page.yaml`, 168 records from one window in April 2026) initially confirmed the three mismappings. Broader R2-corpus validation completed 2026-05-28 across **447 records** from 5 daily payloads (2026-05-05, 05-12, 05-19, 05-25, 05-28) — 177 distinct events, 387 distinct products, 134 distinct firms.

### Confirmations

- **Bug 1 is more impactful than initially scoped.** `DISTRIBUTIONAREASUMMARYTXT` is populated in **100%** of records (max length 1146 chars). Silver's current `recall_event.description ← distribution_area_summary_txt` mapping has been silently surfacing geographic distribution lists ("Distribution in United States to AZ, CA, FL, GA, …") as the user-facing recall description for every single FDA recall ever ingested.
- **`PRODUCTSHORTREASONTXT` is the full-text reason (despite the name).** Max length 1633 chars across the sample. Field-name footnote in §5 is correctly identified.
- **`PRODUCTDESCRIPTIONTXT` is always populated** (0% NULL, max 3993 chars). Bug 3's swap is safe.
- **Date fields are MM/DD/YYYY format, length 10**, consistent.
- **PRODUCTID/RECALLEVENTID:** 387 distinct PIDs across 447 records = ~60 re-extractions of edited products in the sample window (consistent with FDA's edit-detection-via-eventlmd flow).

### Empirical NULL rates for underused captures (§4)

| Field | NULL % | Note |
|---|---|---|
| `center_classification_dt` | 6.7% | Aligns with the 25 'NC' (Not Yet Classified) rows below — classification date is unset pre-classification |
| `determination_dt` | 9.6% | Slightly higher than `center_classification_dt`; some classified records lack a determination date |
| `termination_dt` | **66.2%** | Populated iff `PHASETXT='Terminated'` (151 of 152 — one anomaly worth investigating later) |
| `enforcement_report_dt` | 0.2% | Effectively always populated (1 NULL in 447) |
| `initial_firm_notification_txt` | 4.5% | 6-value enum; see below |
| `voluntary_type_txt` | 0.0% | Always populated; see normalization issue below |

### Low-cardinality enum distributions

**`PHASETXT`** (3 values, 0% NULL) — note the PDF spells "On-going" with a hyphen; the API actually returns "Ongoing":
- Ongoing 265 / Terminated 152 / Completed 30

**`CENTERCLASSIFICATIONTYPETXT`** (4 values, 0% NULL) — **API returns numeric '1'/'2'/'3', not Roman 'I'/'II'/'III'** as the PDF text implies. Plus 'NC':
- '2' 282 / '1' 118 / 'NC' 25 / '3' 22
- **`'NC'` = "Not Yet Classified"** per the PDF's note ("For recalls pending classification, the entry will display as 'Not Yet Classified.'"). The API returns the short code, not the full string.

**`CENTERCD`** (7 values, 0% NULL) — **two surprises, both resolved**:
- CDRH 241 / **HFP 117** / CDER 39 / CVM 38 / CBER 7 / CFSAN 3 / **OCS 2**
- **`HFP` = Human Foods Program**, FDA's 2024 reorg that absorbed CFSAN. The PDF's example centers ("CDRH, CDER, CFSAN") predate this; HFP now dominates food recalls (117 vs 3 CFSAN), suggesting the transition is essentially complete.
- **`OCS` = Office of the Chief Scientist**, which oversees the regulation of cosmetic products and certification of color additives (for cosmetics, food, drugs, and devices). Not a Center in the strict FDA org sense — it's an Office sitting above the Center tier — which is why it didn't appear in the PDF's Center-only enumeration. Two records aligns with the 2-record `Cosmetics` count in `PRODUCTTYPESHORT`.

**`VOLUNTARYTYPETXT`** (2 values, 0% NULL) — **data-quality issue**:
- 'Firm Initiated' 245 / 'Voluntary: Firm Initiated' 202
- Both strings mean the same thing (the PDF only defines voluntary-vs-FDA-requested semantics). FDA is returning two formats for the same underlying value. **No `FDA Requested` / `Mandatory` value appears in this sample** at all — either rare in this window or uses different terminology.
- **Silver should normalize** in the (a) PR — either pick a canonical string or convert to a boolean `voluntary` column. Decision deferred to cross-source consolidation since other sources have similar voluntary/mandatory flags.

**`INITIALFIRMNOTIFICATIONTXT`** (6 values, 4.5% NULL) — clean enum suitable for lifting:
- Letter 237 / Combination 82 / E-Mail 82 / Telephone 19 / Press Release 4 / Other 3

**`PRODUCTTYPESHORT`** (6 values, 0% NULL) — clean enum, cross-source product-category candidate:
- Devices 241 / Food 120 / Drugs 39 / Veterinary 38 / Biologics 7 / Cosmetics 2

### Other observations

- **`RECALLNUM` NULL 6.5%** — closely tracks the 25 'NC' classification rows; not-yet-classified recalls have no `RECALLNUM` assigned. Plus ~4 anomalies. **Cannot use `RECALLNUM` as a guaranteed identifier on landing pages**; need `PRODUCTID` for that.
- **`RID`** appears with 164 distinct values across 447 records (minimum value 1) — confirms RID is query-position-relative per Finding F; not stable across runs.
- **`FIRMLEGALNAM`** max length 68; `FIRMFEINUM` 7-10 chars (variable-length FEI). 134 distinct firms across 139 distinct firm names — five firms have multiple legal-name variations within the sample (likely renaming or whitespace variants — typical Phase 6b RapidFuzz territory).

### Still to do

- [x] Probe HIGH-priority (b) candidates — completed 2026-05-29. `codeinformation` validated (max 205,424 chars). `pressreleaseurl` + `pressreleaseissuedt` returned STATUSCODE 406 (bulk POST datagroup excludes them); lookup-endpoint verdict per §7b. Discovery of Finding K0.2 (sort-in-displaycolumns rule) closed the H1 impersonation-paradox misdiagnosis (see api_observations.md Finding K0.2 for the controlled-test trace).
- [x] Probe MEDIUM-priority (b) candidates — completed 2026-05-29. All firm-address fields validated in a single 14-column sweep. Surfaced `firmstatecd` as a previously-unknown 2-letter state code paired with `firmstateprvncnam`. See §7a table for per-field empirical verdicts.
- [x] `OCS` resolved — Office of the Chief Scientist; cosmetics + color-additive regulation
- [ ] Decide `VOLUNTARYTYPETXT` normalization shape (string canonicalization vs boolean) — deferred to cross-source consolidation

## References

- `src/extractors/fda.py` — `_DISPLAY_COLUMNS` (line 117), page-size penalty comment (line 115), `productlmd` exclusion (line 116), RID semantics (line 257)
- `src/schemas/fda.py` — Pydantic bronze contract
- `dbt/models/staging/stg_fda_recalls.sql` — staging projection
- `dbt/models/silver/recall_event.sql:56-87` — FDA → recall_event mapping (where Bug 1 lives)
- `dbt/models/silver/recall_product.sql:61-81` — FDA → recall_product mapping (where Bugs 2 + 3 live)
- `dbt/models/silver/firm.sql:61-73` — FDA → firm mapping
- `dbt/models/silver/recall_event_firm.sql:53-63` — FDA → recall_event_firm mapping
- `documentation/fda/api_observations.md` — historical findings (Finding D, F, J, K, K0)
