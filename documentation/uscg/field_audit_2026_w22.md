# USCG field audit — 2026 W22

- **Status:** In progress 2026-05-29
- **Scope:** USCG Boating Safety boat-recall data — `https://uscgboating.org/content/recalls.php` listing + `recalls-details.php?id=<recall_number>` details pages. Every documented field vs. what we capture, what silver does with it, and what's missing
- **Methodology:** `documentation/audit/methodology.md`
- **Companions:**
  - Existing USCG findings: `documentation/uscg/scraping_observations.md` (Findings A-S, including Step 3 first-extraction empirical record)
  - Source of truth: USCG's two HTML pages (no PDF, no published API spec); shape reverse-engineered via probes
  - Existing toolkit (referenced, not rebuilt): `scripts/sql/uscg/bronze/explore_first_extraction.sql` (Q1-Q9), `scripts/sql/uscg/bronze/diagnose_rejections.sql`, `scripts/sql/uscg/operations/force_full_walk.sql`, `scripts/uscg/inspect_landing_ndjson.py`
  - Tests + fixtures: `tests/fixtures/cassettes/uscg/` (5 cassettes), `tests/fixtures/uscg/sample_{listing,details,pagination_boundary}_page.html`

### Status note — USCG website reactivated; audit is fully live

The Phase 6 plan's prior "USCG indefinitely deferred (2026-05-09 website outage)" framing (`archive/phase-6-execution-plan.md:8`) is out-of-date per user 2026-05-29: **the USCG website is back up and the project pipeline is fully integrated** (extractors, validators, schemas, cassettes). This audit is on the same footing as the other 4 source audits — not a deferred-state review. §7 decisions are proposed for implementation in the (a) PR; §9 supplementary work is done now (no deferral); §8 actively researches the `boat_type` lookup-table gap.

Pipeline state baseline:
- Silver code in place (USCG branches in `recall_event.sql`, `recall_product.sql`, `firm.sql`, `recall_event_firm.sql`)
- Phase 5d historical seed ran 2026-05-17 (1,763 fetched, 1,512 loaded, 251 quarantined → reduced to ~0 after the year-prefix invariant removal per Finding G)
- `documentation/uscg/scraping_observations.md` Findings A-S are corpus-scale empirical
- The Phase 6 execution plan needs a small touch to remove the deferred-status language; tracked separately

## 1. Source field universe

USCG's source has no published spec — the field list is reverse-engineered via the Step 1 + Step 3 probes documented in `scraping_observations.md`. The audit treats the HTML pages as the spec.

### 1a. Listing-page shape — 6 columns (per Finding A)

Endpoint: `https://uscgboating.org/content/recalls.php?pageNum_allRecalls=N` (N = 0..70 for the 1,763-record / 25-per-page corpus; N=71 is the empty-placeholder pagination boundary per Finding L).

| Column header | Schema name | Type | Notable |
|---|---|---|---|
| `Number` | `source_recall_id` | text (year-prefix encoded, e.g. `26MF0158`, `25CG0017`) | Year-prefix is NOT a stable invariant — Finding G falsified the hypothesis (12.4% violations). Use `opened_on` directly |
| `MIC` | `mic` | text (2-4 chars, "Manufacturer Industry Code") | Structured firm identifier. Sometimes NULL (~6.8% per Finding S null-MIC = 120/1763) |
| `Company Name` | `company_name` | text | Listing-side label is `Company Name`; details-side is `Company:` (Finding B label inconsistency). Sometimes NULL (~1.9% per Finding P) |
| `Model Name` | `model_name` | text, may be empty | Boat model name. Truncated in listing; details version is full-length (Finding A note) |
| `Problem 1` | `problem_1` | text, may be empty | Primary defect narrative. ~42% empty in Step 1 sample (16/38) |
| `Opened On` | `opened_on` | date `YYYY-MM-DD` | **`1970-01-01` is USCG's listing-side sentinel for "no date known"** — 82 recalls (4.7%) per Finding O. Silver maps sentinel → NULL |

Each listing row's first cell wraps an anchor to `recalls-details.php?id=<recall_number>`; the `id` parameter is the recall number itself.

### 1b. Details-page shape — 18 labeled fields (per Finding B)

Endpoint: `https://uscgboating.org/content/recalls-details.php?id=<recall_number>`. Returns small HTML (~3-4 KB) structured as `<strong>Label[:]</strong> ... <span class="defaultFont">value</span>` pairs. The 18 fields below are observed across two samples (`26MF0158` and `25CG0017`):

**Redundant with listing (5):** `Number`, `MIC:`, `Company:`, `Model Name:`, `Problem 1:` — same value, sometimes different label or format.

**Distinct details-only (13):**

| Label on page | Schema name | Sample value | Notable |
|---|---|---|---|
| `Company Official:` | `company_official` | `jlu` (initials) or empty | Often empty |
| `Model Year:` | `model_year` | `2025` or empty or multi-year string | Bronze keeps as string (multi-year format possible) |
| `Problem 2:` | `problem_2` | empty in both samples | Per Finding B; secondary defect narrative |
| `HIN` | `hin` | `NLPEC117K425` or `N/A` | Hull Identification Number — the **boat-recall analog of UPC** for consumer products. `N/A` is a documented sentinel for "not applicable" |
| `Case Open Date:` | `case_open_date` | `3/3/2026` | Same date as listing's `Opened On` but **`M/D/YYYY` format** (Finding F) |
| `Disposition:` | `disposition` | `Open` / `Closed` / `CLOSED` / `OPEN` | **Case-inconsistent across corpus** (Finding R): 83.7% `Closed`, 10.8% `Open`, 5.4% `CLOSED`, 0.1% `OPEN`. Silver lowercases |
| `Case Close Date:` | `case_close_date` | `7/23/2025` or empty | `M/D/YYYY`; populated for closed cases |
| `Units` | `units` | `20`, `401` | Count of affected hulls; bronze keeps as string per ADR 0027, silver casts to integer |
| `Campaign Open Date` | `campaign_open_date` | `3/23/2026` | `M/D/YYYY` |
| `Boat Type` | `boat_type` | `00` or empty | Numeric type code (semantics undocumented — see §5 + §9 for lookup-table gap) |
| `Campaign Close Date` | `campaign_close_date` | empty in both samples | `M/D/YYYY` |
| `Severity:` | `severity` | empty in both samples | Lifecycle/risk category — empirical rate TBD (§9 gap) |
| `Last Date:` | `last_date` | `3/23/2026`, `12/2/2025` | `M/D/YYYY`. Finding E: NOT a render-time timestamp (Finding D byte-stability proves it); likely "last editorial change" date |

### 1c. HTTP response posture (per Findings H, J, K, M, N)

- **No `robots.txt`** — 404 (Finding H). Polite-scraper conventions apply by convention: UA with contact email, throttle, serial walks, no concurrent fetches
- **No `Last-Modified`, no `ETag`** (Finding K) — explicit opt-out via `Cache-Control: no-store, no-cache`. No HTTP-level short-circuit
- **`Records Found: NNNN` total** present on every page (Finding J) — combined with page-0 row IDs, this is a steady-state short-circuit oracle (drops weekly run cost from ~36 min to ~3 sec). Implemented at `_should_short_circuit` per Step 6
- **PHP session cookie** discarded per fetch (Finding M) — fresh `httpx.Client` per request
- **Two date formats coexist** (Finding F): listing uses `YYYY-MM-DD` for `Opened On`; details uses `M/D/YYYY` for all 5 date fields. Distinct Pydantic validators (`_UscgListingDate`, `_UscgDetailsDate`)

## 2. Current bronze capture (`uscg_recalls_bronze` — 19 domain fields + lineage)

Per `src/schemas/uscg.py` (`UscgRecallRecord`). All 19 documented fields land in bronze; `details_url` added for lineage (excluded from `content_hash` per `hash_exclude_fields`). Pydantic `extra='forbid'` + `strict=True` catches a new HTML field or relabeled label.

| Bronze column | Source field | Notes |
|---|---|---|
| `source_recall_id` | listing `Number` (alias) | Required. Stable per Finding C two-sample sanity check |
| `mic` | listing `MIC` + details `MIC:` (normalized to one key by extractor) | Nullable per Finding S (120/1763 = 6.8% NULL) |
| `company_name` | listing `Company Name` + details `Company:` (normalized) | Nullable per Finding P (33/1763 = 1.9% NULL) |
| `model_name` | listing `Model Name` + details `Model Name:` | Nullable. Listing truncates; details is full-length |
| `problem_1` | listing `Problem 1` + details `Problem 1:` | Nullable. ~42% empty in Step 1 sample |
| `opened_on` | listing `Opened On` | Nullable per Finding A defensive widening + Finding O sentinel handling |
| `details_url` | constructed (`recalls-details.php?id=<recall_number>`) | Excluded from `content_hash`. Lineage only |
| `company_official` | details `Company Official:` | Nullable. Often initials |
| `model_year` | details `Model Year:` | Nullable. Multi-year format possible |
| `problem_2` | details `Problem 2:` | Nullable. Often empty per Finding B |
| `hin` | details `HIN` | Nullable. `N/A` sentinel — silver normalizes |
| `case_open_date` | details `Case Open Date:` | Nullable. `M/D/YYYY` |
| `disposition` | details `Disposition:` | Nullable. Case-inconsistent per Finding R |
| `case_close_date` | details `Case Close Date:` | Nullable |
| `units` | details `Units` | Nullable. Text at bronze (ADR 0027); silver casts to integer |
| `campaign_open_date` | details `Campaign Open Date` | Nullable |
| `boat_type` | details `Boat Type` | Nullable. Numeric type code |
| `campaign_close_date` | details `Campaign Close Date` | Nullable |
| `severity` | details `Severity:` | Nullable. Empirical population TBD |
| `last_date` | details `Last Date:` | Nullable. Per Finding D + E, treated as legitimate date (not render-time) |

**Bronze captures everything the two HTML pages serve.** No capture gaps. Storage-forced transforms only (date parsing for two formats; ADR 0027). Value-level normalization (Finding O sentinel, Finding R case-fold, ADR 0027 empty-string → NULL, units integer cast) deferred to staging.

**Bronze identity:** `identity_fields=("source_recall_id",)` per Step 2 design. `content_hash` excludes `details_url` (defense against URL-scheme rewrite). Per Finding D byte-stability + Finding E "last_date is not render-time" inference, no other hash-exclude fields needed.

**Staging (`stg_uscg_recalls.sql`)** — latest-per-source_recall_id projection. Applies:
- Finding O sentinel: `case when opened_on = timestamp '1970-01-01 00:00:00+00' then null else opened_on end` + `coalesce(case_open_date, ...)` for `announced_at`
- Finding R case-fold: `lower(nullif(disposition, ''))`
- ADR 0027 empty-string → NULL: `nullif(col, '')` on every nullable text field
- units text → integer via regex guard: `case when units ~ '^[0-9]+$' then units::integer end`

## 3. Mismappings (silver — fixable in the (a) PR)

Three findings. Bug 1 + Bug 3 are real column-flip fixes. Bug 2 is informational (matches a documented design pattern across 1:1-grain sources).

### Bug 1 — `recall_product.model` AND `recall_product.product_name` both = `model_name` (duplicative)

`dbt/models/silver/recall_product.sql:144-149` (USCG branch):

```sql
uscg_products as (
    select
        ...
        model_name                                    as product_name,
        coalesce(problem_1, problem_2)                as product_description,
        model_name                                    as model,        -- ← duplicates product_name
        boat_type                                     as type,
        ...
```

`recall_product.product_name = model_name` AND `recall_product.model = model_name`. Same value in both columns.

Cross-source check:
- **CPSC** `product_name = products[].Name`, `model = products[].Model` (distinct)
- **FDA** `product_name = product_description_txt` (Bug 2 fix), `model = NULL` (no analog)
- **USDA** `product_name = title`, `model = NULL` (no analog)
- **NHTSA** `product_name = compname` (component name), `model = modeltxt` (vehicle model — Bug 3 informational gap)
- **USCG** currently both `= model_name`

USCG has no separate "boat-part-model" concept distinct from the boat model itself. The boat model name belongs in `product_name`; `recall_product.model` should be NULL for USCG (matching FDA + USDA behavior).

**Fix:** `recall_product.model = NULL` for USCG; keep `product_name = model_name`.

### Bug 2 (informational) — `recall_event.description` AND `recall_product.product_description` both = `coalesce(problem_1, problem_2)`

`recall_event.sql:184` and `recall_product.sql:146`:

```sql
-- recall_event:
coalesce(problem_1, problem_2)                                 as description,
-- recall_product:
coalesce(problem_1, problem_2)                as product_description,
```

Same value at both grains. **This matches the documented design choice for 1:1-grain sources** — USDA does the same thing (`recall_event.description = summary`, `recall_product.product_description = product_items`, both pulled from the single recall row). For USCG with one product per recall, separating event-level vs product-level description has no semantic content.

**No code change recommended.** Document in `_silver.yml` per USCG branch so future readers know it's intentional design parity, not a bug. Same shape as USDA Bug 3 (informational) and CPSC's defensible 1:1 cases.

### Bug 3 — `firm.raw_name = coalesce(mic, company_name)` conflates a 3-char ID code with a firm name

`dbt/models/silver/firm.sql:99-110` (USCG branch):

```sql
uscg_normalized as (
    -- Firm anchor = coalesce(mic, company_name) per Finding S. company_id
    -- is mic when populated (USCG's structured Manufacturer Industry Code,
    -- a 3-character alpha identifier), null otherwise.
    select distinct
        'manufacturer'                                  as role,
        coalesce(mic, company_name)                     as raw_name,
        upper(trim(coalesce(mic, company_name)))        as normalized_name,
        mic                                             as company_id
    from {{ ref('stg_uscg_recalls') }}
    where coalesce(mic, company_name) is not null
      and trim(coalesce(mic, company_name)) <> ''
),
```

`MIC` is a 3-character structured **identifier code** (e.g., `YDV`, `NLP`, `123`); `company_name` is a free-text **firm name** (e.g., `VOLVO GROUP / VOLVO PENTA`, `BOMBARDIER RECREATIONAL PRODU`). Treating them as interchangeable `raw_name` sources is a semantic conflation:

- A firm with `mic='YDV'` and `company_name='BOMBARDIER RECREATIONAL PRODU'` produces `firm.normalized_name = 'BOMBARDIER RECREATIONAL PRODU'` (company_name wins because it's listed after mic in coalesce — wait, **mic wins because coalesce returns the FIRST non-null**). So a firm with both populated ends up with `raw_name = 'YDV'`, a 3-char code, not the company name.

Per Finding P + Finding S empirical breakdown:
- 33 rows total with NULL `company_name`
- 120 rows total with NULL `mic`
- **23 rows with BOTH NULL** (firm.sql + recall_event_firm.sql WHERE-clause filter drops these)
- **10 rows (33 - 23) with NULL `company_name` but populated `mic`** — these get `raw_name = mic` (the 3-char code as the firm name)
- **97 rows (120 - 23) with populated `company_name` and NULL `mic`** — these correctly get `raw_name = company_name`
- 1,610 rows (1,763 - 23 - 10 - 97 = 1,633 actually — but precise breakdown depends on overlap math) with BOTH populated — **these get `raw_name = mic` (the code, not the name), because coalesce returns the first non-null**

That last bullet is the problem at scale. ~92% of corpus rows have both fields populated, and silver puts the 3-char MIC code in `raw_name`, not the human-readable company name.

**Fix:**

```sql
uscg_normalized as (
    select distinct
        'manufacturer'                          as role,
        company_name                            as raw_name,            -- name only
        upper(trim(company_name))               as normalized_name,
        mic                                     as company_id           -- ID only
    from {{ ref('stg_uscg_recalls') }}
    where company_name is not null
      and trim(company_name) <> ''
),
```

Trade-off: the 10 mic-only-no-name rows lose their firm dim entry. But:
- The `mic` value is preserved in `recall_event.source_payload_raw.mic` for those recalls
- Per Finding S option 3 (soft-fail), `recall_event_firm.firm_id = NULL` for those recalls is the cleanest tradeoff for v1
- A landing page can still render "MIC: YDV (manufacturer name unknown)" from `source_payload_raw`

**Recommended.** Cross-source firm rollup quality improves materially: a Bombardier landing page actually shows "Bombardier" instead of "YDV"; FDA + USDA + USCG firm rollup against `firm.normalized_name = 'BOMBARDIER...'` works correctly.

#### Update 2026-05-30 — Phase 5d Step 7 directory enrichment supersedes the soft-fail in part

The Step 7 USCG manufacturers directory ingestion (16,263 manufacturer records scraped from `https://uscgboating.org/content/manufacturers-identification.php`) provides a strictly-better rescue path than the original company_name-only proposal. Implementation: `dbt/models/silver/firm.sql` USCG branch (+ `recall_event_firm.sql` USCG branch kept in lockstep) does a case-insensitive LEFT JOIN to `stg_uscg_manufacturers` on `upper(trim(mic))` with 3-way coalesce priority `coalesce(directory.company_name, recalls.company_name, recalls.mic)`.

**Empirical outcome (corpus-scale `dbt build` + measurement script run 2026-05-30):**

- **§3 Bug 3 mic-only-no-name rescue count: 5** (within the audit's predicted 0-10 range). Rescued MICs: `BLB → BRUNSWICK FAMILY BOAT CO INC`, `BRP → BAYRIPPER LLC`, `CRC → BRUNSWICK BOAT GROUP`, `MHB → MCBC HYDRA BOATS LLC (DBA)`, `PCM → PLEASURECRAFT ENGINE GROUP`. The other 5 of the original 10 mic-only-no-name rows have MICs that aren't in the directory (likely truly-retired regulatory codes); they still fall through to the soft-fail per Option 3.
- **General canonical-name enrichment: ~18 USCG firms collapsed** in the firm dim (Q7b went from 749 → 731 reachable firms after the directory canonicalization replaced stale recall-time names with current directory names). This is a Bonus over the original Bug 3 scope — recalls with both `mic` and `company_name` populated now use the directory's canonical name when available.
- **Cross-source coverage: 99.44%** (714 of 718 distinct recall MICs resolve to a directory entry). 4 unresolvable orphans, all real: `111` (retired regulatory code per Finding I, 30 recall rows reference), `999` (17 rows, sentinel), `777` (1 row, sentinel), `N/A` (1 row, literal "N/A" string).
- **Case-sensitivity finding (silver-only):** USCG recalls bronze contains 7 distinct lowercase MIC values (`cec`, `blb`, `kis`, `lbb`, `ser`, `vky`, `zep`) that are case-mismatched against the directory's all-uppercase canonical form. The case-insensitive JOIN handles them cleanly; bronze still preserves verbatim per ADR 0027.

Measurement script: `scripts/sql/uscg_manufacturers/silver/measure_rescue_and_coverage.sql`. Implementation files: `dbt/models/silver/firm.sql:99-130` (USCG branch with directory LEFT JOIN) + `dbt/models/silver/recall_event_firm.sql:85-103` (USCG branch in lockstep). Plan: `project_scope/archive/phase-5d-uscg-manufacturers.md`. Empirical observations: `documentation/uscg/manufacturer_scraping_observations.md` §L.

The "Option 3 soft-fail" decision still applies for the 5 mic-only-no-name rows that DON'T have a directory entry. Phase 6b can revisit if those retired-MIC rows become recoverable via a synthetic-anchor strategy.

## 4. Underused captures — lift from `source_payload_raw` JSONB to first-class silver columns

USCG bundles 15 fields into `recall_event.source_payload_raw` JSONB at `recall_event.sql:190-206` + more in `recall_product.source_specific_attrs`. Several are landing-page-critical or cross-source-alignable:

| Bronze field | Current placement | Proposed silver column | Why lift |
|---|---|---|---|
| `severity` | `recall_event.source_payload_raw.severity` | `recall_event.severity` (text — case-normalized via `upper()`) | **Confirmed lift per 2026-05-29 Q1: 77% populated** (NOT empty-by-source as Finding B's 2-probe sample suggested). 4-value enum: H (38.23%), L (34.94%), M (1.42%), S (0.11%). NULL 23.14%. **Case-inconsistency parallel to Finding R: 37 lowercase rows (`l`=35, `h`=2 — silver must `upper()`)** + 1 data-quality outlier (`1`). Cross-source-aligned with FDA `centerclassificationtypetxt` and USDA `recall_classification`/`risk_level`. Enum meanings undocumented — email USCG OII alongside the boat_type ask |
| `hin` | `recall_product.source_specific_attrs.hin` | `recall_product.hin` (text) — **with `N/A` → NULL normalization** | **Hull Identification Number = the boat-recall analog of UPC** for consumer products / VIN for vehicles. **2026-05-29 Q2: 52.81% populated (real HIN like `NLPEC117K425`), 1.19% `N/A` sentinel, 46.00% NULL/empty.** Silver normalize: `case when hin = 'N/A' then null else hin end`. Cross-source-alignable with `recall_product.upc` (currently NULL for USCG); a future cross-source enrichment could merge HIN + UPC + VIN into a `product_identifier` column. Lift to `recall_product.hin` (or directly to `recall_product.upc` for cross-source consolidation — defer naming to that pass) |
| `case_close_date` | `recall_event.source_payload_raw.case_close_date` | `recall_event.terminated_at` (timestamptz) | Cross-source-alignable with FDA's `terminationdt`. Closed-recall lifecycle date — useful for landing-page rendering ("Recall closed on...") |
| `campaign_open_date` | `recall_event.source_payload_raw.campaign_open_date` | `recall_event.campaign_started_at` (timestamptz) | Distinct from `case_open_date` (=announced_at) — when the recall *campaign* (consumer-facing remediation) began. Worth lifting for landing pages |
| `campaign_close_date` | `recall_event.source_payload_raw.campaign_close_date` | `recall_event.campaign_ended_at` (timestamptz) | Same pattern |
| `last_date` | `recall_event.source_payload_raw.last_date` | `recall_event.last_editorial_date` (timestamptz) | Already used in `published_at = coalesce(last_date, announced_at)`. Per Finding E, treated as legitimate lifecycle date (USCG's "last editorial change"). Worth exposing separately so consumers don't have to derive from JSONB |
| `disposition` | already drives `recall_event.status` synth + in `source_payload_raw` | `recall_event.disposition` (text — `'open'`/`'closed'` after Finding R lowercase) — keep alongside synthetic `status` | The synthetic `recall_event.status` maps `'open' → 'active'`/`'closed' → 'closed'`. Consumers querying `WHERE status = 'active'` work fine, but exposing the raw disposition lets landing pages display "Disposition: Open" verbatim |
| `model_year` | `recall_product.source_specific_attrs.model_year` | `recall_product.model_year` (text — multi-year format possible) | Cross-source-alignable with NHTSA's `yeartxt`. Useful for boat-year-search landing pages |
| `company_official` | `recall_event.source_payload_raw.company_official` | `recall_event.firm_contact_person` (text) — *low priority* | Often just initials (`jlu`, `PDE`). Niche but available; cross-source-alignable with FDA's firm-contact lifts |
| `units` | already in `recall_product.number_of_units` (integer) | already lifted — no change | Cross-source-aligned |
| `boat_type` | already in `recall_product.type` (numeric code) | already lifted — but **lookup-table gap**, see §5 | Numeric codes (`00`, etc.) have no documented decoding; semantics-unknown |
| `details_url` | already in `recall_event.url` | already lifted — no change | Cross-source-aligned |
| `problem_2` | `recall_event.source_payload_raw.problem_2` + `recall_product.source_specific_attrs.problem_2` | leave in source_payload_raw | Per Finding B sample "empty in both samples"; in current silver `coalesce(problem_1, problem_2)` handles fallback. Promote only if empirical rate shows it's regularly distinct from problem_1 |
| `case_open_date` | already drives `announced_at` | already lifted — no change | Cross-source-aligned |
| `mic` | `recall_event.source_payload_raw.mic` + via Bug 3 fix → `firm.company_id` | partially lifted; via Bug 3 fix flows correctly | Per §3 Bug 3 fix |

**Cross-source naming alignment notes (forward-looking, deferred to cross-source consolidation):**

| Cross-source column | USCG mapping |
|---|---|
| `recall_event.description` (defect narrative) | `coalesce(problem_1, problem_2)` |
| `recall_event.corrective_action` / `remedy_text` | (USCG has none — no remedy field on details page) |
| `recall_event.consequence_of_defect` | (USCG has none) |
| `recall_event.recall_initiator` | (USCG has none — no analog to NHTSA `influenced_by` or FDA `voluntarytypetxt`) |
| `recall_product.type` | `boat_type` (numeric code, see §5) |
| `recall_event.classification` | (USCG has `severity` — see §4 lift candidate above) |
| `recall_product.hin` / `upc` | `hin` (per §4 lift; the boat-recall analog) |

## 5. Field-naming gotchas

Most of these are already documented in `scraping_observations.md` Findings A-S. Cross-referenced here for the audit-doc completeness pattern.

| Surface | Gotcha | Notes |
|---|---|---|
| **Two date formats** | Listing `Opened On` uses `YYYY-MM-DD`; details `Case Open Date` + 4 others use `M/D/YYYY` | Finding F. Distinct Pydantic `BeforeValidator`s per format |
| **1970-01-01 listing sentinel** | USCG renders `1970-01-01` for "no opened date" on listing; details leaves Case Open Date empty | Finding O — 82 recalls (4.7% of corpus). Same logical state, two encodings. Silver `stg_uscg_recalls.sql` maps sentinel → NULL + coalesces details over listing |
| **Disposition case-inconsistent** | `Closed` 83.7%, `Open` 10.8%, `CLOSED` 5.4%, `OPEN` 0.1% | Finding R. Silver `lower()` normalizes; `accepted_values: ['open', 'closed']` in stg yml |
| **Severity case-inconsistent** (parallel to Finding R) | **2026-05-29 Q1: 37 lowercase rows** (`l`=35, `h`=2) alongside uppercase `H`/`L`/`M`/`S` | Same shape as disposition. Silver staging should `upper(nullif(severity, ''))` + add `accepted_values: ['H', 'L', 'M', 'S']` to `stg_uscg_recalls.yml`. Plus 1 data-quality outlier (`severity = '1'`) worth `severity=warn` flagging |
| **Narrative fields capped at 25 chars by source** | **2026-05-29 Q4: problem_1 max=25, p95=25; problem_2 max=25, p95=25.** Average 17-18 chars | **Major landing-page implication.** USCG narratives are effectively snippets, not full defect descriptions. Cross-source comparison: CPSC `Description` max 5,983 chars, NHTSA `desc_defect` max 794 chars, USCG `problem_1` max 25 chars. The 25-char cap is on the USCG website itself (both listing AND details page per Finding B "details truncates at same length"), not a parser bug. FastAPI landing pages should consider linking out to the USCG details URL for the full context. Worth flagging to USCG OII as an information-completeness gap |
| **model_year format diverse** | **2026-05-29 Q5: 59.73% single 4-digit (`2025`-style), 5.28% 2-digit (`14`, `97` — needs century inference in silver), 1.82% range/list (`06-08`, `99 &2000`), 0.62% other numeric, 0.11% non-numeric (`AFTER 1`, `ALL`).** Plus `9999` sentinel observed in single-year bucket | Silver staging needs: (a) pad 2-digit to 4 with century inference (`<=29` → 2000+, `>=80` → 1900+, 30-79 either); (b) keep range/list as-is or parse to JSONB array; (c) map `9999` → NULL (parallel to NHTSA yeartxt = 9999 sentinel); (d) leave free-text as-is. Currently `stg_uscg_recalls.sql` passes model_year through verbatim |
| **`boat_type` "0" vs "00" inconsistency** | **2026-05-29 Q3: 7 rows have `0`, 46 have `00`.** USCG ships both encodings for what's almost certainly the same code | Silver may want to normalize `lpad(boat_type, 2, '0')`. Worth confirming with USCG OII alongside the lookup-table request |
| **MIC value `111` violates HIN spec** | **2026-05-29 Q6: 12 rows have `mic = '111'` → Volvo Penta.** Per HIN spec + USCG-2013-0133-0005 attachment, MIC must be 3 alpha chars ("characters four through eight are a manufacturer serial number consisting of letters of the English alphabet (except 'I', 'O' and 'Q')" — but MIC itself is letters per the spec, with "Z" for state-issued backyard-builders) | Data quality issue. Likely USCG misencoded or used a placeholder. Worth flagging to USCG OII. Silver firm dim already `upper(trim())` normalizes; `111` → firm row with normalized_name `111` is anomalous but doesn't break anything |
| **HIN `N/A` sentinel** | Per schema docstring + Finding B: `hin` may be `"N/A"` — silver normalizes | The §4 lift to `recall_product.hin` needs `case when hin = 'N/A' then null else hin end` |
| **Year-prefix invariant FALSIFIED** | `source_recall_id[:2]` does NOT reliably encode year — 218 of 1,763 (12.4%) violate it | Finding G. Four mechanisms: fiscal-year prefixes, filing-vs-opened workflow (~100+ rows), multi-year offsets for re-issued recalls, the 1970 sentinel cluster. Invariant *removed entirely* from extractor; use `opened_on` directly |
| **Label inconsistencies** | Listing `Company Name` vs details `Company:`; listing `Opened On` vs details `Case Open Date`; listing column `Problem 1` (no colon) vs details `Problem 1:` | Finding A + B. Extractor normalizes both label forms to one bronze key |
| **`MIC` vs `company_name` semantics** | MIC = 3-char structured ID; company_name = free-text firm name. Different concepts; current silver conflates | Per §3 Bug 3 |
| **`boat_type` numeric code with no documented enum** | Sample values: `00`, empty. No published lookup table. Bronze + silver pass the numeric code through verbatim | **Capture-expansion gap — researched 2026-05-29 + documented in §8.** Two USCG-authored PDFs (`documentation/uscg/USCG-2013-0133-0005_attachment_1.pdf`, `documentation/uscg/NRBSS-Exposure-Survey-Final-Report-20201130-v3.0.pdf`) + web searches did not surface a public code → semantic-name lookup. **NRBSS PDF §2.1 page 8 gives the official USCG verbal taxonomy** (13 categories: open power boat, cabin power boat, pontoon boat, air boat, houseboat, PWCs, auxiliary sail boat, sail boat, canoe, kayak, paddleboard, rowed boat, other) — but no numeric mapping. Path forward: §9's `inspect_field_population.sql` Q3 surfaces the actual codes in current bronze; email USCG OII with the observed code list to request the lookup table |
| **HTML entity preservation** | Narrative fields preserve `&amp;`, `&#xN;` etc. verbatim per ADR 0027 | Finding I. Silver staging or landing-page renderer must decode |
| **Non-UTF-8 bytes** | At least one listing page has byte `0xbc` (Latin-1 `¼`) embedded in UTF-8-declared content | Finding Q. BeautifulSoup + lxml handles transparently via encoding auto-detection; inspector uses `errors="replace"` |
| **No `Last-Modified`, no `ETag`** | `Cache-Control: no-store, no-cache` explicitly opts out | Finding K. Page-level `Records Found: NNNN` total + page-0 row IDs is the short-circuit oracle (Finding J → `_should_short_circuit` implementation) |
| **PHP session cookie discard** | First GET sets `PHPSESSID`; never persisted | Finding M. Fresh `httpx.Client` per fetch |
| **`units` is TEXT at bronze, INTEGER at silver** | Per ADR 0027 storage-forced transforms only at bronze; silver casts via regex guard `case when units ~ '^[0-9]+$' then units::integer end` | Defensive — handles a future thousands-separator or range format gracefully (lands as NULL in silver until a schema decision adds parsing) |
| **`last_date` NOT a render timestamp** | Per Finding D byte-stability + Finding E inference: `last_date` is "last editorial change", not server-render | Treated as legitimate lifecycle date; included in `content_hash`. Revisit per Finding E if a previously-stable recall's `last_date` advances without other changes |
| **Pagination boundary = empty placeholder** | Page 71 returns `<a href="recalls-details.php?id=">` (empty id) — stop signal. NOT a 404 | Finding L. Drift-guard: abort walk if page-count exceeds 200 |
| **Website outage** | 2026-05-09 USCG website outage → USCG indefinitely deferred per Phase 6 plan | Current bronze freshness = whatever was captured before the outage. ~1,763 records from 2026-05-17 historical seed. Phase 6a.5 explicitly excludes USCG from backfill (`archive/phase-6-execution-plan.md:85`) |

## 6. The firm-relationship question — MIC + company_name + no FEI analog (structural)

USCG has **two firm-related fields** with different semantic meanings:

- **`mic`** (3-char structured identifier; analogous to FSIS establishment_number / FDA FEI / NHTSA — has-no-analog)
- **`company_name`** (free-text firm name; analogous to USDA `field_establishment` / FDA `firm_legal_nam` / NHTSA `mfgname` / CPSC `Manufacturers[].Name`)

Unlike CPSC (where CompanyID is empirically empty per CPSC §3 Bug 3) and NHTSA (which has no structured ID at all), USCG's MIC is mostly populated (93.2% per Finding S: 1,643/1,763 non-NULL). This puts USCG in the same tier as USDA + FDA for structured-ID coverage.

**Cross-source ID anchor inventory updated:**

| Source | Structured ID | Coverage | Anchor quality |
|---|---|---|---|
| FDA | `firm_fei_num` | High | Strong anchor |
| USDA | `establishment_number` | 100% per audit | Strong anchor |
| **USCG** | **`mic`** | **93.2% per Finding S** | **Strong anchor** |
| NHTSA | None | — | RapidFuzz only |
| CPSC | `CompanyID` (empirically empty) | 0% | None |

A future `firm_identifier` table (Phase 6b territory) would draw from FDA + USDA + USCG. CPSC + NHTSA contribute via name-only matching.

### Bug 3's fix interacts with the §6 architecture

Under the §3 Bug 3 fix:
- `firm.raw_name = company_name` (free-text name)
- `firm.company_id = mic` (structured ID)
- Cross-source `firm` dim collapses correctly when company names match across sources
- The 10 mic-only-no-name rows (mic populated, company_name NULL) lose their firm dim entry; their `mic` lives in `recall_event.source_payload_raw.mic` for landing-page rendering

This is the Phase 6b-aligned architecture — same shape as USDA's `firm.company_id = establishment_number` + `firm.raw_name = establishment_name` design.

### Update 2026-05-30 — Phase 5d Step 7 adds directory enrichment as a third firm-source

Step 7 ingests the USCG manufacturer directory (16,263 records) as a sibling non-recall source `uscg_manufacturers` — paralleling the USDA `usda_establishments` design. Silver lands `stg_uscg_manufacturers` (staging) + `firm_manufacturer_attributes` (per-MIC dim, sibling to `firm_establishment_attributes`). The directory becomes a third firm-name source for `firm.sql`'s USCG branch alongside recalls and the raw `mic` fallback (`coalesce(directory.company_name, recalls.company_name, recalls.mic)` priority).

**Cross-source ID anchor coverage rises materially:** the 99.44% recalls→directory match rate (714/718 distinct USCG recall MICs resolve) puts USCG's structured-ID coverage at parity with USDA when measured corpus-wide. The 4 orphans (`111`, `999`, `777`, `N/A`) are real retirements/sentinels, not coverage gaps. Phase 6b firm-resolution gains a richer anchor: a recall referencing `mic='YDV'` can now resolve to both the structured MIC AND the canonical USCG-registered company name in a single JOIN, with the regulatory address as bonus enrichment.

The case-insensitive JOIN (`upper(trim(r.mic)) = upper(trim(m.mic))`) mirrors the USDA precedent at `firm.sql:82-83`. Both `firm.sql` and `recall_event_firm.sql` USCG branches use the identical computation; a comment on each enforces "keep these two CTEs in lockstep" so future changes don't recreate the firm_id orphan problem we surfaced and resolved during Step 5/6 (1549 orphans → 0).

### USCG-specific firm-name patterns (Phase 6b name-cleaning inputs)

Listing-side samples surface several patterns Phase 6b should consider:

- **Truncation in listing** — `BOMBARDIER RECREATIONAL PRODU` (Finding O sample) vs full-length on details. Bronze captures details version verbatim per Step 1 design
- **Multi-brand corporate naming** — `VOLVO GROUP / VOLVO PENTA` (Finding A sample). Forward-slash-separated brand pair
- **All-caps** — `BOMBARDIER`, `VOLVO GROUP` — different from NHTSA's mixed-case `mfgname` / `mfgtxt`. Same as NHTSA's `maketxt` casing
- **Corporate-form suffix patterns** — likely similar to NHTSA (`Inc.`, `Corp.`, `LLC`) but smaller corpus may surface fewer variants

Phase 6b NHTSA suffix-strip work (per `project_scope/archive/phase-6-execution-plan.md` § Phase 6b → CPSC firm-name normalization) should generalize to USCG with the same regex patterns; corpus-scale validation deferred to website-reactivation + Phase 6b empirical pass.

## 7. Decisions locked in (confirmed 2026-05-29)

All items below confirmed by user 2026-05-29: "Option 3 soft-fail for §3 Bug 3 firm fix; §4 lift list is good". USCG is reactivated (not deferred); implementation lands in the (a) PR alongside the other 4 sources' audit fixes.

1. **Bug 1 fix: `recall_product.model = NULL` for USCG.** Drop the `model_name as model` projection from the USCG branch; keep `product_name = model_name`. Matches FDA + USDA NULL-model pattern; CPSC + NHTSA correctly populate `model` from a distinct field.
2. **Bug 2 — informational, no code change.** USCG's `recall_event.description = recall_product.product_description = coalesce(problem_1, problem_2)` is the documented 1:1-grain-source pattern (matches USDA design). Document in `_silver.yml` USCG branch.
3. **Bug 3 fix: separate MIC from company_name in firm dim.**
   - `firm.raw_name = company_name` (not coalesce(mic, company_name))
   - `firm.company_id = mic` (when populated)
   - `recall_event_firm` filters `where company_name is not null` (instead of `where coalesce(mic, company_name) is not null`)
   - 10 mic-only-no-name rows lose firm dim entry per Finding S option 3 (soft-fail). `mic` still preserved in `recall_event.source_payload_raw.mic` for landing-page rendering
4. **§4 lifts to first-class silver columns** (10 candidates):
   - `severity` → `recall_event.severity` (text, `upper()`-normalized) — **CONFIRMED post-Q1: 77% populated, 4-value enum H/L/M/S + case-inconsistency normalized in silver**
   - `hin` → `recall_product.hin` (text) with `'N/A'` → NULL normalization — the boat-recall analog of UPC. **52.81% populated per Q2**
   - `case_close_date` → `recall_event.terminated_at` (timestamptz)
   - `campaign_open_date` → `recall_event.campaign_started_at` (timestamptz)
   - `campaign_close_date` → `recall_event.campaign_ended_at` (timestamptz)
   - `last_date` → `recall_event.last_editorial_date` (timestamptz)
   - `disposition` → `recall_event.disposition` (text) alongside synthetic `status`
   - `model_year` → `recall_product.model_year` (text — multi-year format possible)
   - `company_official` → `recall_event.firm_contact_person` (text) — low priority
   - Cross-source column names deferred to cross-source consolidation
5. **`boat_type` semantics lookup table — capture-expansion item.** §5 documented gap. When USCG comes back, ask USCG OII for the Boat Type code → semantic-name mapping. Until then, silver passes the numeric code through; landing pages render "Boat Type: 00" verbatim
6. **Documented-empty-by-source decisions resolved post-Q1/Q4 2026-05-29:**
   - ~~`severity` — TBD~~ → **77% populated, lifted (§4 confirmed)**
   - `problem_2` — Q4 confirms populated for 613 rows (34.8%) with same 25-char source-side cap as problem_1. Keep in `coalesce(problem_1, problem_2)` fallback for `recall_event.description`; lift to separate column low-priority
   - `campaign_close_date` — Q5 of `explore_first_extraction.sql` already shows this is part of the date NULL-rate. Lift as proposed (§4 #5) — populated only for closed campaigns
7. **NEW post-empirical decisions from 2026-05-29 SQL:**
   - **Silver staging adds case-normalization for `severity`** (`upper(nullif(severity, ''))`) — Q1 surfaced 37 lowercase rows + 1 outlier
   - **Silver staging adds `'N/A'` → NULL for `hin`** — Q2 surfaced 21 sentinel rows (1.19%)
   - **Silver staging adds `'9999'` → NULL for `model_year`** — Q5 confirmed sentinel parallel to NHTSA yeartxt
   - **Silver staging adds `lpad(boat_type, 2, '0')`** — Q3 surfaced `0`/`00` inconsistency (7 vs 46 rows)
   - **Defer model_year 2-digit padding + range-list parsing to Phase 6/7 enrichment** — Q5 surfaced 5.28% 2-digit + 1.82% range/list; preserves source verbatim per ADR 0027 for now
   - **Email USCG OII consolidated ask:** boat_type lookup table + severity enum semantics + `0`/`00` distinction + MIC `111` data-quality outlier. Single email; track response per Phase 6a (a) PR scope
7. **Cassette philosophy unchanged.** Five cassettes including the large `test_lifecycle_against_real_bytes.yaml` (5,270 lines) + HTML fixtures. Real-content cassettes (different from NHTSA's hand-built-body pattern; more like CPSC/FDA/USDA's real-response approach)
8. **Status quo — short-circuit + watermark behavior unchanged.** `_should_short_circuit` implementation per Finding J + Step 6 is sound; `source_watermarks.last_records_count` + `extraction_runs.was_short_circuited` capture the steady-state behavior

## 8. Capture-expansion items deferred to backlog

USCG is indefinitely deferred. No new capture work proposed.

**Summary by priority:**

- **HIGH — none.** All 18 documented details-page fields + 6 listing-page columns are captured.
- **MEDIUM — none.** Same reason.
- **LOW — `boat_type` lookup table (researched 2026-05-29).** Per §5 + §7 #5: the `Boat Type` numeric-code → semantic-name mapping is not publicly documented. **2026-05-29 research summary:**
  - `documentation/uscg/USCG-2013-0133-0005_attachment_1.pdf` (Jan 2013, OMB 1625-0056): covers HIN/MIC/Maximum Capacities/fuel-tank labeling regulations. Mentions "boat type" only as an OPTIONAL designation a manufacturer might include in their HIN's serial-number portion (chars 4-8); confirms it is NOT a standardized identifier USCG mandates.
  - `documentation/uscg/NRBSS-Exposure-Survey-Final-Report-20201130-v3.0.pdf` (Oct 2020, RTI International for USCG-BSX): provides the official USCG **verbal** taxonomy in §2.1 page 8 — 13 categories: **open power boat, cabin power boat, pontoon boat, air boat, houseboat, PWCs (WaveRunner, Sea-Doo), auxiliary sail boat (sail boat with motor), sail boat (powered only by sails), canoe (incl. inflatable), kayak (incl. inflatable), paddleboard, row(ed) boat (e.g., jon boat, shells, sculls, inflatables), other (kiteboard, dragon boat, etc.)**. But this taxonomy is the NRBSS exposure survey's, not the recall system's numeric encoding.
  - Web searches (uscgboating.org/manufacturers-identification, MISLE casualty forms, CG-2692, AIS vessel-type codes): no recall-system code list found. MISLE has its own vessel-type encoding for commercial vessels; not the same as the recall `Boat Type` 2-digit numeric field.
  - Direct `WebFetch` of a live recall details page (`recalls-details.php?id=26MF0158`) confirms the field renders as bare numeric (`00`) with no tooltip, legend, or decoder visible in the HTML or JavaScript.
  - **Empirical code list — Q3 run 2026-05-29 against 1,763-record bronze:** 25 distinct codes observed (+NULL 35.68% of corpus). Top 4 by population: **`11` 24.90% (439 rows), `12` 9.93%, `13` 6.52%, `17` 3.74%**. Full list: `00` 2.61%, `01` 2.55%, `02` 0.17%, `0` 0.40% (likely synonym of `00`), `11`-`15`, `17`-`22`, `29`, `31`-`34`, `50`, `53`, `54`, `58`. Pattern suggests hierarchical/categorical encoding (10s = one category, 20s = another, etc.) — likely maps onto the NRBSS verbal taxonomy but the specific mapping is unknown.
  - **Recommended path:** email USCG OII (`contact@uscgboating.org` or via the website contact form) with the empirical code list above to request the official lookup table. Suggested email:
    > "We're indexing USCG boat-recall data from `recalls-details.php` and capture the `Boat Type` numeric code field. Our 1,763-record corpus contains these distinct codes (with counts): 11 (439), 12 (175), 13 (115), 17 (66), 31 (54), 00 (46), 01 (45), 32 (38), 22 (28), 21 (26), 19 (26), 14 (15), 18 (15), 15 (12), 34 (8), 0 (7), 20 (5), 33 (4), 02 (3), 50 (3), and singletons 54, 58, 29, 53. NRBSS PDF §2.1 gives the verbal boat-type taxonomy (open power boat, cabin power boat, pontoon boat, etc.) but we can't find the code↔category lookup. Could you share the table or point us to documentation? Also: we observe '0' and '00' as separate values — is that an intentional distinction or formatting variance?"
  - Until received, silver passes the numeric code through verbatim with `lpad(boat_type, 2, '0')` for the `0`/`00` normalization. FastAPI landing pages can render "Boat Type: 11 (USCG code; see USCG documentation)" as a graceful fallback. Reactivation-blocker is removed; the gap is operational, not capture-architectural.
- **SKIP — alternate USCG datasets.** USCG exposes other safety datasets (death/injury reports, boating accident statistics, USCG Auxiliary advisories) at the same `uscgboating.org` domain. These are not boat-product-recall data per the project's recall focus; scope-expansion candidates only if the FastAPI surface adds "boating safety" beyond recalls.
- **SKIP — historical seed via second source.** The 2026-05-17 historical seed pulled the full 1,763-record corpus from the website. No alternate-archive path exists (USCG doesn't publish a `FLAT_RCL` equivalent). If the website reactivates, the existing `recalls deep-rescan uscg` path covers re-extraction.

**Like USDA + CPSC + NHTSA, USCG's (b) capture-expansion PR has zero adds** (modulo the boat_type lookup-table acquisition, which is an enrichment of an existing field, not a new field). The audit's gains are §3 fixes (Bug 1 model duplication + Bug 3 firm conflation) + §4 lifts (10 candidates pending §9 empirical confirmation) + §6 firm-relationship Option fix + §5 documented gotchas.

## 9. R2 validation status — reference existing toolkit, minor gaps

USCG has a smaller existing toolkit than NHTSA (4 SQL scripts + 1 Python inspector vs NHTSA's 18 SQL + 8 Python) but the corpus is also smaller (1,763 records vs NHTSA's ~74,604).

### Existing toolkit

**Bronze SQL:**

| Script | Purpose | Audit §9 use |
|---|---|---|
| `scripts/sql/uscg/bronze/explore_first_extraction.sql` | 9-query batch: extraction_runs summary (Q1), bronze vs rejected sanity (Q2), rejection breakdown by stage + reason (Q3-Q4), **per-field NULL rates for all 18 fields (Q5)**, disposition distribution (Q6 — drove Finding R), top manufacturers (Q7 — drove Finding S null-anchor analysis), opened_on year coverage (Q8), source_recall_id prefix distribution (Q9 — drove Finding G falsification) | **Primary corpus-scale field-rate inspector.** Already covers everything FDA/USDA/CPSC's `inspect_landed_payloads.py` covers for top-level fields. Run after any future USCG re-extraction |
| `scripts/sql/uscg/bronze/diagnose_rejections.sql` | Pydantic + invariant quarantine analysis. Includes the year-prefix heatmap Q6 that drove Finding G | Reference for any future quarantine investigation |
| `scripts/sql/uscg/operations/force_full_walk.sql` | Operator helper: clears `source_watermarks.last_records_count` to force a full walk on the next run (bypasses Finding J short-circuit) | Operational tooling, not audit |

**Python:**

| Script | Purpose |
|---|---|
| `scripts/uscg/inspect_landing_ndjson.py` | R2 byte-level inspection — drove the Finding O sentinel evidence (R2 lines 423-439 showing the literal `1970-01-01`) and the Finding P empty-`Company:` cell evidence (R2 lines 27-35) |

**dbt tests:**

| Test | Purpose |
|---|---|
| `dbt/tests/assert_uscg_row_count_sane.sql` | Row-count assertion — guards against catastrophic shrink (a re-extraction landing < 1,000 rows would be an alarm) |

**Existing documentation as empirical record:**

`documentation/uscg/scraping_observations.md` — Findings A-S already contain corpus-scale empirical numbers:
- Finding A: per-column populated rates (38-row Step 1 sample)
- Finding G: 218/1,763 (12.4%) year-prefix mismatches, with 4 mechanism breakdown
- Finding O: 82/1,763 (4.7%) 1970-01-01 sentinel rate
- Finding P: 33/1,763 (1.9%) NULL company_name
- Finding R: disposition case distribution (1,476 + 190 + 95 + 2)
- Finding S: 23 NULL-both-firm-anchor rows
- The Q1-Q9 of `explore_first_extraction.sql` produces the underlying numbers each Finding cites

### Audit-confirmed at corpus scale (per scraping_observations.md Findings)

The 2026-05-17 first-extraction empirical numbers are already in `scraping_observations.md`. Findings A-S cover everything an FDA/USDA/CPSC/NHTSA-style audit `inspect_landed_payloads.py` would surface for the broader corpus shape (Findings G/O/P/R/S in particular are corpus-scale).

### Audit-specific addition (this audit) — 2026-05-29

Built to fill the gaps that `explore_first_extraction.sql` Q1-Q9 doesn't cover. With USCG reactivated per user 2026-05-29, no reason to defer.

| Script | Purpose | Audit §9 use |
|---|---|---|
| `scripts/sql/uscg/bronze/inspect_field_population.sql` | 8-query batch covering: (Q1) `severity` enum + populated rate, (Q2) `hin` sentinel `'N/A'` + populated breakdown, (Q3) `boat_type` numeric-code distribution, (Q4) narrative-field length distributions for `problem_1` + `problem_2`, (Q5) `model_year` format breakdown (single-year vs multi-year vs other), (Q6) top 30 distinct `(mic, company_name)` pairs, (Q7) per-mic company_name consistency, (Q8) per-company_name mic consistency | **Pending run.** Validates §4 lift NULL rates (`severity`), §5 sentinel rate (`hin 'N/A'`), §5 + §8 boat_type code list (output → email to USCG OII for lookup-table request), §6 firm dim sizing (consistency checks Q7+Q8). Mirrors NHTSA's `inspect_field_population.sql` + `inspect_mfgname_vs_mfgtxt.sql` patterns. |

### SQL findings — 2026-05-29 run (1,763 records, --since=2023-12-01 + 2026-05-17 historical seed)

Run command: `psql -f scripts/sql/uscg/bronze/inspect_field_population.sql`. Bronze corpus is the 2026-05-17 Phase 5d historical seed (full corpus per Finding J) + any incremental since.

**Confirmations:**

- ✅ **§3 Bug 3 sizing confirmed.** Q6 surfaces 23 NULL/NULL rows (Finding S null-firm-anchor count exact match) + Q7/Q8 quantify firm-rollup quality.
- ✅ **§5 + §6 firm-rollup quality empirically strong.** Q7: 83.03% of MICs map to 1 company_name (602/725); 10.21% to 2 (mostly punctuation/spacing variants like `SEA RAY BOATS` vs `SEA RAY BOATS INC`); 4.00% to 3. Q8: 91.29% of companies map to 1 MIC (849/930); 3.98% to 2 (legit multi-plant per HIN spec from USCG-2013-0133-0005); ≤0.65% to 3+. **Phase 6b NHTSA suffix-strip pattern + RapidFuzz will handle the ~10-15% multi-company-name cases cleanly.**
- ✅ **§4 `hin` lift confirmed.** Q2: 52.81% real HIN, 1.19% `N/A` sentinel, 46.00% NULL/empty. Silver normalize `'N/A' → NULL`.

**New findings refining §4 + §5 + §7:**

- 🆕 **§4 reversal: `severity` is 77% populated, not empty-by-source.** Q1 across 1,763 records: H 38.23% / L 34.94% / NULL 23.14% / lowercase outliers 2.10% / M 1.42% / S 0.11% / outlier `1` 0.06%. **Lift confirmed; was incorrectly flagged TBD due to Finding B's 2-probe sample.** Enum meanings undocumented — fold into the USCG OII email alongside boat_type ask.
- 🆕 **§5 new gotcha: severity case-inconsistency** (parallel to disposition Finding R). 37 lowercase rows. Silver needs `upper(nullif(severity, ''))` + `accepted_values: ['H', 'L', 'M', 'S']` test.
- 🆕 **§5 new gotcha: narrative fields capped at 25 chars by source.** Q4: problem_1 max=25, problem_2 max=25, both with p95=25 (i.e., dominant). **Major landing-page implication** — USCG narratives are effectively short snippets. Cross-source: CPSC max 5,983, NHTSA max 794, USCG max 25. FastAPI landing pages must link out to the USCG details URL for full context.
- 🆕 **§5 new gotcha: model_year format diversity** — 59.73% single 4-digit, 5.28% 2-digit, 1.82% range/list, 0.62% other numeric, 0.11% non-numeric (`AFTER 1`, `ALL`), plus `9999` sentinel. Silver staging needs 2-digit padding + range parsing + sentinel mapping.
- 🆕 **§5 new gotcha: boat_type `0` vs `00`** — Q3 surfaces both forms (7 vs 46 rows). Silver `lpad(boat_type, 2, '0')` normalizes.
- 🆕 **§5 + data quality: MIC `111` violates HIN spec** (12 rows → Volvo Penta). Per USCG-2013-0133-0005, MIC must be 3 alpha chars; `111` is numeric. Email USCG OII alongside the lookup-table ask.
- 🆕 **§5 + data quality: `MERCURY MARINE` has NULL MIC for 36 rows** (Q6 row 2). Mercury Marine has its own assigned MIC; these are likely older records or data-entry omissions.
- 🆕 **§3 Bug 3 fix sizing finalized:** 23 NULL-both-anchor rows + ~10 mic-only-no-name rows (33 total NULL-company_name per Finding P, minus 23 NULL/NULL = 10) lose firm dim entry under Option 3 soft-fail. Their MIC value stays in `recall_event.source_payload_raw.mic`; landing-page rendering handles gracefully.

**§8 boat_type empirical list ready for USCG OII** — moved into §8's expanded research summary. The email template is in §8.

### Phase 6a continuation — post-empirical updates to apply

- Update `dbt/models/staging/stg_uscg_recalls.sql`:
  - Add `upper(nullif(severity, ''))` for case normalization
  - Add `case when hin = 'N/A' then null else hin end` for sentinel normalization
  - Add `case when model_year = '9999' then null else model_year end` for sentinel normalization
  - Add `lpad(boat_type, 2, '0')` for `0`/`00` normalization
  - Consider 2-digit year inference (probably defer to Phase 6/7 enrichment)
- Update `dbt/models/staging/stg_uscg_recalls.yml`:
  - Add `accepted_values: ['H', 'L', 'M', 'S']` test on `severity` (severity=warn for the `1` outlier)
- Update `recall_event.sql` USCG branch: lift `severity` to `recall_event.severity` (per §4 confirmed)
- Update `firm.sql` + `recall_event_firm.sql` USCG branches per §3 Bug 3 fix (recommended in §7)
- Email USCG OII with the boat_type empirical code list + severity enum meaning ask + `0`/`00` distinction ask + MIC `111` data-quality question

### Corpus-scale re-validation (2026-06-02 — silver-field-remap W1, 1,763-row re-seed)

The 2026-05-31 historical re-seed (1,763 fetched / 1,763 loaded / **0 rejected** — the year-prefix-invariant removal eliminated the prior 251 quarantines) confirmed every §4/§5 finding at corpus scale; no corrections. Feeds `documentation/audit/bronze_corpus_profile.md` §1–§6.

- ✅ **severity** (`inspect` Q1): H 38.2% / L 34.9% / M 1.4% / S 0.1%, **NULL 23.1%** (77% populated), + 37 lowercase (`l`=35, `h`=2) + the `1` outlier. Silver `upper(nullif(severity,''))` + `accepted_values {H,L,M,S}` warn.
- ✅ **hin** (Q2): 52.8% real / 1.2% `N/A` / 46.0% null. Silver `'N/A'→NULL`.
- ✅ **boat_type** (Q3): 25 distinct codes, NULL 35.7%; `11` 24.9% dominates; `0`(7)≡`00`(46). Lookup-table gap stands (USCG OII ask). Silver `lpad(boat_type,2,'0')`.
- ✅ **25-char narrative cap** (Q4): `problem_1` max 25 / p95 25 (avg 17); `problem_2` max 25 (avg 18). Confirmed source-side cap — snippets only.
- ✅ **model_year** (Q5): single 4-digit 59.7% / null 32.4% / 2-digit 5.3% / range-list 1.8% / non-numeric 0.1% (`AFTER 1`,`ALL`); `9999` sentinel. Silver `9999→NULL`; 2-digit padding deferred to enrichment.
- ✅ **disposition** (Q6): `Closed` 1476 / `Open` 190 / `CLOSED` 95 / `OPEN` 2 — 4 case-forms → `lower()` → {open,closed}.
- ✅ **firm-rollup quality** (Q7/Q8): 83.0% of MICs → 1 company, 91.3% of companies → 1 MIC; recall→directory coverage **714/718 (99.44%)**. The ~10–17% multi-name tail (e.g. `SER`→`SEA RAY BOATS`/`SEA RAY BOATS INC`; `BUJ`→3 forms) is Phase 6b suffix-strip/RapidFuzz scope.
- 🆕 **`''`-club correction:** USCG missing scalars are genuine SQL NULL, not `''` (Q5 `IS NULL` == nullif counts). USCG is the HTML-scraper exception (like CPSC); its traps are named sentinels, not bare `''`.
- **SCD:** 0 edit-versions in the single-shot recalls seed; the SCD axis is the firm anchor `mic`, measured on the manufacturer side (see `manufacturer_scraping_observations.md` §N).

## References

- `src/extractors/uscg.py` — HTML scraping extractor with `_should_short_circuit` + `_check_year_prefix_consistency` (removed per Finding G) + `UscgDeepRescanLoader` override
- `src/schemas/uscg.py` — Pydantic bronze contract (19 domain fields + details_url; two date validators)
- `config/sources/uscg.yaml` — source registry with `expected_columns` drift fence
- `dbt/models/staging/stg_uscg_recalls.sql` — staging with Finding O + R + S handling + ADR 0027 empty-string normalization
- `dbt/models/silver/recall_event.sql:165-211` — USCG → recall_event mapping (where Bug 2 informational lives)
- `dbt/models/silver/recall_product.sql:139-167` — USCG → recall_product mapping (where Bug 1 lives)
- `dbt/models/silver/firm.sql:99-110` — USCG → firm mapping (where Bug 3 lives)
- `dbt/models/silver/recall_event_firm.sql:85-99` — USCG → recall_event_firm mapping (same Finding S null-anchor filter as firm.sql)
- `documentation/uscg/scraping_observations.md` — Findings A-S (Step 1 + Step 3 empirical record)
- `tests/fixtures/cassettes/uscg/` — 5 cassettes including `test_lifecycle_against_real_bytes.yaml`
- `tests/fixtures/uscg/sample_{listing,details,pagination_boundary}_page.html` — HTML fixtures for unit-test coverage
- `documentation/decisions/0001-sources-in-scope.md` — USCG portfolio rationale
- `documentation/decisions/0010-ingestion-cadence-and-github-actions-cron.md` — USCG cron amendment
