# USCG Manufacturers Directory — Scraping Observations

Sibling to `documentation/uscg/scraping_observations.md` (the USCG recalls scraping doc). Same source-family (uscgboating.org), same scraping technique class (paginated HTML listing pages), different dataset.

**Scope:** Phase 5d Step 7 Step 1 — empirical structural observations of `https://uscgboating.org/content/manufacturers-identification.php`. Drives the Step 2 Pydantic schema, drift-fence config, and extractor structure. Findings come from three probe pages (0, 300, 600 of 651) captured 2026-05-30 and analyzed via a 6-dimensional parallel workflow with 62 load-bearing claims adversarially verified (62/62 survived refutation).

**Probe artifacts** (gitignored): `data/exploratory/uscg_manufacturers/probes/page_{0,300,600}_20260530T000803Z.html` + `_headers.txt`.

## A. Listing-page structure — 5 columns, 25 rows, `tr.defaultFont` selector

The manufacturer directory page contains multiple `<table>` elements (header nav, search form, results table, footer). The results table:

- **Has NO `id` or `class`** attribute — disambiguation by header content only (`<strong>MIC</strong>` in the first column header, or the `<h2>Manufacturers List</h2>` heading immediately preceding).
- Starts on page_0 line 367: `<table width="100%" border="0" cellspacing="0" cellpadding="0">` — same generic attributes as several other tables on the page.
- Header `<tr>` (page_0 line 368) carries NO `class` attribute (bare `<tr>`); data rows carry `<tr class="defaultFont">`. The selector `tr.defaultFont` cleanly excludes the header row.
- **Exactly 5 columns** verbatim (no leading/trailing whitespace inside the `<strong>` tags — unlike recalls' `<strong> Company Name</strong>` quirk):

| Column | Header text |
|---|---|
| 1 | `MIC` |
| 2 | `Company` |
| 3 | `Address` |
| 4 | `City` |
| 5 | `State` |

- **Exactly 25 data rows per page** across all probed pages (0, 300, 600). Confirmed via `grep -c '<tr class="defaultFont">'`. Stable rows-per-page is the basis for page-count math.
- Note: a count of bare `class="defaultFont"` substrings yields 26 because of one `<span class="defaultFont">` inside the search form at line 333; use `tr.defaultFont` specifically.

**Drift fence list for `expected_columns` (YAML config):** `["MIC", "Company", "Address", "City", "State"]`.

## B. Identity — MIC is the natural key, anchor-wrapped, with a separate internal `id` PK

**This was Open Question #1 in the planning doc. Resolved decisively.**

The first column is the actual MIC (Manufacturer Identification Code), not a row index. Proof chain:

1. Column header is literally `<strong>MIC</strong>` (page_0 line 369).
2. Page 0 displays the digit-only sub-namespace (`101`-`126`) reserved for major US engine/component makers (BRP, Caterpillar, Cummins, ePropulsion, Honda, Indmar, Kawasaki, Mercury Marine, Volvo Penta, Yamaha Motor, Yanmar Marine, etc.). The earlier WebFetch confusion that prompted Open Q#1 was an alphabetical-sort artifact, not ambiguity.
3. Page 0 row id=10 displays MIC `110`; row id=11 displays MIC `112` — **the MIC sequence SKIPS `111`** while the internal `id` is contiguous (10→11). A row counter cannot skip; the MIC string can (deprecated/withdrawn MICs).
4. Page 600 displays canonical 3-letter MICs (`YCF`, `YCG`, ..., `YDD`) for Canadian/foreign manufacturers — consistent with the regulatory rule that foreign MICs are all-alphabetic 3-char.

**MIC format:** 3-character alphanumeric (regex `^[A-Z0-9]{3}$` confirmed by sample). Pure-digit block 101-126 is reserved for engine makers; everything else is 3-letter alpha. **MIC must be `str`, not `int`** — would corrupt 99%+ of the corpus.

**Anchor structure** (page_0 line 376):

```html
<td><a href="manufacturers-identification-detail.php?id=1" class="iframe" >101</a></td>
```

- The **anchor text** is the MIC (`101`, `YCF`, etc.) — this is the natural-key value.
- The **href `id` query parameter** is a separate sequential internal database PK (1 on page 0 row 1; 7501 on page 300 row 1; 15001 on page 600 row 1; 25 per page × page offset). It is NOT the MIC.
- **The internal `id` is page-offset-deterministic** (page * 25 + 1), which means it's probably not a stable surrogate key — likely renumbers when records are added or removed. Treat MIC as the durable natural key; capture `uscg_directory_id` as a secondary identifier for forensic/dedup purposes only.

**Cross-source join validation:** Recalls `mic="123"` (in our corpus) resolves to page_0's MIC `123` = `VOLVO GROUP / VOLVO PENTA` — semantically plausible for a marine recall. Confirms `silver.recalls.mic = silver.manufacturers.mic` is the cross-source join key with no transformation needed.

## C. Per-manufacturer detail page exists at `manufacturers-identification-detail.php?id=N`

**This was Open Question #2 in the planning doc. Resolved.**

Every data row contains exactly one anchor, wrapped around the MIC cell. The anchor points to a per-manufacturer detail URL:

```
manufacturers-identification-detail.php?id=<internal-id>
```

Relative href; base path is `/content/`. The anchor's `class="iframe"` hints at lightbox rendering on the live site (likely fancybox/colorbox-style overlay), but the underlying URL is a regular PHP endpoint that should respond to direct HTTP GET. **Not yet probed.** Open question for Step 1 follow-up: does the detail URL return the full record when fetched directly (vs in iframe context), and what fields does it expose beyond the 5 listing columns?

**Implication for the extractor.** Two architecturally distinct paths:

- **Path A — listing-only** (default; matches USDA establishments pattern). Bronze captures 5 fields per row + `uscg_directory_id`. Address is truncated at ~30 chars (Finding F). ~651 fetches per run.
- **Path B — listing + per-record details walk** (matches USCG recalls pattern). Bronze captures 5 listing fields + N detail-page fields + `uscg_directory_id`. ~651 + 16,263 ≈ 16,914 fetches per run.

Choice deferred to architectural decision below (§ "Step 2 architectural decisions"). Path A is the recommended default; Path B is reserved for the rare case where detail-page fields are load-bearing for the silver firm-resolution use case.

## D. Pagination — `pageNum_manufacturers=N`, "Records Found: 16263" footer, 651 pages

- **Pagination URL parameter:** `pageNum_manufacturers` (mirrors USCG recalls' `pageNum_allRecalls` convention).
- **Companion parameter:** `totalRows_manufacturers=16263` carried in every nav link. Informational — the server recomputes Records Found per response; the extractor can omit this from constructed URLs.
- **Page index range:** 0 through 650 (`Last` link points to `pageNum_manufacturers=650`).
- **Corpus size:** 16,263 records. 16,263 ÷ 25 = 650.52, so the last page (650) contains 13 rows.
- **`Records Found: NNNN` footer:** present on every page (page_0/300/600 line 359 verbatim: `<td> Records Found: 16263</td>`). **Value is identical across all three probes** — confirms this is a global corpus-size signal, not a paginated sub-count. Regex: `r"Records Found:\s*(\d+)"`.
- **Reuses `source_watermarks.last_records_count`** (migration 0014, originally added for USCG recalls). Column is generic at the table level and accepts any source; USCG manufacturers becomes the second consumer in v1. No new migration needed.

**Pagination nav control** (per page, single occurrence at line 360 — no duplicate bottom nav):

```
<< First | < Previous | Next > | Last >>
```

All four links use absolute `/content/manufacturers-identification.php?pageNum_manufacturers=N&totalRows_manufacturers=16263` URLs. On page 0, `Previous` clamps to 0 (same as current); on page N, `Previous=N-1` and `Next=N+1`. Straightforward walk-loop semantics.

**Walk-loop pseudocode:**

```python
for page in range(0, max_pages):
    url = f"{base}?pageNum_manufacturers={page}"
    rows = parse(fetch(url))
    if not rows:
        break  # last page produced an empty result
    yield from rows
```

Drift guard: abort if page count exceeds `_MAX_PAGES` (recommend 800 — gives 2× current corpus headroom vs the observed 651).

## E. HTTP forensics — no caching, fresh PHPSESSID per request, chunked transfer

Compared across all three probe header files:

| Header | Value | Notes |
|---|---|---|
| Status | `HTTP/1.1 200 OK` | All three pages |
| Content-Type | `text/html; charset=UTF-8` | Consistent |
| Transfer-Encoding | `chunked` | **No Content-Length** — extractor cannot pre-size response bodies or use Content-Length as an invariant |
| Server | `Apache` | No version disclosed; no X-Powered-By |
| Cache-Control | `no-store, no-cache, must-revalidate` | Stronger than recalls' `no-store, no-cache` (adds must-revalidate) |
| Expires | `Thu, 19 Nov 1981 08:52:00 GMT` | PHP's `session_cache_limiter` epoch-past default — confirms PHP session machinery emits these |
| Pragma | `no-cache` | HTTP/1.0-era cache suppression |
| ETag | **absent** | Recalls Finding K analog — `extraction_runs.response_etag` persists as NULL by design |
| Last-Modified | **absent** | Recalls Finding K analog — `extraction_runs.response_last_modified` persists as NULL by design |
| Set-Cookie | `PHPSESSID=<32hex>; path=/` | Fresh random PHPSESSID per request (no security flags — no HttpOnly, no Secure, no SameSite, no Max-Age) |

**Headers are byte-identical across all three pages except the three expected volatile fields** (Date, PHPSESSID hex, response body).

**Session persistence is NOT required for pagination.** Three sequential probes with fresh PHPSESSIDs (no cookie persistence) all returned 200 OK with valid HTML. Recalls Finding M analog does NOT apply — the extractor can use the base class's "fresh `httpx.Client` per fetch" default.

**No CDN, no security headers, no rate-limit headers** observed in three probes. Open question for Step 3 corpus-scale walk: does the server emit `Retry-After` / `X-RateLimit-*` under load, or does it just silently degrade?

## F. Data quirks — address truncation, sentinels, embedded newlines, raw unencoded entities

### F.1 Address truncation at ~30 characters (source DB constraint)

Listing-view Address cells are systematically truncated at ~29-30 characters, suggesting a fixed VARCHAR constraint at source. Examples (page_0):

| Row | Company | Listing Address | Truncated text |
|---|---|---|---|
| id=2 | `CATERPILLAR MARINE DIVISION` | `5205 N. O'Connor Boulevard Su` | `Su` (cuts off `Suite`) |
| id=8 | `HONDA MOTOR CO INC` | `1919 Torrance Blvd\nMail Stop` | (embedded newline, see F.2) |
| id=13 | `MARINE POWER HOLDINGS, LLC` | `17506 Marine Power Industrial` | (cuts off Park/Blvd) |
| id=21 | `TOHATSU AMERICA CORP.` | `670 S. Freeport Parkway Ste 1` | (cuts off unit number) |

**Implication:** Listing-only extraction permanently loses suite/apt-level address detail. Full address recoverable only via the per-record detail page (§C path B). Per ADR 0027 bronze stores verbatim, so the truncated string lands as-is; document this as a known limitation.

### F.2 Address can contain embedded newlines (literal `\n`, not `<br/>`)

Page_0 row id=8 (HONDA MOTOR CO INC):

```html
<td>1919 Torrance Blvd
Mail Stop</td>
```

The `<td>` spans lines 427-428 of the HTML with a real newline character mid-content, NOT a `<br/>` tag. BeautifulSoup's `.get_text()` preserves the newline; `.text` may behave differently. The Pydantic `BeforeValidator` for `address` should either preserve verbatim (ADR 0027 stricter reading) or normalize to single space (silver-friendly reading) — decision below.

### F.3 Sentinel values for missing data (TWO distinct patterns + empty string)

| Sentinel | Where observed | Example | Silver coercion |
|---|---|---|---|
| `UNK` (3 chars) | Address, City | page_600 id=15011 (MIC YCP, YU CHING): Address=`UNK`, City=`UNK`, State=`UNK` | bronze keeps verbatim; silver `nullif(col, 'UNK')` |
| `-` (single char) | Company, Address, City | page_600 id=15015 (MIC YCT): Company=`-`, Address=`-`, City=`-`, State=`FL` | bronze keeps verbatim; silver `nullif(col, '-')` |
| `""` (empty string) | State | page_600 MIC YCI, YCL, YCO, YCU, YCP: `<td></td>` for State | bronze BeforeValidator coerces `""` → None; OR keep verbatim, silver `nullif(col, '')` |

Per ADR 0027 (storage-forced only), bronze should keep all three verbatim. Silver does the multi-pattern coercion. **Three distinct null-coercion rules per affected column** — document in silver-side `stg_uscg_manufacturers.sql`.

### F.4 Raw unencoded `&` and `'` in data cells

Company cells contain literal `&` and `'` characters with NO HTML-entity encoding:

- `LAPRAIRIE'S BAIT & WELD SHOP` (page_300)
- `L & B BOATWORKS` (page_300)
- `INTL YACHTS & EQUIPMNT CO LTD` (page_300)

Technically invalid HTML but parses fine. **Bronze does not need an entity-unescape BeforeValidator** — these are already the raw characters. `grep` for `&amp;|&apos;|&#NNNN;` against data `<tr>` rows returns zero matches; HTML entities appear only in page chrome (nav, footer).

### F.5 Company casing is inconsistent

- Page 0: mix of Title Case and ALL CAPS (`BRP (Rotax/Evinrude)` vs `CATERPILLAR MARINE DIVISION`)
- Pages 300, 600: mostly UPPERCASE

Not normalized at source. **Bronze preserves verbatim; silver normalizes** via the Phase 6b fuzzy-matching layer (alongside CPSC suffix-strip + USDA disambiguation).

### F.6 Parenthetical qualifiers in Company names

`BRP (Rotax/Evinrude)`, `SPECIALTY FIBERGLASS (2003)`, `HOPLITE BOATS (DBA)`. Useful for downstream DBA extraction but bronze just preserves.

### F.7 ASCII-only — no diacritics even for French-Canadian names

`KAYAK EAU-ZONE`, `IMPACT PLEIN-AIR`, `LAPRAIRIE'S` — no accents (`É`, `Ô`, etc.) anywhere in the 75 sampled rows. Source has stripped diacritics. `grep -P "[^\x00-\x7F]"` against all 3 probes returns zero matches. **Bronze does not need NFKC normalization.** UTF-8 storage is safe-by-default.

## G. State column — 2-letter codes, includes Canadian provinces, search dropdown is incomplete

`State` values are 2-letter codes mixing:

- **US states:** CA, TX, FL, NY, WI, etc.
- **US territories:** AS, GU, MP, PR, VI (in search dropdown)
- **Canadian provinces:** BC, ON, AB, NS, NL, QC — **NOT in search dropdown** but present in data

**Implication:** State cannot be a Pydantic `Literal[...]` constrained to US states; it must allow 2-letter Canadian provinces too. Plain `str | None` is correct.

The discrepancy between data (Canadian provinces present) and dropdown (US-only) means a USCG-site state-filter search for "BC" would return zero hits even though such manufacturers exist — a usability bug on the source site, not our concern, but worth documenting for downstream consumers who might wonder why their state-filtered subset is incomplete.

## H. Bulk CSV download — probed 2026-05-30, REJECTED as stale and incomplete

Page_0 lines 568-569 reference bulk-download URLs:

```
../downloads/MIC.csv
../downloads/MIC.mdb
```

Probed `https://uscgboating.org/downloads/MIC.csv` 2026-05-30 (artifact: `data/exploratory/uscg_manufacturers/probes/MIC_20260530T000803Z.csv`). The endpoint returns a valid 5.7 MB CSV with 35 columns (compared to HTML's 5) — but **the CSV is NOT a current equivalent of the HTML directory and cannot replace HTML scraping**. Evidence:

| Equivalence dimension | HTML directory | CSV download | Verdict |
|---|---|---|---|
| HTTP `Last-Modified` | (absent — `no-store, no-cache, must-revalidate`) | **`Fri, 18 Oct 2019 18:19:05 GMT`** (7 years stale at time of probe) | ❌ STALE |
| Total records | 16,263 (Records Found footer) | 16,216 data rows | ❌ 47 missing |
| Engine-maker MIC block (101-126) | Present on page 0 (BRP, Caterpillar, Cummins, Honda, Mercury, Volvo Penta, Yamaha, Yanmar, ...) | **Entirely absent** — CSV starts at `4WN`, no row with `mic=101` or `mic=123` | ❌ Whole administrative sub-namespace missing |
| Column count | 5 (MIC, Company, Address, City, State) | 35 (adds Phone, Last/First Name, Zip, District, MSO, Type 1/2/3, Parent Company, Date In/Out of Business, Country, Date Modified, Comments, w-code, Address 1, Officer, fax, website, Email, Cell Phone, etc.) | CSV richer-but-stale |
| Sentinel conventions | `""`, `"UNK"`, `"-"` | literal `"NULL"` string, `-`, `&`, `*`, `/`, plus empty | ❌ Different vocabularies |
| Parsing complexity | Pure HTML — bs4 handles it | CSV with multi-line embedded commas in quoted Comments fields | CSV needs proper csv-aware parser |
| `Content-Type` | `text/html` | `text/csv` | — |

**The CSV is an administrative archive snapshot from 2019, not a live directory mirror.** It excludes a meaningful sub-namespace (engine-maker MICs), undershoots the live corpus by 47 records, and uses different sentinel conventions. The MIC anchors on page 0 of the live HTML directory (BRP=101, Volvo Penta=123, Yamaha=125) all return zero CSV matches — these manufacturers are clearly active per the live HTML but completely missing from the CSV. This is not freshness lag; this is a different dataset.

**Architectural decision: HTML scraping is the primary path.** The CSV will not be used as an extraction source.

**CSV's 35-column richness could be valuable for downstream historical enrichment** (parent-company relationships, OOB dates, DBA names that aren't in the live HTML view) — but as a **Phase 6+ side-channel** with explicit stale-data warnings, not as an extraction source. Out of scope for this branch; documented here for future consideration.

## I. MIC sub-namespaces and structural gaps

The corpus is organized into structural namespaces visible across the probed pages:

- **Numeric block `100`-`199`** (page 0 sample: 101-126, with 111 missing): reserved for major US engine/component manufacturers (BRP, Caterpillar, Cummins, Honda, Kawasaki, Mercury Marine, Volvo Penta, Yamaha Motor, Yanmar Marine, etc.).
- **Alpha block `LPW`-style** (page 300 sample): general US boat-hull manufacturers.
- **Alpha block `YCF`-`YDD`** (page 600 sample): foreign (predominantly Canadian) manufacturers per regulation (all-alphabetic 3-char rule).

**Gaps in the MIC sequence (e.g., page 0 skipping 111)** indicate withdrawn/deprecated MICs. The directory does NOT renumber to fill gaps — MICs that were retired stay retired. This matters for:

- **dbt test design:** A "MIC sequence completeness" assertion would always fail. Don't write one.
- **Cross-source join robustness:** Recalls referencing a retired MIC may not resolve against the directory. Need to confirm at corpus scale whether retired MICs persist in recalls vs are reissued — open question for Step 3.

## J. Cross-source join validation (recalls.mic ↔ manufacturers.mic)

Recalls corpus has `mic="123"` (per Phase 6a audit). Directory page_0 line 524 shows `mic="123"` = `VOLVO GROUP / VOLVO PENTA`. Semantically plausible (marine recalls on Volvo Penta engines). Confirms the silver join is direct string equality with no normalization needed:

```sql
left join {{ ref('stg_uscg_manufacturers') }} m
    on r.mic = m.mic
```

Subject to corpus-scale validation in Step 3 (does ~93.2% of recalls.mic population — the Finding S figure — resolve to a directory row? are there orphans?).

## K. Short-circuit reuse — `Records Found` + `source_watermarks.last_records_count`

The recalls extractor's two-gate short-circuit (Finding J in recalls) directly reuses for manufacturers:

- **Gate 1 (count):** parse `Records Found: NNNN` from page 0; compare against `source_watermarks.last_records_count` (column is generic at the migration 0014 level; usable for any source).
- **Gate 2 (membership):** every MIC on page 0 already exists in `uscg_manufacturers_bronze`.

If both pass, return `[]` from `extract()` — skips the 650-page walk. Same idempotent two-gate pattern as recalls; reuses the existing `source_watermarks.last_records_count` and `extraction_runs.was_short_circuited` columns (no new migration for these signals).

## L. Step 3 corpus-scale validation (2026-05-30)

First historical-seed extraction landed 16,263 records in `uscg_manufacturers_bronze` with 0% rejection rate (16,263 fetched / 16,263 valid / 16,263 loaded / 0 quarantined). Walk took ~14:46 across 651 paginated listing pages with 1-second polite throttle. Findings A-K all empirically validated at corpus scale. Plus three new findings (L.1-L.3) surfaced by the run.

### L.1 — Confirmations of prior Findings

- **Finding A (table structure):** ~25 rows × 651 pages = 16,263 records, matches `Records Found: 16263` exactly (Q1 silver row count).
- **Finding B (MIC identity):** All 16,263 anchors parsed cleanly to integer `uscg_directory_id` (0.00% NULL per Q1). MIC-as-natural-key holds at scale.
- **Finding D (pagination):** `pageNum_manufacturers` walk + `Records Found` parse worked across all 651 pages. The page-651 boundary (out-of-range) emitted a single `<a href="manufacturers-identification-detail.php?id=">PLACEHOLDER</a>` row — the empty `id=` query parameter triggered the defensive Finding L guard added during Step 2 review (mirroring USCG-recalls' Finding L), producing one `uscg_manufacturer.parse.empty_mic` warning log entry and a clean empty-rows break to end pagination. **Resolves Open Question #10:** out-of-range pages return HTTP 200 with a placeholder row, NOT 404, NOT redirect.
- **Finding F.3 (sentinel coercion):** Q4 confirmed 0 leakage across `company_name` / `address` / `city` / `state` — the staging-layer `CASE WHEN col IN ('UNK', '-', '') THEN NULL` pattern works cleanly.
- **Finding G (state distribution):** Q6 top-20 shows Canadian provinces present at meaningful frequency despite their absence from the search-form dropdown: **BC 3.66% (rank 5), ON 3.63% (rank 6), QC 1.72% (rank 16)**. Confirms the dropdown is silently incomplete relative to the actual data.
- **Finding I (MIC sub-namespaces):** Q5 corpus-scale breakdown:

| Class | Pattern | Rows | % | Range |
|---|---|---|---|---|
| Alpha block | `^[A-Z]{3}$` | 16,166 | 99.40% | `AAA` → `ZZZ` |
| Mixed alphanumeric | `^[A-Z0-9]{3}$` (excl. above) | 66 | 0.41% | `4WN` → `WM2` |
| Digit block (engine makers) | `^[0-9]{3}$` | 26 | 0.16% | `101` → `192` |
| Other (lowercase drift) | else | 5 | 0.03% | `gvz` → `odj` |

  Confirms the Step 1 Finding I claim: alpha block dominates; digit-block is the smaller engine-maker reserved sub-namespace. **Resolves Open Question #4:** the regulatory format `^[A-Z0-9]{3}$` covers 99.97% of corpus — but **the 5 lowercase records (`gvz`, `odj`, etc.) are real source data quality issues**, not parser artifacts. Adding a Pydantic `Field(pattern=...)` constraint at bronze would quarantine these; bronze keeps verbatim per ADR 0027 and silver UPPER-normalizes during the cross-source JOIN.
- **Finding K (short-circuit reuse):** the page-0 `Records Found: 16263` regex parse populated `source_watermarks.last_records_count` cleanly; next run will exercise the two-gate short-circuit.

### L.2 — NEW: BRP is two distinct manufacturers (dense MIC namespace)

The §3 Bug 3 rescue surfaced an interesting collision:

- **MIC `101`** → `BRP (Rotax/Evinrude)` (Bombardier Recreational Products — the well-known engine maker; engine-maker digit block sub-namespace)
- **MIC `BRP`** → `BAYRIPPER LLC` (different manufacturer whose 3-letter regulatory code coincidentally spells the BRP acronym)

These are NOT the same firm. The MIC namespace is dense enough that 3-letter codes can collide with vendor acronyms. Silver firm rollup correctly treats them as distinct firms (different MICs, different canonical names). Worth documentation so future analysts don't conflate them during cross-source firm analysis.

### L.3 — NEW: lowercase MIC data quality in recalls bronze (silver-only handling)

USCG recalls bronze contains 7 distinct lowercase MIC values (`cec`, `blb`, `kis`, `lbb`, `ser`, `vky`, `zep` — totaling 12 recall rows) whose uppercase forms exist in the directory. Pre-fix, these surfaced as cross-source coverage orphans in Q3b (98.48% coverage with 11 distinct orphans including the lowercase 7). Post-fix (case-insensitive JOIN in `firm.sql` USCG branch, mirrored to `recall_event_firm.sql`), coverage rose to **99.44% (Q3: 714/718 matched)** with **4 distinct orphans** — all real retirements/sentinels: `111` (retired regulatory code per Finding I, 30 recall rows reference), `999` (17 rows, sentinel), `777` (1 row, sentinel), `N/A` (1 row, literal "N/A" string).

The fix lives at `dbt/models/silver/firm.sql:107-129` USCG branch (`on upper(trim(r.mic)) = upper(trim(m.mic))`) and is mirrored at `dbt/models/silver/recall_event_firm.sql:91-104` USCG branch. Bronze preserves verbatim per ADR 0027.

### L.4 — Confirmations folded into ongoing observability

- **§3 Bug 3 rescue: 5 mic-only-no-name recovered** (BLB, BRP, CRC, MHB, PCM); within the audit's predicted 0-10 range. The other ~5 of the predicted 10 have MICs that aren't in the directory (likely retired) and remain in the Option 3 soft-fail path.
- **General canonical-name enrichment: ~18 USCG firms collapsed** (Q7b 749 → 731 reachable firms) because recall-time names sometimes drift from the live directory's canonical USCG-registered name.
- **dbt build: PASS=114, WARN=3 (all pre-existing source-assumption assertions unrelated to this branch), ERROR=0.** All relationships tests on the rebuilt `recall_event_firm` pass cleanly after `recall_event_firm.sql` was updated to mirror `firm.sql`'s case-insensitive LEFT JOIN.

### Open questions resolved by Step 3

- ✅ **Q#1 (CSV equivalence)** — resolved in §H: CSV is stale (Last-Modified 2019), incomplete (missing engine-maker digit block), differs in 47 record count. HTML stays the primary source.
- ✅ **Q#4 (MIC format pattern)** — confirmed `^[A-Z0-9]{3}$` covers 99.97%; remaining 0.03% are real source data-quality issues, NOT parser drift. Silver UPPER-normalizes; bronze keeps verbatim per ADR 0027.
- ✅ **Q#9 (boundary page contents)** — page 651 emitted a placeholder row with empty `id=` query parameter, caught by the Finding L defensive guard. Empty-row break terminated the walk cleanly.
- ✅ **Q#10 (out-of-range page behavior)** — HTTP 200 + placeholder row; NOT 404, NOT redirect.
- ✅ **Q#11 (recall→directory coverage)** — 99.44% (714/718). The 4 orphans are real retirements/sentinels.
- 🟡 **Q#2 (detail-page contents when fetched directly)** — still deferred. Listing-only extraction is sufficient for v1; address-detail enrichment via per-record GET remains a Phase 6+ candidate.
- 🟡 **Q#3 (MIC reassignment over time)** — still deferred. Pattern not observed empirically (each MIC resolves to exactly one company in current snapshot); would need a multi-snapshot comparison.
- 🟡 **Q#5 (detail-page id stability across crawls)** — still deferred. Page-offset-deterministic suggests instability; would need re-crawl comparison. Hash-excluded from content_hash defensively.
- 🟡 **Q#6 (sentinel rates corpus-wide)** — addressed via staging coercion (Q4 = 0 leakage) but per-column NULL rates measured at silver: company_name 0.02%, address 0.24%, city 0.45%, state 0.38%.
- 🟡 **Q#7 (multi-line addresses with > 1 newline)** — not yet measured corpus-wide; HONDA example is the only observed 2-line case.
- 🟡 **Q#8 (rate-limit envelope)** — no `Retry-After` / `X-RateLimit-*` headers observed during the 651-page walk at 1-second throttle. Production cadence stays safe.

## Step 2 architectural decisions (locked, contingent on Finding H probe)

**Contingent on the CSV probe outcome in §H.** Default path below assumes CSV is unavailable / not viable.

### Extractor structure

- **Class:** `UscgManufacturerExtractor(HtmlScrapingExtractor[UscgManufacturerRecord])`
- **Sibling:** `UscgManufacturerDeepRescanLoader(UscgManufacturerExtractor)` — symmetry with USCG recalls; overrides `_should_short_circuit → False` and skips `_touch_freshness` / `_update_records_count`.
- **Path A (listing-only) — recommended default.** Walk listing pages; do NOT fetch per-record detail pages. Address field is truncated (~30 chars). Detail-page walk deferred to a Phase 6 optimization or never.
- **Override `_parse_details_page`** to `raise NotImplementedError("Manufacturer directory uses listing-only extraction")` — satisfies the `HtmlScrapingExtractor` abstract contract while making accidental invocation loud.

### Pydantic schema (`src/schemas/uscg_manufacturer.py`)

```python
class UscgManufacturerRecord(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    # Natural key — MIC (3-char alphanumeric per regulation)
    source_recall_id: str = Field(validation_alias="mic")

    # Listing fields (all nullable post-Step 3 corpus confirmation)
    company_name: str | None = Field(default=None, validation_alias="company")
    address: str | None = Field(default=None)  # ~30-char truncated; may have embedded \n; may be "UNK" / "-"
    city: str | None = Field(default=None)  # may be "UNK" / "-"
    state: str | None = Field(default=None)  # 2-letter; may be "" (Canadian rows in search dropdown gap)

    # Secondary identifier — USCG's internal sequential row PK from the detail URL
    # Page-offset-deterministic so likely unstable across re-crawls; capture for forensics only.
    uscg_directory_id: int | None = Field(default=None)

    # Lineage — full detail URL (per row's anchor href, absolutized to base path)
    # Excluded from content_hash via hash_exclude_fields in load_bronze.
    detail_url: str
```

**Note: `mic` and `company_name`** — the Pydantic field is `source_recall_id` (project-wide convention) with `validation_alias="mic"` matching the parser-emitted dict key. Same pattern as USDA establishments using `validation_alias="establishment_id"`.

### Bronze table (`migration 0015`)

```sql
CREATE TABLE uscg_manufacturers_bronze (
    id SERIAL PRIMARY KEY,
    source_recall_id TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    extraction_timestamp TIMESTAMPTZ NOT NULL DEFAULT now(),
    raw_landing_path TEXT NOT NULL,
    company_name TEXT,
    address TEXT,
    city TEXT,
    state TEXT,
    uscg_directory_id INTEGER,
    detail_url TEXT NOT NULL
);

CREATE INDEX ix_uscg_manufacturers_bronze_id_ts
    ON uscg_manufacturers_bronze (source_recall_id, extraction_timestamp DESC);
CREATE INDEX ix_uscg_manufacturers_bronze_state
    ON uscg_manufacturers_bronze (state);
```

Plus `uscg_manufacturers_rejected` via `rejected_table_columns()` helper.

### `source_watermarks` seed (`migration 0016`)

```python
op.execute("""
    insert into source_watermarks (source, last_cursor, last_etag, updated_at)
    values ('uscg_manufacturers', null, null, now())
    on conflict (source) do nothing
""")
```

### YAML config (`config/sources/uscg_manufacturers.yaml`)

```yaml
source_name: uscg_manufacturers
source_type: html_scraping
start_url: https://uscgboating.org/content/manufacturers-identification.php
timeout_seconds: 30.0
scrape_delay_seconds: 1
expected_columns:
  - MIC
  - Company
  - Address
  - City
  - State
```

### Source registry additions (`src/config/source_registry.py:59-79`)

```python
EXTRACTOR_BY_SOURCE_NAME = {
    ...,
    "uscg_manufacturers": UscgManufacturerExtractor,
}
DEEP_RESCAN_BY_SOURCE_NAME = {
    ...,
    "uscg_manufacturers": UscgManufacturerDeepRescanLoader,
}
```

### Identity + hash policy

- **`identity_fields=("source_recall_id",)`** — MIC is the natural key.
- **`hash_exclude_fields=frozenset({"detail_url", "uscg_directory_id"})`** — both are URL-internal forensics that may shift on URL-scheme rewrites or row-reorder events. `detail_url` mirrors the recalls convention; `uscg_directory_id` is added because page-offset row-PK pagination strongly suggests instability across re-crawls.

## Open questions deferred to Step 3 (corpus-scale validation) or later

1. **(GATING)** Does the bulk CSV at `/downloads/MIC.csv` return a valid CSV when fetched? See §H.
2. Does `manufacturers-identification-detail.php?id=N` return useful content when fetched directly (not in iframe context)? What fields does it expose beyond the 5 listing columns? Materially affects whether Path B (listing + details walk) becomes desirable.
3. Are MICs ever reassigned to a different company over time, or is the regulatory rule "once-issued-always-that-company" enforced? Affects whether `(mic, snapshot_date)` should be the natural key vs just `mic`.
4. Does the corpus contain any MIC values not matching `^[A-Z0-9]{3}$`? Sample shows uniform 3-char alphanumeric; full-corpus confirmation would let us add a Field pattern constraint without quarantining real records.
5. Are detail-page `id` values strictly sequential 1..16263 with no gaps or duplicates, or do they skip / re-use? Affects whether `uscg_directory_id` is a usable secondary key.
6. How common is the `UNK` / `-` / empty-string sentinel pattern across the full corpus per column? Sets per-column NULL-coercion priorities in silver.
7. Do addresses ever contain more than one embedded newline (3-line addresses)? Affects silver normalization complexity.
8. What's the rate-limit envelope? Three probes 2 minutes apart returned no `Retry-After` or `X-RateLimit-*` headers — does the server emit those under sustained 651-page walks, or just silently degrade?
9. What does the boundary page (`pageNum_manufacturers=650`) actually look like? Math predicts 13 rows; does the page render those with the same `tr.defaultFont` structure, or pad with blanks?
10. What does `pageNum_manufacturers=651` (out-of-range) return? 200 with empty table? 404? Redirect? Determines whether the walk loop's stop condition is empty-row detection vs HTTP signal.
11. How many recall MICs (~93.2% population per Finding S of recalls) resolve to a directory row? Are there orphans? Cross-source coverage gap is a Phase 6b firm-resolution input.

## References

- Phase 5d Step 7 plan: `project_scope/phase-5d-uscg-manufacturers.md`
- USCG recalls scraping observations: `documentation/uscg/scraping_observations.md` (Findings A-S — analogous patterns from the sibling source)
- USCG recalls extractor (structural mirror): `src/extractors/uscg.py`
- USDA establishments (semantic mirror for silver layer): `src/extractors/usda_establishment.py`, `dbt/models/silver/firm_establishment_attributes.sql`
- Regulatory MIC spec: `documentation/uscg/USCG-2013-0133-0005_attachment_1.pdf`
- ADR 0007 (content-hash dedup), ADR 0014 (Pydantic schema policy), ADR 0027 (storage-forced types)
- Phase 5d Step 7 probe artifacts: `data/exploratory/uscg_manufacturers/probes/page_{0,300,600}_20260530T000803Z.{html,txt}` (gitignored)
- Workflow analysis: 68 agents, 1.67M tokens, 6 dimensions × 25 claims each, 62/62 load-bearing claims survived adversarial verification (workflow run `wf_79c5ed66-fbe`, 2026-05-30)
