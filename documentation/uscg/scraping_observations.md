# USCG Recall-Scraping Observations

**Source URL:** `https://uscgboating.org/content/recalls.php`
**Probe date:** 2026-05-16 (Step 1 of Phase 5d)
**Probe commands:** `curl -A "consumer-product-recalls/<ver> (contact: adriannesta@gmail.com)"` against listing + details URLs (artifacts at `/tmp/uscg_probe/`)
**Companion docs:** `documentation/decisions/0001-sources-in-scope.md` (USCG portfolio-goal rationale), `documentation/decisions/0027-bronze-storage-forced-transforms-only.md` (bronze schema convention), `project_scope/implementation_plan.md:408-432` (Phase 5d step text), the approved plan at `~/.claude/plans/i-checked-out-to-shiny-falcon.md`.

These findings describe the static shape of USCG's recall pages as observed at Step 1. They calibrate the schema, drift fences, polite-scraper knobs, and identity choices that Step 2 implements. Complement to `documentation/nhtsa/flat_file_observations.md`'s Finding-style structure.

---

## A. Listing-page shape

`https://uscgboating.org/content/recalls.php?pageNum_allRecalls=N` returns an HTML table with 6 columns:

| Column header | Sample value | Type |
|---|---|---|
| `Number` | `26MF0158`, `25CG0017` | text (year-prefix encoding) |
| `MIC` | `123`, `NLP` | text (Manufacturer ID Code, 2-4 chars) |
| `Company Name` | `VOLVO GROUP / VOLVO PENTA` | text |
| `Model Name` | `VOLVO PENTA AUTOPILO` (note: truncated) | text, may be empty |
| `Problem 1` | `Stability test (starboard` (may be truncated) | text, may be empty |
| `Opened On` | `2026-03-03` | date (`YYYY-MM-DD`) |

Each row's first cell wraps an anchor link to `recalls-details.php?id=<recall_number>`. The `id` parameter is the recall number itself (no separate internal numeric ID).

Page 0 size: ~30 KB. Real rows per page: ~25. Total record count printed on page: `Records Found: 01763`.

**Per-column completeness in the probe sample** (38 rows across pages 0 + 70 = ~2.2% of corpus):

| Column | Populated rate (sample) |
|---|---|
| `Number` | 38/38 |
| `MIC` | 38/38 |
| `Company Name` | 38/38 |
| `Model Name` | 38/38 |
| `Problem 1` | 16/38 (22 empty) |
| `Opened On` | 38/38 |

**Scope caveat:** the 38-row sample is too small to justify a `NOT NULL` constraint on `Opened On` or `Company Name` for the corpus-wide bronze schema. Per memory rule `feedback_full_corpus_validation` ("don't commit to architecture based on dev-bronze slice"), `opened_on` is marked nullable in `src/schemas/uscg.py` and migration 0013 despite 100% sample-completeness. Step 1.5 corpus probe (deferred) will validate; if `1763/1763 populated` holds, a follow-up migration can tighten the constraint with real evidence.

## B. Details-page shape

`https://uscgboating.org/content/recalls-details.php?id=<recall_number>` returns a small HTML document (~3-4 KB) with **18 labeled fields**, structured as `<strong>Label[:]</strong> ... <span class="defaultFont">value</span>` pairs interspersed across table cells.

Fields observed across two samples (`26MF0158` and `25CG0017`):

| Label on page | Schema name (proposed) | Sample value | Notes |
|---|---|---|---|
| `Number` | `source_recall_id` | `26MF0158` | Redundant with listing column 1 — verifies parser |
| `MIC:` | `mic` | `123` / `NLP` | Redundant with listing |
| `Company:` | `company_name` | `VOLVO GROUP / VOLVO PENTA` | Note label differs: listing says "Company Name", details says "Company" |
| `Company Official:` | `company_official` | `jlu` (initials) or empty | New |
| `Model Name:` | `model_name` | `Volvo Penta Autopilo` | Redundant; details version is full-length (listing truncates) |
| `Model Year:` | `model_year` | `2025` or empty or multi-year string | New |
| `Problem 1:` | `problem_1` | `Stability test (starboard` | Redundant with listing; details truncates at same length |
| `Problem 2:` | `problem_2` | empty in both samples | New (the field exists but was empty for both probes) |
| `HIN` | `hin` | `NLPEC117K425` or `N/A` | Hull Identification Number; new |
| `Case Open Date:` | `case_open_date` | `3/3/2026` | Same date as listing's `Opened On` but **`M/D/YYYY` format** |
| `Disposition:` | `disposition` | `Open` / `Closed` | New |
| `Case Close Date:` | `case_close_date` | `7/23/2025` or empty | New, `M/D/YYYY` |
| `Units` | `units` | `20`, `401` | Count of affected hulls; new |
| `Campaign Open Date` | `campaign_open_date` | `3/23/2026` | New, `M/D/YYYY` |
| `Boat Type` | `boat_type` | `00` or empty | New (numeric type code) |
| `Campaign Close Date` | `campaign_close_date` | empty in both | New, `M/D/YYYY` |
| `Severity:` | `severity` | empty in both | New |
| `Last Date:` | `last_date` | `3/23/2026`, `12/2/2025` | New, `M/D/YYYY` |

The details page **adds 13 new fields** beyond the 5 redundant ones already on the listing. Two label inconsistencies between listing and details:

1. `Opened On` (listing, `YYYY-MM-DD`) vs `Case Open Date` (details, `M/D/YYYY`) — same date, different label + format.
2. `Company Name` (listing) vs `Company:` (details) — same value, different label.

No sub-tables, no repeating elements within a details page, no defect description or remedy text. The details page is purely a labeled-field summary; no narrative free-text fields.

## C. `source_recall_id` uniqueness — DEFER to Step 1.5

The plan requires "Parse all 71 listing pages once; count distinct `Number`; verify no duplicates." This is a corpus-wide check (~1,763 records). Deferred to **Step 1.5** — a single-shot exploration script invoked after the Step 2 extractor lands but before Step 3 formal first-extraction. Until that script runs, the design assumes single-column identity (`source_recall_id`) is unique; this is consistent with the year-prefix encoding pattern observed and ADR 0030's lesson (`implementation_plan.md:168` "Apply this verification to USCG").

Two-sample sanity check passed: `26MF0158` and `26MF0172` are distinct; `25CG0017` is distinct from both. No collisions observed in any 25-row page.

## D. Details-page render stability — STABLE

Two consecutive fetches of `recalls-details.php?id=26MF0158`, 2 seconds apart, returned **byte-identical** HTML:

```
707532455469d769565a5b42b6edcd0bb76eb62b3d22431fab7b24e43bb669a4  detail_26MF0158_a.html
707532455469d769565a5b42b6edcd0bb76eb62b3d22431fab7b24e43bb669a4  detail_26MF0158_b.html
```

Implication: the server does not embed render timestamps, session IDs, or other per-request state in the details-page body. `content_hash` will be stable across runs for unchanged data. No need to exclude render-side fields from the hash.

## E. `last_date` semantics — DEFER, but evidence so far is benign

Both 26MF0158 and 25CG0017 details pages show `Last Date` values that match other dates in the same record (e.g., 26MF0158's `Last Date` = `Campaign Open Date` = `3/23/2026`). This is consistent with `last_date` being **a recall-lifecycle date set at last editorial change**, not a server-render timestamp. Combined with Finding D (byte-stable consecutive fetches), the inference is that `last_date` does NOT re-stamp on every render.

Defer to Step 3 confirmation: monitor `last_date` across consecutive daily/weekly runs. If a previously-stable recall's `last_date` advances without any other field changing, USCG is using it as a "last touched in CMS" field and it should be added to `hash_exclude_fields`. Until then, include in the hash.

## F. Date format inventory — TWO FORMATS confirmed

| Page | Field | Format | Sample |
|---|---|---|---|
| Listing | `Opened On` | `YYYY-MM-DD` | `2026-03-03` |
| Details | `Case Open Date`, `Case Close Date`, `Campaign Open Date`, `Campaign Close Date`, `Last Date` | `M/D/YYYY` | `3/3/2026`, `12/2/2025` |

Schema implication: Pydantic `BeforeValidator`s coerce both formats to UTC `datetime` per ADR 0027. Two distinct validators (`_UscgListingDate`, `_UscgDetailsDate`) keep the parsing per-field-explicit. The details version is the canonical (always fetched alongside listing); the listing date is preserved as a sanity-check facility — if it ever diverges from `case_open_date`, that's worth quarantining.

Empty cells (e.g., `Case Close Date` on open recalls) parse to `None`.

## G. Year-prefix invariant — FALSIFIED + REMOVED (Step 3, 2026-05-17)

**Original Step 1 hypothesis (refuted):** that `source_recall_id[:2]` always matches `opened_on.year % 100` — based on a 2-sample probe:

| Recall | `source_recall_id` | Prefix | `opened_on` | Year-suffix | Match? |
|---|---|---|---|---|---|
| 26MF0158 | `26MF0158` | `26` | `2026-03-03` | `26` | ✅ |
| 25CG0017 | `25CG0017` | `25` | `2025-06-04` | `25` | ✅ |

**Step 3 first-extraction falsified the hypothesis** — 218 of 1,763 corpus rows (~12.4%) violate it. The `(prefix, opened_on_year)` heatmap from `scripts/sql/uscg/bronze/diagnose_rejections.sql` Q6 shows at least four distinct mechanisms:

1. **Fiscal-year prefixes on Oct-Dec openings.** USCG FY runs Oct 1 → Sep 30. `23MF0066` opened `2022-10-05`, `82R...` opened `1981-...`. The recall number is assigned in fiscal-year terms; the `opened_on` is calendar.

2. **Prefix = opened_on year − 1 (filing-vs-opened workflow).** Dominant pattern: `04/2005` (14 cases), `95/1996` (14), `94/1995` (10), `03/2004` (9), `97/1998` (8), `02/2003` (8), `92/1993` (7), `96/1997` (7), `00/2001` (7), `98/1999` (6), `93/1994` (6), plus smaller buckets. ~100+ rows. Hypothesis: number assigned at filing (year N), case officially opened later (year N+1).

3. **Multi-year offsets for re-issued / amended recalls.** `00/2002`(4), `03/2005`(4), `04/2006`(4), `96/1998`(2), `04/2003`(2), `040057S` opened `2000-04-01` (4-year offset). Recall number from years ago; the case was re-opened or amended later under the same ID.

4. **Unix-epoch sentinel.** ~45+ cases with `opened_on_year=1970` — see Finding O below. Listing renders `1970-01-01` as a "no opened date known" sentinel; year-prefix invariant fundamentally cannot apply.

R2 byte-confirmation (`scripts/uscg/inspect_landing_ndjson.py --recall-number 23MF0066 --show-field "Case Open Date"`) verified USCG genuinely serves these mismatches — not a parser bug on our side.

**Decision:** the `_check_year_prefix_consistency` invariant was **removed entirely** in `src/extractors/uscg.py`, not relaxed. No single rule captures these patterns, and the underlying USCG numbering scheme is more complex than the Step 1 working hypothesis assumed. Bronze records the mismatches verbatim; downstream consumers that care about the encoding can rebuild whatever fiscal-year vs calendar-year mapping they need from `opened_on` directly.

## H. robots.txt — 404 not found

`https://uscgboating.org/robots.txt` returns HTTP 404. No published crawler policy. **Polite-scraper defaults apply by convention:** identifying User-Agent with contact email, serial requests with throttle, respect 429 + `Retry-After`. No `Allow`/`Disallow` rules to honor; the absence of robots.txt is not equivalent to permission for aggressive scraping.

## I. HTML entity preservation

Narrative fields preserved verbatim (per ADR 0027 storage-only convention at bronze). Listing fixture (`tests/fixtures/uscg/sample_listing_page.html`) confirms entities like `&nbsp;` appear in cell-separator markup but not in data fields observed. If a future recall contains `&amp;` or `&#xN;` in `company_name` or `problem_*`, the bronze row stores it verbatim; silver `stg_uscg_recalls.sql` decodes.

## J. `Records Found: NNNN` total — present on every page

Confirmed `Records Found: 01763` string at the bottom of page 0. Persists on partial-data pages (page 70). Pagination boundary page 71 also displays the same count — i.e., the count is a global signal not gated by the current page slice.

**Future optimization** (not v1): a steady-state run can HEAD or GET only page 0, scrape `Records Found`, and short-circuit the full walk if the count is unchanged since last run. Documented here for Phase 6+; not implemented in v1.

## K. `Last-Modified` / `ETag` on listing page — NEITHER

`curl -I https://uscgboating.org/content/recalls.php` response headers (relevant excerpt):

```
HTTP/1.1 200 OK
Server: Apache
Expires: Thu, 19 Nov 1981 08:52:00 GMT
Cache-Control: no-store, no-cache, must-revalidate
Pragma: no-cache
Set-Cookie: PHPSESSID=...; path=/
Content-Type: text/html; charset=UTF-8
```

USCG explicitly opts out of caching (`Cache-Control: no-store, no-cache`). **No `Last-Modified`, no `ETag`.** Implication: no HTTP-level short-circuit available. The "Records Found total" approach in Finding J is the cheapest available change-detection signal.

The `Set-Cookie: PHPSESSID` is observed but **should not be persisted across fetches** — pinning to a single session offers no benefit and could trigger session-bound rate limits. The extractor uses an independent `httpx.Client` per fetch and discards cookies.

## L. Pagination-boundary behavior — empty placeholder row, not 404

| URL | Status | Body size | Real rows | Behavior |
|---|---|---|---|---|
| `?pageNum_allRecalls=0` | 200 | 30,635 B | 25 | Full page |
| `?pageNum_allRecalls=70` | 200 | 27,127 B | 13 | Partial last page (1,763 % 25 = 13) |
| `?pageNum_allRecalls=71` | 200 | 23,430 B | 0 real | **One empty placeholder row** with `<a href="recalls-details.php?id="></a>` (empty `id`) |

**Stop condition:** the parser detects end-of-pagination when the only rows on a page have empty `id` parameters. Stop, don't continue probing higher page numbers. As a drift guard, abort the walk if page-count exceeds 200 (~5,000 records — 3× current corpus).

Fixture: `tests/fixtures/uscg/sample_pagination_boundary.html` (page 71) captured for unit-test coverage.

## M. PHP session cookie — discard

The listing page sets `PHPSESSID` on first GET. Since USCG's pages are pre-rendered and not session-personalized (the same anonymous PHP backend renders identical bytes for any session), the cookie carries no useful state for a scraper. **Best practice:** use a fresh `httpx.Client` per fetch (no cookie persistence). This also avoids the theoretical case where USCG ties rate limits to PHPSESSID and we self-throttle by pinning to one session.

## N. Cookie + cache pragma summary → polite-scraper UA recommendation

USCG's response posture (no Last-Modified, no ETag, no-cache headers, PHP session) is consistent with a Apache/PHP-rendered static-content site that has no CDN and no specific abuse-mitigation surface. Polite-scraper conventions therefore land on:

- **User-Agent:** `consumer-product-recalls/<version> (contact: adriannesta@gmail.com)` — identifies project + provides operator contact for misbehavior reports. Do not impersonate a browser (no Firefox/Chrome string).
- **Throttle:** `scrape_delay_seconds=1.0` between fetches (configurable per source via YAML). For ~1,834 fetches (initial seed) = ~30 min total wall time, acceptable for a one-time deep-rescan.
- **No concurrency:** serial walks only. The total request budget is bounded by throttle × fetch count.
- **Honor `Retry-After` on 429** via base-class `RateLimitError` machinery; no 429s observed across the ~10 probe requests.
- **No persistent session** — fresh `httpx.Client` per fetch discards cookies.

---

## Findings calibration to Step 2

Each finding maps to a Step 2 design decision. Cross-reference for review:

| Finding | Step 2 artifact |
|---|---|
| A | `expected_columns` in `config/sources/uscg.yaml` |
| B | `UscgRecallRecord` schema field list in `src/schemas/uscg.py` |
| C | `identity_fields=("source_recall_id",)` in extractor (with Step 1.5 corpus verification) |
| D | No `hash_exclude` needed for render-stability reasons |
| E | `last_date` kept in `content_hash` for now; revisit after Step 3 |
| F | Two `BeforeValidator`s: `_UscgListingDate` + `_UscgDetailsDate` |
| G | Step 2 added `_check_year_prefix_consistency` invariant; Step 3 **removed** it after corpus-wide evidence (218/1763 violations) falsified the hypothesis |
| H | UA + throttle defaults |
| I | No bronze-side decoding; preserved verbatim per ADR 0027 |
| J | Future optimization, captured for Phase 6+ |
| K | No HTTP-level short-circuit; `_record_run` populates `response_status_code` from page 0 fetch but leaves `response_etag`/`response_last_modified` NULL |
| L | Stop condition: empty `id` → break; safeguard: 200-page cap |
| M | Fresh `httpx.Client` per fetch; no cookie persistence |
| N | Polite-scraper UA + throttle + serial-walk pattern |

---

## O. Listing-side Unix-epoch sentinel for "no opened date" (Step 3, 2026-05-17)

USCG's listing page renders the literal string `1970-01-01` in the Opened On column when no opening date is recorded for a recall, while the corresponding **details page** leaves Case Open Date empty (blank `<span>` cell). The same logical "no date known" semantic gets two different encodings depending on which page you look at.

**Evidence:** R2 byte-inspection of listing page 31 via `scripts/uscg/inspect_landing_ndjson.py --scan-listings-for 22MF0628`:

```
--- lines 423..439 (match at 431) ---
     423            <td><a class="iframe" href="recalls-details.php?id=22MF0627">22MF0627</a></td>
     424            <td>YDV</td>
     425            <td>BOMBARDIER RECREATIONAL PRODU</td>
     426            <td></td>
     427            <td></td>
     428            <td>1970-01-01</td>           ← sentinel
     429          </tr>
     430                  <tr class="defaultFont">
>>   431            <td><a class="iframe" href="recalls-details.php?id=22MF0628">22MF0628</a></td>
     432            <td>YDV</td>
     433            <td>BOMBARDIER RECREATIONAL PRODU</td>
     434            <td></td>
     435            <td></td>
     436            <td>1970-01-01</td>           ← sentinel
     437          </tr>
     438                  <tr class="defaultFont">
     439            <td><a class="iframe" href="recalls-details.php?id=22MF0629">22MF0629</a></td>
```

`22MF0627`, `22MF0628`, `22MF0629` all share the literal `1970-01-01` string in the listing's Opened On column, all from the same manufacturer (Bombardier `YDV`). The corresponding details pages (verified for 22MF0628 via `--recall-number 22MF0628`) show `Case Open Date:` literally empty.

**Corpus scale:** ~45+ rows in the 2026-05-17 extraction had `opened_on = 1970-01-01`. The exact distribution is in `diagnose_rejections.sql` Q6's `prefix → opened_on_year=1970` cluster.

**Implications:**
- Bronze captures verbatim per ADR 0027 — `opened_on=1970-01-01` lands as `1970-01-01 UTC`. The corresponding `case_open_date` (details-page-derived) lands as NULL because the cell is empty.
- The same logical date thus shows two different bronze values for ~45 rows. This is silver's problem to normalize, not bronze's.
- Silver `stg_uscg_recalls.sql` should map `opened_on = 1970-01-01` → NULL (canonical "no date known"), then prefer `case_open_date` when both are populated. Document as known fragmentation in the silver staging model.

NHTSA's `ODATE 19010101` sentinel (`documentation/nhtsa/flat_file_observations.md` Finding H) is structurally identical — USCG just picked a different epoch.

## P. `company_name` is corpus-nullable (Step 3, 2026-05-17)

Finding A's 38-row sample showed 38/38 populated `Company Name` cells. **Step 3 first-extraction surfaced 33/1763 (~1.9%) corpus rows with empty `Company:` cells** — Pydantic-quarantined under the Step 2 schema's `company_name: str` requirement. Examples: `20SD0027`, `160001S`, `040099T`, `030119T`, `960112T`, `920542T`, `950359T`, `950354T`, `950355T`, `930140T` — mostly pre-2005 historical entries where USCG didn't record the manufacturer name.

**R2 byte-confirmation** via `scripts/uscg/inspect_landing_ndjson.py --recall-number 920542T --show-field "Company"`:

```
--- lines 27..35 (match at 31) ---
      27    <tr>
      28      <td><span class="defaultFont"><strong>Company:</strong></span></td>
      29      <td><span class="defaultFont"></span></td>     ← empty cell
      30      <td>&nbsp;</td>
>>    31      <td><span class="defaultFont"><strong>Company Official:</strong></span></td>
      32      <td><span class="defaultFont">PDE</span></td>
```

The HTML cell is literally empty — not a parser bug. USCG genuinely has no Company value for these rows.

**Decision:** `company_name` made nullable in `src/schemas/uscg.py` + migration 0013 (Step 3 follow-up edit). Same shape as the earlier `opened_on` correction (Finding A scope caveat) — Step 1's small-sample claim of "always populated" didn't hold at corpus scale.

## Q. Listing pages contain non-UTF-8 bytes (Step 3, minor)

Surfaced incidentally during R2 inspection: at least one listing page (page 31 archived 2026-05-17) contains byte `0xbc` (`¼` in Latin-1 / Windows-1252) embedded in a UTF-8-declared page. Common pattern from Word copy-paste sources on `.gov` static pages. The production parser handles this transparently via BeautifulSoup's `lxml` backend (encoding auto-detection from `<meta>` + content sniffing); the forensic inspector originally choked on a naive `bytes.decode("utf-8")` and was fixed to use `errors="replace"`.

Not blocking for any data-quality decision; documented because the inspector now renders `�` in those positions, which is itself a useful diagnostic signal if it ever crops up in narrative text fields that bronze captures verbatim.

## R. Disposition value case-inconsistency (Step 3, silver-layer concern)

`Disposition` is rendered by USCG with inconsistent letter case across the corpus. Step 3's clean extraction (`explore_first_extraction.sql` Q6) shows:

| Disposition value | Count | Share |
|---|---|---|
| `Closed` | 1,476 | 83.7% |
| `Open` | 190 | 10.8% |
| `CLOSED` | 95 | 5.4% |
| `OPEN` | 2 | 0.1% |

The lowercase-modal forms (`Closed`/`Open`) dominate but the upper-case forms (`CLOSED`/`OPEN`) account for ~5.5% of the corpus. Likely a generational difference in USCG's CMS — pre-some-date entries used uppercase; the modern UI title-cases.

Bronze stores verbatim per ADR 0027. **Silver `stg_uscg_recalls.sql` will normalize** — `lower(disposition)` or canonical title-case + `accepted_values: ['open', 'closed']` (or whatever convention the cross-source `recall_event.status` enum settles on). Same shape as the `mfgname` / `companyname` normalization NHTSA does in its staging model.

## S. Null-firm-anchor cluster (Step 3, Phase 6 entity-resolution implication)

`explore_first_extraction.sql` Q7 surfaced **23 bronze rows where BOTH `mic` AND `company_name` are NULL**. These recalls have no manufacturer code AND no manufacturer name — the row carries `source_recall_id`, `opened_on`, and miscellaneous downstream fields but no firm anchor at all.

Combined with the broader corpus null rates (`null_mic=120`, `null_company_name=33`), the intersection of "no MIC" and "no company" suggests these are pre-USCG-CMS records imported from an older system without the firm metadata fields populated. The 23 rows are a subset of the 120 null-MIC and (partial overlap with) the 33 null-company-name rows.

**Phase 6 firm-entity-resolution implication:** these 23 rows cannot be matched to any `firm` via the cross-source resolution logic (ADR 0022). Three plausible silver-layer treatments:

1. **Synthetic "UNKNOWN" firm anchor.** Assign these rows a deterministic placeholder firm-id (e.g., `firm_id = md5('USCG' || source_recall_id)`). Preserves row count, separates true-unknown from real-firm cases in downstream queries. Recommended.
2. **Drop from silver.** Lose ~1.3% of corpus to maintain firm-cardinality cleanliness. Aggressive; loses real recalls.
3. **Soft-fail in entity resolution.** Let the rows land in silver with `firm_id = NULL`; downstream gold queries `JOIN` to firm via OUTER and the rows appear in counts but not firm rollups. Cleanest tradeoff for v1.

Decision deferred to Phase 6 silver landing — document the count + the three options here so it's not a surprise then.

---

## Open items deferred to Step 1.5 / Step 3

- **Step 1.5** (after Step 2 extractor lands, before first formal extraction): a focused script that walks all 71 pages once, asserts `source_recall_id` is corpus-unique (Finding C). Originally Step 1.5 also included the year-prefix invariant check (Finding G) — that resolved in Step 3 by falsification (see Finding G above).
- **Step 3** (first formal extraction + bronze findings): ✓ completed 2026-05-17. Findings G (replaced), O, P, Q, R, S landed. `opened_on` `last_date` drift monitoring (Finding E) deferred to subsequent runs.
- **Phase 5d Step 5** (silver `stg_uscg_recalls.sql`): must handle Finding O (`opened_on = 1970-01-01` → NULL), Finding R (disposition case-normalization), and Finding S (null-firm-anchor decision — recommend option 3 soft-fail). Capture these as the staging model's documented invariants.
- **Phase 6** (firm entity resolution): Finding S informs how USCG rows participate in cross-source firm rollups.
- **Phase 6+** (smart-skip layer): `Records Found` total as a steady-state short-circuit signal (Finding J).
