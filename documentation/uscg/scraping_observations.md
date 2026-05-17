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

## G. Year-prefix invariant — DEFER to Step 1.5

The plan asks: does `source_recall_id[:2]` always match `opened_on.year % 100`? Two-sample check:

| Recall | `source_recall_id` | Prefix | `opened_on` | Year-suffix | Match? |
|---|---|---|---|---|---|
| 26MF0158 | `26MF0158` | `26` | `2026-03-03` | `26` | ✅ |
| 25CG0017 | `25CG0017` | `25` | `2025-06-04` | `25` | ✅ |

Defer corpus-wide confirmation to Step 1.5. If 100% adherence holds, enforce as a reject-on-mismatch invariant in `check_invariants`. If exceptions appear (e.g., recall filed late-December coded with next-year prefix), demote to log-and-pass with a quarantine warning row.

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
| G | `check_year_prefix_consistency` invariant added with Step 1.5 calibration |
| H | UA + throttle defaults |
| I | No bronze-side decoding; preserved verbatim per ADR 0027 |
| J | Future optimization, captured for Phase 6+ |
| K | No HTTP-level short-circuit; `_record_run` populates `response_status_code` from page 0 fetch but leaves `response_etag`/`response_last_modified` NULL |
| L | Stop condition: empty `id` → break; safeguard: 200-page cap |
| M | Fresh `httpx.Client` per fetch; no cookie persistence |
| N | Polite-scraper UA + throttle + serial-walk pattern |

---

## Open items deferred to Step 1.5 / Step 3

- **Step 1.5** (after Step 2 extractor lands, before first formal extraction): a focused script that walks all 71 pages once, asserts (a) `source_recall_id` is corpus-unique (Finding C), (b) year-prefix invariant holds across the corpus (Finding G). Captures findings into this document as appendix entries.
- **Step 3** (first formal extraction + bronze findings): monitor `last_date` drift across consecutive runs (Finding E). If `last_date` is a re-stamping field, add to `hash_exclude_fields` and document as a re-baseline event.
- **Phase 6+** (smart-skip layer): `Records Found` total as a steady-state short-circuit signal (Finding J).
