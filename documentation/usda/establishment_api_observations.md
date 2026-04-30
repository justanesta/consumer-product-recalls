# USDA FSIS Establishment Listing API — Empirical Observations

> **Status: Exploration complete.** All findings confirmed 2026-04-29 via Bruno collection
> in `bruno/usda/establishment_exploration/`.

## Background

USDA FSIS publishes a separate establishment listing endpoint at
`https://www.fsis.usda.gov/fsis/api/establishments/v/1` providing demographic data for all
FSIS-regulated establishments (meat, poultry, egg processors). The dataset is updated weekly
on Monday or Tuesday.

The primary ETL motivation for exploring this API is **recall enrichment**: the recall API's
`field_establishment` field (free text) can potentially be joined to `establishment_name` in
this API to attach address, geolocation, size, and activity data to recall records.

Pre-extraction exploration was conducted via the Bruno collection in
`bruno/usda/establishment_exploration/` on the `feature/explore-usda-api` branch.

The highest-priority unknowns before building the extractor are:
1. Whether the API paginates (changes extractor architecture significantly)
2. Whether `field_establishment` values from recalls reliably match `establishment_name` here
3. Whether to pull all establishments or only active MPI directory entries

---

## Base URL and Authentication

- **Base URL:** `https://www.fsis.usda.gov/fsis/api/establishments/v/1`
- **Active MPI shortcut:** `https://www.fsis.usda.gov/fsis/api/establishments/mpi`
- **Auth:** None — unauthenticated public API.
- **Update cadence:** Weekly, Monday or Tuesday.

---

## Response Shape

### Finding A — Response is a flat JSON array; no pagination; no ETag

Confirmed 2026-04-29 via `establishment_exploration/get_all_establishments_cardinality.yml`.

Response is a bare flat JSON array — no pagination envelope, no `_links`, `pager`, `meta`, `next`,
`page_count`, or `total` keys. The entire 7,945-record dataset is returned in one response.

**No ETag and no CDN caching** — critical contrast with the recall API:

| Header | Recall API (full dump) | Establishment API |
|---|---|---|
| `etag` | Present (`"1777472976"`) | **Absent** |
| `cache-control` | `public, max-age=3100` | `max-age=0, no-cache, no-store` |
| `x-drupal-dynamic-cache` | varies | `UNCACHEABLE (poor cacheability)` |
| `transfer-encoding` | — | `chunked` (no `content-length`) |

**The ETag conditional-GET optimization used by `UsdaExtractor` is NOT available here.**
Every extraction run must download the full dataset. At 7,945 records this is acceptable — the
recall API's ETag optimization was motivated by its 12 MB payload; the establishment dataset is
smaller. Content-hash dedup (ADR 0007) handles idempotency on the loader side.

---

## Dataset Cardinality

### Finding B — Total establishment count and active MPI split

| Metric | Count |
|---|---|
| Total establishments (full unfiltered response) | 7,945 |
| Active MPI establishments (`status_regulated_est=""`) | 7,168 (90.2%) |
| Inactive / other establishments | 777 (9.8%) |
| Wire size | unknown — `transfer-encoding: chunked`, no `content-length` header |

Confirmed 2026-04-29 via cardinality probe.

**Implication for extractor scope:** 777 establishments (9.8%) are inactive. Recalled establishments
may no longer be active at time of extraction. The extractor must pull ALL establishments (full dump,
no filter) to ensure historical recall records can be enriched. Active MPI filter is useful for
building a "currently regulated" view in silver but not for the join enrichment use case.

---

## Field Serialization

### Finding C — `activities`, `dbas`, and `geolocation` types

Confirmed 2026-04-29 via cardinality probe.

**`activities` field:**
- Observed: true JSON array — e.g., `["Meat Processing", " Poultry Processing"]`
- **Leading space on items after the first** — array elements beyond index 0 may have a leading
  space (e.g., `" Poultry Processing"`). The extractor transformer must call `.strip()` on each
  element. Use `List[str]` in Pydantic with a validator that trims whitespace.
- Can be an empty array `[]` (establishment with no documented activities).

**`dbas` field:**
- Observed: true JSON array — e.g., `["Long Phung Food Products"]`, `["Royalton Meats", " Sharon Beef"]`
- Same leading-space issue on elements after index 0 — strip whitespace.
- Can be an empty array `[]`.

**`geolocation` field:**
- **When populated:** single lat,lng string — e.g., `"29.83860699, -95.47217297"`
- **When missing: boolean `false`** — NOT `null`, NOT `""`. The inactive DNATA record shows
  `"geolocation": false`. This is a critical schema concern — the field is either a `str` or
  the literal `False`. Pydantic validator must handle both and normalize to `Optional[str]`.

**`county` field:**
- Same `false` sentinel pattern as `geolocation` — the DNATA inactive record shows `"county": false`.
- When populated: string e.g., `"Harris County"`, `"Tarrant County"`.
- Pydantic validator must accept `str | Literal[False]` and normalize `false` → `None`.

**`status_regulated_est` observed values (confirmed exhaustive across all 7,945 records):**
- `""` (empty string) — active MPI establishment (7,168 records)
- `"Inactive"` — inactive establishment (777 records); no third value observed

---

## Field Nullability Map

### Finding D — Field presence rates

Confirmed 2026-04-29 via cardinality probe (n=7,945). Empty counts include both `""` (empty string) and `false` (boolean) missing sentinels.

| Field | Empty/false count | Rate | Notes |
|---|---|---|---|
| `establishment_name` | 0 | 0.0% | Always present — safe `str` required field |
| `address` | 0 | 0.0% | Always present |
| `state` | 0 | 0.0% | Always present — 2-letter abbreviation |
| `zip` | 0 | 0.0% | Always present |
| `phone` | 311 | 3.9% | `Optional[str]` |
| `duns_number` | 6,794 | 85.5% | Almost always empty — `Optional[str]` |
| `county` | 122 | 1.5% | Can be `false` (boolean) or empty string — normalize to `Optional[str]` |
| `fips_code` | 339 | 4.3% | `Optional[str]` |
| `geolocation` | ~122+ | ~1.5%+ | Can be `false` (boolean) — normalize to `Optional[str]` |
| `LatestMPIActiveDate` | 0 | 0.0% | **100% populated on ALL records** including inactive |
| `size` | unknown | unknown | `""` on inactive records; values: Large/Small/Very Small |
| `district` | unknown | unknown | `""` on inactive records; 2-digit string |
| `circuit` | unknown | unknown | `""` on inactive records |

---

## Name Filter Behavior

### Finding E — `name` filter: comma causes OR token split; quotes required for phrase match

Confirmed 2026-04-29 via `establishment_exploration/get_establishment_by_name.yml` (both variants).

The `name` parameter matches `establishment_name` using a "contain" method (per PDF Appendix A),
but the comma character triggers token splitting with OR semantics.

**Without URL-encoded quotes (`name=CS Beef Packers, LLC`):**
- Records returned: **2,481+** (essentially a large fraction of the full 7,945-record dataset)
- The comma splits the value into tokens: `CS Beef Packers` and ` LLC`. The API performs an OR
  match — any establishment whose name contains "LLC" is returned. Completely unusable for joins.

**With URL-encoded quotes (`name=%22CS%20Beef%20Packers%2C%20LLC%22`):**
- Records returned: **1** — clean 1:1 phrase match.
- `establishment_id=6163082`, `establishment_number=M630`, `establishment_name="CS Beef Packers, LLC"`,
  city=Kuna, state=ID. Status: active MPI (`status_regulated_est=""`).

**Case sensitivity:** Not directly tested, but the quoted phrase match worked with exact case.

**Critical implication for ETL join strategy:** URL-encoded double quotes are **required** for
any programmatic join. The extractor must always construct the name filter as
`name=%22{url_encoded_value}%22` — never pass the raw `field_establishment` value without wrapping.
For names without commas, the unquoted form might work as a substring match, but quote-wrapping
is the safe universal strategy.

---

## Recall-Establishment Join Fidelity

### Finding F — `field_establishment` → `establishment_name` match via quoted `name` filter: 1:1

Confirmed 2026-04-29 via `establishment_exploration/get_establishment_by_name.yml` (quoted variant;
functionally equivalent to `probe_recall_establishment_join.yml` with the same env var).

The core ETL question: does the free-text `field_establishment` value in recall records reliably
match `establishment_name` in the establishment API?

**Test case:** `field_establishment="CS Beef Packers, LLC"` (user-confirmed recall record)
- Establishments returned: **1** — clean 1:1 match.
- Match via `establishment_name`: **YES** — exact match on official grant name.
- Match via `dbas`: **N/A** — `dbas=[]` (this establishment has no DBA names).
- `establishment_id` of match: **6163082** — stable FK for bronze schema (`source_establishment_id`)
- `establishment_number`: M630 (M = meat grant)
- `status_regulated_est`: `""` → **ACTIVE MPI** as of 2026-04-27

**Observed join reliability:** 1:1 for this test case. Match was via `establishment_name`, not `dbas`.

**Implication:** For the one confirmed test case, the join strategy works perfectly: recall
`field_establishment` → quoted `name=%22...%22` filter → exactly 1 establishment record.
The DBA fallback (`field_dbas_value` filter) was not needed here, but remains necessary for
cases where the recall uses a DBA name rather than the official `establishment_name`.

**Extractor join pattern:**
```
GET /fsis/api/establishments/v/1?name=%22{url_encoded_field_establishment}%22
→ expect 0 or 1 results
→ 0 results: retry with field_dbas_value filter, or mark as unresolvable
→ 1 result: use establishment_id as foreign key
→ 2+ results: use state/establishment_number to disambiguate
```

---

## `LatestMPIActiveDate` Behavior

### Finding G — `LatestMPIActiveDate` semantics confirmed

Confirmed 2026-04-29 via cardinality probe and active MPI probe.

`LatestMPIActiveDate` appears in the API response but is not documented in the data documentation PDF.
Observed behavior reveals its semantics:

- **Active MPI establishments:** `LatestMPIActiveDate` = date of the most recent weekly MPI directory
  update (all sampled active records showed `2026-04-27` — consistent with weekly Mon/Tue refresh cadence).
- **Inactive establishments:** `LatestMPIActiveDate` = the last date the establishment was present in
  the active MPI directory before going inactive (e.g., DNATA record: `2023-08-21`).

In effect: "When was this establishment last confirmed as active in the MPI directory?"

- Present on ALL 7,945 records (100%) — both active and inactive.
- Format: YYYY-MM-DD.
- **Useful for the extractor:** for inactive establishments, this field tells you approximately when they
  stopped being active regulated establishments — useful context for recall enrichment (a 2023 recall
  against an establishment with `LatestMPIActiveDate="2023-08-21"` was likely active at time of recall).

---

## Extraction Strategy

Confirmed strategy based on Findings A–G.

**Full dump, no filter (confirmed Finding A — no pagination, no ETag).**
`UsdaEstablishmentExtractor.extract()`: `GET /fsis/api/establishments/v/1` with no filter
parameters. All 7,945 establishments returned in a single flat JSON array.

**No ETag optimization available (contrast with recall API).** Every extraction run downloads
the full dataset. At ~7,945 records this is acceptable. Content-hash dedup (ADR 0007) handles
idempotency on the loader side.

**Extraction scope: full dataset, not active-MPI filter.** The `status_regulated_est_value_1_op=empty`
shortcut excludes 777 inactive establishments (9.8%) that may still be referenced by historical recalls.

**Join enrichment cadence:** Establishment data updates weekly (Mon/Tue). The recall extractor
runs more frequently. The silver join model should use the most recent establishment snapshot.

**Join pattern for silver model:**
```sql
name=%22{url_encoded_field_establishment}%22  -- quotes required; comma triggers OR token split
→ 0 results: retry with field_dbas_value filter, or mark unresolvable
→ 1 result: join on establishment_id (source_establishment_id in bronze)
→ 2+ results: disambiguate by state, establishment_number
```

---

## Implications for Pydantic Schema

- `establishment_id`: `str` — serialized as string in JSON (e.g., `"6163082"`); stable FK to other FSIS datasets; use as `source_establishment_id` in bronze schema
- `establishment_name`: `str` — always present (0% empty); join key from recall `field_establishment`
- `establishment_number`: `str` — format [M/P/I/G/V]+digits+optional suffix; multi-grant joined with `+`
- `activities`: `List[str]` — true JSON array; strip leading/trailing whitespace from each element
- `dbas`: `List[str]` — true JSON array; strip leading/trailing whitespace from each element; can be `[]`
- `geolocation`: `Optional[str]` — **field value can be boolean `false` in JSON**; validator must accept
  `Union[str, Literal[False]]` and normalize `false` → `None`
- `county`: `Optional[str]` — same `false` sentinel issue as `geolocation`; normalize `false` → `None`
- `grant_date`: `Optional[str]` — YYYY-MM-DD format
- `size`: `Optional[str]` — observed values: `"Large"`, `"Small"`, `"Very Small"`, `"N / A"`, `""` (inactive/incomplete records)
- `district`: `Optional[str]` — 2-digit string (e.g., `"40"`); `""` on inactive records
- `circuit`: `Optional[str]` — 4-digit FSIS or 2-digit+2-letter Talmage-Aiken; `""` on inactive
- `status_regulated_est`: `str` — `""` = active MPI; `"Inactive"` = inactive (other values may exist)
- `LatestMPIActiveDate`: `str` — 100% populated on all records; YYYY-MM-DD format
- `address`, `state`, `zip`: `str` — always present (0% empty)
- `phone`: `Optional[str]` — 3.9% empty
- `duns_number`: `Optional[str]` — 85.5% empty
- `fips_code`: `Optional[str]` — 4.3% empty; 5-digit county FIPS when present

---

## Open Items

- [x] Confirm pagination behavior (Finding A) — confirmed 2026-04-29: flat array, no pagination; no ETag (contrast with recall API); no CDN caching; chunked transfer
- [x] Confirm total cardinality and active MPI count (Finding B) — confirmed 2026-04-29: 7,945 total, 7,168 active MPI (90.2%), 777 inactive (9.8%)
- [x] Confirm `activities` and `dbas` serialization format (Finding C) — confirmed 2026-04-29: true JSON arrays; leading spaces on non-first elements; `geolocation` and `county` use boolean `false` as missing sentinel
- [x] Confirm field nullability rates (Finding D) — confirmed 2026-04-29: `establishment_name`/`address`/`state`/`zip` 100% populated; `duns_number` 85.5% empty; `LatestMPIActiveDate` 100% populated
- [x] Confirm `name` filter behavior: contain vs exact, quote effect (Finding E) — confirmed 2026-04-29: comma triggers OR token split (2,481+ results without quotes); URL-encoded quotes enforce phrase match (1 result); quotes REQUIRED for joins
- [x] Validate recall-establishment join fidelity (Finding F) — confirmed 2026-04-29: `field_establishment="CS Beef Packers, LLC"` → 1:1 match via `establishment_name`, `establishment_id=6163082`, status ACTIVE MPI
- [x] Confirm `LatestMPIActiveDate` population pattern (Finding G) — confirmed 2026-04-29: 100% populated on ALL 7,945 records including inactive
- [x] Confirm `status_regulated_est` full value enumeration — `""` (7,168 active MPI) and `"Inactive"` (777) account for all 7,945 records from the cardinality probe. No third value observed. The active MPI probe confirmed all 7,168 active records have exactly `""`. Treat as two-value enum: `""` = active, `"Inactive"` = inactive.
