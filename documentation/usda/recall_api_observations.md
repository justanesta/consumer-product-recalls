# USDA FSIS Recall API — Empirical Observations

> **Status: Exploration complete.** All primary findings confirmed 2026-04-29 via Bruno collection
> in `bruno/usda/`. Two items deferred (Finding E, Finding M) — see Open Items.

## Background

USDA FSIS publishes a single REST endpoint at `https://www.fsis.usda.gov/fsis/api/recall/v/1`
returning food safety recall records. Pre-extraction API exploration was conducted via the Bruno
collection in `bruno/usda/` on the `feature/explore-usda-api` branch, following the same
empirical process used for CPSC (`documentation/cpsc/`) and FDA (`documentation/fda/`).

The highest-priority unknown before building the extractor is whether `field_last_modified_date`
can be used as a server-side filter parameter — the entire incremental extraction strategy
depends on the answer (see Finding D below).

---

## Base URL and Authentication

- **Base URL:** `https://www.fsis.usda.gov/fsis/api/recall/v/1`
- **Auth:** None — unauthenticated public API. No API key, OAuth, or signed requests required.
- **HTTP method:** GET only.
- **No `signature=` cache-busting required** (unlike FDA iRES).

---

## Response Shape

### Finding A — Response is a flat JSON array; no pagination

Confirmed 2026-04-29 via `data_exploration/get_all_recalls_cardinality.yml`.

The response is a bare JSON array — no pagination envelope, no `_links`, `pager`, `meta`, `next`,
`page_count`, or `total` keys. The entire dataset is returned in a single response.

Wire size: `content-length: 1,641,691` bytes compressed (~12 MB uncompressed in Bruno).
Stack: Drupal 10 / nginx 1.22.1 / PHP 8.3 / Akamai CDN.

**Akamai Bot Manager:** Every response sets `ak_bmsc` cookie (Akamai bot detection fingerprinting).
Monitor for extraction blocking in production — similar risk surface to FDA's HTML apology throttle
(finding N in `documentation/fda/api_observations.md`), though the mechanism differs.

**ETags present:** `etag: "1777472976"` + `last-modified: Wed, 29 Apr 2026 14:29:36 GMT`.
The extractor could use `If-None-Match` / `If-Modified-Since` conditional GETs to skip downloading
~12 MB when the dataset is unchanged since the last run. Worth implementing if per-run cost is a
concern, especially given the full-dump extraction strategy (see Finding D).

**CDN caching:** `cache-control: public, max-age=3100` (~51 min). Repeated identical requests
within the window return a cached copy. Rapid back-to-back identical requests are safe; the CDN
absorbs them. `x-drupal-dynamic-cache: UNCACHEABLE` indicates Drupal itself doesn't cache
filtered requests, but the CDN layer does via `cache-control`.

### Finding B — Total record cardinality

Confirmed 2026-04-29 via `data_exploration/get_all_recalls_cardinality.yml`.

| Metric | Count |
|---|---|
| Total records (English + Spanish combined) | 2,001 |
| English records | 1,212 |
| Spanish records | 789 |
| Unique recalls (= English count; every recall has an English version) | 1,212 |
| Bilingual recalls (have a Spanish companion) | 789 |
| English-only recalls | 423 |
| Archived records (`field_archive_recall=True`) | 1,829 (91.4%) |
| Active records (`field_archive_recall=False`) | 172 (8.6%) |

**The USDA FSIS dataset is tiny compared to other sources** (FDA ~134K, CPSC ~9,700).
The full 2,001-record dataset is 1.6 MB compressed. Pulling the full dataset on every extraction
run is computationally cheap — content-hash dedup in the bronze loader handles idempotency.

**91.4% of records are archived.** The default unfiltered response is almost entirely historical
data. Active recalls (172 records) are a small fraction. The deep-rescan workflow must include
archived records — they are valid historical data, not noise.

### Finding C — Field nullability map

Confirmed 2026-04-29 via `data_exploration/get_all_recalls_cardinality.yml` (n=2,001).

| Field | Empty count | Empty rate | Notes |
|---|---|---|---|
| `field_last_modified_date` | 845 | 42.2% | **Critical** — watermark field; nearly half the dataset has no value (see Finding D) |
| `field_en_press_release` | 2,001 | 100% | **Always empty** — dead field; exclude from schema or always `None` |
| `field_press_release` | 2,000 | 99.9% | Effectively always empty — treat as `Optional[str]`, expect `None` in practice |
| `field_distro_list` | 1,651 | 82.5% | Sparsely populated |
| `field_company_media_contact` | 979 | 48.9% | ~half populated |
| `field_states` | 576 | 28.8% | Missing on ~30% of records |
| `field_related_to_outbreak` | 501 | 25.0% | Missing on ~25% of records |
| `field_labels` | 272 | 13.6% | |
| `field_qty_recovered` | 172 | 8.6% | |
| `field_closed_year` | 177 | 8.8% | |
| `field_closed_date` | 168 | 8.4% | |
| `field_has_spanish` | 0 | 0% | Always populated — reliable boolean string |
| `field_active_notice` | ~189 | ~9.4% | **Empirical, post-Finding C addendum (2026-05-01):** the original cardinality probe did not check this field, so it was initially treated as required in the schema. Phase 5b first extraction surfaced 189/2001 records with empty-string values (`field_active_notice == ""`). Schema now treats this as `Optional[bool]`. Likely correlates with archived/closed records where the "active notice" concept no longer applies. |

**Pydantic schema implications:**
- `field_en_press_release` — always `""` in the dataset; declare as `Optional[str] = None` and exclude from content hash (dead field that may never populate)
- `field_press_release` — same posture as above
- All other fields with non-zero empty rates — `Optional[str]`
- `field_has_spanish` — always present; safe to declare as required `str` with `Literal["True", "False"]`

---

## field_last_modified_date: Filter Availability and Reliability (CRITICAL)

### Finding D — Server-side filter: IGNORED; full-dump is the only extraction strategy

Confirmed 2026-04-29 via `data_exploration/probe_last_modified_date_as_filter.yml`.

Both parameter name variants tested and confirmed ignored:
- `field_last_modified_date_value=2026-04-01` — 2,001 records returned, 0 matching filter date
- `field_last_modified_date=2026-04-01` — 2,001 records returned, 0 matching filter date

`field_last_modified_date` is a response field only. It is not a supported query parameter under
any naming convention. The API silently ignores unrecognized parameters and returns the full dataset,
consistent with the same behavior documented for CPSC (invalid parameter names → full dataset fallback).

This was already the expected conclusion from Finding C: 845 records (42.2%) have an empty
`field_last_modified_date`, so a date filter would have been incomplete even if it had worked.

**Production extraction strategy: full dump on every run.**
`UsdaExtractor.extract()` sends `GET /fsis/api/recall/v/1` with no filter parameters. The full
2,001-record dataset (~1.6 MB compressed) is returned in a single response. The bronze content-hash
loader (ADR 0007) handles idempotency — records whose content is unchanged since the last run are
no-op inserts. The `field_last_modified_date` field is still stored in the bronze row and used
client-side to populate the `source_last_modified_at` column for lineage purposes, but it cannot
drive the server-side query.

**ETag optimization (from Finding A):** The response includes `etag` and `last-modified` headers.
`UsdaExtractor.extract()` can send `If-None-Match: <last_etag>` on subsequent runs — if the dataset
is unchanged, the server returns `304 Not Modified` and the extractor skips processing entirely.
This is the most effective efficiency lever available for this source.

**Implication for `deep-rescan-usda.yml`:** There is no meaningful distinction between "incremental"
and "full rescan" for USDA — every run already pulls the full dataset. The deep-rescan workflow
exists for operational consistency with other sources and to provide a manual trigger for forced
re-ingestion, but its implementation is identical to the incremental extractor.

### Finding E — field_last_modified_date reliability on edits

**Deferred — not probed during initial exploration.**

Does `field_last_modified_date` reliably advance when FSIS amends a recall record?
This requires identifying a recall known to have been edited and comparing before/after values.
Not tested during the `feature/explore-usda-api` branch.

Since `field_last_modified_date` cannot be used as a server-side filter regardless (Finding D),
this question affects only client-side freshness detection — not the extraction architecture.
If unreliable, the `deep-rescan-usda.yml` workflow becomes the primary re-ingestion mechanism
rather than a safety net (same conclusion as ADR 0023 for FDA).

---

## Bilingual Content Model

### Finding F — Bilingual pair structure

Confirmed 2026-04-29 via `data_exploration/get_bilingual_pair.yml` (recall `004-2020`).

- A single `field_recall_number` query returns exactly **2 records** — one `langcode=English`, one `langcode=Spanish`.
- Both records share the **same `field_recall_number`** (`004-2020`).
- **`field_last_modified_date` is identical across both language versions** (`2020-05-20`). FSIS updates both records atomically — the watermark moves in sync. This means content-hash dedup will catch changes to either language version on the same run.
- **`field_has_spanish` is `"True"` on both the English AND Spanish record** — it is not a flag that distinguishes which version is the English original. It simply signals "this recall has a bilingual pair." Use `langcode` to distinguish EN from ES.

**Fields that differ between EN/ES versions:**
- `field_title` — translated
- `field_recall_url` — different paths (`/recalls-alerts/...` for EN, `/es/retirada/...` for ES)
- `field_product_items` — translated
- `field_summary` — full HTML body translated

**Fields identical across EN/ES versions** (all factual/metadata fields):
`field_recall_number`, `field_recall_date`, `field_last_modified_date`, `field_closed_date`,
`field_closed_year`, `field_year`, `field_recall_classification`, `field_risk_level`,
`field_recall_type`, `field_recall_reason`, `field_processing`, `field_states`,
`field_establishment`, `field_labels`, `field_qty_recovered`, `field_active_notice`,
`field_archive_recall`, `field_related_to_outbreak`, `field_has_spanish`,
`field_media_contact`, `field_company_media_contact`

**Implication for `check_invariants()`:** The bilingual dedup in `src/bronze/invariants.py`
keys on `field_recall_number` and `langcode`. Spanish records are companion translations,
not distinct recall events. `source_recall_id` in bronze = `field_recall_number` (language-agnostic).
The invariant should assert: for each `field_recall_number`, at most one `langcode=English` and
at most one `langcode=Spanish` record. A `langcode=Spanish` record with no matching English
record is a data anomaly worth quarantining.

### Finding G — Recalls with no Spanish version

Partially confirmed via Finding F and cardinality probe.

- `field_has_spanish` is never empty (0/2,001 empty per Finding C) — always populated.
- For bilingual recalls, `field_has_spanish="True"` on **both** language versions (confirmed).
- English-only recalls: `field_has_spanish="False"` — inferred from the 423 English records with no Spanish companion (1,212 English − 789 Spanish = 423 English-only). Empirically confirm via `probe_archive_behavior.yml` or lookup on a known English-only recall.
- A Spanish record without a corresponding English record has not been observed but has not been explicitly tested.

### Finding H — Undocumented response field: `field_recall_url`

Confirmed 2026-04-29 via bilingual pair payload.

The API returns a `field_recall_url` field not listed in the PDF documentation:
- English: `http://www.fsis.usda.gov/recalls-alerts/<slug>`
- Spanish: `http://www.fsis.usda.gov/es/retirada/<slug>`

The URL differs between language versions (different path prefixes and slugs). This field should
be added to the Pydantic bronze schema as `Optional[str]` — it may not be present on all records
(PDF did not document it, suggesting it was added after the docs were written).

---

## Primary Key Semantics

### Finding I — field_recall_number as primary key

Confirmed 2026-04-29 via `lookup/get_recall_by_recall_number.yml` (recall `004-2020`).

- **Lookup returns the full bilingual pair.** Passing `field_recall_number=004-2020` returned exactly 2 records — `langcode=English` and `langcode=Spanish`. The lookup correctly retrieves both language versions in a single call.
- **Both records share `field_recall_number="004-2020"` and `field_last_modified_date="2020-05-20"`.** Consistent with Finding F (bilingual pairs are atomically updated).
- **`field_recall_number` filter behavior: no spurious matches observed.** Only the 2 expected records were returned. The PDF documents this as a text/contains filter, but in practice a full `DDD-YYYY` value like `004-2020` will never appear as a substring in another recall number — exact vs contains is a distinction without a difference for the extractor.
- **No separate numeric primary key.** `field_recall_number` is the only identifier in the response schema. The natural composite key for `check_invariants()` is `(field_recall_number, langcode)`.
- **`field_recall_number` format exceptions confirmed.** Documented format is `DDD-YYYY`. Raw API output includes `PHA-MMDDYYYY-NN` format for Public Health Alerts. Treat as opaque `str` in the Pydantic schema.
- **Filtered lookup requests are not CDN-cached.** Response headers: `cache-control: max-age=0, no-cache, no-store`, `x-drupal-dynamic-cache: UNCACHEABLE (poor cacheability)`. Consistent with Finding J.

---

## Archive Behavior

### Finding J — Archived records and selection filter behavior

Confirmed 2026-04-29 via `data_exploration/probe_archive_behavior.yml` (`field_archive_recall=1`).

**`field_archive_recall=1` filter works server-side** — returned exactly 1,829 records, matching the
archived count from the cardinality probe. All returned records had `field_archive_recall=True`.
This confirms that Appendix A selection parameters (integer IDs) are genuine server-side filters,
unlike `field_last_modified_date` which is silently ignored (Finding D).

**Archived records are included in the default no-filter response** (confirmed via cardinality probe —
1,829 of 2,001 total records are archived). The extractor must ingest archived records; they are
valid historical data.

**`field_last_modified_date` population in archived records:**
- Populated: 986 / 1,829 (53.9%)
- Empty: 843 / 1,829 (46.1%)

Cross-referencing with cardinality probe totals (845 empty across all 2,001 records):
843 of 845 records with empty `field_last_modified_date` are archived. Only 2 active records
lack a date. The empty-date pattern is almost entirely an artifact of older archived records
predating when FSIS began populating the field — not a random missingness pattern.

**Active vs archived bilingual breakdown (derived):**

| | English | Spanish | Total |
|---|---|---|---|
| Archived | 1,045 | 784 | 1,829 |
| Active | 167 | 5 | 172 |
| Total | 1,212 | 789 | 2,001 |

Only 5 active recalls have Spanish translations. Active recalls are almost entirely English-only —
translations appear to be added later, or not at all for newer events. The `check_invariants()`
bilingual dedup should not assume that every active English record has a Spanish companion.

**Filtered requests are not CDN-cached** — filtered requests (`?field_archive_recall=1` etc.)
return `cache-control: max-age=0, no-cache, no-store` and `x-drupal-cache: UNCACHEABLE (request policy)`.
The ETag conditional-GET optimization (Finding A) only applies to the no-filter full-dump request,
which has `cache-control: public, max-age=3100`. Filtered requests always go to origin.

---

## Deep Rescan Strategy (field_year_id)

### Finding K — Drupal taxonomy ID mappings (from Appendix A of PDF documentation)

Confirmed from documentation (no empirical probe needed — IDs are fully documented in Appendix A).

**`field_year_id` (Recall Issue Year) — filters by the year the recall was issued (`field_recall_date` year). Immutable.**

| Year | ID | Year | ID | Year | ID |
|---|---|---|---|---|---|
| 1970 | 470 | 1997 | 231 | 2014 | 177 |
| 1980 | 469 | 1998 | 230 | 2015 | 6 |
| 1990 | 468 | 1999 | 229 | 2016 | 5 |
| 1991 | 467 | 2000 | 228 | 2017 | 4 |
| 1992 | 466 | 2001 | 227 | 2018 | 3 |
| 1993 | 465 | 2002 | 226 | 2019 | 2 |
| 1994 | 464 | 2003 | 225 | 2020 | 1 |
| 1995 | 462 | 2004 | 224 | 2021 | 446 |
| 1996 | 463 | 2005 | 223 | 2022 | 444 |
| | | 2006 | 222 | 2023 | 445 |
| | | 2007 | 221 | 2024 | 606 |
| | | 2008 | 220 | 2025 | 684 |
| | | 2009 | 219 | 2026 | 685 |
| | | 2010 | 218 | 2027 | 686 |
| | | 2011 | 217 | | |
| | | 2012 | 216 | | |
| | | 2013 | 215 | | |

**`field_closed_year_id` (Closed Year)** — same IDs as `field_year_id` for overlapping years (1970–2024 per PDF); filters by the year the recall was closed (`field_closed_year`). Not documented beyond 2024.

**These are NOT incremental watermarks.** `field_year_id` is immutable (issue year never changes). `field_closed_year_id` captures newly-closed records but misses amendments and records without a close date. Neither solves the incremental extraction problem — full-dump remains the correct strategy (Finding D).

**Where these ARE useful:** `deep_rescan/get_recalls_by_year.yml` uses `field_year_id` to chunk a historical load year-by-year. For USDA's 2,001-record total dataset this is optional, but the pattern is available if needed. Update `environments/Development.yml` with `recall_year_id: 685` (2026) to probe the current year.

**Other fully-documented taxonomy IDs from Appendix A (for Pydantic schema `Literal` types):**

| Parameter | Values → IDs |
|---|---|
| `field_archive_recall` | All, TRUE→1, FALSE→0 |
| `field_recall_classification_id` | Class I→10, Class II→11, Class III→12, Public Health Alert→554 |
| `field_risk_level_id` | High - Class I→9, Low - Class II→7, Marginal - Class III→611, Medium - Class I→8, Public Health Alert→555 |
| `field_recall_type_id` | Outbreak→338, Public Health Alert→22, Active Recall→23, Closed Recall→24 |
| `field_recall_reason_id` | Import Violation→19, Insanitary Conditions→17, Misbranding→13, Mislabeling→15, Processing Defect→21, Produced Without Benefit of Inspection→18, Product Contamination→16, Unfit for Human Consumption→20, Unreported Allergens→14 |
| `field_processing_id` | Eggs/Egg Products→162, Fully Cooked-Not Shelf Stable→159, Heat Treated-Not Fully Cooked-Not Shelf Stable→160, Heat Treated-Shelf Stable→158, Not Heat Treated-Shelf Stable→157, Products with Secondary Inhibitors-Not Shelf Stable→161, Raw-Intact→154, Raw-Non Intact→155, Slaughter→153, Thermally Processed-Commercially Sterile→156, Unknown→625 |
| `field_translation_language` | English→en, Spanish→es |
| `field_related_to_outbreak` | TRUE→1, FALSE→0 |
| `field_states_id` | Full 50-state + territory mapping in PDF Appendix A pages 5–7. Sample: Alabama→25, California→29, Florida→33, New York→57, Texas→68, Nationwide→557, DC→76, Puerto Rico→80, Guam→78. Range: 25–82 (states) + 557 (Nationwide). |

**Text-type documented filter parameters (not taxonomy IDs):**

| Parameter | Type | Format / Behavior |
|---|---|---|
| `field_closed_date_value` | Text | YYYY-MM-DD (e.g., `2023-07-18`) — exact date match for `field_closed_date` |
| `field_recall_number` | Text | `DDD-YYYY` format (e.g., `021-2023`) — **contains/substring match**, not exact |
| `field_product_items_value` | Text | Free text — **contains match**, not exact |
| `field_summary_value` | Text | Free text — **contains match**, not exact |

`field_closed_date_value` is the only documented date-based filter parameter. It operates on the recall's close date, not last_modified_date or recall_date — useful for narrowing historical deep-rescans by close window, but cannot serve as a general watermark (many records have no close date).

---

## Filter Parameter Encoding

### Finding L — Filter input vs response output asymmetry

Confirmed 2026-04-29 via `data_exploration/probe_archive_behavior.yml` and PDF Appendix A.

**Response boolean fields use capitalized strings; filter input uses integers.**

| Context | TRUE | FALSE |
|---|---|---|
| API response output (`field_archive_recall`, `field_related_to_outbreak`, etc.) | `"True"` (string) | `"False"` (string) |
| Query filter parameter | `1` (integer) | `0` (integer) |

Confirmed: `field_archive_recall=1` returns 1,829 archived records. Passing string values
(`TRUE`, `True`) as filter input is silently ignored — the full 2,001-record dataset is returned.
Same applies to `field_related_to_outbreak` (TRUE→1, FALSE→0 per Appendix A).

**`field_translation_language` filter uses abbreviated codes; `langcode` response uses full words.**

| Context | English | Spanish |
|---|---|---|
| Query filter (`field_translation_language=`) | `en` | `es` |
| Response field (`langcode`) | `"English"` | `"Spanish"` |

Extractor code that inspects `langcode` in response records must use the full-word forms.
Any query that filters by language must use the abbreviated codes.

---

## Rate Limiting / Throttling

### Finding M — Rate limit behavior

**No rate limiting observed during exploration (2026-04-29).**

No 429 responses, no `Retry-After` headers, no redirect-to-apology behavior encountered across
all probes (cardinality, archive filter, bilingual pair, lookup, year-scoped). Server stack:
nginx 1.22.1 / Drupal 10 / PHP 8.3 / Akamai CDN. Bot detection fingerprinting via `ak_bmsc`
cookie is present on every response — monitor for blocking in production under higher request
volumes, but no throttling was triggered during exploration.

---

## Implications for Pydantic Schema

- All boolean-valued fields (`field_active_notice`, `field_archive_recall`, `field_related_to_outbreak`,
  `field_has_spanish`) are `"True"` / `"False"` capitalized strings — use `str` with a validator, not `bool`.
- `langcode` is `"English"` or `"Spanish"` — use `Literal["English", "Spanish"]`.
- `field_recall_number` — documented format `DDD-YYYY`; treat as `str` (natural key for `source_recall_id`).
- `field_year` is a string year like `"2022"`, not an integer.
- `field_last_modified_date` is `Optional[str]` — 42.2% empty (Finding C); cannot drive server-side filter (Finding D).
- `field_en_press_release` always `""` (100% empty, Finding C) — declare `Optional[str] = None`; exclude from content hash.
- `field_press_release` — 99.9% empty (Finding C) — same posture as `field_en_press_release`.
- All other fields with non-zero empty rates per Finding C — declare as `Optional[str]`.
- `field_recall_url` — undocumented field (Finding H); different value per language version — include as `Optional[str]`.

---

## Implications for Extractor Design

Confirmed strategy — Findings A, B, C, D, and J all inform this design.

**Full-dump extraction (confirmed Finding D).** `UsdaExtractor.extract()` sends `GET /fsis/api/recall/v/1`
with no filter parameters. The complete 2,001-record dataset (~1.6 MB compressed) is returned in a single
response. `field_last_modified_date` cannot drive a server-side filter (both naming variants silently ignored),
and 42.2% of records have no value for it anyway.

**ETag optimization (Finding A).** Send `If-None-Match: <last_etag>` on each run. If the dataset is
unchanged, the server returns `304 Not Modified` and the extractor skips the ~12 MB download entirely.
This is the most effective efficiency lever available for this source.

**Content-hash dedup (ADR 0007).** Unchanged records are no-op inserts — idempotency is handled by the
bronze loader, not the extractor.

**Bilingual dedup in `check_invariants()`.** Assert that each `field_recall_number` has at most
one `langcode=English` and one `langcode=Spanish` record. `source_recall_id` = `field_recall_number`.
A Spanish record with no matching English record is a data anomaly worth quarantining.

---

## ETag Conditional-GET Reliability

### Finding N — ETag conditional-GET behavior depends on client request shape (DISABLED in production, pending more probe evidence)

Status updated 2026-05-01 after Phase 5b first-extraction surfaced new evidence.
**The optimization is disabled in production via the extractor class default
(`UsdaExtractor.etag_enabled = False` in `src/extractors/usda.py`).** The extractor
pulls the full ~1.6 MB compressed payload on every run; idempotency is handled by
the bronze content-hash loader (ADR 0007).

> **About the YAML file:** `config/sources/usda.yaml` also carries `etag_enabled:
> false`, but **that file is not currently loaded by any code path** (the YAML
> loader described in ADR 0012 has not yet been implemented). The live kill-switch
> is the class default in Python; the YAML value is documentation of intent.

**Probe sequence and headers observed:**

| Run | Time (UTC) | etag | last-modified | cache-control | transfer | x-drupal-cache | akamai-grn |
|---|---|---|---|---|---|---|---|
| Finding A baseline | 2026-04-29 14:29 | `"1777472976"` | `Wed, 29 Apr 2026 14:29:36 GMT` | `public, max-age=3100` | `content-length: 1,641,691` | (HIT-shape; not recorded) | — |
| capture (round 1) | 2026-04-30 22:10 | absent | absent | `max-age=0, no-cache, no-store` | chunked | `UNCACHEABLE (request policy)` | `0.6be82d17.…2194f2a3` |
| cardinality (round 1) | 2026-04-30 22:22 | absent | absent | `max-age=0, no-cache, no-store` | chunked | `UNCACHEABLE (request policy)` | `0.6be82d17.…21a9af9c` |
| capture +25s (round 1) | 2026-04-30 22:23 | absent | absent | `max-age=0, no-cache, no-store` | chunked | `UNCACHEABLE (request policy)` | `0.ad24c317.…90074de6` |

**Root-cause analysis:**

1. **Drupal origin explicitly forbids caching.** Every today-response carried
   `x-drupal-cache: UNCACHEABLE (request policy)` and `cache-control: max-age=0,
   no-cache, no-store`. The cacheable response observed in Finding A was Akamai
   *overriding* the origin directive (synthesizing an etag and `cache-control:
   public, max-age=3100`) — not Drupal serving a cacheable response.
2. **Akamai's CDN cache override does not fire reliably.** Three sequential
   no-filter GETs within 25 seconds all bypassed the cache and went origin-direct.
   Priming did not produce a HIT on the immediately-following request.
3. **Akamai edge node rotation compounds miss rates.** The third probe
   (`akamai-grn: 0.ad24c317.…`) hit a different edge node than the prior two
   (`akamai-grn: 0.6be82d17.…`); each Akamai node has its own local cache, so a HIT
   on one node does nothing for a request that lands on another.
4. **Akamai bot manager is the most likely gate.** Set-Cookie evolved across
   probes: run 1 carried `ak_bmsc` (initial bot fingerprint); runs 2 and 3 carried
   `bm_sv` (bot session validation). Akamai is progressively fingerprinting
   Bruno (and would similarly classify Python httpx in CI) as a non-browser
   client, which can cause Akamai to gate the cached fast-path behind
   browser-like reputation.

**Implication for production:** Our extractor's httpx client will look at least
as bot-like as Bruno. The etag optimization would work occasionally and fail
unpredictably — the worst-case combination, because it would also expose us to
stale-positive 304s when a cached edge response carries an etag that no longer
matches origin. Disabling is the correct call.

### Finding N addendum (2026-05-01) — ETag works once browser-like headers are sent

Phase 5b first-extraction surfaced contradicting evidence. Sequence:

1. We added browser-like headers to the extractor (Finding O fix): Firefox/Linux
   User-Agent + matching `Accept` / `Accept-Language` / `Accept-Encoding`.
2. First successful extraction (2026-05-01 00:51 UTC) returned `200 OK` with
   `etag: "1777596670"` — Akamai *did* serve a CDN-cached response for our request
   shape. The extractor stored the etag in `source_watermarks.last_etag`.
3. Second extraction 24 minutes later sent `If-None-Match: "1777596670"` and
   received `304 Not Modified` — clean short-circuit, no body downloaded, no
   bronze write, contradiction guard silent (last-modified header unchanged).

So the optimization *does* work for our specific request shape, contradicting the
Finding N evidence collected via Bruno. Most likely cause for the divergence:
Akamai's bot manager scoring keys partly on header set (Accept/Accept-Language/
Accept-Encoding). Bruno sends different Accept-Encoding / lacks Accept-Language,
landing it on a different cache key (or the bot-throttled path); our extractor's
deliberate Firefox-style header set lands on the cached path.

**Why the optimization stays disabled despite this evidence:**

- Two data points (one cache HIT followed by one valid 304) is not enough to
  prove the cached path is *consistently* available across days, IP rotations,
  Akamai bot-reputation cycles, or upstream cache-key changes. Bruno's failures
  earlier the same day showed the cached path is conditional on factors we don't
  fully control.
- A stale-positive 304 (etag matched but origin actually advanced) silently
  drops new records until the next deep-rescan run catches it. The contradiction
  guard helps but only when last-modified also moves; if Akamai serves an entire
  stale cached object, last-modified would also be stale and the guard wouldn't
  fire.
- Production correctness > cost optimization at this dataset size. 1.6 MB
  compressed per run is cheap.

**Re-enabling criteria** (when these are met, flip `etag_enabled` to `True`):

- Multi-day probe sequence: capture etag on day N, send conditional GET on day
  N+1, N+3, N+7, with at least one cycle landing on a different Akamai edge node
  (`akamai-grn` rotation visible in headers). All cycles must show the expected
  pattern: 304 if the dataset truly hasn't changed, 200 with new etag if it has.
- At least one observed cache-key flip (e.g. Akamai promotes/expires the cached
  object) without a stale-positive 304 leaking past the contradiction guard.
- Cron-cadence simulation in dev: run the extractor on the same cron schedule
  the production workflow will use for one full week and confirm no
  contradiction-guard fires occur.

Until then, the deep-rescan workflow + contradiction guard remain the safety net
for whenever the optimization is enabled, and the extractor still captures and
stores `last_etag` / `last_cursor` on every successful run so the data is ready
when we are.

**Future API exploration opportunities** (Rounds 2 and 3 of the probe runbook —
optional, only if revisiting this optimization later):

- **Round 2 (TTL-window test):** ~10 minutes after a primed pair, run
  `probe_etag_capture.yml` once. If headers match the last successful HIT, the
  CDN survives the gap; if MISS, caching is dropping unpredictably.
- **Round 3 (post-cache-window test):** ~30 minutes after Round 2, run
  `probe_etag_capture.yml` again. The 3100s `max-age` should still be active.
  HIT with the same etag confirms the cache survives the full window; MISS
  indicates active invalidation.
- **Browser-like User-Agent test (not in the probe scripts):** repeat capture
  with a real browser User-Agent string and standard browser `Accept` /
  `Accept-Language` headers. If Akamai serves a HIT for a browser-like client
  but MISS for httpx, bot detection is confirmed as the gate. A workaround would
  be possible but adds substantial complexity (User-Agent management, cookie
  jar, possibly TLS fingerprint matching) for a 1.6 MB payload — the cost
  almost certainly does not justify the savings at this dataset size.

The kill switch (`etag_enabled: false` in config) is sufficient to disable
without removing the extractor's ETag-handling code path. Re-enable by flipping
to `true` if a future audit shows Akamai/FSIS behavior has changed.

---

## Akamai Bot Manager — request shape requirements

### Finding O — Browser-like UA + Accept headers required for production extraction

Confirmed 2026-04-30 during Phase 5b first-extraction verification. **The USDA
extractor cannot use httpx defaults; it must send a real-browser User-Agent +
matching `Accept` / `Accept-Language` / `Accept-Encoding` headers.** This is wired
in `src/extractors/usda.py` (`_load_user_agent`, `_browser_headers`); UA strings
are vendored in `data/user_agents.json` and refreshed weekly by the
`.github/workflows/refresh-user-agents.yml` workflow.

**Probe sequence and observed headers:**

| Run | Client | UA | Result |
|---|---|---|---|
| 1 | `uv run recalls extract usda` | `python-httpx/<default>` | Hung indefinitely; tenacity retried up to ~5 min |
| 2 | `curl --http2` (default UA) | `curl/7.81.0` | TLS+ALPN OK; HTTP/2 stream killed with `INTERNAL_ERROR (err 2)` after ~100 ms |
| 3 | `curl --http2 -H "User-Agent: python-httpx/0.27.0"` | `python-httpx/0.27.0` | Same as run 2 — HTTP/2 stream `INTERNAL_ERROR` |
| 4 | `curl --http1.1` (default UA) | `curl/7.81.0` | TCP+TLS OK; GET sent; **silent slowloris**, no response after 2:29 min |
| 5 | `curl --http1.1 -H "User-Agent: Mozilla/5.0 (X11; Linux x86_64; rv:125.0) Gecko/20100101 Firefox/125.0"` + Accept-* | Firefox/Linux | **200 OK in 294 ms, 1.6 MB body downloaded** |

**Root-cause analysis:**

1. **Akamai Bot Manager is deployed on this endpoint** (confirmed by `ak_bmsc` /
   `bm_sv` Set-Cookie headers across all responses). It uses multi-signal scoring:
   TLS handshake fingerprint (JA3/JA4), User-Agent string, request headers, request
   rate, IP reputation, cookie history.
2. **TLS fingerprint alone is not the sole gate.** The user's prior project against
   the same endpoint succeeded with `python requests` (Python's `ssl` module +
   OpenSSL — same TLS stack as httpx) using only a real-Firefox UA override. This
   means a real-browser UA can pull the request below the bot threshold even with
   a Python TLS stack — at least for a clean IP at low volume.
3. **Akamai's rejection mode varies by HTTP version.** On HTTP/2, the edge sends
   `INTERNAL_ERROR` (run 2/3). On HTTP/1.1, the edge slowloris-es (run 4) — TCP+TLS
   complete, the GET goes out, the server holds the connection open and never
   responds. Slowloris is harsher because retries deepen it.
4. **Per-IP reputation degrades intra-day.** Yesterday's Bruno probes returned
   cacheable responses with etags (Finding A); today's Bruno probes returned
   `UNCACHEABLE (request policy)`; today's curl/httpx requests slowloris-ed
   regardless of UA. After we hammered the endpoint with a hung extraction +
   diagnostic curls, even Bruno's CDN cache override stopped firing. Treat IP
   reputation as a fast-decaying resource.

**Resolution:**

- `src/extractors/usda.py` constructs `httpx.Client` with browser-like default
  headers (UA + Accept + Accept-Language + Accept-Encoding) on every fetch.
- The User-Agent string is loaded from `data/user_agents.json` per fetch; the
  file is vendored in the repo and refreshed weekly via
  `.github/workflows/refresh-user-agents.yml`, which fetches Mozilla
  product-details and Chromium Dash, templates the UA, and opens a PR if either
  upstream version has moved.
- A `_FALLBACK_FIREFOX_UA` constant covers the case where the JSON file is
  missing or malformed at runtime; the loader emits
  `usda.user_agents_load_failed` so a degraded run is visible in logs.

**Defense-in-depth follow-ups** (open if Akamai escalates further):

- **Add Chrome UA rotation.** `data/user_agents.json` already includes
  `chrome_linux`. If Akamai starts rejecting Firefox UAs, a one-line change in
  `_load_user_agent()` rotates to Chrome.
- **Adopt `browserforge`** (newer, ~3-year-active library) which generates whole
  consistent header sets including Sec-CH-UA client hints. Useful when Akamai
  begins fingerprinting on cross-header consistency.
- **Adopt `curl-cffi`** if multi-signal scoring escalates to JA3/JA4 enforcement
  for our IP class. `curl-cffi` impersonates Chrome's TLS handshake and is the
  industry-standard remedy.
- **Egress IP rotation / cooldown** if individual extraction runs start to fail
  while UA + headers remain valid.

---

## Open Items

- [x] Document total cardinality and field nullability map (Findings B, C) — confirmed 2026-04-29
- [x] Confirm pagination behavior (Finding A) — confirmed 2026-04-29: no pagination, flat array; see Finding A
- [x] Confirm `field_last_modified_date` filter availability (Finding D) — confirmed 2026-04-29: both `field_last_modified_date` and `field_last_modified_date_value` are silently ignored; full-dump is the only production strategy
- [x] Confirm bilingual pair structure for `field_has_spanish=True` recalls (Finding F) — confirmed 2026-04-29: 2 records per recall number (EN+ES), identical `field_last_modified_date` across pair, `field_has_spanish=True` on both versions, `field_recall_url` is an undocumented field (Finding H)
- [x] Confirm `field_recall_number` filter behavior (Finding I) — confirmed 2026-04-29: returns full bilingual pair (2 records), no spurious matches, natural composite key is `(field_recall_number, langcode)`
- [x] Document archive behavior (Finding J) — confirmed 2026-04-29: filter works, 1,829 archived, 46% have empty `field_last_modified_date`, only 5 active Spanish records, filtered requests are not CDN-cached
- [x] Document Drupal taxonomy ID mapping for all selection filters (Finding K) — confirmed from PDF Appendix A; full mapping documented above including `field_processing_id` and `field_states_id`; no empirical probe needed
- [ ] Verify `field_last_modified_date` reliability on known-edited recall (Finding E)
- [ ] Optionally probe `field_closed_date_value` behavior (Finding K text filters) — documented as YYYY-MM-DD exact date filter on close date; test whether it works server-side (unlike `field_last_modified_date`)
