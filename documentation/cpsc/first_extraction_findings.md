# CPSC SaferProducts First Extraction Findings

**Extraction window:** 2025-04-21 – 2026-04-17 (~1 year of incremental runs)  
**Total rows in bronze:** 1,193  
**Unique recall IDs:** 1,193  
**Duplicate rows:** 0  
**Extraction date (most recent run):** 2026-04-17  

---

## A. Data Model: One Recall = One Product Line

CPSC's bronze row is structurally simpler than FDA's. Every recall record has exactly one entry in the `products` JSONB array — confirmed across all 1,193 rows. `recall_id` is unique in the bronze table; `source_recall_id = recall_id` is both the dedup key and a natural primary key.

This is the inverse of FDA's model: FDA groups many products under one event; CPSC publishes one product per recall record. A CPSC "recall" is already at the product-line level.

**Silver implication:** No header/line split is needed for CPSC. Each bronze row maps 1:1 to a silver recall record. JSONB fields (`products`, `hazards`, `remedies`, `manufacturers`, etc.) will be unnested into normalized silver tables.

---

## B. Cadence

CPSC publishes on weekdays. Volume is substantially lower than FDA.

**Weekly totals (sample — full year):**

| week_start | records | active_days |
|---|---|---|
| 2025-04-21 | 28 | 5 |
| 2025-05-26 | 21 | 4 |
| 2025-06-02 | 32 | 5 |
| 2025-08-25 | 16 | 4 |
| 2025-10-27 | 31 | 5 |
| 2025-11-03 | 42 | 5 |
| 2025-11-24 | 11 | 1 |
| 2025-12-22 | 7 | 2 |
| 2026-02-23 | 59 | 5 |
| 2026-04-13 | 28 | 5 |

**Typical daily range:** 1–18 records on normal days.  
**Average on active days:** ~5–6 records/day.  
**Weekly range:** 7–59 records.

**Volume comparison to FDA (same 90-day window Jan 29 – Apr 27, 2026):**

| source | total records | avg/active day | weekly range |
|---|---|---|---|
| FDA | 2,705 | ~40 | 54–325 |
| CPSC | ~420 (est.) | ~5–6 | 7–59 |

FDA produces roughly 7× more records per day than CPSC.

---

## C. Weekday Gaps

CPSC has substantially more weekday gaps than FDA — 36 over ~1 year, versus FDA's 1 in 90 days. Most are explained by US federal holidays:

| period | gap days | explanation |
|---|---|---|
| 2025-05-26 | 1 | Memorial Day |
| 2025-07-04 | 1 | Independence Day |
| 2025-08-28–09-01 | 2 | Late-summer gap + Labor Day |
| 2025-10-20–24 | 4 | Columbus Day week (multi-day) |
| 2025-11-10–12 | 3 | Veterans Day + adjacent days |
| 2025-11-24–12-02 | 8 | Thanksgiving week + post-holiday |
| 2025-12-15–2026-01-02 | 13 | Christmas/New Year holiday period |
| 2026-01-23 | 1 | Isolated gap |
| 2026-02-11–20 | 6 | Presidents Day week + surrounding |

The holiday clustering confirms CPSC follows a US federal government publishing schedule. The Christmas/New Year block (Dec 15 – Jan 2) represents a ~2.5-week publication pause.

**Operational note:** the incremental watermark handles these gaps automatically — `LastPublishDate` advances only when CPSC publishes, so runs during holiday periods return zero records rather than missing data.

---

## D. Volume Spikes

CPSC has no multi-product event spikes like FDA. The highest single-day volumes are modest:

| day | records |
|---|---|
| 2026-02-27 | 28 |
| 2026-02-25 | 22 |
| 2026-04-17 | 19 |
| 2026-02-06 | 18 |
| 2026-10-17 | 18 |

These are simply high-publication days, not structural anomalies. The absence of 100+ record days confirms that CPSC does not batch large groups of products under a single recall event the way FDA does.

---

## E. Edit Detection (Content Hash Dedup)

**Zero edits detected** across 1,193 records. Every `recall_id` appears exactly once with a single content hash. No record has been re-fetched with a changed hash.

This is an important architectural distinction from FDA:

- **FDA** advances `EVENTLMD` when a record is edited, so edited records naturally re-enter incremental windows and get captured.
- **CPSC** uses `LastPublishDate` as the watermark. If CPSC edits an existing recall without advancing its `LastPublishDate`, that change will **not** be captured by incremental runs. The deep-rescan workflow (`deep-rescan-cpsc.yml`) exists specifically to close this gap by re-fetching a wide historical window periodically.

The zero-edit observation may mean CPSC rarely edits published recalls, or it may mean edits have occurred but went undetected because `LastPublishDate` did not advance. The deep-rescan is the safeguard against the latter.

---

## F. Null and Empty Field Rates

### Scalar fields

| field | null % | notes |
|---|---|---|
| `title` | 0.0% | Always present |
| `recall_date` | 0.0% | Always present |
| `description` | 0.0% | Always present |
| `url` | 0.0% | Always present |
| `consumer_contact` | 0.0% | Always present |
| `injuries` | 0.0% | Always present (may be empty string) |
| `sold_at_label` | 100.0% | See note below |
| `product_upcs` | 0.0% | Present but always `[]` — see JSONB section |

### `sold_at_label` — 100% null (data gap)

`sold_at_label` was added in migration 0003 as a derived text field. It is null for all existing rows, which means either the extractor does not yet populate it, or it requires a backfill from the `products` JSONB (e.g. extracting the retailer/sold-at information). This is a known gap to resolve before the silver layer consumes this field.

### JSONB fields (empty array rates)

| field | empty % | notes |
|---|---|---|
| `products` | 0.0% | Always 1 element |
| `remedies` | 0.0% | Always populated |
| `retailers` | 0.7% | 8 recalls with no retailer listed |
| `hazards` | 0.2% | 2 recalls with no hazard listed |
| `manufacturers` | 56.0% | Over half have no manufacturer — likely direct importer |
| `manufacturer_countries` | 28.0% | Absent for ~1 in 3 recalls |
| `product_upcs` | ~100% | Always `[]` — UPC filter confirmed non-functional (see TODO) |

---

## G. `HazardType` Field — Confirmed Empty

Despite 99.8% of recalls having a populated `hazards` array, `HazardType` inside each hazard object is an empty string (`""`) for 100% of records (0 of 1,191 hazard-bearing rows have a non-empty `HazardType`). This confirms the finding documented in the TODO:

> `Hazard=` targets `HazardType` which is consistently `""` across all sampled records.

`HazardTypeID` will need to be investigated as an alternative filter key before silver hazard joins can be built reliably.

---

## H. Pipeline Performance

No `extraction_runs` rows exist for CPSC because all prior CPSC runs predate the `_record_run()` fix implemented in `feature/fda-first-extraction`. Future CPSC runs will populate `extraction_runs` correctly.

From indirect evidence (watermark timestamps and row counts), CPSC incremental runs process 5–30 records in under 1 second. The API returns a single response with no pagination — the entire CPSC dataset (~9,700 records) fits in one page, and incremental windows return a tiny fraction of that.

---

## I. Silver Layer Implications

1. **1:1 row mapping:** each bronze row becomes one silver recall record — no header/line split needed.
2. **JSONB unnesting:** `products`, `hazards`, `remedies`, `manufacturers`, `retailers` all need silver unnesting tables. Each can be a separate dbt model joining back on `recall_id`.
3. **`sold_at_label` backfill:** needs investigation — either populate from the extractor or derive from `products` JSONB in a silver transform.
4. **`HazardType` unreliable:** do not use as a silver filter; use `HazardTypeID` or the hazard description text instead.
5. **`manufacturer_countries` gaps:** 28% empty; silver should treat this as optional with NULL-safe joins.
6. **Deep-rescan necessity:** unlike FDA (which self-signals edits via `EVENTLMD`), CPSC edit detection depends entirely on periodic deep-rescans. This must be turned on in Phase 7 before the silver layer can claim edit completeness.

---

## J. SQL Reference

All queries used to produce this analysis live in
`scripts/sql/cpsc/bronze/explore_bronze_shape.sql`. The file is a 12-query
batch with `\echo` headers; sections A–I above cite individual queries by
their `Q<n>` number.

Run with:

```bash
set -a && . .env && set +a
PGPASSWORD="$NEON_PASSWORD" psql -h "$NEON_HOST" -U "$NEON_USER" -d "$NEON_DBNAME" \
  -f scripts/sql/cpsc/bronze/explore_bronze_shape.sql
```

See `scripts/sql/README.md` for the broader query-organization convention.
