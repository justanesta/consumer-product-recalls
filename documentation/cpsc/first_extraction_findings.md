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

All queries used to produce this analysis. Each can be re-run against `cpsc_recalls_bronze` as the dataset grows.

```sql
-- 1. Overall row count and date range
--    Establishes the extraction window and confirms the watermark field used.
SELECT COUNT(*), MIN(last_publish_date), MAX(last_publish_date)
FROM cpsc_recalls_bronze;

-- 2. Weekly cadence
--    Shows publication rhythm week-by-week; reveals holiday gaps and seasonal patterns.
SELECT
  DATE_TRUNC('week', last_publish_date)::date AS week_start,
  COUNT(*) AS records,
  COUNT(DISTINCT last_publish_date::date) AS active_days
FROM cpsc_recalls_bronze
GROUP BY DATE_TRUNC('week', last_publish_date)
ORDER BY week_start;

-- 3. Records per day
--    Detailed daily view; used to identify spikes and confirm no weekend activity.
SELECT last_publish_date::date AS day, COUNT(*) AS records
FROM cpsc_recalls_bronze
GROUP BY last_publish_date::date
ORDER BY last_publish_date::date;

-- 4. Edit detection: recall_ids with multiple distinct content hashes
--    A non-empty result would indicate a record was re-fetched with changed content
--    (the content hash dedup mechanism captured an edit). Zero rows = no edits detected.
SELECT recall_id, COUNT(DISTINCT content_hash) AS hash_versions, COUNT(*) AS total_rows
FROM cpsc_recalls_bronze
GROUP BY recall_id
HAVING COUNT(DISTINCT content_hash) > 1
ORDER BY hash_versions DESC;

-- 5. Total rows vs unique recall_ids (dedup summary)
--    Confirms whether source_recall_id is truly unique in bronze or whether
--    multi-version rows exist from edit detection.
SELECT
  COUNT(*) AS total_rows,
  COUNT(DISTINCT recall_id) AS unique_recall_ids,
  COUNT(*) - COUNT(DISTINCT recall_id) AS apparent_duplicates
FROM cpsc_recalls_bronze;

-- 6. Null rates for scalar fields
--    Identifies which fields are reliably populated vs. optional. Critical input
--    for deciding which silver columns can be NOT NULL.
SELECT
  ROUND(100.0 * SUM(CASE WHEN title IS NULL THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_null_title,
  ROUND(100.0 * SUM(CASE WHEN recall_date IS NULL THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_null_recall_date,
  ROUND(100.0 * SUM(CASE WHEN description IS NULL THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_null_description,
  ROUND(100.0 * SUM(CASE WHEN url IS NULL THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_null_url,
  ROUND(100.0 * SUM(CASE WHEN consumer_contact IS NULL THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_null_consumer_contact,
  ROUND(100.0 * SUM(CASE WHEN sold_at_label IS NULL THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_null_sold_at_label,
  ROUND(100.0 * SUM(CASE WHEN product_upcs IS NULL THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_null_upcs,
  ROUND(100.0 * SUM(CASE WHEN injuries IS NULL THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_null_injuries
FROM cpsc_recalls_bronze;

-- 7. JSONB field empty-array rates
--    Checks whether JSONB arrays are populated, not just non-null.
--    A non-null but empty array (e.g. manufacturers = '[]') means the field exists
--    structurally but carries no data — different from a null field.
SELECT
  ROUND(100.0 * SUM(CASE WHEN products IS NULL OR products = '[]'::jsonb THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_empty_products,
  ROUND(100.0 * SUM(CASE WHEN hazards IS NULL OR hazards = '[]'::jsonb THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_empty_hazards,
  ROUND(100.0 * SUM(CASE WHEN remedies IS NULL OR remedies = '[]'::jsonb THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_empty_remedies,
  ROUND(100.0 * SUM(CASE WHEN manufacturers IS NULL OR manufacturers = '[]'::jsonb THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_empty_manufacturers,
  ROUND(100.0 * SUM(CASE WHEN retailers IS NULL OR retailers = '[]'::jsonb THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_empty_retailers,
  ROUND(100.0 * SUM(CASE WHEN manufacturer_countries IS NULL OR manufacturer_countries = '[]'::jsonb THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_empty_mfr_countries
FROM cpsc_recalls_bronze;

-- 8. Products per recall (confirms 1:1 model)
--    FDA has 1-to-many; this query confirms CPSC always has exactly 1 product per row.
SELECT
  jsonb_array_length(products) AS product_count,
  COUNT(*) AS recalls
FROM cpsc_recalls_bronze
WHERE products IS NOT NULL
GROUP BY jsonb_array_length(products)
ORDER BY product_count;

-- 9. HazardType population check
--    Validates the TODO finding that HazardType is always an empty string
--    despite hazards arrays being non-empty. Used to assess silver filter viability.
SELECT
  COUNT(*) AS total_recalls,
  SUM(CASE WHEN hazards != '[]'::jsonb THEN 1 ELSE 0 END) AS recalls_with_hazards,
  SUM(CASE WHEN hazards != '[]'::jsonb AND (hazards->0->>'HazardType') != '' THEN 1 ELSE 0 END) AS recalls_with_hazard_type
FROM cpsc_recalls_bronze;

-- 10. Top spike days
--     Identifies the highest-volume single days; used to check for anomalies
--     vs. expected publication patterns.
SELECT last_publish_date::date AS day, COUNT(*) AS records
FROM cpsc_recalls_bronze
GROUP BY last_publish_date::date
ORDER BY records DESC
LIMIT 5;

-- 11. Weekday gap analysis
--     Finds weekdays with zero CPSC activity. Expected gaps = US federal holidays.
--     Unexpected gaps may indicate pipeline failures or API outages.
WITH date_series AS (
  SELECT generate_series(MIN(last_publish_date)::date, MAX(last_publish_date)::date, '1 day'::interval)::date AS day
  FROM cpsc_recalls_bronze
),
active_days AS (
  SELECT DISTINCT last_publish_date::date AS day FROM cpsc_recalls_bronze
)
SELECT d.day, TO_CHAR(d.day, 'Day') AS day_name
FROM date_series d
LEFT JOIN active_days a ON d.day = a.day
WHERE a.day IS NULL AND EXTRACT(DOW FROM d.day) NOT IN (0, 6)
ORDER BY d.day;

-- 12. Extraction run history
--     Confirms pipeline runs were recorded with correct counts and status.
--     Note: runs prior to the extraction_runs fix (feature/fda-first-extraction)
--     will not appear here.
SELECT source, status, records_extracted, records_inserted, records_rejected,
  started_at, EXTRACT(EPOCH FROM (finished_at - started_at))::int AS duration_seconds
FROM extraction_runs
WHERE source = 'cpsc'
ORDER BY started_at;
```
