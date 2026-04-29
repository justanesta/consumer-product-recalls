# FDA iRES First Extraction Findings

**Extraction window:** 2026-01-29 – 2026-04-27 (90-day lookback)  
**Total rows in bronze:** 2,705  
**Unique recall events:** 755  
**Unique products (source_recall_id):** 2,692  
**Extraction date:** 2026-04-29  

---

## A. Data Model: PRODUCTID Is the Atomic Unit

The most important structural finding: `RECALLEVENTID` is a grouping key, not a unique row identifier. A single recall event covers many individual products, each with its own `PRODUCTID`.

| recall_event_id | products (rows) | example |
|---|---|---|
| 96869 | 86 | Large food recall, 2026-03-24 |
| 97019 | 83 | Multi-product recall, 2026-03-13 |
| 97369 | 57 | Multi-product recall, 2026-03-13 |
| 91466 | 53 | Multi-product recall, 2026-02-20 |
| 97631 | 52 | Multi-product recall, 2026-02-12 |

This confirms that `source_recall_id = PRODUCTID` (ADR 0007) is the correct bronze dedup key. `RECALLEVENTID` is a natural join key for the silver layer — it groups all product lines belonging to the same recall event — but it cannot serve as a primary key.

**Silver implication:** the silver recall header table should key on `recall_event_id`; the line table keys on `(recall_event_id, source_recall_id)`.

---

## B. Cadence

FDA publishes Mon–Fri. No weekend activity was observed in the full 90-day window.

**Weekly totals:**

| week_start | records | active_days |
|---|---|---|
| 2026-01-26 | 58 | 3 |
| 2026-02-02 | 226 | 5 |
| 2026-02-09 | 205 | 5 |
| 2026-02-16 | 282 | 5 |
| 2026-02-23 | 120 | 5 |
| 2026-03-02 | 131 | 5 |
| 2026-03-09 | 325 | 5 |
| 2026-03-16 | 197 | 6 |
| 2026-03-23 | 271 | 5 |
| 2026-03-30 | 221 | 5 |
| 2026-04-06 | 171 | 6 |
| 2026-04-13 | 184 | 6 |
| 2026-04-20 | 260 | 6 |
| 2026-04-27 | 54 | 1 (partial) |

**Typical daily range:** 20–70 records on normal days.  
**Average on active days:** ~40 records/day.  
**Weekday gaps in 90 days:** 1 — 2026-02-16 (Presidents Day, US federal holiday).

---

## C. Volume Spikes

Three days exceeded 90 records:

| day | records | driver |
|---|---|---|
| 2026-03-13 | 198 | Events 97019 (83 products) + 97369 (57 products) published same day |
| 2026-03-24 | 122 | Event 96869 (86 products) published |
| 2026-04-17 | 105 | Multiple mid-size events |

Spikes are caused by single large multi-product recall events — one event announcement covering dozens of individual product lots all stamped with the same `event_lmd`. This is expected behavior and does not indicate API anomalies.

**Incremental guard validation:** the `_MAX_INCREMENTAL_RECORDS` guard is not applicable to FDA's extractor (`FdaExtractor` has no such guard by design — see ADR 0010 and the FDA-specific count guard discussion). Even the largest single-day spike (198 records) is far below any threshold that would indicate a watermark failure.

---

## D. Pagination

No pagination occurred in this 90-day window. All 2,559 records fit in a single page (PAGE_SIZE = 5,000). This is consistent with finding O in `api_observations.md`: FDA's ~20–40 records/day cadence means incremental windows never approach the page boundary.

The `_paginate` loop is exercised by unit tests with mocked pages and by `FdaDeepRescanLoader` for historical windows, but incremental `FdaExtractor` runs should remain single-page for any window under ~3–4 months.

---

## E. Edit Detection (Content Hash Dedup)

### Between-run dedup (unchanged records)
The 90-day run fetched 2,559 records. 13 were already present with identical content hashes from the prior 7-day run — the dedup logic skipped them correctly. Net new insertions: 2,546.

### Within-run content changes (genuine edits captured)
Event 98779 (Philips North America, 19 products) appeared in both the Apr 28 and Apr 29 extractions with different content hashes for each product. Every one of the 19 products shows two rows: one from Apr 28 (extraction_timestamp) and one from Apr 29, same `event_lmd` date (2026-04-27), same `recall_num`, different `content_hash`.

This is the edit-detection mechanism (ADR 0007) working correctly. FDA updated these records the day after initial publication — a one-day edit cycle on a new recall. The bronze table preserves both versions; the silver "latest version" view will resolve to the Apr 29 row for each product.

**Edit rate observed:** 19 of 2,692 unique products (0.7%) had a genuine content change captured in this window.

---

## F. Null Field Rates

| field | null % | notes |
|---|---|---|
| `termination_dt` | 60.6% | Expected — mirrors 56% Ongoing phase |
| `product_distributed_quantity` | 6.9% | Free-text field; some recalls omit it |
| `recall_num` | 1.0% | 27 rows, mostly CDER (16) and HFP (7) |
| `rid` | 0.0% | Always present |
| `firm_fei_num` | 0.0% | Always present in this window |
| `phase_txt` | 0.0% | Always present |
| `center_classification_type_txt` | 0.0% | Always present |
| `recall_initiation_dt` | 0.0% | Always present |
| `product_description_txt` | 0.0% | Always present |

`termination_dt` nullability is driven entirely by phase. Use `phase_txt = 'Terminated'` as an alternative signal when `termination_dt` is needed but null.

`product_distributed_quantity` is a free-text field with no enforced format (e.g. "3509532 bags", "139,863 units across all items", "Unknown"). Parsing it to a numeric value for silver will require heuristic cleaning.

---

## G. Center and Product Type Distribution

| center_cd | rows | unique events | product_type_short |
|---|---|---|---|
| CDRH | 1,086 (40.1%) | 285 | Devices |
| HFP | 807 (29.8%) | 256 | Food |
| CDER | 415 (15.3%) | 110 | Drugs |
| CFSAN | 236 (8.7%) | 61 | Food |
| CVM | 105 (3.9%) | 24 | Veterinary |
| CBER | 54 (2.0%) | 17 | Biologics |
| OCS | 2 (0.1%) | 2 | Cosmetics |

Food recalls (HFP + CFSAN combined) represent ~38.5% of rows. Devices are the single largest category at 40%.

---

## H. Phase Distribution

| phase_txt | rows | % |
|---|---|---|
| Ongoing | 1,515 | 56.0% |
| Terminated | 1,063 | 39.3% |
| Completed | 127 | 4.7% |

More than half of all active records are still open recalls. The `deep-rescan-fda.yml` workflow exists specifically to capture records that transition from Ongoing → Terminated after initial publication (their `event_lmd` won't advance, so they won't be caught by incremental extractions).

---

## I. Pipeline Performance

| run | lookback | fetched | inserted | deduped | duration |
|---|---|---|---|---|---|
| 2026-04-29 00:27 | 1 day | 0 | 0 | 0 | 1s |
| 2026-04-29 00:49 | 90 days | 2,559 | 2,546 | 13 | 2s |

2,559 records in 2 seconds. Single HTTP POST, no pagination, no retries. The FDA iRES API is fast and reliable for incremental windows.

---

## J. Silver Layer Implications

1. **Primary keys:** silver header keys on `recall_event_id`; silver line keys on `(recall_event_id, source_recall_id)`.
2. **Latest-version view:** bronze may have multiple rows per `source_recall_id` (from edits). Silver should select the row with `MAX(extraction_timestamp)` per `source_recall_id`.
3. **`termination_dt`:** unreliable as a filter alone — use in conjunction with `phase_txt`.
4. **`product_distributed_quantity`:** needs string cleaning before silver can expose a numeric quantity field. Parse as text in bronze, clean in silver transform.
5. **Deep-rescan coverage:** phase transitions (Ongoing → Terminated) update existing records without advancing `event_lmd`. Weekly deep-rescan (once turned on in Phase 7) is required to capture these changes for the silver `phase_txt` and `termination_dt` fields.

---

## K. SQL Reference

All queries used to produce this analysis. Each can be re-run against `fda_recalls_bronze` as the dataset grows.

```sql
-- 1. Overall row count and date range
--    Establishes the extraction window and confirms the watermark field (event_lmd).
SELECT COUNT(*), MIN(event_lmd), MAX(event_lmd)
FROM fda_recalls_bronze;

-- 2. Records per day (cadence)
--    Reveals publication rhythm, weekend gaps, and spike days.
--    event_lmd is a timestamp; cast to date to group by calendar day.
SELECT event_lmd::date AS day, COUNT(*) AS records
FROM fda_recalls_bronze
GROUP BY event_lmd::date
ORDER BY event_lmd::date;

-- 3. Weekly cadence summary
--    Aggregates daily counts into weeks to show volume trends and
--    confirm how many days per week FDA publishes.
SELECT
  DATE_TRUNC('week', event_lmd)::date AS week_start,
  COUNT(*) AS records,
  COUNT(DISTINCT event_lmd::date) AS active_days
FROM fda_recalls_bronze
GROUP BY DATE_TRUNC('week', event_lmd)
ORDER BY week_start;

-- 4. Edit detection: recall_event_ids with multiple distinct content hashes
--    A hash_versions > 1 means the same recall_event_id was re-fetched with
--    changed content — the content hash dedup mechanism captured an edit.
--    hash_versions = total_rows confirms every row for that event has a unique hash.
SELECT recall_event_id, COUNT(DISTINCT content_hash) AS hash_versions, COUNT(*) AS total_rows
FROM fda_recalls_bronze
GROUP BY recall_event_id
HAVING COUNT(DISTINCT content_hash) > 1
ORDER BY hash_versions DESC
LIMIT 20;

-- 5. Total rows vs unique recall identifiers (dedup summary)
--    recall_event_id is NOT unique (one event = many products).
--    source_recall_id (PRODUCTID) is the true dedup key — but even that
--    can have 2 rows if the same product was edited (different content hashes).
SELECT
  COUNT(*) AS total_rows,
  COUNT(DISTINCT recall_event_id) AS unique_event_ids,
  COUNT(*) - COUNT(DISTINCT recall_event_id) AS apparent_duplicates
FROM fda_recalls_bronze;

-- 6. Multi-product event detail: rows, hash versions, and distinct product IDs per event
--    Confirms whether multiple rows per event_id are from different products
--    (distinct source_recall_ids) or from edits to the same product (same
--    source_recall_id, different content_hash).
SELECT
  recall_event_id,
  COUNT(*) AS total_rows,
  COUNT(DISTINCT content_hash) AS hash_versions,
  COUNT(DISTINCT source_recall_id) AS distinct_product_ids,
  MIN(event_lmd::date) AS event_lmd_date
FROM fda_recalls_bronze
GROUP BY recall_event_id
HAVING COUNT(*) > 1
ORDER BY total_rows DESC
LIMIT 10;

-- 7. Deep-dive into a specific recall event (substitute any recall_event_id)
--    Used to confirm whether multiple rows are from different products or
--    from content edits — key evidence for validating dedup key design.
SELECT source_recall_id, recall_event_id, recall_num, firm_legal_nam,
  event_lmd::date, content_hash, extraction_timestamp::date
FROM fda_recalls_bronze
WHERE recall_event_id = 98779
ORDER BY source_recall_id, extraction_timestamp;

-- 8. Center code distribution
--    Shows which FDA centers (CDRH, CDER, CFSAN, etc.) contribute most records
--    and unique events. Important for silver partitioning and join strategy.
SELECT center_cd, COUNT(*) AS rows, COUNT(DISTINCT recall_event_id) AS unique_events
FROM fda_recalls_bronze
GROUP BY center_cd
ORDER BY rows DESC;

-- 9. Product type distribution
--    Cross-checks center_cd mapping (CDRH=Devices, HFP=Food, etc.)
--    and gives a product-level view for silver filtering.
SELECT product_type_short, COUNT(*) AS rows
FROM fda_recalls_bronze
GROUP BY product_type_short
ORDER BY rows DESC;

-- 10. Phase distribution
--     Reveals the ratio of open vs. closed recalls.
--     Ongoing phase drives termination_dt nullability (finding F).
SELECT phase_txt, COUNT(*) AS rows
FROM fda_recalls_bronze
GROUP BY phase_txt
ORDER BY rows DESC;

-- 11. Null rates for all nullable fields
--     Identifies which columns are reliably populated vs. sparsely filled.
--     Directly informs which silver columns can be NOT NULL vs. must allow NULL.
SELECT
  ROUND(100.0 * SUM(CASE WHEN recall_num IS NULL THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_null_recall_num,
  ROUND(100.0 * SUM(CASE WHEN rid IS NULL THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_null_rid,
  ROUND(100.0 * SUM(CASE WHEN firm_fei_num IS NULL THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_null_firm_fei_num,
  ROUND(100.0 * SUM(CASE WHEN phase_txt IS NULL THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_null_phase_txt,
  ROUND(100.0 * SUM(CASE WHEN center_classification_type_txt IS NULL THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_null_class_type,
  ROUND(100.0 * SUM(CASE WHEN recall_initiation_dt IS NULL THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_null_initiation_dt,
  ROUND(100.0 * SUM(CASE WHEN termination_dt IS NULL THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_null_termination_dt,
  ROUND(100.0 * SUM(CASE WHEN product_description_txt IS NULL THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_null_product_desc,
  ROUND(100.0 * SUM(CASE WHEN product_distributed_quantity IS NULL THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_null_quantity
FROM fda_recalls_bronze;

-- 12. product_distributed_quantity value samples
--     This is a free-text field with no enforced format. Sampling the most
--     common values reveals the range of formats that silver cleaning must handle.
SELECT product_distributed_quantity, COUNT(*) AS occurrences
FROM fda_recalls_bronze
WHERE product_distributed_quantity IS NOT NULL
GROUP BY product_distributed_quantity
ORDER BY occurrences DESC
LIMIT 10;

-- 13. null recall_num breakdown by center
--     Identifies which centers are responsible for the 1% null recall_num rate.
--     Helps determine whether silver can require this field for specific centers.
SELECT center_cd, COUNT(*) AS null_recall_num_rows
FROM fda_recalls_bronze
WHERE recall_num IS NULL
GROUP BY center_cd
ORDER BY null_recall_num_rows DESC;

-- 14. Top spike days
--     Identifies the highest-volume single days; used to distinguish
--     normal variation from large multi-product batch publications.
SELECT event_lmd::date AS day, COUNT(*) AS records
FROM fda_recalls_bronze
GROUP BY event_lmd::date
ORDER BY records DESC
LIMIT 5;

-- 15. Weekday gap analysis
--     Finds weekdays with zero FDA activity. Expected = US federal holidays.
--     No-gap result in 90 days (except Presidents Day) confirms reliable
--     daily publication.
WITH date_series AS (
  SELECT generate_series(
    '2026-01-29'::date, '2026-04-27'::date, '1 day'::interval
  )::date AS day
),
active_days AS (
  SELECT DISTINCT event_lmd::date AS day FROM fda_recalls_bronze
)
SELECT d.day, TO_CHAR(d.day, 'Day') AS day_name
FROM date_series d
LEFT JOIN active_days a ON d.day = a.day
WHERE a.day IS NULL AND EXTRACT(DOW FROM d.day) NOT IN (0, 6)
ORDER BY d.day;

-- 16. Extraction run history
--     Confirms pipeline runs were recorded with correct counts, status, and timing.
SELECT source, status, records_extracted, records_inserted, records_rejected,
  started_at, finished_at,
  EXTRACT(EPOCH FROM (finished_at - started_at))::int AS duration_seconds
FROM extraction_runs
WHERE source = 'fda'
ORDER BY started_at;
```
