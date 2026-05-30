-- Phase 5d Step 7 (detail) pre-flight: confirm ready to seed uscg_manufacturer_details.
-- Run BEFORE `recalls extract uscg_manufacturer_details`.
--   listing_rows         : raw listing rows (all versions) — sanity only
--   listing_distinct_mics: the actual work-list size = # detail pages to fetch
--                          (~16.3k expected; this drives the ~4.5h wall-clock)
--   detail_rows          : should be 0 on a cold start (run #1 = full seed)
--   watermark_row        : must be 1 (seeded by migration 0018; FK + freshness target)
SELECT 'listing_rows'          AS check_name, count(*)::text AS value
FROM uscg_manufacturers_bronze
UNION ALL
SELECT 'listing_distinct_mics', count(DISTINCT source_recall_id)::text
FROM uscg_manufacturers_bronze
UNION ALL
SELECT 'detail_rows',           count(*)::text
FROM uscg_manufacturer_details_bronze
UNION ALL
SELECT 'watermark_row',         count(*)::text
FROM source_watermarks
WHERE source = 'uscg_manufacturer_details';
