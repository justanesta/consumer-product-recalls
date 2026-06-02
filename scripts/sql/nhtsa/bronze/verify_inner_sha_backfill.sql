-- Verify the W6 by-archive inner-SHA backfill (migration 0021 + backfill_inner_sha_by_archive.py).
-- Run against the target Neon branch AFTER applying migration 0021 and running the backfill.
--
-- Expectation after a clean backfill: every NHTSA deep-rescan run (raw_landing_path is a JSON
-- manifest) carries a response_inner_content_sha256_by_archive map with BOTH archive URLs
-- (PRE_2010 + POST_2010). A run with the column still NULL, or missing an archive key, needs review.

-- Q1 — coverage: deep-rescan runs, how many populated, how many carry both archive keys.
SELECT
    count(*)                                          AS deep_rescan_runs,
    count(response_inner_content_sha256_by_archive)   AS populated,
    count(*) FILTER (
        WHERE response_inner_content_sha256_by_archive ?& array[
            'https://static.nhtsa.gov/odi/ffdd/rcl/FLAT_RCL_PRE_2010.zip',
            'https://static.nhtsa.gov/odi/ffdd/rcl/FLAT_RCL_POST_2010.zip'
        ]
    )                                                 AS populated_with_both_archives
FROM extraction_runs
WHERE source = 'nhtsa'
  AND raw_landing_path LIKE '%.json%';

-- Q2 — any deep-rescan run still missing the map (should return zero rows after backfill).
SELECT id, started_at, change_type, status, raw_landing_path
FROM extraction_runs
WHERE source = 'nhtsa'
  AND raw_landing_path LIKE '%.json%'
  AND response_inner_content_sha256_by_archive IS NULL
ORDER BY started_at DESC;

-- Q3 — the most-recent successful run's map: the baseline the W6 short-circuit reads.
SELECT id, started_at, response_inner_content_sha256_by_archive
FROM extraction_runs
WHERE source = 'nhtsa'
  AND status = 'success'
  AND response_inner_content_sha256_by_archive IS NOT NULL
ORDER BY started_at DESC
LIMIT 1;
