-- Phase 5c — diagnose why the second `recalls extract nhtsa --since 2024-01-01`
-- run inserted all 66,078 rows instead of skipping the duplicates loaded by
-- the prior run.
--
-- Three possibilities to distinguish, per the loader's dedup logic
-- (src/bronze/loader.py:191-199):
--   (1) The prior run isn't in this DB — different Neon branch, dropped
--       table, or a migration recreated the bronze table between runs.
--   (2) The data genuinely re-versioned — NHTSA edited at least one field
--       on every row in the 2-day window. Implausible at face value.
--   (3) The hash function is non-deterministic — bug in
--       src/bronze/hashing.py reading something order-dependent or
--       timezone-sensitive out of model_dump().
--
-- Decision rules:
--   Q3 returns 0 rows AND Q1 total ≈ 66k → (1).
--   Q3 returns ~66k rows with hash_versions=2 AND Q1 total ≈ 132k → (2) or (3).
--   For (2) vs (3), inspect Q4's two timestamped versions of the sample
--   row — a real field difference points to (2); identical field values
--   with different content_hash points to (3).
--
-- Run with: psql ... -f scripts/sql/nhtsa/bronze/diagnose_full_reinsert.sql

\echo '=== Q1: total bronze rows, distinct ids, extraction-timestamp spread ==='
-- Discriminator between possibility (1) and (2)/(3):
--   total ≈ 66k, distinct_load_dates = 1 → (1) prior run isn't here
--   total ≈ 132k, distinct_load_dates = 2 → both runs landed (no dedup)
select
  count(*) as total_rows,
  count(distinct source_recall_id) as distinct_ids,
  min(extraction_timestamp) as earliest,
  max(extraction_timestamp) as latest,
  count(distinct extraction_timestamp::date) as distinct_load_dates
from nhtsa_recalls_bronze;

\echo ''
\echo '=== Q2: today''s contribution alone ==='
-- Confirms today's run landed exactly the 66,078 rows the log claimed
-- (rules out a partial transaction).
select
  extraction_timestamp::date as load_date,
  count(*) as rows
from nhtsa_recalls_bronze
group by extraction_timestamp::date
order by load_date;

\echo ''
\echo '=== Q3: source_recall_ids with multiple distinct content hashes ==='
-- The decisive query for case (1) vs (2)/(3):
--   0 rows → prior run isn't in this DB; today's batch was effectively a
--           first load. Expect normal dedup behavior on the next run.
--   ~66k rows with hash_versions=2 → hashes really differ between the two
--           runs. Continue to Q4 to see whether a real field changed.
select source_recall_id, count(distinct content_hash) as hash_versions
from nhtsa_recalls_bronze
group by source_recall_id
having count(distinct content_hash) > 1
order by source_recall_id
limit 10;

\echo ''
\echo '=== Q3b: count of source_recall_ids that have multiple hash versions ==='
-- Scalar version of Q3 — quick "how widespread is this".
select count(*) as ids_with_multiple_hashes
from (
  select source_recall_id
  from nhtsa_recalls_bronze
  group by source_recall_id
  having count(distinct content_hash) > 1
) t;

\echo ''
\echo '=== Q4: side-by-side dump of one sample row''s two versions ==='
-- Auto-picks the first source_recall_id with 2 hash versions and dumps
-- both bronze rows. Use psql's expanded display (\x on) to read this.
-- Compare field-by-field:
--   - All scalar fields identical, only content_hash differs → case (3)
--     hash non-determinism. Open src/bronze/hashing.py.
--   - One or more scalar fields differ → case (2) genuine NHTSA edit.
--     File as a Finding in flat_file_observations.md.
-- If Q3 returned no rows, this query returns no rows too — that's
-- expected for case (1).
with sample_id as (
  select source_recall_id
  from nhtsa_recalls_bronze
  group by source_recall_id
  having count(distinct content_hash) > 1
  order by source_recall_id
  limit 1
)
select
  b.source_recall_id,
  b.extraction_timestamp,
  b.content_hash,
  b.campno, b.maketxt, b.modeltxt, b.yeartxt, b.mfgcampno,
  b.compname, b.mfgname, b.bgman, b.endman, b.rcltype, b.potaff,
  b.odate, b.influenced_by, b.mfgtxt, b.rcdate, b.datea,
  b.rpno, b.fmvss,
  -- Narrative fields truncated for readability; if the diff is in one
  -- of these, eyeball with a follow-up SELECT on the full column.
  left(b.desc_defect, 80) as desc_defect_80,
  left(b.conequence_defect, 80) as conequence_defect_80,
  left(b.corrective_action, 80) as corrective_action_80,
  left(b.notes, 80) as notes_80,
  b.rcl_cmpt_id,
  b.mfr_comp_name,
  left(b.mfr_comp_desc, 80) as mfr_comp_desc_80,
  b.mfr_comp_ptno,
  b.do_not_drive, b.park_outside
from nhtsa_recalls_bronze b
join sample_id s on b.source_recall_id = s.source_recall_id
order by b.extraction_timestamp;

\echo ''
\echo '=== Q5: extraction_runs history — confirms two distinct runs landed ==='
-- Cross-reference: both NHTSA runs should appear here with status='success'
-- and matching records_inserted counts. response_inner_content_sha256
-- reveals whether the un-zipped TSV bytes were identical between runs —
-- if they match, the upstream content didn't change and case (3)
-- non-determinism is the prime suspect.
select
  started_at,
  status,
  records_extracted,
  records_inserted,
  records_rejected,
  change_type,
  left(response_body_sha256, 12) as zip_sha_prefix,
  left(response_inner_content_sha256, 12) as inner_sha_prefix
from extraction_runs
where source = 'nhtsa'
order by started_at;

\echo ''
\echo '=== Q6: alembic version (rule out a migration between runs) ==='
-- If the alembic_version row has changed since the prior run (look at
-- `git log migrations/`), bronze may have been recreated and case (1) is
-- explained. 0011 is current head as of Phase 5c Step 2.
select version_num from alembic_version;
