-- Phase 5c — find the field(s) that differentiate apparent-duplicate
-- rows in NHTSA's TSV.
--
-- Context: verify_natural_key_candidate.sql showed that no field or
-- 5-tuple composite is row-unique within a single TSV. The
-- Import_Instructions_Recalls.pdf MS-Access wizard tells users to let
-- Access auto-generate a synthetic primary key — NHTSA itself
-- acknowledges no natural per-row identity exists. Two possibilities:
--
--   (1) The "duplicate" rows are byte-identical → NHTSA exports
--       genuine duplicates (likely from a denormalized relational
--       export). Extract-time dedup on full-row content is the right
--       answer. Identity in bronze becomes the row's content_hash.
--   (2) The rows differ on a field we haven't inspected → the row
--       identity tuple just needs widening. Likely candidates:
--       mfr_comp_ptno (part-number variants), bgman/endman (mfg date
--       subranges), potaff (count tiers), one of the narrative fields.
--
-- Three queries, smallest-to-largest sample:
--   Q1: 4-row Vermeer manual set (smallest, fastest to eyeball).
--   Q2: 139-row tire-marking set (the Q-B finding).
--   Q3: vertical full-row dump of the 4 Vermeer rows for visual
--       confirmation — psql `\x on` recommended before running.
--
-- A column showing distinct_count = row_count is the differentiator.
-- A column showing distinct_count = 1 is constant across the dup set.
--
-- Run with: psql ... -f scripts/sql/nhtsa/bronze/find_row_differentiator.sql
-- Tip: `\x on` before running makes Q3 readable.

\echo '=== Q1: distinct-count per column across the 4-row Vermeer manual set ==='
\echo 'rows=4 expected. Any column with distinct_count=4 is the row-grain'
\echo 'differentiator. All columns with distinct_count=1 are constant.'
with dup_set as (
  select *
  from nhtsa_recalls_bronze
  where extraction_timestamp::date = '2026-05-05'
    and campno = '24V357000'
    and rcl_cmpt_id = '000127008005646759000000350'
)
select
  count(*) as rows,
  count(distinct source_recall_id) as source_recall_id,
  count(distinct campno) as campno,
  count(distinct maketxt) as maketxt,
  count(distinct modeltxt) as modeltxt,
  count(distinct yeartxt) as yeartxt,
  count(distinct mfgcampno) as mfgcampno,
  count(distinct compname) as compname,
  count(distinct mfgname) as mfgname,
  count(distinct bgman) as bgman,
  count(distinct endman) as endman,
  count(distinct rcltype) as rcltype,
  count(distinct potaff) as potaff,
  count(distinct odate) as odate,
  count(distinct influenced_by) as influenced_by,
  count(distinct mfgtxt) as mfgtxt,
  count(distinct rcdate) as rcdate,
  count(distinct datea) as datea,
  count(distinct rpno) as rpno,
  count(distinct fmvss) as fmvss,
  count(distinct desc_defect) as desc_defect,
  count(distinct conequence_defect) as conequence_defect,
  count(distinct corrective_action) as corrective_action,
  count(distinct notes) as notes,
  count(distinct rcl_cmpt_id) as rcl_cmpt_id,
  count(distinct mfr_comp_name) as mfr_comp_name,
  count(distinct mfr_comp_desc) as mfr_comp_desc,
  count(distinct mfr_comp_ptno) as mfr_comp_ptno,
  count(distinct do_not_drive) as do_not_drive,
  count(distinct park_outside) as park_outside,
  count(distinct content_hash) as content_hash
from dup_set;

\echo ''
\echo '=== Q2: distinct-count per column across the 139-row tire set ==='
\echo 'rows=139 expected. Same logic as Q1; tire recalls likely have a'
\echo 'higher-cardinality differentiator (mfr_comp_ptno is the prime'
\echo 'suspect for tire SKU variants).'
with dup_set as (
  select *
  from nhtsa_recalls_bronze
  where extraction_timestamp::date = '2026-05-07'
    and rcl_cmpt_id = '000127729005664532000000305'
)
select
  count(*) as rows,
  count(distinct source_recall_id) as source_recall_id,
  count(distinct campno) as campno,
  count(distinct maketxt) as maketxt,
  count(distinct modeltxt) as modeltxt,
  count(distinct yeartxt) as yeartxt,
  count(distinct mfgcampno) as mfgcampno,
  count(distinct compname) as compname,
  count(distinct mfgname) as mfgname,
  count(distinct bgman) as bgman,
  count(distinct endman) as endman,
  count(distinct rcltype) as rcltype,
  count(distinct potaff) as potaff,
  count(distinct odate) as odate,
  count(distinct influenced_by) as influenced_by,
  count(distinct mfgtxt) as mfgtxt,
  count(distinct rcdate) as rcdate,
  count(distinct datea) as datea,
  count(distinct rpno) as rpno,
  count(distinct fmvss) as fmvss,
  count(distinct desc_defect) as desc_defect,
  count(distinct conequence_defect) as conequence_defect,
  count(distinct corrective_action) as corrective_action,
  count(distinct notes) as notes,
  count(distinct rcl_cmpt_id) as rcl_cmpt_id,
  count(distinct mfr_comp_name) as mfr_comp_name,
  count(distinct mfr_comp_desc) as mfr_comp_desc,
  count(distinct mfr_comp_ptno) as mfr_comp_ptno,
  count(distinct do_not_drive) as do_not_drive,
  count(distinct park_outside) as park_outside,
  count(distinct content_hash) as content_hash
from dup_set;

\echo ''
\echo '=== Q3: vertical full-row dump of the 4 Vermeer manual rows ==='
\echo 'Use `\x on` for readability. Side-by-side, eyeball any field that'
\echo 'differs across the 4 rows. If everything except source_recall_id'
\echo 'is identical, NHTSA is shipping genuine duplicates and extract-'
\echo 'time dedup is the answer.'
select *
from nhtsa_recalls_bronze
where extraction_timestamp::date = '2026-05-05'
  and campno = '24V357000'
  and rcl_cmpt_id = '000127008005646759000000350'
order by source_recall_id;
