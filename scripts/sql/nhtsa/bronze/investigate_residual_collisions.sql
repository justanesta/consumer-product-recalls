-- Phase 5c — investigate the 477-row residue from the 7-tuple identity test.
--
-- investigate_tire_collision.sql Q3 showed 65,601 distinct 7-tuples
-- across 66,078 rows. The 477 colliding rows fall into at least three
-- patterns (per Q3b):
--
--   (A) NISSAN TITAN style: same rcl_cmpt_id, same mfr_comp_ptno, 4 dup
--       rows. Yet another dimension hidden, or genuine NHTSA duplicates.
--   (B) Orange EV style: same rcl_cmpt_id, EMPTY mfr_comp_ptno, 4 dup
--       rows (vehicle recall — should have a part number but doesn't).
--   (C) ACHILLES style: same rcl_cmpt_id, EMPTY mfr_comp_ptno, 5 dup
--       rows (tire recall variant of B).
--
-- Same column-by-column distinct-count diagnostic that worked for the
-- Vermeer and PRINX cases. Whichever column shows distinct = row_count
-- is the missing dimension. If every column shows distinct = 1 except
-- source_recall_id (which is the regen-unstable counter), the rows are
-- byte-identical and the answer is dedup-at-extract-time.
--
-- Run with: psql ... -f scripts/sql/nhtsa/bronze/investigate_residual_collisions.sql

\echo '=== Q1: distinct-count per column across NISSAN TITAN 4-row collision ==='
\echo 'rows=4 expected. distinct=4 column = the hidden dimension.'
\echo 'all distinct=1 except source_recall_id and content_hash = byte-identical rows.'
with dup_set as (
  select *
  from nhtsa_recalls_bronze
  where extraction_timestamp::date = '2026-05-07'
    and campno = '24V580000'
    and maketxt = 'NISSAN'
    and modeltxt = 'TITAN'
    and yeartxt = '2021'
    and compname = 'AIR BAGS: AIR BAG/RESTRAINT CONTROL MODULE'
    and rcl_cmpt_id = '000127294004588751000001543'
    and mfr_comp_ptno = '98820 9FW4B'
)
select
  count(*) as rows,
  count(distinct source_recall_id) as source_recall_id,
  count(distinct mfgcampno) as mfgcampno,
  count(distinct mfgname) as mfgname,
  count(distinct bgman) as bgman,
  count(distinct endman) as endman,
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
  count(distinct mfr_comp_name) as mfr_comp_name,
  count(distinct mfr_comp_desc) as mfr_comp_desc,
  count(distinct do_not_drive) as do_not_drive,
  count(distinct park_outside) as park_outside,
  count(distinct content_hash) as content_hash
from dup_set;

\echo ''
\echo '=== Q1b: vertical dump of the 4 NISSAN TITAN rows ==='
\echo 'Use `\x on` for readability. Eyeball every field for the variation.'
select *
from nhtsa_recalls_bronze
where extraction_timestamp::date = '2026-05-07'
  and campno = '24V580000'
  and maketxt = 'NISSAN'
  and modeltxt = 'TITAN'
  and yeartxt = '2021'
  and compname = 'AIR BAGS: AIR BAG/RESTRAINT CONTROL MODULE'
  and rcl_cmpt_id = '000127294004588751000001543'
  and mfr_comp_ptno = '98820 9FW4B'
order by source_recall_id;

\echo ''
\echo '=== Q2: distinct-count per column across ACHILLES 5-row empty-ptno collision ==='
\echo 'rows=5 expected. Same diagnostic; mfr_comp_ptno is empty here so'
\echo 'we expect mfr_comp_name or mfr_comp_desc to carry the part identity.'
with dup_set as (
  select *
  from nhtsa_recalls_bronze
  where extraction_timestamp::date = '2026-05-07'
    and campno = '25T020000'
    and maketxt = 'ACHILLES'
    and modeltxt = 'ATR SPORT 2'
    and yeartxt = '9999'
    and compname = 'TIRES:MARKINGS'
    and rcl_cmpt_id = '000260791004398735000000305'
    and (mfr_comp_ptno is null or mfr_comp_ptno = '')
)
select
  count(*) as rows,
  count(distinct source_recall_id) as source_recall_id,
  count(distinct mfgcampno) as mfgcampno,
  count(distinct mfgname) as mfgname,
  count(distinct bgman) as bgman,
  count(distinct endman) as endman,
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
  count(distinct mfr_comp_name) as mfr_comp_name,
  count(distinct mfr_comp_desc) as mfr_comp_desc,
  count(distinct do_not_drive) as do_not_drive,
  count(distinct park_outside) as park_outside,
  count(distinct content_hash) as content_hash
from dup_set;

\echo ''
\echo '=== Q2b: vertical dump of ACHILLES rows ==='
select *
from nhtsa_recalls_bronze
where extraction_timestamp::date = '2026-05-07'
  and campno = '25T020000'
  and maketxt = 'ACHILLES'
  and modeltxt = 'ATR SPORT 2'
  and yeartxt = '9999'
  and compname = 'TIRES:MARKINGS'
  and rcl_cmpt_id = '000260791004398735000000305'
  and (mfr_comp_ptno is null or mfr_comp_ptno = '')
order by source_recall_id;

\echo ''
\echo '=== Q3: are NISSAN-style collisions a population-wide pattern? ==='
\echo 'Counts how many of the 477 colliding rows fall into each pattern:'
\echo '  empty-ptno: mfr_comp_ptno is null or empty'
\echo '  populated-ptno: real ptno but still colliding (the worrying case)'
with collisions as (
  select campno, maketxt, modeltxt, yeartxt, compname, rcl_cmpt_id, mfr_comp_ptno,
    count(*) as rows
  from nhtsa_recalls_bronze
  where extraction_timestamp::date = '2026-05-07'
  group by 1,2,3,4,5,6,7
  having count(*) > 1
)
select
  case
    when mfr_comp_ptno is null or mfr_comp_ptno = '' then 'empty_ptno'
    else 'populated_ptno'
  end as pattern,
  count(*) as collision_groups,
  sum(rows) as total_colliding_rows,
  sum(rows - 1) as excess_rows
from collisions
group by 1
order by total_colliding_rows desc;
