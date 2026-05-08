-- Phase 5c — investigate the 45-row tire-recall collision and test
-- whether the 7-tuple (adding rcl_cmpt_id) is row-unique.
--
-- Q1b of verify_six_tuple_identity.sql showed 19,482 rows colliding on
-- the 6-tuple (campno, make, model, year, compname, mfr_comp_ptno).
-- All visible collisions are tire (rcltype='T') recalls. The
-- (24T014000, PRINX, HiCOUNTRY R/T HR1, 9999, TIRES:MARKINGS,
-- 9265030306) tuple repeats 45 times. Three things to figure out:
--
--   Q1 — Within the 45-row collision, what differs? Distinct-count per
--        column reveals the missing row-grain dimension.
--   Q2 — Is rcl_cmpt_id stable across regenerations using a SET-equality
--        test (same shape as verify_six_tuple_identity.sql Q2)?
--        Replaces the cartesian-blown Q-A3 from verify_natural_key_candidate.sql.
--   Q3 — Does adding rcl_cmpt_id to make a 7-tuple yield row-uniqueness?
--   Q4 — Visual dump of a small slice of the 45-row PRINX set so we can
--        eyeball what's going on. Use \x on.
--
-- Run with: psql ... -f scripts/sql/nhtsa/bronze/investigate_tire_collision.sql

\echo '=== Q1: distinct-count per column across the 45-row PRINX collision ==='
\echo 'rows=45 expected. Whichever column shows distinct_count=45 is the'
\echo 'row-grain dimension hidden under the 6-tuple.'
with dup_set as (
  select *
  from nhtsa_recalls_bronze
  where extraction_timestamp::date = '2026-05-07'
    and campno = '24T014000'
    and maketxt = 'PRINX'
    and modeltxt = 'HiCOUNTRY R/T HR1'
    and yeartxt = '9999'
    and compname = 'TIRES:MARKINGS'
    and mfr_comp_ptno = '9265030306'
)
select
  count(*) as rows,
  count(distinct source_recall_id) as source_recall_id,
  count(distinct mfgcampno) as mfgcampno,
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
  count(distinct do_not_drive) as do_not_drive,
  count(distinct park_outside) as park_outside,
  count(distinct content_hash) as content_hash
from dup_set;

\echo ''
\echo '=== Q2: rcl_cmpt_id stability across regenerations (set-equality test) ==='
\echo 'For every 5-tuple (campno + vehicle scope + compname) present in'
\echo 'both May 5 and May 7, compare the SET of rcl_cmpt_id values.'
\echo 'matches = identical sets; mismatches = real shift, would disqualify.'
\echo 'Identical structure to verify_six_tuple_identity.sql Q2 which gave 0 mismatches for mfr_comp_ptno.'
with may5 as (
  select campno, maketxt, modeltxt, yeartxt, compname,
    array_agg(distinct rcl_cmpt_id order by rcl_cmpt_id) as ids
  from nhtsa_recalls_bronze
  where extraction_timestamp::date = '2026-05-05'
    and rcl_cmpt_id is not null
  group by 1,2,3,4,5
),
may7 as (
  select campno, maketxt, modeltxt, yeartxt, compname,
    array_agg(distinct rcl_cmpt_id order by rcl_cmpt_id) as ids
  from nhtsa_recalls_bronze
  where extraction_timestamp::date = '2026-05-07'
    and rcl_cmpt_id is not null
  group by 1,2,3,4,5
)
select
  count(*) as five_tuples_in_both,
  count(*) filter (where a.ids = b.ids) as matches,
  count(*) filter (where a.ids <> b.ids) as mismatches
from may5 a
inner join may7 b using (campno, maketxt, modeltxt, yeartxt, compname);

\echo ''
\echo '=== Q2b: sample of rcl_cmpt_id mismatches (if any) ==='
\echo 'Empty result = rcl_cmpt_id is fully stable across the two snapshots.'
\echo 'Non-empty rows are real-shift evidence — eyeball the sample arrays.'
with may5 as (
  select campno, maketxt, modeltxt, yeartxt, compname,
    array_agg(distinct rcl_cmpt_id order by rcl_cmpt_id) as ids_may5
  from nhtsa_recalls_bronze
  where extraction_timestamp::date = '2026-05-05'
    and rcl_cmpt_id is not null
  group by 1,2,3,4,5
),
may7 as (
  select campno, maketxt, modeltxt, yeartxt, compname,
    array_agg(distinct rcl_cmpt_id order by rcl_cmpt_id) as ids_may7
  from nhtsa_recalls_bronze
  where extraction_timestamp::date = '2026-05-07'
    and rcl_cmpt_id is not null
  group by 1,2,3,4,5
)
select a.campno, a.maketxt, a.modeltxt, a.yeartxt, a.compname,
  array_length(a.ids_may5, 1) as count_may5,
  array_length(b.ids_may7, 1) as count_may7,
  a.ids_may5[1:5] as sample_may5,
  b.ids_may7[1:5] as sample_may7
from may5 a
inner join may7 b using (campno, maketxt, modeltxt, yeartxt, compname)
where a.ids_may5 <> b.ids_may7
limit 10;

\echo ''
\echo '=== Q3: 7-tuple row-uniqueness across the full May 7 corpus ==='
\echo 'Adds rcl_cmpt_id to the 6-tuple. If total_rows = distinct_tuples,'
\echo 'identity is locked. If a deficit remains, yet another dimension'
\echo 'is in play (likely zero given Q1 results).'
select
  count(*) as total_rows,
  count(distinct (campno, maketxt, modeltxt, yeartxt, compname, rcl_cmpt_id, mfr_comp_ptno)) as distinct_7_tuples,
  count(*) - count(distinct (campno, maketxt, modeltxt, yeartxt, compname, rcl_cmpt_id, mfr_comp_ptno)) as still_colliding
from nhtsa_recalls_bronze
where extraction_timestamp::date = '2026-05-07';

\echo ''
\echo '=== Q3b: any remaining 7-tuple collisions ==='
\echo 'If Q3 shows still_colliding > 0, this surfaces concrete examples.'
select campno, maketxt, modeltxt, yeartxt, compname, rcl_cmpt_id,
  coalesce(mfr_comp_ptno, '<null>') as mfr_comp_ptno,
  count(*) as rows
from nhtsa_recalls_bronze
where extraction_timestamp::date = '2026-05-07'
group by 1,2,3,4,5,6,7
having count(*) > 1
order by rows desc
limit 10;

\echo ''
\echo '=== Q4: vertical dump of 5 rows from the 45-row PRINX collision ==='
\echo 'Use `\x on` for readability. Eyeball what differs across the rows;'
\echo 'cross-reference with Q1 distinct-counts to confirm the dimension.'
select source_recall_id, rcl_cmpt_id, mfr_comp_name, mfr_comp_desc,
  mfr_comp_ptno, content_hash
from nhtsa_recalls_bronze
where extraction_timestamp::date = '2026-05-07'
  and campno = '24T014000'
  and maketxt = 'PRINX'
  and modeltxt = 'HiCOUNTRY R/T HR1'
  and yeartxt = '9999'
  and compname = 'TIRES:MARKINGS'
  and mfr_comp_ptno = '9265030306'
order by rcl_cmpt_id
limit 5;
