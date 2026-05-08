-- Phase 5c — verify the proposed 6-tuple identity for NHTSA bronze:
--   (campno, maketxt, modeltxt, yeartxt, compname, mfr_comp_ptno)
--
-- find_row_differentiator.sql showed mfr_comp_ptno is the row-grain
-- differentiator within both small (Vermeer 4-row) and large (tire
-- 139-row) duplicate sets. Three things still need checking before
-- committing this tuple to the schema:
--
--   Q1 — Within-snapshot row-uniqueness across the FULL May 7 corpus
--        (not just two samples).
--   Q2 — Cross-regeneration stability of mfr_comp_ptno itself
--        (the FORTUNE TIRES rcl_cmpt_id shift in Q-A3 is a yellow
--        flag for any NHTSA-numbering field).
--   Q3 — mfr_comp_ptno null rate, especially for the loaded scope.
--        Field added 2020-03-23 per RCL.txt change-log; expect NULLs
--        in any pre-2020 historical-seed corpus, but want to know
--        the null rate within the post-2020 corpus we've actually
--        loaded.
--
-- Run with: psql ... -f scripts/sql/nhtsa/bronze/verify_six_tuple_identity.sql

\echo '=== Q1: within-snapshot uniqueness of the 6-tuple ==='
\echo '0 rows = row-unique across all of May 7 — identity is safe.'
\echo 'Non-zero rows = remaining collisions; the tuple still needs widening.'
\echo 'NULL mfr_comp_ptno is treated as a single bucket (postgres groups NULLs together in GROUP BY).'
select
  campno, maketxt, modeltxt, yeartxt, compname,
  coalesce(mfr_comp_ptno, '<null>') as mfr_comp_ptno_or_null,
  count(*) as rows
from nhtsa_recalls_bronze
where extraction_timestamp::date = '2026-05-07'
group by 1,2,3,4,5,6
having count(*) > 1
order by rows desc
limit 20;

\echo ''
\echo '=== Q1b: how many distinct 6-tuples in the May 7 snapshot? ==='
\echo 'If row-unique, distinct_tuples = total_rows = 66,078.'
\echo 'Any deficit is the count of rows colliding on the 6-tuple.'
select
  count(*) as total_rows,
  count(distinct (campno, maketxt, modeltxt, yeartxt, compname, mfr_comp_ptno)) as distinct_tuples
from nhtsa_recalls_bronze
where extraction_timestamp::date = '2026-05-07';

\echo ''
\echo '=== Q2: cross-regeneration stability of mfr_comp_ptno ==='
\echo 'For every 5-tuple (campno + vehicle scope + compname) present in'
\echo 'both snapshots, check whether the SET of mfr_comp_ptno values is'
\echo 'identical. Different sets on the same 5-tuple = mfr_comp_ptno'
\echo 'shifted between regenerations (would disqualify it).'
\echo 'Expected: 0 mismatches if mfr_comp_ptno is stable.'
with may5 as (
  select campno, maketxt, modeltxt, yeartxt, compname,
    array_agg(distinct coalesce(mfr_comp_ptno, '<null>') order by coalesce(mfr_comp_ptno, '<null>')) as ptnos
  from nhtsa_recalls_bronze
  where extraction_timestamp::date = '2026-05-05'
  group by 1,2,3,4,5
),
may7 as (
  select campno, maketxt, modeltxt, yeartxt, compname,
    array_agg(distinct coalesce(mfr_comp_ptno, '<null>') order by coalesce(mfr_comp_ptno, '<null>')) as ptnos
  from nhtsa_recalls_bronze
  where extraction_timestamp::date = '2026-05-07'
  group by 1,2,3,4,5
)
select
  count(*) as five_tuples_in_both,
  count(*) filter (where a.ptnos = b.ptnos) as matches,
  count(*) filter (where a.ptnos <> b.ptnos) as mismatches
from may5 a
inner join may7 b using (campno, maketxt, modeltxt, yeartxt, compname);

\echo ''
\echo '=== Q2b: sample of mfr_comp_ptno mismatches (if any) ==='
\echo 'Empty result = mfr_comp_ptno is fully stable across the two'
\echo 'regenerations. Non-empty rows are concrete examples to eyeball.'
with may5 as (
  select campno, maketxt, modeltxt, yeartxt, compname,
    array_agg(distinct coalesce(mfr_comp_ptno, '<null>') order by coalesce(mfr_comp_ptno, '<null>')) as ptnos_may5
  from nhtsa_recalls_bronze
  where extraction_timestamp::date = '2026-05-05'
  group by 1,2,3,4,5
),
may7 as (
  select campno, maketxt, modeltxt, yeartxt, compname,
    array_agg(distinct coalesce(mfr_comp_ptno, '<null>') order by coalesce(mfr_comp_ptno, '<null>')) as ptnos_may7
  from nhtsa_recalls_bronze
  where extraction_timestamp::date = '2026-05-07'
  group by 1,2,3,4,5
)
select a.campno, a.maketxt, a.modeltxt, a.yeartxt, a.compname,
  array_length(a.ptnos_may5, 1) as ptno_count_may5,
  array_length(b.ptnos_may7, 1) as ptno_count_may7,
  a.ptnos_may5[1:5] as sample_may5,
  b.ptnos_may7[1:5] as sample_may7
from may5 a
inner join may7 b using (campno, maketxt, modeltxt, yeartxt, compname)
where a.ptnos_may5 <> b.ptnos_may7
limit 10;

\echo ''
\echo '=== Q3: mfr_comp_ptno null rate ==='
\echo 'Field added 2020-03-23 per RCL.txt change-log. Expect ~0% null'
\echo 'within the --since 2024-01-01 scope; non-zero is a finding worth'
\echo 'documenting (means even some post-2020 records lack the field,'
\echo 'and the historical-seed path will need a different identity).'
select
  count(*) as total_rows,
  count(*) filter (where mfr_comp_ptno is null) as null_rows,
  round(100.0 * count(*) filter (where mfr_comp_ptno is null) / count(*), 2) as pct_null,
  count(distinct mfr_comp_ptno) as distinct_ptnos,
  -- Per-rcltype split — tire recalls (T) might use it differently
  -- than vehicle recalls (V) etc.
  (select rcltype || ': ' || count(*) || ' rows, ' ||
    count(*) filter (where mfr_comp_ptno is null) || ' null'
   from nhtsa_recalls_bronze
   where extraction_timestamp::date = '2026-05-07' and rcltype = 'V'
   group by rcltype) as v_breakdown
from nhtsa_recalls_bronze
where extraction_timestamp::date = '2026-05-07';

\echo ''
\echo '=== Q3b: null rate broken down by rcltype + rcdate-year ==='
\echo 'Cleaner per-segment view. Columns: rcltype, year, rows, null_rows, pct_null.'
select
  rcltype,
  extract(year from rcdate)::int as year,
  count(*) as rows,
  count(*) filter (where mfr_comp_ptno is null) as null_rows,
  round(100.0 * count(*) filter (where mfr_comp_ptno is null) / count(*), 2) as pct_null
from nhtsa_recalls_bronze
where extraction_timestamp::date = '2026-05-07'
  and rcdate is not null
group by rcltype, extract(year from rcdate)
order by rcltype, year;
