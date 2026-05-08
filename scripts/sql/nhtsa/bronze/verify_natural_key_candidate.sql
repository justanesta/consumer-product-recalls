-- Phase 5c — verify NHTSA natural-key candidates against the existing
-- (polluted) bronze data, before redesigning identity_fields.
--
-- Context: RCL.txt line 30 documents RECORD_ID as a "Running Sequence
-- Number" (= row counter assigned at file-generation time), not a
-- stable per-row natural key. Empirical evidence in
-- diagnose_full_reinsert.sql Q4 confirms NHTSA reassigns RECORD_ID on
-- regeneration. We need a different identity field.
--
-- RCL_CMPT_ID (field 24) is the strongest single-field candidate per
-- RCL.txt: "Number That Uniquely Identifies A Recalled Component" —
-- different wording from RECORD_ID's "Running Sequence Number." But
-- "uniquely identifies" already misled us once; verify empirically
-- before trusting.
--
-- Both queries operate against the polluted bronze (132,135 rows from
-- the May 5 + May 7 snapshots). The pollution doesn't affect this test
-- — we can still pin a real-world recall by CAMPNO + vehicle scope and
-- compare its RCL_CMPT_ID across the two snapshots.
--
-- Decision rules:
--   Q-A returns same rcl_cmpt_id on both rows → RCL_CMPT_ID is stable.
--     Use identity_fields=("rcl_cmpt_id",) or ("campno","rcl_cmpt_id").
--   Q-A returns different rcl_cmpt_id on the two rows → it's also a
--     regen-time counter. Fall back to the semantic composite
--     ("campno","maketxt","modeltxt","yeartxt","compname").
--   Q-B returns rows → RCL_CMPT_ID isn't even within-file unique
--     (would contradict the field description; would be a finding).
--
-- Run with: psql ... -f scripts/sql/nhtsa/bronze/verify_natural_key_candidate.sql

\echo '=== Q-A: stability across regenerations ==='
\echo 'Same physical recall (Vermeer BC900XL 2019 lug-nut, CAMPNO 24V357000)'
\echo 'pinned in both May 5 and May 7 snapshots. Compare rcl_cmpt_id.'
select
  extraction_timestamp::date as load_date,
  source_recall_id,
  rcl_cmpt_id,
  compname
from nhtsa_recalls_bronze
where campno = '24V357000'
  and maketxt = 'VERMEER'
  and modeltxt = 'BC900XL'
  and yeartxt = '2019'
order by extraction_timestamp;

\echo ''
\echo '=== Q-A2: broader stability check across multiple recalls ==='
\echo 'For every CAMPNO present in both snapshots, check whether the'
\echo 'rcl_cmpt_id values are identical (joined on campno + vehicle'
\echo 'scope). Counts rcl_cmpt_id mismatches — 0 = stable across the'
\echo 'whole corpus, not just one sample.'
with may5 as (
  select campno, maketxt, modeltxt, yeartxt, compname, rcl_cmpt_id
  from nhtsa_recalls_bronze
  where extraction_timestamp::date = '2026-05-05'
),
may7 as (
  select campno, maketxt, modeltxt, yeartxt, compname, rcl_cmpt_id
  from nhtsa_recalls_bronze
  where extraction_timestamp::date = '2026-05-07'
),
joined as (
  select
    a.campno, a.maketxt, a.modeltxt, a.yeartxt, a.compname,
    a.rcl_cmpt_id as rcl_cmpt_id_may5,
    b.rcl_cmpt_id as rcl_cmpt_id_may7
  from may5 a
  inner join may7 b using (campno, maketxt, modeltxt, yeartxt, compname)
)
select
  count(*) as rows_present_in_both_snapshots,
  count(*) filter (where rcl_cmpt_id_may5 is distinct from rcl_cmpt_id_may7) as mismatches,
  count(*) filter (where rcl_cmpt_id_may5 = rcl_cmpt_id_may7) as matches,
  count(*) filter (where rcl_cmpt_id_may5 is null and rcl_cmpt_id_may7 is null) as both_null,
  count(*) filter (where (rcl_cmpt_id_may5 is null) <> (rcl_cmpt_id_may7 is null)) as one_null
from joined;

\echo ''
\echo '=== Q-A3: sample of rcl_cmpt_id mismatches (if any) ==='
\echo 'Empty result = RCL_CMPT_ID is fully stable. Non-empty rows are'
\echo 'concrete counter-examples worth eyeballing.'
with may5 as (
  select campno, maketxt, modeltxt, yeartxt, compname, rcl_cmpt_id
  from nhtsa_recalls_bronze
  where extraction_timestamp::date = '2026-05-05'
),
may7 as (
  select campno, maketxt, modeltxt, yeartxt, compname, rcl_cmpt_id
  from nhtsa_recalls_bronze
  where extraction_timestamp::date = '2026-05-07'
)
select
  a.campno, a.maketxt, a.modeltxt, a.yeartxt, a.compname,
  a.rcl_cmpt_id as rcl_cmpt_id_may5,
  b.rcl_cmpt_id as rcl_cmpt_id_may7
from may5 a
inner join may7 b using (campno, maketxt, modeltxt, yeartxt, compname)
where a.rcl_cmpt_id is distinct from b.rcl_cmpt_id
limit 10;

\echo ''
\echo '=== Q-B: within-snapshot uniqueness of rcl_cmpt_id ==='
\echo '0 rows = rcl_cmpt_id is row-unique within a single TSV (matches'
\echo 'RCL.txt field description). Non-zero rows = it is not even'
\echo 'within-file unique, which would be a fresh finding.'
select rcl_cmpt_id, count(*) as rows
from nhtsa_recalls_bronze
where extraction_timestamp::date = '2026-05-07'
  and rcl_cmpt_id is not null
group by rcl_cmpt_id
having count(*) > 1
order by rows desc
limit 10;

\echo ''
\echo '=== Q-C: semantic-composite uniqueness fallback ==='
\echo 'If Q-A shows RCL_CMPT_ID is also unstable, this is the next-best'
\echo 'identity. 0 rows = (campno, maketxt, modeltxt, yeartxt, compname)'
\echo 'is row-unique within May 7 alone. Non-zero rows = we need to'
\echo 'widen the tuple.'
select campno, maketxt, modeltxt, yeartxt, compname, count(*) as rows
from nhtsa_recalls_bronze
where extraction_timestamp::date = '2026-05-07'
group by 1,2,3,4,5
having count(*) > 1
order by rows desc
limit 10;
