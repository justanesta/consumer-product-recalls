-- Phase 5c — characterize the rows that survived bronze dedup on a
-- specific NHTSA extraction run.
--
-- Why this exists: a re-run of `recalls extract nhtsa --since=2023-12-01`
-- after a prior identical run inserted only 194 of 72,567 fetched rows
-- (run 8fb4e268-... on 2026-05-08). The other 72,373 were rejected by
-- the content-hash dedup path (ADR 0007 + ADR 0030 amended). This
-- script answers: WHY did the survivors land — net-new identities, or
-- amendments to existing identities — and what fields drove the hash
-- to change?
--
-- Identity model (per src/extractors/nhtsa.py:464-477):
--   * identity_fields = 11-tuple
--       (campno, maketxt, modeltxt, yeartxt, compname, rcl_cmpt_id,
--        mfr_comp_ptno, mfr_comp_desc, mfr_comp_name, endman, bgman)
--   * hash_exclude_fields = {source_recall_id}
--   * within_batch_dedup = True
--   * allow_null_identity = True
-- A row lands iff its 11-tuple is new OR its content_hash differs from
-- the latest prior row sharing that 11-tuple. source_recall_id alone
-- changing does NOT trigger a re-insert (it's hash-excluded), so any
-- amendment we observe here means at least one of the 17 non-identity,
-- non-excluded fields changed upstream.
--
-- Usage:
--   psql ... -f explore_incremental_delta.sql
--       -> targets the most recent successful nhtsa run
--   psql ... -v run_id='8fb4e268-0769-4ce7-8913-1557c288aae1' \
--            -f explore_incremental_delta.sql
--       -> targets the passed run

\set ON_ERROR_STOP on
\pset null '<NULL>'

-- Resolve the run_id: passed via -v, or default to most recent success.
\if :{?run_id}
\echo
\echo === Using passed run_id: :run_id ===
\else
select run_id as run_id
from extraction_runs
where source = 'nhtsa' and status = 'success'
order by started_at desc
limit 1
\gset
\echo
\echo === Defaulted to most recent successful nhtsa run: :run_id ===
\endif

-- Map run_id -> raw_landing_path so we can pick the run's bronze rows.
-- (nhtsa_recalls_bronze has raw_landing_path but no run_id column.)
select raw_landing_path as run_landing_path
from extraction_runs
where source = 'nhtsa' and run_id = :'run_id'
\gset

\echo Landing path: :run_landing_path
\echo

\echo '=== Q1: row count, RCDATE span, null RCDATE count ==='
select
    count(*)                                  as rows_inserted,
    min(rcdate)                               as min_rcdate,
    max(rcdate)                               as max_rcdate,
    count(*) filter (where rcdate is null)    as null_rcdate
from nhtsa_recalls_bronze
where raw_landing_path = :'run_landing_path';

\echo
\echo '=== Q1b: top 10 manufacturers in this run ==='
select mfgname, count(*) as n
from nhtsa_recalls_bronze
where raw_landing_path = :'run_landing_path'
group by mfgname
order by n desc
limit 10;

\echo
\echo '=== Q1c: top 10 components in this run ==='
select compname, count(*) as n
from nhtsa_recalls_bronze
where raw_landing_path = :'run_landing_path'
group by compname
order by n desc
limit 10;

\echo
\echo '=== Q1d: RCDATE histogram by year — recent vs. backfilled? ==='
select extract(year from rcdate)::int as rcdate_year, count(*) as n
from nhtsa_recalls_bronze
where raw_landing_path = :'run_landing_path'
group by rcdate_year
order by rcdate_year;

\echo
\echo '=== Q2: net-new 11-tuple vs. amendment of existing 11-tuple ==='
\echo 'net_new   : no prior bronze row shares the 11-tuple identity'
\echo 'amendment : 11-tuple existed; ≥1 hashed field differed -> new content_hash'
with run_rows as (
    select *
    from nhtsa_recalls_bronze
    where raw_landing_path = :'run_landing_path'
),
classified as (
    select
        r.id,
        case when exists (
            select 1
            from nhtsa_recalls_bronze prior
            where prior.raw_landing_path <> r.raw_landing_path
              and prior.campno        is not distinct from r.campno
              and prior.maketxt       is not distinct from r.maketxt
              and prior.modeltxt      is not distinct from r.modeltxt
              and prior.yeartxt       is not distinct from r.yeartxt
              and prior.compname      is not distinct from r.compname
              and prior.rcl_cmpt_id   is not distinct from r.rcl_cmpt_id
              and prior.mfr_comp_ptno is not distinct from r.mfr_comp_ptno
              and prior.mfr_comp_desc is not distinct from r.mfr_comp_desc
              and prior.mfr_comp_name is not distinct from r.mfr_comp_name
              and prior.endman        is not distinct from r.endman
              and prior.bgman         is not distinct from r.bgman
        ) then 'amendment' else 'net_new' end as kind
    from run_rows r
)
select kind, count(*) as n
from classified
group by kind
order by kind;

\echo
\echo '=== Q3: which non-identity fields changed across amendments? ==='
\echo 'Joins each run row to its latest prior bronze row on the 11-tuple,'
\echo 'then counts how many amendments differ on each non-identity field.'
\echo 'Source_recall_id is hash-excluded, so its changes alone never cause'
\echo 'an insert — but they may co-occur with real changes.'
with run_rows as (
    select *
    from nhtsa_recalls_bronze
    where raw_landing_path = :'run_landing_path'
),
paired as (
    select distinct on (r.id)
        r.id                        as run_id,
        r.source_recall_id          as new_source_recall_id,
        prior.source_recall_id      as old_source_recall_id,
        r.mfgcampno         as new_mfgcampno,         prior.mfgcampno         as old_mfgcampno,
        r.mfgname           as new_mfgname,           prior.mfgname           as old_mfgname,
        r.rcltype           as new_rcltype,           prior.rcltype           as old_rcltype,
        r.potaff            as new_potaff,            prior.potaff            as old_potaff,
        r.odate             as new_odate,             prior.odate             as old_odate,
        r.influenced_by     as new_influenced_by,     prior.influenced_by     as old_influenced_by,
        r.mfgtxt            as new_mfgtxt,            prior.mfgtxt            as old_mfgtxt,
        r.rcdate            as new_rcdate,            prior.rcdate            as old_rcdate,
        r.datea             as new_datea,             prior.datea             as old_datea,
        r.rpno              as new_rpno,              prior.rpno              as old_rpno,
        r.fmvss             as new_fmvss,             prior.fmvss             as old_fmvss,
        r.desc_defect       as new_desc_defect,       prior.desc_defect       as old_desc_defect,
        r.conequence_defect as new_conequence_defect, prior.conequence_defect as old_conequence_defect,
        r.corrective_action as new_corrective_action, prior.corrective_action as old_corrective_action,
        r.notes             as new_notes,             prior.notes             as old_notes,
        r.do_not_drive      as new_do_not_drive,      prior.do_not_drive      as old_do_not_drive,
        r.park_outside      as new_park_outside,      prior.park_outside      as old_park_outside
    from run_rows r
    join nhtsa_recalls_bronze prior
      on prior.raw_landing_path <> r.raw_landing_path
     and prior.campno        is not distinct from r.campno
     and prior.maketxt       is not distinct from r.maketxt
     and prior.modeltxt      is not distinct from r.modeltxt
     and prior.yeartxt       is not distinct from r.yeartxt
     and prior.compname      is not distinct from r.compname
     and prior.rcl_cmpt_id   is not distinct from r.rcl_cmpt_id
     and prior.mfr_comp_ptno is not distinct from r.mfr_comp_ptno
     and prior.mfr_comp_desc is not distinct from r.mfr_comp_desc
     and prior.mfr_comp_name is not distinct from r.mfr_comp_name
     and prior.endman        is not distinct from r.endman
     and prior.bgman         is not distinct from r.bgman
    order by r.id, prior.extraction_timestamp desc
),
changed as (
    select unnest(array_remove(array[
        case when new_source_recall_id  is distinct from old_source_recall_id  then 'source_recall_id (hash-excluded)' end,
        case when new_mfgcampno         is distinct from old_mfgcampno         then 'mfgcampno'         end,
        case when new_mfgname           is distinct from old_mfgname           then 'mfgname'           end,
        case when new_rcltype           is distinct from old_rcltype           then 'rcltype'           end,
        case when new_potaff            is distinct from old_potaff            then 'potaff'            end,
        case when new_odate             is distinct from old_odate             then 'odate'             end,
        case when new_influenced_by     is distinct from old_influenced_by     then 'influenced_by'     end,
        case when new_mfgtxt            is distinct from old_mfgtxt            then 'mfgtxt'            end,
        case when new_rcdate            is distinct from old_rcdate            then 'rcdate'            end,
        case when new_datea             is distinct from old_datea             then 'datea'             end,
        case when new_rpno              is distinct from old_rpno              then 'rpno'              end,
        case when new_fmvss             is distinct from old_fmvss             then 'fmvss'             end,
        case when new_desc_defect       is distinct from old_desc_defect       then 'desc_defect'       end,
        case when new_conequence_defect is distinct from old_conequence_defect then 'conequence_defect' end,
        case when new_corrective_action is distinct from old_corrective_action then 'corrective_action' end,
        case when new_notes             is distinct from old_notes             then 'notes'             end,
        case when new_do_not_drive      is distinct from old_do_not_drive      then 'do_not_drive'      end,
        case when new_park_outside      is distinct from old_park_outside      then 'park_outside'      end
    ], NULL)) as changed_field
    from paired
)
select changed_field, count(*) as n_amendments_changed_here
from changed
group by changed_field
order by n_amendments_changed_here desc;

\echo
\echo '=== Q4: sample 5 amendments — old vs new for the most-changed text fields ==='
\echo 'Useful for eyeballing: do amendments look like real edits, or whitespace/'
\echo 'casing churn? If the latter, consider canonicalization upstream.'
with run_rows as (
    select *
    from nhtsa_recalls_bronze
    where raw_landing_path = :'run_landing_path'
),
sample as (
    select distinct on (r.id)
        r.campno,
        r.maketxt, r.modeltxt, r.yeartxt,
        r.source_recall_id          as new_record_id,
        prior.source_recall_id      as old_record_id,
        r.desc_defect               as new_desc_defect,
        prior.desc_defect           as old_desc_defect,
        r.corrective_action         as new_corrective_action,
        prior.corrective_action     as old_corrective_action,
        r.notes                     as new_notes,
        prior.notes                 as old_notes
    from run_rows r
    join nhtsa_recalls_bronze prior
      on prior.raw_landing_path <> r.raw_landing_path
     and prior.campno        is not distinct from r.campno
     and prior.maketxt       is not distinct from r.maketxt
     and prior.modeltxt      is not distinct from r.modeltxt
     and prior.yeartxt       is not distinct from r.yeartxt
     and prior.compname      is not distinct from r.compname
     and prior.rcl_cmpt_id   is not distinct from r.rcl_cmpt_id
     and prior.mfr_comp_ptno is not distinct from r.mfr_comp_ptno
     and prior.mfr_comp_desc is not distinct from r.mfr_comp_desc
     and prior.mfr_comp_name is not distinct from r.mfr_comp_name
     and prior.endman        is not distinct from r.endman
     and prior.bgman         is not distinct from r.bgman
    order by r.id, prior.extraction_timestamp desc
)
select *
from sample
where new_desc_defect       is distinct from old_desc_defect
   or new_corrective_action is distinct from old_corrective_action
   or new_notes             is distinct from old_notes
limit 5;
