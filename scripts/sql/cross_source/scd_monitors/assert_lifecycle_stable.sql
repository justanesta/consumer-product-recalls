-- SCD monitor — recall lifecycle/status transition rate (cross-source).
--
-- PURPOSE: validate the SCD-type designation for the recall *lifecycle* axis
-- (FDA phase_txt Ongoing/Completed/Terminated; USDA recall_type Active Recall/Closed Recall/PHA;
-- USCG disposition open/closed). These are designated **Type-2-BENEFIT** in
-- documentation/audit/scd_field_designations.md: the key is stable (no fragmentation), but the
-- Ongoing→Terminated / Active→Closed transition is a genuine time-varying lifecycle event the
-- Type-1 latest-wins silver currently drops into bronze-only. This monitor measures how often
-- it transitions so the designation is data-driven.
--   • 0 transitions observed  → latest-wins is lossless for lifecycle.
--   • >0 transitions observed → confirms the Type-2-BENEFIT call (a lifecycle timeline exists to surface).
--
-- ⚠ MEASURE-FORWARD CAVEAT (identical to assert_classification_stable.sql): the Phase 6a.5
-- re-seeds wiped bronze's edit-version history, so this reads ~0 TODAY — "no banked history yet,"
-- NOT "recalls never change status." It accrues signal as daily incrementals + the weekly
-- deep-rescan (ADR 0010) re-bank content-hash versions. Re-run periodically.
--
-- METHOD: mirrors assert_classification_stable.sql — LAG over each key's bronze versions ordered
-- by extraction_timestamp; on rows where content_hash actually changed, bucket what the status did.
-- Rebaseline filter excludes change_type IN ('schema_rebaseline','hash_helper_rebaseline') so a
-- re-stamp isn't a false transition. USCG disposition is lower()-normalized (Finding R case-folding)
-- so Closed/CLOSED is not a false transition.
--
-- CPSC + NHTSA have no recall-lifecycle/status field (CPSC has no phase; NHTSA recalls carry no
-- open/closed status) — out of scope. NHTSA identity stability is covered by
-- nhtsa/bronze/assert_eleven_tuple_identity_stable.sql.
--
-- Feeds: documentation/audit/scd_field_designations.md (the `lifecycle_status` row's status).
-- Run with: psql ... -f scripts/sql/cross_source/scd_monitors/assert_lifecycle_stable.sql

\set ON_ERROR_STOP on
\pset null '<NULL>'

\echo '=== Q1: FDA phase transitions across content edits ==='
-- phase_txt over each PRODUCTID's bronze versions. CHANGED = a lifecycle move (Ongoing→Terminated).
with ordered as (
  select
    source_recall_id,
    extraction_timestamp,
    content_hash,
    nullif(trim(phase_txt), '')                 as status,
    lag(nullif(trim(phase_txt), '')) over w      as prev_status,
    lag(content_hash) over w                     as prev_content_hash
  from fda_recalls_bronze b
  left join extraction_runs r on b.raw_landing_path = r.raw_landing_path
  where r.change_type is null
     or r.change_type not in ('schema_rebaseline', 'hash_helper_rebaseline')
  window w as (partition by source_recall_id order by extraction_timestamp)
),
content_edits as (
  select * from ordered
  where prev_content_hash is not null and content_hash is distinct from prev_content_hash
)
select
  case
    when prev_status is null and status is null     then 'both_null'
    when prev_status is null                         then 'appeared_from_null'
    when status is null                              then 'disappeared_to_null'
    when status is distinct from prev_status         then 'CHANGED'
    else 'stable'
  end as transition_class,
  count(*) as n
from content_edits
group by 1
order by n desc;

\echo ''
\echo '=== Q2: USDA recall_type transitions across content edits ==='
-- recall_type over each (recall_number, langcode)'s bronze versions (Active Recall → Closed Recall).
with ordered as (
  select
    source_recall_id,
    langcode,
    extraction_timestamp,
    content_hash,
    nullif(trim(recall_type), '')                 as status,
    lag(nullif(trim(recall_type), '')) over w      as prev_status,
    lag(content_hash) over w                       as prev_content_hash
  from usda_fsis_recalls_bronze b
  left join extraction_runs r on b.raw_landing_path = r.raw_landing_path
  where r.change_type is null
     or r.change_type not in ('schema_rebaseline', 'hash_helper_rebaseline')
  window w as (partition by source_recall_id, langcode order by extraction_timestamp)
),
content_edits as (
  select * from ordered
  where prev_content_hash is not null and content_hash is distinct from prev_content_hash
)
select
  case
    when prev_status is null and status is null     then 'both_null'
    when prev_status is null                         then 'appeared_from_null'
    when status is null                              then 'disappeared_to_null'
    when status is distinct from prev_status         then 'CHANGED'
    else 'stable'
  end as transition_class,
  count(*) as n
from content_edits
group by 1
order by n desc;

\echo ''
\echo '=== Q3: USCG disposition transitions across content edits (case-normalized) ==='
-- disposition over each recall's bronze versions (open → closed); lower() so Closed/CLOSED is not
-- a false transition (Finding R).
with ordered as (
  select
    source_recall_id,
    extraction_timestamp,
    content_hash,
    lower(nullif(trim(disposition), ''))                 as status,
    lag(lower(nullif(trim(disposition), ''))) over w      as prev_status,
    lag(content_hash) over w                              as prev_content_hash
  from uscg_recalls_bronze b
  left join extraction_runs r on b.raw_landing_path = r.raw_landing_path
  where r.change_type is null
     or r.change_type not in ('schema_rebaseline', 'hash_helper_rebaseline')
  window w as (partition by source_recall_id order by extraction_timestamp)
),
content_edits as (
  select * from ordered
  where prev_content_hash is not null and content_hash is distinct from prev_content_hash
)
select
  case
    when prev_status is null and status is null     then 'both_null'
    when prev_status is null                         then 'appeared_from_null'
    when status is null                              then 'disappeared_to_null'
    when status is distinct from prev_status         then 'CHANGED'
    else 'stable'
  end as transition_class,
  count(*) as n
from content_edits
group by 1
order by n desc;

\echo ''
\echo '=== Q4: CHANGED samples across sources (investigation — old → new) ==='
-- Every observed lifecycle transition, for eyeballing direction (e.g. Ongoing→Terminated,
-- Active→Closed). Empty today (re-seed); populates as incrementals bank edits.
with fda as (
  select 'FDA' as source, source_recall_id as key, extraction_timestamp,
    nullif(trim(phase_txt), '')            as val,
    lag(nullif(trim(phase_txt), '')) over w as prev_val,
    lag(content_hash) over w as prev_hash, content_hash
  from fda_recalls_bronze b
  left join extraction_runs r on b.raw_landing_path = r.raw_landing_path
  where r.change_type is null or r.change_type not in ('schema_rebaseline', 'hash_helper_rebaseline')
  window w as (partition by source_recall_id order by extraction_timestamp)
),
usda as (
  select 'USDA' as source, source_recall_id || ':' || langcode as key, extraction_timestamp,
    nullif(trim(recall_type), '')            as val,
    lag(nullif(trim(recall_type), '')) over w as prev_val,
    lag(content_hash) over w as prev_hash, content_hash
  from usda_fsis_recalls_bronze b
  left join extraction_runs r on b.raw_landing_path = r.raw_landing_path
  where r.change_type is null or r.change_type not in ('schema_rebaseline', 'hash_helper_rebaseline')
  window w as (partition by source_recall_id, langcode order by extraction_timestamp)
),
uscg as (
  select 'USCG' as source, source_recall_id as key, extraction_timestamp,
    lower(nullif(trim(disposition), ''))            as val,
    lag(lower(nullif(trim(disposition), ''))) over w as prev_val,
    lag(content_hash) over w as prev_hash, content_hash
  from uscg_recalls_bronze b
  left join extraction_runs r on b.raw_landing_path = r.raw_landing_path
  where r.change_type is null or r.change_type not in ('schema_rebaseline', 'hash_helper_rebaseline')
  window w as (partition by source_recall_id order by extraction_timestamp)
),
all_changes as (
  select source, key, prev_val, val, extraction_timestamp
  from (select * from fda union all select * from usda union all select * from uscg) u
  where prev_hash is not null
    and content_hash is distinct from prev_hash
    and prev_val is not null and val is not null
    and val is distinct from prev_val
)
select source, key, prev_val as prev_value, val as new_value, extraction_timestamp
from all_changes
order by source, extraction_timestamp desc
limit 50;
