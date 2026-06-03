-- SCD monitor — recall-classification / severity amendment rate (cross-source).
--
-- PURPOSE: validate the SCD-type designation for the recall *classification* axis
-- (FDA center_classification_type_txt 1/2/3/NC; USDA recall_classification Class I/II/III/PHA;
-- USCG severity H/L/M/S). The exercise stipulated "Type 0 (immutable)"; reality is that
-- classifications get AMENDED as info emerges (e.g. Class II escalated to Class I). This
-- monitor measures how often that happens, per source, so the designation in
-- documentation/audit/scd_field_designations.md is data-driven, not assumed.
--   • 0 amendments observed  → the Type-1 latest-wins silver is lossless for this field.
--   • >0 amendments observed → classification is a Type-2-BENEFIT field (silver is currently
--     DROPPING the escalation history into bronze-only); promote it in the designations table.
--
-- ⚠ MEASURE-FORWARD CAVEAT: the Phase 6a.5 re-seeds wiped bronze's edit-version history
-- (FDA/CPSC/USDA/USCG all show 0 edit-versions in the current single-shot seeds). So this
-- monitor reads ~0 transitions TODAY — that is "no banked history yet," NOT "classifications
-- never change." It accrues signal as daily incrementals + the weekly deep-rescan (ADR 0010)
-- re-bank content-hash versions. Re-run it periodically; it is the report that keeps the
-- classification designation honest.
--
-- METHOD (mirrors usda_recalls/bronze/assert_field_last_modified_date_advances_on_edit.sql):
-- for each business key ordered by extraction_timestamp, LAG to the prior bronze version;
-- on rows where content_hash actually changed (a genuine edit), bucket what the classification
-- did. The rebaseline filter excludes change_type IN ('schema_rebaseline','hash_helper_rebaseline')
-- so a canonical-shape re-stamp doesn't synthesize false amendments (ADR 0027 + the USDA template).
-- USCG severity is upper()-normalized first so a case-only re-stamp (Finding R, H/h, L/l) is not
-- counted as an amendment.
--
-- NHTSA + CPSC have no classification/severity analog (NHTSA rcltype is a product TYPE, not a
-- severity; CPSC has no severity field) — they are out of scope for this monitor. NHTSA identity
-- stability is covered by nhtsa/bronze/assert_eleven_tuple_identity_stable.sql.
--
-- Feeds: documentation/audit/scd_field_designations.md (the `classification` row's status).
-- Run with: psql ... -f scripts/sql/cross_source/scd_monitors/assert_classification_stable.sql

\set ON_ERROR_STOP on
\pset null '<NULL>'

\echo '=== Q1: FDA classification transitions across content edits ==='
-- center_classification_type_txt over each PRODUCTID's bronze versions. AMENDED = the
-- Type-2-BENEFIT signal (a recall whose classification changed between extractions).
with ordered as (
  select
    source_recall_id,
    extraction_timestamp,
    content_hash,
    nullif(trim(center_classification_type_txt), '')                 as classification,
    lag(nullif(trim(center_classification_type_txt), '')) over w      as prev_classification,
    lag(content_hash) over w                                          as prev_content_hash
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
    when prev_classification is null and classification is null     then 'both_null'
    when prev_classification is null                                then 'appeared_from_null'
    when classification is null                                     then 'disappeared_to_null'
    when classification is distinct from prev_classification        then 'AMENDED'
    else 'stable'
  end as transition_class,
  count(*) as n
from content_edits
group by 1
order by n desc;

\echo ''
\echo '=== Q2: USDA classification transitions across content edits ==='
-- recall_classification over each (recall_number, langcode)'s bronze versions.
with ordered as (
  select
    source_recall_id,
    langcode,
    extraction_timestamp,
    content_hash,
    nullif(trim(recall_classification), '')                 as classification,
    lag(nullif(trim(recall_classification), '')) over w      as prev_classification,
    lag(content_hash) over w                                 as prev_content_hash
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
    when prev_classification is null and classification is null     then 'both_null'
    when prev_classification is null                                then 'appeared_from_null'
    when classification is null                                     then 'disappeared_to_null'
    when classification is distinct from prev_classification        then 'AMENDED'
    else 'stable'
  end as transition_class,
  count(*) as n
from content_edits
group by 1
order by n desc;

\echo ''
\echo '=== Q3: USCG severity transitions across content edits (case-normalized) ==='
-- severity over each recall's bronze versions; upper() so a case-only re-stamp (Finding R)
-- is not a false amendment.
with ordered as (
  select
    source_recall_id,
    extraction_timestamp,
    content_hash,
    upper(nullif(trim(severity), ''))                 as severity,
    lag(upper(nullif(trim(severity), ''))) over w      as prev_severity,
    lag(content_hash) over w                           as prev_content_hash
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
    when prev_severity is null and severity is null     then 'both_null'
    when prev_severity is null                          then 'appeared_from_null'
    when severity is null                               then 'disappeared_to_null'
    when severity is distinct from prev_severity        then 'AMENDED'
    else 'stable'
  end as transition_class,
  count(*) as n
from content_edits
group by 1
order by n desc;

\echo ''
\echo '=== Q4: AMENDED samples across sources (investigation — old → new) ==='
-- Every observed classification/severity amendment, for eyeballing direction (escalation vs
-- de-escalation) and plausibility. Empty today (re-seed wiped versions); populates as
-- incrementals bank edits. Read prev_value → new_value with the two extraction timestamps.
with fda as (
  select 'FDA' as source, source_recall_id as key, extraction_timestamp,
    nullif(trim(center_classification_type_txt), '')            as val,
    lag(nullif(trim(center_classification_type_txt), '')) over w as prev_val,
    lag(content_hash) over w as prev_hash, content_hash
  from fda_recalls_bronze b
  left join extraction_runs r on b.raw_landing_path = r.raw_landing_path
  where r.change_type is null or r.change_type not in ('schema_rebaseline', 'hash_helper_rebaseline')
  window w as (partition by source_recall_id order by extraction_timestamp)
),
usda as (
  select 'USDA' as source, source_recall_id || ':' || langcode as key, extraction_timestamp,
    nullif(trim(recall_classification), '')            as val,
    lag(nullif(trim(recall_classification), '')) over w as prev_val,
    lag(content_hash) over w as prev_hash, content_hash
  from usda_fsis_recalls_bronze b
  left join extraction_runs r on b.raw_landing_path = r.raw_landing_path
  where r.change_type is null or r.change_type not in ('schema_rebaseline', 'hash_helper_rebaseline')
  window w as (partition by source_recall_id, langcode order by extraction_timestamp)
),
uscg as (
  select 'USCG' as source, source_recall_id as key, extraction_timestamp,
    upper(nullif(trim(severity), ''))            as val,
    lag(upper(nullif(trim(severity), ''))) over w as prev_val,
    lag(content_hash) over w as prev_hash, content_hash
  from uscg_recalls_bronze b
  left join extraction_runs r on b.raw_landing_path = r.raw_landing_path
  where r.change_type is null or r.change_type not in ('schema_rebaseline', 'hash_helper_rebaseline')
  window w as (partition by source_recall_id order by extraction_timestamp)
),
all_amendments as (
  select source, key, prev_val, val, extraction_timestamp
  from (select * from fda union all select * from usda union all select * from uscg) u
  where prev_hash is not null
    and content_hash is distinct from prev_hash
    and prev_val is not null and val is not null
    and val is distinct from prev_val
)
select source, key, prev_val as prev_value, val as new_value, extraction_timestamp
from all_amendments
order by source, extraction_timestamp desc
limit 50;
