-- Phase 6a foundation audit — NHTSA per-field population, length, enum,
-- and sentinel distributions at corpus scale.
--
-- Sibling to scripts/sql/nhtsa/bronze/explore_bronze_shape.sql, which covers
-- cadence (Q1-Q3, Q15), 1-to-many shape (Q5-Q7), edit detection (Q4),
-- nullable-date sentinel rates (Q8), drift-added field population BY YEAR
-- (Q9), and several pre-existing distributions (Q10 rcltype, Q11 yeartxt,
-- Q12 makes, Q13 fmvss length, Q14 quarantine, Q16 extraction_runs).
--
-- What this script ADDS beyond explore_bronze_shape.sql — the §3, §4, §5
-- validations from documentation/nhtsa/field_audit_2026_w22.md that the
-- pre-existing toolkit doesn't cover:
--
--   • Q1: per-field NULL/empty rate for ALL 29 fields (lift-design inputs
--         for §4 — explore Q9 only covers drift-added fields by year)
--   • Q2: length distribution for the 4 narrative fields (desc_defect,
--         conequence_defect, corrective_action, notes) — sizing inputs for
--         landing-page rendering / FastAPI response shaping
--   • Q3: influenced_by enum distribution (MFR/OVSC/ODI per RCL.txt) —
--         validates §4 lift design + cross-source alignment with FDA
--         voluntarytypetxt
--   • Q4: do_not_drive / park_outside true/false/null breakdown —
--         validates §4 lift + recall_event.status synth from recall_event.sql
--   • Q5: sentinel frequencies (yeartxt='9999' unknown, odate=1901-01-01
--         unknown per Finding H) — §5 documented sentinels
--   • Q6: potaff distribution buckets — sizing input for §4 lift design
--         (kept as string at bronze; silver casts as needed)
--
-- Companion to scripts/sql/nhtsa/bronze/inspect_mfgname_vs_mfgtxt.sql which
-- specifically validates §3 Bug 2 (filer-vs-manufacturer role split). Both
-- scripts together complete the corpus-scale empirical layer.
--
-- Scope note: bronze currently holds the --since=2023-12-01 incremental
-- slice (~75k rows). Re-run after Phase 6a.5 historical seed for the full
-- 1966-present corpus — pre-2008 records will surface different population
-- rates on drift-added fields (notes, rcl_cmpt_id, mfr_comp_*, etc.) and
-- the sentinel rates will shift toward the older cohort.
--
-- Run with:
--   PGPASSWORD="$NEON_PASSWORD" psql -h "$NEON_HOST" -U "$NEON_USER" -d "$NEON_DBNAME" \
--     -f scripts/sql/nhtsa/bronze/inspect_field_population.sql

\echo '=== Q1: per-field NULL/empty rates (all 29 fields) ==='
-- One row per RCL.txt field, ordered by missing rate descending. NULL and
-- empty string treated as equivalent for text fields (storage-forced
-- empty-string → None happens in staging per ADR 0027, but bronze may carry
-- either form). For date/bool fields only NULL is meaningful (BeforeValidator
-- coerces empty string → None).
--
-- Drift-added fields (NOTES post-2007, RCL_CMPT_ID post-2008, MFR_COMP_*
-- post-2020, DO_NOT_DRIVE / PARK_OUTSIDE post-May-2025) expected to be ~0%
-- missing at the current --since=2023-12-01 scope. Post-Phase-6a.5 seed
-- they jump to non-zero for the older cohort.
with agg as (
  select
    count(*)::numeric as total,
    count(*) filter (where source_recall_id is null or source_recall_id = '') as a_source_recall_id,
    count(*) filter (where campno is null or campno = '') as a_campno,
    count(*) filter (where maketxt is null or maketxt = '') as a_maketxt,
    count(*) filter (where modeltxt is null or modeltxt = '') as a_modeltxt,
    count(*) filter (where yeartxt is null or yeartxt = '') as a_yeartxt,
    count(*) filter (where mfgcampno is null or mfgcampno = '') as a_mfgcampno,
    count(*) filter (where compname is null or compname = '') as a_compname,
    count(*) filter (where mfgname is null or mfgname = '') as a_mfgname,
    count(*) filter (where rcltype is null or rcltype = '') as a_rcltype,
    count(*) filter (where potaff is null or potaff = '') as a_potaff,
    count(*) filter (where mfgtxt is null or mfgtxt = '') as a_mfgtxt,
    count(*) filter (where rcdate is null) as a_rcdate,
    count(*) filter (where desc_defect is null or desc_defect = '') as a_desc_defect,
    count(*) filter (where conequence_defect is null or conequence_defect = '') as a_conequence_defect,
    count(*) filter (where corrective_action is null or corrective_action = '') as a_corrective_action,
    count(*) filter (where bgman is null) as a_bgman,
    count(*) filter (where endman is null) as a_endman,
    count(*) filter (where odate is null) as a_odate,
    count(*) filter (where datea is null) as a_datea,
    count(*) filter (where influenced_by is null or influenced_by = '') as a_influenced_by,
    count(*) filter (where rpno is null or rpno = '') as a_rpno,
    count(*) filter (where fmvss is null or fmvss = '') as a_fmvss,
    count(*) filter (where notes is null or notes = '') as a_notes,
    count(*) filter (where rcl_cmpt_id is null or rcl_cmpt_id = '') as a_rcl_cmpt_id,
    count(*) filter (where mfr_comp_name is null or mfr_comp_name = '') as a_mfr_comp_name,
    count(*) filter (where mfr_comp_desc is null or mfr_comp_desc = '') as a_mfr_comp_desc,
    count(*) filter (where mfr_comp_ptno is null or mfr_comp_ptno = '') as a_mfr_comp_ptno,
    count(*) filter (where do_not_drive is null) as a_do_not_drive,
    count(*) filter (where park_outside is null) as a_park_outside
  from nhtsa_recalls_bronze
)
select field_name, missing_count, round(100.0 * missing_count / nullif(total, 0), 2) as pct_missing
from agg,
lateral (
  values
    ('source_recall_id (field 1)', a_source_recall_id),
    ('campno (field 2)', a_campno),
    ('maketxt (field 3)', a_maketxt),
    ('modeltxt (field 4)', a_modeltxt),
    ('yeartxt (field 5)', a_yeartxt),
    ('mfgcampno (field 6)', a_mfgcampno),
    ('compname (field 7)', a_compname),
    ('mfgname (field 8 — filer)', a_mfgname),
    ('bgman (field 9)', a_bgman),
    ('endman (field 10)', a_endman),
    ('rcltype (field 11)', a_rcltype),
    ('potaff (field 12)', a_potaff),
    ('odate (field 13)', a_odate),
    ('influenced_by (field 14)', a_influenced_by),
    ('mfgtxt (field 15 — manufacturer)', a_mfgtxt),
    ('rcdate (field 16)', a_rcdate),
    ('datea (field 17)', a_datea),
    ('rpno (field 18)', a_rpno),
    ('fmvss (field 19)', a_fmvss),
    ('desc_defect (field 20)', a_desc_defect),
    ('conequence_defect (field 21)', a_conequence_defect),
    ('corrective_action (field 22)', a_corrective_action),
    ('notes (field 23 — drift 2007)', a_notes),
    ('rcl_cmpt_id (field 24 — drift 2008)', a_rcl_cmpt_id),
    ('mfr_comp_name (field 25 — drift 2020)', a_mfr_comp_name),
    ('mfr_comp_desc (field 26 — drift 2020)', a_mfr_comp_desc),
    ('mfr_comp_ptno (field 27 — drift 2020)', a_mfr_comp_ptno),
    ('do_not_drive (field 28 — drift 2025)', a_do_not_drive),
    ('park_outside (field 29 — drift 2025)', a_park_outside)
) as v(field_name, missing_count)
order by missing_count desc, field_name;

\echo ''
\echo '=== Q2: narrative field length distributions ==='
-- desc_defect, conequence_defect, corrective_action, notes — the four CHAR
-- fields with CHAR(2000+) source widths (DESC_DEFECT and CORRECTIVE_ACTION
-- widened to CHAR(6000) in May 2025 per Finding F). Min/avg/max/percentiles
-- for landing-page rendering + FastAPI response shaping. Excludes NULL +
-- empty string.
select
  'desc_defect' as field,
  count(*) as populated_rows,
  min(length(desc_defect)) as min_len,
  round(avg(length(desc_defect)))::int as avg_len,
  percentile_disc(0.5) within group (order by length(desc_defect))::int as p50_len,
  percentile_disc(0.95) within group (order by length(desc_defect))::int as p95_len,
  max(length(desc_defect)) as max_len
from nhtsa_recalls_bronze
where desc_defect is not null and desc_defect <> ''
union all
select
  'conequence_defect',
  count(*),
  min(length(conequence_defect)),
  round(avg(length(conequence_defect)))::int,
  percentile_disc(0.5) within group (order by length(conequence_defect))::int,
  percentile_disc(0.95) within group (order by length(conequence_defect))::int,
  max(length(conequence_defect))
from nhtsa_recalls_bronze
where conequence_defect is not null and conequence_defect <> ''
union all
select
  'corrective_action',
  count(*),
  min(length(corrective_action)),
  round(avg(length(corrective_action)))::int,
  percentile_disc(0.5) within group (order by length(corrective_action))::int,
  percentile_disc(0.95) within group (order by length(corrective_action))::int,
  max(length(corrective_action))
from nhtsa_recalls_bronze
where corrective_action is not null and corrective_action <> ''
union all
select
  'notes',
  count(*),
  min(length(notes)),
  round(avg(length(notes)))::int,
  percentile_disc(0.5) within group (order by length(notes))::int,
  percentile_disc(0.95) within group (order by length(notes))::int,
  max(length(notes))
from nhtsa_recalls_bronze
where notes is not null and notes <> '';

\echo ''
\echo '=== Q3: influenced_by enum distribution ==='
-- §4 lift design: RCL.txt documents MFR / OVSC / ODI as the values
-- (manufacturer-initiated / Office of Vehicle Safety Compliance /
-- Office of Defects Investigation). Cross-source-alignable with FDA's
-- voluntarytypetxt (Voluntary: Firm Initiated vs FDA Requested). If
-- additional values surface here, document them in §5 as a gotcha.
select
  influenced_by,
  count(*) as rows,
  round(100.0 * count(*) / sum(count(*)) over (), 2) as pct
from nhtsa_recalls_bronze
group by influenced_by
order by rows desc;

\echo ''
\echo '=== Q4: do_not_drive / park_outside true/false/null breakdown ==='
-- May-2025-added bool fields (per Finding F). recall_event.status synth
-- in recall_event.sql:137-141 uses these:
--   CASE WHEN do_not_drive is true THEN 'do_not_drive'
--        WHEN park_outside is true THEN 'park_outside'
--        ELSE null
-- Validates the assumption that NULL > true count significantly (only
-- a small fraction of recalls trigger advisory).
select
  'do_not_drive' as field,
  count(*) filter (where do_not_drive is true) as true_count,
  count(*) filter (where do_not_drive is false) as false_count,
  count(*) filter (where do_not_drive is null) as null_count,
  round(100.0 * count(*) filter (where do_not_drive is true) / nullif(count(*), 0), 2) as pct_true
from nhtsa_recalls_bronze
union all
select
  'park_outside',
  count(*) filter (where park_outside is true),
  count(*) filter (where park_outside is false),
  count(*) filter (where park_outside is null),
  round(100.0 * count(*) filter (where park_outside is true) / nullif(count(*), 0), 2)
from nhtsa_recalls_bronze;

\echo ''
\echo '=== Q5: sentinel frequencies (yeartxt=9999, odate=1901-01-01) ==='
-- yeartxt='9999' per RCL.txt is "Unknown Or N/A". odate=1901-01-01 per
-- Finding H is the "unknown date" sentinel. Bronze preserves both;
-- staging maps odate sentinel to NULL per ADR 0027. The corpus-scale
-- frequency informs:
--   • §5 documentation accuracy (how common is "9999")
--   • Whether yeartxt should also get a sentinel→NULL silver mapping
--     (currently it doesn't, per stg_nhtsa_recalls.sql)
select
  'yeartxt = 9999' as sentinel,
  count(*) filter (where yeartxt = '9999') as count_rows,
  round(100.0 * count(*) filter (where yeartxt = '9999') / nullif(count(*), 0), 2) as pct
from nhtsa_recalls_bronze
union all
select
  'odate = 1901-01-01',
  count(*) filter (where date_trunc('day', odate) = '1901-01-01'::timestamptz),
  round(100.0 * count(*) filter (where date_trunc('day', odate) = '1901-01-01'::timestamptz) / nullif(count(*), 0), 2)
from nhtsa_recalls_bronze
union all
select
  'odate IS NULL (vs sentinel)',
  count(*) filter (where odate is null),
  round(100.0 * count(*) filter (where odate is null) / nullif(count(*), 0), 2)
from nhtsa_recalls_bronze;

\echo ''
\echo '=== Q6: potaff distribution buckets ==='
-- POTAFF (field 12) is "Potential Number of Units Affected" — NUMBER(9)
-- in source per RCL.txt, stored as string in bronze. Bucket distribution
-- informs §4 lift design (recall_event.potential_units_affected) and
-- FastAPI numeric-search facet design. Some recalls have very large
-- potaff (Takata-class events 10M+); some have small/zero. Need to cast
-- to int for bucketing — assumes potaff matches NUMBER(9) format. Cast
-- failures land in 'unparseable' bucket for visibility.
with parsed as (
  select
    case
      when potaff is null or potaff = '' then null
      when potaff ~ '^[0-9]+$' then potaff::bigint
      else null
    end as n,
    potaff as raw
  from nhtsa_recalls_bronze
),
bucketed as (
  select
    case
      when raw is null or raw = '' then 'NULL or empty'
      when n is null then 'unparseable: ' || left(raw, 30)
      when n = 0 then '0'
      when n between 1 and 99 then '1-99'
      when n between 100 and 999 then '100-999'
      when n between 1000 and 9999 then '1,000-9,999'
      when n between 10000 and 99999 then '10,000-99,999'
      when n between 100000 and 999999 then '100,000-999,999'
      when n between 1000000 and 9999999 then '1M-9.9M'
      else '10M+'
    end as bucket,
    n
  from parsed
)
select
  bucket,
  count(*) as rows,
  round(100.0 * count(*) / sum(count(*)) over (), 2) as pct,
  min(n) as min_n,
  max(n) as max_n
from bucketed
group by bucket
order by case bucket
  when 'NULL or empty' then 0
  when '0' then 1
  when '1-99' then 2
  when '100-999' then 3
  when '1,000-9,999' then 4
  when '10,000-99,999' then 5
  when '100,000-999,999' then 6
  when '1M-9.9M' then 7
  when '10M+' then 8
  else 9
end;
