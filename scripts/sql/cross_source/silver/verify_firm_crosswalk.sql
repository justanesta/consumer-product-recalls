-- Verify the firm_crosswalk after `recalls resolve-firms` (Phase 6b PR 6b.1, Increment A).
--
-- PURPOSE: inspect the CPSC clean-name crosswalk BEFORE wiring it into the silver firm
-- models (Increment B). Confirms: (a) the resolver wrote one row per distinct firm,
-- (b) cleaning actually MERGED raw variants (distinct canonical < distinct firm_id),
-- (c) the blocklist keeps are intact, (d) no clean_name still carries an unstripped
-- ", of <geo>" or " dba " suffix.
--
-- Feeds: project_scope/phase-6b-execution-plan.md PR 6b.1 Increment A gate.
-- Run AFTER `recalls resolve-firms`, with:
--   psql ... -f scripts/sql/cross_source/silver/verify_firm_crosswalk.sql

\set ON_ERROR_STOP on
\pset null '<NULL>'

\echo '=== Q1: crosswalk size + merge magnitude + confidence distribution ==='
-- merges = how many raw firm_ids collapsed away under cleaning (distinct firm_id minus
-- distinct canonical_firm_id). dba_rows = names that yielded a DBA brand.
select
  count(*)                                   as rows_total,
  count(distinct firm_id)                    as distinct_firm_id,
  count(distinct canonical_firm_id)          as distinct_canonical_firm_id,
  count(*) - count(distinct canonical_firm_id) as merges,
  count(*) filter (where extracted_dba is not null) as dba_rows
from firm_crosswalk;

\echo ''
\echo '=== Q1b: match_confidence breakdown ==='
select match_confidence, count(*) as n
from firm_crosswalk
group by match_confidence
order by n desc;

\echo ''
\echo '=== Q2: largest merge groups — clean names that absorbed multiple raw variants ==='
-- A canonical_firm_id with >1 firm_id means cleaning collapsed that many raw spellings
-- into one firm. The clean_name is the surviving canonical.
select canonical_firm_id, count(*) as n_raw_variants, min(canonical_name) as clean_name
from firm_crosswalk
group by canonical_firm_id
having count(*) > 1
order by n_raw_variants desc, clean_name
limit 30;

\echo ''
\echo '=== Q3: sample DBA extractions (clean_name + extracted brand) ==='
select left(clean_name, 55) as clean_name, left(extracted_dba, 35) as extracted_dba
from firm_crosswalk
where extracted_dba is not null
order by clean_name
limit 30;

\echo ''
\echo '=== Q4: SANITY — clean_names that still carry an unstripped suffix (expect 0 rows) ==='
-- A ", of <X>" tail or a standalone " dba " in clean_name means the cleaner missed a
-- comma-anchored geo / DBA clause. Blocklist keeps ("Bank of America") have " of " but
-- NOT ", of ", so they are correctly NOT flagged here.
select left(clean_name, 80) as clean_name, match_confidence
from firm_crosswalk
where clean_name ~* ',\s*of\s'
   or clean_name ~* '\sdba\s'
   or clean_name ~* '\sd/b/a\s'
   or clean_name ~* 'doing business as'
order by clean_name
limit 40;

\echo ''
\echo '=== Q5: reconstructed raw -> clean -> canonical for the top merge groups (proof) ==='
-- Joins the bronze names back to the crosswalk so the actual raw spellings that
-- collapsed are visible (e.g. "Fisher-Price of East Aurora, N.Y." + "Fisher-Price").
with latest as (
  select manufacturers, importers, distributors,
         row_number() over (partition by source_recall_id order by extraction_timestamp desc) as rn
  from cpsc_recalls_bronze
),
names as (
  select e.value ->> 'name' as raw from latest l, jsonb_array_elements(coalesce(l.manufacturers,'[]'::jsonb)) e where l.rn=1
  union all
  select e.value ->> 'name' from latest l, jsonb_array_elements(coalesce(l.importers,'[]'::jsonb)) e where l.rn=1
  union all
  select e.value ->> 'name' from latest l, jsonb_array_elements(coalesce(l.distributors,'[]'::jsonb)) e where l.rn=1
),
reps as (
  select upper(trim(raw)) as firm_norm, min(raw) as rep_raw
  from names where nullif(trim(raw),'') is not null
  group by upper(trim(raw))
),
joined as (
  select x.canonical_firm_id, x.canonical_name as clean_name, r.rep_raw
  from firm_crosswalk x
  join reps r on x.firm_id = md5(r.firm_norm)
),
multi as (
  select canonical_firm_id from joined group by canonical_firm_id having count(*) > 1
)
select j.clean_name, left(j.rep_raw, 70) as raw_variant
from joined j
join multi m on m.canonical_firm_id = j.canonical_firm_id
order by j.clean_name, raw_variant
limit 60;
