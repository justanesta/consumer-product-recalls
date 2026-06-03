-- SCD monitor — USCG MIC reassignment (the firm-anchor holder changing over time).
--
-- PURPOSE: validate the **Type-2-NEED** designation for the USCG firm anchor
-- (documentation/audit/scd_field_designations.md). This is NOT an "attribute amended on a stable
-- entity" monitor (that is assert_classification_stable / assert_lifecycle_stable). It is an
-- "the IDENTITY ANCHOR points to a DIFFERENT entity over time" monitor — unique to USCG because
-- the MIC is a finite 3-char code that USCG RECYCLES to a new builder when the prior one goes out
-- of business (MIC ⊂ HIN; manufacturer_scraping_observations.md §M). The same `mic` join key
-- denotes ARMY SURPLUS on a 1975 hull and SOSA on a 2026 hull, so a Type-1 "current holder" silver
-- MISATTRIBUTES a pre-reassignment recall to the current company — a correctness bug (NEED), not a
-- missing feature. No other source recycles its firm identifier (FDA FEI, USDA establishment_number
-- stable; NHTSA none; CPSC CompanyID empty), so this monitor is USCG-only.
--
-- WHY THIS ONE READS REAL DATA TODAY (unlike the amendment monitors): the reassignment signal has
-- TWO sources, and only one was wiped by the re-seed —
--   • DYNAMIC (Q1): mic→company changes across listing-table edit-versions. Wiped by the re-seed
--     (0 edit-versions now); measure-forward, like the amendment monitors.
--   • STATIC  (Q2–Q4): the detail page's source-native `past_company_*` / `out_of_business` lineage
--     (a Type-3 history the source hands us) — present in the CURRENT snapshot, so it quantifies the
--     reassignment surface NOW even with 0 edit-versions. This is the load-bearing half.
--
-- Feeds: documentation/audit/scd_field_designations.md (the USCG firm-anchor row).
-- Run with: psql ... -f scripts/sql/cross_source/scd_monitors/assert_mic_holder_stable.sql

\set ON_ERROR_STOP on
\pset null '<NULL>'

\echo '=== Q1: DYNAMIC — mic -> company_name changes across listing edit-versions (measure-forward) ==='
-- REASSIGNED = the same MIC re-loaded with a different company_name (an AXY/COP-style recycle
-- captured as an edit-version). ~0 today (re-seed wiped versions); accrues as incrementals bank edits.
-- company_name upper()-normalized + the rebaseline filter so casing / re-stamps aren't false hits.
with ordered as (
  select
    source_recall_id                                   as mic,
    extraction_timestamp,
    content_hash,
    upper(trim(company_name))                          as company,
    lag(upper(trim(company_name))) over w               as prev_company,
    lag(content_hash) over w                            as prev_content_hash
  from uscg_manufacturers_bronze b
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
    when prev_company is null and company is null     then 'both_null'
    when prev_company is null                          then 'appeared_from_null'
    when company is null                               then 'disappeared_to_null'
    when company is distinct from prev_company         then 'REASSIGNED'
    else 'stable'
  end as transition_class,
  count(*) as n
from content_edits
group by 1
order by n desc;

\echo ''
\echo '=== Q2: STATIC — reassignment lineage in the current detail snapshot (survives the re-seed) ==='
-- The source-native Type-3 lineage. has_prior_holder = ≥1 Past Company on the detail page (the
-- recycle signal); oob_marked_prior = Past Company carries an (OOB) marker (high-confidence recycle);
-- current_holder_defunct = top-level Out of Business (the CURRENT holder ceased — an SCD valid_to,
-- distinct from reassignment). This is the corpus-wide §M.6 measurement, read statically.
-- NOTE: the source's past_company_1/2/3 slots are NOT filled sequentially (W1 found
-- past_company_3 fill > past_company_2), so a MIC's recycle lineage can sit in slot 2 or 3
-- with slot 1 empty. ALL THREE slots must be checked — checking only past_company_1
-- undercounts the recycle surface by ~40% (verified against §M.6's all-slot probe).
with latest as (
  select distinct on (source_recall_id) *
  from uscg_manufacturer_details_bronze
  order by source_recall_id, extraction_timestamp desc
)
select
  count(*)                                                                        as total_mics,
  count(*) filter (where coalesce(nullif(trim(past_company_1), ''),
                                  nullif(trim(past_company_2), ''),
                                  nullif(trim(past_company_3), '')) is not null)  as has_prior_holder,
  round(100.0 * count(*) filter (where coalesce(nullif(trim(past_company_1), ''),
                                                 nullif(trim(past_company_2), ''),
                                                 nullif(trim(past_company_3), '')) is not null)
    / nullif(count(*), 0), 1)                                                     as pct_with_prior_holder,
  count(*) filter (where past_company_1 ~ '\(OOB'
                      or past_company_2 ~ '\(OOB'
                      or past_company_3 ~ '\(OOB')                                as oob_marked_prior,
  count(*) filter (where out_of_business is not null)                             as current_holder_defunct
from latest;

\echo ''
\echo '=== Q3: the MISATTRIBUTION SURFACE — recalled MICs that carry a prior holder ==='
-- Of the MICs actually referenced by recalls, how many have a recycle lineage → the set where a
-- Type-1 "current holder" silver risks attributing the recall to the wrong (current) firm. §M.6
-- measured 28.7% on the recall-directed probe; this is the corpus-wide standing number.
with recall_mics as (
  select distinct upper(trim(mic)) as mic
  from uscg_recalls_bronze
  where nullif(trim(mic), '') is not null
),
det as (
  select distinct on (source_recall_id)
    upper(trim(source_recall_id)) as mic, past_company_1, past_company_2, past_company_3, out_of_business
  from uscg_manufacturer_details_bronze
  order by source_recall_id, extraction_timestamp desc
)
select
  count(*)                                                                       as recalled_mics,
  count(*) filter (where coalesce(nullif(trim(d.past_company_1), ''),
                                  nullif(trim(d.past_company_2), ''),
                                  nullif(trim(d.past_company_3), '')) is not null) as recalled_with_prior_holder,
  count(*) filter (where d.past_company_1 ~ '\(OOB'
                      or d.past_company_2 ~ '\(OOB'
                      or d.past_company_3 ~ '\(OOB')                             as recalled_oob_recycled,
  round(100.0 * count(*) filter (where d.past_company_1 ~ '\(OOB'
                                    or d.past_company_2 ~ '\(OOB'
                                    or d.past_company_3 ~ '\(OOB')
    / nullif(count(*), 0), 1)                                                    as pct_recalled_recycled
from recall_mics rm
left join det d on d.mic = rm.mic;

\echo ''
\echo '=== Q4: samples — recalled+recycled MICs (current holder vs prior holders) for investigation ==='
-- Eyeball the misattribution risk: current company_name vs the (OOB)-marked prior holder a
-- pre-reassignment recalled hull actually belongs to.
with recall_mics as (
  select distinct upper(trim(mic)) as mic
  from uscg_recalls_bronze
  where nullif(trim(mic), '') is not null
),
det as (
  select distinct on (source_recall_id)
    source_recall_id as mic, company_name, past_company_1, past_company_2, past_company_3, out_of_business
  from uscg_manufacturer_details_bronze
  order by source_recall_id, extraction_timestamp desc
)
select d.mic, d.company_name as current_holder, d.past_company_1, d.past_company_2, d.past_company_3
from det d
join recall_mics rm on upper(trim(d.mic)) = rm.mic
where d.past_company_1 ~ '\(OOB'
   or d.past_company_2 ~ '\(OOB'
   or d.past_company_3 ~ '\(OOB'
order by d.mic
limit 30;
