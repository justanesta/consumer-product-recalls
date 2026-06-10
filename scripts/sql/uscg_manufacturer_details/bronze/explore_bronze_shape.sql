-- Phase 6 (feature/silver-field-remap, W1) — USCG manufacturer DETAIL-page bronze shape.
--
-- CAVEAT: population / lineage-fill / change-signal EVIDENCE for the silver firm remap
-- and the cross-source SCD verdict (W3). The detail page is the SOURCE-NATIVE SUCCESSION
-- LINEAGE — `past_company_1/2/3`, `parent_company`/`parent_mic`, `in_business` /
-- `out_of_business`, and crucially `date_modified` — that drives the eventual SCD-2 firm
-- dim (Phase 6 / ADR 0035; NOT built on this branch). NOT a re-derivation of which fields
-- to capture (Phase 6a, done). Empty-in-bronze describes capture state only.
--
-- SEED-STATE WARNING: the detail seed is Path B — a ~16.3k-page / ~4.5h walk that may not
-- have run. Q1 is the guard: **if total_detail_rows = 0 the seed has NOT run — stop and
-- report; the rest of this script returns zeros.** Everything is read-only and safe on an
-- empty table.
--
-- When to run: against uscg_manufacturer_details_bronze, after the Path B detail seed.
-- Sibling to preflight_check.sql (the pre-seed readiness check). Latest-per-MIC =
-- current code holder on most queries; sentinels are '-', 'UNK', '-, -', '' (silver
-- normalizes; the fill rates below use nullif('') only — a slight over-count flagged
-- inline, since the multi-sentinel strip is a staging concern).
--
-- Output feeds: documentation/uscg/field_audit_2026_w22.md §9 + bronze_corpus_profile.md
-- §2/§5 (USCG manufacturer-detail grain + the temporal-SCD lineage fill).
--
-- Run with: psql ... -f scripts/sql/uscg_manufacturer_details/bronze/explore_bronze_shape.sql

\echo '=== Q1: SEED-STATE GUARD — cardinality + edit-versions ==='
\echo 'If total_detail_rows = 0 the Path B detail seed has NOT run. Stop and report.'
-- total − distinct = edit-versions (date_modified-driven re-loads; the Path B change signal
-- per §M.5). This is the SCD-NEED measure for the detail table.
select
  count(*)                                          as total_detail_rows,
  count(distinct source_recall_id)                  as distinct_mics,
  count(*) - count(distinct source_recall_id)       as edit_versions
from uscg_manufacturer_details_bronze;

\echo ''
\echo '=== Q2: listing-coverage join (detail seed completeness vs the 16,263 listing MICs) ==='
-- Detail MICs should be a subset of the listing work-list. listing_with_detail / listing
-- = how complete the Path B seed is; detail_not_in_listing flags MICs that left the
-- listing between the two seeds (reassignment churn) or a work-list drift.
with detail_mics as (select distinct source_recall_id from uscg_manufacturer_details_bronze),
     listing_mics as (select distinct source_recall_id from uscg_manufacturers_bronze)
select
  (select count(*) from listing_mics) as listing_mics,
  (select count(*) from detail_mics)  as detail_mics,
  (select count(*) from listing_mics l
     where exists (select 1 from detail_mics d where d.source_recall_id = l.source_recall_id)) as listing_with_detail,
  (select count(*) from detail_mics d
     where not exists (select 1 from listing_mics l where l.source_recall_id = d.source_recall_id)) as detail_not_in_listing;

\echo ''
\echo '=== Q3: succession-lineage fill rates (the SCD-2 inputs) ==='
-- Fill rate for each lineage field on the current holder. past_company_* + out_of_business
-- are the recycle signal; parent_* the corporate-tree signal; dba the alternate-name
-- signal. NOTE: nullif('') only — 'UNK'/'-'/'-, -' sentinels are NOT excluded here, so
-- these slightly over-count; silver's multi-sentinel strip lands the true rate (§M.6
-- detail-probe: ~28% of recalled MICs carry an (OOB)-recycled past company).
with latest as (
  select distinct on (source_recall_id) *
  from uscg_manufacturer_details_bronze
  order by source_recall_id, extraction_timestamp desc
),
counts as (
  select
    count(*) as total,
    count(*) filter (where nullif(trim(past_company_1), '') is not null) as past1,
    count(*) filter (where nullif(trim(past_company_2), '') is not null) as past2,
    count(*) filter (where nullif(trim(past_company_3), '') is not null) as past3,
    count(*) filter (where nullif(trim(parent_company), '') is not null) as parent_company,
    count(*) filter (where nullif(trim(parent_mic), '')     is not null) as parent_mic,
    count(*) filter (where nullif(trim(dba), '')            is not null) as dba,
    count(*) filter (where out_of_business is not null)                  as oob_date,
    count(*) filter (where in_business is not null)                      as in_business_date
  from latest
)
select field, n, total, round(100.0 * n / nullif(total, 0), 2) as pct
from counts,
     lateral (values
       ('past_company_1',        past1),
       ('past_company_2',        past2),
       ('past_company_3',        past3),
       ('parent_company',        parent_company),
       ('parent_mic',            parent_mic),
       ('dba',                   dba),
       ('out_of_business(date)', oob_date),
       ('in_business(date)',     in_business_date)
     ) as t(field, n)
order by n desc;

\echo ''
\echo '=== Q4: (OOB)-year parseability in past-company entries (§M.6 date-reliability) ==='
-- The recycle signal `Past Company N (OOB YYYY)` rarely carries a parseable year (§M.6:
-- only ~13 of 205 recalled-recycled MICs). oob_any vs oob_with_year sizes how much the
-- time-aware recall↔manufacturer join can ever be precise vs flag-only.
with latest as (
  select distinct on (source_recall_id) *
  from uscg_manufacturer_details_bronze
  order by source_recall_id, extraction_timestamp desc
),
past as (
  select unnest(array[past_company_1, past_company_2, past_company_3]) as pc
  from latest
)
select
  count(*) filter (where nullif(trim(pc), '') is not null) as past_company_entries,
  count(*) filter (where pc ~ '\(OOB')                     as oob_any,
  count(*) filter (where pc ~ '\(OOB\s+[0-9]{4}\)')        as oob_with_year
from past;

\echo ''
\echo '=== Q5: date_modified change signal (the Path B incremental oracle) ==='
-- date_modified is INCLUDED in content_hash (the Path B change signal §M.5) — a far better
-- incremental oracle than re-hashing 16k listing rows. Range + recency shows how live the
-- directory edits are; the edit-versions in Q1 are date_modified-driven.
with latest as (
  select distinct on (source_recall_id) *
  from uscg_manufacturer_details_bronze
  order by source_recall_id, extraction_timestamp desc
)
select
  count(*) filter (where date_modified is not null)     as with_date_modified,
  round(100.0 * count(*) filter (where date_modified is not null) / nullif(count(*), 0), 1) as pct_populated,
  min(date_modified)                                    as oldest,
  max(date_modified)                                    as newest,
  count(distinct date_modified::date)                   as distinct_dates
from latest;

\echo ''
\echo '=== Q6: status enum distribution (accepted_values SSOT) ==='
-- Observed: 'In Business' / 'Inactive' / 'Federal or State Agency'. Corpus value set →
-- the firm_uscg_attributes.status accepted_values list. '' shown via nullif.
with latest as (
  select distinct on (source_recall_id) *
  from uscg_manufacturer_details_bronze
  order by source_recall_id, extraction_timestamp desc
)
select
  coalesce(nullif(trim(status), ''), '<empty>') as status,
  count(*) as n,
  round(100.0 * count(*) / sum(count(*)) over (), 2) as pct
from latest
group by 1
order by n desc;

\echo ''
\echo '=== Q7: type fill + run-on shape (verbal vessel-type taxonomy) ==='
-- The detail `type` is the verbal boat-type taxonomy (<br/>-concatenated run-on at
-- bronze) — distinct from the recalls numeric boat_type code. Fill + the multi-value
-- run-on rate; structured split is Phase 6/7 enrichment.
with latest as (
  select distinct on (source_recall_id) *
  from uscg_manufacturer_details_bronze
  order by source_recall_id, extraction_timestamp desc
)
select
  count(*) filter (where nullif(trim(type), '') is not null)                          as type_populated,
  round(100.0 * count(*) filter (where nullif(trim(type), '') is not null) / nullif(count(*), 0), 1) as pct_populated,
  count(*) filter (where type ~* '<br')                                               as contains_br_runon
from latest;
