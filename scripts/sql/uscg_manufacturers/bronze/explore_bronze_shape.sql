-- Phase 6 (feature/silver-field-remap, W1) — USCG manufacturer-directory bronze shape.
--
-- CAVEAT: population / format / sentinel / fragmentation EVIDENCE for the silver firm
-- remap — the `stg_uscg_manufacturers` staging + the `firm_manufacturer_attributes`
-- per-MIC dim + the recalls→directory firm JOIN. NOT a re-derivation of which fields to
-- capture (Phase 6a, done — see documentation/uscg/field_audit_2026_w22.md +
-- manufacturer_scraping_observations.md). Empty-in-bronze describes capture state only.
--
-- THE HEADLINE — MIC is a TEMPORAL-SCD ANCHOR, not a static key (§M). The 3-char code
-- space is finite, so when a builder goes out of business USCG recycles its MIC to a new
-- builder (AXY: ARMY SURPLUS –1978 → SOSA 2026; COP: CONSER –2008 → COPALO). MIC ⊂ HIN
-- (the code is chars 1-3 of every permanent hull ID), so `MIC → manufacturer` is a
-- time-varying function. Bronze keeps `mic` as the natural key but it is an SCD anchor;
-- a reassignment shows up here as an edit-version (Q1/Q6).
--
-- When to run: against the full-corpus uscg_manufacturers_bronze (Phase 5d Step 7 seed,
-- 16,263 distinct MICs). Sibling to diagnose_short_circuit_miss.sql (which covers the
-- short-circuit gates + isolates the 2 reassignment inserts). This adds the corpus shape
-- the firm remap needs: MIC uniqueness/format/sub-namespaces, the multi-sentinel missing
-- rates, state distribution (incl. Canadian provinces), the address-truncation cliff
-- (listing-only limitation), and the reassignment edit-version pattern.
--
-- SENTINELS (§F.3): the listing uses THREE missing conventions — 'UNK', '-', '' —
-- preserved verbatim at bronze (ADR 0027); silver does the multi-pattern nullif. The
-- missing-rate queries below fold all three in (silver-accurate), so they read higher
-- than a raw IS NULL count. Most queries use latest-per-MIC = the current code holder.
--
-- Output feeds: documentation/uscg/field_audit_2026_w22.md §9 + bronze_corpus_profile.md
-- §1/§2/§5/§6 (USCG manufacturer grain / firm key / temporal-SCD signal).
--
-- Run with: psql ... -f scripts/sql/uscg_manufacturers/bronze/explore_bronze_shape.sql

\echo '=== Q1: cardinality + reassignment edit-versions ==='
-- total_rows − distinct_mics = reassignment edit-versions (append-only bronze keeps both
-- the prior and the new holder per recycled MIC). uscg_directory_id is page-offset
-- positional (= alphabetical rank), hash-excluded — distinct count is informational only.
select
  count(*)                                          as total_rows,
  count(distinct source_recall_id)                  as distinct_mics,
  count(*) - count(distinct source_recall_id)       as edit_versions_from_reassignment,
  count(distinct uscg_directory_id)                 as distinct_directory_ids
from uscg_manufacturers_bronze;

\echo ''
\echo '=== Q2: MIC format / sub-namespace breakdown (§I + §L.1 Finding I) ==='
-- Regulatory format is ^[A-Z0-9]{3}$. Digit-block 101-126 is the reserved engine-maker
-- sub-namespace; alpha-3 dominates; lowercase rows are real source data-quality (silver
-- UPPER-normalizes on the cross-source JOIN; bronze keeps verbatim per ADR 0027).
with latest as (
  select distinct on (source_recall_id) *
  from uscg_manufacturers_bronze
  order by source_recall_id, extraction_timestamp desc
)
select
  case
    when source_recall_id ~ '^[0-9]{3}$'  then 'digit_block (engine makers 101-126)'
    when source_recall_id ~ '^[A-Z]{3}$'  then 'alpha_3'
    when source_recall_id ~ '^[A-Z0-9]{3}$' then 'mixed_alnum'
    when source_recall_id ~ '[a-z]'       then 'lowercase_drift (data quality)'
    else 'other'
  end as mic_class,
  count(*) as n,
  round(100.0 * count(*) / sum(count(*)) over (), 2) as pct
from latest
group by 1
order by n desc;

\echo ''
\echo '=== Q3: multi-sentinel missing rates per listing field (silver nullif sizing) ==='
-- Folds null + the three sentinels ('UNK' / '-' / '') into one missing rate per column —
-- what silver's multi-pattern nullif coercion will see. (§L.2 measured these tiny at
-- silver: company 0.02% / address 0.24% / city 0.45% / state 0.38%.)
with latest as (
  select distinct on (source_recall_id) *
  from uscg_manufacturers_bronze
  order by source_recall_id, extraction_timestamp desc
),
counts as (
  select
    count(*) as total,
    count(*) filter (where company_name is null or company_name in ('UNK', '-', '')) as company_missing,
    count(*) filter (where address      is null or address      in ('UNK', '-', '')) as address_missing,
    count(*) filter (where city         is null or city         in ('UNK', '-', '')) as city_missing,
    count(*) filter (where state        is null or state        in ('UNK', '-', '')) as state_missing
  from latest
)
select field, missing, total, round(100.0 * missing / nullif(total, 0), 2) as pct_missing
from counts,
     lateral (values
       ('company_name', company_missing),
       ('address',      address_missing),
       ('city',         city_missing),
       ('state',        state_missing)
     ) as t(field, missing)
order by pct_missing desc;

\echo ''
\echo '=== Q4: state distribution top 20 (§G — Canadian provinces present despite dropdown gap) ==='
-- US states + US territories + Canadian provinces (BC/ON/QC) coexist; state cannot be a
-- US-only enum. Confirms the §G dropdown-incompleteness finding at corpus scale.
with latest as (
  select distinct on (source_recall_id) *
  from uscg_manufacturers_bronze
  order by source_recall_id, extraction_timestamp desc
)
select
  coalesce(nullif(state, ''), '<empty>') as state,
  count(*) as n,
  round(100.0 * count(*) / sum(count(*)) over (), 2) as pct
from latest
group by 1
order by n desc
limit 20;

\echo ''
\echo '=== Q5: address truncation cliff (§F.1 — ~30-char listing-only limitation) ==='
-- Listing address is source-truncated at ~29-30 chars (a VARCHAR cap); the full address
-- is detail-page-only. at_or_near_cap_29plus quantifies how much is clipped — the cost of
-- listing-only extraction for firm_manufacturer_attributes.address.
with latest as (
  select distinct on (source_recall_id) *
  from uscg_manufacturers_bronze
  order by source_recall_id, extraction_timestamp desc
)
select
  count(*) filter (where address is not null and address not in ('UNK', '-', '')) as real_addresses,
  min(length(address))                                          as min_len,
  round(avg(length(address)))                                   as avg_len,
  percentile_cont(0.5) within group (order by length(address))  as p50_len,
  percentile_cont(0.95) within group (order by length(address)) as p95_len,
  max(length(address))                                          as max_len,
  count(*) filter (where length(address) >= 29)                 as at_or_near_cap_29plus
from latest
where address is not null and address not in ('UNK', '-', '');

\echo ''
\echo '=== Q6: reassignment detail — every MIC with >1 content version (the SCD anchor) ==='
-- The temporal-SCD-anchor evidence (§M): each MIC re-loaded with changed company/address
-- = a reassignment (or a same-firm attribute edit). Read the rows per MIC top-to-bottom:
-- a company_name change between versions is a code recycle. Expect AXY + COP at minimum.
select
  b.source_recall_id            as mic,
  b.company_name,
  b.city,
  b.state,
  left(b.content_hash, 12)      as content_hash_prefix,
  b.extraction_timestamp,
  count(*) over (partition by b.source_recall_id) as versions
from uscg_manufacturers_bronze b
where b.source_recall_id in (
  select source_recall_id
  from uscg_manufacturers_bronze
  group by source_recall_id
  having count(distinct content_hash) > 1
)
order by b.source_recall_id, b.extraction_timestamp;

\echo ''
\echo '=== Q7: recall→directory MIC coverage (cross-source firm JOIN, case-insensitive) ==='
-- The firm JOIN resolves recalls.mic → directory.mic (upper-trim). Bronze-level coverage
-- check (silver measure_rescue_and_coverage.sql reported 99.44% / 714 of 718). Orphans are
-- retired regulatory codes + sentinels (111 / 999 / 777 / N/A).
with recall_mics as (
  select distinct upper(trim(mic)) as mic
  from uscg_recalls_bronze
  where nullif(trim(mic), '') is not null
),
dir_mics as (
  select distinct upper(trim(source_recall_id)) as mic
  from uscg_manufacturers_bronze
)
select
  (select count(*) from recall_mics) as distinct_recall_mics,
  (select count(*) from recall_mics r where exists (select 1 from dir_mics d where d.mic = r.mic)) as resolved_to_directory,
  (select count(*) from recall_mics r where not exists (select 1 from dir_mics d where d.mic = r.mic)) as orphans;
