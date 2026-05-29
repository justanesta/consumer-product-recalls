-- Phase 6a foundation audit — NHTSA mfgname vs mfgtxt empirical comparison.
--
-- Background: RCL.txt distinguishes two firm fields that current silver
-- collapses:
--   • mfgname (field 8) = "Manufacturer that filed Part 573 Defect/Noncompliance Report"
--     — the legal *filer* of the recall report.
--   • mfgtxt  (field 15) = "Manufacturers of Recalled Vehicles/Equipment/Child Restraint/Tires"
--     — the actual *manufacturer of the recalled items*.
--
-- Current silver firm.sql + recall_event_firm.sql use mfgname only as
-- role='manufacturer'. The §3 Bug 2 fix in
-- documentation/nhtsa/field_audit_2026_w22.md proposes Option A: split into
-- two roles ('filer' for mfgname, 'manufacturer' for mfgtxt). This script
-- empirically sizes the impact of that split by quantifying:
--
--   (1) How often do mfgname and mfgtxt match exactly / after normalization?
--   (2) When they differ, what are the dominant patterns
--       (parent/subsidiary, regional qualifiers, casing)?
--   (3) Within a single recall campaign, does mfgtxt vary across rows
--       (suggesting per-product manufacturer distinct from per-campaign filer)?
--   (4) How does Option A affect cross-source firm-rollup volume?
--
-- Run with:
--   PGPASSWORD="$NEON_PASSWORD" psql -h "$NEON_HOST" -U "$NEON_USER" -d "$NEON_DBNAME" \
--     -f scripts/sql/nhtsa/bronze/inspect_mfgname_vs_mfgtxt.sql
--
-- Scope note: current bronze holds the user's --since=2023-12-01 incremental
-- slice (~2.5 years). Re-run after Phase 6a.5 historical seed for the full
-- 1966-present corpus — older corporate forms, acquired-and-renamed entities,
-- and defunct manufacturers will surface additional mismatch patterns.

\echo '=== Q1: headline match rate (exact + normalized) ==='
-- How often does mfgname equal mfgtxt, before and after upper(trim()) normalization?
-- Pre-normalization comparison shows raw-byte equality; post-normalization
-- shows what firm.sql's UPPER+TRIM key would see if both fields fed the dim.
select
  count(*) as total_rows,
  sum(case when mfgname = mfgtxt then 1 else 0 end) as exact_match,
  sum(case when upper(trim(mfgname)) = upper(trim(mfgtxt)) then 1 else 0 end) as normalized_match,
  round(100.0 * sum(case when mfgname = mfgtxt then 1 else 0 end) / nullif(count(*), 0), 2) as pct_exact_match,
  round(100.0 * sum(case when upper(trim(mfgname)) = upper(trim(mfgtxt)) then 1 else 0 end) / nullif(count(*), 0), 2) as pct_normalized_match
from nhtsa_recalls_bronze
where mfgname is not null
  and mfgtxt is not null;

\echo ''
\echo '=== Q2: top 30 distinct (mfgname, mfgtxt) pairs where they differ ==='
-- Most-common mismatches reveal the dominant patterns:
--   • Parent/subsidiary corporate hierarchy
--   • Regional qualifiers ("X" vs "X of North America" vs "X USA")
--   • Casing / spacing variants
--   • Truncated forms (CHAR(40) limit may chop one but not the other)
--   • Genuinely distinct entities (filer ≠ manufacturer)
select mfgname, mfgtxt, count(*) as rows
from nhtsa_recalls_bronze
where mfgname is not null and mfgtxt is not null
  and mfgname != mfgtxt
group by mfgname, mfgtxt
order by rows desc
limit 30;

\echo ''
\echo '=== Q3: substring-relationship analysis on mismatches ==='
-- When they differ, is one a substring of the other? This is the dominant
-- parent/subsidiary or corporate-form pattern (e.g., "Ford Motor Company"
-- in one, "Ford" in the other; "Stellantis" containing "FCA US LLC").
-- Completely disjoint values likely represent genuine filer-vs-manufacturer
-- distinctions (the strongest argument for Option A's role split).
with mismatches as (
  select mfgname, mfgtxt
  from nhtsa_recalls_bronze
  where mfgname is not null and mfgtxt is not null
    and mfgname != mfgtxt
)
select
  count(*) as differing_rows,
  sum(case when mfgname like '%' || mfgtxt || '%' then 1 else 0 end) as mfgname_contains_mfgtxt,
  sum(case when mfgtxt like '%' || mfgname || '%' then 1 else 0 end) as mfgtxt_contains_mfgname,
  sum(case when mfgname like '%' || mfgtxt || '%' or mfgtxt like '%' || mfgname || '%' then 1 else 0 end) as any_substring_match,
  sum(case when mfgname not like '%' || mfgtxt || '%' and mfgtxt not like '%' || mfgname || '%' then 1 else 0 end) as completely_disjoint,
  round(100.0 * sum(case when mfgname not like '%' || mfgtxt || '%' and mfgtxt not like '%' || mfgname || '%' then 1 else 0 end) / nullif(count(*), 0), 1) as pct_completely_disjoint
from mismatches;

\echo ''
\echo '=== Q4: per-campno mfgtxt variation ==='
-- Within a single recall campaign (one campno), how often does mfgtxt vary
-- across rows? A non-zero count here means the same recall has multiple
-- manufacturer-of-recalled-items values — i.e., per-product manufacturer
-- distinct from per-campaign filer. This is the strongest case for Option A:
-- if mfgtxt varies per row, collapsing it into a single per-campaign firm row
-- loses information.
with per_campno as (
  select campno,
         count(distinct mfgtxt) as distinct_mfgtxt,
         count(distinct mfgname) as distinct_mfgname,
         count(*) as rows
  from nhtsa_recalls_bronze
  where mfgname is not null and mfgtxt is not null
  group by campno
)
select
  count(*) as total_campaigns,
  sum(case when distinct_mfgtxt > 1 then 1 else 0 end) as campaigns_with_mfgtxt_variation,
  sum(case when distinct_mfgname > 1 then 1 else 0 end) as campaigns_with_mfgname_variation,
  round(100.0 * sum(case when distinct_mfgtxt > 1 then 1 else 0 end) / nullif(count(*), 0), 2) as pct_campaigns_with_mfgtxt_variation,
  round(100.0 * sum(case when distinct_mfgname > 1 then 1 else 0 end) / nullif(count(*), 0), 2) as pct_campaigns_with_mfgname_variation,
  max(distinct_mfgtxt) as max_distinct_mfgtxt_in_one_campaign,
  max(distinct_mfgname) as max_distinct_mfgname_in_one_campaign
from per_campno;

\echo ''
\echo '=== Q5: top campaigns where mfgtxt varies most ==='
-- The specific campaigns with the highest mfgtxt cardinality. Qualitative
-- review: are these multi-manufacturer recalls (e.g., a tire that shipped
-- under multiple brand labels) or data-quality issues?
select campno,
       count(distinct mfgtxt) as distinct_mfgtxt,
       count(distinct mfgname) as distinct_mfgname,
       count(*) as rows,
       string_agg(distinct mfgname, ' | ' order by mfgname) as mfgnames,
       left(string_agg(distinct mfgtxt, ' | ' order by mfgtxt), 200) as mfgtxts_sample
from nhtsa_recalls_bronze
where mfgname is not null and mfgtxt is not null
group by campno
having count(distinct mfgtxt) > 1
order by count(distinct mfgtxt) desc, count(*) desc
limit 10;

\echo ''
\echo '=== Q6: length + cardinality comparison ==='
-- Compares the two fields at scale: are they similar in length distribution?
-- A material difference (e.g., mfgtxt avg=35 vs mfgname avg=18) would suggest
-- mfgtxt carries more elaborate descriptions (parent-co + division naming).
select
  'mfgname' as field,
  count(*) as populated_rows,
  min(length(mfgname)) as min_len,
  round(avg(length(mfgname)))::int as avg_len,
  max(length(mfgname)) as max_len,
  count(distinct mfgname) as distinct_values,
  round(100.0 * count(distinct mfgname) / nullif(count(*), 0), 2) as pct_distinct
from nhtsa_recalls_bronze
where mfgname is not null and trim(mfgname) <> ''
union all
select
  'mfgtxt',
  count(*),
  min(length(mfgtxt)),
  round(avg(length(mfgtxt)))::int,
  max(length(mfgtxt)),
  count(distinct mfgtxt),
  round(100.0 * count(distinct mfgtxt) / nullif(count(*), 0), 2)
from nhtsa_recalls_bronze
where mfgtxt is not null and trim(mfgtxt) <> '';

\echo ''
\echo '=== Q7: cross-source firm-rollup volume impact ==='
-- If Option A lands, NHTSA contributes:
--   • One filer row per distinct upper(trim(mfgname))
--   • One manufacturer row per distinct upper(trim(mfgtxt))
--   • One firm dim row per distinct normalized name (deduped across both)
-- This query estimates how many distinct firm rows NHTSA would contribute
-- pre- and post-split, sizing the cross-source firm dim impact.
with all_normalized as (
  select 'filer' as role, upper(trim(mfgname)) as normalized_name
  from nhtsa_recalls_bronze
  where mfgname is not null and trim(mfgname) <> ''
  union all
  select 'manufacturer', upper(trim(mfgtxt))
  from nhtsa_recalls_bronze
  where mfgtxt is not null and trim(mfgtxt) <> ''
)
select
  count(distinct normalized_name) filter (where role = 'filer') as distinct_filers,
  count(distinct normalized_name) filter (where role = 'manufacturer') as distinct_manufacturers,
  count(distinct normalized_name) as distinct_combined,
  count(distinct normalized_name) filter (where role = 'filer')
    + count(distinct normalized_name) filter (where role = 'manufacturer')
    - count(distinct normalized_name) as overlap_count,
  round(100.0 * (
    count(distinct normalized_name) filter (where role = 'filer')
    + count(distinct normalized_name) filter (where role = 'manufacturer')
    - count(distinct normalized_name)
  ) / nullif(count(distinct normalized_name), 0), 2) as pct_overlap
from all_normalized;

\echo ''
\echo '=== Q8: random sample of mismatches for qualitative review ==='
-- 15 random (campno, mfgname, mfgtxt) triples where the two fields differ.
-- Qualitative pattern surfacing: read these and look for:
--   • Parent/subsidiary ("Stellantis" ↔ "FCA US LLC")
--   • Regional qualifier ("Honda" ↔ "Honda Motor Co., Ltd.")
--   • Tier-1 supplier ("Takata" filed, but the vehicle MFG made the car)
--   • Casing/spacing only
select campno, mfgname, mfgtxt
from nhtsa_recalls_bronze
where mfgname != mfgtxt
  and mfgname is not null and mfgtxt is not null
order by random()
limit 15;

\echo ''
\echo '=== Q9: NULL/empty rates for both fields ==='
-- Confirms both are populated reliably. Per RCL.txt both fields are required
-- (CHAR(40), no nullability noted), but bronze may have edge cases.
select
  count(*) as total_rows,
  sum(case when mfgname is null then 1 else 0 end) as null_mfgname,
  sum(case when mfgname = '' then 1 else 0 end) as empty_mfgname,
  sum(case when mfgtxt is null then 1 else 0 end) as null_mfgtxt,
  sum(case when mfgtxt = '' then 1 else 0 end) as empty_mfgtxt,
  round(100.0 * sum(case when mfgname is null or mfgname = '' then 1 else 0 end) / nullif(count(*), 0), 2) as pct_mfgname_missing,
  round(100.0 * sum(case when mfgtxt is null or mfgtxt = '' then 1 else 0 end) / nullif(count(*), 0), 2) as pct_mfgtxt_missing
from nhtsa_recalls_bronze;
