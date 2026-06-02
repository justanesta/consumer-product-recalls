-- Phase 6 (feature/silver-field-remap, W1) — FDA free-text normalization scoping.
--
-- CAVEAT: shape evidence to SIZE the normalization design for
-- `product_distributed_quantity` (→ recall_product.number_of_units) and
-- `distribution_area_summary_txt` (→ recall_event.distribution_area_summary +
-- derived distribution_scope). NOT a field-selection re-derivation (Phase 6a, done).
--
-- The CASE buckets below are SCOPING HEURISTICS to measure pattern coverage, not the
-- final production normalize logic. They answer: (a) what % of distribution rows fall
-- under Nationwide/Worldwide so a deterministic distribution_scope derive is worth it,
-- (b) how big the negation false-positive risk is, (c) what % of quantity rows match the
-- clean numeric patterns a Tier-2 value/unit parse would target vs the messy tail.
--
-- When to run: against full-corpus fda_recalls_bronze (134,461 rows).
-- Output feeds: the normalization-tier decision in cross_source_consolidation.md (W2)
-- + the staging/silver edits in W4. Cite by Qn.
--
-- Run with: psql ... -f scripts/sql/fda/bronze/profile_freetext_normalization.sql

\echo '=== Q1: product_distributed_quantity — pattern coverage buckets ==='
-- Decides whether a Tier-2 deterministic quantity parse is worth it now (pure_integer +
-- int_plus_unit are the extractable share) vs deferring to enrichment (other_messy tail).
select
  case
    when nullif(btrim(product_distributed_quantity), '') is null then '0_empty'
    when product_distributed_quantity ~* '^\s*(unknown|n/?a|none|tbd|undetermined|not available|not known|see attached)\s*$' then '1_sentinel'
    when product_distributed_quantity ~ '^\s*[0-9][0-9,]*\s*$' then '2_pure_integer'
    when product_distributed_quantity ~* '^\s*[0-9][0-9,]*\s*(unit|each|case|bottle|box|lot|piece|pound|lb|kg|gram|ml|liter|litre|count|carton|pack|vial|bag|can|jar|tube|kit|dozen|gallon|oz|ounce)' then '3_int_plus_unit'
    when product_distributed_quantity ~* '^\s*(one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve|approximately|approx|about|several|many|multiple)' then '4_word_or_approx'
    else '5_other_messy'
  end as bucket,
  count(*) as rows,
  round(100.0 * count(*) / sum(count(*)) over (), 1) as pct
from fda_recalls_bronze
group by 1
order by 1;

\echo ''
\echo '=== Q2: product_distributed_quantity — cardinality + sentinel spread ==='
-- distinct_normalized vs non_empty shows how compressible the field is; the case-insensitive
-- normalize (lower+trim) is the floor for any standardization.
select
  count(*) as total_rows,
  count(nullif(btrim(product_distributed_quantity), '')) as non_empty,
  count(distinct lower(btrim(product_distributed_quantity))) as distinct_normalized
from fda_recalls_bronze;

\echo ''
\echo '=== Q3: product_distributed_quantity — top messy-tail samples (what a parser must face) ==='
-- The rows no clean pattern catches. If this tail is small + low-value, Tier-2 parse stays
-- deferred; if it is large + structured, it argues for an enrichment parser sooner.
select product_distributed_quantity, count(*) as rows
from fda_recalls_bronze
where nullif(btrim(product_distributed_quantity), '') is not null
  and product_distributed_quantity !~* '^\s*(unknown|n/?a|none|tbd|undetermined|not available|not known|see attached)\s*$'
  and product_distributed_quantity !~ '^\s*[0-9][0-9,]*\s*$'
  and product_distributed_quantity !~* '^\s*[0-9][0-9,]*\s*(unit|each|case|bottle|box|lot|piece|pound|lb|kg|gram|ml|liter|litre|count|carton|pack|vial|bag|can|jar|tube|kit|dozen|gallon|oz|ounce)'
  and product_distributed_quantity !~* '^\s*(one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve|approximately|approx|about|several|many|multiple)'
group by 1
order by 2 desc
limit 20;

\echo ''
\echo '=== Q4: distribution_area_summary_txt — scope coverage buckets ==='
-- Sizes the distribution_scope derive. negated_scope is checked FIRST so it does not get
-- mislabeled Nationwide — its size is the false-positive risk of a naive ilike rule.
select
  case
    when nullif(btrim(distribution_area_summary_txt), '') is null then '0_empty'
    when distribution_area_summary_txt ~* '(not|except|excluding|other than|no )[^.]{0,40}(nationwide|worldwide)' then '1_negated_scope'
    when distribution_area_summary_txt ~* 'worldwide|international|all 50 states|all fifty states' then '2_worldwide_or_all'
    when distribution_area_summary_txt ~* 'nationwide' then '3_nationwide'
    when length(btrim(distribution_area_summary_txt)) <= 30 and distribution_area_summary_txt !~ ',' then '4_single_short_region'
    when length(btrim(distribution_area_summary_txt)) <= 80 and distribution_area_summary_txt ~ ',' then '5_short_state_list'
    when length(btrim(distribution_area_summary_txt)) >= 120 then '6_narrative_long'
    else '7_other_mid'
  end as bucket,
  count(*) as rows,
  round(100.0 * count(*) / sum(count(*)) over (), 1) as pct
from fda_recalls_bronze
group by 1
order by 1;

\echo ''
\echo '=== Q5: distribution_area_summary_txt — negation false-positive samples ==='
-- The concrete rows where "nationwide" appears alongside a negation. If this is ~0, a
-- simple ilike derive is safe; if non-trivial, the negation guard in the CASE is load-bearing.
select left(btrim(distribution_area_summary_txt), 100) as sample, count(*) as rows
from fda_recalls_bronze
where distribution_area_summary_txt ~* 'nationwide'
  and distribution_area_summary_txt ~* '(not|except|excluding|other than)'
group by 1
order by 2 desc
limit 15;

\echo ''
\echo '=== Q6: distribution_area_summary_txt — Nationwide surface-form compression ==='
-- How many distinct raw strings collapse into a single distribution_scope=Nationwide flag —
-- the direct payoff of the Tier-1 derive (top-15 alone showed 8 variants).
select
  count(distinct btrim(distribution_area_summary_txt)) as distinct_nationwide_surface_forms,
  count(*) as nationwide_rows
from fda_recalls_bronze
where distribution_area_summary_txt ~* 'nationwide'
  and distribution_area_summary_txt !~* '(not|except|excluding|other than)';
