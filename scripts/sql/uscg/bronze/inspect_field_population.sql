-- Phase 6a foundation audit — USCG per-field population, enum, sentinel,
-- and narrative-length distributions at corpus scale.
--
-- Sibling to scripts/sql/uscg/bronze/explore_first_extraction.sql, which
-- covers extraction_runs summary (Q1), bronze vs rejected sanity (Q2),
-- rejection breakdown by stage + reason (Q3-Q4), per-field NULL rates for
-- all 18 fields (Q5), disposition distribution (Q6 — drove Finding R), top
-- manufacturers (Q7 — drove Finding S), opened_on year coverage (Q8), and
-- source_recall_id prefix distribution (Q9 — drove Finding G falsification).
--
-- What this script ADDS — the §3, §4, §5 validations from
-- documentation/uscg/field_audit_2026_w22.md that explore_first_extraction.sql
-- doesn't cover. Brings the USCG audit's empirical layer to parity with the
-- other 4 sources' inspect scripts.
--
--   • Q1: severity enum distribution + populated rate — §4 lift design.
--         Finding B sample showed "empty in both samples" (2 probes), but
--         the corpus-wide rate determines whether to lift as a column or
--         document as empty-by-source
--   • Q2: hin sentinel ('N/A') frequency — §4 + §5 (silver normalization)
--   • Q3: boat_type distribution — §5 + §8 lookup-table gap. Surfaces the
--         actual numeric codes present in the corpus, which is the
--         empirical input for emailing USCG OII to request the
--         code → semantic-name mapping
--   • Q4: narrative field length distributions (problem_1, problem_2) —
--         sizing inputs for FastAPI response shaping + landing-page
--         rendering. Mirrors the NHTSA inspect_field_population.sql Q2 pattern
--   • Q5: model_year format consistency — distinct values, multi-year
--         vs single-year breakdown
--   • Q6: top 30 distinct (mic, company_name) pairs — Phase 6b
--         firm-name-cleaning input (USCG variant of NHTSA's
--         inspect_mfgname_vs_mfgtxt.sql but simpler since USCG has only
--         one firm role, not two)
--
-- Companion to scripts/sql/uscg/bronze/explore_first_extraction.sql which
-- covers the broader corpus shape + drove Findings G/O/P/R/S.
--
-- Scope note: bronze currently holds the 2026-05-17 historical seed + any
-- daily incremental additions since USCG website reactivation. Re-run after
-- significant new extractions; the Findings A-S empirical record in
-- scraping_observations.md is the comparison baseline.
--
-- Run with:
--   PGPASSWORD="$NEON_PASSWORD" psql -h "$NEON_HOST" -U "$NEON_USER" -d "$NEON_DBNAME" \
--     -f scripts/sql/uscg/bronze/inspect_field_population.sql

\echo '=== Q1: severity enum distribution + populated rate ==='
-- Per Finding B's 2-probe sample, severity was empty in both. The corpus-
-- wide rate determines §4 lift design: if >50% populated, lift as
-- recall_event.severity; if ~100% empty, document as empty-by-source and
-- skip the lift.
--
-- Cross-source alignment: severity is the closest USCG analog to FDA's
-- centerclassificationtypetxt + USDA's recall_classification / risk_level.
select
  coalesce(severity, '<NULL>') as severity_value,
  count(*) as rows,
  round(100.0 * count(*) / sum(count(*)) over (), 2) as pct
from uscg_recalls_bronze
group by severity
order by rows desc
limit 20;

\echo ''
\echo '=== Q2: hin sentinel and populated-value distribution ==='
-- Per Finding B + schema docstring: hin may be 'N/A' as a documented
-- sentinel ('not applicable'). §4 lift design needs the empirical
-- 'N/A' rate to size the silver normalization impact:
--   case when hin = 'N/A' then null else hin end
--
-- Population breakdown:
--   • populated (real HIN like 'NLPEC117K425')
--   • 'N/A' sentinel
--   • NULL / empty-string
select
  case
    when hin is null or hin = '' then 'NULL or empty'
    when hin = 'N/A' then 'N/A sentinel'
    else 'populated (real HIN)'
  end as hin_class,
  count(*) as rows,
  round(100.0 * count(*) / sum(count(*)) over (), 2) as pct
from uscg_recalls_bronze
group by 1
order by rows desc;

\echo ''
\echo '=== Q3: boat_type distribution (numeric-code enum + lookup-table gap) ==='
-- §5 + §8: boat_type is a numeric code with NO published lookup table per
-- the 2026-05-29 research (NRBSS PDF doesn't cover the recall system's
-- numeric encoding; web search turned up no USCG documentation linking
-- numeric codes to the verbal NRBSS taxonomy of 13 boat-type categories).
--
-- This query surfaces the actual codes present in current bronze. Use the
-- output as the empirical list to send to USCG OII when requesting the
-- code → semantic-name mapping:
--   "Hi USCG OII, the recalls-details.php page exposes a Boat Type
--   numeric code that we capture verbatim. We observe these specific
--   codes in our corpus: [list from this query]. Could you share the
--   code-to-category lookup table or point us to documentation?"
--
-- NRBSS verbal categories (for reference, from
-- documentation/uscg/NRBSS-Exposure-Survey-Final-Report-20201130-v3.0.pdf
-- §2.1 page 8): open power boat, cabin power boat, pontoon boat, air boat,
-- houseboat, PWCs, auxiliary sail boat, sail boat, canoe, kayak,
-- paddleboard, rowed boat, other.
select
  coalesce(boat_type, '<NULL>') as boat_type_code,
  count(*) as rows,
  round(100.0 * count(*) / sum(count(*)) over (), 2) as pct
from uscg_recalls_bronze
group by boat_type
order by rows desc;

\echo ''
\echo '=== Q4: narrative field length distributions (problem_1, problem_2) ==='
-- §4 lift sizing — landing-page rendering + FastAPI response shaping.
-- Mirrors NHTSA's inspect_field_population.sql Q2 pattern. Excludes NULL
-- + empty string from length stats.
--
-- problem_1 = primary defect narrative (~42% empty in Step 1 38-row sample
-- per Finding B); problem_2 = secondary defect narrative ("empty in both
-- samples" per Finding B — corpus rate measured here).
select
  'problem_1' as field,
  count(*) as populated_rows,
  min(length(problem_1)) as min_len,
  round(avg(length(problem_1)))::int as avg_len,
  percentile_disc(0.5) within group (order by length(problem_1))::int as p50_len,
  percentile_disc(0.95) within group (order by length(problem_1))::int as p95_len,
  max(length(problem_1)) as max_len
from uscg_recalls_bronze
where problem_1 is not null and problem_1 <> ''
union all
select
  'problem_2',
  count(*),
  min(length(problem_2)),
  round(avg(length(problem_2)))::int,
  percentile_disc(0.5) within group (order by length(problem_2))::int,
  percentile_disc(0.95) within group (order by length(problem_2))::int,
  max(length(problem_2))
from uscg_recalls_bronze
where problem_2 is not null and problem_2 <> '';

\echo ''
\echo '=== Q5: model_year format breakdown (single-year vs multi-year vs other) ==='
-- Per schema docstring + Finding B: model_year may be "2025" (single year),
-- "2024-2025" (multi-year), or other formats. §4 lift design needs to know
-- how often each format appears to plan silver casting/parsing.
--
-- Categorizes by regex pattern:
--   single_year_4_digit  : ^\d{4}$ (e.g., "2025")
--   year_range_or_list   : contains '-' or ',' or '&' (multi-year)
--   short_year           : ^\d{2}$ (e.g., "25" — should not exist but check)
--   other                : anything else (free text, blank, etc.)
select
  case
    when model_year is null or model_year = '' then 'NULL or empty'
    when model_year ~ '^[0-9]{4}$' then 'single_year_4_digit'
    when model_year ~ '^[0-9]{2}$' then 'short_year_2_digit (unexpected)'
    when model_year ~ '[-,&/]' then 'year_range_or_list'
    when model_year ~ '^[0-9]' then 'other_numeric_prefix'
    else 'other (non-numeric)'
  end as model_year_format,
  count(*) as rows,
  round(100.0 * count(*) / sum(count(*)) over (), 2) as pct,
  min(model_year) as min_sample,
  max(model_year) as max_sample
from uscg_recalls_bronze
group by 1
order by rows desc;

\echo ''
\echo '=== Q6: top 30 distinct (mic, company_name) pairs ==='
-- Phase 6b firm-name-cleaning input + §6 firm-relationship validation.
-- Mirrors NHTSA's inspect_mfgname_vs_mfgtxt.sql Q2 pattern but simpler:
-- USCG has only one firm role per row, not the filer-vs-manufacturer
-- split NHTSA has. The interesting axis is mic ↔ company_name consistency
-- (do recalls with the same mic always have the same company_name? Do
-- recalls with the same company_name always have the same mic?).
--
-- After §3 Bug 3 fix (firm.raw_name = company_name, firm.company_id = mic),
-- the firm dim collapses identical (mic, company_name) pairs to one row.
-- This query surfaces what those distinct pairs look like — useful for
-- empirical validation that the Bug 3 fix produces sensible firm rows.
select
  coalesce(mic, '<NULL>') as mic,
  coalesce(company_name, '<NULL>') as company_name,
  count(*) as rows
from uscg_recalls_bronze
group by mic, company_name
order by rows desc
limit 30;

\echo ''
\echo '=== Q7: per-mic company_name consistency check ==='
-- Diagnostic for the §3 Bug 3 fix. If a single mic value maps to MULTIPLE
-- distinct company_name values across recalls, that's a data-quality issue
-- (the manufacturer changed names mid-MIC, or USCG misencoded). Expected:
-- most mics → exactly 1 company_name; a few may have 2+ if name evolved.
with per_mic as (
  select mic,
         count(distinct company_name) filter (
           where company_name is not null and company_name <> ''
         ) as distinct_companies,
         count(*) as rows
  from uscg_recalls_bronze
  where mic is not null and mic <> ''
  group by mic
)
select
  distinct_companies as distinct_companies_per_mic,
  count(*) as n_mics,
  round(100.0 * count(*) / sum(count(*)) over (), 2) as pct,
  sum(rows) as total_rows_in_bucket
from per_mic
group by distinct_companies
order by distinct_companies;

\echo ''
\echo '=== Q8: per-company_name mic consistency check ==='
-- Inverse of Q7. If a single company_name maps to MULTIPLE distinct mic
-- values, that's expected for large manufacturers with multiple production
-- plants (per the §6 reference: "Application of Manufacturer Identification
-- Code (MIC): Once per boat manufacturer, unless manufacturer has multiple
-- plants or product lines and desires additional MICs"). Useful for
-- Phase 6b firm-rollup quality measurement.
with per_company as (
  select company_name,
         count(distinct mic) filter (where mic is not null and mic <> '') as distinct_mics,
         count(*) as rows
  from uscg_recalls_bronze
  where company_name is not null and company_name <> ''
  group by company_name
)
select
  distinct_mics as distinct_mics_per_company,
  count(*) as n_companies,
  round(100.0 * count(*) / sum(count(*)) over (), 2) as pct,
  sum(rows) as total_rows_in_bucket
from per_company
group by distinct_mics
order by distinct_mics;
