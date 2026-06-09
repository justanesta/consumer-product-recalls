-- Gate U1 (6b.2) — USDA recall -> establishment FAN-OUT sizing: the disambiguation denominator.
--
-- PURPOSE: 6b.2 only matters for recalls whose free-text `establishment` name matches MORE THAN
-- ONE FSIS establishment. That recall-side fan-out rate has never been measured directly (the
-- audit measured the establishment-SIDE 14% shared-name rate). This sizes the problem: of the
-- USDA recalls, how many have no name / match 0 / match 1 / match 2+ establishments, and what the
-- candidate-count distribution looks like. Also reports the recall-side signal-field coverage
-- (states / processing / product_items non-null) that the disambiguation hierarchy depends on.
--
-- Join mirrors the resolution model: stg_usda_fsis_recalls.establishment (HTML-decoded, silver)
-- ~ firm_usda_attributes.establishment_name on upper(trim()). firm_usda_attributes
-- is keyed one row per establishment_number (composites like 'M46712+P46712' are ONE row), so a
-- candidate count > 1 means genuinely distinct establishments sharing a name.
--
-- Feeds: project_scope/phase-6-execution-plan.md PR 6b.2 (sizes the effort + the <10% ambiguous_null gate).
-- Run with: psql ... -f scripts/sql/usda_recalls/silver/probe_recall_establishment_fanout.sql

\set ON_ERROR_STOP on
\pset null '<NULL>'

\echo '=== Q1: recall-side signal-field coverage (the hierarchy inputs) ==='
select
  count(*)                                                  as total_recalls,
  count(*) filter (where establishment is not null)        as has_establishment,
  count(*) filter (where states is not null)               as has_states,
  count(*) filter (where processing is not null)           as has_processing,
  count(*) filter (where product_items is not null)        as has_product_items,
  round(100.0 * count(*) filter (where establishment is not null) / nullif(count(*),0), 1) as pct_has_establishment
from stg_usda_fsis_recalls;

\echo ''
\echo '=== Q2: fan-out distribution — how many recalls match 0 / 1 / 2+ establishments ==='
-- fan_out_multi is the 6b.2 surface. pct_multi_of_matched = the share of joinable recalls that
-- actually need disambiguation.
with matched as (
  select
    r.source_recall_id,
    r.establishment,
    count(e.establishment_id) as n_candidates
  from stg_usda_fsis_recalls r
  left join firm_usda_attributes e
    on upper(trim(e.establishment_name)) = upper(trim(r.establishment))
  where r.establishment is not null
  group by r.source_recall_id, r.establishment
)
select
  count(*)                                            as recalls_with_name,
  count(*) filter (where n_candidates = 0)            as name_no_establishment_match,
  count(*) filter (where n_candidates = 1)            as unambiguous_single,
  count(*) filter (where n_candidates >= 2)           as fan_out_multi,
  max(n_candidates)                                   as max_candidates,
  round(100.0 * count(*) filter (where n_candidates >= 2)
        / nullif(count(*) filter (where n_candidates >= 1), 0), 1) as pct_multi_of_matched
from matched;

\echo ''
\echo '=== Q3: candidate-count histogram for the fan-out (multi) recalls ==='
with matched as (
  select r.source_recall_id, count(e.establishment_id) as n_candidates
  from stg_usda_fsis_recalls r
  left join firm_usda_attributes e
    on upper(trim(e.establishment_name)) = upper(trim(r.establishment))
  where r.establishment is not null
  group by r.source_recall_id
)
select n_candidates, count(*) as n_recalls
from matched
where n_candidates >= 2
group by n_candidates
order by n_candidates;

\echo ''
\echo '=== Q4: states token coverage — structured vs non-state ("Nationwide"/"Midwest") pollution ==='
-- Signal 2 viability. Explodes the comma-separated states; a token is "non-state" if it is not a
-- US state/territory full name. High non-state share weakens Signal 2.
with toks as (
  select source_recall_id, trim(tok) as state_tok
  from stg_usda_fsis_recalls,
       lateral unnest(string_to_array(states, ',')) as tok
  where states is not null and trim(tok) <> ''
)
select
  case when upper(state_tok) in (
    'ALABAMA','ALASKA','ARIZONA','ARKANSAS','CALIFORNIA','COLORADO','CONNECTICUT','DELAWARE',
    'FLORIDA','GEORGIA','HAWAII','IDAHO','ILLINOIS','INDIANA','IOWA','KANSAS','KENTUCKY','LOUISIANA',
    'MAINE','MARYLAND','MASSACHUSETTS','MICHIGAN','MINNESOTA','MISSISSIPPI','MISSOURI','MONTANA',
    'NEBRASKA','NEVADA','NEW HAMPSHIRE','NEW JERSEY','NEW MEXICO','NEW YORK','NORTH CAROLINA',
    'NORTH DAKOTA','OHIO','OKLAHOMA','OREGON','PENNSYLVANIA','RHODE ISLAND','SOUTH CAROLINA',
    'SOUTH DAKOTA','TENNESSEE','TEXAS','UTAH','VERMONT','VIRGINIA','WASHINGTON','WEST VIRGINIA',
    'WISCONSIN','WYOMING','DISTRICT OF COLUMBIA','PUERTO RICO'
  ) then 'us_state' else 'NON_STATE' end as token_class,
  count(*) as n_token_occurrences,
  count(distinct state_tok) as distinct_tokens
from toks
group by 1
order by n_token_occurrences desc;

\echo ''
\echo '=== Q4b: the actual NON-STATE tokens (build the Signal-2 blocklist from these) ==='
with toks as (
  select trim(tok) as state_tok
  from stg_usda_fsis_recalls, lateral unnest(string_to_array(states, ',')) as tok
  where states is not null and trim(tok) <> ''
)
select state_tok, count(*) as n
from toks
where upper(state_tok) not in (
  'ALABAMA','ALASKA','ARIZONA','ARKANSAS','CALIFORNIA','COLORADO','CONNECTICUT','DELAWARE',
  'FLORIDA','GEORGIA','HAWAII','IDAHO','ILLINOIS','INDIANA','IOWA','KANSAS','KENTUCKY','LOUISIANA',
  'MAINE','MARYLAND','MASSACHUSETTS','MICHIGAN','MINNESOTA','MISSISSIPPI','MISSOURI','MONTANA',
  'NEBRASKA','NEVADA','NEW HAMPSHIRE','NEW JERSEY','NEW MEXICO','NEW YORK','NORTH CAROLINA',
  'NORTH DAKOTA','OHIO','OKLAHOMA','OREGON','PENNSYLVANIA','RHODE ISLAND','SOUTH CAROLINA',
  'SOUTH DAKOTA','TENNESSEE','TEXAS','UTAH','VERMONT','VIRGINIA','WASHINGTON','WEST VIRGINIA',
  'WISCONSIN','WYOMING','DISTRICT OF COLUMBIA','PUERTO RICO'
)
group by state_tok
order by n desc, state_tok;

\echo ''
\echo '=== Q5: processing domain (Signal 3 input — the recall-side categories) ==='
select trim(processing) as processing_value, count(*) as n
from stg_usda_fsis_recalls
where processing is not null
group by trim(processing)
order by n desc
limit 40;
