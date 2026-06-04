-- Probe B (6b.2) — Signal 2 selectivity: does `field_states` ∩ establishment.state resolve a fan-out?
--
-- WHY: Signal 1 (the embedded number) is high-precision but low-coverage (U2: ~2%). Signal 2 is the
-- likely workhorse for the `multi_state` (different-business, same-name) fan-outs. This measures how
-- often the recall's distribution states narrow the candidate set to EXACTLY ONE establishment.
--
-- CAVEATS measured here: (a) recall `states` are full names, establishment `state` is likely a
-- 2-letter abbreviation, so both sides are normalized to an abbreviation via the map; (b) `Nationwide`
-- recalls are non-disambiguating (excluded); (c) recall `states` are DISTRIBUTION geography, not the
-- production state — so a producer in AR shipping to TX yields "no candidate in-state" (the
-- precision-over-recall NULL case), which Q1 counts as `no_candidate_in_state`.
--
-- Feeds: project_scope/phase-6-execution-plan.md PR 6b.2 gate G4 + Signal 2.
-- Run with: psql ... -f scripts/sql/usda_recalls/silver/probe_signal2_states_selectivity.sql

\set ON_ERROR_STOP on
\pset null '<NULL>'
-- Silence the harmless "table does not exist, skipping" NOTICEs from DROP TABLE IF EXISTS on first run.
set client_min_messages = warning;

-- Full-name -> USPS abbreviation; territories included. Used to normalize BOTH sides (fallback =
-- the value uppercased, so an already-abbreviated establishment.state passes through).
drop table if exists _sm;
create temporary table _sm (name text, abbr text);
insert into _sm (name, abbr) values
 ('ALABAMA','AL'),('ALASKA','AK'),('ARIZONA','AZ'),('ARKANSAS','AR'),('CALIFORNIA','CA'),
 ('COLORADO','CO'),('CONNECTICUT','CT'),('DELAWARE','DE'),('FLORIDA','FL'),('GEORGIA','GA'),
 ('HAWAII','HI'),('IDAHO','ID'),('ILLINOIS','IL'),('INDIANA','IN'),('IOWA','IA'),('KANSAS','KS'),
 ('KENTUCKY','KY'),('LOUISIANA','LA'),('MAINE','ME'),('MARYLAND','MD'),('MASSACHUSETTS','MA'),
 ('MICHIGAN','MI'),('MINNESOTA','MN'),('MISSISSIPPI','MS'),('MISSOURI','MO'),('MONTANA','MT'),
 ('NEBRASKA','NE'),('NEVADA','NV'),('NEW HAMPSHIRE','NH'),('NEW JERSEY','NJ'),('NEW MEXICO','NM'),
 ('NEW YORK','NY'),('NORTH CAROLINA','NC'),('NORTH DAKOTA','ND'),('OHIO','OH'),('OKLAHOMA','OK'),
 ('OREGON','OR'),('PENNSYLVANIA','PA'),('RHODE ISLAND','RI'),('SOUTH CAROLINA','SC'),
 ('SOUTH DAKOTA','SD'),('TENNESSEE','TN'),('TEXAS','TX'),('UTAH','UT'),('VERMONT','VT'),
 ('VIRGINIA','VA'),('WASHINGTON','WA'),('WEST VIRGINIA','WV'),('WISCONSIN','WI'),('WYOMING','WY'),
 ('DISTRICT OF COLUMBIA','DC'),('PUERTO RICO','PR'),('GUAM','GU'),('AMERICAN SAMOA','AS'),
 ('VIRGIN ISLANDS','VI'),('U.S. VIRGIN ISLANDS','VI');

-- Fan-out recalls + a nationwide flag.
drop table if exists _fo;
create temporary table _fo as
select r.source_recall_id, r.establishment, r.states,
       (r.states ~* 'nationwide') as is_nationwide
from stg_usda_fsis_recalls r
where r.establishment is not null
  and (select count(*) from firm_establishment_attributes e
       where upper(trim(e.establishment_name)) = upper(trim(r.establishment))) >= 2;

\echo '=== Q0: format check — establishment.state vs recall states tokens (full name? abbrev?) ==='
select 'establishment.state' as side, upper(trim(state)) as val, count(*) as n
from firm_establishment_attributes where state is not null group by 1,2 order by n desc limit 8;

-- Normalized recall state set (abbrev), excluding nationwide recalls.
drop table if exists _rs;
create temporary table _rs as
select f.source_recall_id,
       coalesce((select abbr from _sm where _sm.name = upper(trim(tok))), upper(trim(tok))) as st
from _fo f, lateral unnest(string_to_array(f.states, ',')) as tok
where f.states is not null and not f.is_nationwide
  and upper(trim(tok)) <> 'MIDWEST' and trim(tok) <> '';

-- Candidates with normalized state.
drop table if exists _cand;
create temporary table _cand as
select f.source_recall_id, e.establishment_id,
       coalesce((select abbr from _sm where _sm.name = upper(trim(e.state))), upper(trim(e.state))) as est_st
from _fo f
join firm_establishment_attributes e on upper(trim(e.establishment_name)) = upper(trim(f.establishment));

\echo ''
\echo '=== Q1: Signal-2 resolution of the fan-out set ==='
-- state_resolves_one = exactly one candidate sits in a recall-affected state. no_candidate_in_state =
-- the production-elsewhere / abbrev-mismatch gap (precision-over-recall NULL).
with per as (
  select f.source_recall_id, f.is_nationwide,
         (select count(*) from _cand c
            where c.source_recall_id = f.source_recall_id
              and exists (select 1 from _rs rs where rs.source_recall_id = f.source_recall_id and rs.st = c.est_st)
         ) as n_in_state,
         (select count(*) from _rs rs where rs.source_recall_id = f.source_recall_id) as n_recall_states
  from _fo f
)
select
  count(*)                                                              as fanout_recalls,
  count(*) filter (where is_nationwide)                                 as nationwide_excluded,
  count(*) filter (where not is_nationwide and n_recall_states = 0)     as has_no_usable_states,
  count(*) filter (where not is_nationwide and n_in_state = 1)          as state_resolves_one,
  count(*) filter (where not is_nationwide and n_in_state = 0 and n_recall_states > 0) as no_candidate_in_state,
  count(*) filter (where not is_nationwide and n_in_state >= 2)         as still_ambiguous_multi_in_state,
  round(100.0 * count(*) filter (where not is_nationwide and n_in_state = 1) / nullif(count(*), 0), 1) as pct_resolved
from per;

\echo ''
\echo '=== Q2: DUMP fan-out recalls + state evidence to data/exploratory/usda_recalls/ ==='
\pset format csv
\pset footer off
\o data/exploratory/usda_recalls/u_signal2_states.csv
select
  f.source_recall_id,
  f.establishment,
  f.is_nationwide,
  left(f.states, 80)                                                                       as recall_states,
  (select count(*) from _cand c where c.source_recall_id = f.source_recall_id
     and exists (select 1 from _rs rs where rs.source_recall_id = f.source_recall_id and rs.st = c.est_st)) as n_candidates_in_state,
  (select string_agg(distinct c.est_st, ',' order by c.est_st) from _cand c where c.source_recall_id = f.source_recall_id) as candidate_states
from _fo f
order by f.is_nationwide, f.source_recall_id;
\o
\pset format aligned
\pset footer on
\echo 'Wrote data/exploratory/usda_recalls/u_signal2_states.csv'
