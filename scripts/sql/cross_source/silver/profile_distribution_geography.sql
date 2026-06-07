-- Profile cross-source distribution geography (Phase 6e geography foundation — Tier-2 parser tuning).
--
-- PURPOSE: tune the FDA free-text -> distribution_state_codes[] parser against the FULL corpus
-- (not the ~5 audit samples), and cross-check the USDA reuse. FDA distribution_area_summary is
-- free text ("Nationwide", "Florida", "CA", "AR, GA, IL, ...", "Worldwide", country names). The
-- precision danger: 2-letter state codes collide with English words ("in"/"or"/"ok"), so the
-- parser will accept only UPPERCASE 2-letter tokens that are known state codes + exact full state
-- names, and drop everything else (precision-over-recall, per the project principle). These
-- queries measure how that rule behaves at corpus scale BEFORE the parser is written.
--
-- RUN (silver tables are on search_path; dumps land in data/exploratory/ which is gitignored):
--   psql "$DATABASE_URL" -f scripts/sql/cross_source/silver/profile_distribution_geography.sql
-- Then share the CSVs under data/exploratory/cross_source/ so the FDA parser can be tuned to the
-- real value distribution.

\set ON_ERROR_STOP on
\pset null '<NULL>'

-- Inline state lookup (mirrors dbt/seeds/us_state_abbr.csv) so this script is self-contained and
-- does not depend on `dbt seed` having run yet. Session-temp; dropped on disconnect.
create temporary table _st(name text, abbr text);
insert into _st(name, abbr) values
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
  ('VIRGIN ISLANDS','VI');

\echo '=== Q1: FDA distribution_area_summary — population + length ==='
select
  count(*)                                                   as fda_events,
  count(distribution_area_summary)                          as populated,
  round(100.0*count(distribution_area_summary)/count(*),1)  as pct_populated,
  count(distinct distribution_area_summary)                 as distinct_vals,
  min(length(distribution_area_summary))                    as min_len,
  round(avg(length(distribution_area_summary)))             as avg_len,
  max(length(distribution_area_summary))                    as max_len
from recall_event where source = 'FDA';

\echo '=== Q2: FDA top 60 distinct distribution_area_summary values (-> CSV) ==='
\copy (select distribution_area_summary, count(*) as n from recall_event where source='FDA' and distribution_area_summary is not null group by 1 order by 2 desc limit 60) to 'data/exploratory/cross_source/fda_distribution_top60.csv' with (format csv, header true)

\echo '=== Q3: FDA shape buckets (rows can match more than one) ==='
select
  count(*) filter (where lower(t) ~ 'nationwide|all 50|all states')  as nationwide_ish,
  count(*) filter (where lower(t) ~ 'worldwide|international|global') as worldwide_ish,
  count(*) filter (where t ~ '\m[A-Z]{2}\M')                         as has_uppercase_2char_token,
  count(*) filter (where t ~ ',')                                    as has_comma,
  count(*) filter (where length(t) > 200)                            as long_gt200,
  count(*)                                                           as total
from (select distribution_area_summary as t from recall_event where source='FDA' and distribution_area_summary is not null) z;

\echo '=== Q4: FDA candidate state extraction preview — eyeball precision (-> CSV) ==='
\copy (with fda as (select recall_event_id, distribution_area_summary as t from recall_event where source='FDA' and distribution_area_summary is not null), ex as (select f.recall_event_id, f.t, (select array_agg(distinct st) from (select m[1] as st from regexp_matches(f.t,'\m([A-Z]{2})\M','g') m join _st s on s.abbr=m[1] union select s.abbr from _st s where f.t ~* ('\m'||s.name||'\M')) u) as states from fda f) select recall_event_id, t, states, coalesce(cardinality(states),0) as n_states from ex order by n_states desc, t) to 'data/exploratory/cross_source/fda_state_extraction_preview.csv' with (format csv, header true)

\echo '=== Q5: FDA extraction summary — how many resolve to >=1 state ==='
with fda as (select distribution_area_summary as t from recall_event where source='FDA' and distribution_area_summary is not null),
ex as (select (select count(distinct st) from (select m[1] st from regexp_matches(t,'\m([A-Z]{2})\M','g') m join _st s on s.abbr=m[1] union select s.abbr from _st s where t ~* ('\m'||s.name||'\M')) u) as n_states from fda)
select
  count(*) filter (where n_states = 0)            as zero_states,
  count(*) filter (where n_states between 1 and 3) as one_to_three,
  count(*) filter (where n_states >= 4)            as four_plus,
  count(*)                                        as total
from ex;

\echo '=== Q6: USDA distribution_states — population + top values (-> CSV) ==='
select count(*) as usda_events, count(distribution_states) as populated,
  round(100.0*count(distribution_states)/count(*),1) as pct_populated
from recall_event where source='USDA';
\copy (select distribution_states, count(*) n from recall_event where source='USDA' and distribution_states is not null group by 1 order by 2 desc limit 60) to 'data/exploratory/cross_source/usda_states_top60.csv' with (format csv, header true)

\echo '=== Q7: USDA comma-tokens that are NOT a known state (Nationwide/Midwest/etc.) (-> CSV) ==='
\copy (with toks as (select trim(tok) as tok from recall_event, lateral unnest(string_to_array(distribution_states, ',')) tok where source='USDA' and distribution_states is not null) select upper(tok) as token, count(*) n from toks where upper(trim(tok)) not in (select name from _st) and upper(trim(tok)) not in (select abbr from _st) and trim(tok) <> '' group by 1 order by 2 desc) to 'data/exploratory/cross_source/usda_nonstate_tokens.csv' with (format csv, header true)

\echo '=== Q8: distribution_scope distribution after the G1 finish (sanity) ==='
select source, distribution_scope, count(*) n
from recall_event group by 1,2 order by 1, 2;
