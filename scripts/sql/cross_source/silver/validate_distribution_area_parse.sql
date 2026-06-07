-- Validate recall_distribution_area parse precision (Phase 6e geography foundation).
-- Run AFTER `dbt build --select recall_distribution_area`; dumps samples for a precision eyeball.
--   psql "$DATABASE_URL" -f scripts/sql/cross_source/silver/validate_distribution_area_parse.sql
-- Then share the CSVs under data/exploratory/cross_source/.

\set ON_ERROR_STOP on
\pset null '<NULL>'

\echo '=== Q1: coverage + n_states distribution ==='
select
  (select count(*) from recall_distribution_area)                            as rows_with_states,
  (select count(*) from recall_event where source in ('FDA','USDA'))         as fda_usda_events,
  (select round(avg(n_distribution_states),2) from recall_distribution_area) as avg_states,
  (select max(n_distribution_states) from recall_distribution_area)          as max_states;

\echo '=== Q2: extracted state-code frequency (sanity: CA/NY/TX/FL lead) (-> CSV) ==='
\copy (select code, count(*) n from recall_distribution_area, unnest(distribution_state_codes) code group by 1 order by 2 desc) to 'data/exploratory/cross_source/dist_state_code_freq.csv' with (format csv, header true)

\echo '=== Q3: PRECISION — FDA rows whose RAW text has an international marker but still got states ==='
\echo '       (extracted codes should be domestic-head only; eyeball for foreign-code leakage) (-> CSV)'
\copy (select re.recall_event_id, rda.distribution_state_codes, re.distribution_area_summary as raw from recall_distribution_area rda join recall_event re using (recall_event_id) where re.source='FDA' and re.distribution_area_summary ~* '(\mOUS\M|internationa|foreign|countr(y|ies)|\mROW\M|abroad)' order by re.recall_event_id) to 'data/exploratory/cross_source/dist_intl_marker_rows.csv' with (format csv, header true)

\echo '=== Q4: RESIDUAL — GA extracted AND raw text mentions Georgia (state vs country) (-> CSV) ==='
\copy (select re.recall_event_id, rda.distribution_state_codes, re.distribution_area_summary as raw from recall_distribution_area rda join recall_event re using (recall_event_id) where re.source='FDA' and 'GA' = any(rda.distribution_state_codes) and re.distribution_area_summary ~* '\ygeorgia\y' order by re.recall_event_id) to 'data/exploratory/cross_source/dist_georgia_rows.csv' with (format csv, header true)

\echo '=== Q5: recall (sanity) — FDA events with distribution text but NO extracted states ==='
select count(*) as fda_with_text_no_states
from recall_event re
left join recall_distribution_area rda using (recall_event_id)
where re.source='FDA' and re.distribution_area_summary is not null and rda.recall_event_id is null;

\echo '=== Q6: USDA coverage (clean comma lists) ==='
select
  count(*) filter (where rda.recall_event_id is not null) as usda_with_states,
  count(*)                                                as usda_with_text
from recall_event re
left join recall_distribution_area rda using (recall_event_id)
where re.source='USDA' and re.distribution_states is not null;
