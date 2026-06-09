-- Verify the uscg_mic_reassignment_years funnel — explains why the lookup is 393 rows
-- (DIRECTORY-WIDE) while the gate probe_uscg_refinement_gates.sql reported ~23 (RECALL-SCOPED)
-- and only 6 recalls land uscg_mic_build_date_resolved. Read-only, re-runnable. Reads the dbt
-- model tables (public) + bronze. Run from repo root:
--   psql "$NEON_DATABASE_URL" -f scripts/sql/cross_source/scd_monitors/verify_uscg_reassignment_funnel.sql

\set ON_ERROR_STOP on
\pset null '<NULL>'

\echo '=== Q1: lookup scope — directory-wide OOB-recycled vs the lookup vs the recall overlap ==='
-- reassignment_years_rows (393) is keyed off oob_recycled_directory, NOT the recalled subset.
-- reassignment_years_on_recalled_mic (~23) is the only part the recall_event_firm join can use.
select
    (select count(*) from firm_uscg_attributes)                         as mics_total_directory,
    (select count(*) from firm_uscg_attributes where mic_oob_recycled)  as oob_recycled_directory,
    (select count(*) from uscg_mic_reassignment_years)                          as reassignment_years_rows,
    (select count(*) from uscg_mic_reassignment_years ry
       where exists (select 1 from uscg_recalls_bronze r
                     where upper(trim(r.mic)) = ry.mic))                        as reassignment_years_on_recalled_mic;

\echo ''
\echo '=== Q2: sample parsed years — eyeball for false positives (year not actually the OOB year) ==='
select mic, current_holder_since_year, reassignment_years
from uscg_mic_reassignment_years
order by mic
limit 25;

\echo ''
\echo '=== Q3: build-year funnel on recalls whose MIC has a PARSEABLE reassignment year ==='
-- Mirrors recall_event_firm: reads stg_uscg_recalls, same announced_at filter + same mic key.
--   resolved_build_date            = build_date_resolved (model_year >= reassignment year).
--   built_before_stays_timesensitive = older boat, built under a PRIOR holder — correctly stays
--                                      time-sensitive (this IS the recycle hazard, not a miss).
--   model_year_unusable            = no clean 4-digit model_year to compare.
with rec as (
    select source_recall_id, upper(trim(mic)) as mic, model_year
    from stg_uscg_recalls
    where announced_at is not null
)
select
    count(*)                                                                              as recall_rows_on_parseable_year_mic,
    count(*) filter (where rec.model_year ~ '^(19|20)\d\d$')                              as with_clean_4digit_model_year,
    count(*) filter (where rec.model_year ~ '^(19|20)\d\d$'
                       and rec.model_year::int >= ry.current_holder_since_year)           as resolved_build_date,
    count(*) filter (where rec.model_year ~ '^(19|20)\d\d$'
                       and rec.model_year::int <  ry.current_holder_since_year)           as built_before_stays_timesensitive,
    count(*) filter (where rec.model_year is null or rec.model_year !~ '^(19|20)\d\d$')   as model_year_unusable
from rec
join uscg_mic_reassignment_years ry on ry.mic = rec.mic;
