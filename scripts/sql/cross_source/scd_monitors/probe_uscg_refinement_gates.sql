-- GATE PROBE for Phase 6c.5 (USCG SCD refinements) — validates the prerequisites for all three
-- parts BEFORE building the SQL (ADR 0035 §5 explicitly says the rename tier "needs validation
-- before building"; mirrors the 6b G1-G9 gate pattern + the full-corpus-validation principle).
-- All dbt-SQL after this — no Python.
--
--   Q1  (a) rename-vs-recycle tier  — does the "(previous name)" marker exist + how many recalled,
--                                      prior-holder, no-OOB MICs would downgrade to unambiguous?
--   Q2  (b) historical backfill     — how many OOB priors carry a parseable year? what does
--                                      In Business look like (interval-construction inputs)?
--   Q3  (c) as-of-build-year join   — model_year + hin population on recalls whose MIC has a prior
--                                      holder (the join needs model_year, NOT a HIN parse).
--   Q4  (c) the RESOLVABLE set       — recalls where the reassignment year AND the recall model_year
--                                      are both known = the true payoff of the build-year join.
--
-- Read-only, re-runnable. Reads bronze directly (like the sibling monitors). Run from repo root:
--   psql "$NEON_DATABASE_URL" -f scripts/sql/cross_source/scd_monitors/probe_uscg_refinement_gates.sql

\set ON_ERROR_STOP on
\pset null '<NULL>'

-- Shared: latest detail per RECALLED MIC that has a prior holder (the 365 set), past-company
-- sentinels cleaned to match firm_uscg_attributes / the monitors.
drop view if exists uscg_refine;
create temporary view uscg_refine as
with recall_mics as (
    select distinct upper(trim(mic)) as mic
    from uscg_recalls_bronze
    where nullif(trim(mic), '') is not null
),
det as (
    select distinct on (upper(trim(source_recall_id)))
        upper(trim(source_recall_id))                               as mic,
        company_name                                                as current_holder,
        nullif(nullif(nullif(trim(past_company_1), ''), '-'), 'UNK') as p1,
        nullif(nullif(nullif(trim(past_company_2), ''), '-'), 'UNK') as p2,
        nullif(nullif(nullif(trim(past_company_3), ''), '-'), 'UNK') as p3,
        out_of_business,
        in_business
    from uscg_manufacturer_details_bronze
    order by upper(trim(source_recall_id)), extraction_timestamp desc
)
select d.*
from det d
join recall_mics rm on rm.mic = d.mic
where coalesce(d.p1, d.p2, d.p3) is not null;

\echo '=== Q1: (a) rename tier — "(previous name)" marker among recalled MICs with a prior holder ==='
-- has_previous_name = any past slot carries the rename marker; rename_no_oob = those with NO OOB
-- marker (the set that would DOWNGRADE uscg_mic_time_sensitive_unresolved -> uscg_mic_unambiguous).
select
    count(*) as mics_with_prior,
    count(*) filter (
        where coalesce(p1, '') ~* 'previous name'
           or coalesce(p2, '') ~* 'previous name'
           or coalesce(p3, '') ~* 'previous name'
    ) as has_previous_name_marker,
    count(*) filter (
        where (coalesce(p1, '') ~* 'previous name'
            or coalesce(p2, '') ~* 'previous name'
            or coalesce(p3, '') ~* 'previous name')
          and not (coalesce(p1, '') ~ '\yOOB\y'
                or coalesce(p2, '') ~ '\yOOB\y'
                or coalesce(p3, '') ~ '\yOOB\y')
    ) as rename_no_oob
from uscg_refine;

\echo ''
\echo '=== Q1b: samples — confirm the EXACT "(previous name)" marker format before regex-matching it ==='
select mic, current_holder, p1, p2, p3
from uscg_refine
where coalesce(p1, '') ~* 'previous name'
   or coalesce(p2, '') ~* 'previous name'
   or coalesce(p3, '') ~* 'previous name'
limit 20;

\echo ''
\echo '=== Q2: (b) backfill — OOB priors with a parseable year (interval starts) ==='
select
    count(*) filter (
        where coalesce(p1, '') ~ '\yOOB\y' or coalesce(p2, '') ~ '\yOOB\y' or coalesce(p3, '') ~ '\yOOB\y'
    ) as oob_marked,
    count(*) filter (
        where (coalesce(p1, '') ~ '\yOOB\y' or coalesce(p2, '') ~ '\yOOB\y' or coalesce(p3, '') ~ '\yOOB\y')
          and (coalesce(p1, '') || coalesce(p2, '') || coalesce(p3, '')) ~ '(19|20)\d\d'
    ) as oob_with_year
from uscg_refine;

\echo ''
\echo '=== Q2b: samples — OOB priors + in_business (eyeball interval-construction inputs) ==='
select mic, p1, p2, p3, in_business
from uscg_refine
where coalesce(p1, '') ~ '\yOOB\y' or coalesce(p2, '') ~ '\yOOB\y' or coalesce(p3, '') ~ '\yOOB\y'
limit 20;

\echo ''
\echo '=== Q3: (c) build-year — model_year + hin population on recalls whose MIC has a prior holder ==='
with rec as (
    select distinct on (source_recall_id)
        source_recall_id,
        upper(trim(mic))                            as mic,
        nullif(nullif(trim(model_year), ''), '9999') as model_year,
        nullif(nullif(trim(hin), ''), 'N/A')         as hin
    from uscg_recalls_bronze
    order by source_recall_id, extraction_timestamp desc
)
select
    count(*)                                       as recalls_on_prior_holder_mic,
    count(*) filter (where r.model_year is not null) as with_model_year,
    count(*) filter (where r.hin is not null)        as with_hin
from rec r
join uscg_refine u on u.mic = r.mic;

\echo ''
\echo '=== Q4: (c) the RESOLVABLE set — reassignment year known AND recall model_year known ==='
with rec as (
    select distinct on (source_recall_id)
        source_recall_id,
        upper(trim(mic))                            as mic,
        nullif(nullif(trim(model_year), ''), '9999') as model_year
    from uscg_recalls_bronze
    order by source_recall_id, extraction_timestamp desc
)
select count(*) as as_of_year_resolvable
from rec r
join uscg_refine u on u.mic = r.mic
where r.model_year is not null
  and (coalesce(u.p1, '') || coalesce(u.p2, '') || coalesce(u.p3, '')) ~ 'OOB';
