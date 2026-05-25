-- List establishments whose `status_regulated_est` flipped between the
-- prior bronze version and the version landed in a given run.
--
-- Companion to diagnose_rebaseline_column_diffs.sql. When that diagnostic
-- reports `status_regulated_est_diff = N` (N small, all other column diffs
-- ≈ 0), this script enumerates the N establishments: which way they
-- flipped (active ↔ Inactive), what their old/new extraction timestamps
-- are, and basic identity columns to eyeball whether the flips look like
-- plausible FSIS regulatory activity vs ingestion artifacts.
--
-- Per `documentation/usda/establishment_api_observations.md:344`,
-- `status_regulated_est` is confirmed exhaustive over two values:
--   ''         → active MPI (currently regulated by FSIS)
--   'Inactive' → inactive establishment
-- A flip is a real upstream change in regulatory status.
--
-- When to run:
--   * Immediately after diagnose_rebaseline_column_diffs.sql surfaces
--     non-zero status_regulated_est_diff.
--   * As a Phase 6 firm-resolution test-case generator — these are
--     concrete establishments whose attributes change over time.
--
-- What to look for:
--   * Q1: direction breakdown. Both directions are plausible (re-grants
--     and shutdowns); a one-sided sweep would be unusual and worth
--     correlating with FSIS publication events.
--   * Q2: per-establishment detail. If every flipped record carries a
--     plausible name/city/state and the old→new timestamps span a few
--     days, the flips are real edits. If multiple flipped records share
--     a suspect feature (e.g., all NULL state, or all extraction_timestamp
--     within milliseconds), it's worth correlating with the bronze
--     loader path.
--   * Q3: a sentinel third value. The accepted_values invariant on this
--     field is currently NOT enforced in `dbt/models/silver/_silver.yml`;
--     this query catches a future schema surprise before it propagates
--     to silver.
--
-- Pass the run_id via -v run_id='<uuid>'. Defaults to the most recent
-- successful usda_establishments run if omitted.

\set ON_ERROR_STOP on
\pset null '<NULL>'

\if :{?run_id}
\echo
\echo === Using passed run_id: :run_id ===
\else
select run_id as run_id
from extraction_runs
where source = 'usda_establishments' and status = 'success'
order by started_at desc
limit 1
\gset
\echo
\echo === Defaulted to most recent successful usda_establishments run: :run_id ===
\endif

-- Resolve the run's started_at once and capture as a psql variable so the
-- subsequent CTEs don't each re-query extraction_runs.
select started_at as run_started_at
from extraction_runs
where run_id = :'run_id'
\gset

\echo Run started_at: :run_started_at
\echo

\echo '=== Q1: direction summary (active ↔ Inactive) ==='
\echo 'Counts each flip type. Expected for a healthy small wave:'
\echo '  active_to_inactive + inactive_to_active ≈ N (no third-value rows).'
\echo 'A non-zero `unexpected_value` count means a third status appeared'
\echo 'upstream — file as a schema-invariant violation in establishment_api_observations.md.'

with new_versions as (
    select source_recall_id, status_regulated_est, extraction_timestamp
    from usda_fsis_establishments_bronze
    where extraction_timestamp >= :'run_started_at'::timestamptz
),
old_versions as (
    select distinct on (source_recall_id)
        source_recall_id, status_regulated_est, extraction_timestamp
    from usda_fsis_establishments_bronze
    where extraction_timestamp < :'run_started_at'::timestamptz
    order by source_recall_id, extraction_timestamp desc
),
flipped as (
    select
        n.source_recall_id,
        o.status_regulated_est as old_status,
        n.status_regulated_est as new_status
    from new_versions n
    join old_versions o using (source_recall_id)
    where n.status_regulated_est is distinct from o.status_regulated_est
)
select
    count(*) filter (
        where old_status = ''         and new_status = 'Inactive'
    ) as active_to_inactive,
    count(*) filter (
        where old_status = 'Inactive' and new_status = ''
    ) as inactive_to_active,
    count(*) filter (
        where old_status not in ('', 'Inactive')
           or new_status not in ('', 'Inactive')
    ) as unexpected_value,
    count(*) as total_flips
from flipped;

\echo
\echo '=== Q2: per-establishment detail ==='
\echo 'One row per flip. Eyeball: do the establishment names/locations look'
\echo 'like real FSIS-regulated entities? Are the timestamps tight against'
\echo 'today''s run? Are there clusters (e.g., 5 in one state suggesting a'
\echo 'regional FSIS action)?'

with new_versions as (
    select * from usda_fsis_establishments_bronze
    where extraction_timestamp >= :'run_started_at'::timestamptz
),
old_versions as (
    select distinct on (source_recall_id) *
    from usda_fsis_establishments_bronze
    where extraction_timestamp < :'run_started_at'::timestamptz
    order by source_recall_id, extraction_timestamp desc
)
select
    n.source_recall_id              as establishment_id,
    n.establishment_number,
    n.establishment_name,
    n.city,
    n.state,
    case
        when o.status_regulated_est = ''         then 'active'
        when o.status_regulated_est = 'Inactive' then 'Inactive'
        else o.status_regulated_est
    end                             as old_status,
    case
        when n.status_regulated_est = ''         then 'active'
        when n.status_regulated_est = 'Inactive' then 'Inactive'
        else n.status_regulated_est
    end                             as new_status,
    o.extraction_timestamp          as old_extracted_at,
    n.extraction_timestamp          as new_extracted_at,
    n.latest_mpi_active_date        as new_latest_mpi_active_date,
    n.grant_date                    as new_grant_date
from new_versions n
join old_versions o using (source_recall_id)
where n.status_regulated_est is distinct from o.status_regulated_est
order by old_status, new_status, n.state, n.city;

\echo
\echo '=== Q3: geographic distribution of flips ==='
\echo 'A wave concentrated in one state is plausible (regional FSIS push)'
\echo 'but worth noting; an even spread is the steady-state expectation.'

with new_versions as (
    select * from usda_fsis_establishments_bronze
    where extraction_timestamp >= :'run_started_at'::timestamptz
),
old_versions as (
    select distinct on (source_recall_id) *
    from usda_fsis_establishments_bronze
    where extraction_timestamp < :'run_started_at'::timestamptz
    order by source_recall_id, extraction_timestamp desc
)
select
    coalesce(n.state, '<NULL>') as state,
    count(*) as flips
from new_versions n
join old_versions o using (source_recall_id)
where n.status_regulated_est is distinct from o.status_regulated_est
group by n.state
order by flips desc, state;
