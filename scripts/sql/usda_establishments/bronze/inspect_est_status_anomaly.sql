-- Inspect out-of-domain `status_regulated_est` values in the USDA FSIS
-- establishment bronze — the incident diagnostic behind the 2026-07-08
-- `est_status` anomaly (see the "Finding G addendum (2026-07-08)" in
-- documentation/usda/establishment_api_observations.md).
--
-- Context: `status_regulated_est` has an exhaustive two-value domain (Finding C):
--   ''         -> active MPI
--   'Inactive' -> inactive establishment
-- On 2026-07-08 FSIS served a third value, `est_status` (the field name echoed
-- as its value), for exactly one establishment (M21734+P21734 / id 8537, Joseph
-- Epstein Foods) — a live upstream data glitch, not a domain change.
-- `stg_usda_fsis_establishments` normalizes any such token to NULL via
-- nullif(status_regulated_est, 'est_status'); this query is the standing
-- tripwire that enumerates ANY out-of-domain value so a future glitch (or a
-- genuinely new status) is caught and characterized.
--
-- Companion to list_status_flips.sql (which enumerates '' <-> 'Inactive' flips);
-- this one targets values OUTSIDE the documented domain. Read-only.
--
-- When to run:
--   * When accepted_values(['', 'Inactive']) fails on
--     stg_usda_fsis_establishments or firm_usda_attributes.
--   * Periodically, to confirm the domain still holds upstream.
--
-- What to look for:
--   * Q1: which out-of-domain values exist and how many bronze rows carry each.
--     A field-name-like token (e.g. 'est_status') is source garbage -> keep the
--     staging nullif. A plausible word (e.g. 'Suspended') may be a REAL new
--     status -> update the documented domain + accepted_values instead.
--   * Q2: per-establishment version history — did a real prior value ('' /
--     'Inactive') flip to the bad token, and when?
--   * Q3: current-version isolation — how many establishments present an
--     out-of-domain value in their LATEST bronze version (this is what staging
--     and the snapshot see before the nullif normalizes it away).

\set ON_ERROR_STOP on
\pset null '<NULL>'

\echo
\echo '=== Q1: out-of-domain status_regulated_est values (all bronze versions) ==='
\echo 'Expected domain: empty-string (active) or Inactive. Any other value is'
\echo 'either upstream garbage (a field-name-like token) or a genuinely new status.'

select
    status_regulated_est             as out_of_domain_value,
    count(*)                         as bronze_rows,
    count(distinct source_recall_id) as distinct_establishments,
    min(extraction_timestamp)        as first_seen,
    max(extraction_timestamp)        as last_seen
from usda_fsis_establishments_bronze
where status_regulated_est is not null
  and status_regulated_est not in ('', 'Inactive')
group by status_regulated_est
order by bronze_rows desc;

\echo
\echo '=== Q2: full version history of each affected establishment ==='
\echo 'Every bronze version, so you can see the flip (real prior value -> bad'
\echo 'token) and exactly when it happened.'

with affected as (
    select distinct source_recall_id
    from usda_fsis_establishments_bronze
    where status_regulated_est is not null
      and status_regulated_est not in ('', 'Inactive')
)
select
    b.source_recall_id       as establishment_id,
    b.establishment_number,
    b.establishment_name,
    b.city,
    b.state,
    b.status_regulated_est,
    b.latest_mpi_active_date,
    b.extraction_timestamp,
    b.content_hash
from usda_fsis_establishments_bronze b
join affected a using (source_recall_id)
order by b.source_recall_id, b.extraction_timestamp;

\echo
\echo '=== Q3: current-version isolation (what staging/the snapshot see) ==='
\echo 'Status distribution over the LATEST bronze version per establishment.'
\echo 'A healthy corpus has only empty-string and Inactive buckets; any other'
\echo 'bucket is the set of establishments whose current value staging nullifs to NULL.'

with latest as (
    select distinct on (source_recall_id)
        source_recall_id, status_regulated_est
    from usda_fsis_establishments_bronze
    order by source_recall_id, extraction_timestamp desc
)
select
    status_regulated_est as current_status_value,
    count(*)             as establishments
from latest
group by status_regulated_est
order by establishments desc;
