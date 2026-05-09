-- Find FDA productids with observable real edits in `fda_recalls_bronze` —
-- i.e., the same `source_recall_id` (= PRODUCTID) appearing with >1 distinct
-- `content_hash`, post-rebaseline filter. The output is the candidate set
-- for the Bruno re-probe at `bruno/fda/lookup/get_product_by_id.yml`,
-- which feeds the K0.1 closure logic at
-- `documentation/fda/api_observations.md`.
--
-- Why filter rebaseline runs: per ADR 0027 + the F2 row in
-- `documentation/source_assumption_audit.md`, bronze rows from
-- `extraction_runs.change_type IN ('schema_rebaseline', 'hash_helper_rebaseline')`
-- get re-hashed by the loader — those re-hashes are *our* artifacts, not
-- FDA edits. Pre-filter F2 saw 2,535 false silent edits attributable to
-- the 2026-05-01 architecture realignment; post-filter, 41 real edits.
-- This script applies the same filter so the productids it surfaces are
-- ones we have actual evidence FDA edited (content_hash changed across
-- two routine extractions), not extraction-side phantoms.
--
-- NULL change_type (pre-migration-0009 rows) is kept — those predate
-- the rebaseline mechanism and are routine. Mirrors the filter at
-- `dbt/tests/source_assumptions/assert_fda_eventlmd_correlates_with_content_change.sql:26-27`
-- and `scripts/sql/fda/bronze/assert_eventlmd_correlates_with_content_change.sql:92-93`.
--
-- Output usage:
--   Q1's productids → set `product_id` env var in
--   `bruno/fda/lookup/get_product_by_id.yml` and run the fixture for
--   each. Per K0.1 closure: if any returns non-null `PRODUCTLMD`,
--   reopen the capture decision (the 2026-05-09 probe got 5/5 null on
--   the unfiltered query; this filtered version validates the finding
--   on records where rebaseline noise is excluded).

\set ON_ERROR_STOP on
\pset null '<NULL>'

\echo
\echo '=== Q1: productids with real edits (post-rebaseline filter) ==='
\echo 'These are FDA records whose content_hash changed across >1 routine'
\echo 'extraction — i.e., evidence FDA actually edited the record. Use as'
\echo 'PRODUCTLMD probe candidates: feed each productid into bruno/fda/lookup/'
\echo 'get_product_by_id.yml and observe whether PRODUCTLMD populates.'

with routine_only as (
    select b.*
    from fda_recalls_bronze b
    left join extraction_runs r on b.raw_landing_path = r.raw_landing_path
    where r.change_type is null
       or r.change_type not in ('schema_rebaseline', 'hash_helper_rebaseline')
)
select
    source_recall_id as productid,
    count(distinct content_hash) as n_distinct_hashes,
    count(*) as n_rows,
    min(extraction_timestamp) as first_seen,
    max(extraction_timestamp) as last_seen,
    max(event_lmd) as latest_event_lmd
from routine_only
group by source_recall_id
having count(distinct content_hash) > 1
order by n_distinct_hashes desc, max(extraction_timestamp) desc
limit 10;

\echo
\echo '=== Q2: same query without the rebaseline filter (for comparison) ==='
\echo 'Shows what the unfiltered query (used in the 2026-05-09 probe) returns.'
\echo 'Productids appearing here but NOT in Q1 are rebaseline-driven phantoms —'
\echo 'their hash deltas are extraction-side artifacts, not FDA edits, and'
\echo 'they say nothing about F3.'

select
    source_recall_id as productid,
    count(distinct content_hash) as n_distinct_hashes,
    count(*) as n_rows,
    min(extraction_timestamp) as first_seen,
    max(extraction_timestamp) as last_seen,
    max(event_lmd) as latest_event_lmd
from fda_recalls_bronze
group by source_recall_id
having count(distinct content_hash) > 1
order by n_distinct_hashes desc, max(extraction_timestamp) desc
limit 10;

\echo
\echo '=== Q3: summary — real-edit class size vs. rebaseline-phantom class size ==='
\echo 'Top number is the real-edit population (Q1 universe); bottom is the'
\echo 'rebaseline-only population (in Q2 but not Q1). If real-edit count is 0,'
\echo 'no real FDA edits are observable in current bronze — F3 cannot be tested'
\echo 'on this corpus and the K0.1 closure stands on prior evidence alone.'

with routine_only as (
    select b.source_recall_id, b.content_hash
    from fda_recalls_bronze b
    left join extraction_runs r on b.raw_landing_path = r.raw_landing_path
    where r.change_type is null
       or r.change_type not in ('schema_rebaseline', 'hash_helper_rebaseline')
),
real_edit_productids as (
    select source_recall_id
    from routine_only
    group by source_recall_id
    having count(distinct content_hash) > 1
),
all_hash_churn_productids as (
    select source_recall_id
    from fda_recalls_bronze
    group by source_recall_id
    having count(distinct content_hash) > 1
)
select
    (select count(*) from real_edit_productids)              as real_edit_class_size,
    (select count(*) from all_hash_churn_productids)
        - (select count(*) from real_edit_productids)         as rebaseline_only_class_size,
    (select count(*) from all_hash_churn_productids)         as total_hash_churn_class_size;
