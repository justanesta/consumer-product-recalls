-- Inspect FSIS establishments that share a name with at least one other establishment.
--
-- Audit context: documentation/usda/field_audit_2026_w22.md §9 (2026-05-28 R2 validation)
-- surfaced that 6885 distinct establishment_names appear across 7970 records → ~14% of
-- FSIS establishments share a name with another. This is the empirical root of the
-- name-fan-out problem flagged in project_scope/phase-6-execution-plan.md § Phase 6b →
-- "USDA recall-to-establishment disambiguation."
--
-- Three queries:
--   Q1 — distribution of duplicate-group sizes (how many 2-grant groups, 3-grant groups, ...)
--   Q2 — top 50 duplicate-name groups with full context (number, city, state, MPI status, dates)
--   Q3 — heuristic categorization: multi-grant-same-state (likely same business with
--        multiple FSIS grants) vs multi-state (likely different businesses) vs mixed
--
-- Q3's categorization shapes Phase 6b's disambiguation hierarchy weighting: if most
-- duplicates are multi-grant-same-state, the per-grant-type signal (M vs P prefix) +
-- field_processing matters most. If many are multi-state, field_states ∩ establishment.state
-- is the dominant signal.
--
-- Uses the latest snapshot per establishment_id since bronze may have multiple rows per
-- establishment from re-extractions. establishment_id (= bronze source_recall_id) is the
-- canonical FSIS-side unique key.
--
-- Run:
--   psql -f scripts/sql/usda_establishments/bronze/inspect_duplicate_names.sql

\echo '=== Latest snapshot per establishment_id (temp view) ==='

create temp view latest_per_establishment as
select distinct on (source_recall_id)
    source_recall_id                     as establishment_id,
    establishment_name,
    establishment_number,
    city,
    state,
    status_regulated_est,
    latest_mpi_active_date::date         as latest_mpi_active_date,
    grant_date::date                     as grant_date
from usda_fsis_establishments_bronze
order by source_recall_id, extraction_timestamp desc;

\echo ''
\echo '=== Q1: Distribution of duplicate-group sizes ==='
\echo '    group_size = number of establishments sharing the same establishment_name'
\echo '    num_groups = how many distinct names fall in that size bucket'
\echo ''

select
    instance_count                       as group_size,
    count(*)                             as num_groups,
    sum(instance_count)                  as total_records_in_groups
from (
    select establishment_name, count(*) as instance_count
    from latest_per_establishment
    group by establishment_name
    having count(*) > 1
) groups
group by instance_count
order by instance_count;

\echo ''
\echo '=== Q2: Top 50 duplicate-name groups with full context ==='
\echo '    establishment_number prefix encodes grant type:'
\echo '    M = Meat, P = Poultry, I = Imports, G = Eggs, V = Voluntary'
\echo '    Look for: same state + different number prefixes → multi-grant same business'
\echo '              different states + same number prefix → different businesses, identical names'
\echo ''

select
    le.establishment_name,
    count(*)                             as instances,
    jsonb_agg(
        jsonb_build_object(
            'id',          le.establishment_id,
            'number',      le.establishment_number,
            'city',        le.city,
            'state',       le.state,
            'status',      case when le.status_regulated_est = '' then 'Active' else le.status_regulated_est end,
            'mpi_active',  le.latest_mpi_active_date,
            'grant_date',  le.grant_date
        )
        order by le.establishment_number
    )                                    as instances_detail
from latest_per_establishment le
where le.establishment_name in (
    select establishment_name
    from latest_per_establishment
    group by establishment_name
    having count(*) > 1
)
group by le.establishment_name
order by count(*) desc, le.establishment_name
limit 50;

\echo ''
\echo '=== Q3: Heuristic categorization of duplicate-name groups ==='
\echo '    multi_grant_same_state  — same name + same state, different numbers'
\echo '                              (likely same business with multiple FSIS grants)'
\echo '    multi_state             — same name + different states for every member'
\echo '                              (likely different businesses with identical legal names)'
\echo '    mixed                   — blend of above patterns'
\echo ''
\echo '    groups_with_inactive_member: how many groups include at least one Inactive'
\echo '    establishment — these are high-value disambiguation candidates (active wins)'
\echo ''

with grouped as (
    select
        establishment_name,
        count(*)                         as instances,
        count(distinct state)            as distinct_states,
        count(distinct establishment_number) as distinct_numbers,
        bool_or(status_regulated_est = '')        as any_active,
        bool_or(status_regulated_est = 'Inactive') as any_inactive
    from latest_per_establishment
    group by establishment_name
    having count(*) > 1
),
classified as (
    select
        *,
        case
            when distinct_states = 1                 then 'multi_grant_same_state'
            when distinct_states = instances         then 'multi_state'
            else 'mixed'
        end                              as likely_category
    from grouped
)
select
    likely_category,
    count(*)                             as group_count,
    sum(instances)                       as records_in_category,
    round(avg(instances)::numeric, 2)    as avg_group_size,
    sum(case when any_inactive then 1 else 0 end) as groups_with_inactive_member
from classified
group by likely_category
order by group_count desc;
