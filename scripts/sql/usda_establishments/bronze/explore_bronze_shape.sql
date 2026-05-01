-- Phase 5b.2 Step 3 — bronze data exploration after first live extraction.
--
-- When to run: after `recalls extract usda_establishments` lands rows in
-- usda_fsis_establishments_bronze. Read top-down; output feeds the bronze
-- findings doc at documentation/usda/establishment_first_extraction_findings.md.
--
-- All queries use the latest version per source_recall_id (mirrors what silver
-- staging will see) by ranking on extraction_timestamp DESC. Bronze currently
-- holds two extraction generations (v1 with empty-string leakage, v2 with
-- normalization), so naive counts would double-count.

\echo '=== Q1: bronze cardinality (raw rows + latest-per-id) ==='
select
    (select count(*) from usda_fsis_establishments_bronze) as total_bronze_rows,
    (select count(distinct source_recall_id) from usda_fsis_establishments_bronze) as distinct_establishments,
    (select count(distinct extraction_timestamp::date) from usda_fsis_establishments_bronze) as distinct_extraction_dates;

-- Latest-per-id CTE used by every subsequent query.
-- Convenience: paste this CTE into ad-hoc psql to reuse.

\echo ''
\echo '=== Q2: status_regulated_est value distribution (Finding C exhaustiveness) ==='
with latest as (
    select distinct on (source_recall_id) *
    from usda_fsis_establishments_bronze
    order by source_recall_id, extraction_timestamp desc
)
select
    coalesce(nullif(status_regulated_est, ''), '<empty string>') as status_value,
    count(*) as n,
    round(100.0 * count(*) / sum(count(*)) over (), 2) as pct
from latest
group by status_value
order by n desc;

\echo ''
\echo '=== Q3: nullability rates per optional field (compare to Finding D) ==='
with latest as (
    select distinct on (source_recall_id) *
    from usda_fsis_establishments_bronze
    order by source_recall_id, extraction_timestamp desc
),
counts as (
    select
        count(*) as total,
        count(*) filter (where phone is null) as phone_null,
        count(*) filter (where duns_number is null) as duns_null,
        count(*) filter (where county is null) as county_null,
        count(*) filter (where county = 'false') as county_false_text,  -- post-ADR-0027 sentinel
        count(*) filter (where fips_code is null) as fips_null,
        count(*) filter (where geolocation is null) as geo_null,
        count(*) filter (where geolocation = 'false') as geo_false_text,
        count(*) filter (where grant_date is null) as grant_date_null,
        count(*) filter (where size is null) as size_null,
        count(*) filter (where district is null) as district_null,
        count(*) filter (where circuit is null) as circuit_null
    from latest
)
select
    field,
    null_count,
    total,
    round(100.0 * null_count / nullif(total, 0), 2) as pct_null
from counts,
     lateral (values
        ('phone',       phone_null),
        ('duns_number', duns_null),
        ('county',      county_null),
        ('fips_code',   fips_null),
        ('geolocation', geo_null),
        ('grant_date',  grant_date_null),
        ('size',        size_null),
        ('district',    district_null),
        ('circuit',     circuit_null)
     ) as t(field, null_count)
order by pct_null desc;

\echo ''
\echo '=== Q4: false-sentinel-as-text observations (post-ADR-0027) ==='
with latest as (
    select distinct on (source_recall_id) *
    from usda_fsis_establishments_bronze
    order by source_recall_id, extraction_timestamp desc
)
select
    'county' as field,
    count(*) filter (where county = 'false') as false_text_count,
    count(*) filter (where county is null) as null_count,
    count(*) as total
from latest
union all
select
    'geolocation' as field,
    count(*) filter (where geolocation = 'false') as false_text_count,
    count(*) filter (where geolocation is null) as null_count,
    count(*) as total
from latest;

\echo ''
\echo '=== Q5: activities and dbas array shape ==='
with latest as (
    select distinct on (source_recall_id) *
    from usda_fsis_establishments_bronze
    order by source_recall_id, extraction_timestamp desc
)
select
    'activities' as field,
    count(*) as total_records,
    count(*) filter (where jsonb_array_length(activities) = 0) as empty_array_count,
    round(avg(jsonb_array_length(activities))::numeric, 2) as avg_length,
    max(jsonb_array_length(activities)) as max_length
from latest
union all
select
    'dbas' as field,
    count(*) as total_records,
    count(*) filter (where jsonb_array_length(dbas) = 0) as empty_array_count,
    round(avg(jsonb_array_length(dbas))::numeric, 2) as avg_length,
    max(jsonb_array_length(dbas)) as max_length
from latest;

\echo ''
\echo '=== Q6: latest_mpi_active_date Finding G verification (100% populated?) ==='
with latest as (
    select distinct on (source_recall_id) *
    from usda_fsis_establishments_bronze
    order by source_recall_id, extraction_timestamp desc
)
select
    count(*) as total,
    count(*) filter (where latest_mpi_active_date is null) as null_count,
    min(latest_mpi_active_date) as oldest_date,
    max(latest_mpi_active_date) as newest_date,
    count(distinct latest_mpi_active_date::date) as distinct_dates
from latest;

\echo ''
\echo '=== Q7: state distribution (top 10) — sanity check on geographic spread ==='
with latest as (
    select distinct on (source_recall_id) *
    from usda_fsis_establishments_bronze
    order by source_recall_id, extraction_timestamp desc
)
select state, count(*) as n
from latest
group by state
order by n desc
limit 10;

\echo ''
\echo '=== Q8: re-version pattern — how many extractions has each record been through? ==='
select
    versions_per_record,
    count(*) as record_count
from (
    select source_recall_id, count(*) as versions_per_record
    from usda_fsis_establishments_bronze
    group by source_recall_id
) v
group by versions_per_record
order by versions_per_record;
