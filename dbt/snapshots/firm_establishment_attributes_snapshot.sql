{% snapshot firm_establishment_attributes_snapshot %}
{{
  config(
    schema='silver_snapshots',
    unique_key='establishment_number',
    strategy='check',
    check_cols=[
      'establishment_name', 'address', 'city', 'state', 'zip', 'county', 'fips_code',
      'geolocation', 'grant_date', 'status_regulated_est', 'size', 'district', 'circuit',
      'activities', 'dbas',
    ],
  )
}}

-- SCD-2 history for USDA FSIS establishment attributes (ADR 0035 Policy C; Phase 6c.4 BENEFIT
-- dim). Stable anchor = establishment_number (the FSIS canonical id). strategy='check' over the
-- demographic / regulatory attributes; dbt manages dbt_valid_from / dbt_valid_to / dbt_scd_id.
-- The current view (dbt_valid_to is null) feeds firm_establishment_attributes; the full table is
-- the queryable peer history. The 2026-05-15 status_regulated_est '' -> 'Inactive' flip (13
-- establishments) is the motivating case. Lands in silver_snapshots (exempt from ADR 0007
-- bronze-snapshot pruning, schema-level — the same exemption ADR 0035 added for USCG).
--
-- check_cols EXCLUDES latest_mpi_active_date: it is a weekly-FSIS-republish heartbeat (ADR 0032
-- hash-excludes it for the same reason). Versioning on it would bank phantom history on every
-- re-scan. It is carried as a point-in-time column for the current-view sidecar.
--
-- BENEFIT, not NEED: establishment_number is stable (does not fragment) — 0 edit-versions in the
-- Phase 6a.5 re-seed, so this banks one version per anchor now and grows forward. (jsonb
-- check_cols activities/dbas + geolocation: if source reordering / precision jitter ever spawns
-- false versions, drop them to carried-only — staging already order-stabilizes the arrays.)

with latest as (
    select
        establishment_number,
        establishment_name,
        address,
        city,
        state,
        zip,
        county,
        fips_code,
        geolocation,
        latest_mpi_active_date,
        grant_date,
        status_regulated_est,
        size,
        district,
        circuit,
        activities,
        dbas,
        row_number() over (
            partition by establishment_number
            order by extraction_timestamp desc
        ) as _rn
    from {{ ref('stg_usda_fsis_establishments') }}
    where establishment_number is not null
)

select
    establishment_number,
    establishment_name,
    address,
    city,
    state,
    zip,
    county,
    fips_code,
    geolocation,
    latest_mpi_active_date,
    grant_date,
    status_regulated_est,
    size,
    district,
    circuit,
    activities,
    dbas
from latest
where _rn = 1

{% endsnapshot %}
