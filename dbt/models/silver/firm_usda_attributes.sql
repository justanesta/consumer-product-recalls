{{ config(
    materialized='table',
    indexes=[
      {'columns': ['establishment_id'], 'unique': True},
      {'columns': ['state']},
    ],
    post_hook="analyze {{ this }}"
) }}

-- FSIS-regulated establishment attributes — demographic + geolocation + regulatory metadata
-- that doesn't fit on firm.sql (keyed on normalized name, shared across sources). One row per
-- establishment_number (the FSIS canonical id, written as company_id on USDA firms in firm.sql).
--
-- As of Phase 6c.4 this is the CURRENT view (dbt_valid_to is null) over the SCD-2 snapshot
-- firm_usda_attributes_snapshot (ADR 0035 Policy C) — an additive history layer over
-- the same stg_usda_fsis_establishments data. The column contract is UNCHANGED, so the consumer
-- (recall_event_establishment_resolution) is unaffected; null-establishment_number rows are
-- excluded by the snapshot driver. The latest-per-establishment_number collapse now lives there.

select
    establishment_number          as establishment_id,
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
from {{ ref('firm_usda_attributes_snapshot') }}
where dbt_valid_to is null
