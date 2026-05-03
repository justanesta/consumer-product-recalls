{{ config(materialized='table') }}

-- FSIS-regulated establishment attributes — demographic + geolocation +
-- regulatory metadata that doesn't fit on firm.sql (which is keyed on
-- normalized name and shared across CPSC/FDA/USDA). Phase 5b.2 Step 5
-- per project_scope/implementation_plan.md line 333-334.
--
-- One row per establishment_number (the FSIS canonical id, which is the
-- column populated as company_id on USDA-establishment firms in firm.sql).
-- Records with null establishment_number are excluded — they can't be joined
-- back to a firm so they have no place in this dim.
--
-- Source: stg_usda_fsis_establishments (the new Step-5 staging view).
-- The recall side has no analogous fields; firm.sql remains keyed on
-- normalized name for cross-source dedup, and this dim sits alongside it.

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
from {{ ref('stg_usda_fsis_establishments') }}
where establishment_number is not null
