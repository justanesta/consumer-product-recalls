{{ config(materialized='view') }}

-- Latest-per-establishment projection over the FSIS Establishment Listing
-- bronze table. Identity is (source_recall_id,) — single-key, no bilingual
-- dimension (Establishment Listing API is English-only).
--
-- Per ADR 0027 (line 200), bronze preserves source-verbatim values; this
-- staging layer applies the value-level normalization:
--
--   - nullif(col, '')          on optional text columns (Finding D in
--                              establishment_api_observations.md)
--   - nullif(col, 'false')     on geolocation / county — the source returns
--                              the JSON boolean false (~1.5% of records) as
--                              a "no value" sentinel. Cast to text in
--                              bronze, normalize to null in silver.
--   - jsonb_array_elements +   on activities / dbas — the source returns
--     trim + jsonb_agg          ragged whitespace on array elements after
--                              index 0 (Finding C: " Poultry Processing").

with ranked as (
    select
        *,
        row_number() over (
            partition by source_recall_id
            order by extraction_timestamp desc
        ) as rn
    from {{ source('usda_establishments', 'usda_fsis_establishments_bronze') }}
)

select
    source_recall_id,
    establishment_name,
    nullif(establishment_number, '') as establishment_number,
    nullif(address, '')              as address,
    city,
    state,
    zip,
    nullif(county, 'false')          as county,
    nullif(fips_code, '')            as fips_code,
    nullif(geolocation, 'false')     as geolocation,
    nullif(phone, '')                as phone,
    nullif(duns_number, '')          as duns_number,
    nullif(size, '')                 as size,
    nullif(district, '')             as district,
    nullif(circuit, '')              as circuit,
    status_regulated_est,
    latest_mpi_active_date::timestamptz as latest_mpi_active_date,
    grant_date::timestamptz             as grant_date,
    case
        when activities is not null then (
            select jsonb_agg(trim(elem))
            from jsonb_array_elements_text(activities) elem
        )
    end as activities,
    case
        when dbas is not null then (
            select jsonb_agg(trim(elem))
            from jsonb_array_elements_text(dbas) elem
        )
    end as dbas,
    content_hash,
    extraction_timestamp,
    raw_landing_path
from ranked
where rn = 1
