-- Diagnose which columns are driving the schema_rebaseline wave for
-- usda_establishments. For each column tracked in bronze, count how many
-- re-versioned identities have a different value before vs after this run.
--
-- ADR 0027 line 214 expects the diffs to concentrate in:
--   - geolocation     (None ↔ "false" reversal)
--   - county          (None ↔ "false" reversal)
--   - activities      (whitespace-strip removal)
--   - dbas            (whitespace-strip removal)
-- and possibly leading/trailing whitespace on text columns if the strip was
-- applied broadly. If diff counts spread evenly across ALL columns including
-- ones the ADR didn't touch (state, zip, fips_code, etc.), that's the signal
-- the wave is "phantom" — driven by a hash-function or serialization change
-- rather than real semantic differences. That would warrant inspecting the
-- bronze hashing code before continuing the rebaseline to FDA.
--
-- Pass the run_id via -v run_id='<uuid>'.

with new_versions as (
    select *
    from usda_fsis_establishments_bronze
    where extraction_timestamp >= (
        select started_at from extraction_runs where run_id = :'run_id'
    )
),
old_versions as (
    -- Latest pre-run bronze row per source_recall_id (the version that today's
    -- record dedups against during load).
    select distinct on (source_recall_id) *
    from usda_fsis_establishments_bronze
    where extraction_timestamp < (
        select started_at from extraction_runs where run_id = :'run_id'
    )
    order by source_recall_id, extraction_timestamp desc
)
select
    count(*)                                                                                   as total_compared,
    sum((n.geolocation         is distinct from o.geolocation)::int)         as geolocation_diff,
    sum((n.county              is distinct from o.county)::int)              as county_diff,
    sum((n.activities          is distinct from o.activities)::int)          as activities_diff,
    sum((n.dbas                is distinct from o.dbas)::int)                as dbas_diff,
    sum((n.establishment_name  is distinct from o.establishment_name)::int)  as establishment_name_diff,
    sum((n.establishment_number is distinct from o.establishment_number)::int) as establishment_number_diff,
    sum((n.address             is distinct from o.address)::int)             as address_diff,
    sum((n.city                is distinct from o.city)::int)                as city_diff,
    sum((n.state               is distinct from o.state)::int)               as state_diff,
    sum((n.zip                 is distinct from o.zip)::int)                 as zip_diff,
    sum((n.phone               is distinct from o.phone)::int)               as phone_diff,
    sum((n.duns_number         is distinct from o.duns_number)::int)         as duns_number_diff,
    sum((n.fips_code           is distinct from o.fips_code)::int)           as fips_code_diff,
    sum((n.size                is distinct from o.size)::int)                as size_diff,
    sum((n.district            is distinct from o.district)::int)            as district_diff,
    sum((n.circuit             is distinct from o.circuit)::int)             as circuit_diff,
    sum((n.status_regulated_est is distinct from o.status_regulated_est)::int) as status_regulated_est_diff,
    sum((n.latest_mpi_active_date is distinct from o.latest_mpi_active_date)::int) as latest_mpi_active_date_diff,
    sum((n.grant_date          is distinct from o.grant_date)::int)          as grant_date_diff
from new_versions n
join old_versions o using (source_recall_id);
