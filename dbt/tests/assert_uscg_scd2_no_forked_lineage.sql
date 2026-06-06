-- ADR 0035 SCD-2 integrity: under a `mic`-only unique_key, each MIC must have exactly
-- ONE current version (dbt_valid_to is null). More than one current row per MIC means
-- the lineage forked — e.g. a case-variant anchor slipped past the snapshot's
-- upper(trim(mic)) normalization, or the unique_key was widened to mic+company. This
-- is a hard correctness invariant for the current-view sidecar (it joins one row/mic),
-- so it fails the build (error) rather than warning.

select
    mic,
    count(*) as current_versions
from {{ ref('uscg_manufacturer_attributes_snapshot') }}
where dbt_valid_to is null
group by mic
having count(*) > 1
