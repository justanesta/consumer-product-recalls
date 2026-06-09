{{ config(severity='warn') }}
-- Defensive guard (C12): every emitted distribution_country_codes element must be a known
-- ISO-3166-1 alpha-2 in the country_iso seed. By construction the parse JOINs the seed, so this is
-- green today; it catches a future hand-edit, a manual backfill, or seed drift that introduces a
-- code outside the curated set. Mirrors the USDA token-in-taxonomy tests. Severity=warn.
with emitted as (
    select distinct unnest(distribution_country_codes) as alpha2
    from {{ ref('recall_distribution_area') }}
)

select e.alpha2
from emitted e
left join {{ ref('country_iso') }} ci on ci.alpha2 = e.alpha2
where ci.alpha2 is null
