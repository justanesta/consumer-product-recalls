-- Post-seed verification for the production (main) backfill / cutover runbook
-- (documentation/operations.md → "Production (main) backfill / cutover").
--
-- Confirms, against whatever branch the current session points at (run
-- whoami.sql first!):
--   1. Migrations are applied to head.
--   2. Every bronze table is populated, with the Phase 6a.5 row-count gates
--      from project_scope/phase-6-execution-plan.md alongside for eyeballing.
--   3. source_watermarks has a row per source (a missing row silently fails
--      run-record inserts — see operations.md "Operator added a new source").
--
-- Watermark *progression* and *quarantine rate* are intentionally NOT duplicated
-- here — use watermark_health.sql and quarantine_check.sql in this same dir.
--
-- No parameters. Run as:  psql -f scripts/sql/_pipeline/seed_verify.sql

\pset null '<NULL>'

-- 1. Migration head — must match the expected alembic revision (0018 as of
--    the Phase 6a.5 cutover).
\echo '=== 1. alembic head applied ==='
select version_num from alembic_version;

-- 2. Bronze row counts vs the Phase 6a.5 gates. `gate` is a human reference,
--    not enforced. Full-dump sources (usda*, uscg*) have no deep-history gate —
--    one extract run is the full corpus.
\echo '=== 2. bronze row counts (vs 6a.5 gates) ==='
select 'cpsc_recalls_bronze'              as bronze_table, count(*) as rows, '>= 9,000'                  as gate from cpsc_recalls_bronze
union all
select 'fda_recalls_bronze',                       count(*), '> 5x prior daily-incremental count'         from fda_recalls_bronze
union all
select 'nhtsa_recalls_bronze',                     count(*), 'prior + ~380-440k (PRE+POST_2010, dedup)'    from nhtsa_recalls_bronze
union all
select 'usda_fsis_recalls_bronze',                 count(*), 'full corpus (~2,000)'                        from usda_fsis_recalls_bronze
union all
select 'usda_fsis_establishments_bronze',         count(*), 'full corpus (~7,900)'                        from usda_fsis_establishments_bronze
union all
select 'uscg_recalls_bronze',                      count(*), 'full corpus (~1,763)'                        from uscg_recalls_bronze
union all
select 'uscg_manufacturers_bronze',                count(*), 'full corpus (~16,263)'                       from uscg_manufacturers_bronze
union all
select 'uscg_manufacturer_details_bronze',        count(*), 'full corpus (~16,263, ~4.5h seed)'           from uscg_manufacturer_details_bronze
order by bronze_table;

-- 3. Watermark coverage — every expected source must have a seed row. Anything
--    reported here as MISSING means the corresponding seed migration did not run
--    (re-check `alembic upgrade head`).
\echo '=== 3. source_watermarks coverage ==='
with expected(source) as (
    values
        ('cpsc'), ('fda'), ('nhtsa'),
        ('usda'), ('usda_establishments'),
        ('uscg'), ('uscg_manufacturers'), ('uscg_manufacturer_details')
)
select
    e.source,
    case when sw.source is null then 'MISSING — investigate' else 'present' end as watermark_row
from expected e
left join source_watermarks sw using (source)
order by e.source;
