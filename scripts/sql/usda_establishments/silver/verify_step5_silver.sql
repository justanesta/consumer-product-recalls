-- Verify Phase 5b.2 Step 5 — USDA FSIS Establishment silver implementation.
-- Run after `dbt build` produces stg_usda_fsis_establishments + firm +
-- firm_establishment_attributes. No psql variables — all probes are
-- self-contained.
--
-- Expected outcomes per documentation/usda/establishment_join_coverage.md
-- and project_scope/implementation_plan.md lines 325-339:
--   1. Staging row count == bronze distinct source_recall_id count.
--   2. Per-distinct-name match rate >= 95% (vs 82.85% pre-HTML-decode baseline).
--   3. USDA-establishment firms have observed_company_ids populated for
--      ~95%+ of distinct establishment names.
--   4. firm_establishment_attributes has one row per FSIS establishment.

\pset null '<NULL>'

-- 1. Staging row-count parity. The new staging view should emit one row per
--    distinct (source_recall_id) in bronze. Mismatch suggests the ROW_NUMBER
--    dedup in stg_usda_fsis_establishments.sql is wrong.
select
    (select count(*)
     from public.stg_usda_fsis_establishments)            as silver_rows,
    (select count(distinct source_recall_id)
     from public.usda_fsis_establishments_bronze)         as bronze_distinct_pks,
    case
        when (select count(*) from public.stg_usda_fsis_establishments)
             = (select count(distinct source_recall_id)
                from public.usda_fsis_establishments_bronze)
        then 'OK'
        else 'MISMATCH — investigate'
    end                                                   as status;

-- 2. Per-distinct-name match-rate probe. Re-runs the probe from
--    documentation/usda/establishment_join_coverage.md. The HTML-entity
--    decode added in stg_usda_fsis_recalls.sql should lift the match rate
--    from the documented 82.85% baseline to ~97%.
with recall_names as (
    select distinct upper(trim(establishment)) as nrm
    from public.stg_usda_fsis_recalls
    where establishment is not null
      and trim(establishment) <> ''
),
est_names as (
    select distinct upper(trim(establishment_name)) as nrm
    from public.stg_usda_fsis_establishments
)
select
    count(*)                                  as total_distinct_recall_names,
    count(est_names.nrm)                      as matched,
    count(*) - count(est_names.nrm)           as unmatched,
    round(count(est_names.nrm) * 100.0 / nullif(count(*), 0), 2)
                                              as match_pct
from recall_names
left join est_names using (nrm);

-- 3. firm.sql USDA company_id population rate. For firms whose role on at
--    least one recall_event_firm row is 'establishment' AND whose only
--    contributing source is USDA (i.e. the firm wasn't already populated
--    via CPSC/FDA), how many now carry establishment_number(s) in
--    observed_company_ids?
with usda_only_establishment_firms as (
    select distinct ref.firm_id
    from public.recall_event_firm ref
    join public.recall_event re on ref.recall_event_id = re.recall_event_id
    where ref.role = 'establishment'
      and re.source = 'USDA'
)
select
    count(*)                                                              as usda_establishment_firms,
    count(*) filter (where f.observed_company_ids is not null
                       and jsonb_array_length(f.observed_company_ids) > 0)
                                                                          as with_establishment_id,
    round(
        count(*) filter (where f.observed_company_ids is not null
                           and jsonb_array_length(f.observed_company_ids) > 0)
        * 100.0 / nullif(count(*), 0),
        2
    )                                                                     as populated_pct
from usda_only_establishment_firms u
join public.firm f using (firm_id);

-- 4. firm_establishment_attributes row count vs distinct establishment_number
--    in staging. The dim filters out null establishment_number rows; counts
--    should match `count(distinct establishment_number) where not null`
--    in staging.
select
    (select count(*) from public.firm_establishment_attributes)                          as dim_rows,
    (select count(distinct establishment_number)
     from public.stg_usda_fsis_establishments
     where establishment_number is not null)                                             as staging_distinct_non_null,
    case
        when (select count(*) from public.firm_establishment_attributes)
             = (select count(distinct establishment_number)
                from public.stg_usda_fsis_establishments
                where establishment_number is not null)
        then 'OK'
        else 'MISMATCH — investigate'
    end                                                                                  as status;
