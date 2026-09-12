{{ config(severity='error') }}
-- Singular test: a CPSC product does not CHANGE SLOT across bronze snapshots.
-- Returns one row per (source_recall_id, name, model) whose array position moved.
--
-- SEVERITY = ERROR, baseline 0 (verified on the full corpus 2026-09-12) --
-- overrides the source_assumptions group default of `warn` (dbt_project.yml).
-- This is the identity guard ADR 0031 actually needs. CPSC product identity keys
-- on (event, ordinal) alone -- md5('CPSC'|source_recall_id|product_ordinal),
-- `recall_product.sql` cpsc_products CTE -- so a product that moves slots means a
-- later product silently inherits an earlier slot's recall_product_id. That is
-- *conflation*, not fragmentation: no row count changes, no test fails, the id
-- just starts denoting something else. It must fail the build.
--
-- Split out 2026-09-12 from assert_cpsc_products_array_append_only, which now
-- carries only the shrinkage class. The two classes need different baselines --
-- this one is 0 and should stay a hard gate; shrinkage is 64 and rising (C2 is
-- falsified, see that file) -- and a single unioned test cannot hold two
-- thresholds.
--
-- PREDICATE. For each (recall, name, model) that occupies EXACTLY ONE slot in
-- every snapshot it appears in, flag it when that slot number differs across
-- snapshots. The singleton restriction is what makes this sound; without it the
-- test inherits the false positives that made the old append-only formulation
-- unusable:
--   * a (name, model) that legitimately appears twice in one array spans two
--     ordinals forever, with nothing having moved -- excluded, cardinality > 1
--   * one of a duplicate pair being renamed changes the group's ordinal SET
--     between snapshots without anything moving -- excluded, cardinality 2 then 1
-- Still catches the real modes whenever the moved element keeps its name:
--   reorder            [A,B]   -> [B,A]     A moves 1 -> 2
--   mid-array insert   [A,B]   -> [A,X,B]   B moves 2 -> 3
--   mid-array delete   [A,B,C] -> [A,C]     C moves 3 -> 2
--
-- KNOWN LIMITATION: blind to a product that is BOTH duplicated AND moved. If two
-- slots are textually identical nothing in the payload can say which one moved --
-- `CpscProduct` (src/schemas/cpsc.py) has no stable per-product identifier (name,
-- description, model, type, category_id, number_of_units, all mutable free text).
-- Also blind to a move that coincides with a rename of the moved element, which
-- is why the shrinkage class is metered separately rather than folded in here.
--
-- Drill-down: scripts/sql/cpsc/bronze/diagnose_products_array_append_only_violations.sql (Q7)

with exploded as (
    select
        source_recall_id,
        raw_landing_path,
        prod.ordinality::int as product_ordinal,
        -- sentinel-coalesced so the CTEs below can join on these keys without
        -- `is not distinct from`; Postgres GROUP BY already collides NULLs
        coalesce(prod.value ->> 'name',  '<<NULL>>') as product_name,
        coalesce(prod.value ->> 'model', '<<NULL>>') as product_model
    from {{ source('cpsc', 'cpsc_recalls_bronze') }},
         lateral jsonb_array_elements(coalesce(products, '[]'::jsonb))
             with ordinality as prod(value, ordinality)
),

-- per (recall, name, model, snapshot): how many slots the pair occupies there,
-- and which one (min is exact for the singletons kept below)
per_path as (
    select
        source_recall_id,
        product_name,
        product_model,
        raw_landing_path,
        count(*)             as n_slots_in_snapshot,
        min(product_ordinal) as the_ordinal
    from exploded
    group by source_recall_id, product_name, product_model, raw_landing_path
),

-- keep only pairs that are a singleton in EVERY snapshot they appear in
singletons as (
    select source_recall_id, product_name, product_model
    from per_path
    group by source_recall_id, product_name, product_model
    having max(n_slots_in_snapshot) = 1
)

select
    p.source_recall_id,
    p.product_name,
    p.product_model,
    count(*)                      as n_snapshots,
    count(distinct p.the_ordinal) as n_distinct_ordinals
from per_path p
join singletons s
  on  s.source_recall_id = p.source_recall_id
  and s.product_name     = p.product_name
  and s.product_model    = p.product_model
group by p.source_recall_id, p.product_name, p.product_model
having count(distinct p.the_ordinal) > 1
