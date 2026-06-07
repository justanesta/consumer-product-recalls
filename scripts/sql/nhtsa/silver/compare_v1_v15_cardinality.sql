-- Diagnostic: v1 recall_product (NHTSA slice, 11-tuple recipe) vs v1.5 recall_product_v15 (7-tuple
-- recipe, ADR 0033 2026-06-06 amendment) row counts (Phase 6c.6 Layer 2, ADR 0033 Layer-2 gate).
-- v1.5 <= v1 always; the DELTA is the products v1 over-counts. With the 7-tuple anchor (mfr_comp_ptno
-- back in the key) the delta should now be SMALL (~hundreds: the ~822 corpus rows differing only on
-- desc/name/bgman/endman within a 7-tuple + ~35 pure desc/name drift), NOT the 127,163 the 6-tuple
-- collapsed (those 126k structural part rows are now preserved). Read-only, re-runnable. Run from
-- repo root:
--   psql "$NEON_DATABASE_URL" -f scripts/sql/nhtsa/silver/compare_v1_v15_cardinality.sql

\set ON_ERROR_STOP on
\pset null '<NULL>'

\echo '=== Q1: overall NHTSA row count — v1 vs v1.5 (delta_collapsed = products v1 over-counts) ==='
select
    (select count(*) from recall_product where source = 'NHTSA') as v1_rows,
    (select count(*) from recall_product_v15)                    as v15_rows,
    (select count(*) from recall_product where source = 'NHTSA')
        - (select count(*) from recall_product_v15)              as delta_collapsed;

\echo ''
\echo '=== Q2: campaigns where v1 and v1.5 differ (the collapse points; empty = no within-corpus collisions) ==='
with v1 as (
    select source_recall_id as campno, count(*) as v1_rows
    from recall_product
    where source = 'NHTSA'
    group by 1
),
v15 as (
    select source_recall_id as campno, count(*) as v15_rows
    from recall_product_v15
    group by 1
)
select v1.campno, v1.v1_rows, v15.v15_rows, v1.v1_rows - v15.v15_rows as collapsed
from v1
join v15 on v15.campno = v1.campno
where v1.v1_rows <> v15.v15_rows
order by collapsed desc, v1.campno
limit 50;

\echo ''
\echo '=== Q3: sanity — every v1.5 recall_event_id must resolve in recall_event (expect 0) ==='
select count(*) as v15_orphan_events
from recall_product_v15 v
where not exists (
    select 1 from recall_event e where e.recall_event_id = v.recall_event_id
);

\echo ''
\echo '=== Q4: sanity — recall_product_id row-unique within v1.5 (expect 0 dupes) ==='
select count(*) as v15_duplicate_ids
from (
    select recall_product_id
    from recall_product_v15
    group by 1
    having count(*) > 1
) d;
