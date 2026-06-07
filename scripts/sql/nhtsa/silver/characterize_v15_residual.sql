-- Characterize the 7-tuple RESIDUAL collapse (the 1,237 rows compare_v1_v15_cardinality.sql still
-- shows after the 6-tuple -> 7-tuple fix; Phase 6c.6 Layer 2). These are rows that share a 7-tuple
-- anchor but differ on the DEMOTED fields (mfr_comp_desc, mfr_comp_name, bgman, endman). The question:
-- is each collapse benign?
--   - batch-window (bgman/endman vary)  = one part recalled across multiple production date ranges
--     (Finding L's ~822 "anomalies"); simultaneous-distinct -> the accepted ~0.4% latest-wins loss.
--   - component-metadata (desc/name vary) = supplier description/name variants or temporal edits;
--     temporal ones the snapshot tracks forward.
-- Reads stg_nhtsa_recalls (11-tuple-unique latest-per-identity). make_norm mirrors normalize_maketxt.
-- Read-only, re-runnable. Run from repo root:
--   psql "$NEON_DATABASE_URL" -f scripts/sql/nhtsa/silver/characterize_v15_residual.sql

\set ON_ERROR_STOP on
\pset null '<NULL>'

drop view if exists v15_residual;
create temporary view v15_residual as
select
    campno,
    regexp_replace(upper(trim(coalesce(maketxt, ''))), '\s+', '', 'g') as make_norm,
    modeltxt,
    yeartxt,
    compname,
    rcl_cmpt_id,
    mfr_comp_ptno,
    count(*)                              as v1_rows,
    count(distinct bgman)                 as n_bgman,
    count(distinct endman)                as n_endman,
    count(distinct mfr_comp_desc)         as n_desc,
    count(distinct mfr_comp_name)         as n_name,
    count(distinct extraction_timestamp)  as n_extractions
from stg_nhtsa_recalls
group by 1, 2, 3, 4, 5, 6, 7;

\echo '=== Q1: collapsing 7-tuples — how many vary on each demoted field? ==='
select
    count(*) filter (where v1_rows > 1)                                       as collapsing_7tuples,
    count(*) filter (where v1_rows > 1 and (n_bgman > 1 or n_endman > 1))     as vary_batch_window,
    count(*) filter (where v1_rows > 1 and n_desc > 1)                        as vary_desc,
    count(*) filter (where v1_rows > 1 and n_name > 1)                        as vary_name
from v15_residual;

\echo ''
\echo '=== Q2: attribute the 1,237 collapsed rows — batch-window vs desc/name-only ==='
select
    sum(v1_rows - 1) filter (where v1_rows > 1)                                                   as total_collapsed,
    sum(v1_rows - 1) filter (where v1_rows > 1 and (n_bgman > 1 or n_endman > 1))                 as collapsed_batch_window,
    sum(v1_rows - 1) filter (
        where v1_rows > 1 and n_bgman = 1 and n_endman = 1 and (n_desc > 1 or n_name > 1)
    )                                                                                            as collapsed_desc_name_only
from v15_residual;

\echo ''
\echo '=== Q3: simultaneous (one regen) vs possibly-temporal (spans regens) ==='
-- n_extractions=1: the colliding rows all came from ONE regen -> genuine simultaneous multi-batch
--   (the accepted latest-wins loss). n_extractions>1: spans regens -> likely temporal edit-versions
--   (the snapshot tracks these forward; the dropped older version is not lost history, it is the
--   pre-edit value bronze still holds).
select
    count(*) filter (where v1_rows > 1 and n_extractions = 1) as single_regen_simultaneous,
    count(*) filter (where v1_rows > 1 and n_extractions > 1) as multi_regen_possibly_temporal
from v15_residual;

\echo ''
\echo '=== Q4: top residual collapser 21V215000 (900->675) — what actually differs? ==='
select bgman, endman, mfr_comp_desc, mfr_comp_name, count(*) as rows
from stg_nhtsa_recalls
where campno = '21V215000'
group by 1, 2, 3, 4
order by count(*) desc
limit 20;
