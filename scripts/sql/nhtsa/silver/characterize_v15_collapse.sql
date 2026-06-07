-- Characterize the v1.5 6-tuple collapse (Phase 6c.6 Layer 2 finding, 2026-06-06). The full-corpus
-- compare showed delta_collapsed = 127,163 (321,540 v1 -> 194,377 v1.5, -40%) — far above the
-- Pierce-style edit-drift the ADR 0033 example implied. The re-seed wiped edit-history, so this is
-- WITHIN-CORPUS collision: one 6-tuple anchor with many SIMULTANEOUS distinct 11-tuple rows. This
-- script answers the decision-forcing question: are those extra rows STRUCTURAL (distinct part
-- numbers / production batches — collapsing them drops real silver detail) or EDIT-DRIFT on
-- desc/name (collapsing them is correct de-fragmentation)?
--
-- Reads stg_nhtsa_recalls (already 11-tuple-unique, latest-per-identity). make_norm mirrors the
-- normalize_maketxt macro EXACTLY. Read-only, re-runnable. Run from repo root:
--   psql "$NEON_DATABASE_URL" -f scripts/sql/nhtsa/silver/characterize_v15_collapse.sql

\set ON_ERROR_STOP on
\pset null '<NULL>'

drop view if exists v15_collapse;
create temporary view v15_collapse as
select
    campno,
    regexp_replace(upper(trim(coalesce(maketxt, ''))), '\s+', '', 'g') as make_norm,
    modeltxt,
    yeartxt,
    compname,
    rcl_cmpt_id,
    count(*)                       as v1_rows,
    count(distinct mfr_comp_ptno)  as n_ptno,
    count(distinct bgman)          as n_bgman,
    count(distinct endman)         as n_endman,
    count(distinct mfr_comp_desc)  as n_desc,
    count(distinct mfr_comp_name)  as n_name
from stg_nhtsa_recalls
group by 1, 2, 3, 4, 5, 6;

\echo '=== Q1: of the COLLAPSING 6-tuples (v1_rows>1), how many vary on each demoted field? ==='
-- structural fields = mfr_comp_ptno / bgman / endman ; drift fields = mfr_comp_desc / mfr_comp_name
select
    count(*) filter (where v1_rows > 1)                  as collapsing_6tuples,
    count(*) filter (where v1_rows > 1 and n_ptno  > 1)  as vary_ptno,
    count(*) filter (where v1_rows > 1 and n_bgman > 1)  as vary_bgman,
    count(*) filter (where v1_rows > 1 and n_endman > 1) as vary_endman,
    count(*) filter (where v1_rows > 1 and n_desc  > 1)  as vary_desc,
    count(*) filter (where v1_rows > 1 and n_name  > 1)  as vary_name
from v15_collapse;

\echo ''
\echo '=== Q2: attribute the 127,163 collapsed rows — structural (part/batch) vs pure desc/name drift ==='
-- collapsed rows for a group = v1_rows - 1. Buckets are mutually exclusive + exhaustive over
-- collapsing groups (a group with all 11-tuple attrs identical would be ONE 11-tuple, so cannot
-- collapse). If collapsed_pure_desc_name_drift ~ 0, the collapse is ~entirely structural = real
-- part/batch detail leaving silver.
select
    sum(v1_rows - 1) filter (where v1_rows > 1)                                   as total_collapsed_rows,
    sum(v1_rows - 1) filter (
        where v1_rows > 1 and (n_ptno > 1 or n_bgman > 1 or n_endman > 1)
    )                                                                            as collapsed_with_structural_variation,
    sum(v1_rows - 1) filter (
        where v1_rows > 1 and n_ptno = 1 and n_bgman = 1 and n_endman = 1
              and (n_desc > 1 or n_name > 1)
    )                                                                            as collapsed_pure_desc_name_drift
from v15_collapse;

\echo ''
\echo '=== Q3: the biggest collapser 24T014000 — what do its many rows actually differ on? ==='
select
    count(*)                       as bronze_rows,
    count(distinct rcl_cmpt_id)    as n_components,
    count(distinct mfr_comp_ptno)  as n_part_numbers,
    count(distinct bgman)          as n_begin_dates,
    count(distinct endman)         as n_end_dates,
    count(distinct mfr_comp_desc)  as n_descs,
    count(distinct mfr_comp_name)  as n_names
from stg_nhtsa_recalls
where campno = '24T014000';

\echo ''
\echo '=== Q3b: 24T014000 sample rows (eyeball: distinct parts/batches, or repeated with desc edits?) ==='
select mfr_comp_ptno, bgman, endman, mfr_comp_desc, mfr_comp_name
from stg_nhtsa_recalls
where campno = '24T014000'
order by mfr_comp_ptno, bgman, endman
limit 25;
