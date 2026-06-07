{{ config(materialized='view') }}

-- 7-tuple "current per logical product" projection over stg_nhtsa_recalls (Phase 6c.6 Layer 2,
-- ADR 0033 + its 2026-06-06 amendment). stg_nhtsa_recalls is already latest-per-11-tuple; this
-- collapses FURTHER to one row per 7-tuple STABLE ANCHOR:
--   (campno, normalize_maketxt(maketxt), modeltxt, yeartxt, compname, rcl_cmpt_id, mfr_comp_ptno)
-- keeping the latest extraction's values for the demoted drift-prone attributes (mfr_comp_desc,
-- mfr_comp_name, bgman, endman, ...). It is the input to nhtsa_recall_product_snapshot (the SCD-2
-- Type-2 history) and, post-cutover (6c.7), the current state of recall_product's NHTSA branch.
--
-- WHY 7-tuple, not the 6-tuple ADR 0033 originally proposed (the load-bearing full-corpus finding):
-- building this prototype against the full corpus showed the 6-tuple collapsed recall_product
-- 321,540 -> 194,377 (-127,163, -40%). characterize_v15_collapse.sql attributed 126,417 (99.4%) of
-- that to STRUCTURAL mfr_comp_ptno variation — the documented Section-G multi-part fan-out (Takata-
-- class tire recall 24T014000 = 139 components x 139 part numbers; Fortune Tormenta LT235/75R15) —
-- and only 35 rows (0.03%) to genuine temporal desc/name drift. mfr_comp_ptno is the STRUCTURAL part
-- identity (legitimate distinct facts, "silver-correct" per incremental_delta_findings.md), not a
-- mutable attribute, so it belongs IN the anchor. Only the genuinely-drift-prone fields
-- (mfr_comp_desc/name = Pierce; bgman/endman = batch-window edits) are demoted to snapshot
-- attributes — fixing all 113 real_drift fragments without discarding the 126k structural rows.
--
-- Graceful degeneration for old records: mfr_comp_ptno was added 2020 and rcl_cmpt_id 2008 (Finding
-- F), so pre-2020 rows have ptno empty (7-tuple -> effective 6-tuple) and pre-2008 rows have both
-- empty (-> effective 5-tuple). Those cohorts carried no part granularity to begin with, so the
-- degenerate anchor loses nothing the source provided. recall_product_id is the v1.5 7-tuple md5,
-- SINGLE-HOMED here. maketxt is canonicalized via normalize_maketxt FOR THE ANCHOR (folds the AC
-- DELCO class); the RAW maketxt column is carried unchanged (survivor's spelling).

with current_per_product as (
    select distinct on (
        campno,
        {{ normalize_maketxt('maketxt') }},
        modeltxt,
        yeartxt,
        compname,
        rcl_cmpt_id,
        mfr_comp_ptno
    )
        *
    from {{ ref('stg_nhtsa_recalls') }}
    order by
        campno,
        {{ normalize_maketxt('maketxt') }},
        modeltxt,
        yeartxt,
        compname,
        rcl_cmpt_id,
        mfr_comp_ptno,
        extraction_timestamp desc,
        -- Deterministic tiebreaker (6c.7): latest-extraction wins, but the 832 collapsing 7-tuples
        -- (siblings differing only on these demoted fields) all share one seed-extraction timestamp,
        -- so without a tiebreaker the distinct-on pick is arbitrary and could FLIP on a physical
        -- reorder (VACUUM, etc.) -> the snapshot would bank a phantom version. The 4 demoted fields
        -- uniquely distinguish siblings within a 7-tuple (anything sharing all 4 is the same 11-tuple,
        -- already deduped by stg_nhtsa_recalls), so this makes the pick provably stable.
        mfr_comp_desc nulls last,
        mfr_comp_name nulls last,
        bgman nulls last,
        endman nulls last
)

select
    md5(
        'NHTSA' || '|' || campno
        || '|' || {{ normalize_maketxt('maketxt') }}
        || '|' || coalesce(modeltxt, '')
        || '|' || coalesce(yeartxt, '')
        || '|' || coalesce(compname, '')
        || '|' || coalesce(rcl_cmpt_id, '')
        || '|' || coalesce(mfr_comp_ptno, '')
    ) as recall_product_id,
    *
from current_per_product
