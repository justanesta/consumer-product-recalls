-- Settle whether the 7-tuple residual collapse (1,237 rows, Phase 6c.6) loses VEHICLE-MODEL coverage
-- or just redundant component-description text. modeltxt is IN the 7-tuple anchor, so distinct
-- modeltxt values are never collapsed. The question is whether mfr_comp_desc's variation sits WITHIN
-- a fixed modeltxt (then it is sub-description text — collapsing loses nothing material) or whether
-- modeltxt is blank/generic while the real vehicle lives in mfr_comp_desc (then collapsing loses
-- model coverage — still 0.38% and unavoidable since desc must stay a snapshot attribute, but worth
-- knowing). Reads stg_nhtsa_recalls. Read-only. Run from repo root:
--   psql "$NEON_DATABASE_URL" -f scripts/sql/nhtsa/silver/inspect_v15_residual_modeltxt.sql

\set ON_ERROR_STOP on
\pset null '<NULL>'

\echo '=== Q1: 21V215000 — maketxt / modeltxt / yeartxt vs mfr_comp_desc (does modeltxt carry the model?) ==='
select maketxt, modeltxt, yeartxt, mfr_comp_ptno, mfr_comp_desc
from stg_nhtsa_recalls
where campno = '21V215000'
order by maketxt, modeltxt, yeartxt, mfr_comp_ptno, mfr_comp_desc
limit 50;

\echo ''
\echo '=== Q2: corpus-wide — of collapsing 7-tuples, is modeltxt usually populated? (blank => desc carries coverage) ==='
-- A collapsing 7-tuple with a populated modeltxt means the vehicle is already pinned by the anchor and
-- the desc/name variation is sub-description (no coverage loss). A blank modeltxt means the desc may be
-- the only model signal.
with collapsing as (
    select
        campno,
        regexp_replace(upper(trim(coalesce(maketxt, ''))), '\s+', '', 'g') as make_norm,
        modeltxt, yeartxt, compname, rcl_cmpt_id, mfr_comp_ptno,
        count(*) as v1_rows
    from stg_nhtsa_recalls
    group by 1, 2, 3, 4, 5, 6, 7
    having count(*) > 1
)
select
    count(*)                                                          as collapsing_7tuples,
    count(*) filter (where coalesce(trim(modeltxt), '') <> '')        as with_populated_modeltxt,
    count(*) filter (where coalesce(trim(modeltxt), '') = '')         as with_blank_modeltxt
from collapsing;
