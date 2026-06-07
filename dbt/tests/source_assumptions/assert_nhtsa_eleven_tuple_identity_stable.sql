-- Singular test: NHTSA's 11-tuple identity (per ADR 0030) is stable
-- across runs — no identity field has been observed shifting its
-- *per-archive value set* for the same (other 10 identity fields) across
-- multiple raw_landing_paths. Severity=warn via dbt_project.yml.
--
-- Mechanism (refactored 2026-05-12): for each rotated identity field,
-- the test computes the set of distinct values that field took *within
-- each raw_landing_path* (string-agg, NULL-coalesced, sorted), then
-- groups by the other 9 identity fields and flags groups where the
-- per-path value sets are not all identical. This is the
-- "value-set-unchanged-across-paths" check foreshadowed at
-- `documentation/nhtsa/incremental_delta_findings.md:241-243` and
-- implemented as a diagnostic in
-- `scripts/sql/nhtsa/bronze/decompose_eleven_tuple_drift.sql`.
--
-- What this suppresses: structural multi-batch (silver-correct false
-- positive). One physical recall component legitimately reporting
-- multiple values for a field (e.g., Ferrari 26V152000 mfr_comp_ptno
-- `000788416` + `000788418`, both real, both in every archive). Every
-- path's value set is `{000788416, 000788418}`; the per-path string
-- aggregates to the identical canonical string; the group is not
-- flagged. Silver's `(11-tuple → max(extraction_timestamp))` lookup
-- materializes these correctly (one row per real value), so they were
-- never an anomaly — only the prior boolean filter mistook them for one.
--
-- What this still catches: real drift — the failure mode ADR 0030/0031
-- traded off. Field takes value A in archive 1 and value B in archive
-- 2, or swaps NULL ↔ populated (per-path string aggregates differ).
-- Examples currently flagged: Chrysler Pacifica `26V189000` airbag
-- `bgman: 2022-05-10 → 2022-05-17` (4 ptno variants synced by upstream
-- edit), Western Star `26V079000` `endman: 2026-02-03 → 2026-04-10`,
-- Mack `26V261000` brake-modulator `bgman`/`endman` populated → NULL
-- regressions.
--
-- NOTE (Phase 6c, ADR 0033/0034): the demoted fields most of these flag
-- (mfr_comp_desc / mfr_comp_name / bgman / endman) are now SCD-2 check_cols
-- on the `nhtsa_recall_product` 7-tuple anchor, so their drift is captured
-- as a banked snapshot version — NOT consumer-grain silver fragmentation.
-- This test therefore now reads as a BRONZE-level drift signal / "an SCD
-- version will be banked" indicator, not the silver-fragmentation alarm its
-- older framing implied. Genuine consumer-grain fragmentation only happens
-- on the 7-tuple ANCHOR fields (campno / maketxt / modeltxt / yeartxt /
-- compname / rcl_cmpt_id / mfr_comp_ptno); anchor drift specifically is also
-- watched by `assert_nhtsa_maketxt_drift_caught.sql`. (This is why the Phase
-- 6d "assert_nhtsa_daily_drift_under_threshold" velocity alert was dropped as
-- obsolete — see project_scope/phase-6d-execution-plan.md.)
--
-- Divergence from the rich SQL script: this dbt test no longer mirrors
-- `scripts/sql/nhtsa/bronze/assert_eleven_tuple_identity_stable.sql`
-- exactly. The rich script still uses the original boolean filter and
-- emits all drift groups (structural + real); `decompose_eleven_tuple_drift.sql`
-- splits its output into the two classes. The dbt test now reports only
-- real drift, so the dbt warn count is the operational signal — it
-- feeds ADR 0031:84 directly without manual decomposition. As of
-- 2026-05-12 the split is 95 structural / 9 real (104 → 9).

with per_path_maketxt as (
    select campno, modeltxt, yeartxt, compname, rcl_cmpt_id,
           mfr_comp_ptno, mfr_comp_desc, mfr_comp_name, endman, bgman,
           raw_landing_path,
           string_agg(distinct coalesce(maketxt::text, '<NULL>'),
                      ', ' order by coalesce(maketxt::text, '<NULL>')) as path_value_set
    from {{ source('nhtsa', 'nhtsa_recalls_bronze') }}
    group by campno, modeltxt, yeartxt, compname, rcl_cmpt_id,
             mfr_comp_ptno, mfr_comp_desc, mfr_comp_name, endman, bgman,
             raw_landing_path
),
per_path_modeltxt as (
    select campno, maketxt, yeartxt, compname, rcl_cmpt_id,
           mfr_comp_ptno, mfr_comp_desc, mfr_comp_name, endman, bgman,
           raw_landing_path,
           string_agg(distinct coalesce(modeltxt::text, '<NULL>'),
                      ', ' order by coalesce(modeltxt::text, '<NULL>')) as path_value_set
    from {{ source('nhtsa', 'nhtsa_recalls_bronze') }}
    group by campno, maketxt, yeartxt, compname, rcl_cmpt_id,
             mfr_comp_ptno, mfr_comp_desc, mfr_comp_name, endman, bgman,
             raw_landing_path
),
per_path_yeartxt as (
    select campno, maketxt, modeltxt, compname, rcl_cmpt_id,
           mfr_comp_ptno, mfr_comp_desc, mfr_comp_name, endman, bgman,
           raw_landing_path,
           string_agg(distinct coalesce(yeartxt::text, '<NULL>'),
                      ', ' order by coalesce(yeartxt::text, '<NULL>')) as path_value_set
    from {{ source('nhtsa', 'nhtsa_recalls_bronze') }}
    group by campno, maketxt, modeltxt, compname, rcl_cmpt_id,
             mfr_comp_ptno, mfr_comp_desc, mfr_comp_name, endman, bgman,
             raw_landing_path
),
per_path_compname as (
    select campno, maketxt, modeltxt, yeartxt, rcl_cmpt_id,
           mfr_comp_ptno, mfr_comp_desc, mfr_comp_name, endman, bgman,
           raw_landing_path,
           string_agg(distinct coalesce(compname::text, '<NULL>'),
                      ', ' order by coalesce(compname::text, '<NULL>')) as path_value_set
    from {{ source('nhtsa', 'nhtsa_recalls_bronze') }}
    group by campno, maketxt, modeltxt, yeartxt, rcl_cmpt_id,
             mfr_comp_ptno, mfr_comp_desc, mfr_comp_name, endman, bgman,
             raw_landing_path
),
per_path_rcl_cmpt_id as (
    select campno, maketxt, modeltxt, yeartxt, compname,
           mfr_comp_ptno, mfr_comp_desc, mfr_comp_name, endman, bgman,
           raw_landing_path,
           string_agg(distinct coalesce(rcl_cmpt_id::text, '<NULL>'),
                      ', ' order by coalesce(rcl_cmpt_id::text, '<NULL>')) as path_value_set
    from {{ source('nhtsa', 'nhtsa_recalls_bronze') }}
    group by campno, maketxt, modeltxt, yeartxt, compname,
             mfr_comp_ptno, mfr_comp_desc, mfr_comp_name, endman, bgman,
             raw_landing_path
),
per_path_mfr_comp_ptno as (
    select campno, maketxt, modeltxt, yeartxt, compname, rcl_cmpt_id,
           mfr_comp_desc, mfr_comp_name, endman, bgman,
           raw_landing_path,
           string_agg(distinct coalesce(mfr_comp_ptno::text, '<NULL>'),
                      ', ' order by coalesce(mfr_comp_ptno::text, '<NULL>')) as path_value_set
    from {{ source('nhtsa', 'nhtsa_recalls_bronze') }}
    group by campno, maketxt, modeltxt, yeartxt, compname, rcl_cmpt_id,
             mfr_comp_desc, mfr_comp_name, endman, bgman,
             raw_landing_path
),
per_path_mfr_comp_desc as (
    select campno, maketxt, modeltxt, yeartxt, compname, rcl_cmpt_id,
           mfr_comp_ptno, mfr_comp_name, endman, bgman,
           raw_landing_path,
           string_agg(distinct coalesce(mfr_comp_desc::text, '<NULL>'),
                      ', ' order by coalesce(mfr_comp_desc::text, '<NULL>')) as path_value_set
    from {{ source('nhtsa', 'nhtsa_recalls_bronze') }}
    group by campno, maketxt, modeltxt, yeartxt, compname, rcl_cmpt_id,
             mfr_comp_ptno, mfr_comp_name, endman, bgman,
             raw_landing_path
),
per_path_mfr_comp_name as (
    select campno, maketxt, modeltxt, yeartxt, compname, rcl_cmpt_id,
           mfr_comp_ptno, mfr_comp_desc, endman, bgman,
           raw_landing_path,
           string_agg(distinct coalesce(mfr_comp_name::text, '<NULL>'),
                      ', ' order by coalesce(mfr_comp_name::text, '<NULL>')) as path_value_set
    from {{ source('nhtsa', 'nhtsa_recalls_bronze') }}
    group by campno, maketxt, modeltxt, yeartxt, compname, rcl_cmpt_id,
             mfr_comp_ptno, mfr_comp_desc, endman, bgman,
             raw_landing_path
),
per_path_endman as (
    select campno, maketxt, modeltxt, yeartxt, compname, rcl_cmpt_id,
           mfr_comp_ptno, mfr_comp_desc, mfr_comp_name, bgman,
           raw_landing_path,
           string_agg(distinct coalesce(endman::text, '<NULL>'),
                      ', ' order by coalesce(endman::text, '<NULL>')) as path_value_set
    from {{ source('nhtsa', 'nhtsa_recalls_bronze') }}
    group by campno, maketxt, modeltxt, yeartxt, compname, rcl_cmpt_id,
             mfr_comp_ptno, mfr_comp_desc, mfr_comp_name, bgman,
             raw_landing_path
),
per_path_bgman as (
    select campno, maketxt, modeltxt, yeartxt, compname, rcl_cmpt_id,
           mfr_comp_ptno, mfr_comp_desc, mfr_comp_name, endman,
           raw_landing_path,
           string_agg(distinct coalesce(bgman::text, '<NULL>'),
                      ', ' order by coalesce(bgman::text, '<NULL>')) as path_value_set
    from {{ source('nhtsa', 'nhtsa_recalls_bronze') }}
    group by campno, maketxt, modeltxt, yeartxt, compname, rcl_cmpt_id,
             mfr_comp_ptno, mfr_comp_desc, mfr_comp_name, endman,
             raw_landing_path
)

select 'maketxt' as drifting_field
from per_path_maketxt
group by campno, modeltxt, yeartxt, compname, rcl_cmpt_id,
         mfr_comp_ptno, mfr_comp_desc, mfr_comp_name, endman, bgman
having count(distinct raw_landing_path) > 1
   and count(distinct path_value_set) > 1
union all
select 'modeltxt'
from per_path_modeltxt
group by campno, maketxt, yeartxt, compname, rcl_cmpt_id,
         mfr_comp_ptno, mfr_comp_desc, mfr_comp_name, endman, bgman
having count(distinct raw_landing_path) > 1
   and count(distinct path_value_set) > 1
union all
select 'yeartxt'
from per_path_yeartxt
group by campno, maketxt, modeltxt, compname, rcl_cmpt_id,
         mfr_comp_ptno, mfr_comp_desc, mfr_comp_name, endman, bgman
having count(distinct raw_landing_path) > 1
   and count(distinct path_value_set) > 1
union all
select 'compname'
from per_path_compname
group by campno, maketxt, modeltxt, yeartxt, rcl_cmpt_id,
         mfr_comp_ptno, mfr_comp_desc, mfr_comp_name, endman, bgman
having count(distinct raw_landing_path) > 1
   and count(distinct path_value_set) > 1
union all
select 'rcl_cmpt_id'
from per_path_rcl_cmpt_id
group by campno, maketxt, modeltxt, yeartxt, compname,
         mfr_comp_ptno, mfr_comp_desc, mfr_comp_name, endman, bgman
having count(distinct raw_landing_path) > 1
   and count(distinct path_value_set) > 1
union all
select 'mfr_comp_ptno'
from per_path_mfr_comp_ptno
group by campno, maketxt, modeltxt, yeartxt, compname, rcl_cmpt_id,
         mfr_comp_desc, mfr_comp_name, endman, bgman
having count(distinct raw_landing_path) > 1
   and count(distinct path_value_set) > 1
union all
select 'mfr_comp_desc'
from per_path_mfr_comp_desc
group by campno, maketxt, modeltxt, yeartxt, compname, rcl_cmpt_id,
         mfr_comp_ptno, mfr_comp_name, endman, bgman
having count(distinct raw_landing_path) > 1
   and count(distinct path_value_set) > 1
union all
select 'mfr_comp_name'
from per_path_mfr_comp_name
group by campno, maketxt, modeltxt, yeartxt, compname, rcl_cmpt_id,
         mfr_comp_ptno, mfr_comp_desc, endman, bgman
having count(distinct raw_landing_path) > 1
   and count(distinct path_value_set) > 1
union all
select 'endman'
from per_path_endman
group by campno, maketxt, modeltxt, yeartxt, compname, rcl_cmpt_id,
         mfr_comp_ptno, mfr_comp_desc, mfr_comp_name, bgman
having count(distinct raw_landing_path) > 1
   and count(distinct path_value_set) > 1
union all
select 'bgman'
from per_path_bgman
group by campno, maketxt, modeltxt, yeartxt, compname, rcl_cmpt_id,
         mfr_comp_ptno, mfr_comp_desc, mfr_comp_name, endman
having count(distinct raw_landing_path) > 1
   and count(distinct path_value_set) > 1
