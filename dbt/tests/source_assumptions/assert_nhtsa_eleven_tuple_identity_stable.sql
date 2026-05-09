-- Singular test: NHTSA's 11-tuple identity (per ADR 0030) is stable
-- across runs — no identity field has been observed taking >1 distinct
-- value for the same (campno + 9 other identity fields + the dropped
-- one's complement) across multiple raw_landing_paths. Wraps the Q1
-- contingency from scripts/sql/nhtsa/bronze/assert_eleven_tuple_identity_stable.sql.
-- Severity=warn via dbt_project.yml — the AC DELCO maketxt drift
-- documented in ADR 0031:84 is an expected ongoing warning.
--
-- Mirrors the rich script's per-field UNION ALL exactly; each field
-- contributes any drift groups to the result set and dbt fails the
-- test (warns) if the result is non-empty.

select 'maketxt' as drifting_field
from {{ source('nhtsa', 'nhtsa_recalls_bronze') }}
group by campno, modeltxt, yeartxt, compname, rcl_cmpt_id,
         mfr_comp_ptno, mfr_comp_desc, mfr_comp_name, endman, bgman
having (count(distinct maketxt) > 1
        or (count(*) > count(maketxt) and count(maketxt) > 0))
   and count(distinct raw_landing_path) > 1
union all
select 'modeltxt'
from {{ source('nhtsa', 'nhtsa_recalls_bronze') }}
group by campno, maketxt, yeartxt, compname, rcl_cmpt_id,
         mfr_comp_ptno, mfr_comp_desc, mfr_comp_name, endman, bgman
having (count(distinct modeltxt) > 1
        or (count(*) > count(modeltxt) and count(modeltxt) > 0))
   and count(distinct raw_landing_path) > 1
union all
select 'yeartxt'
from {{ source('nhtsa', 'nhtsa_recalls_bronze') }}
group by campno, maketxt, modeltxt, compname, rcl_cmpt_id,
         mfr_comp_ptno, mfr_comp_desc, mfr_comp_name, endman, bgman
having (count(distinct yeartxt) > 1
        or (count(*) > count(yeartxt) and count(yeartxt) > 0))
   and count(distinct raw_landing_path) > 1
union all
select 'compname'
from {{ source('nhtsa', 'nhtsa_recalls_bronze') }}
group by campno, maketxt, modeltxt, yeartxt, rcl_cmpt_id,
         mfr_comp_ptno, mfr_comp_desc, mfr_comp_name, endman, bgman
having (count(distinct compname) > 1
        or (count(*) > count(compname) and count(compname) > 0))
   and count(distinct raw_landing_path) > 1
union all
select 'rcl_cmpt_id'
from {{ source('nhtsa', 'nhtsa_recalls_bronze') }}
group by campno, maketxt, modeltxt, yeartxt, compname,
         mfr_comp_ptno, mfr_comp_desc, mfr_comp_name, endman, bgman
having (count(distinct rcl_cmpt_id) > 1
        or (count(*) > count(rcl_cmpt_id) and count(rcl_cmpt_id) > 0))
   and count(distinct raw_landing_path) > 1
union all
select 'mfr_comp_ptno'
from {{ source('nhtsa', 'nhtsa_recalls_bronze') }}
group by campno, maketxt, modeltxt, yeartxt, compname, rcl_cmpt_id,
         mfr_comp_desc, mfr_comp_name, endman, bgman
having (count(distinct mfr_comp_ptno) > 1
        or (count(*) > count(mfr_comp_ptno) and count(mfr_comp_ptno) > 0))
   and count(distinct raw_landing_path) > 1
union all
select 'mfr_comp_desc'
from {{ source('nhtsa', 'nhtsa_recalls_bronze') }}
group by campno, maketxt, modeltxt, yeartxt, compname, rcl_cmpt_id,
         mfr_comp_ptno, mfr_comp_name, endman, bgman
having (count(distinct mfr_comp_desc) > 1
        or (count(*) > count(mfr_comp_desc) and count(mfr_comp_desc) > 0))
   and count(distinct raw_landing_path) > 1
union all
select 'mfr_comp_name'
from {{ source('nhtsa', 'nhtsa_recalls_bronze') }}
group by campno, maketxt, modeltxt, yeartxt, compname, rcl_cmpt_id,
         mfr_comp_ptno, mfr_comp_desc, endman, bgman
having (count(distinct mfr_comp_name) > 1
        or (count(*) > count(mfr_comp_name) and count(mfr_comp_name) > 0))
   and count(distinct raw_landing_path) > 1
union all
select 'endman'
from {{ source('nhtsa', 'nhtsa_recalls_bronze') }}
group by campno, maketxt, modeltxt, yeartxt, compname, rcl_cmpt_id,
         mfr_comp_ptno, mfr_comp_desc, mfr_comp_name, bgman
having (count(distinct endman) > 1
        or (count(*) > count(endman) and count(endman) > 0))
   and count(distinct raw_landing_path) > 1
union all
select 'bgman'
from {{ source('nhtsa', 'nhtsa_recalls_bronze') }}
group by campno, maketxt, modeltxt, yeartxt, compname, rcl_cmpt_id,
         mfr_comp_ptno, mfr_comp_desc, mfr_comp_name, endman
having (count(distinct bgman) > 1
        or (count(*) > count(bgman) and count(bgman) > 0))
   and count(distinct raw_landing_path) > 1
