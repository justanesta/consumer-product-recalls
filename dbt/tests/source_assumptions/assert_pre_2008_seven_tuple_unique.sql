-- Defensive monitor (ADR 0033 pre-2008 edge case; Phase 6c.6 Layer 2, 7-tuple anchor). The 7-tuple
-- adds rcl_cmpt_id (added 2008-03-14) and mfr_comp_ptno (added 2020-03) over the 5 stable fields, so
-- for pre-2008 records BOTH are empty and the anchor degenerates to the 5-tuple
-- (campno, normalize_maketxt(maketxt), modeltxt, yeartxt, compname). If two GENUINELY DISTINCT
-- pre-2008 products shared that 5-tuple, v1.5 would wrongly collapse them to one recall_product_id.
-- This flags any pre-2008 5-tuple that maps to >1 staging row — read against stg_nhtsa_recalls,
-- which is 11-tuple-unique, so >1 row = a real collapse point.
--
-- Rows here are NOT automatically wrong: they may be benign batch drift (bgman/endman) on the same
-- product, which v1.5 is designed to absorb as attribute history. Eyeball them. severity=warn via
-- tests/source_assumptions/ (+severity: warn in dbt_project.yml). ADR 0033: "fix the anchor recipe
-- if a real distinct-product collision is found." Escalate to error in 6c.8 once the full-corpus
-- build confirms clean. (Records 2008-2020 with rcl_cmpt_id present but mfr_comp_ptno empty degrade
-- to the 6-tuple and are covered by the broader compare_v1_v15_cardinality delta, not this test.)

select
    campno,
    {{ normalize_maketxt('maketxt') }} as make_norm,
    modeltxt,
    yeartxt,
    compname,
    count(*) as colliding_rows
from {{ ref('stg_nhtsa_recalls') }}
where coalesce(rcl_cmpt_id, '') = ''
group by 1, 2, 3, 4, 5
having count(*) > 1
