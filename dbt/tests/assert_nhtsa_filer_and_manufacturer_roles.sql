-- NHTSA filer/manufacturer split regression guard. The split must emit BOTH
-- roles: 'filer' (mfgname — the entity that filed the recall) and 'manufacturer'
-- (mfgtxt — the product manufacturer). Returns any expected role missing from
-- NHTSA's bridge rows — i.e., the split was reverted to a single role.
-- recall_event_firm has no source column, so NHTSA rows are identified by
-- joining to recall_event.

with expected(role) as (
    values ('filer'), ('manufacturer')
),

nhtsa_present as (
    select distinct ref.role
    from {{ ref('recall_event_firm') }} ref
    join {{ ref('recall_event') }} re
        on ref.recall_event_id = re.recall_event_id
    where re.source = 'NHTSA'
)

select e.role as missing_role
from expected e
left join nhtsa_present p
    on e.role = p.role
where p.role is null
