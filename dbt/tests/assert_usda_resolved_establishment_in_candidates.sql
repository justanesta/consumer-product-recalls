-- Phase 6b PR 6b.2 correctness guard: every resolved establishment_number must be one of THAT
-- recall's name-matched establishment candidates. A resolution outside the candidate set means the
-- disambiguation produced an establishment the recall's name never matched — a bug. Expect 0 rows.

with resolved as (
    select recall_event_id, establishment_number
    from {{ ref('recall_event_establishment_resolution') }}
    where establishment_number is not null
),

candidates as (
    select
        md5('USDA' || '|' || r.source_recall_id) as recall_event_id,
        e.establishment_id
    from {{ ref('stg_usda_fsis_recalls') }} r
    join {{ ref('firm_establishment_attributes') }} e
        on upper(trim(e.establishment_name)) = upper(trim(r.establishment))
)

select res.recall_event_id, res.establishment_number
from resolved res
where not exists (
    select 1 from candidates c
    where c.recall_event_id = res.recall_event_id
      and c.establishment_id = res.establishment_number
)
