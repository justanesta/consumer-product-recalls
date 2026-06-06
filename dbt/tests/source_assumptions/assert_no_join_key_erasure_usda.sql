-- SCD monitor (erasure tripwire) — a USDA join-key field populated in a prior snapshot but
-- empty/null in the latest. This is upstream FSIS clearing a field mid-lifecycle (the
-- PHA-04302026-01 case: field_establishment "Richelieu Foods, Inc." → "" between snapshots),
-- which silently breaks the silver establishment join (ADR 0026 / implementation_plan §6c).
--
-- establishment + company_media_contact are jsonb arrays as of the 2026-06 API change
-- (Finding S), so "empty" = NULL or []. To monitor another field, add a UNION branch.
-- severity=warn via dbt_project.yml. ~0 post-6a.5-reseed; measure-forward. (No re-baseline
-- filter: a parser change that empties a join key is itself worth surfacing, unlike the
-- amendment monitors where re-baseline edits are noise.)

with snaps as (
    select
        source_recall_id,
        langcode,
        extraction_timestamp,
        establishment,
        company_media_contact,
        row_number() over (
            partition by source_recall_id, langcode
            order by extraction_timestamp desc
        ) as rn
    from {{ source('usda', 'usda_fsis_recalls_bronze') }}
    where langcode = 'English'
)

select cur.source_recall_id, cur.langcode, 'establishment' as erased_field
from snaps cur
where cur.rn = 1
  and (cur.establishment is null or jsonb_array_length(cur.establishment) = 0)
  and exists (
      select 1 from snaps p
      where p.source_recall_id = cur.source_recall_id
        and p.langcode = cur.langcode
        and p.rn > 1
        and p.establishment is not null
        and jsonb_array_length(p.establishment) > 0
  )

union all

select cur.source_recall_id, cur.langcode, 'company_media_contact' as erased_field
from snaps cur
where cur.rn = 1
  and (cur.company_media_contact is null or jsonb_array_length(cur.company_media_contact) = 0)
  and exists (
      select 1 from snaps p
      where p.source_recall_id = cur.source_recall_id
        and p.langcode = cur.langcode
        and p.rn > 1
        and p.company_media_contact is not null
        and jsonb_array_length(p.company_media_contact) > 0
  )
