-- Singular test: USDA EN/ES bilingual siblings are atomically updated
-- (last_modified_date matches between the latest English and Spanish
-- versions of the same recall). Returns rows for each non-atomic pair.
-- Severity=warn via dbt_project.yml — ADR 0026 references a 13.3%
-- non-atomic baseline, so this is expected to surface ongoing warnings
-- until Phase 6 reconciliation. Rich version with date-gap analysis
-- at scripts/sql/usda_recalls/bronze/assert_bilingual_atomic_update.sql.

with latest as (
    select
        source_recall_id,
        langcode,
        last_modified_date,
        row_number() over (
            partition by source_recall_id, langcode
            order by extraction_timestamp desc
        ) as rn
    from {{ source('usda', 'usda_fsis_recalls_bronze') }}
),
latest_per_pair as (
    select source_recall_id, langcode, last_modified_date
    from latest
    where rn = 1
)
select
    en.source_recall_id,
    en.last_modified_date as en_last_modified,
    es.last_modified_date as es_last_modified
from latest_per_pair en
join latest_per_pair es
    on en.source_recall_id = es.source_recall_id
   and en.langcode = 'English'
   and es.langcode = 'Spanish'
where en.last_modified_date is distinct from es.last_modified_date
