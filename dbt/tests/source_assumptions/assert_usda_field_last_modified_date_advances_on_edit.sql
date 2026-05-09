-- Singular test: USDA last_modified_date does not regress or stay put
-- when bronze content_hash changes (Finding E falsification). Returns
-- rows for each consecutive-pair transition where content changed but
-- last_modified_date either went backwards or stayed the same. The
-- NULL→non-NULL "advanced_from_null" and NULL→NULL "unusable" cells
-- from the rich diagnostic are NOT flagged here — those reflect
-- field-population gaps, not signal failures. Severity=warn via
-- dbt_project.yml. Full transition contingency at
-- scripts/sql/usda_recalls/bronze/assert_field_last_modified_date_advances_on_edit.sql.
--
-- Rebaseline filter (matches the rich script): rows from
-- change_type IN ('schema_rebaseline', 'hash_helper_rebaseline') are
-- excluded before LAG so that per-ADR-0027 hash recomputations don't
-- look like edits. NULL change_type (pre-migration-0009) is kept as
-- routine. Phase 6 recall_event_history will use the same filter
-- (implementation_plan.md:611).

with routine_only as (
    select b.*
    from {{ source('usda', 'usda_fsis_recalls_bronze') }} b
    left join extraction_runs r on b.raw_landing_path = r.raw_landing_path
    where r.change_type is null
       or r.change_type not in ('schema_rebaseline', 'hash_helper_rebaseline')
),
ordered as (
    select
        source_recall_id,
        langcode,
        extraction_timestamp,
        last_modified_date,
        content_hash,
        lag(last_modified_date) over w as prev_last_modified_date,
        lag(content_hash)       over w as prev_content_hash
    from routine_only
    window w as (partition by source_recall_id, langcode order by extraction_timestamp)
)
select source_recall_id, langcode, extraction_timestamp, last_modified_date
from ordered
where prev_content_hash is not null
  and content_hash is distinct from prev_content_hash
  and prev_last_modified_date is not null
  and (
       last_modified_date is not distinct from prev_last_modified_date
       or last_modified_date < prev_last_modified_date
  )
