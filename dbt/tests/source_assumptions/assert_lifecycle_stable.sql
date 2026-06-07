-- SCD monitor (Type-2-BENEFIT, measure-forward) — lifecycle_status TRANSITIONS
-- (FDA Ongoing→Terminated, USDA Active→Closed, USCG open→closed).
--
-- Same mechanism as the classification monitor: the lifecycle_status slice of
-- recall_event_history (6c.1), which already excludes re-baselines + cosmetic noise. Returning
-- rows = transitions exist → graduate the designation in scd_field_designations.md. ~0
-- post-reseed; accrues forward. severity=warn via dbt_project.yml. Forensic version:
-- scripts/sql/cross_source/scd_monitors/assert_lifecycle_stable.sql.

select source, source_recall_id, langcode, old_value, new_value, changed_at
from {{ ref('recall_event_history') }}
where field_name = 'lifecycle_status'
