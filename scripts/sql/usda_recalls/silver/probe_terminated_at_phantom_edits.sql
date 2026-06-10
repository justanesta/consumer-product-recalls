-- C6 verification (check-your-work sweep, 2026-06-09): did the USDA closed_date -> NULL transition
-- (the 2026-06 FSIS API change that dropped field_closed_date) synthesize PHANTOM terminated_at
-- edits in recall_event_history? recall_event_history.sql:87 still reads cast(closed_date as text)
-- as terminated_at, terminated_at is a tracked change-field, and a phantom date->NULL edit is
-- suppressed ONLY if the re-extract that nulled closed_date ran under change_type in
-- (schema_rebaseline, hash_helper_rebaseline) — the plan C6 mirror + confirmation were never done.
--
-- EXPECT 0 rows. Any rows with change_type='routine' = live phantom edits in the history lens
-- (the C6 fix is then needed: re-stamp the recovery extract OR make terminated_at closed_year-aware).
select
    change_type,
    count(*) as phantom_terminated_at_edits
from recall_event_history
where source = 'USDA'
  and field_name = 'terminated_at'
  and new_value is null
  and old_value is not null
group by change_type
order by phantom_terminated_at_edits desc;
