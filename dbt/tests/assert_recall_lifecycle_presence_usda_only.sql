-- The presence dims (is_currently_active / was_ever_retracted) are USDA-only in v1 — the only
-- track_presence source (ADR 0026 / Phase 6c.2). Any non-USDA row carrying a non-NULL presence
-- value means a source's manifest was wired without enabling track_presence (or the model's
-- USDA-only join guard regressed). Returns offending rows; expected empty. When another source's
-- track_presence lands, relax this to that source set.

select recall_event_id, source, is_currently_active, was_ever_retracted
from {{ ref('recall_lifecycle') }}
where source <> 'USDA'
  and (is_currently_active is not null or was_ever_retracted is not null)
