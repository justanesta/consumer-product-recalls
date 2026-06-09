-- The presence dims (is_currently_active / was_ever_retracted) come ONLY from the track_presence
-- sources — USDA + NHTSA (C16, ADR 0026 / Phase 6c.2). Any CPSC/FDA/USCG row carrying a non-NULL
-- presence value means a source's manifest was wired without enabling track_presence (or the
-- model's source-scoped join guard regressed). Returns offending rows; expected empty. (NHTSA
-- presence itself is NULL until a full-enumerating deep-rescan banks its first complete manifest —
-- recall_lifecycle gates it to historical_seed runs — so this stays green pre- and post-H-b.)

select recall_event_id, source, is_currently_active, was_ever_retracted
from {{ ref('recall_lifecycle') }}
where source not in ('USDA', 'NHTSA')
  and (is_currently_active is not null or was_ever_retracted is not null)
