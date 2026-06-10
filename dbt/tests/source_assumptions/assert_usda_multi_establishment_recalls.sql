-- Monitor (severity=warn, inherited from the source_assumptions/ directory in dbt_project.yml):
-- USDA recalls that name 2+ establishments. These do NOT establishment-resolve today — the
-- resolution model (recall_event_establishment_resolution) picks ONE establishment per recall,
-- and a multi-establishment recall genuinely has several (see recall_api_observations.md
-- Finding S / phase-7 C4 for the full grain-mismatch reasoning).
--
-- BASELINE = 4 (2026-06-09). A MATERIAL increase is the signal that multi-establishment recalls
-- have stopped being a rounding error and the per-element / grain-change resolution work is worth
-- revisiting (it would change recall_event_establishment_resolution's grain + recall_event_firm's
-- 1:1 join to 1:many + re-calibrate the signal hierarchy). Examine the population with
-- scripts/sql/usda_recalls/silver/inspect_multi_establishment_recalls.sql.
--
-- jsonb_typeof guard: migration 0028 made `establishment` jsonb; the guard keeps the length call
-- safe against any non-array replay row (a wrapped scalar is a 1-element array → excluded anyway).
select
    trim(source_recall_id)            as source_recall_id,
    jsonb_array_length(establishment) as n_establishments
from {{ source('usda', 'usda_fsis_recalls_bronze') }}
where langcode = 'English'
  and establishment is not null
  and jsonb_typeof(establishment) = 'array'
  and jsonb_array_length(establishment) > 1
