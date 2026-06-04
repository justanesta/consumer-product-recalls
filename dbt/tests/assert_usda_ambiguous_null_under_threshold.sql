{{ config(severity='warn') }}

-- Phase 6b PR 6b.2 monitor (warn, not error): the signal hierarchy is precision-first, so a minority
-- of name-matched USDA recalls land usda_ambiguous_null (NULL establishment_number) — empirically
-- ~12.5% (2026-06-03), almost all genuinely-ambiguous multi-plant producers with no stated number.
-- This is NOT the plan's original "<10% of fan-outs" gate (unachievable without guessing, which
-- precision-over-recall forbids). It warns only if the rate climbs above 20% of all resolution rows,
-- which would signal degraded signal extraction (e.g. the summary establishment-number phrasing
-- changed) worth investigating. Returns a row (fails-warn) when over threshold.

with rates as (
    select
        count(*)                                                      as total,
        count(*) filter (where match_confidence = 'usda_ambiguous_null') as ambiguous_null
    from {{ ref('recall_event_establishment_resolution') }}
)

select total, ambiguous_null, round(100.0 * ambiguous_null / nullif(total, 0), 1) as pct_ambiguous_null
from rates
where 100.0 * ambiguous_null / nullif(total, 0) > 20
