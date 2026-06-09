{{ config(materialized='view') }}

-- Units recalled per source × month (Phase 6e, ADR 0038). ONLY the two clean-count sources —
-- NHTSA (vehicles potentially affected) + USCG (boats). CPSC/FDA/USDA units are free-text /
-- weights (USDA = pounds), so they have no clean integer unit_count and need the Tier-2
-- value+unit parse (the units-enrichment workstream, TODO.md) before they are aggregatable.
--
-- NOT cross-source comparable (vehicles != boats) — there is deliberately NO 'ALL' rollup; always
-- filter by source.
--
-- GRAIN TRAP (the reason this isn't a one-liner): NHTSA potaff repeats across a campaign's
-- make/model/component product rows, so a naive sum over recall_product overcounts ~100x (96.5B).
-- We collapse to ONE count per recall_event (max unit_count) first. Verified 2026-06-07: potaff is
-- CONSTANT across every campaign's component rows (30,045/30,045 = 100%), so max is EXACT, not a
-- heuristic; and USCG is 1 product/recall (product-grain sum == event-grain sum — no explosion).
-- total_units is a SUM OF PER-RECALL AFFECTED COUNTS — a recall-magnitude measure, NOT unique
-- vehicles (a vehicle recurs across recalls — Takata is in many). The deferred build is the
-- free-text value+unit parse for CPSC/FDA/USDA (TODO.md units enrichment), not anything NHTSA-side.
with recall_units as (
    select
        re.source,
        re.recall_event_id,
        re.published_at,
        max(rp.unit_count) as units
    from {{ ref('recall_event') }} re
    join {{ ref('recall_product') }} rp on rp.recall_event_id = re.recall_event_id
    where re.source in ('NHTSA', 'USCG')
      and rp.unit_count is not null
    group by re.source, re.recall_event_id, re.published_at
)

-- C11 (2026-06-09): month period from dim_date (lossless join on published_at::date).
select
    ru.source,
    dd.month_start         as period,
    count(*)               as recalls_with_units,
    sum(ru.units)          as total_units,
    round(avg(ru.units))   as avg_units_per_recall,
    max(ru.units)          as max_units
from recall_units ru
join {{ ref('dim_date') }} dd on dd.date_day = ru.published_at::date
group by ru.source, dd.month_start
order by period desc, source
