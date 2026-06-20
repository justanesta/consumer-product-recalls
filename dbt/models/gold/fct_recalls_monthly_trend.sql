{{ config(materialized='view') }}

-- Monthly recall trend per source with rolling averages + year-over-year change (Phase 6e,
-- ADR 0038 — the window-function showpiece). Built on a DENSE month spine (generate_series over
-- each source's min..max month, 0-filled) so the rolling windows and the lag(12) YoY are over
-- contiguous calendar months, not just months that happened to have a recall.
-- C11 (2026-06-09): the monthly grain comes from dim_date (lossless join), not an inline date_trunc.
-- The per-source dense spine below still uses generate_series (it needs each source's own min..max
-- month, which dim_date doesn't carry).
-- 2026-W25 (fix/announced-at-date-join, ADR 0038 amendment): join key is
-- coalesce(announced_at, published_at)::date — bucket on the announce date, not the publish watermark
-- (see fct_recalls_by_month). NOTE: the dense spine's min..max now spans each source's ANNOUNCE range,
-- so FDA's spine extends back to its earliest initiation date (it was clustered ~2018-09 under
-- published_at); the 0-filled month count grows accordingly — expected, not a defect.
with monthly as (
    select
        dd.month_start                     as month,
        re.source,
        count(distinct re.recall_event_id) as event_count
    from {{ ref('recall_event') }} re
    join {{ ref('dim_date') }} dd on dd.date_day = coalesce(re.announced_at, re.published_at)::date
    group by 1, 2
),

bounds as (
    select source, min(month) as min_m, max(month) as max_m
    from monthly
    group by source
),

spine as (
    select b.source, gs::date as month
    from bounds b,
         generate_series(b.min_m, b.max_m, interval '1 month') as gs
),

dense as (
    select s.source, s.month, coalesce(m.event_count, 0) as event_count
    from spine s
    left join monthly m on m.source = s.source and m.month = s.month
)

select
    month,
    source,
    event_count,
    round(avg(event_count) over w3, 1)                                            as rolling_3mo_avg,
    round(avg(event_count) over w12, 1)                                           as rolling_12mo_avg,
    lag(event_count, 12) over wsrc                                                as event_count_year_ago,
    round(
        100.0 * (event_count - lag(event_count, 12) over wsrc)
        / nullif(lag(event_count, 12) over wsrc, 0), 1
    )                                                                             as yoy_pct_change
from dense
window
    wsrc as (partition by source order by month),
    w3   as (partition by source order by month rows between 2 preceding and current row),
    w12  as (partition by source order by month rows between 11 preceding and current row)
order by source, month
