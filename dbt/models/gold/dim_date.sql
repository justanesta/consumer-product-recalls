-- dim_date — generated calendar dimension (ADR 0038 §2 reserves the `dim_` prefix; decided
-- 2026-06-08 to build pre-Phase-8 regardless of the star call). Replaces the inline
-- `date_trunc(published_at)` repeated across the five date-grained `fct_*` models (wiring is
-- the separate C11 step) and unlocks fiscal/holiday calendars cheaply.
--
-- Spine 1970-01-01 .. 2035-12-31 covers the CPSC 1970 floor through a comfortable forward
-- buffer. Grain = one row per calendar day; `date_day` is the unique key.
--
-- NOTE (morning dbt verification): `dbt_utils.date_spine` emits its column as `date_day` for
-- datepart="day" — confirm on first `dbt build --select dim_date`.
{{
  config(
    materialized='table',
    indexes=[{'columns': ['date_day'], 'unique': True}]
  )
}}

with spine as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('1970-01-01' as date)",
        end_date="cast('2036-01-01' as date)"
    ) }}
)

select
    date_day::date                                          as date_day,
    extract(year    from date_day)::int                     as year,
    extract(quarter from date_day)::int                     as quarter,
    extract(month   from date_day)::int                     as month,
    trim(to_char(date_day, 'Month'))                        as month_name,
    extract(week    from date_day)::int                     as iso_week,
    extract(isodow  from date_day)::int                     as iso_day_of_week,
    trim(to_char(date_day, 'Day'))                          as day_name,
    extract(doy     from date_day)::int                     as day_of_year,
    date_trunc('week',    date_day)::date                   as iso_week_start,
    date_trunc('month',   date_day)::date                   as month_start,
    date_trunc('quarter', date_day)::date                   as quarter_start,
    date_trunc('year',    date_day)::date                   as year_start,
    (extract(isodow from date_day) in (6, 7))               as is_weekend,
    -- US federal fiscal year: starts Oct 1, labelled by the calendar year it ends in.
    case
        when extract(month from date_day) >= 10
            then extract(year from date_day)::int + 1
        else extract(year from date_day)::int
    end                                                     as us_fiscal_year
from spine
