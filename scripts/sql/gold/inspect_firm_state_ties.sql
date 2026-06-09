-- C18 blast radius: how often does the most-frequent-registered-state collapse TIE, forcing the
-- deterministic `state_code` tiebreak in fct_recalls_by_geography? A tie = a firm whose top
-- registration frequency is shared by 2+ states. Mirrors firm_states_us -> firm_primary_state.
-- The tiebreak only changes a result when a firm (a) is multi-state AND (b) ties at the top.
with firm_state_freq as (
    select fs.firm_id, fs.state_code, count(*) as n
    from (
        select f.firm_id, upper(trim(s.state_code)) as state_code
        from firm f
        cross join lateral
            jsonb_array_elements_text(coalesce(f.observed_company_ids, '[]'::jsonb)) as cid(company_id)
        cross join lateral (
            select ea.state as state_code from firm_usda_attributes ea where ea.establishment_id = cid.company_id
            union all
            select ma.state from firm_uscg_attributes ma where ma.mic = cid.company_id
            union all
            select fa.firm_state_cd from firm_fda_attributes fa where fa.firm_fei_num::text = cid.company_id
        ) s
        where s.state_code is not null and trim(s.state_code) <> ''
    ) fs
    join us_state_abbr usa on usa.abbr = fs.state_code
    group by fs.firm_id, fs.state_code
),
ranked as (
    select
        firm_id,
        state_code,
        n,
        max(n) over (partition by firm_id)   as top_n,
        count(*) over (partition by firm_id) as n_distinct_states
    from firm_state_freq
),
firm_summary as (
    select
        firm_id,
        max(n_distinct_states)            as n_distinct_states,
        count(*) filter (where n = top_n) as states_at_top_freq
    from ranked
    group by firm_id
)
select
    count(*)                                                          as firms_with_us_state,
    count(*) filter (where n_distinct_states > 1)                     as firms_multistate,
    count(*) filter (where states_at_top_freq > 1)                    as firms_tie_at_top,
    round(100.0 * count(*) filter (where states_at_top_freq > 1)
          / nullif(count(*), 0), 3)                                   as pct_tie_of_all_firms,
    round(100.0 * count(*) filter (where states_at_top_freq > 1)
          / nullif(count(*) filter (where n_distinct_states > 1), 0), 2) as pct_tie_of_multistate,
    max(states_at_top_freq)                                           as max_states_tied
from firm_summary
