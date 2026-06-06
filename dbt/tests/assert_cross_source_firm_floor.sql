{{ config(severity='warn') }}

-- 6b.6 cross-source unification monitor (the headline firm-resolution value). Firms that appear
-- under >=2 sources are the cross-agency dedup payoff — 490 measured (12 spanning 3). A drop below
-- the floor signals a broken/empty crosswalk (cross-source collapse to the ~78 exact-name matches),
-- or a re-key that orphaned the bridge. warn-severity — a quality signal, not a hard gate (the
-- "right" number drifts as the corpus grows). Returns one row when the count is under the floor.
with firm_sources as (
    select
        ref.firm_id,
        count(distinct re.source) as n_src
    from {{ ref('recall_event_firm') }} ref
    join {{ ref('recall_event') }} re using (recall_event_id)
    group by ref.firm_id
)

select count(*) as cross_source_firms
from firm_sources
where n_src >= 2
having count(*) < 350
