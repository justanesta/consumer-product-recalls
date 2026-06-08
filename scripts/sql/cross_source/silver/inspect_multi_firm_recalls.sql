\set ON_ERROR_STOP on
set client_min_messages = warning;
\pset null '<NULL>'
\echo '=== Multi-firm recalls: events party to several firms / several roles (recall_event_firm bridge) ==='
-- recall_event_firm is the M:N bridge between a recall and the firms party to it, carrying `role`
-- (manufacturer | importer | distributor | establishment | filer). A single recall can list a
-- manufacturer AND an importer AND a distributor (CPSC), or a filer + manufacturer (NHTSA) — which
-- is the whole reason firm hangs off a bridge rather than a column on recall_event. This script
-- surfaces those fan-outs so you can eyeball the M:N in action.
--
-- Run: psql -f scripts/sql/cross_source/silver/inspect_multi_firm_recalls.sql

\echo ''
\echo '=== Q1: distinct firms / roles per recall (overall shape) ==='
with per_recall as (
    select recall_event_id,
           count(distinct firm_id) as n_firms,
           count(distinct role)    as n_roles
    from recall_event_firm
    group by recall_event_id
)
select
    count(*)                                         as recalls_with_firms,
    count(*) filter (where n_firms = 1)              as one_firm,
    count(*) filter (where n_firms between 2 and 3)  as firms_2_3,
    count(*) filter (where n_firms > 3)              as firms_gt3,
    count(*) filter (where n_roles > 1)              as multi_role,
    max(n_firms)                                     as worst_firm_fanout
from per_recall;

\echo ''
\echo '=== Q2: multi-firm / multi-role recalls by source ==='
with per_recall as (
    select ref.recall_event_id, re.source,
           count(distinct ref.firm_id) as n_firms,
           count(distinct ref.role)    as n_roles
    from recall_event_firm ref
    join recall_event re using (recall_event_id)
    group by ref.recall_event_id, re.source
)
select source,
       count(*)                            as recalls,
       count(*) filter (where n_firms > 1) as multi_firm,
       count(*) filter (where n_roles > 1) as multi_role,
       max(n_firms)                        as worst_firm_fanout
from per_recall
group by source
order by multi_firm desc;

\echo ''
\echo '=== Q3: 25 example multi-ROLE recalls (manufacturer + importer + distributor, etc.) — drill in ==='
with multi as (
    select recall_event_id
    from recall_event_firm
    group by recall_event_id
    having count(distinct role) > 1
    order by count(distinct role) desc, count(distinct firm_id) desc
    limit 25
)
select re.source,
       re.source_recall_id,
       left(re.title, 55)   as title,
       ref.role,
       f.canonical_name     as firm,
       ref.match_confidence
from multi
join recall_event_firm ref using (recall_event_id)
join recall_event re using (recall_event_id)
join firm f on f.firm_id = ref.firm_id
order by re.source, re.source_recall_id, ref.role;

\echo ''
\echo '=== Full multi-firm recalls (n_firms > 1) -> data/exploratory/cross_source/multi_firm_recalls.csv ==='
\pset format csv
\o data/exploratory/cross_source/multi_firm_recalls.csv
with per_recall as (
    select recall_event_id
    from recall_event_firm
    group by recall_event_id
    having count(distinct firm_id) > 1
)
select re.source,
       re.source_recall_id,
       re.title,
       ref.role,
       f.canonical_name        as firm,
       ref.match_confidence,
       ref.establishment_number
from per_recall
join recall_event_firm ref using (recall_event_id)
join recall_event re using (recall_event_id)
join firm f on f.firm_id = ref.firm_id
order by re.source, re.source_recall_id, ref.role, f.canonical_name;
\o
\pset format aligned
\echo '   done — Read data/exploratory/cross_source/multi_firm_recalls.csv'
