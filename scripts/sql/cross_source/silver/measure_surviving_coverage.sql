\set ON_ERROR_STOP on
set client_min_messages = warning;
\pset null '<NULL>'
\echo '=== FDA surviving-name/FEI coverage (Phase 6b PR 6b.4 — can the cross-corporate split be automatic?) ==='
-- The Tier-0 FEI mega-clusters (Biomat, BPL, ICU) chain ACROSS corporate boundaries via a shared
-- name node. Candidate AUTO-FIX: split a component when its members' CURRENT identity
-- (coalesce(firm_surviving_nam, firm_legal_nam)) spans >1 firm. That only works if FDA's surviving
-- fields are populated densely enough to collapse old names onto the current owner — AND, crucially,
-- densely on the firms that actually CHANGED hands (the consolidations), not just overall. This
-- sizes both, and dumps the rows so the component test can run offline.
--
-- Run: psql -f scripts/sql/cross_source/silver/measure_surviving_coverage.sql

\echo ''
\echo '=== Q1: overall surviving coverage (FDA firm-rows + distinct firm names) ==='
select
    count(*)                                                                 as fda_firm_rows,
    count(*) filter (where nullif(trim(firm_surviving_nam), '') is not null) as rows_surv_name,
    round(
        100.0 * count(*) filter (where nullif(trim(firm_surviving_nam), '') is not null) / count(*), 1
    ) as pct_rows_surv_name,
    count(*) filter (where nullif(firm_surviving_fei::text, '') is not null) as rows_surv_fei,
    round(
        100.0 * count(*) filter (where nullif(firm_surviving_fei::text, '') is not null) / count(*), 1
    ) as pct_rows_surv_fei,
    count(distinct upper(trim(firm_legal_nam)))                              as distinct_names
from stg_fda_recalls
where firm_legal_nam is not null and trim(firm_legal_nam) <> '' and firm_fei_num is not null;

\echo ''
\echo '=== Q2: do the high-fan-out BRIDGE names carry surviving info? (the names that actually matter) ==='
with fda as (
    select
        upper(trim(firm_legal_nam))            as legal_nam,
        firm_fei_num::text                     as fei,
        nullif(trim(firm_surviving_nam), '')   as surviving_nam
    from stg_fda_recalls
    where firm_legal_nam is not null and trim(firm_legal_nam) <> '' and firm_fei_num is not null
),
fanout as (
    select
        legal_nam,
        count(distinct fei)                                as n_feis,
        count(*)                                           as rows,
        count(*) filter (where surviving_nam is not null)  as rows_with_surv
    from fda
    group by legal_nam
)
select
    legal_nam, n_feis, rows, rows_with_surv,
    case when rows_with_surv > 0 then 'has surviving' else 'NO surviving' end as flag
from fanout
where n_feis >= 5
order by n_feis desc
limit 30;

\echo ''
\echo '=== Full FDA firm-identity rows -> data/exploratory/cross_source/fda_surviving.csv (offline component test) ==='
\pset format csv
\o data/exploratory/cross_source/fda_surviving.csv
select distinct
    upper(trim(firm_legal_nam))                                        as legal_nam,
    firm_fei_num::text                                                 as fei,
    nullif(firm_surviving_fei::text, '')                               as surviving_fei,
    nullif(trim(firm_surviving_nam), '')                               as surviving_nam,
    coalesce(nullif(firm_surviving_fei::text, ''), firm_fei_num::text) as current_fei,
    upper(trim(coalesce(nullif(trim(firm_surviving_nam), ''), firm_legal_nam))) as current_name
from stg_fda_recalls
where firm_legal_nam is not null and trim(firm_legal_nam) <> '' and firm_fei_num is not null;
\o
\pset format aligned
\echo '   done — Read data/exploratory/cross_source/fda_surviving.csv'
