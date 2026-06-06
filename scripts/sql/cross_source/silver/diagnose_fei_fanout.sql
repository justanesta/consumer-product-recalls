\set ON_ERROR_STOP on
set client_min_messages = warning;
\pset null '<NULL>'
\echo '=== FEI fan-out diagnosis (Phase 6b PR 6b.4 — why fei_must_link builds mega-hubs) ==='
-- The 6b.4 RapidFuzz pass treats a shared firm_fei_num (and firm_surviving_fei succession)
-- as an UNCONDITIONAL same-firm must-link. But an FEI is a FACILITY id: a contract
-- manufacturer / packer / parent registrant can carry ONE FEI across many UNRELATED brand
-- names, and a sentinel/placeholder FEI (or surviving-FEI sink) can be shared by hundreds.
-- This query finds the FEIs whose distinct-name fan-out is too large to be one firm — the
-- seeds of the Whole-Foods=Stryker / Teva=Bayer hubs. Use it to size the fan-out gate.
--
-- Run: psql -f scripts/sql/cross_source/silver/diagnose_fei_fanout.sql

\echo ''
\echo '=== Q1: FEIs by distinct-name fan-out (a firm should be ~1-5; >10 is a facility/sentinel) ==='
with per_fei as (
  select firm_fei_num, count(distinct normalized_name) as n_names
  from firm_fei_edges
  group by firm_fei_num
)
select
  count(*)                                   as total_feis,
  count(*) filter (where n_names = 1)        as singletons,
  count(*) filter (where n_names between 2 and 5)  as small_2_5,
  count(*) filter (where n_names between 6 and 10) as mid_6_10,
  count(*) filter (where n_names > 10)       as big_gt10,
  max(n_names)                               as worst_fanout
from per_fei;

\echo ''
\echo '=== Q2: the 30 worst FEIs (largest distinct-name fan-out) — eyeball for unrelated brands ==='
select
  firm_fei_num,
  count(distinct normalized_name) as n_names,
  left(string_agg(distinct normalized_name, ' | ' order by normalized_name), 150) as names
from firm_fei_edges
group by firm_fei_num
having count(distinct normalized_name) > 5
order by n_names desc
limit 30;

\echo ''
\echo '=== Q3: succession sinks — surviving_fei values that absorb many distinct FEIs ==='
select
  firm_surviving_fei,
  count(distinct firm_fei_num)    as n_source_feis,
  count(distinct normalized_name) as n_names
from firm_fei_edges
where firm_surviving_fei is not null
group by firm_surviving_fei
having count(distinct firm_fei_num) > 3
order by n_source_feis desc
limit 30;

\echo ''
\echo '=== Q4: suspicious sentinel FEI values (short / low-digit placeholders) ==='
select firm_fei_num, count(distinct normalized_name) as n_names
from firm_fei_edges
where length(firm_fei_num) <= 4 or firm_fei_num ~ '^0+$' or firm_fei_num in ('0','1','9','99','999','9999')
group by firm_fei_num
order by n_names desc
limit 20;

\echo ''
\echo '=== Full FEI fan-out (n_names >= 2) -> data/exploratory/cross_source/fei_fanout.csv ==='
\pset format csv
\o data/exploratory/cross_source/fei_fanout.csv
select
  firm_fei_num,
  count(distinct normalized_name) as n_names,
  string_agg(distinct normalized_name, ' | ' order by normalized_name) as names
from firm_fei_edges
group by firm_fei_num
having count(distinct normalized_name) >= 2
order by n_names desc;
\o
\pset format aligned
\echo '   done — Read data/exploratory/cross_source/fei_fanout.csv'
