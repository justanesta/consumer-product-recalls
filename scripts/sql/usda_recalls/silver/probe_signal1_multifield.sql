-- Probe A (6b.2) — Signal 1 re-measured: FSIS establishment number across product_items + summary
-- + labels, format-robust (prefix-before / letter-after), full-grant-token match (composite split).
--
-- WHY: U2 found only ~2% of fan-out recalls carry a matchable number in product_items with a
-- prefix-before regex. But (a) FSIS writes the number bare / prefix-before / letter-after
-- ("19924", "P9002", "19924 M" — per the API data doc), and (b) "establishment numbers are found on
-- package labels", so the recall notice often states it in `summary` or `labels`, not
-- `product_items`. This re-measures over ALL three text fields, matching the FULL grant token
-- (prefix+number+suffix; M1234A != M1234B) against the candidate's '+'-split composite — the correct
-- high-precision form. Q2 reports a separate lower-precision bare-number-in-est-context tier.
--
-- Feeds: project_scope/phase-6-execution-plan.md PR 6b.2 gate G3 (the real Signal-1 power + the regex).
-- Run with: psql ... -f scripts/sql/usda_recalls/silver/probe_signal1_multifield.sql

\set ON_ERROR_STOP on
\pset null '<NULL>'
-- Silence the harmless "table does not exist, skipping" NOTICEs from DROP TABLE IF EXISTS on first run.
set client_min_messages = warning;

drop table if exists _fo;
create temporary table _fo as
select r.source_recall_id, r.establishment,
       coalesce(r.product_items, '') as product_items,
       coalesce(r.summary, '')       as summary,
       coalesce(r.labels, '')        as labels,
       coalesce(r.product_items,'') || '  ' || coalesce(r.summary,'') || '  ' || coalesce(r.labels,'') as alltext
from stg_usda_fsis_recalls r
where r.establishment is not null
  and (select count(*) from firm_usda_attributes e
       where upper(trim(e.establishment_name)) = upper(trim(r.establishment))) >= 2;

-- STRICT: grant tokens (prefix-before OR letter-after) normalized to canonical LETTER+NUMBER+SUFFIX.
drop table if exists _ext;
create temporary table _ext as
select source_recall_id, array_agg(distinct norm_tok) as toks
from (
  select f.source_recall_id,
         regexp_replace(
           regexp_replace(upper(m[1]), '^([0-9]+)[ .-]*([MPGIV])$', '\2\1'),  -- "19924 M" -> "M19924"
           '[^A-Z0-9]', '', 'g'
         ) as norm_tok
  from _fo f,
       lateral regexp_matches(f.alltext, '[MPGIV][ .-]?[0-9]{2,6}[A-Z]?|[0-9]{2,6}[ .-]?[MPGIV]', 'gi') as m
) z
where norm_tok ~ '^[MPGIV][0-9]{2,6}[A-Z]?$'
group by source_recall_id;

-- Candidate grant sets (composite split + normalized) and numeric cores (for the bare tier).
drop table if exists _cand;
create temporary table _cand as
select f.source_recall_id, e.establishment_id,
       array_agg(distinct regexp_replace(upper(g), '[^A-Z0-9]', '', 'g')) as grants,
       array_agg(distinct regexp_replace(g, '[^0-9]', '', 'g'))           as cores
from _fo f
join firm_usda_attributes e on upper(trim(e.establishment_name)) = upper(trim(f.establishment)),
     lateral unnest(string_to_array(e.establishment_id, '+')) as g
group by f.source_recall_id, e.establishment_id;

-- Bare numbers in est-context (lower-precision tier).
drop table if exists _bare;
create temporary table _bare as
select f.source_recall_id, array_agg(distinct m[1]) as nums
from _fo f,
     lateral regexp_matches(f.alltext, '(?:establishment|est\.?|inspection)[^0-9a-z]{0,15}([0-9]{2,6})', 'gi') as m
group by f.source_recall_id;

\echo '=== Q1: STRICT Signal-1 resolution over product_items + summary + labels ==='
with mt as (
  select c.source_recall_id,
         count(*) filter (
           where exists (select 1 from _ext x where x.source_recall_id = c.source_recall_id and x.toks && c.grants)
         ) as n_matching
  from _cand c group by c.source_recall_id
)
select
  count(*)                                  as fanout_recalls,
  count(*) filter (where n_matching = 1)    as resolves_one,
  count(*) filter (where n_matching >= 2)   as still_ambiguous,
  count(*) filter (where n_matching = 0)    as no_match,
  round(100.0 * count(*) filter (where n_matching = 1) / nullif(count(*), 0), 1) as pct_resolved
from mt;

\echo ''
\echo '=== Q2: BARE-number tier (est-context digits -> candidate numeric core; lower precision) ==='
with bm as (
  select c.source_recall_id, c.establishment_id,
         coalesce((select b.nums && c.cores from _bare b where b.source_recall_id = c.source_recall_id), false) as hit
  from _cand c
),
per as (select source_recall_id, count(*) filter (where hit) as n from bm group by source_recall_id)
select
  count(*) filter (where n = 1)  as bare_resolves_one,
  count(*) filter (where n >= 2) as bare_still_ambiguous
from per;

\echo ''
\echo '=== Q3: DUMP fan-out recalls + Signal-1 evidence (summary + product_items heads) to data/exploratory/ ==='
\pset format csv
\pset footer off
\o data/exploratory/usda_recalls/u_signal1_multifield.csv
with mt as (
  select c.source_recall_id,
         count(*) filter (
           where exists (select 1 from _ext x where x.source_recall_id = c.source_recall_id and x.toks && c.grants)
         ) as n_matching
  from _cand c group by c.source_recall_id
)
select
  f.source_recall_id,
  f.establishment,
  mt.n_matching,
  (select x.toks from _ext x where x.source_recall_id = f.source_recall_id)                       as extracted_tokens,
  (select array_agg(e.establishment_id order by e.establishment_id)
     from firm_usda_attributes e where upper(trim(e.establishment_name)) = upper(trim(f.establishment))) as candidate_numbers,
  left(f.summary, 200)        as summary_head,
  left(f.product_items, 160)  as product_items_head
from _fo f join mt on mt.source_recall_id = f.source_recall_id
order by mt.n_matching desc, f.source_recall_id;
\o
\pset format aligned
\pset footer on
\echo 'Wrote data/exploratory/usda_recalls/u_signal1_multifield.csv'
