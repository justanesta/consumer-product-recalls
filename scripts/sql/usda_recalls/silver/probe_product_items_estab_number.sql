-- Gate U2 (6b.2) — Signal 1: FSIS establishment number embedded in `product_items`.
--
-- PURPOSE: for the recalls that FAN OUT (establishment name -> 2+ establishments, per U1), the
-- strongest disambiguation lever is the FSIS number printed on the label and echoed in
-- `product_items` ("...EST. P-46712...", "establishment number M46712"). FSIS labeling regs
-- require it, so coverage should be meaningful. This measures how often a regex-extracted number
-- resolves the fan-out to EXACTLY ONE candidate, and DUMPS the real text so the production regex
-- is designed from corpus evidence, not a guess.
--
-- Matching: extracted tokens and each candidate's establishment_number are normalized
-- (upper, strip non-alphanumerics) and the composite ('M46712+P46712') is SPLIT on '+', so an
-- embedded single grant matches its composite parent (audit §9: Signal 1 must split the composite).
--
-- Feeds: project_scope/phase-6-execution-plan.md PR 6b.2 gate G3 (Signal-1 coverage + the regex).
-- Run with: psql ... -f scripts/sql/usda_recalls/silver/probe_product_items_estab_number.sql

\set ON_ERROR_STOP on
\pset null '<NULL>'

-- Fan-out recalls (name -> >=2 establishments) once; reused below.
drop table if exists _fanout;
create temporary table _fanout as
select r.source_recall_id, r.establishment, r.product_items,
       (select count(*) from firm_usda_attributes e
        where upper(trim(e.establishment_name)) = upper(trim(r.establishment))) as n_candidates
from stg_usda_fsis_recalls r
where r.establishment is not null
  and (select count(*) from firm_usda_attributes e
       where upper(trim(e.establishment_name)) = upper(trim(r.establishment))) >= 2;

-- Normalized FSIS tokens extracted from product_items (prefixed M/P/I/G/V form).
drop table if exists _extracted;
create temporary table _extracted as
select f.source_recall_id,
       array_agg(distinct regexp_replace(upper(m[1]), '[^A-Z0-9]', '', 'g')) as toks
from _fanout f,
     lateral regexp_matches(coalesce(f.product_items, ''), '[MPIGV]-? ?[0-9]{2,6}[A-Z]?', 'gi') as m
group by f.source_recall_id;

-- Per fan-out candidate: its grant set (composite split + normalized) and whether any grant is in
-- the recall's extracted tokens.
drop table if exists _candmatch;
create temporary table _candmatch as
select c.source_recall_id, c.establishment_id,
       coalesce((select x.toks && c.grants from _extracted x where x.source_recall_id = c.source_recall_id), false) as is_matched
from (
  select f.source_recall_id, e.establishment_id,
         array_agg(distinct regexp_replace(upper(g), '[^A-Z0-9]', '', 'g')) as grants
  from _fanout f
  join firm_usda_attributes e on upper(trim(e.establishment_name)) = upper(trim(f.establishment)),
       lateral unnest(string_to_array(e.establishment_id, '+')) as g
  group by f.source_recall_id, e.establishment_id
) c;

\echo '=== Q1: Signal-1 resolution of the fan-out set ==='
-- signal1_resolves_one = the win (one candidate carries the embedded number). still_ambiguous =
-- 2+ candidates match (composite overlap); no_match = no embedded number found among candidates.
with res as (
  select f.source_recall_id,
         f.product_items is not null as has_pi,
         (select count(*) filter (where m.is_matched) from _candmatch m where m.source_recall_id = f.source_recall_id) as n_match
  from _fanout f
)
select
  count(*)                                          as fanout_recalls,
  count(*) filter (where has_pi)                    as with_product_items,
  count(*) filter (where n_match = 1)               as signal1_resolves_one,
  count(*) filter (where n_match >= 2)              as signal1_still_ambiguous,
  count(*) filter (where n_match = 0)               as signal1_no_match,
  round(100.0 * count(*) filter (where n_match = 1) / nullif(count(*), 0), 1) as pct_resolved_by_signal1
from res;

\echo ''
\echo '=== Q2: DUMP every fan-out recall + its Signal-1 evidence to data/exploratory/usda_recalls/ ==='
-- For holistic regex design: the extracted tokens, the candidate composite numbers, the match
-- count, and a product_items snippet. Inspect the no_match rows for number formats the regex missed
-- ("EST. 17564" bare digits, "Est # 46712", etc.).
\pset format csv
\pset footer off
\o data/exploratory/usda_recalls/u2_signal1_fanout.csv
select
  f.source_recall_id,
  f.establishment,
  f.n_candidates,
  (select count(*) filter (where m.is_matched) from _candmatch m where m.source_recall_id = f.source_recall_id) as n_matching_candidates,
  (select x.toks from _extracted x where x.source_recall_id = f.source_recall_id) as extracted_tokens,
  (select array_agg(e.establishment_id order by e.establishment_id)
     from firm_usda_attributes e
     where upper(trim(e.establishment_name)) = upper(trim(f.establishment)))    as candidate_numbers,
  left(f.product_items, 240)                                                    as product_items_head
from _fanout f
order by n_matching_candidates, f.n_candidates desc, f.source_recall_id;
\o
\pset format aligned
\pset footer on
\echo 'Wrote data/exploratory/usda_recalls/u2_signal1_fanout.csv'
