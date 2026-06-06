\set ON_ERROR_STOP on
\echo '=== G8: maketxt space+case collapse safety (Phase 6b PR 6b.3) ==='
-- Gates the AC DELCO product-level fix: normalizing maketxt as
-- regexp_replace(upper(trim(maketxt)),'\s+','','g') at BOTH the stg_nhtsa_recalls
-- identity partition AND the recall_product_id md5. Confirm the normalization only
-- merges spelling variants of the SAME make (safe) and size the de-fragmentation.
-- Run: psql "$NEON_DATABASE_URL" -f scripts/sql/nhtsa/bronze/probe_maketxt_space_collapse_safety.sql

\echo '=== Q1: distinct maketxt, raw vs normalized ==='
-- spellings_collapsed = how many raw maketxt spellings fold into a normalized make.
-- 0 -> the fix is purely preventive on the current corpus (no rows merge).
select
  count(distinct maketxt)                                              as distinct_raw,
  count(distinct regexp_replace(upper(trim(maketxt)), '\s+', '', 'g')) as distinct_normalized,
  count(distinct maketxt)
    - count(distinct regexp_replace(upper(trim(maketxt)), '\s+', '', 'g')) as spellings_collapsed
from nhtsa_recalls_bronze
where maketxt is not null and trim(maketxt) <> '';

\echo ''
\echo '=== Q2: normalized makes covering >1 raw spelling — the SAFETY check ==='
-- Each row groups the raw spellings that fold to one normalized make. EYEBALL every
-- group: all spellings must be the SAME make ("AC DELCO | ACDELCO", "LAND ROVER |
-- LANDROVER"). A group mixing GENUINELY DIFFERENT makes is an over-merge -> abandon the
-- blanket normalize and use a targeted alias map (AC DELCO only) instead.
select
  regexp_replace(upper(trim(maketxt)), '\s+', '', 'g') as make_normalized,
  count(distinct maketxt)                              as n_raw_spellings,
  string_agg(distinct maketxt, ' | ' order by maketxt) as raw_spellings
from nhtsa_recalls_bronze
where maketxt is not null and trim(maketxt) <> ''
group by 1
having count(distinct maketxt) > 1
order by n_raw_spellings desc, make_normalized;

\echo ''
\echo '=== Q3: identity-grain fragmentation fixed — the unique-test blast radius ==='
-- How many (campno + the other 9 identity fields + normalized make) groups currently
-- hold >1 row differing ONLY by a maketxt that normalizes to the same string? These are
-- the rows the staging partition will collapse N -> 1 (latest-wins) — and the rows that
-- would otherwise DUPLICATE recall_product_id under an md5-only fix (breaking the unique
-- test). 0 here -> no current fragmentation -> md5-only would NOT have broken unique on
-- today's slice, but the staging normalize stays (preventive + invariant id).
select
  count(*)                          as identity_groups_collapsing,
  coalesce(sum(n_rows_over_one), 0) as rows_removed_by_collapse
from (
  select
    count(distinct maketxt) - 1 as n_rows_over_one
  from nhtsa_recalls_bronze
  where maketxt is not null and trim(maketxt) <> ''
  group by
    campno,
    regexp_replace(upper(trim(maketxt)), '\s+', '', 'g'),
    modeltxt, yeartxt, compname, rcl_cmpt_id, mfr_comp_ptno,
    mfr_comp_desc, mfr_comp_name, endman, bgman
  having count(distinct maketxt) > 1
) g;

\echo ''
\echo '=== Q4: the documented AC DELCO case (campno 22E002000) ==='
-- May return 0 rows: 22E002000 is a 2022 equipment recall that may sit OUTSIDE the
-- current --since=2023-12-01 bronze slice (it was caught via full-TSV cross-corpus
-- analysis, not the bronze slice). Empty here does NOT invalidate the fix — it lands as
-- a Tier-1 prevention enhancement that activates for this case at the 6a.5 full seed.
select
  campno,
  maketxt,
  regexp_replace(upper(trim(maketxt)), '\s+', '', 'g') as make_normalized,
  count(*) as bronze_rows
from nhtsa_recalls_bronze
where campno = '22E002000'
group by campno, maketxt
order by maketxt;

\echo ''
\echo '=== Q5: AGGRESSIVE (alphanumeric-only) preview — future-upgrade visibility, NOT shipped ==='
-- The shipped fix (normalize_maketxt macro) is the CONSERVATIVE whitespace+case collapse
-- (provably over-merge-safe). This previews what a MORE aggressive alphanumeric-only normalize
-- would ADDITIONALLY merge — i.e. punctuation drift ('MERCEDES-BENZ' vs 'MERCEDES BENZ') that
-- the conservative form leaves split. EYEBALL: if these groups are same-make and numerous, a
-- future macro upgrade is worth it; if ANY group mixes genuinely distinct makes, aggressive is
-- unsafe and the conservative macro + the assert_nhtsa_maketxt_drift_caught monitor stay the
-- answer. (The monitor is the PERMANENT version of this probe — it watches new drift forever.)
select
  regexp_replace(upper(maketxt), '[^A-Z0-9]', '', 'g')   as make_alnum,
  count(distinct maketxt)                                as n_raw_spellings,
  string_agg(distinct maketxt, ' | ' order by maketxt)   as raw_spellings
from nhtsa_recalls_bronze
where maketxt is not null and trim(maketxt) <> ''
group by 1
having count(distinct regexp_replace(upper(trim(maketxt)), '\s+', '', 'g')) > 1
order by n_raw_spellings desc, make_alnum
limit 50;
