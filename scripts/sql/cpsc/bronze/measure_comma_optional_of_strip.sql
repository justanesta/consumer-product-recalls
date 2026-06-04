-- Corpus gate G1 — comma-OPTIONAL vs comma-REQUIRED "of"-strip coverage + false-positive risk.
--
-- PURPOSE: PR 6b.1's clean_firm_name() macro strips a trailing geographic clause from CPSC
-- firm names ("ZOLIQUEX, of China" -> "ZOLIQUEX"). The current comma-REQUIRED pattern
-- (",\s*of\s+...") UNDERCOUNTS the comma-LESS form ("Fisher-Price of East Aurora, N.Y.",
-- "Acme United of Rocky Mount, North Carolina" — a space, not a comma, before "of"). Making
-- the comma OPTIONAL (",?\s*\mof\s+...") catches those, but naive comma-optional also truncates
-- names where "of" is INTEGRAL ("City of Industry", "King of Prussia"), NARRATIVE ("a division
-- of Eaton", "is out of business"), or a BRAND ("Book Club of the Month", "Boy Scouts of
-- America"). This gate measures the headroom AND validates a head-word/narrative BLOCKLIST that
-- separates legit geo strips from those false positives.
--
-- NOTE (2026-06-03, revised): the first cut used a "corporate-form token before of" heuristic —
-- it was WRONG (most brands aren't Inc./LLC, so it mislabeled clean geo strips like "Acme United
-- of Rocky Mount" as false positives). Replaced with the blocklist below, validated against the
-- real Q3 corpus sample: 16/16 true FPs caught, 0/12 legit geo over-blocked. The full-corpus
-- re-run (Q1: 505 would-strip / 45 blocked of the 550 comma-less cohort) then surfaced ONE
-- over-block — "state of" withheld the brand "Altar'd State of Knoxville, Tenn." — so "state" was
-- dropped from the head-word list (it caught only the lone messy "Mexican state of Chihuahua"
-- narrative; net harmful). Final verdict: adopt comma-optional + this blocklist for clean_firm_name.
--
-- BLOCKLIST — the "of" is integral/narrative, NOT a strippable geo suffix. INLINED verbatim in
-- all 3 queries (single backslashes in a single-quoted literal, like the sibling's \y(dba|...)\y;
-- NOT a \set/:'var' — that psql mechanic's backslash handling is untested here):
--   \y(city|king|isle|cape|gulf|division|subsidiary|club|world|month|empire|centre|center|board|
--      department|university|college|institute|bank|house|taste|scouts|county|district)\s+of\y
--   | ,\s*a\s+[a-z][a-z ]{0,25}\s+of\s        (narrative "..., a subsidiary of ...")
--   | \yformerly\s+of\y | \yout\s+of\s+business\y
-- IF YOU EDIT THE BLOCKLIST, edit all three copies identically.
--
-- METHOD: M/I/D firm names from cpsc_recalls_bronze RAW (retailer excluded per §6 Option B; raw
-- read valid under the G1c single-shot confirmation). Two strip ops compared by strip-and-equal:
--   req strip (current):   regexp_replace(name, ',\s*of\s+.*$', '', 'i')      -- comma mandatory
--   opt strip (proposed):  regexp_replace(name, ',?\s*\mof\s+.*$', '', 'i')   -- comma optional,
--     word-bounded \m so glued "of" ("PROFOF") is NOT stripped; ,? consumes the comma.
-- comma_less cohort = names opt strips but req does NOT. The macro would strip comma_less names
-- ONLY when NOT blocklisted; comma_less_would_strip is that safe net, comma_less_blocklisted_fp
-- the excluded FPs.
--
-- EXPECTED SIGNAL: adopt comma-OPTIONAL + the blocklist IFF (Q1 comma_less_would_strip is
-- materially > 0) AND (Q2's would-strip sample is ~all clean geo — zero residual FPs) AND (Q3's
-- blocklisted sample is ~all real FPs — not legit geo being over-blocked). If Q2 still leaks FPs,
-- extend the blocklist; if Q3 over-blocks legit geo, trim it. If the residual won't settle,
-- fall back to comma-REQUIRED only (the 576-firm collapse the sibling already measures).
--
-- Inline CTEs per query (no temp table — psql autocommits, ON COMMIT DROP would drop it before
-- the next read; matches the sibling inspect_firm_name_fragmentation.sql).
--
-- Feeds: project_scope/phase-6b-execution-plan.md PR 6b.1 gate G1 (clean_firm_name regex).
-- Run with: psql ... -f scripts/sql/cpsc/bronze/measure_comma_optional_of_strip.sql

\set ON_ERROR_STOP on
\pset null '<NULL>'

\echo '=== Q1: comma-required vs comma-optional(+blocklist) "of"-strip coverage (M/I/D) ==='
-- comma_less_would_strip = the SAFE net comma-optional+blocklist adds over comma-required.
-- comma_less_blocklisted_fp = the integral/narrative names the blocklist correctly withholds.
with mid_names as (
  select m.value->>'name' as name
  from cpsc_recalls_bronze, jsonb_array_elements(coalesce(manufacturers, '[]'::jsonb)) m
  where nullif(trim(m.value->>'name'), '') is not null
  union all
  select m.value->>'name'
  from cpsc_recalls_bronze, jsonb_array_elements(coalesce(importers, '[]'::jsonb)) m
  where nullif(trim(m.value->>'name'), '') is not null
  union all
  select m.value->>'name'
  from cpsc_recalls_bronze, jsonb_array_elements(coalesce(distributors, '[]'::jsonb)) m
  where nullif(trim(m.value->>'name'), '') is not null
),
flags as (
  select
    name,
    (regexp_replace(name, ',\s*of\s+.*$', '', 'i')   <> name) as req_strips,
    (regexp_replace(name, ',?\s*\mof\s+.*$', '', 'i') <> name) as opt_strips,
    (name ~* '\y(city|king|isle|cape|gulf|division|subsidiary|club|world|month|empire|centre|center|board|department|university|college|institute|bank|house|taste|scouts|county|district)\s+of\y|,\s*a\s+[a-z][a-z ]{0,25}\s+of\s|\yformerly\s+of\y|\yout\s+of\s+business\y')
                                                              as blocklisted
  from mid_names
)
select
  count(distinct name)                                                              as distinct_names,
  count(distinct name) filter (where req_strips)                                    as req_strip_distinct,
  count(distinct name) filter (where opt_strips and not req_strips)                 as comma_less_distinct,
  count(distinct name) filter (where opt_strips and not req_strips and blocklisted)     as comma_less_blocklisted_fp,
  count(distinct name) filter (where opt_strips and not req_strips and not blocklisted)  as comma_less_would_strip
from flags;

\echo ''
\echo '=== Q2: WOULD-STRIP sample — comma-less, NOT blocklisted (the safe geo net; any FP here = concern) ==='
-- This is exactly what comma-optional+blocklist NEWLY strips. Scan for residual false positives
-- (a non-geo tail that slipped past the blocklist) — those would extend the blocklist.
with mid_names as (
  select m.value->>'name' as name
  from cpsc_recalls_bronze, jsonb_array_elements(coalesce(manufacturers, '[]'::jsonb)) m
  where nullif(trim(m.value->>'name'), '') is not null
  union all
  select m.value->>'name'
  from cpsc_recalls_bronze, jsonb_array_elements(coalesce(importers, '[]'::jsonb)) m
  where nullif(trim(m.value->>'name'), '') is not null
  union all
  select m.value->>'name'
  from cpsc_recalls_bronze, jsonb_array_elements(coalesce(distributors, '[]'::jsonb)) m
  where nullif(trim(m.value->>'name'), '') is not null
)
select distinct
  left(name, 70)                                            as name,
  left(regexp_replace(name, ',?\s*\mof\s+.*$', '', 'i'), 50) as opt_stripped
from mid_names
where regexp_replace(name, ',?\s*\mof\s+.*$', '', 'i') <> name   -- opt strips it
  and regexp_replace(name, ',\s*of\s+.*$', '', 'i')    = name   -- req does NOT (comma-less)
  and name !~* '\y(city|king|isle|cape|gulf|division|subsidiary|club|world|month|empire|centre|center|board|department|university|college|institute|bank|house|taste|scouts|county|district)\s+of\y|,\s*a\s+[a-z][a-z ]{0,25}\s+of\s|\yformerly\s+of\y|\yout\s+of\s+business\y'
order by name
limit 100;

\echo ''
\echo '=== Q3: BLOCKLISTED sample — comma-less FPs the blocklist excludes (legit geo here = over-block) ==='
-- Confirm the blocklist is withholding REAL false positives (City of Industry, division of, Boy
-- Scouts of America), not clean geo strips. Any legit "Brand of City, State" here = over-block;
-- trim the offending head word.
with mid_names as (
  select m.value->>'name' as name
  from cpsc_recalls_bronze, jsonb_array_elements(coalesce(manufacturers, '[]'::jsonb)) m
  where nullif(trim(m.value->>'name'), '') is not null
  union all
  select m.value->>'name'
  from cpsc_recalls_bronze, jsonb_array_elements(coalesce(importers, '[]'::jsonb)) m
  where nullif(trim(m.value->>'name'), '') is not null
  union all
  select m.value->>'name'
  from cpsc_recalls_bronze, jsonb_array_elements(coalesce(distributors, '[]'::jsonb)) m
  where nullif(trim(m.value->>'name'), '') is not null
)
select distinct
  left(name, 70)                                            as name,
  left(regexp_replace(name, ',?\s*\mof\s+.*$', '', 'i'), 50) as opt_stripped
from mid_names
where regexp_replace(name, ',?\s*\mof\s+.*$', '', 'i') <> name
  and regexp_replace(name, ',\s*of\s+.*$', '', 'i')    = name
  and name ~* '\y(city|king|isle|cape|gulf|division|subsidiary|club|world|month|empire|centre|center|board|department|university|college|institute|bank|house|taste|scouts|county|district)\s+of\y|,\s*a\s+[a-z][a-z ]{0,25}\s+of\s|\yformerly\s+of\y|\yout\s+of\s+business\y'
order by name
limit 100;
