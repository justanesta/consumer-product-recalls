-- Diagnose violations of the products[] append-only invariant — the drill-down
-- companion to `assert_products_array_append_only.sql` (which only counts them)
-- and to the dbt singular test
-- `dbt/tests/source_assumptions/assert_cpsc_products_array_append_only.sql`
-- (severity=error since the 2026-06-13 ADR 0031 amendment).
--
-- WHEN TO RUN: whenever that dbt test returns > 0 results. The assert file
-- answers "how many"; this file answers "which, why, and does it actually
-- matter" — i.e. whether the hits are a real identity incident or an artifact
-- of how the assertion is phrased.
--
-- ORIGINATING INCIDENT (2026-09-07 → 2026-09-12, transform.yml red for 6 days,
-- "Got 12 results" every run):
--   2026-08-23 05:19  deep-rescan-cpsc  success
--   2026-08-30 10:10  deep-rescan-cpsc  FAILURE   <- a week of rescan skipped
--   2026-09-06 07:23  transform         success   (ran BEFORE the rescan below)
--   2026-09-06 09:01  deep-rescan-cpsc  success   <- 2 weeks of back-dated edits at once
--   2026-09-07 07:39  transform         FAILURE   Got 12 results
-- No dbt/ or CPSC extractor code changed in that window (only the uv.lock dep
-- bumps #129/#130), and the count never moved off 12 while sibling warn-level
-- tests drifted daily — so the trigger was a bronze data event, specifically a
-- deep rescan giving a batch of recalls their second snapshot.
--
-- HYPOTHESIS THIS FILE FALSIFIES (H1) — the dbt test has a false-positive mode.
-- Its predicate is:
--     having count(distinct product_ordinal) > 1
--        and count(distinct raw_landing_path) > 1
-- Those two conditions are evaluated INDEPENDENTLY over the same group. The
-- intent (see assert_products_array_append_only.sql:20-27) is "the ordinal
-- CHANGED BETWEEN snapshots". What the SQL measures is "spans >1 ordinal" AND,
-- separately, "spans >1 snapshot". A recall that lists the same (name, model)
-- TWICE INSIDE ONE ARRAY — which that same header calls legitimate ("CPSC can
-- list the same product variant twice in one response") — satisfies both the
-- moment it acquires a second bronze snapshot, with nothing ever reordered.
--
-- Q2 is the discriminator. Q3 is the invariant silver actually depends on.
--
-- NULL SEMANTICS: Q2-Q5 coalesce name/model to the sentinel '<<NULL>>'.
-- Postgres GROUP BY already collides NULLs into one group, so this is
-- semantically identical to the dbt test's grouping — it just lets the CTEs
-- JOIN on those keys without `is not distinct from` throughout. Q1 is left
-- phrased verbatim as the dbt test so it provably reproduces the same count.
--
-- RESULT COLUMNS:
--   Q2  n_snapshots            distinct raw_landing_path for this (recall, name, model)
--       n_distinct_ordinals    distinct ordinals it occupies across ALL snapshots
--       n_distinct_ordinal_sets  distinct per-snapshot ordinal SETS. 1 = never moved.
--       verdict  STABLE_DUPLICATE  -> every snapshot shows the same ordinal set.
--                                    Nothing moved; the array legitimately lists
--                                    the pair twice. FALSE POSITIVE, data is fine.
--                ORDINALS_CHANGED  -> the set genuinely differs between snapshots.
--                                    Real reorder / mid-array insert / mid-array
--                                    delete = identity-conflation incident.
--   Q3  n_distinct_pairs_in_slot  how many different (name, model) pairs have
--                                 occupied this (recall, ordinal) slot over time.
--                                 CONTEXT ONLY — this is NOT a conflation count.
--                                 It cannot tell a copy-edit of one product from
--                                 a replacement by a different one, and CPSC
--                                 copy-edits names constantly. It duplicates the
--                                 existing severity=warn dbt test
--                                 `assert_cpsc_name_model_normalization_stable`,
--                                 whose header states a returned row is "an
--                                 INFORMATIONAL editorial signal — it no longer
--                                 fragments or re-keys silver". Measured 233 on
--                                 2026-09-12, matching that test's own CI line.
--                                 Recall 00079 — named at recall_product.sql:59
--                                 as the canonical post-publication copy-edit —
--                                 is in the output. Read it as editorial churn.
--   Q6  the LENGTH_REGRESSION half of the replacement predicate. 0 rows = pass.
--   Q7  the ORDINAL_MOVED half of the replacement predicate. 0 rows = pass.
--
-- EXPECTED OUTCOME IF H1 HOLDS: Q1 = the failing count, Q2 verdict is
-- STABLE_DUPLICATE for every row, and Q5 shows products[] length unchanged
-- between the snapshot pair. That combination means nothing was reordered,
-- inserted, or deleted, and the test — not the data — needs the fix. Q3/Q3b are
-- expected to be LARGE and are not evidence either way.
--
-- Q6/Q7 VERIFY THE REPLACEMENT PREDICATE before it is committed to the dbt test.
-- Both must return 0 rows against the live corpus; a non-zero Q7 would be a real
-- reorder/insert/delete the superseded predicate was blind to, and would mean
-- this is an incident after all rather than a test defect.

\set ON_ERROR_STOP on
\pset null '<NULL>'

\echo
\echo '=== Q1: reproduce the dbt test headline verbatim ==='
\echo 'Must match the "Got N results" in the failing transform.yml run. If it does'
\echo 'not, the rest of this file is diagnosing the wrong thing — stop here.'

with exploded as (
    select
        source_recall_id,
        raw_landing_path,
        prod.ordinality::int   as product_ordinal,
        prod.value ->> 'name'  as product_name,
        prod.value ->> 'model' as product_model
    from cpsc_recalls_bronze,
         lateral jsonb_array_elements(coalesce(products, '[]'::jsonb))
             with ordinality as prod(value, ordinality)
)
select count(*) as violating_group_count
from (
    select source_recall_id, product_name, product_model
    from exploded
    group by source_recall_id, product_name, product_model
    having count(distinct product_ordinal) > 1
       and count(distinct raw_landing_path) > 1
) g;

\echo
\echo '=== Q2: THE DISCRIMINATOR — every violation, with its per-snapshot ordinal sets ==='
\echo 'STABLE_DUPLICATE = false positive (within-snapshot duplicate pair, nothing moved).'
\echo 'ORDINALS_CHANGED = real reorder/insert/delete; a recall_product_id was conflated.'

with exploded as (
    select
        source_recall_id,
        raw_landing_path,
        prod.ordinality::int as product_ordinal,
        coalesce(prod.value ->> 'name',  '<<NULL>>') as product_name,
        coalesce(prod.value ->> 'model', '<<NULL>>') as product_model
    from cpsc_recalls_bronze,
         lateral jsonb_array_elements(coalesce(products, '[]'::jsonb))
             with ordinality as prod(value, ordinality)
),

-- the ordinal set this (recall, name, model) occupies WITHIN each single snapshot
per_path as (
    select
        source_recall_id,
        product_name,
        product_model,
        raw_landing_path,
        array_agg(distinct product_ordinal order by product_ordinal) as ordinals_in_snapshot
    from exploded
    group by source_recall_id, product_name, product_model, raw_landing_path
),

overall as (
    select
        source_recall_id,
        product_name,
        product_model,
        count(distinct product_ordinal)  as n_distinct_ordinals,
        count(distinct raw_landing_path) as n_snapshots
    from exploded
    group by source_recall_id, product_name, product_model
    having count(distinct product_ordinal) > 1
       and count(distinct raw_landing_path) > 1
)

select
    o.source_recall_id,
    left(o.product_name, 44)  as product_name,
    left(o.product_model, 22) as product_model,
    o.n_snapshots,
    o.n_distinct_ordinals,
    count(distinct p.ordinals_in_snapshot)                 as n_distinct_ordinal_sets,
    string_agg(distinct p.ordinals_in_snapshot::text, ' , ') as ordinal_sets_seen,
    case
        when count(distinct p.ordinals_in_snapshot) = 1 then 'STABLE_DUPLICATE'
        else 'ORDINALS_CHANGED'
    end as verdict
from overall o
join per_path p
  on  p.source_recall_id = o.source_recall_id
  and p.product_name     = o.product_name
  and p.product_model    = o.product_model
group by o.source_recall_id, o.product_name, o.product_model,
         o.n_snapshots, o.n_distinct_ordinals
order by verdict, o.source_recall_id;

\echo
\echo '=== Q3: slot-content churn — CONTEXT ONLY, NOT a conflation count ==='
\echo 'For a given (recall, ordinal), how many distinct (name, model) pairs has that slot'
\echo 'held across snapshots? This CANNOT distinguish a copy-edit of one product from a'
\echo 'replacement by a different one, and CPSC copy-edits names constantly — so a large'
\echo 'number here is editorial churn, not damage. Duplicates the existing severity=warn'
\echo 'test assert_cpsc_name_model_normalization_stable (233 as of 2026-09-12). Kept here'
\echo 'so the churn rate is visible next to Q2 when triaging.'

with exploded as (
    select
        source_recall_id,
        raw_landing_path,
        prod.ordinality::int as product_ordinal,
        coalesce(prod.value ->> 'name',  '<<NULL>>') as product_name,
        coalesce(prod.value ->> 'model', '<<NULL>>') as product_model
    from cpsc_recalls_bronze,
         lateral jsonb_array_elements(coalesce(products, '[]'::jsonb))
             with ordinality as prod(value, ordinality)
)
select
    source_recall_id,
    product_ordinal,
    count(distinct raw_landing_path)                       as n_snapshots,
    count(distinct product_name || '|@|' || product_model) as n_distinct_pairs_in_slot,
    string_agg(
        distinct left(product_name, 34) || ' /m:' || left(product_model, 14),
        '  ->  ' order by left(product_name, 34) || ' /m:' || left(product_model, 14)
    )                                                      as pairs_seen_in_slot
from exploded
group by source_recall_id, product_ordinal
having count(distinct product_name || '|@|' || product_model) > 1
order by source_recall_id, product_ordinal
limit 60;

\echo
\echo '=== Q3b: slot-churn count (headline for Q3; expected LARGE, tracks editorial churn) ==='

with exploded as (
    select
        source_recall_id,
        prod.ordinality::int as product_ordinal,
        coalesce(prod.value ->> 'name',  '<<NULL>>') as product_name,
        coalesce(prod.value ->> 'model', '<<NULL>>') as product_model
    from cpsc_recalls_bronze,
         lateral jsonb_array_elements(coalesce(products, '[]'::jsonb))
             with ordinality as prod(value, ordinality)
)
select count(*) as conflated_slot_count
from (
    select source_recall_id, product_ordinal
    from exploded
    group by source_recall_id, product_ordinal
    having count(distinct product_name || '|@|' || product_model) > 1
) s;

\echo
\echo '=== Q4: how common are within-snapshot duplicate (name, model) pairs corpus-wide? ==='
\echo 'The population H1 says the test misfires on, split by whether the recall has yet'
\echo 'accumulated the second snapshot the test needs. The single-snapshot row is the'
\echo 'backlog: how many more will fire the next time a deep rescan re-lands them.'

with exploded as (
    select
        source_recall_id,
        raw_landing_path,
        prod.ordinality::int as product_ordinal,
        coalesce(prod.value ->> 'name',  '<<NULL>>') as product_name,
        coalesce(prod.value ->> 'model', '<<NULL>>') as product_model
    from cpsc_recalls_bronze,
         lateral jsonb_array_elements(coalesce(products, '[]'::jsonb))
             with ordinality as prod(value, ordinality)
),
dupes_within_snapshot as (
    select source_recall_id, raw_landing_path, product_name, product_model
    from exploded
    group by source_recall_id, raw_landing_path, product_name, product_model
    having count(*) > 1
),
snap_counts as (
    select source_recall_id, count(distinct raw_landing_path) as n_snapshots
    from exploded
    group by source_recall_id
)
select
    case when s.n_snapshots > 1 then 'multi-snapshot (test CAN fire)'
         else 'single-snapshot (test cannot fire yet)' end as cohort,
    count(distinct d.source_recall_id) as n_recalls_with_dupe_pairs,
    count(*)                           as n_dupe_pair_groups
from dupes_within_snapshot d
join snap_counts s using (source_recall_id)
group by 1
order by 1;

\echo
\echo '=== Q5: snapshot chronology for the affected recalls ==='
\echo 'Confirms which extraction run landed the snapshot that tripped the test, and'
\echo 'whether products[] length changed between snapshots (a length change points at'
\echo 'a real insert/delete rather than the duplicate-pair false positive).'

with exploded as (
    select
        source_recall_id,
        raw_landing_path,
        prod.ordinality::int as product_ordinal,
        coalesce(prod.value ->> 'name',  '<<NULL>>') as product_name,
        coalesce(prod.value ->> 'model', '<<NULL>>') as product_model
    from cpsc_recalls_bronze,
         lateral jsonb_array_elements(coalesce(products, '[]'::jsonb))
             with ordinality as prod(value, ordinality)
),
violating_recalls as (
    select distinct source_recall_id
    from (
        select source_recall_id, product_name, product_model
        from exploded
        group by source_recall_id, product_name, product_model
        having count(distinct product_ordinal) > 1
           and count(distinct raw_landing_path) > 1
    ) g
)
select
    b.source_recall_id,
    b.extraction_timestamp,
    left(b.content_hash, 12)                              as content_hash,
    jsonb_array_length(coalesce(b.products, '[]'::jsonb)) as n_products,
    right(b.raw_landing_path, 46)                         as landing_path_tail
from cpsc_recalls_bronze b
join violating_recalls v using (source_recall_id)
order by b.source_recall_id, b.extraction_timestamp;

-- ---------------------------------------------------------------------------
-- Q6 / Q7 — the REPLACEMENT PREDICATE, run here first so it can be proven green
-- on the live corpus before it is committed to the dbt singular test. Both must
-- return 0 rows. Together they replace the unsound
--   count(distinct product_ordinal) > 1 and count(distinct raw_landing_path) > 1
-- formulation that produced the 2026-09-07 false positives.
-- ---------------------------------------------------------------------------

\echo
\echo '=== Q6: replacement predicate, class LENGTH_REGRESSION — MUST return 0 rows ==='
\echo 'products[] got SHORTER than the immediately preceding snapshot of the same recall.'
\echo 'Catches deletion/truncation even when every element was renamed in the same wave,'
\echo 'which is the case the name-keyed predicate is structurally blind to. No false-'
\echo 'positive mode: under a genuinely append-only array, length cannot decrease.'

with lengths as (
    select
        source_recall_id,
        extraction_timestamp,
        content_hash,
        jsonb_array_length(coalesce(products, '[]'::jsonb)) as n_products
    from cpsc_recalls_bronze
),
with_previous as (
    select
        source_recall_id,
        extraction_timestamp,
        n_products,
        lag(n_products) over (
            -- content_hash breaks ties deterministically: two rows for one recall
            -- sharing an extraction_timestamp would otherwise order arbitrarily,
            -- which could flip a shrink into a growth between runs.
            partition by source_recall_id
            order by extraction_timestamp, content_hash
        ) as previous_n_products
    from lengths
)
select
    source_recall_id,
    extraction_timestamp,
    previous_n_products,
    n_products
from with_previous
where previous_n_products is not null
  and n_products < previous_n_products
order by source_recall_id, extraction_timestamp
limit 60;

\echo
\echo '=== Q6b: LENGTH_REGRESSION count (headline for Q6) ==='

with lengths as (
    select
        source_recall_id,
        extraction_timestamp,
        content_hash,
        jsonb_array_length(coalesce(products, '[]'::jsonb)) as n_products
    from cpsc_recalls_bronze
),
with_previous as (
    select
        source_recall_id,
        n_products,
        lag(n_products) over (
            partition by source_recall_id
            order by extraction_timestamp, content_hash
        ) as previous_n_products
    from lengths
)
select count(*) as length_regression_count
from with_previous
where previous_n_products is not null
  and n_products < previous_n_products;

\echo
\echo '=== Q7: replacement predicate, class ORDINAL_MOVED — MUST return 0 rows ==='
\echo 'For each (recall, name, model) that occupies EXACTLY ONE slot in every snapshot it'
\echo 'appears in, flag it when that slot number differs across snapshots. The singleton'
\echo 'restriction is what removes the false positives: a (name, model) that legitimately'
\echo 'appears twice in one array is excluded, and so is one that appears twice in an'
\echo 'earlier snapshot and once in a later one (a rename of one of a duplicate pair).'
\echo 'Still catches reorder ([A,B]->[B,A]), mid-array insert ([A,B]->[A,X,B], B moves'
\echo '2->3) and mid-array delete ([A,B,C]->[A,C], C moves 3->2) whenever the moved'
\echo 'element keeps its name. Blind by construction to a product that is BOTH duplicated'
\echo 'AND moved -- if two slots are textually identical nothing in the payload can say'
\echo 'which one moved, and the superseded predicate could not see that case either.'

with exploded as (
    select
        source_recall_id,
        raw_landing_path,
        prod.ordinality::int as product_ordinal,
        coalesce(prod.value ->> 'name',  '<<NULL>>') as product_name,
        coalesce(prod.value ->> 'model', '<<NULL>>') as product_model
    from cpsc_recalls_bronze,
         lateral jsonb_array_elements(coalesce(products, '[]'::jsonb))
             with ordinality as prod(value, ordinality)
),

-- per (recall, name, model, snapshot): how many slots the pair occupies there, and
-- which one (min is exact for the singletons we keep below)
per_path as (
    select
        source_recall_id,
        product_name,
        product_model,
        raw_landing_path,
        count(*)             as n_slots_in_snapshot,
        min(product_ordinal) as the_ordinal
    from exploded
    group by source_recall_id, product_name, product_model, raw_landing_path
),

-- keep only pairs that are a singleton in EVERY snapshot they appear in
singletons as (
    select source_recall_id, product_name, product_model
    from per_path
    group by source_recall_id, product_name, product_model
    having max(n_slots_in_snapshot) = 1
)

select
    p.source_recall_id,
    left(p.product_name, 50)  as product_name,
    left(p.product_model, 20) as product_model,
    count(*)                      as n_snapshots,
    count(distinct p.the_ordinal) as n_distinct_ordinals,
    string_agg(distinct p.the_ordinal::text, ' -> ' order by p.the_ordinal::text) as ordinals_seen
from per_path p
join singletons s
  on  s.source_recall_id = p.source_recall_id
  and s.product_name     = p.product_name
  and s.product_model    = p.product_model
group by p.source_recall_id, p.product_name, p.product_model
having count(distinct p.the_ordinal) > 1
order by p.source_recall_id
limit 60;

\echo
\echo '=== Q7b: ORDINAL_MOVED count (headline for Q7) ==='

with exploded as (
    select
        source_recall_id,
        raw_landing_path,
        prod.ordinality::int as product_ordinal,
        coalesce(prod.value ->> 'name',  '<<NULL>>') as product_name,
        coalesce(prod.value ->> 'model', '<<NULL>>') as product_model
    from cpsc_recalls_bronze,
         lateral jsonb_array_elements(coalesce(products, '[]'::jsonb))
             with ordinality as prod(value, ordinality)
),
per_path as (
    select
        source_recall_id,
        product_name,
        product_model,
        raw_landing_path,
        count(*)             as n_slots_in_snapshot,
        min(product_ordinal) as the_ordinal
    from exploded
    group by source_recall_id, product_name, product_model, raw_landing_path
),
singletons as (
    select source_recall_id, product_name, product_model
    from per_path
    group by source_recall_id, product_name, product_model
    having max(n_slots_in_snapshot) = 1
)
select count(*) as ordinal_moved_count
from (
    select p.source_recall_id, p.product_name, p.product_model
    from per_path p
    join singletons s
      on  s.source_recall_id = p.source_recall_id
      and s.product_name     = p.product_name
      and s.product_model    = p.product_model
    group by p.source_recall_id, p.product_name, p.product_model
    having count(distinct p.the_ordinal) > 1
) g;

-- ---------------------------------------------------------------------------
-- Q8 / Q9 — follow-through on a NON-ZERO Q6. Added 2026-09-12 after Q6 returned
-- 64 length regressions on the live corpus (Q7 = 0), every one collapsing to
-- exactly n_products = 1. Q8 shows the shape of the collapse; Q9 measures
-- whether silver is currently serving fewer products than bronze has observed.
-- ---------------------------------------------------------------------------

\echo
\echo '=== Q8: shape of the collapse — full chronology for the largest length drops ==='
\echo 'Every snapshot of the recalls with the biggest regressions, with each snapshot''s'
\echo 'product names. Distinguishes "CPSC consolidated N blank-name placeholder entries'
\echo 'into 1 properly-named product" (a source-side cleanup) from "CPSC dropped N-1 real'
\echo 'distinct products" (data loss). Read the names column down the page per recall.'

with lengths as (
    select
        source_recall_id,
        extraction_timestamp,
        content_hash,
        jsonb_array_length(coalesce(products, '[]'::jsonb)) as n_products
    from cpsc_recalls_bronze
),
with_previous as (
    select
        source_recall_id,
        n_products,
        lag(n_products) over (
            partition by source_recall_id
            order by extraction_timestamp, content_hash
        ) as previous_n_products
    from lengths
),
biggest_drops as (
    select source_recall_id, max(previous_n_products - n_products) as drop_size
    from with_previous
    where previous_n_products is not null
      and n_products < previous_n_products
    group by source_recall_id
    order by drop_size desc
    limit 6
)
select
    b.source_recall_id,
    b.extraction_timestamp,
    jsonb_array_length(coalesce(b.products, '[]'::jsonb)) as n_products,
    (
        select string_agg(left(coalesce(p.value ->> 'name', '<<NULL>>'), 30), ' | '
                          order by p.ordinality)
        from jsonb_array_elements(coalesce(b.products, '[]'::jsonb))
             with ordinality as p(value, ordinality)
    ) as product_names
from cpsc_recalls_bronze b
join biggest_drops d using (source_recall_id)
order by d.drop_size desc, b.source_recall_id, b.extraction_timestamp;

\echo
\echo '=== Q9: SILVER BLAST RADIUS — is the serving layer exposing fewer products today? ==='
\echo 'stg_cpsc_recalls keeps row_number() over (partition by source_recall_id order by'
\echo 'extraction_timestamp desc) = 1, so recall_product only ever sees the LATEST'
\echo 'snapshot. This compares that latest length against the maximum ever observed in'
\echo 'bronze. Non-zero means product rows that once existed in silver are gone today,'
\echo 'and their recall_product_id values are orphaned for any consumer that cached them.'

with ranked as (
    select
        source_recall_id,
        jsonb_array_length(coalesce(products, '[]'::jsonb)) as n_products,
        row_number() over (
            -- mirrors stg_cpsc_recalls exactly (no secondary sort key there either)
            partition by source_recall_id
            order by extraction_timestamp desc
        ) as rn
    from cpsc_recalls_bronze
),
agg as (
    select
        source_recall_id,
        max(n_products)                             as max_n_products_ever,
        max(n_products) filter (where rn = 1)       as latest_n_products
    from ranked
    group by source_recall_id
)
select
    count(*)                                                     as total_recalls,
    count(*) filter (where latest_n_products < max_n_products_ever)
                                                                 as recalls_exposing_fewer_products,
    coalesce(sum(max_n_products_ever - latest_n_products)
             filter (where latest_n_products < max_n_products_ever), 0)
                                                                 as product_rows_dropped_from_silver
from agg;

\echo
\echo '=== Q9b: per-recall detail for Q9 (which recalls lost how many product rows) ==='

with ranked as (
    select
        source_recall_id,
        jsonb_array_length(coalesce(products, '[]'::jsonb)) as n_products,
        row_number() over (
            partition by source_recall_id
            order by extraction_timestamp desc
        ) as rn
    from cpsc_recalls_bronze
),
agg as (
    select
        source_recall_id,
        max(n_products)                       as max_n_products_ever,
        max(n_products) filter (where rn = 1) as latest_n_products
    from ranked
    group by source_recall_id
)
select
    source_recall_id,
    max_n_products_ever,
    latest_n_products,
    max_n_products_ever - latest_n_products as product_rows_dropped
from agg
where latest_n_products < max_n_products_ever
order by product_rows_dropped desc, source_recall_id
limit 80;
