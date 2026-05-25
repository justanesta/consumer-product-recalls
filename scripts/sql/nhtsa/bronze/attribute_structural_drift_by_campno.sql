-- Attribute structural_multi_batch `mfr_comp_ptno` drift groups to their
-- driving campno.
--
-- Context: `assert_eleven_tuple_identity_stable.sql` Q1 flags 10-field groups
-- whose dropped 11th field disagrees across runs.
-- `decompose_eleven_tuple_drift.sql` then splits those into
-- structural_multi_batch (silver-correct false positive — per-path value sets
-- identical across archives; documented at
-- `documentation/nhtsa/incremental_delta_findings.md` Section G.3 and the J.5
-- Ford 25V685000 writeup) vs real_drift (true cross-run divergence, feeds
-- ADR 0031:84 silver-fragmentation triggers).
--
-- This script answers a follow-up question: when structural_multi_batch on
-- `mfr_comp_ptno` grows day-over-day (e.g., 137 → 194 on 2026-05-16, +57
-- groups, while real_drift held at 0 — Section L.3), which campno is driving
-- the growth?
--
-- Use case: confirm a +N structural delta is continuation of a known recall
-- wave (Ford 25V685000 engine-block-heater per J.5; the Ferrari 12Cilindri
-- pair reclassified in H.2; etc.) rather than a previously-unseen campno
-- crossing the multi-batch threshold for the first time. The latter merits a
-- brief writeup; the former is routine.
--
-- Mechanism: re-derive structural_multi_batch groups for `mfr_comp_ptno`
-- using the same Cartesian-product test as `decompose_eleven_tuple_drift.sql`
-- (path_value_pairs == n_paths * n_distinct_vals), then aggregate by campno.
-- Each output row = one campno + how many structural groups it contributes,
-- with auxiliary group-size measures.
--
-- Wire-up: ad-hoc — run after observing a non-trivial structural delta in
-- `decompose_eleven_tuple_drift.sql` Q1. Output is informational; no
-- triggers, no thresholds.
--
-- Scope: `mfr_comp_ptno` only. Structural growth on the other identity
-- fields is rare-to-nonexistent in observed runs (see Section H.3's per-field
-- breakdown); if that changes, copy this script's pattern and rotate the
-- field, mirroring `decompose_eleven_tuple_drift.sql`'s per-field UNION.
--
-- The output joins per-campno structural counts to per-campno identification
-- (manufacturer makes, distinct component-class count, RCDATE range, total
-- bronze row count) in a single result, so attribution and recall-identity
-- lookup happen together. Without identification, a campno number is
-- opaque — "26V281000 has 61 groups" doesn't tell you whether that's a tire
-- recall, an OEM vehicle recall, or an equipment recall.

\set ON_ERROR_STOP on
\pset null '<NULL>'

\echo
\echo '=== Structural mfr_comp_ptno drift groups attributed by campno + identified ==='
\echo 'n_structural_groups = how many silver-correct multi-batch groups this campno'
\echo 'contributes to the corpus-wide total in decompose_eleven_tuple_drift.sql.'
\echo 'makes / n_compnames / rcdate range identify the recall so attribution is'
\echo 'human-readable. Compare n_structural_groups to a prior run to attribute'
\echo 'day-over-day structural growth.'

with structural_groups as (
    select campno,
           count(distinct raw_landing_path) as n_paths,
           count(distinct coalesce(mfr_comp_ptno::text, '<NULL>')) as n_distinct_vals,
           count(distinct (raw_landing_path, coalesce(mfr_comp_ptno::text, '<NULL>'))) as path_value_pairs
    from nhtsa_recalls_bronze
    group by campno, maketxt, modeltxt, yeartxt, compname, rcl_cmpt_id,
             mfr_comp_desc, mfr_comp_name, endman, bgman
    having (count(distinct mfr_comp_ptno) > 1
            or (count(*) > count(mfr_comp_ptno) and count(mfr_comp_ptno) > 0))
       and count(distinct raw_landing_path) > 1
       and count(distinct (raw_landing_path, coalesce(mfr_comp_ptno::text, '<NULL>')))
           = count(distinct raw_landing_path)
             * count(distinct coalesce(mfr_comp_ptno::text, '<NULL>'))
),
campno_counts as (
    select campno,
           count(*) as n_structural_groups,
           round(avg(n_distinct_vals)::numeric, 1) as avg_distinct_ptnos_per_group,
           max(n_paths) as max_landing_paths_per_group
    from structural_groups
    group by campno
),
campno_identity as (
    select campno,
           string_agg(distinct maketxt, ', ' order by maketxt) as makes,
           count(distinct compname) as n_compnames,
           (array_agg(distinct compname order by compname))[1] as sample_compname,
           min(rcdate)::date as min_rcdate,
           max(rcdate)::date as max_rcdate,
           count(*) as n_bronze_rows
    from nhtsa_recalls_bronze
    where campno in (select campno from campno_counts)
    group by campno
)
select c.campno,
       c.n_structural_groups,
       i.makes,
       i.n_compnames,
       i.sample_compname,
       i.min_rcdate,
       i.max_rcdate,
       i.n_bronze_rows,
       c.avg_distinct_ptnos_per_group,
       c.max_landing_paths_per_group
from campno_counts c
join campno_identity i using (campno)
order by c.n_structural_groups desc, c.campno
limit 50;
