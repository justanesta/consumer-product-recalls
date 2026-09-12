{{ config(severity='error', warn_if='>0', error_if='>64') }}
-- Singular test: CPSC products[] never SHRINKS across bronze snapshots.
-- Returns one row per (recall, snapshot) whose products[] array is shorter than
-- the immediately preceding snapshot of the same recall.
--
-- C2 IS FALSIFIED (2026-09-12). This test no longer guards an assumption that
-- holds; it meters one that does not. CPSC is collapsing enumerated per-model
-- product rows into a single summary row across the historical archive --
-- verified upstream, not an extraction artifact (the 2026-09-06 deep rescan
-- independently re-fetched the collapsed arrays via a different query and
-- landing file). Examples:
--   00176  8 -> 1  eight named Empire ride-on models -> '"Power Drivers" and "Buddy L"'
--   00119  7 -> 1  six named robe brands            -> 'Children''s robes'
--   00105  6 -> 1  six named Answer/Manitou forks   -> 'Answer and Manitou brand bicycle forks'
-- Baseline 2026-09-12: 64 regressions / 64 recalls / 107 product rows absent from
-- silver. Walking forward through the archive roughly daily since 2026-06-10.
-- Full evidence: documentation/cpsc/array_stability_findings.md, ADR 0031
-- (2026-09-12 amendment), source_assumption_audit.md C2.
--
-- SEVERITY: warn_if '>0' / error_if '>64'. Not a plain warn -- that would go
-- unread within a fortnight for a violation this consequential. Not a plain
-- error -- 64 true positives sit in bronze today and would pin transform.yml
-- red. The threshold keeps the count in every run's output and hard-fails only
-- if the rate accelerates past the reviewed baseline. BUMPING error_if IS A
-- DELIBERATE ACT: re-run Q6/Q9 in the diagnostic, confirm the new rows are the
-- same consolidation pattern and not a new failure mode, update the baselines in
-- the docs above, then raise the number in the same commit.
--
-- PREDICATE REPLACED 2026-09-12. The previous formulation grouped by
-- (source_recall_id, product_name, product_model) and flagged any group spanning
-- > 1 ordinal AND > 1 raw_landing_path -- two independently-evaluated conditions,
-- unsound in BOTH directions once the 2026-06-13 amendment demoted name/model to
-- mutable Type-1 attributes:
--   * FALSE POSITIVE -- a recall that legitimately lists the same (name, model)
--     twice in one array trips both conditions the moment it gains a second
--     snapshot, with nothing reordered. Fired on 12 legacy recalls (all blank
--     name + blank model) when the 2026-09-06 deep rescan gave them their second
--     snapshot; held transform.yml red 2026-09-07 -> 2026-09-12. Verified
--     non-events: identical per-snapshot ordinal sets, unchanged array length.
--   * FALSE NEGATIVE -- it caught NONE of the 64 real regressions above, because
--     a consolidation renames slot 1 as it truncates, so the old (name, model)
--     group and the new one each hold a single landing path and both are filtered
--     out. Three months, 64 violations, silent.
--
-- KNOWN LIMITATION: detects shrinkage only. An equal-length reorder or in-place
-- replacement is invisible here -- that is what
-- assert_cpsc_product_ordinal_stable covers, to the extent it is observable at
-- all. `CpscProduct` (src/schemas/cpsc.py) carries no stable per-product
-- identifier, so position is the only anchor CPSC gives us.
--
-- Drill-down: scripts/sql/cpsc/bronze/diagnose_products_array_append_only_violations.sql

with lengths as (
    select
        source_recall_id,
        extraction_timestamp,
        content_hash,
        jsonb_array_length(coalesce(products, '[]'::jsonb)) as n_products
    from {{ source('cpsc', 'cpsc_recalls_bronze') }}
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
