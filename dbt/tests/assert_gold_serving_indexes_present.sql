{{ config(severity='error') }}

-- Serving-mart index guard (gold-audit-workstream G0). The R2 keyset index and the R3 UPC GIN are
-- load-bearing for the recalls-api (keyset pagination over mart_recall_summary; UPC containment over
-- mart_product_search) and are created during the gold build — R2 via a `post_hook`, R3 via
-- config(indexes). On 2026-06-14 the R2 post_hook index was found MISSING from the live catalog after a
-- build (it recreated cleanly on a targeted rebuild, so the post_hook is functional — a one-off, not a
-- code bug). This test fails the nightly `dbt test` step if either index ever regresses again, instead of
-- letting the public API silently degrade to seq-scans. A non-empty result = a required index is absent.
--
-- Reads pg_indexes (catalog, visible to the build/owner role). severity=error: a missing serving index is
-- a hard go-live failure, not a warning.

with expected (label, tablename, must_match) as (
    values
        ('R2 keyset (published_at DESC, recall_event_id)', 'mart_recall_summary', '%published_at desc%recall_event_id%'),
        ('R3 recall_product_upcs GIN',                     'mart_product_search', '%using gin%recall_product_upcs%')
)

select
    e.label,
    e.tablename
from expected e
where not exists (
    select 1
    from pg_indexes i
    where i.schemaname = 'public'
      and i.tablename = e.tablename
      and lower(i.indexdef) like e.must_match
)
