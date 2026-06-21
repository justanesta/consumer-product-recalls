{{ config(severity='error') }}

-- depends_on: {{ ref('mart_recall_summary') }}
-- depends_on: {{ ref('mart_product_search') }}
-- depends_on: {{ ref('mart_firm_profile') }}

-- Serving-mart index guard (gold-audit-workstream G0; widened 2026-W26). Every index below is
-- load-bearing for the recalls-api, so its absence silently degrades the public API to a seq-scan. They
-- are all built oscillation-safe via the rebuild_indexes() post_hook (DROP-THEN-CREATE on the final
-- relation); this test is the regression guard.
--
-- ⚠️ The `depends_on` refs above are LOAD-BEARING for this test. It reads pg_indexes directly (no ref in
-- the query), so without them dbt schedules it at node ~9 of the run — BEFORE the marts rebuild — and it
-- validates the PREVIOUS build's catalog, producing false fails AND false passes. (That stale read, not a
-- missing index, was the 2026-W26 false failure: it ran ~75s before mart_product_search rebuilt.) The
-- refs force it to run AFTER the three serving marts are rebuilt, so it checks THIS build's output.
--
-- Reads pg_indexes (catalog, visible to the build/owner role). severity=error: a missing serving index is
-- a hard go-live failure, not a warning.

with expected (label, tablename, must_match) as (
    values
        -- mart_recall_summary — GET /recalls (list/detail), search, firm + geo filters
        ('keyset (event_date DESC, recall_event_id)', 'mart_recall_summary', '%event_date desc%recall_event_id%'),
        ('(source, event_date) composite',            'mart_recall_summary', '%(source, event_date)%'),
        ('recall_event_id (unique)',                  'mart_recall_summary', '%unique index%recall_event_id%'),
        ('firms GIN (?firm_id)',                      'mart_recall_summary', '%using gin%firms%'),
        ('search_vector GIN (/recalls/search)',       'mart_recall_summary', '%using gin%search_vector%'),
        ('distribution_state_codes GIN',              'mart_recall_summary', '%using gin%distribution_state_codes%'),
        ('distribution_country_codes GIN',            'mart_recall_summary', '%using gin%distribution_country_codes%'),
        -- mart_product_search — GET /products/search
        ('recall_product_upcs GIN (?upc)',            'mart_product_search', '%using gin%recall_product_upcs%'),
        ('search_vector GIN (?q)',                    'mart_product_search', '%using gin%search_vector%'),
        -- mart_firm_profile — GET /firms/{id}
        ('firm_id (unique)',                          'mart_firm_profile',   '%unique index%firm_id%')
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
