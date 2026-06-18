-- Empty-string free-text audit — silver + gold.
--
-- PURPOSE
--   Verify how many (if any) free-text columns carry a literal '' (or whitespace-only)
--   value instead of NULL, broken out per table x column x source. This is the empirical
--   backstop for the empty-string -> NULL normalization fix (TODO "Performance" §2):
--   recall_product.sql now wraps its free-text product passthroughs in nullif(trim(...), ''),
--   and this probe answers the open "Consider the same guard on ... other free-text
--   passthroughs" question for recall_event + the gold marts, with counts.
--
-- WHAT IT CHECKS
--   Every text / varchar / char column (data_type-driven, so it cannot silently miss a
--   column) in the four serving-content tables:
--     silver.recall_product, silver.recall_event, gold.mart_product_search,
--     gold.mart_recall_summary
--   All four expose a `source` column, so results are grouped by source. Expected offenders
--   are CPSC + NHTSA only: those two staging models never got the ADR 0027 empty-string ->
--   NULL dance, while USDA / FDA / USCG all `nullif(col,'')` at staging (so they should read 0).
--   CPSC: product fields live in the `products` jsonb, extracted via `->>` (returns '' for
--   absent Model/Type/Description) with no normalization. NHTSA: only `yeartxt` is nullif'd;
--   every other free-text column passes raw. A non-zero FDA/USCG/USDA cell here would mean a
--   staging nullif was missed — worth a look.
--
-- HOW TO READ IT
--   Query 1 (detail): one row per (table, column, source) that has any '' or whitespace-only
--     cell. ZERO ROWS == clean. Run it BEFORE `dbt build` to capture the pre-fix baseline,
--     and AFTER to confirm recall_product (+ its marts) drop to zero.
--   Query 2 (rollup): affected column count + total dirty cells per layer/table.
--   n_empty       = exactly ''      (what nullif(trim(x),'') fixes)
--   n_whitespace  = whitespace-only, non-empty, e.g. ' ' / tab / newline (matched via
--                   ~ '^\s+$'). trim() in the fix collapses SPACE-only to NULL; if this
--                   column is non-zero for tab/newline-only values, the guard would need an
--                   all-whitespace btrim instead of trim() — surface it here rather than guess.
--
-- READ-ONLY. Run in psql against the target database (no writes beyond a session TEMP table).
-- To widen coverage (e.g. firm / mart_firm_profile), add table names to the IN (...) list in
-- the DO block below — but only tables that also have a `source` column, or the GROUP BY fails.

\set ON_ERROR_STOP on

drop table if exists _empty_string_audit;
create temp table _empty_string_audit (
    layer         text,
    table_schema  text,
    table_name    text,
    column_name   text,
    source        text,
    n_empty       bigint,   -- exactly ''
    n_whitespace  bigint,   -- whitespace-only, non-empty (~ '^\s+$')
    n_total       bigint
);

do $$
declare
    rec record;
begin
    for rec in
        select
            c.table_schema,
            c.table_name,
            c.column_name,
            case when c.table_name like 'mart\_%' then 'gold' else 'silver' end as layer
        from information_schema.columns c
        join information_schema.tables t
          on t.table_schema = c.table_schema
         and t.table_name   = c.table_name
        where t.table_type = 'BASE TABLE'
          and c.table_name in (
                'recall_product', 'recall_event',
                'mart_product_search', 'mart_recall_summary'
          )
          and c.data_type in ('text', 'character varying', 'character')
          and c.table_schema not in ('pg_catalog', 'information_schema')
        order by c.table_name, c.ordinal_position
    loop
        execute format($f$
            insert into _empty_string_audit
            select %L, %L, %L, %L, source,
                   count(*) filter (where %I = ''),
                   count(*) filter (where %I <> '' and %I ~ '^\s+$'),
                   count(*)
            from %I.%I
            group by source
        $f$,
            rec.layer, rec.table_schema, rec.table_name, rec.column_name,
            rec.column_name, rec.column_name, rec.column_name,
            rec.table_schema, rec.table_name
        );
    end loop;
end $$;

\echo ''
\echo '=== Query 1: dirty free-text cells (ZERO ROWS == clean) ==='
select
    layer,
    table_name,
    column_name,
    source,
    n_empty,
    n_whitespace,
    n_total
from _empty_string_audit
where n_empty > 0 or n_whitespace > 0
order by layer, table_name, n_empty desc, column_name, source;

\echo ''
\echo '=== Query 2: rollup by layer/table ==='
select
    layer,
    table_name,
    count(*) filter (where n_empty > 0 or n_whitespace > 0)               as dirty_col_source_pairs,
    count(distinct column_name) filter (where n_empty > 0 or n_whitespace > 0) as dirty_columns,
    sum(n_empty)      as total_empty_cells,
    sum(n_whitespace) as total_whitespace_cells
from _empty_string_audit
group by layer, table_name
order by layer, table_name;
