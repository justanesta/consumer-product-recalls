-- Profile the units fields across sources (Phase 6e — units gold-view scoping).
-- recall_product.number_of_units (TEXT, all sources) vs unit_count (INTEGER, the clean-count
-- sources NHTSA + USCG). Confirms how aggregatable each source's units really are.
--   psql "$DATABASE_URL" -f scripts/sql/cross_source/silver/profile_units.sql

\set ON_ERROR_STOP on
\pset null '<NULL>'

\echo '=== per source: free-text vs clean-integer population + the aggregatable totals ==='
select
    source,
    count(*)                                          as product_rows,
    count(number_of_units)                            as has_text,
    count(unit_count)                                 as has_int,
    round(100.0 * count(unit_count) / count(*), 1)    as pct_clean_int,
    sum(unit_count)                                   as total_units,
    round(avg(unit_count))                            as avg_units,
    max(unit_count)                                   as max_units
from recall_product
group by source
order by source;

\echo '=== top 50 free-text number_of_units values (shows count-vs-weight-vs-Unknown heterogeneity) ==='
\copy (select source, number_of_units, count(*) n from recall_product where number_of_units is not null group by 1, 2 order by 3 desc limit 50) to 'data/exploratory/cross_source/units_freetext_samples.csv' with (format csv, header true)
