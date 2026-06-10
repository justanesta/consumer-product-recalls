-- Quantity-parser discordance + corpus dump (2026-06-09): write the full discordant + parse corpus to
-- data/exploratory/cross_source/ for the assistant to read and refine the free-text parser. RUN AFTER
-- `recalls parse-quantities` + `dbt build` so it reflects the CURRENT parser (incl. the ISSUE-3 fix).
-- data/exploratory/ is gitignored; data/exploratory/cross_source/ already holds the C13 corpus dump.
-- Uses COPY ... TO STDOUT + \o (multi-line-query safe across psql versions).

-- 1) MIXED-BASIS DISCORDANCE DETAIL — every product row in a (source, recall_event, category) group
--    that carries BOTH bases AND where max(total) <> sum(per_product). The raw string + parsed value
--    + basis exposes WHICH strings misparse (e.g. "1 1/2 lb (Total Quantity: 105)" flagged total but
--    value parsed as 1). Sorted by the size of the gap (the biggest undercounts the old logic dropped).
\o data/exploratory/cross_source/quantity_discordance_detail.csv
copy (
  with disc as (
    select
      re.source,
      rp.recall_event_id,
      rp.quantity_category,
      max(rp.quantity_value) filter (where rp.quantity_basis = 'total_all_products') as max_total,
      sum(rp.quantity_value) filter (where rp.quantity_basis = 'per_product')        as sum_per_product
    from recall_event re
    join recall_product rp on rp.recall_event_id = re.recall_event_id
    where re.source in ('FDA', 'USDA')
      and rp.quantity_value is not null
      and rp.quantity_category is not null
    group by re.source, rp.recall_event_id, rp.quantity_category
    having max(rp.quantity_value) filter (where rp.quantity_basis = 'total_all_products') is not null
       and sum(rp.quantity_value) filter (where rp.quantity_basis = 'per_product') is not null
       and max(rp.quantity_value) filter (where rp.quantity_basis = 'total_all_products')
           <> sum(rp.quantity_value) filter (where rp.quantity_basis = 'per_product')
  )
  select
    d.source,
    d.recall_event_id,
    d.quantity_category,
    d.max_total,
    d.sum_per_product,
    rp.quantity_basis,
    rp.number_of_units as raw_string,
    rp.quantity_value,
    rp.quantity_unit
  from disc d
  join recall_product rp
    on rp.recall_event_id = d.recall_event_id
   and rp.quantity_category = d.quantity_category
  where rp.quantity_value is not null
  order by d.source, abs(d.max_total - d.sum_per_product) desc, d.recall_event_id, rp.quantity_basis
) to stdout with csv header;
\o

-- 2) CORPUS-WIDE STRING -> PARSE -> FREQUENCY — every distinct FDA+USDA raw string, its parse, and how
--    many product rows carry it. Lets parser refinement target the highest-frequency misparses (not
--    only the discordant ones). ~58k+ rows = the full corpus, deliberately un-LIMITed.
\o data/exploratory/cross_source/quantity_string_parse_frequency.csv
copy (
  select
    rp.source,
    rp.number_of_units as raw_string,
    count(*)           as occurrences,
    rp.quantity_value,
    rp.quantity_unit,
    rp.quantity_category,
    rp.quantity_basis
  from recall_product rp
  where rp.source in ('FDA', 'USDA')
    and rp.number_of_units is not null
  group by rp.source, rp.number_of_units, rp.quantity_value, rp.quantity_unit,
           rp.quantity_category, rp.quantity_basis
  order by occurrences desc, raw_string
) to stdout with csv header;
\o
