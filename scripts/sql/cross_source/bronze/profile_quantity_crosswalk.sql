-- C13 re-verify (check-your-work sweep, 2026-06-09): commit a reproducible count of quantity_crosswalk
-- parse coverage. The "58,727 distinct / 58,490 value / 46,824 unit" figures came from ephemeral CLI
-- stdout (recalls parse-quantities) — save THIS output as the committed WS-H baseline. The 80% warn
-- threshold in assert_quantity_parse_coverage.sql is independently grounded (corpus profile 87.5%),
-- so this is reproducibility, not a threshold risk.
select
    count(*)                                           as distinct_raw_values,
    count(*) filter (where quantity_value is not null) as rows_with_value,
    count(*) filter (where quantity_unit is not null)  as rows_with_unit,
    round(100.0 * count(*) filter (where quantity_value is not null)
          / nullif(count(*), 0), 1)                    as pct_value
from quantity_crosswalk;
