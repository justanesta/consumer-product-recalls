-- recall_distribution_area grain contract (C12): every row must carry at least one parseable US
-- state OR foreign country. The FULL OUTER at the bottom of the model can only emit a row when one
-- side had a key, so a row with BOTH arrays empty would be a build bug (e.g. a coalesce that
-- fabricated a row). Severity error.
select recall_event_id
from {{ ref('recall_distribution_area') }}
where coalesce(cardinality(distribution_state_codes), 0) = 0
  and coalesce(cardinality(distribution_country_codes), 0) = 0
