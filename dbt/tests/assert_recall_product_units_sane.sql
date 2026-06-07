-- Singular test (ADR 0015): recall_product unit counts are non-negative. unit_count is the
-- clean-integer derive (NHTSA potaff, USCG units); number_of_units stays free text and is not
-- checked here. A negative count is a parse/source error. Returns offending rows (severity=error).
select
    source,
    source_recall_id,
    recall_product_id,
    unit_count
from {{ ref('recall_product') }}
where unit_count < 0
