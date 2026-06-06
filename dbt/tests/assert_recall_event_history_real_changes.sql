-- Every recall_event_history row must represent a REAL change: the normalized old and new
-- values must differ. A row whose normalized old/new are equal means the cosmetic-noise
-- filter (whitespace + ''/NULL folding via norm_text_for_change) leaked a phantom edit —
-- the exact failure mode ADR 0022 / 6c.1 guards against (USDA Finding Q whitespace churn,
-- ''↔NULL representation flips). Guards against the model's change-detection predicate being
-- weakened or the macro drifting. Returns offending rows; expected empty.

select
    source,
    source_recall_id,
    langcode,
    field_name,
    changed_at,
    old_value,
    new_value
from {{ ref('recall_event_history') }}
where {{ norm_text_for_change('old_value') }} is not distinct from {{ norm_text_for_change('new_value') }}
