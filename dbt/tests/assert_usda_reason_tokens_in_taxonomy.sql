{{ config(severity='warn') }}
-- C14 guard (the safety net for "the comma-split is clean"). Every reason_category_tokens element
-- must be in the known FSIS recall-reason taxonomy (9 tokens). A returned row means EITHER:
--   (a) FSIS added a new reason  -> legit taxonomy drift: extend the list here, or
--   (b) the comma-split over-split a token that contains an internal comma -> a BUG: the split
--       corrupted a value (e.g. produced a fragment like "Inc."), which is exactly the failure mode
--       the "tokens have no internal commas" assumption rules out.
-- severity=warn so legit drift surfaces for triage without breaking the build. Empirically the
-- 9 tokens are comma-free (verified 2026-06-09), so the baseline is 0.
select
    re.recall_event_id,
    tok.token as offending_token
from {{ ref('recall_event') }} re,
     lateral jsonb_array_elements_text(re.reason_category_tokens) as tok(token)
where re.reason_category_tokens is not null
  and trim(tok.token) not in (
      'Import Violation',
      'Insanitary Conditions',
      'Misbranding',
      'Mislabeling',
      'Processing Defect',
      'Produced Without Benefit of Inspection',
      'Product Contamination',
      'Unfit for Human Consumption',
      'Unreported Allergens'
  )
