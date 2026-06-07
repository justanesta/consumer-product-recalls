-- Singular test (ADR 0015, refined Phase 6e.5): recall dates are sane. Both published_at (the
-- non-null contract date downstream sorts on) and announced_at (the TRUE recall-initiation date,
-- which can be genuinely old — NHTSA carries mid-20th-century vehicle recalls) must fall in
-- [1940, now]. The 1940 floor (lowered from ADR 0015's guessed 1960 once the corpus surfaced
-- legitimate pre-1960 recalls) still catches the real garbage: the FDA dropped-century typo
-- (year 0013) and any future date. Returns offending rows (severity=error).
select
    source,
    source_recall_id,
    announced_at,
    published_at
from {{ ref('recall_event') }}
where published_at > now()
   or published_at < '1940-01-01'
   or announced_at > now()
   or announced_at < '1940-01-01'
