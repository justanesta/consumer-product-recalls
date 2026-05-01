-- Phase 5b.2 Step 3 — investigate why the first live extraction rejected
-- 100% of records into usda_fsis_establishments_rejected.
--
-- When to run: immediately after a live `recalls extract usda_establishments`
-- run that produced a non-zero rejected count. Read top-down.
--
-- Result reading guide:
--   Q1: top-line counts so you know the rejection scale.
--   Q2: histogram of failure_reason prefixes — Pydantic ValidationError messages
--       are long, so we group on the first ~120 chars to collapse near-duplicates.
--       The most common prefix is the schema bug to chase first.
--   Q3: pick one representative raw_record for the most common failure so you
--       can eyeball the actual API response shape vs what the schema declared.
--   Q4: which fields are mentioned in failure_reason — finds the columns whose
--       declared type is wrong. (The Pydantic error string includes the field
--       name, so a substring match across known field names works as a poor-
--       man's classifier.)

-- ---------------------------------------------------------------------------
-- Q1. Top-line counts
-- ---------------------------------------------------------------------------
\echo '=== Q1: rejected vs bronze counts ==='
select
    (select count(*) from usda_fsis_establishments_bronze) as bronze_rows,
    (select count(*) from usda_fsis_establishments_rejected) as rejected_rows,
    (select count(distinct failure_stage) from usda_fsis_establishments_rejected) as distinct_stages;

-- ---------------------------------------------------------------------------
-- Q2. Failure reason histogram (top 10 distinct prefixes)
-- ---------------------------------------------------------------------------
\echo ''
\echo '=== Q2: top 10 failure_reason prefixes ==='
select
    failure_stage,
    left(failure_reason, 200) as reason_prefix,
    count(*) as occurrences
from usda_fsis_establishments_rejected
group by failure_stage, reason_prefix
order by occurrences desc
limit 10;

-- ---------------------------------------------------------------------------
-- Q3. One sample raw_record for the most common failure
-- ---------------------------------------------------------------------------
\echo ''
\echo '=== Q3: a representative raw_record for the most-common failure ==='
with most_common as (
    select left(failure_reason, 200) as reason_prefix
    from usda_fsis_establishments_rejected
    group by reason_prefix
    order by count(*) desc
    limit 1
)
select
    r.source_recall_id,
    r.failure_reason,
    jsonb_pretty(r.raw_record) as raw_record_pretty
from usda_fsis_establishments_rejected r, most_common m
where left(r.failure_reason, 200) = m.reason_prefix
limit 1;

-- ---------------------------------------------------------------------------
-- Q4. Which schema fields appear in failure messages?
-- Pydantic includes the field name in each validation-error message. This
-- counts how often each declared field name appears across all failure
-- reasons — the most-mentioned field is the one whose declared type is
-- contradicted by real data.
-- ---------------------------------------------------------------------------
\echo ''
\echo '=== Q4: schema-field mentions in failure_reason ==='
with fields(name) as (values
    ('establishment_id'),
    ('establishment_name'),
    ('establishment_number'),
    ('address'),
    ('state'),
    ('zip'),
    ('LatestMPIActiveDate'),
    ('status_regulated_est'),
    ('activities'),
    ('dbas'),
    ('phone'),
    ('duns_number'),
    ('county'),
    ('fips_code'),
    ('geolocation'),
    ('grant_date'),
    ('size'),
    ('district'),
    ('circuit')
)
select
    f.name as field_name,
    count(*) filter (where r.failure_reason like '%' || f.name || '%') as mention_count
from fields f
cross join usda_fsis_establishments_rejected r
group by f.name
having count(*) filter (where r.failure_reason like '%' || f.name || '%') > 0
order by mention_count desc;
