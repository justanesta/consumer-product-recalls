{{ config(materialized='view') }}

-- Latest-per-recall projection over the USCG bronze table.
-- Bronze may contain multiple rows per source_recall_id when content changes —
-- content-hash dedup with hash_exclude_fields={details_url} prevents identical
-- re-ingestion, but genuine field edits (Disposition Open→Closed, Case Close
-- Date populated, last_date advancing) produce new rows. Silver consumes only
-- the most recent version per source_recall_id.
--
-- Normalizations applied here per ADR 0027 (bronze stores verbatim; silver
-- normalizes):
--   - Finding O — opened_on = 1970-01-01 is USCG's listing-side sentinel for
--     "no date known"; the details-page case_open_date is empty for the same
--     records. Both encodings map to NULL here. announced_at prefers
--     case_open_date when populated (details-page date is more authoritative).
--   - Finding R — disposition has mixed letter case across the corpus
--     (Closed/Open/CLOSED/OPEN, 83.7%/10.8%/5.4%/0.1%). lower() normalizes.
--   - ADR 0027 — empty-string → NULL via nullif(col, '') wrappers, same shape
--     as FDA staging. Bronze keeps the source's mixed null/'' representation
--     verbatim; silver consumers don't have to remember the dance.
--   - units bronze is TEXT per ADR 0027; cast to INTEGER here (NULL on non-
--     numeric so accidental "N/A" or " " values don't blow up downstream).
--   - Finding S — rows with NULL mic AND NULL company_name have no firm
--     anchor. They flow through this staging model intact; the recall_event_firm
--     bridge filters them via WHERE coalesce(mic, company_name) IS NOT NULL.
--     They still appear in recall_event (firm is irrelevant there); they're
--     absent from the firm bridge.

with ranked as (
    select
        *,
        row_number() over (
            partition by source_recall_id
            order by extraction_timestamp desc
        ) as rn
    from {{ source('uscg', 'uscg_recalls_bronze') }}
)

select
    source_recall_id,

    -- Finding O: announced_at coalesces details-page date over listing date,
    -- mapping the listing-side Unix-epoch sentinel to NULL.
    coalesce(
        case_open_date,
        case
            when opened_on = timestamp '1970-01-01 00:00:00+00' then null
            else opened_on
        end
    )                                                       as announced_at,

    -- Listing-derived fields (empty-string → NULL per ADR 0027).
    nullif(mic, '')                                         as mic,
    nullif(company_name, '')                                as company_name,
    nullif(model_name, '')                                  as model_name,
    nullif(problem_1, '')                                   as problem_1,

    -- Details-derived fields.
    nullif(company_official, '')                            as company_official,
    nullif(nullif(model_year, ''), '9999')                  as model_year,  -- W4 Phase A: 9999 sentinel → NULL (2-digit/range parse deferred to 6/7)
    nullif(problem_2, '')                                   as problem_2,
    nullif(nullif(hin, ''), 'N/A')                          as hin,  -- W4 Phase A: 'N/A' sentinel → NULL

    -- Finding R: case-normalize disposition.
    lower(nullif(disposition, ''))                          as disposition,

    case_open_date,
    case_close_date,
    campaign_open_date,
    campaign_close_date,
    last_date,

    -- units bronze is TEXT (ADR 0027); silver casts numeric-looking values
    -- to INTEGER and lets the rest fall through as NULL.
    case when units ~ '^[0-9]+$' then units::integer end    as units,

    -- W4 Phase A normalizations (USCG audit §9): boat_type 0/00 → lpad(2);
    -- severity case-fold (Finding-R parallel — l/h → L/H).
    lpad(nullif(boat_type, ''), 2, '0')                     as boat_type,
    -- One out-of-set severity value warns accepted_values [H,L,M,S] (2026-06-09). NULL it in silver
    -- until its provenance is known (probe: scripts/sql/uscg_recalls/bronze/inspect_out_of_set_severity.sql);
    -- bronze keeps the raw value verbatim (ADR 0027). accepted_values ignores NULLs, so the warn clears.
    case
        when upper(nullif(severity, '')) in ('H', 'L', 'M', 'S') then upper(nullif(severity, ''))
    end                                                     as severity,

    -- Cosmetic; preserved for audit lineage but never load-bearing.
    details_url,

    -- Lineage / audit.
    content_hash,
    extraction_timestamp,
    raw_landing_path
from ranked
where rn = 1
