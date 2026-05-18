-- Force the next `recalls extract uscg` to do a full walk instead of
-- short-circuiting on the page-0 precheck (Phase 5d Step 6 Finding J).
--
-- Use cases:
--   - Suspect details-only edits that the short-circuit misses (e.g., a
--     Disposition transition Open -> Closed without listing-row change).
--   - Verifying the safety-net deep-rescan cadence behaves as designed.
--   - Recovering from a parser regression that wrote stale data into
--     bronze under a successful short-circuit run.
--
-- The next incremental run will fall through to the full 1,834-fetch walk
-- and re-populate last_records_count, restoring short-circuit eligibility
-- for the run after that.
--
-- Idempotent — clears the column whether populated or already NULL.

update source_watermarks
set last_records_count = null,
    updated_at = now()
where source = 'uscg';

-- Verification: confirm the column is cleared.
select source, last_records_count, last_successful_extract_at
from source_watermarks
where source = 'uscg';
