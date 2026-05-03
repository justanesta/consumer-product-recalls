-- Response-capture smoke test.
-- Confirms the new response_* columns (added in migration 0010) are being
-- populated by the extractors. Run this immediately after a fresh extract
-- to verify the capture path fired correctly for each source.
--
-- Expected per-source after a successful run:
--   cpsc                — status=200; etag/last_modified likely NULL
--                         (CPSC API doesn't emit those headers; NULL itself is
--                         the captured truth and is fine).
--   fda                 — status=200; etag/last_modified possibly NULL.
--   usda                — status=200; etag populated (e.g. "1777687203");
--                         last_modified populated in RFC 1123 format.
--   usda_establishments — status=200; etag/last_modified usually NULL
--                         (Finding A: no ETag mechanism on this endpoint).
--
-- hash_len should be exactly 64 for every row (SHA-256 hex is fixed-width).
-- Any other value means body capture is broken.
-- header_count should be > 0 for every row; 0 implies the headers dict was
-- empty, which would be unusual.
--
-- No parameters. Run as:  psql -f scripts/sql/_pipeline/response_capture_check.sql

\pset null '<NULL>'

select
    source,
    response_status_code                                           as status,
    left(response_etag, 24)                                        as etag,
    left(response_last_modified, 32)                               as last_modified,
    length(response_body_sha256)                                   as hash_len,
    (select count(*) from jsonb_object_keys(response_headers))     as header_count,
    started_at
from extraction_runs
where started_at > now() - interval '10 minutes'
order by started_at desc;
