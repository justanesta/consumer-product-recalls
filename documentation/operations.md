# Operations guide

This document covers production operations: scheduled runs, monitoring, secret rotation, and recovery procedures. For architectural rationale, see the ADRs in `documentation/decisions/`. For system architecture and component relationships, see [`architecture.md`](architecture.md).

---

## Pipeline overview

Per-source extraction workflows (per [ADR 0010](decisions/0010-ingestion-cadence-and-github-actions-cron.md), with empirical revisions noted):

| Source | Cadence | Strategy | Workflow file |
|---|---|---|---|
| CPSC | daily | Incremental on `LastPublishDate` (publication-time only) | `.github/workflows/extract-cpsc.yml` |
| CPSC deep rescan | weekly (Sun) | **Mandatory** edit detection — `LastPublishDate` does not advance on edits | `.github/workflows/deep-rescan-cpsc.yml` |
| FDA | daily | Incremental on `eventlmd` | `.github/workflows/extract-fda.yml` |
| FDA deep rescan | weekly (Sun) | Archive-migration coverage per [ADR 0023](decisions/0023-fda-deep-rescan-required-archive-migration-detected.md) | `.github/workflows/deep-rescan-fda.yml` |
| USDA recalls | daily | **Full-dump** every run — no server-side filter exists | `.github/workflows/extract-usda.yml` |
| USDA establishments | weekly (Mon) | Full-dump every run; ETag absent | `.github/workflows/extract-usda-establishments.yml` |
| NHTSA | weekly | Full flat-file download per [ADR 0008](decisions/0008-nhtsa-flat-file-primary-api-for-vehicle-lookup.md) | `.github/workflows/extract-nhtsa.yml` (Phase 5c) |
| USCG | weekly | Polite HTML scrape | `.github/workflows/extract-uscg.yml` (Phase 5d) |

Plus a transformation workflow scheduled to run after the latest extraction completes (per [ADR 0018](decisions/0018-ci-posture.md)):

| Workflow | Schedule | Action |
|---|---|---|
| `transform.yml` | Daily, time-shifted ~30 min after the latest daily extractor | `dbt build --project-dir dbt` + `dbt test --project-dir dbt`. Posts dbt docs to Cloudflare Pages on success. |

Pipeline state — per-source watermarks (last-seen publication timestamps, ETags, pagination cursors) and per-run metadata (status, counts, duration, `change_type`) — lives in two Neon Postgres tables: `source_watermarks` and `extraction_runs`. Full rationale in [ADR 0020](decisions/0020-pipeline-state-tracking.md). The queries below are written against these tables.

### Alerting strategy

v1 alerting is the GitHub Actions UI. There is no paging, on-call rotation, or push notification. The operator is expected to manually check the GHA UI and the canonical queries below on a recurring cadence (weekly is sufficient for v1). Formal upgrade triggers and the threshold for installing real monitoring are documented in [ADR 0029](decisions/0029-application-observability-and-alerting.md).

---

## Monitoring

Three complementary surfaces:

1. **GitHub Actions UI** — workflow run history, per-step logs, re-run buttons, manual `workflow_dispatch`.
2. **Neon Postgres state tables** — SQL-queryable operational state (see canonical queries below).
3. **dbt** — `source_freshness:` assertions (per [ADR 0015](decisions/0015-testing-strategy.md)) compare `source_watermarks.last_successful_run_at` against expected cadence and warn on staleness.

### Canonical operational queries

**Did every source run successfully in the last 24 hours?**

```sql
SELECT
  sw.source,
  sw.last_successful_run_at,
  NOW() - sw.last_successful_run_at AS age,
  er.status AS latest_status
FROM source_watermarks sw
LEFT JOIN LATERAL (
  SELECT status
  FROM extraction_runs
  WHERE source = sw.source
  ORDER BY started_at DESC
  LIMIT 1
) er ON TRUE
ORDER BY sw.source;
```

**Recent failures with a click-through to the GHA log:**

```sql
SELECT source, started_at, status, records_rejected, error_message, github_run_url
FROM extraction_runs
WHERE status IN ('failed', 'partial')
  AND started_at >= NOW() - INTERVAL '7 days'
ORDER BY started_at DESC;
```

**Rejection-rate trend per source (last 30 days):**

```sql
SELECT
  source,
  DATE_TRUNC('day', started_at) AS day,
  SUM(records_fetched) AS fetched,
  SUM(records_rejected) AS rejected,
  ROUND(100.0 * SUM(records_rejected) / NULLIF(SUM(records_fetched), 0), 2) AS reject_pct
FROM extraction_runs
WHERE status != 'running'
  AND started_at >= NOW() - INTERVAL '30 days'
GROUP BY source, DATE_TRUNC('day', started_at)
ORDER BY source, day DESC;
```

Correlate spikes in `reject_pct` with the corresponding `_rejected` bronze tables per [ADR 0013](decisions/0013-error-handling-retries-idempotency-and-quarantine.md) to understand *why* records were rejected.

**Stale `running` rows (runs that died mid-execution):**

```sql
SELECT run_id, source, started_at, github_run_url
FROM extraction_runs
WHERE status = 'running'
  AND started_at < NOW() - INTERVAL '2 hours'
ORDER BY started_at;
```

A handful are normal; an accumulation suggests GitHub Actions runs are being killed before they can update their terminal row — follow the `github_run_url` to diagnose.

---

## Secret rotation runbooks

Per [ADR 0016](decisions/0016-secrets-management.md), all credentials are rotated every 90 days. A quarterly scheduled workflow auto-opens a "Rotate secrets" GitHub Issue as a reminder.

Follow the per-credential runbook below for each set. Rotate one credential at a time, verify, then move to the next.

### Rotating the FDA API key

1. Request a new key via [OII Unified Logon](https://www.accessdata.fda.gov/scripts/ires/apidocs/). Keep the OII support email in the request for record-keeping.
2. Receive the new `Authorization-User` and `Authorization-Key` values.
3. Update the local `.env` (or password-manager vault item) with the new values.
4. Update the corresponding GitHub Actions repository secrets: `FDA_AUTHORIZATION_USER`, `FDA_AUTHORIZATION_KEY`.
5. Trigger a manual run of the FDA extractor workflow via `workflow_dispatch`. Verify it succeeds in the GitHub Actions UI.
6. Once verified working, revoke the old key via OII Unified Logon.
7. Close the "Rotate secrets" issue with a checkmark on FDA.

### Rotating the Neon Postgres password

1. In the Neon console, open the project's connection settings and generate a new password for the role the pipeline uses.
2. Construct the new `NEON_DATABASE_URL` with the new password (keep the host, database name, and options unchanged).
3. Update the local `.env` (or password-manager vault item) with the new URL.
4. Update the `NEON_DATABASE_URL` GitHub Actions repository secret.
5. Trigger a manual run of any extractor workflow to verify database connectivity.
6. Run `dbt test` against the new connection to verify end-to-end functionality.
7. Once verified, invalidate the old password via the Neon console.
8. Close the "Rotate secrets" issue with a checkmark on Neon.

**Note:** Neon's connection pooler is shared across all connections; no application-side connection pool flush is required on rotation.

### Rotating Cloudflare R2 credentials

1. In the Cloudflare dashboard, open R2 → Manage R2 API Tokens.
2. Create a new API token with the same scope as the existing one (read/write access to the pipeline's bucket). Note the new Access Key ID and Secret Access Key.
3. Update the local `.env` (or password-manager vault item) with the new values: `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY`.
4. Update the corresponding GitHub Actions repository secrets.
5. Trigger a manual run of any extractor workflow. Verify that raw payloads are being written to R2 successfully.
6. Once verified, delete the old R2 API token via the Cloudflare dashboard.
7. Close the "Rotate secrets" issue with a checkmark on R2.

### Unplanned rotation (suspected compromise)

If a credential is suspected compromised, rotate immediately — do not wait for the quarterly cycle:

1. Revoke the compromised credential first (via OII / Neon console / Cloudflare dashboard).
2. Immediately generate a replacement.
3. Update GitHub secrets and local `.env`.
4. Trigger a manual workflow run to verify.
5. File an incident note in the repository describing what was compromised and how.

---

## Re-ingestion procedure (after schema change)

Per [ADR 0014](decisions/0014-schema-evolution-policy.md), when an agency changes its schema, the response is:

1. Observe the loud `ValidationError` in the workflow logs (or the accumulation of rows in the `_rejected` table per [ADR 0013](decisions/0013-error-handling-retries-idempotency-and-quarantine.md)).
2. Inspect the rejected records to understand what changed:
   ```sql
   SELECT failure_reason, raw_record
   FROM <source>_rejected
   WHERE rejected_at >= NOW() - INTERVAL '1 day'
   LIMIT 10;
   ```
3. Update the corresponding Pydantic model in `src/schemas/<source>.py` to accept the new schema shape. Open a PR. The PR description must include `RE-BASELINE: yes` if the change alters the canonical record dict (per the [re-baseline playbook](operations/re_baseline_playbook.md) introduced by [ADR 0027](decisions/0027-bronze-storage-forced-transforms-only.md)).
4. Once the PR merges, run the re-ingestion command for the affected date window. Per [ADR 0028](decisions/0028-backfill-historical-reextraction-semantics.md) Mechanism B (R2 replay), this re-processes raw payloads from R2 without contacting the source:
   ```bash
   # Re-ingest a date range — re-reads R2 raw payloads, re-runs validation
   # and bronze load with the updated schema. Idempotent via content-hash
   # conditional insert.
   uv run recalls re-ingest <source> \
     --from-date YYYY-MM-DD \
     --to-date YYYY-MM-DD \
     --change-type schema_rebaseline
   ```
   The `--change-type=schema_rebaseline` flag is **required** — without it the new bronze rows are marked `routine`, which causes `recall_event_history` (Phase 6) to synthesize false-edit events for every record in the wave. See [ADR 0027](decisions/0027-bronze-storage-forced-transforms-only.md) and [ADR 0028](decisions/0028-backfill-historical-reextraction-semantics.md).
5. The re-ingest reads raw payloads from R2 landing, re-runs validation and bronze load with the updated schema, and relies on content hashing (per [ADR 0007](decisions/0007-lineage-via-bronze-snapshots-and-content-hashing.md)) to keep the operation idempotent.
6. Verify `_rejected` rows for the window have cleared; any remaining rejections indicate a schema fix that's still incomplete.
7. Confirm `source_watermarks` reflects the re-ingest. Re-ingests read raw from R2 and do not require watermark state to be correct, but a post-reingest sanity check is worth running:

   ```sql
   SELECT source, last_successful_run_at, last_seen_published_at, last_record_count
   FROM source_watermarks
   WHERE source = '<source>';
   ```

   If the watermark advanced past the re-ingest window without issue, scheduled runs will continue forward. If the re-ingest was a full backfill, manually setting the watermark back may be desired so the next scheduled run fetches nothing new — adjust via UPDATE only after verifying the expected cadence.

---

## Re-recording VCR cassettes

Per [ADR 0015](decisions/0015-testing-strategy.md), cassettes are the authoritative archive of historical API responses. Re-record when:

- A schema-drift event is detected and a cassette needs to capture the new response shape.
- On a quarterly scheduled refresh (verify cassettes still match live responses).

Procedure:

1. Ensure valid credentials are in `.env` (re-recording hits real APIs).
2. Run the re-record command for the affected source:
   ```bash
   uv run pytest tests/integration/test_<source>_extractor.py --record-mode=rewrite
   ```
3. VCR's `before_record_request` filter strips `Authorization` / `X-API-Key` headers automatically, but verify before committing:
   ```bash
   grep -ri "authorization\|api[_-]key" tests/fixtures/cassettes/<source>/
   ```
4. Diff the cassettes to see what changed in the API:
   ```bash
   git diff tests/fixtures/cassettes/<source>/
   ```
5. Commit the updated cassettes alongside any corresponding schema changes in a single PR. The PR title should make the drift visible (e.g. "NHTSA: add DO_NOT_DRIVE + PARK_OUTSIDE fields").

### Non-recordable scenarios

Some test scenarios cannot be re-recorded from the live API because the live API will not produce them on demand. These cassettes are hand-constructed and must NOT be included in a blanket `--record-mode=rewrite` sweep — doing so would hit the real API and replace the synthetic response with a 200.

| Scenario | How to produce |
|---|---|
| 401 auth failure | Record live with a deliberately-bad `Authorization-User` / `Authorization-Key` pair. Still a real server response, just with synthetic credentials. |
| 429 rate limit | Hand-edit an existing 200 cassette's response status code and headers (including `Retry-After`), or replace with a `respx` mock in the unit-test layer per ADR 0015. |
| 500 transient server error | Same approach as 429 — hand-edit or `respx`. |
| Malformed record in response | Hand-edit a recorded cassette to inject the malformed field, or use `respx` / `responses`. The live API validates its own output and will not return malformed payloads. |

Hand-constructed cassettes must carry a comment at the top of the YAML indicating they are synthetic (e.g. `# SYNTHETIC CASSETTE — do not re-record from live API`) so that re-record sweeps can skip them explicitly.

---

## Troubleshooting

A diagnostic surface for failures encountered during cron operation. Each entry pairs a symptom with the diagnostic query, the most likely cause, and the fix.

### Extractor failing with 401 / 403 auth error

**Symptom:** `extract-fda.yml` (or any auth-bearing source workflow) fails with `ExtractionError: Auth failure (401)` early in the run.

**Diagnose:**
```sql
SELECT source, started_at, status, error_message, github_run_url
FROM extraction_runs
WHERE source = 'fda' AND status = 'failed'
ORDER BY started_at DESC LIMIT 5;
```

**Most common causes:**
- The FDA OII credential expired (180-day expiry from issuance) — request a new key per [Rotating the FDA API key](#rotating-the-fda-api-key) above.
- The credential was rotated locally but not in GitHub Actions secrets — verify both are in sync.
- For FDA specifically: a stale `signature=` cache-bust value can cause the iRES server to return a *cached* 401 from a previous bad-credential test. Test by changing the signature value and trying again — see `bruno/fda/lookup/get_product_types.yml` documentation for the iRES quirk.

### Extractor returns 0 records when records were expected

**Symptom:** `extraction_runs.records_extracted = 0` for a source that should have new records.

**Diagnose:**
```sql
SELECT source, started_at, records_extracted, records_inserted, records_rejected, error_message
FROM extraction_runs
WHERE source = '<source>'
ORDER BY started_at DESC LIMIT 10;
```

**Most common causes:**
- **CPSC**: `LastPublishDate` does not advance on edits — see ADR 0010 revision note. If you expected an *edit* to surface, the daily incremental will not catch it; the weekly deep-rescan will. This is now expected behavior, not a bug.
- **USDA**: any value of `field_last_modified_date` in the request URL is silently ignored, so the extractor must be in full-dump mode (it is by default per ADR 0010 revision). 0 records would mean the bot-manager threw an HTML page or the connection was throttled — see next entry.
- **FDA**: incremental window `eventlmdfrom=<yesterday>` may legitimately return 0 records on weekends (FDA does not publish Sat/Sun). Confirm by checking the day-of-week before debugging further.

### USDA / FDA hangs or returns HTML when JSON is expected

**Symptom:** `extract-usda.yml` or `extract-fda.yml` hangs (USDA) or completes too quickly with 0 records (FDA), no obvious error.

**Diagnose:** Look at the structured log lines for `response_content_type` — both extractors now log this on every fetch.
```bash
# In the GHA workflow output (or local logs):
gh run view <run-id> --log | jq 'select(.event == "request_completed")'
```

**Likely causes:**
- **FDA HTML-redirect throttling.** A `302 → /apology_objects/abuse-detection-apology.html` (which 404s) means the IP is anti-abuse blocked. Per ADR 0013, do **NOT** retry. Recovery is time-based (~30 min minimum). Most likely after a deep-rescan ran 27 sequential POSTs; the next daily incremental at 1 POST/run will not re-trigger. If a daily incremental is hitting this, revisit retry policy in `src/bronze/retry.py` to ensure no surprise concurrent loops.
- **USDA Akamai bot-manager throttle.** A slowloris connection (HTTP/1.1) or `INTERNAL_ERROR` (HTTP/2) means the bot-manager is rejecting the User-Agent fingerprint. Per ADR 0016 amendment, USDA requires a Firefox/Linux UA + matching Accept headers. Verify the current UA via:
  ```bash
  cat data/user_agents.json | jq -r '.firefox_linux'
  ```
  and compare to the request header logged in `request_completed`. If the UA is missing or has reverted to `python-httpx/...`, run the UA refresh workflow:
  ```bash
  gh workflow run refresh-user-agents.yml
  ```

### `_rejected` table accumulating rows

**Symptom:** Rejection-rate trend query (above) shows `reject_pct > 0` for a source where it was 0% before.

**Diagnose:**
```sql
SELECT failure_stage, failure_reason, COUNT(*) AS n
FROM <source>_recalls_rejected
WHERE rejected_at >= NOW() - INTERVAL '7 days'
GROUP BY failure_stage, failure_reason
ORDER BY n DESC;
```

**Interpretation:**
- `failure_stage='validate'` with a Pydantic missing-required-field reason → upstream schema drift; follow the [Re-ingestion procedure](#re-ingestion-procedure-after-schema-change) above.
- `failure_stage='validate'` with an `extra='forbid'` reason → upstream added a new field; same procedure applies.
- `failure_stage='invariants'` → business invariant failed (e.g., USDA bilingual orphan, null `source_recall_id`). Inspect the rejected `raw_record` JSONB and decide whether to amend the invariant or treat the record as legitimately bad data.

### Workflow hitting rate limit consistently

**Symptom:** Multiple `extraction_runs.error_message` rows mention `429` (or for FDA, `RateLimitedException` from the HTML-redirect detection in ADR 0013).

**Most common causes:**
- The extractor's pagination loop is too aggressive — check whether `_PAGE_SIZE` is set correctly and there's no inadvertent retry storm.
- For FDA: the deep-rescan workflow's 27 sequential POSTs are running too close together. The workflow uses `time.sleep(2)` between pages by default; verify it hasn't been removed.

**Fix:** retry-tuning in [ADR 0013](decisions/0013-error-handling-retries-idempotency-and-quarantine.md). The retry decorators live in `src/bronze/retry.py`; per-source overrides in each extractor's `__init__`.

### Neon cold-start timeouts

**Symptom:** First query of the day takes 10–15s and times out the extractor; subsequent queries are fast.

**Likely cause:** Neon's free tier auto-suspends compute after a period of inactivity. The cold-start to wake up the compute can exceed default `connect_timeout`. This is **not a bug** — it's expected behavior for the free tier per [ADR 0005](decisions/0005-storage-tier-neon-and-r2.md).

**Fix:** the SQLAlchemy engine's `connect_timeout` should be set to ≥30s (currently 10s in `dbt/profiles.yml` — increase if cold-start timeouts surface in production). For dbt specifically, edit `dbt/profiles.yml` `connect_timeout`. For application code, the engine's `pool_pre_ping=True` + a longer `connect_timeout` handles this cleanly.

### `extraction_runs` has stale `running` rows

**Symptom:** Stale-rows query (above) returns rows older than 2 hours with `status = 'running'`.

**Likely cause:** GitHub Actions runs were killed before they could update their terminal `extraction_runs` row. Common after a runner cancellation or a workflow timeout.

**Fix:**
- Follow each row's `github_run_url` to confirm the underlying workflow did fail.
- Update the row to terminal status:
  ```sql
  UPDATE extraction_runs
  SET status = 'failed', error_message = 'Manual cleanup: workflow killed'
  WHERE run_id = '<run-id>' AND status = 'running';
  ```
- Trigger a fresh `workflow_dispatch` of the extractor for the affected source to resume normal cadence.

### Source watermark not advancing despite successful runs

**Symptom:** `source_watermarks.last_extracted_at` is stale even though `extraction_runs` shows successful runs since then.

**Likely cause:** The bronze-load transaction is committing without the watermark update — should be impossible per [ADR 0020](decisions/0020-pipeline-state-tracking.md)'s transactional coupling. Treat as a **code bug**, not an ops fix.

**Diagnose:** Check whether the success path in `BronzeLoader.load()` is updating `source_watermarks` inside the same `engine.begin()` block as bronze inserts. If not, that's the bug.

**Workaround until fixed:** Manually advance the watermark to the most recent successful run's high-water mark:
```sql
UPDATE source_watermarks SET last_extracted_at = NOW() - INTERVAL '1 day'
WHERE source = '<source>';
```
Don't do this without confirming the underlying bug is filed.

### dbt build fails with "relation does not exist"

**Symptom:** `dbt build` fails on a model that references a bronze table that exists in production but not on the operator's dev branch.

**Likely cause:** A new bronze table (e.g., `usda_fsis_establishments_bronze`) was added but the dev Neon branch was not migrated.

**Fix:**
```bash
uv run alembic upgrade head
```
on the dev branch, then re-run `dbt build`.

### dbt source freshness warns / errors

**Symptom:** `dbt source freshness` (run by the transform workflow) emits a warning or error for one or more sources.

**Likely cause:** The bronze table for that source has not received new rows within its configured threshold (CPSC: 48h, FDA: 72h, USDA: 48h). Most often this means an upstream extractor failure cascaded.

**Diagnose:**
```sql
SELECT source, MAX(extraction_timestamp) AS latest_bronze, NOW() - MAX(extraction_timestamp) AS age
FROM cpsc_recalls_bronze
GROUP BY 1
UNION ALL
SELECT 'fda', MAX(extraction_timestamp), NOW() - MAX(extraction_timestamp)
FROM fda_recalls_bronze
UNION ALL
SELECT 'usda', MAX(extraction_timestamp), NOW() - MAX(extraction_timestamp)
FROM usda_fsis_recalls_bronze;
```

If an extractor failed, the [Auth error](#extractor-failing-with-401--403-auth-error), [HTML throttle](#usda--fda-hangs-or-returns-html-when-json-is-expected), or [Rate limit](#workflow-hitting-rate-limit-consistently) entries above will lead to the right fix.

### Operator added a new source but `extraction_runs.record_failed` warning surfaces

**Symptom:** First extraction of a newly-added source succeeds (bronze rows land) but a `extraction_run.record_failed` warning appears in the logs.

**Likely cause:** `source_watermarks` does not have a seed row for the new source. `extraction_runs.source` is FK-constrained to that table, so the run-record insert silently fails inside the broad `except` block. Surfaced during Phase 5b.2 first extraction.

**Fix:** Add a one-row seed migration for the new source (model on `0008_seed_usda_establishments_watermark.py`). Long-term fix is documented as a Phase 7 prerequisite in `project_scope/implementation_plan.md` "Architectural follow-ups."

---

## References

- [Architecture Decision Records](decisions/)
- [Development guide](development.md)
- [ADR 0020 — Pipeline state tracking](decisions/0020-pipeline-state-tracking.md)
- [GitHub Actions workflows](../.github/workflows/) (not yet created)
