# Operations guide

This document covers production operations: scheduled runs, monitoring, secret rotation, and recovery procedures. For architectural rationale, see the ADRs in `documentation/decisions/`.

Sections marked **TBD during implementation** describe procedures that depend on code not yet written.

---

## Pipeline overview

Five scheduled GitHub Actions workflows (per [ADR 0010](decisions/0010-ingestion-cadence-and-github-actions-cron.md)):

| Source | Cadence | Workflow file (planned) |
|---|---|---|
| CPSC | daily | `.github/workflows/extract-cpsc.yml` |
| FDA | daily | `.github/workflows/extract-fda.yml` |
| USDA | daily | `.github/workflows/extract-usda.yml` |
| NHTSA | weekly | `.github/workflows/extract-nhtsa.yml` |
| USCG | weekly | `.github/workflows/extract-uscg.yml` |

Plus a transformation workflow that runs `dbt build` + `dbt test` after extractors complete — details TBD during implementation.

Pipeline state — per-source watermarks (last-seen publication timestamps, ETags, pagination cursors) and per-run metadata (status, counts, duration) — lives in two Neon Postgres tables: `source_watermarks` and `extraction_runs`. Full rationale in [ADR 0020](decisions/0020-pipeline-state-tracking.md). The queries below are written against these tables.

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
3. Update the corresponding Pydantic model in `src/schemas/<source>.py` to accept the new schema shape. Open a PR.
4. Once the PR merges, run the re-ingestion command for the affected date window (exact command TBD during implementation; expected shape):
   ```bash
   uv run python -m src.cli re-ingest --source <source> --from <date> --to <date>
   ```
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

**TBD during implementation.**

Common anticipated sections:

- Extractor failing with auth error → rotation runbook above
- `_rejected` table accumulating rows → schema drift procedure above
- Workflow hitting rate limit consistently → retry-tuning in ADR 0013
- Neon cold-start timeouts → not a bug; acceptable for cron-driven usage per ADR 0005
- `extraction_runs` has stale `running` rows → see Monitoring query above; follow `github_run_url` to the killed workflow run for root cause
- Source watermark not advancing despite successful runs → indicates the bronze-load transaction is committing without the watermark update (should be impossible per ADR 0020's transactional coupling); treat as a code bug, not an ops fix

---

## References

- [Architecture Decision Records](decisions/)
- [Development guide](development.md)
- [ADR 0020 — Pipeline state tracking](decisions/0020-pipeline-state-tracking.md)
- [GitHub Actions workflows](../.github/workflows/) (not yet created)
