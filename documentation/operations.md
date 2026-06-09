# Operations guide

This document covers production operations: scheduled runs, monitoring, secret rotation, and recovery procedures. For architectural rationale, see the ADRs in `documentation/decisions/`. For system architecture and component relationships, see [`architecture.md`](architecture.md).

> ⚠️ **Partially stale (as of 2026-06-01) — full refresh scheduled in the Phase 6f doc-sync** (`project_scope/archive/phase-6-execution-plan.md` §6f). USCG is **live since 2026-05-15** (3 extraction sources: recalls + manufacturers + manufacturer-details; 8 total across the pipeline). The USCG row below and any "4-source" framing predate the reactivation; live USCG cadences are in `documentation/uscg/` and the Phase 6f plan. The rest of this guide is current.

---

## Pipeline overview

Per-source extraction workflows (per [ADR 0010](decisions/0010-ingestion-cadence-and-github-actions-cron.md), with empirical revisions noted):

> **Cadence rationale is single-homed in [ADR 0010](decisions/0010-ingestion-cadence-and-github-actions-cron.md)** (the *2026-06-08 revision note* — full 9-source matrix + grid + the freshness-vs-compute reasoning). This table is the **operational** view: each source/sweep → cadence → workflow file. ⚠️ Several USCG extract/deep-rescan **workflow files do not exist yet** — Phase 7 creates them (the loaders exist in the registry today).

| Source / sweep | Cadence | Workflow file |
|---|---|---|
| CPSC | daily | `extract-cpsc.yml` |
| CPSC deep-rescan | weekly (Sun) — **mandatory** edit-catch | `deep-rescan-cpsc.yml` |
| FDA | daily | `extract-fda.yml` |
| FDA deep-rescan | weekly (Sun), 90-day window ([ADR 0023](decisions/0023-fda-deep-rescan-required-archive-migration-detected.md)) | `deep-rescan-fda.yml` |
| FDA press releases | weekly (Mon, after FDA) — per-event work-list | `extract-fda-press-releases.yml` |
| FDA press releases · full sweep | quarterly — recent-first, resumable (~25k) | `deep-rescan-fda-press-releases.yml` |
| USDA recalls | daily — full-dump | `extract-usda.yml` |
| USDA establishments | weekly (Wed) — full-dump directory | `extract-usda-establishments.yml` |
| NHTSA | daily (Mon–Fri) — full POST_2010 file | `extract-nhtsa.yml` |
| NHTSA deep-rescan | monthly — adds the static PRE_2010 archive | `deep-rescan-nhtsa.yml` |
| USCG recalls | daily — short-circuit scrape | **Phase 7** — `extract-uscg.yml` not yet created |
| USCG recalls · deep-rescan | monthly — safety-net full walk | **Phase 7** — `deep-rescan-uscg.yml` not yet created |
| USCG manufacturers (listing) | weekly (Mon) — short-circuit directory | **Phase 7** — `extract-uscg-manufacturers.yml` not yet created |
| USCG manufacturers · deep-rescan | monthly — safety-net | **Phase 7** — not yet created |
| USCG manufacturer details · Tier-1 | weekly (Mon, after listing) — listing-delta work-list | `extract-uscg-manufacturers-detail.yml` |
| USCG manufacturer details · Tier-2 | **monthly 1/3 shard** — full ~16k sweep, >6h ⇒ tranched | `deep-rescan-uscg-manufacturers-detail.yml` |

Plus a transformation workflow scheduled to run after the latest extraction completes (per [ADR 0018](decisions/0018-ci-posture.md)):

| Workflow | Schedule | Action |
|---|---|---|
| `transform.yml` | Daily, time-shifted ~30 min after the latest daily extractor | `dbt build --project-dir dbt` + `dbt test --project-dir dbt`. Posts dbt docs to Cloudflare Pages on success. |

Pipeline state — per-source watermarks (last-seen publication timestamps, ETags, pagination cursors) and per-run metadata (status, counts, duration, `change_type`) — lives in Neon Postgres: `source_watermarks` and `extraction_runs`. For presence-tracked sources (USDA initially, [ADR 0026](decisions/0026-lifecycle-tracking-snapshot-presence-manifest.md)) a third table, `extraction_run_identities` (migration 0027), records which recall identities each successful run returned — written automatically inside the run's `_record_run` transaction, **no operator action**. Retention: keep-forever for now (~2K rows per USDA run); revisit a TTL only if disk growth becomes material. Full rationale in [ADR 0020](decisions/0020-pipeline-state-tracking.md). The queries below are written against these tables.

### SCD-2 snapshots (dbt-managed history)

The silver SCD-2 history tables live in the `silver_snapshots` schema — the firm sidecars ([ADR 0035](decisions/0035-cross-source-scd2-silver-dimensions.md)) plus the NHTSA `nhtsa_recall_product_snapshot` v1.5 product-grain track ([ADR 0033](decisions/0033-silver-row-versioning-via-scd-on-stable-anchor.md), 6c.6). They are **dbt snapshots**, so two operational rules apply:

- **`transform.yml` runs `dbt build`, which executes snapshots in DAG order.** Never substitute `dbt run` for the silver layer — `dbt run` skips snapshots, leaving the snapshot-backed current surfaces (`firm_*_attributes`, `recall_product`'s NHTSA branch + `recall_product_history`) reading a stale snapshot. `dbt build` (or `dbt snapshot`) is the only correct invocation.
- **Snapshots are intentionally exempt from `--full-refresh`** (history protection). Resetting one — needed only when its `unique_key`/recipe changes, never in normal forward operation — is a manual `DROP` of the snapshot table plus any dependent views in dependency order, then `dbt build` re-initializes it. The NHTSA reset is scripted at `scripts/sql/nhtsa/silver/reset_nhtsa_recall_product_snapshot.sql`. A reset discards accumulated history, so it is a dev-only operation; on `main` it requires a deliberate decision.

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
5. The re-ingest reads raw payloads from R2 landing, re-runs validation and bronze load with the updated schema, and relies on content hashing (per [ADR 0007](decisions/0007-lineage-via-bronze-snapshots-and-content-hashing.md)) to keep the operation idempotent. **Scope (Phase 6d):** supported for the JSON REST sources — `cpsc`, `fda`, `usda`, `usda_establishments`, `fda_press_releases` (their landed payload is a `json.dumps(raw_records)` array, replayed via `json.loads`). NHTSA (flat file) and USCG (HTML) are **not** re-ingestable — they are cheaply re-fetchable, so re-process them with `recalls deep-rescan` instead (the CLI rejects them with that guidance). Each replayed payload is re-landed to a fresh R2 key under a new `schema_rebaseline`/`hash_helper_rebaseline` run, so `recall_event_history` excludes the wave from edit detection. **Caveat:** a re-baseline that changes a record's canonical hash *does* increment `recall_lifecycle.edit_count` for that recall — that model counts distinct content hashes with no `change_type` filter — so expect `edit_count` to tick up by one across the rebaselined corpus even though `recall_event_history` stays honest.
6. Verify `_rejected` rows for the window have cleared; any remaining rejections indicate a schema fix that's still incomplete.
7. Confirm `source_watermarks` reflects the re-ingest. Re-ingests read raw from R2 and do not require watermark state to be correct, but a post-reingest sanity check is worth running:

   ```sql
   SELECT source, last_successful_run_at, last_seen_published_at, last_record_count
   FROM source_watermarks
   WHERE source = '<source>';
   ```

   If the watermark advanced past the re-ingest window without issue, scheduled runs will continue forward. If the re-ingest was a full backfill, manually setting the watermark back may be desired so the next scheduled run fetches nothing new — adjust via UPDATE only after verifying the expected cadence.

### USDA presence-manifest backfill (one-shot)

`scripts/backfill_manifest.py` ([ADR 0028](decisions/0028-backfill-historical-reextraction-semantics.md) Mechanism C) reconstructs the USDA presence manifest (`extraction_run_identities`) for runs that predate the table (6c migration 0027), so `recall_lifecycle.is_currently_active` / `was_ever_retracted` extend back to a `run_id` floor. **USDA-only** (the only `default_track_presence` source). **Census-first** — the default mode is read-only:

```bash
# (a Python script, not a `recalls` subcommand — run it directly)
python scripts/backfill_manifest.py             # census (read-only SQL): floor + NULL-run_id count + backfillable count
python scripts/backfill_manifest.py --dry-run   # preview: replays each backfillable run, reports would-be row counts, no write
python scripts/backfill_manifest.py --apply     # then, if warranted, insert the backfillable runs
```

Review the census before applying. `--dry-run` and `--apply` are mutually exclusive. Runs with a NULL `run_id` (run_id was nullable from the baseline) are **permanently un-backfillable** (no FK target), so the manifest history has a hard floor — the earliest run carrying a `run_id`. `--dry-run` re-validates each old payload under the *current* schema and reports the would-be roster size, so a count well below the run's corpus size flags rows the current schema now rejects (e.g. after a source schema change). The `--apply` insert is `ON CONFLICT DO NOTHING` and skips runs already manifested, so it is safe to re-run. After applying, verify with `scripts/sql/_pipeline/verify_presence_manifest.sql` and `dbt build`.

---

### FDA press-release historical seed (checkpointed, one-shot)

The FDA Tier-3 press-release backfill is per-event (`GET /search/pressreleaseurls/{eventid}`, ~50.5K events, ~17 h at the polite 1 req/s) — too long for a single GitHub Actions job (6 h cap) and too long to risk losing to a laptop sleep or Neon blip. `FdaPressReleaseCheckpointedSeedLoader` (CLI `--checkpointed`) sweeps **recent-first** (`recall_initiation_dt` DESC — press releases concentrate in 2018+, so value front-loads) in batches that land+load+checkpoint per batch; the resume cursor is co-committed with each batch's bronze rows in `deep_rescan_checkpoints`, so a crash/throttle costs at most one batch.

```bash
alembic upgrade head    # ensure migration 0030 (deep_rescan_checkpoints) is applied
systemd-inhibit --what=sleep:idle --mode=block --why="FDA PR seed" \
  recalls deep-rescan fda_press_releases --checkpointed --change-type historical_seed \
  2>&1 | tee logs/seed_pr_checkpointed_$(date +%Y%m%dT%H%M%S).log
```

**Resume:** re-run the *exact same command* — it continues from the committed DB cursor (a `complete` checkpoint is a no-op). **Self-healing on transient failure (overnight, unattended):** a transient batch failure (network drop, a plain 5xx, **or a `503`-with-HTML — a cached Akamai NetStorage "Accessdata Error" failover page served when the iRES origin hiccups**, verified 2026-06-08; distinct from the permanent UA-fingerprint 302→apology page, Finding N) no longer kills the sweep. The driver sleeps an **escalating cooldown** (`--cooldown-base-seconds`, default 120 s, doubling each consecutive failure, capped at 30 min) and re-runs the batch from the committed cursor; after `--max-consecutive-failures` (default 6) it trips a **circuit breaker** and exits cleanly (cursor preserved → just re-run). Per-event 5xx blips are absorbed earlier by the existing backoff retry, so most throttles never reach the driver. Watch `seed.batch_failed_cooling_down` / `seed.circuit_open` in the log. The loader sends the Mozilla `IRES_USER_AGENT` (the default python-httpx UA trips the fingerprint 302 on the first request). Knobs: `--batch-size` (default 250 events ≈ ~5 min), `--since YYYY-MM-DD` (floor the work-list; date-unknown events kept), `--cooldown-base-seconds`, `--max-consecutive-failures`. **Verify on completion:** re-run → `already complete`, then `dbt build` the press-release silver. Supersedes the legacy `scripts/fda_press_releases/seed_chunked.sh` (ascending-id, log-grep cursor).

---

### USCG manufacturer-details Tier-2 full sweep (sharded)

The Tier-2 sweep re-fetches **every** MIC's detail page (~16.3k pages at the polite 1 s throttle ≈ 4.5–7.75 h) — the only mechanism that catches **detail-only drift** (a `status` flip, a `Parent MIC` / `Past Company` / `DBA` edit, or a bare `Date Modified` bump) that Tier-1's listing-delta cursor is blind to (`project_scope/phase-5d-uscg-manufacturers-detail.md`, "Tier-1 vs Tier-2 coverage"). Because a whole sweep exceeds the GitHub-Actions **6 h cap**, it runs as a **monthly 1/3 shard rotation** (full corpus every quarter) once the `--min-id/--max-id` shard param lands (per the [ADR 0010 2026-06-08 note](decisions/0010-ingestion-cadence-and-github-actions-cron.md)); the interim is operator-triggered or the chunked-process driver (plan W9).

```bash
recalls deep-rescan uscg_manufacturer_details \
  2>&1 | tee logs/uscg_detail_tier2_$(date +%Y%m%dT%H%M%S).log
# sharded form (once the param exists): append  --min-id <N> --max-id <M>
```

**Bulk `Date Modified` → mark it a re-baseline.** `date_modified` is **in** the bronze content hash (`src/extractors/uscg_manufacturer_detail.py`), so if USCG bulk-bumps it across the directory (a cosmetic, site-wide timestamp change with no real content edit) a Tier-2 sweep re-inserts the *entire* corpus. Run that sweep with **`--change-type=schema_rebaseline`** so the wave is recorded as a re-baseline, not real edits — keeping the audit trail honest and freshness untouched (the deep-rescan path already skips `_touch_freshness`). The SCD-2 sidecar is unaffected either way: `uscg_manufacturer_attributes_snapshot` excludes `date_modified` from its `check_cols`, so no phantom version is banked.

---

## Production (`main`) backfill / cutover

A **controlled manual op** (the kind [ADR 0005](decisions/0005-storage-tier-neon-and-r2.md) sanctions as the only non-CI writer to `main`) for the rare case of seeding the production branch from a local shell — e.g. the Phase 6a.5 historical backfill, or any time `main` must be populated/re-seeded outside the scheduled workflows.

The steady-state invariant is: **local `.env` always points at `dev`; only GitHub Actions secrets point at `main`** (ADR 0005 branch conventions). This procedure temporarily breaks that invariant, so the **revert step is mandatory** — skipping it means a later routine local `recalls extract` silently writes to production.

### What actually targets `main`
- **Alembic + the extractors (SQLAlchemy)** read `NEON_DATABASE_URL` only — `migrations/env.py` pulls it from `Settings` (falling back to `os.environ`), and `alembic.ini`'s `sqlalchemy.url` is intentionally blank. Flip that one var and `alembic upgrade head` + every `recalls extract|deep-rescan` target `main`. **There is no separate alembic credential.**
- **dbt + bare `psql`** read the split fields `NEON_HOST` / `NEON_USER` / `NEON_PASSWORD` / `NEON_DBNAME` (from `.envrc`, consumed by `dbt/profiles.yml` and the `PG*` exports). These are **not** needed to seed — only to run verification `psql` / `dbt` against `main`.

### Procedure
1. **Back up the dev env** so revert is trivial: `cp .env .env.dev.bak` (and note the current `dev` `NEON_DATABASE_URL`).
2. **Repoint** `.env` `NEON_DATABASE_URL` at the `main` branch DSN (host/creds from the Neon console; keep `?sslmode=require`). If you want verification queries to hit `main` too, also update the split `NEON_*` / `PG*` vars in `.envrc`, then `direnv reload`.
3. **Branch-pointer sanity check — do not write until confirmed.** The branch is named only by the endpoint host in your connection string (Neon reports identical `database`/role/`inet_server_addr=::1` on every branch). Confirm the host **and** remember the two credentials are separate — `psql` uses `PG*`, but the seed (alembic + `recalls`) uses `NEON_DATABASE_URL`; verify both:
   ```bash
   echo "$PGHOST"                                                       # psql / PG* path
   echo "$NEON_DATABASE_URL" | sed -E 's#://([^:]+):[^@]*@#://\1:****@#'  # alembic / recalls path
   psql -f scripts/sql/_pipeline/whoami.sql   # branch-STATE fingerprint (fresh main is empty/unmigrated)
   ```
4. **Apply migrations:** `alembic upgrade head`, then `alembic current` must show the expected head revision. Watermark-seed migrations run as part of `upgrade head`; a missing `source_watermarks` row silently fails run-record inserts (see *Operator added a new source…* below), so spot-check coverage:
   ```bash
   psql -f scripts/sql/_pipeline/seed_verify.sql   # bronze row counts + watermark coverage
   ```
5. **Seed** in the documented order (attended/DB-sensitive sources first, long unattended HTML scrapes last; respect the USCG manufacturers → manufacturer-details dependency). For the canonical Phase 6a.5 order and per-source commands see [`project_scope/archive/phase-6-execution-plan.md`](../project_scope/archive/phase-6-execution-plan.md). Every seed run must carry `--change-type=historical_seed` so `recall_event_history` (Phase 6) doesn't synthesize false-edit events (same rule as the re-ingestion procedure above).
6. **Verify** with `scripts/sql/_pipeline/seed_verify.sql` (bronze counts + migration head + watermark coverage), plus `quarantine_check.sql` (rate < 5%) and `watermark_health.sql` in the same dir — counts within ~10% of projection.
7. **Revert the local env (mandatory):** `cp .env.dev.bak .env` (restore `.envrc` if changed), `direnv reload`, and re-run the step-3 sanity check to confirm you are back on `dev`.
8. **Reset `dev` from `main`** (Neon console/API — ADR 0005's sanctioned `main`→`dev` direction). This gives `dev` the freshly-seeded full corpus and discards local experiment cruft, so subsequent local dbt work builds against production-equivalent bronze.

---

## Firm resolution (`recalls resolve-firms`)

Cross-source firm entity resolution runs as a **manual transformation stage**, not a cron workflow — the operator runs it against the production Neon branch, eyeballs the fuzzy clusters, then rebuilds the silver firm models. (Only the *precision review* of its output is scheduled — see "Review loop & cadence" below.) Architecture + method: [`architecture.md`](architecture.md#srcenrichment--firm-resolution-stage-adr-0037); the *why* is [ADR 0037](decisions/0037-firm-resolution-python-stage-not-sql-fuzzy.md); table/column shapes are in [`data_schemas.md`](data_schemas.md#firm-resolution).

The stage is **additive and idempotent**: it truncate-and-reloads `firm_crosswalk`, and silver coalesces a missing/empty crosswalk back to "every firm is its own canonical," so a bad run never corrupts correctness (worst case is no merges). Re-run freely.

### Procedure

1. **Build the inputs.** The resolver reads the `stg_*` views, so build staging first. (The default needs nothing else; `firm_fei_edges` is required only if you opt into the deferred `--fei-merge`.)
   ```bash
   dbt build --select staging
   ```
2. **Preview** (no writes) and sanity-check the merge counts:
   ```bash
   recalls resolve-firms --dry-run
   ```
   The summary prints `distinct_names`, `cleaned_count`, `fuzzy_merged`, and `fei_merged`/`fei_gated` (both **0** in the default run — FEI is attribute-only, ADR 0037). A `fuzzy_merged` that jumped sharply from the prior run is the signal a place hub may have formed (see the troubleshooting entry). `--no-rollup` ships Tier 1 only; `--fei-merge` opts into the deferred FEI tier (not for the firm grain).
3. **Write** the crosswalk:
   ```bash
   recalls resolve-firms
   ```
4. **Eyeball the clusters** before trusting the merges — the gate dumps every multi-member cluster, largest first:
   ```bash
   psql "$NEON_DATABASE_URL" -f scripts/sql/cross_source/silver/verify_fuzzy_clusters.sql
   # then review data/exploratory/cross_source/fuzzy_clusters.csv
   ```
   Q3 lists the 30 largest clusters inline; scan for one that mixes genuinely-unrelated firms. `fei_exact` clusters are authoritative — don't second-guess those.
5. **Rebuild silver** so the new canonical grouping flows into the dimension + bridge:
   ```bash
   dbt build --select firm recall_event_firm
   dbt test  --select firm recall_event_firm
   ```

### The tiers, and which knob moves which

The resolver ships **two name tiers** (full method in [architecture.md](architecture.md#srcenrichment--firm-resolution-stage-adr-0037)). **Tier 1 (name repair)** is always on; **Tier 2 (entity rollup)** is the reviewable one, toggled by `--rollup` / `--no-rollup` and tuned by `--rollup-threshold` (default 90; higher = stricter). **Tier 0 (FDA FEI)** is deferred/opt-in (`--fei-merge`, default off) — FEI is an *attribute*, not a merge key, because establishment-grain FEIs chain unrelated firms across owner changes (ADR 0037). The active config is stamped into `resolver_version` (e.g. `allsrc-tier12-roll90-v3`).

- A **Tier-2 false-merge** (two unrelated firms welded by a shared 2-token phrase) → the audit report (`recalls audit-firm-rollups`) surfaces it ranked; fix via the lever for its class — `place_words.py` (place coincidence), `GENERIC_WORDS` (generic-business coincidence, Tier-2 only), or `never_merge.py` (two genuinely-distinctive shared tokens) — then re-run. These are small, finite, auditable denylists/overrides — they do **not** balloon into an English-dictionary denylist (that failure mode was removed with the old subset clusterer). Full procedure: "Review loop & cadence" below.
- An **FEI mega-cluster / cross-corporate blob** → only possible with `--fei-merge`; the blobs are exactly why FEI is attribute-only by default. Don't use `--fei-merge` for the firm grain. (`diagnose_fei_fanout.sql` / `measure_surviving_coverage.sql` size the FEI tangle if you're exploring the deferred tier or a future establishment dimension.)
- Too many borderline rollups generally → raise `--rollup-threshold`, or ship `--no-rollup` for the safe core and revisit.

### Review loop & cadence (6b.6)

Tier 2 is not self-healing — a new ingest can introduce a fresh 2-token coincidence — so review on a cadence, surfacing only the **incremental change**. The review is driven by a *ranked report*, not a raw cluster dump.

**Who runs what.** Resolving is **manual** (you, locally, against prod — the procedure above). Only the *audit* is **scheduled** (CI, read-only): the cron never re-resolves — it just re-checks the standing `firm_crosswalk` and pings you when suspicious merges pile up. The rhythm:

```
daily              : extractors → bronze → silver        (firm_crosswalk UNCHANGED — resolve is manual)
when you choose to : recalls resolve-firms → dbt build --select firm recall_event_firm
                     (the ONLY thing that introduces new rollup clusters)
monthly (CI cron)  : audit reads prod crosswalk, drops reviewed_ok sigs → opens an Issue if many high-risk remain
on that ping       : do steps 1–3, re-resolve, record the legit ones
```
Because the crosswalk only changes when *you* re-resolve, the monthly report is **identical between resolves** — it catches drift *after* a resolve, not in real time.

1. **Surface.** Run the audit (read-only; after `resolve-firms`). The monthly cron runs this exact command against prod:
   ```bash
   recalls audit-firm-rollups   # -> data/exploratory/cross_source/firm_rollup_review.csv (gitignored)
   ```
   It ranks every `rapidfuzz_rollup` cluster by false-merge suspicion — lowest distinctive-token **Jaccard** first (members diverge), then **no rare anchor** (`weakest_anchor_df` high = the shared tokens are all common), then borderline **score**. Signatures in `documentation/audit/firm_rollup_reviewed_ok.txt` drop out, so each cycle shows only **new / unreviewed** merges. The one-liner prints `rollup_clusters` / `high_risk` / `reviewed_ok`.
2. **Evaluate.** Eyeball the top of the CSV. `match_confidence` is `warn`-severity and the raw `firm_id` is preserved on every row, so nothing silently corrupts — a bad merge is visible and reversible by re-resolving.
3. **Fix** — each false-merge class has one durable, version-controlled home:

   | What you see | Lever |
   |---|---|
   | unrelated `… <place> …` firms (San Antonio, Mountain View) | add the modifier → `src/enrichment/place_words.py` |
   | unrelated `… <generic business word> …` (Marketing, Concepts, Cooperative) | add the word → `GENERIC_WORDS` in `place_words.py` (**Tier-2 only** — never Tier-1, or `Quality Foods`+`Quality Foods Inc` stops merging) |
   | two distinct firms sharing 2 *real* tokens (Eagle Family, General Parts) | add the clean-name pair → `never_merge.py` `_PAIRS`; to pull ONE odd firm out of a legit cluster (Best Buy Bones ≠ the Best Buy retailer), use `_APART_FROM` = `(odd, [family…])` |
   | a genuine cluster (real name variants) | record its `signature` → `firm_rollup_reviewed_ok.txt` (so it stops re-surfacing) |
   | a real brand that failed to roll **up** | a cleaner / normalization fix; FEI hub → `FEI_FANOUT_CAP` |

   Then re-resolve (`recalls resolve-firms` → `dbt build --select firm recall_event_firm`) and re-run the audit to confirm the tail shrank.
4. **Cadence + where to read the output.** `.github/workflows/firm-rollup-audit.yml` runs the audit against **production** (`NEON_DATABASE_URL` secret) **monthly** (07:00 UTC on the 1st) and on demand (Actions tab → *Firm Rollup Audit* → **Run workflow**; optional `alert_threshold`, default 25). Three places to read a run:
   - **Issue** (push) — opened/refreshed *only* when `high_risk > threshold`; title *"Firm-rollup review backlog…"*, labels `data-quality` / `firm-resolution`. A single reused issue, not a pile. **This is the "go review" signal**; a quiet month means nothing new.
   - **Artifact** (pull, every run) — the ranked CSV, uploaded as `firm-rollup-review`. Get it from Actions → the run → **Artifacts**, or `gh run download <run-id> -n firm-rollup-review`. Expires ~90 days.
   - **Run log** — the `audit-firm-rollups: rollup_clusters=… high_risk=… reviewed_ok=…` line, no download needed.
5. **Escalation path:** if list maintenance ever outgrows the corpus (millions of firms), graduate Tier 2 to a probabilistic linkage library (Splink) using name **+ address + source** — overkill at federal-recall scale.

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

### Extractor fails at startup with FileNotFoundError or ValidationError on YAML config

**Symptom:** `recalls extract <source>` exits with `FileNotFoundError: No source config at config/sources/<source>.yaml` or `pydantic.ValidationError: ... Field required` / `Extra inputs are not permitted` / `Input tag 'X' found using 'source_type' does not match any of the expected tags`.

**Diagnose:** The startup error fires before any DB or HTTP work. Check:

1. Does `config/sources/<source>.yaml` exist? The filename must match the source name exactly (e.g., `usda_establishments.yaml`, not `usda_establishment.yaml` — the canonical name in `extraction_runs.source` is plural).
2. Does the YAML's `source_type` match a known discriminator value (`rest_api` or `flat_file`)?
3. Does the YAML's set of keys match `RestApiSourceConfig` or `FlatFileSourceConfig` in `src/config/source_registry.py`? Extra keys (e.g., the typo `etagh_enabled`) are rejected under `extra="forbid"`. Missing required fields are also rejected.

**Most common causes:**

- **Filename ↔ source-name mismatch.** If you renamed a source's canonical name, the YAML file needs the matching rename. Migration seed files identify the canonical name (e.g., `0008_seed_usda_establishments_watermark.py`).
- **YAML key typo.** Pydantic `extra="forbid"` is strict; the error message names the offending key.
- **Schema drift between YAML and code.** If `src/config/source_registry.py` was updated to require a new field but the YAML wasn't, validation fails. Update the YAML to add the missing key.

**Fix:** Read the `ValidationError` message — it names the exact field. Edit the YAML to match `src/config/source_registry.py`'s field set, or rename the YAML file to match the canonical source name.

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

**Also required:** Add a `config/sources/<new_source>.yaml` file matching the discriminated union schema (`RestApiSourceConfig` or `FlatFileSourceConfig` — see `src/config/source_registry.py`). Without it, `load_source_config(source_name)` raises `FileNotFoundError` at CLI startup, before any extraction work begins. The `EXTRACTOR_BY_SOURCE_NAME` dict in `src/config/source_registry.py` also needs the new entry.

### A firm groups unrelated companies (a fuzzy-resolution hub)

**Symptom:** A silver `firm` row's `observed_names` array (or a firm view downstream) lists clearly-unrelated companies in one cluster.

**Diagnose:** Re-dump the clusters and read the largest, and check the FEI fan-out:
```bash
psql "$NEON_DATABASE_URL" -f scripts/sql/cross_source/silver/verify_fuzzy_clusters.sql
psql "$NEON_DATABASE_URL" -f scripts/sql/cross_source/silver/diagnose_fei_fanout.sql
```
Q2 reports the largest cluster size; the cluster's `match_confidence` tells you which tier to fix.

**Most likely cause + fix** (precision-over-recall; the raw `firm_id` is preserved on every row, `match_confidence` is `warn`-severity, and a re-run is idempotent, so over-merges are visible and reversible — never silent corruption):
- **`rapidfuzz_rollup` cluster of `… <place> …` firms** (e.g. unrelated `San Antonio …`) → a Tier-2 place coincidence. Add the modifier to `src/enrichment/place_words.py` and re-run. (Or ship `--no-rollup` to drop Tier 2 entirely.)
- **`fei_exact` cluster welding unrelated firms** → only appears with `--fei-merge` (deferred). Drop `--fei-merge`; FEI is attribute-only by default (ADR 0037), so the shipped crosswalk has no `fei_exact` rows.
- **`rapidfuzz_rollup` from borderline similarity** → raise `--rollup-threshold`.

These are config/code changes, not crosswalk edits — file them; hand-editing `firm_crosswalk` is overwritten by the next run.

---

## References

- [Architecture Decision Records](decisions/)
- [Development guide](development.md)
- [ADR 0020 — Pipeline state tracking](decisions/0020-pipeline-state-tracking.md)
- [GitHub Actions workflows](../.github/workflows/)
