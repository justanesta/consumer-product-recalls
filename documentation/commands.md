# Commands cheat sheet

Tool-organized quick reference for the commands you reach for daily. Lookup-oriented, not procedural — for narrative context (when to use what, why a command exists), see [`development.md`](development.md) and [`operations.md`](operations.md).

Almost every command runs through `uv run` so it picks up the project's pinned Python and dependencies. If you have direnv configured per [`development.md` § Method 2](development.md#method-2--direnv-optional-recommended-for-regular-development), `.venv/bin` is on `$PATH` and the bare command works too.

---

## Shortcuts via env vars

The verbose forms in the per-tool sections below are kept for clarity (any reader can copy-paste without prior setup). If you've exported the right env vars in `.env` / `.envrc` (autoloaded by direnv), several flags become redundant — the bare command works on its own.

| Tool | Env vars in `.env` / `.envrc` | What gets dropped |
|---|---|---|
| **dbt** | `DBT_PROJECT_DIR=$(pwd)/dbt`, `DBT_PROFILES_DIR=$(pwd)/dbt` | `--project-dir dbt --profiles-dir dbt` on every dbt subcommand |
| **aws** (R2) | `AWS_ENDPOINT_URL=https://${R2_ACCOUNT_ID}.r2.cloudflarestorage.com` (plus the standard `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` mapped from your R2 credentials, or `AWS_PROFILE` if you keep them in `~/.aws/credentials`) | `--endpoint-url $R2_ENDPOINT` on every `aws s3 ...` invocation |
| **psql** | `PGHOST`, `PGUSER`, `PGPASSWORD`, `PGDATABASE`, `PGSSLMODE` (libpq standards — split out of `NEON_DATABASE_URL`) | The `$NEON_DATABASE_URL` argument; bare `psql` connects to the dev branch |

Concrete examples — same command both ways:

```bash
# dbt — verbose vs. with env vars set
uv run dbt parse --project-dir dbt --profiles-dir dbt
uv run dbt parse

# aws (R2) — verbose vs. with env vars set
aws s3 ls s3://${R2_BUCKET_NAME}/cpsc/2026-04-25/ --endpoint-url $R2_ENDPOINT
aws s3 ls s3://${R2_BUCKET_NAME}/cpsc/2026-04-25/

# psql — verbose vs. with PG* env vars set
psql $NEON_DATABASE_URL -c "select 1"
psql -c "select 1"
```

**Note on dbt + Settings.** dbt and the application code use different env-var schemes by design. dbt reads `DBT_PROJECT_DIR` / `DBT_PROFILES_DIR` plus its own `NEON_HOST` / `NEON_USER` / `NEON_PASSWORD` / `NEON_DBNAME` (split from `NEON_DATABASE_URL`); the application code's `pydantic-settings` reads `NEON_DATABASE_URL` whole. **The dbt-only vars cannot live in `.env`** — Settings has `extra='forbid'` and would reject any key it doesn't declare. Put dbt-only vars in `.envrc` directly (not via `dotenv .env`), or in a sibling file that pydantic-settings doesn't read. See [`development.md` § Design rationale](development.md#design-rationale--strict-scope-on-env).

---

## uv — package + venv manager

```bash
uv sync --frozen           # install deps from uv.lock — CI / fresh clone
uv sync                    # sync to pyproject.toml, updates uv.lock if needed
uv lock                    # recompute uv.lock without installing
uv add <pkg>               # add a runtime dep
uv add --dev <pkg>         # add a dev dep
uv remove <pkg>            # drop a dep
uv tree                    # show dep tree (debug version conflicts)
uv run <cmd> <args...>     # execute in the uv-managed venv
uv python list             # available Python versions
```

See also: [`development.md` § Initial setup](development.md#initial-setup), [ADR 0017](decisions/0017-package-management-via-uv.md).

---

## recalls — project CLI

Wired via `[project.scripts]` in `pyproject.toml` → `src.cli.main:app` (Typer).

```bash
uv run recalls --help                                     # list subcommands
uv run recalls version                                    # sanity check
uv run recalls extract <source>                           # incremental run; uses watermark
uv run recalls extract <source> --lookback-days 7         # override watermark with a window
uv run recalls extract <source> --change-type=schema_rebaseline    # mark as re-baseline
uv run recalls extract <source> --change-type=historical_seed      # one-time CPSC backfill
uv run recalls re-ingest <source> \                       # R2 replay (Phase 6)
    --from-date 2026-01-01 --to-date 2026-01-31 \
    --change-type=schema_rebaseline
```

Sources: `cpsc`, `fda`, `usda`, `usda_establishments`. NHTSA + USCG land in Phase 5c/5d.

See also: [`cli.md`](cli.md) for full flag semantics + per-source quirks, [`development.md` § Running extractors locally](development.md#running-extractors-locally), [`operations.md` § Re-ingestion procedure](operations.md#re-ingestion-procedure-after-schema-change), [ADR 0028](decisions/0028-backfill-historical-reextraction-semantics.md).

---

## alembic — schema migrations

```bash
uv run alembic upgrade head             # apply all pending migrations
uv run alembic upgrade +1               # apply next one
uv run alembic current                  # show current revision
uv run alembic history --verbose        # full migration history
uv run alembic revision -m "add column" # new migration (manual)
uv run alembic downgrade -1             # revert last (rare; forward-only by convention)
```

Migrations live at `migrations/versions/<NNNN>_<slug>.py`. Forward-only by convention — `downgrade()` bodies exist but should not be relied on.

See also: [`development.md` § Initial setup](development.md#initial-setup) (run `upgrade head` after fresh clone).

---

## ruff — linter + formatter

```bash
uv run ruff check                       # lint everything per pyproject.toml paths
uv run ruff check --fix                 # apply autofixes
uv run ruff check src/extractors/cpsc.py    # one file
uv run ruff format                      # format
uv run ruff format --check              # verify formatting only — what CI runs
```

See also: [ADR 0018](decisions/0018-ci-posture.md), [`pyproject.toml`](../pyproject.toml) `[tool.ruff]`.

---

## pyright — static type checker

```bash
uv run pyright src tests scripts        # type-check the configured paths
uv run pyright --watch                  # continuous (during refactors)
uv run pyright src/extractors/cpsc.py   # one file
```

Strict mode is enabled. `# pyright: ignore[<rule>]` for unavoidable suppressions; pair with a comment explaining why.

See also: [ADR 0018](decisions/0018-ci-posture.md).

---

## pytest — test runner

```bash
uv run pytest                                              # full suite
uv run pytest tests/unit/                                  # subset by directory
uv run pytest tests/integration/test_cpsc_live_cassettes.py    # one file
uv run pytest tests/integration/test_cpsc_live_cassettes.py::test_happy_path_recent   # one test
uv run pytest -k "happy"                                   # filter by test name substring
uv run pytest -x                                           # stop on first failure
uv run pytest -v                                           # verbose
uv run pytest --pdb                                        # drop into debugger on failure
uv run pytest --cov=src --cov-fail-under=85                # coverage gate (CI mode)
```

### VCR cassettes (within pytest)

```bash
uv run pytest --vcr-record=none tests/integration/         # default — replay only
uv run pytest --vcr-record=rewrite tests/integration/test_<source>_live_cassettes.py    # re-record from live API
uv run pytest --vcr-record=new_episodes tests/integration/                              # record only missing interactions
grep -ri "authorization\|api[_-]key" tests/fixtures/cassettes/    # verify scrub before commit
```

After re-recording, run `scripts/truncate_cassette.py` (planned — see [`development.md` § Managing cassette size](development.md#managing-cassette-size)) to keep cassette files small.

See also: [`development.md` § Running tests](development.md#running-tests), [`development.md` § Re-recording VCR cassettes](development.md#re-recording-vcr-cassettes), [ADR 0015](decisions/0015-testing-strategy.md).

---

## dbt — transformations

All commands need `--project-dir dbt` since the dbt project is not at repo root.

```bash
uv run dbt parse --project-dir dbt --profiles-dir dbt                    # compile-only — fast sanity check
uv run dbt build --project-dir dbt --profiles-dir dbt                    # run + test all models
uv run dbt run --project-dir dbt --profiles-dir dbt                      # run only (no tests)
uv run dbt test --project-dir dbt --profiles-dir dbt                     # tests only
uv run dbt source freshness --project-dir dbt --profiles-dir dbt         # bronze freshness check
uv run dbt docs generate --project-dir dbt --profiles-dir dbt            # generate docs catalog
uv run dbt docs serve --project-dir dbt --profiles-dir dbt               # serve docs at localhost:8080
```

### Model selection

```bash
uv run dbt run --project-dir dbt --profiles-dir dbt --select stg_cpsc_recalls         # one model
uv run dbt run --project-dir dbt --profiles-dir dbt --select stg_cpsc_recalls+        # model + everything downstream
uv run dbt run --project-dir dbt --profiles-dir dbt --select +recall_event            # model + everything upstream
uv run dbt run --project-dir dbt --profiles-dir dbt --select +recall_event+           # both directions
uv run dbt run --project-dir dbt --profiles-dir dbt --select tag:silver               # by tag
```

Compiled SQL output: `dbt/target/compiled/<project>/<path>/<model>.sql`. Inspect when a model surprises you.

dbt reads `NEON_HOST` / `NEON_USER` / `NEON_PASSWORD` / `NEON_DBNAME` (separate from `Settings`'s `NEON_DATABASE_URL` per [`development.md` § Design rationale](development.md#design-rationale--strict-scope-on-env)).

See also: [`development.md` § Running dbt locally](development.md#running-dbt-locally), [ADR 0011](decisions/0011-transformation-framework-dbt-core.md), [ADR 0015](decisions/0015-testing-strategy.md).

---

## pre-commit — git hooks

```bash
uv run pre-commit install                # install the hook (one-time per clone)
uv run pre-commit run --all-files        # run all hooks once across the tree
uv run pre-commit run ruff --all-files   # one hook by id
uv run pre-commit autoupdate             # bump hook versions in .pre-commit-config.yaml
```

Hooks: ruff check, ruff format, pyright, gitleaks, the custom `check_pydantic_strict.py`, and the cassette-secret-scrub verifier. See [ADR 0018](decisions/0018-ci-posture.md), [ADR 0016](decisions/0016-secrets-management.md).

---

## bru — Bruno CLI (API exploration)

```bash
bru run bruno/<source>/<request>.bru                    # run one request
bru run bruno/<source>/                                 # run a folder
bru run -e dev bruno/<source>/                          # run with the dev environment
bru run --reporter-html out.html bruno/<source>/        # produce a report
```

Bruno reads credentials via `{{process.env.<VAR>}}` from your shell — direnv or Proton Pass loads them naturally. Don't hardcode secrets in `.bru` files.

See also: [`development.md` § API exploration with Bruno](development.md#api-exploration-with-bruno).

---

## R2 (Cloudflare) — raw payload inspection

R2 is S3-compatible. Use `aws` CLI with `--endpoint-url` pointing at your R2 endpoint:

```bash
export R2_ENDPOINT="https://${R2_ACCOUNT_ID}.r2.cloudflarestorage.com"

aws s3 ls s3://${R2_BUCKET_NAME} --endpoint-url $R2_ENDPOINT                 # list top level
aws s3 ls s3://${R2_BUCKET_NAME}/cpsc/2026-04-25/ --endpoint-url $R2_ENDPOINT   # inspect a day
aws s3 cp s3://${R2_BUCKET_NAME}/cpsc/2026-04-25/<key> - --endpoint-url $R2_ENDPOINT | jq    # fetch + pretty-print
aws s3 cp <local> s3://${R2_BUCKET_NAME}/<key> --endpoint-url $R2_ENDPOINT       # upload (rare; the loader handles this)
```

Wrangler (Cloudflare-native, more terse) is an alternative:

```bash
wrangler r2 object list <bucket>
wrangler r2 object get <bucket>/<key>
```

**Important:** `R2_BUCKET_NAME` should be the *dev* bucket (`consumer-product-recalls-dev`) when running locally — see [`development.md` § Dev vs. production isolation](development.md#dev-vs-production-isolation).

See also: [ADR 0004](decisions/0004-four-layer-medallion-pipeline.md), [ADR 0005](decisions/0005-storage-tier-neon-and-r2.md), [ADR 0028](decisions/0028-backfill-historical-reextraction-semantics.md) (R2 is the substrate for re-ingest).

---

## neonctl — Neon CLI

```bash
neonctl auth                                                              # one-time login
neonctl projects list                                                     # all projects
neonctl branches list --project-id <id>                                   # branches in a project
neonctl branches create --project-id <id> --name dev --parent main        # fork dev from main
neonctl branches delete --project-id <id> dev                             # delete dev (be careful)
neonctl connection-string --project-id <id> --branch-name dev             # get a DSN
neonctl operations list --project-id <id>                                 # see what's in flight
```

The dev/main branching pattern is described in [ADR 0005](decisions/0005-storage-tier-neon-and-r2.md). Phase 7 plans to use Neon branching for integration-test DBs (per ADR 0015) — until then, dev branch is shared local scratch.

---

## psql — direct SQL

```bash
psql $NEON_DATABASE_URL                                                   # interactive
psql $NEON_DATABASE_URL -c "select 1"                                     # one-shot
psql $NEON_DATABASE_URL -f scripts/sql/cpsc/bronze/explore_cpsc_bronze.sql  # run a script
psql $NEON_DATABASE_URL -c "\copy bronze_table TO 'out.csv' CSV HEADER"   # export CSV
```

### Interactive shortcuts (inside psql)

```
\d <table>                # describe a table
\dt                       # list tables in current schema
\dn                       # list schemas
\df+                      # list functions with definitions
\timing on                # show query duration
\x                        # toggle expanded display (useful for wide rows)
\q                        # quit
\i <file.sql>             # run a script from inside the session
\watch 5                  # repeat last query every 5s
```

For canonical operational queries (extraction-run inspection, freshness, rejected-row triage), see [`operations.md` § Monitoring](operations.md#monitoring). For the SQL exploration scripts library, see `scripts/sql/<source>/<layer>/`.

---

## gh — GitHub CLI (workflow management)

```bash
gh workflow list                                                  # all workflows
gh workflow run extract-cpsc.yml                                  # manual dispatch
gh workflow run deep-rescan-cpsc.yml -f LastPublishDateStart=2005-01-01    # with input
gh run list --workflow=extract-cpsc.yml --limit 10                # recent runs
gh run view <run-id> --log                                        # full log
gh run view <run-id> --log | jq 'select(.event == "request_completed")'    # filter JSON logs
gh run rerun <run-id>                                             # rerun failed
gh run watch <run-id>                                             # tail in real time
gh secret list                                                    # repo secrets (names only)
gh secret set NEON_DATABASE_URL                                   # set/rotate (prompts for value)
```

See also: [`operations.md` § Secret rotation runbooks](operations.md#secret-rotation-runbooks), [ADR 0010](decisions/0010-ingestion-cadence-and-github-actions-cron.md).

---

## Cross-cutting recipes

### Daily extraction simulation

```bash
# 1. Routine extracts (no flags — mirrors what cron will do)
recalls extract cpsc
recalls extract fda
recalls extract usda
recalls extract usda_establishments

# 2. Pipeline-health snapshot
psql -f scripts/sql/_pipeline/recent_runs.sql
psql -f scripts/sql/_pipeline/quarantine_check.sql
psql -f scripts/sql/_pipeline/watermark_health.sql
```

Then if anything in the snapshot looks off for a specific source, drill in with that source's `verify_schema_rebaseline_wave.sql` (the file works for any run, not just rebaselines — pass the routine run's run_id from the log).

#### What to actually look at in the output

- **`recent_runs.sql` query 1** — every source has a row, all show `status=success`, `change_type=routine`. If a row is missing, that source didn't run; if status is anything else (`aborted` = rejection-rate threshold exceeded, `failed` = exception raised), look at `error_excerpt`.
- **`recent_runs.sql` query 3** (daily volume) — `total_inserted` per source per day should be small for routine extracts (single digits to low dozens for daily windows). A sudden jump to thousands means the watermark broke.
- **`quarantine_check.sql` query 1** — `last_24h` and `last_7d` columns. If they were 0 yesterday and >0 today, the source published a record your schema rejected. Drill into query 3 for the actual `failure_reason`.
- **`watermark_health.sql` query 2** — `watermark_status` column should say "advanced this run" for any source that inserted records. "STUCK — investigate" is the alarm; "stuck (no new records — likely benign)" is fine.

#### Two things you may notice on day-1

1. **CPSC's watermark is far in the past** (currently `2025-04-21` from your earlier override). Tomorrow's routine run will fetch ~143 records (the buffer-window backfill) plus anything published today. After that one run, the watermark advances to ~today and subsequent days will fetch single digits.
2. **USDA recalls and USDA establishments will fetch the full payload every run** (~2002 and ~7945 records respectively) regardless of watermark — that's by design per Finding D. The `inserted` count should be tiny (1-10) once bronze is stable; the high `extracted` count is normal.

### Fresh-clone setup, end to end

```bash
git clone <repo> && cd consumer-product-recalls
uv sync --frozen
cp .env.example .env && $EDITOR .env       # fill in dev creds — see development.md § Environment variables
direnv allow                               # if using direnv
uv run alembic upgrade head                # provision the dev Neon branch
uv run pytest                              # sanity check
uv run dbt parse --project-dir dbt         # verify dbt config
```

### "What just ran?"

```bash
psql $NEON_DATABASE_URL -c "
  select source, started_at, status, records_extracted, records_inserted, change_type
  from extraction_runs order by started_at desc limit 10
"
```

### "Why is this source failing?"

```bash
gh run list --workflow=extract-<source>.yml --status=failure --limit 3
gh run view <run-id> --log | jq 'select(.level == "error")'
psql $NEON_DATABASE_URL -c "
  select failure_stage, failure_reason, count(*)
  from <source>_recalls_rejected
  where rejected_at >= now() - interval '7 days'
  group by 1, 2 order by count(*) desc
"
```

### Re-record one source's cassettes

```bash
uv run pytest --vcr-record=rewrite tests/integration/test_<source>_live_cassettes.py
grep -ri "authorization\|api[_-]key" tests/fixtures/cassettes/<source>/    # scrub check
git diff tests/fixtures/cassettes/<source>/                                # what changed
```

### Verify an R2 payload landed

```bash
aws s3 ls s3://${R2_BUCKET_NAME}/<source>/$(date -u +%Y-%m-%d)/ --endpoint-url $R2_ENDPOINT
aws s3 cp s3://${R2_BUCKET_NAME}/<source>/<date>/<key> - --endpoint-url $R2_ENDPOINT | jq '. | length'
```

---

## See also

- [`cli.md`](cli.md) — deep reference for the `recalls` CLI (flags, change types, per-source quirks)
- [`development.md`](development.md) — narrative onboarding, environment setup, debugging walkthroughs
- [`operations.md`](operations.md) — production runbooks, monitoring queries, troubleshooting
- [`architecture.md`](architecture.md) — system shape and load-bearing invariants
- [`decisions/`](decisions/) — full rationale for every architectural choice
