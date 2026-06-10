# Project CLI — `recalls`

The `recalls` CLI is the entry point for running the pipeline manually or from
scheduled jobs. It is wired via `[project.scripts]` in
[`pyproject.toml`](../pyproject.toml) (`recalls = "src.cli.main:app"`) and built
on [Typer](https://typer.tiangolo.com/).

For the daily-driver cheat sheet across all tools, see
[`commands.md`](commands.md) § _recalls — project CLI_. **This file is the
deeper reference**: full flag semantics, per-source behavior quirks, and how to
choose between `extract` / `deep-rescan` and the `--change-type` values.

Source of truth: [`src/cli/main.py`](../src/cli/main.py).

---

## Invocation

Examples below use bare `recalls`, which assumes direnv has put `.venv/bin` on
`$PATH` (per
[`development.md` § Method 2](development.md#method-2--direnv-optional-recommended-for-regular-development)).
If direnv is not active, prefix every command with `uv run`:

```bash
recalls --help            # bare (direnv)
uv run recalls --help     # explicit
```

`recalls --help` lists subcommands; `recalls <subcommand> --help` lists that
subcommand's flags.

---

## Commands

There are seven commands: [`version`](#recalls-version),
[`extract`](#recalls-extract-source),
[`deep-rescan`](#recalls-deep-rescan-source),
[`re-ingest`](#recalls-re-ingest-source),
[`recover-rejected`](#recalls-recover-rejected-source),
[`resolve-firms`](#recalls-resolve-firms),
[`audit-firm-rollups`](#recalls-audit-firm-rollups), and
[`parse-quantities`](#recalls-parse-quantities).

### `recalls version`

Prints the package version. Useful as a sanity check that the install is wired
up correctly.

```bash
$ recalls version
consumer-product-recalls 0.1.0
```

The version is read from package metadata via
`importlib.metadata.version("consumer-product-recalls")`, so the only place to
edit when bumping is `version` in [`pyproject.toml`](../pyproject.toml).

### `recalls extract <source>`

Runs the **incremental** extractor for one source. This is the daily-driver
command — for sources that support a watermark, each run picks up where the
last left off.

```bash
recalls extract <source> [--lookback-days N] [--change-type TYPE]
```

#### Argument

- `source` (required) — one of:
  - `cpsc` — Consumer Product Safety Commission recalls
  - `fda` — FDA Imported Recall Enforcement (IRES) recalls
  - `fda_press_releases` — FDA press-release recall announcements (HTML scrape)
  - `usda` — USDA FSIS recalls
  - `usda_establishments` — USDA FSIS establishments database
  - `nhtsa` — NHTSA flat-file recall data (per [ADR 0008](decisions/0008-nhtsa-flat-file-primary-api-for-vehicle-lookup.md))
  - `uscg` — USCG boating-defect recalls (HTML scrape; live since 2026-05-15)
  - `uscg_manufacturers` — USCG manufacturer-identification (MIC) directory
  - `uscg_manufacturer_details` — USCG per-MIC detail pages (succession lineage)

#### Options

##### `--lookback-days N`

Override the persisted watermark with `today - N` (UTC). Use this to re-pull a
recent window without manually editing `source_watermarks` in the DB.

Per-source behavior:

| Source | `--lookback-days` effect |
|---|---|
| `cpsc` | **Effective.** Sets `source_watermarks.last_cursor` to `today - N` before the run. |
| `fda` | **Effective.** Same mechanism as CPSC. |
| `fda_press_releases` | **Ignored.** The press-release watermark defaults to `today - 1` on first run; use `--limit` for dev sampling. Accepted for CLI shape parity, prints a notice. |
| `usda` | **Ignored.** USDA has no usable server-side date filter (Finding D in [`usda/recall_api_observations.md`](usda/recall_api_observations.md)); the extractor pulls the full payload every run regardless. The flag is accepted for CLI shape parity and prints a notice. |
| `usda_establishments` | **Ignored.** No incremental cursor exists (Finding A); same shape-parity rationale, prints a notice. |
| `nhtsa` | **Ignored.** Flat-file full-dump every run (Findings B + C); accepted for CLI shape parity, prints a notice. |
| `uscg` | **Ignored.** Page-0 precheck + full HTML re-scrape on cache miss; accepted for CLI shape parity, prints a notice. |
| `uscg_manufacturers` | **Ignored.** Directory has no date-range query surface; full re-scrape on cache miss; accepted for CLI shape parity, prints a notice. |
| `uscg_manufacturer_details` | **Ignored.** Work-list is a listing-delta cursor over bronze; accepted for CLI shape parity, prints a notice. |

##### `--since YYYY-MM-DD`

NHTSA-only flag. For all other sources, `--since` is accepted but ignored with a notice.

When passed for `nhtsa`, the extractor drops rows whose `RCDATE` is earlier than the
given date. Intended for free-tier-aware dev workflows on the Neon dev branch — for
example, `recalls extract nhtsa --since 2024-01-01` loads only recent records and
avoids filling the dev branch with the full 1966-present corpus.

Important caveats:

- **Production historical seed uses the deep-rescan path**, which has no `--since`
  filter and lands the full corpus. Do not use `--since` for production backfills.
- **Validation happens upfront.** An invalid format (e.g., `--since 2024/01/01`)
  exits 1 with a clear message before any extraction or DB work begins.
- The filter is applied after fetching and decompressing the flat file — the full ZIP
  is still downloaded; rows are just not loaded to bronze.

##### `--limit N`

Cap the work-list to the first N items. Honored by `uscg_manufacturer_details`
(bronze-driven work-list) and `fda_press_releases` (event sweep). Ignored by
other sources with a notice. Minimum 1.

##### `--change-type TYPE`

How this run is labeled in `extraction_runs.change_type`. One of `routine`
(default), `schema_rebaseline`, `hash_helper_rebaseline`, `historical_seed`,
`etag_audit`.
See [§ Change types — explained](#change-types--explained) below.

The CLI validates this against the allowed set **before** any DB or HTTP work,
so a typo fails fast (exit 1) without burning an extraction-run row. The
database has a matching `CHECK` constraint as a backstop.

**Source restriction for `etag_audit`:** only `usda` and `usda_establishments`
accept `--change-type=etag_audit`. The CLI exits 1 with a clear error for other
sources. See [§ Change types — explained](#change-types--explained) for the full
rationale.

### `recalls deep-rescan <source>`

Runs a **historical / deep rescan** load over an explicit date window. Used for
one-time backfills (e.g., a multi-year historical seed per
[ADR 0028](decisions/0028-backfill-historical-reextraction-semantics.md)
Mechanism A) and as periodic edit-detection rescans where the upstream supports
them.

```bash
recalls deep-rescan <source> [--start-date YYYY-MM-DD] [--end-date YYYY-MM-DD] [--change-type TYPE]
    # fda_press_releases only:
    [--limit N] [--resume-after-event-id N]
    [--checkpointed [--batch-size N] [--since YYYY-MM-DD]
                    [--cooldown-base-seconds S] [--max-consecutive-failures N]]
    # uscg_manufacturer_details only:
    [--shard N --shard-count M]
```

#### Argument

- `source` (required) — one of:
  - `cpsc` — full-corpus historical seed (fixed `LastPublishDateStart=1970-01-01` floor; ~9,800 records every run). Date window flags are accepted but ignored with a notice.
  - `fda` — date-windowed deep rescan. Provide both `--start-date` and `--end-date` for a window; omit both for a full-corpus historical seed (three-way: neither → full-corpus, both → windowed, exactly one → error).
  - `fda_press_releases` — full event sweep, chunked via `--limit` + `--resume-after-event-id` or `--checkpointed` (resumable recent-first). Date window flags are accepted but ignored.
  - `usda` — accepts the command but ignores the date window (full-dump every run, per Finding D).
  - `nhtsa` — historical / deep-rescan that pulls both the PRE_2010 and POST_2010 archives (covers the full 1966-present corpus). Date window flags are accepted but ignored (archives are partitioned by `DATEA` at the source, per Finding H Q2).
  - `uscg` — full re-scrape of the USCG recalls listing. Date window flags accepted but ignored.
  - `uscg_manufacturers` — full re-scrape of the USCG MIC directory. Date window flags accepted but ignored.
  - `uscg_manufacturer_details` — full sweep of per-MIC detail pages. Supports `--shard`/`--shard-count` for Tier-2 monthly 1/3 rotation (see below). Date window flags accepted but ignored.

  `usda_establishments` does not have a deep-rescan path.

#### Options

##### `--start-date YYYY-MM-DD`, `--end-date YYYY-MM-DD`

Date window for the rescan. Inclusive of both endpoints.

| Source | Window flags |
|---|---|
| `fda` | **Conditional.** Both dates required for a windowed rescan; omit both for the full-corpus historical seed; exactly one → exits 1. |
| `fda_press_releases` | **Ignored.** The event sweep uses `--limit`/`--resume-after-event-id` or `--checkpointed` instead; date flags are accepted for CLI shape parity and print a notice. |
| `usda` | **Ignored.** USDA has no server-side date filter (Finding D); the loader pulls the full payload every run. Flags are accepted for CLI shape parity and print a notice. |
| `nhtsa` | **Ignored.** Archives are partitioned by `DATEA` at the source; the deep-rescan loader downloads both the PRE_2010 and POST_2010 archives unconditionally. Flags are accepted for CLI shape parity and print a notice. |
| `cpsc`, `uscg`, `uscg_manufacturers`, `uscg_manufacturer_details` | **Ignored.** No date-range query surface; flags are accepted for CLI shape parity and print a notice. |

##### `--change-type TYPE`

Same allowed values as `extract`. Typical pairings for `deep-rescan`:

- `historical_seed` — one-time multi-year backfills.
- `routine` — periodic edit-detection rescans (the weekly USDA safety net per
  Finding N is one such case).
- `schema_rebaseline` / `hash_helper_rebaseline` — rare for `deep-rescan` but
  allowed for symmetry with `extract`.

##### `--limit N`, `--resume-after-event-id N` (`fda_press_releases` only)

Chunk the event sweep so it fits under the GitHub Actions 6h job limit.
`--limit N` caps the run to the first N events (by `recall_event_id`);
`--resume-after-event-id N` skips events with `recall_event_id <= N` — the
hand-off cursor between chunks. Content-hash dedup makes overlapping runs
idempotent. Ignored by other sources.

##### `--checkpointed` (`fda_press_releases` only)

Runs the resumable, recent-first full-sweep seed (`recall_initiation_dt DESC`).
Lands, loads, and checkpoints per `--batch-size` events; resumes from the DB
cursor (`deep_rescan_checkpoints`), so a crash or throttle costs at most one
batch. Mutually exclusive with `--limit`/`--resume-after-event-id`. Optional
controls:

- `--batch-size N` — events per batch (default 250; minimum 1).
- `--since YYYY-MM-DD` — floor the work-list to `recall_initiation_dt >= DATE`
  (date-unknown events kept). Omit for the full sweep.
- `--cooldown-base-seconds S` — initial wait after a transient/throttle failure
  (default 120); doubles each consecutive failure, capped at 30 min.
- `--max-consecutive-failures N` — circuit breaker: abort after N consecutive
  failed batches (default 6); the cursor is preserved so a re-run resumes cleanly.

##### `--shard N`, `--shard-count M` (`uscg_manufacturer_details` only)

Runs strided shard N (0-based) of M of the full Tier-2 detail work-list, so a
single monthly job fits under the GitHub Actions 6h cap. The monthly cron
workflow derives `--shard` from the calendar month, covering the full corpus per
quarter at a 1/3 rotation. `--shard` and `--shard-count` must be given together;
`--shard` must be in `[0, M)`. The sharding is stateless — no persistent
per-shard cursor. Ignored by other sources.

```bash
# Run shard 0 of 3 (first third of the MIC detail work-list)
recalls deep-rescan uscg_manufacturer_details --shard 0 --shard-count 3
```

---

## `recalls re-ingest <source>`

Replays landed R2 payloads through the current schema, without re-contacting the
source. This is [ADR 0028](decisions/0028-backfill-historical-reextraction-semantics.md)
Mechanism B (R2 replay) — for recovering from a schema-drift or normalizer bug:
fix the Pydantic schema, then re-ingest the affected window.

```bash
recalls re-ingest <source> \
    --from-date YYYY-MM-DD --to-date YYYY-MM-DD \
    --change-type TYPE \
    [--dry-run] [--force]
```

Supported sources: `cpsc`, `fda`, `usda`, `usda_establishments`,
`fda_press_releases`. NHTSA and USCG are cheaply re-fetchable — use
`deep-rescan` instead.

`--change-type` is required and must be one of `schema_rebaseline` or
`hash_helper_rebaseline`. A `routine` label would synthesize false edits in
`recall_event_history`.

`--dry-run` reports the candidate payload count without writing. `--force`
replays payloads even if a prior re-ingest already replayed them (default skips
already-replayed originals via `extraction_runs.replayed_from_run_id`).

---

## `recalls recover-rejected <source>`

Re-loads quarantined (`*_rejected` table) records that were confirmed false
positives — without re-fetching from the API. Reads the stored payload,
reconstructs the record, and passes it through `BronzeLoader` (content-hash
idempotent). Bypasses `check_invariants` by design; only run after a census
confirms the rejection class is a false positive.

```bash
recalls recover-rejected <source> \
    [--landing-path PATH] \
    [--reason-contains TEXT] \
    [--dry-run]
```

Supported for sources that call `check_date_sanity`; exits 1 for unsupported
sources with the supported list.

Options:

- `--landing-path PATH` — scope to a specific `raw_landing_path` (default: the
  source's most-recent rejection).
- `--reason-contains TEXT` — override the default predicate: recover
  invariant-stage rejections whose `failure_reason` contains `TEXT`. Default
  scope is the confirmed `>70 years in the past` date-sanity class. Census the
  rejected table first (`scripts/sql/<source>/bronze/`) before using this flag.
- `--dry-run` — reconstruct and report the recovery plan without writing.

---

## `recalls resolve-firms`

Rebuilds `firm_crosswalk` from all-source staging firm names (Phase 6b firm
resolution). Reads the `stg_*` views, normalizes each name through
`src.enrichment.firm_normalization`, and truncate-reloads `firm_crosswalk` keyed
by `md5(upper(trim(name)))`. Idempotent; no API or watermark side effects.

**Run AFTER `dbt build --select staging`** (reads the `stg_*` views), before the
full dbt build. The `transform.yml` cron workflow sequences this automatically.

```bash
recalls resolve-firms [--dry-run] [--rollup/--no-rollup] [--rollup-threshold N] [--fei-merge/--no-fei-merge]
```

Options:

- `--dry-run` — report row counts without writing `firm_crosswalk`.
- `--rollup / --no-rollup` — Tier 2 entity rollup (≥2 shared distinctive
  tokens). On by default; `--no-rollup` ships Tier 1 only (near-identical name
  repair).
- `--rollup-threshold N` — Tier 2 `token_set_ratio` merge threshold 0–100
  (default 90; higher = stricter).
- `--fei-merge / --no-fei-merge` — DEFERRED (default off, per
  [ADR 0037](decisions/0037-firm-entity-resolution-crosswalk.md)): opt into Tier
  0 FDA-FEI merging. FEI is an attribute, not a merge key.

---

## `recalls audit-firm-rollups`

Ranks Tier-2 (`rapidfuzz_rollup`) firm clusters by false-merge suspicion for
manual review. Part of the Phase 6b precision review loop described in
`operations.md` "Firm resolution review loop."

```bash
recalls audit-firm-rollups \
    [--out PATH] \
    [--reviewed-ok PATH] \
    [--low-score N]
```

Options:

- `--out PATH` — where to write the ranked review CSV (default:
  `data/exploratory/cross_source/firm_rollup_review.csv`).
- `--reviewed-ok PATH` — allowlist of confirmed-legit cluster signatures (one
  per line; `#` comments). Filtered out so each cycle shows only new merges
  (default: `documentation/audit/firm_rollup_reviewed_ok.txt`).
- `--low-score N` — rollups whose lowest score is below this threshold (or min
  Jaccard < 0.5) count as high-risk for the scheduled-alert tally (default 95.0).

Emits `GITHUB_OUTPUT` keys `high_risk_count`, `rollup_clusters`, and
`report_path` so the monthly GHA cron can open an issue when the high-risk tally
spikes. Read-only; run after `resolve-firms`.

---

## `recalls parse-quantities`

Rebuilds `quantity_crosswalk` from the distinct FDA and USDA staging quantity
strings. Reads the distinct `product_distributed_quantity` (FDA) and
`qty_recovered` (USDA) strings from the `stg_*` views, parses each through
`src.enrichment.quantity.parse_quantity` into `(value, unit, category, basis)`,
and truncate-reloads `quantity_crosswalk`. Silver `recall_product` LEFT JOINs it
on `number_of_units` for the structured columns. Idempotent; no API or watermark
side effects.

**Run AFTER `dbt build --select staging`**, before the full dbt build. The
`transform.yml` cron workflow sequences this automatically.

```bash
recalls parse-quantities [--dry-run]
```

- `--dry-run` — report parse counts without writing `quantity_crosswalk`.

See `documentation/data_schemas.md` for the `quantity_crosswalk` field
definitions and the v1 precision scope (clean single-quantity shapes only; messy
~10% deferred to AI extractor v2 — see `project_scope/freetext-enrichment-backlog.md`).

---

## Change types — explained

`--change-type` labels the run in `extraction_runs.change_type` so downstream
logic — primarily `recall_event_history`'s edit-detection — can filter out
waves that aren't real upstream changes. The five allowed values:

| Value | Meaning | When to use |
|---|---|---|
| `routine` | Normal scheduled run. The default. | Every cron-driven run; manual reruns when no schema or hash logic has changed. |
| `schema_rebaseline` | A schema migration changed how records hash, so today's load may show diffs that aren't real upstream edits. | After any migration that adds, removes, or renames fields contributing to the row hash. Bronze and silver re-load the full payload to establish a new hash baseline. |
| `hash_helper_rebaseline` | The hashing helper itself changed (e.g., normalization rules), with the same downstream effect as a schema rebaseline. | After editing the hashing helper or normalization rules — anywhere a hash output could shift without a real upstream edit. |
| `historical_seed` | One-time multi-year backfill. | The CPSC 2005-2024 gap-fill (ADR 0028 Mechanism A). Filtered out of edit detection because the entire wave is "new to us, not new to the source." |
| `etag_audit` | One-time audit run that bypasses ETag conditional GET (forces unconditional 200) so the audit-check SQL can verify ETag-validation honesty. Only `usda` and `usda_establishments` accept this value; the CLI exits 1 for other sources. | After enabling ETag in production, periodically (~weekly) to convert inferential trust into directly measured verification. See [`documentation/usda/establishment_api_observations.md`](usda/establishment_api_observations.md) Finding A addendum (2026-05-10) for the audit-run pattern; `scripts/sql/_pipeline/etag_audit_check.sql` for the verification SQL. |

See
[ADR 0027](decisions/0027-bronze-storage-forced-transforms-only.md) and
[ADR 0028](decisions/0028-backfill-historical-reextraction-semantics.md)
for full rationale.

---

## Exit codes

Typer defaults — `0` on success, `1` on any error. Errors that exit 1 before
the extractor runs:

- Unknown source argument
- Invalid `--change-type` value
- `deep-rescan fda` with exactly one of `--start-date`/`--end-date` (must give both or neither)
- `--change-type=etag_audit` for a source other than `usda` or `usda_establishments`
- `deep-rescan --checkpointed` for a source other than `fda_press_releases`
- `--checkpointed` combined with `--limit` or `--resume-after-event-id`
- `--shard` without `--shard-count` (or vice versa)
- `--shard` out of range `[0, shard_count)`
- `--limit < 1`

If the extractor itself raises after starting, the run row's `status` is set to
`failed` and a stack trace excerpt is persisted before the process exits non-zero.

---

## Common workflows

The full cross-cutting recipes live in [`commands.md`](commands.md) §
_Cross-cutting recipes_. The most common shapes:

```bash
# Daily extraction simulation (mirrors the cron shape)
recalls extract cpsc
recalls extract fda
recalls extract usda
recalls extract usda_establishments

# NHTSA weekly flat-file extraction
recalls extract nhtsa

# NHTSA dev-mode: restrict to recent RCDATE (free-tier-aware; dev branch only)
recalls extract nhtsa --since 2024-01-01

# Re-pull the last week to debug
recalls extract cpsc --lookback-days 7

# Rebaseline after a schema or hash-helper change
recalls extract <source> --change-type=schema_rebaseline
recalls extract <source> --change-type=hash_helper_rebaseline

# One-time historical seed for FDA over a year
recalls deep-rescan fda \
    --start-date 2020-01-01 --end-date 2020-12-31 \
    --change-type=historical_seed

# FDA full-corpus historical seed (no date window)
recalls deep-rescan fda --change-type=historical_seed

# NHTSA historical seed (full 1966-present corpus: PRE_2010 + POST_2010 archives)
recalls deep-rescan nhtsa --change-type=historical_seed

# Periodic USDA safety-net rescan (Finding N)
recalls deep-rescan usda --change-type=routine

# USCG Tier-2 manufacturer details — monthly 1/3 strided shard
recalls deep-rescan uscg_manufacturer_details --shard 0 --shard-count 3

# FDA press-release seed — checkpointed (resumable, recent-first)
recalls deep-rescan fda_press_releases --checkpointed --change-type=historical_seed

# FDA press-release seed — manual chunked run (first 500 events)
recalls deep-rescan fda_press_releases --limit 500 --change-type=historical_seed

# ETag-validation audit run (usda + usda_establishments only)
recalls extract usda --change-type=etag_audit
recalls extract usda_establishments --change-type=etag_audit

# Firm resolution (run after dbt build --select staging, before full dbt build)
recalls resolve-firms
recalls resolve-firms --dry-run          # preview counts without writing
recalls resolve-firms --no-rollup        # Tier 1 only (name-variant repair)

# Audit firm rollup clusters for false merges (run after resolve-firms)
recalls audit-firm-rollups

# Parse quantity strings into structured crosswalk (run after dbt build --select staging)
recalls parse-quantities
recalls parse-quantities --dry-run       # preview parse counts without writing

# Recover quarantined-but-valid records (census the rejected table first)
recalls recover-rejected fda
recalls recover-rejected fda --dry-run   # preview candidates without writing

# R2 replay after schema fix — re-ingest a date window
recalls re-ingest fda \
    --from-date 2024-01-01 --to-date 2024-12-31 \
    --change-type=schema_rebaseline
```

---

## Source configuration

Source-level configuration (URL, timeout, ETag enabled flag, rate limit, deep-rescan seed URLs) lives in `config/sources/*.yaml`, not in `.env` or in code. Editing a YAML file takes effect on the next `recalls` invocation — there is no restart, no rebuild, no migration.

### YAML files

| File | `source_type` | Notes |
|---|---|---|
| `config/sources/cpsc.yaml` | `rest_api` | `RestApiSourceConfig` |
| `config/sources/fda.yaml` | `rest_api` | `RestApiSourceConfig`; `timeout_seconds: 60.0` |
| `config/sources/fda_press_releases.yaml` | `rest_api` | `RestApiSourceConfig`; HTML scrape of FDA press-release pages |
| `config/sources/usda.yaml` | `rest_api` | `RestApiSourceConfig`; `etag_enabled: true` |
| `config/sources/usda_establishments.yaml` | `rest_api` | `RestApiSourceConfig`; `etag_enabled: true` |
| `config/sources/nhtsa.yaml` | `flat_file` | `FlatFileSourceConfig`; `historical_seed_urls` carries the PRE_2010 archive URL |
| `config/sources/uscg.yaml` | `rest_api` | USCG recall listing HTML scrape |
| `config/sources/uscg_manufacturers.yaml` | `rest_api` | USCG MIC directory HTML scrape |
| `config/sources/uscg_manufacturer_details.yaml` | `rest_api` | USCG per-MIC detail pages; Tier-2 monthly 1/3 shard sweep |

### Wired fields — quick reference

| Field | Applies to | Effect |
|---|---|---|
| `base_url` | REST sources | Extractor constructor `base_url` arg |
| `file_url` | Flat-file sources | Extractor constructor `file_url` arg (= POST_2010 archive for NHTSA) |
| `timeout_seconds` | All | HTTP read timeout |
| `rate_limit_rps` | All | Currently `null` everywhere; reserved |
| `etag_enabled` | USDA recall + establishments | Enables/disables `If-None-Match` conditional GET |
| `historical_seed_urls` | NHTSA deep-rescan only | Additional archive URLs downloaded by `NhtsaDeepRescanLoader` |

All other fields in the YAML files (`incremental_filter_param`, `credentials`, `expected_field_count`, etc.) are validated but not yet wired to extractor constructors — see ADR 0012's Wave 2 implementation notes for the full wired-vs-intent breakdown.

### Cross-references

- [`development.md` § Source configuration](development.md#source-configuration) — full mental model, three-tier hierarchy, per-field tables, and the "how to safely edit" walkthrough
- [ADR 0012](decisions/0012-extractor-pattern-custom-abc-and-per-source-subclasses.md) — "Implementation notes — source-config loader and registry (Wave 2, landed 2026-05-10)"

---

## See also

- [`commands.md`](commands.md) — quick reference across all tools (uv, dbt,
  alembic, pytest, gh, psql, …)
- [`development.md`](development.md) § _Running extractors locally_ — first-time
  setup for running the CLI
- [`operations.md`](operations.md) § _Re-ingestion procedure_ — when and why to
  reach for `--change-type` flags
- [ADR 0027](decisions/0027-bronze-storage-forced-transforms-only.md) and
  [ADR 0028](decisions/0028-backfill-historical-reextraction-semantics.md) —
  why `change_type` exists and what `historical_seed` means
- [`src/cli/main.py`](../src/cli/main.py) — the source of truth
