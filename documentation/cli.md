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

There are three commands: [`version`](#recalls-version),
[`extract`](#recalls-extract-source), and
[`deep-rescan`](#recalls-deep-rescan-source).

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
| `usda` | **Ignored.** USDA has no usable server-side date filter (Finding D in [`usda/recall_api_observations.md`](usda/recall_api_observations.md)); the extractor pulls the full payload every run regardless. The flag is accepted for CLI shape parity and prints a notice. |
| `usda_establishments` | **Ignored.** No incremental cursor exists (Finding A); same shape-parity rationale, prints a notice. |
| `nhtsa` | **Ignored.** Flat-file full-dump every run (Findings B + C); accepted for CLI shape parity, prints a notice. |

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
```

#### Argument

- `source` (required) — one of:
  - `fda` — date-windowed deep rescan (window required)
  - `usda` — accepts the command but ignores the date window (see below)
  - `nhtsa` — historical / deep-rescan that pulls both the PRE_2010 and POST_2010
    archives (covers the full 1966-present corpus). Date window flags are accepted
    but ignored (archives are partitioned by `DATEA` at the source, per Finding H Q2).

  CPSC's historical seed is handled separately — see
  [ADR 0028](decisions/0028-backfill-historical-reextraction-semantics.md).
  CPSC and `usda_establishments` do not have a deep-rescan path.

#### Options

##### `--start-date YYYY-MM-DD`, `--end-date YYYY-MM-DD`

Date window for the rescan. Inclusive of both endpoints.

| Source | Window flags |
|---|---|
| `fda` | **Required.** Command exits 1 if either is omitted. |
| `usda` | **Ignored.** USDA has no server-side date filter (Finding D); the loader pulls the full payload every run. Flags are accepted for CLI shape parity and print a notice. |
| `nhtsa` | **Ignored.** Archives are partitioned by `DATEA` at the source; the deep-rescan loader downloads both the PRE_2010 and POST_2010 archives unconditionally. Flags are accepted for CLI shape parity and print a notice. |

##### `--change-type TYPE`

Same allowed values as `extract`. Typical pairings for `deep-rescan`:

- `historical_seed` — one-time multi-year backfills.
- `routine` — periodic edit-detection rescans (the weekly USDA safety net per
  Finding N is one such case).
- `schema_rebaseline` / `hash_helper_rebaseline` — rare for `deep-rescan` but
  allowed for symmetry with `extract`.

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
- `deep-rescan fda` without `--start-date` or `--end-date`

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

# NHTSA historical seed (full 1966-present corpus: PRE_2010 + POST_2010 archives)
recalls deep-rescan nhtsa --change-type=historical_seed

# Periodic USDA safety-net rescan (Finding N)
recalls deep-rescan usda --change-type=routine

# ETag-validation audit run (usda + usda_establishments only)
recalls extract usda --change-type=etag_audit
recalls extract usda_establishments --change-type=etag_audit
```

---

## Source configuration

Source-level configuration (URL, timeout, ETag enabled flag, rate limit, deep-rescan seed URLs) lives in `config/sources/*.yaml`, not in `.env` or in code. Editing a YAML file takes effect on the next `recalls` invocation — there is no restart, no rebuild, no migration.

### YAML files

| File | `source_type` | Notes |
|---|---|---|
| `config/sources/cpsc.yaml` | `rest_api` | `RestApiSourceConfig` |
| `config/sources/fda.yaml` | `rest_api` | `RestApiSourceConfig`; `timeout_seconds: 60.0` |
| `config/sources/usda.yaml` | `rest_api` | `RestApiSourceConfig`; `etag_enabled: true` |
| `config/sources/usda_establishments.yaml` | `rest_api` | `RestApiSourceConfig`; `etag_enabled: true` |
| `config/sources/nhtsa.yaml` | `flat_file` | `FlatFileSourceConfig`; `historical_seed_urls` carries the PRE_2010 archive URL |

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
