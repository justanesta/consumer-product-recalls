# Development guide

This document covers local setup, environment configuration, and day-to-day development workflow. For architectural rationale behind choices described here, see the ADRs in `documentation/decisions/`. For system architecture and component relationships, see [`architecture.md`](architecture.md). For a tool-organized command cheat sheet, see [`commands.md`](commands.md).

---

## Prerequisites

- Python 3.12 or later
- `uv` (preferred) or `pip` for dependency management
- A Neon Postgres account (free tier) with a project provisioned — see [ADR 0005](decisions/0005-storage-tier-neon-and-r2.md)
- A Cloudflare account with R2 enabled (free tier) and a bucket created
- An FDA iRES API key, requested via OII Unified Logon — see [ADR 0012](decisions/0012-extractor-pattern-custom-abc-and-per-source-subclasses.md)
- **Optional:** `direnv` for automatic environment loading (see below)

---

## Initial setup

```bash
# 1. Clone the repository
git clone <repo-url>
cd consumer-product-recalls

# 2. Install dependencies into uv-managed venv at .venv/
uv sync --frozen

# 3. Configure environment — see Environment variables below
cp .env.example .env
$EDITOR .env
# (optional but recommended: install direnv and run `direnv allow` — see Method 2 below)

# 4. Provision the dev Neon branch and run migrations
#    (Neon project must already be provisioned per ADR 0005;
#     dev branch is forked from main once, then reused.)
uv run alembic upgrade head

# 5. Sanity-check: run the test suite
uv run pytest

# 6. Verify dbt parses
uv run dbt parse --project-dir dbt
```

After step 5, `pytest` should report all green; if any tests fail without code changes, your environment isn't set up correctly — check `.env` first, then verify the dev Neon branch is reachable via `psql $NEON_DATABASE_URL -c 'select 1'`.

Step 4 also seeds the `source_watermarks` table with one row per source per [Phase 1 baseline migration](../migrations/versions/0001_baseline.py); without it, extractor runs would fail on the FK insert.

The `recalls` CLI is wired via `[project.scripts]` in `pyproject.toml` — invoke as `uv run recalls <subcommand>` after step 2.

---

## Git workflow

### Branching convention

The `main` branch is protected per [ADR 0018](decisions/0018-ci-posture.md): all changes merge via pull request, CI checks must pass, no direct pushes. Feature work happens on branches named with a type prefix that signals the nature of the change:

| Prefix | Use for | Example |
|---|---|---|
| `feature/` | New features or capabilities | `feature/phase-1-scaffolding`, `feature/cpsc-extractor`, `feature/firm-entity-resolution` |
| `fix/` | Bug fixes | `fix/cpsc-pagination-termination`, `fix/usda-english-fallback` |
| `docs/` | Documentation-only changes | `docs/adr-0020-frontend-framework`, `docs/update-operations-runbook` |
| `chore/` | Tooling, config, maintenance, dependency bumps | `chore/bump-ruff`, `chore/renovate-config` |

When a branch corresponds to a specific implementation phase (see [`project_scope/implementation_plan.md`](../project_scope/implementation_plan.md)) or to a specific ADR, reference that in the branch name. The git log becomes self-documenting and reviewers can jump directly to the relevant context.

### Pull requests

- Link to the relevant ADR(s) and implementation phase in the PR description when applicable.
- CI status checks must pass before merging (per ADR 0018).
- Self-review counts as review under the solo-contributor workflow; add formal reviewers if contributors join.

---

## Environment variables

All credentials and environment-specific configuration live in a `.env` file at the project root. This file is **gitignored** and must never be committed. See [ADR 0016](decisions/0016-secrets-management.md) for the full rationale.

### Design rationale — strict scope on `.env`

`pydantic-settings` loads `.env` into the `Settings` class with `extra='forbid'` (per [ADR 0014](decisions/0014-schema-evolution-policy.md) / [ADR 0016](decisions/0016-secrets-management.md)). Two consequences:

- **Missing required field** — something declared in `Settings` isn't in `.env`. `Settings()` raises `ValidationError` at process boot, naming the missing field. Loud, obvious, fix-it-and-move-on.
- **Extra input** — something in `.env` isn't declared in `Settings`. Same loud error, naming the extra key. A typo like `NEON_DATABSE_URL=...` produces an immediate failure, not a silent misconfiguration that runs against the wrong database.

This contract makes `Settings` the complete specification of what's allowed in `.env`. To add a new variable, declare it as a field on `Settings` first, then add it to `.env`.

**Implication for dbt-only variables.** dbt reads its own connection-string components directly from `os.environ`, not via `pydantic-settings`. If you add dbt-specific split variables (e.g., `NEON_HOST`, `NEON_USER`, `NEON_PASSWORD`, `NEON_DATABASE`) to `.env`, they'll be rejected by `extra='forbid'` because they're not declared on `Settings`.

The clean solution is **not** to weaken `extra='forbid'`. Instead, set dbt-only variables outside `.env` — either in your shell rc (`export NEON_HOST=...`), in a `.env.dbt` file that pydantic-settings doesn't read, or in a direnv-managed environment that scopes them. The Settings class stays the complete spec for the Python codebase; dbt's environment is a separate scope.

### Method 1 — `.env` with manual sourcing (simplest, no extra tools)

Copy the template and edit:

```bash
cp .env.example .env
$EDITOR .env
```

Your `.env` should look like:

```bash
NEON_DATABASE_URL=postgresql://user:pass@ep-xxx-dev.neon.tech/recalls?sslmode=require

R2_ACCOUNT_ID=your_cloudflare_account_id
R2_ACCESS_KEY_ID=your_dev_r2_access_key
R2_SECRET_ACCESS_KEY=your_dev_r2_secret_access_key
R2_BUCKET_NAME=consumer-product-recalls-dev

FDA_AUTHORIZATION_USER=your_oii_user
FDA_AUTHORIZATION_KEY=your_oii_key
```

Note the bucket name is `consumer-product-recalls-dev` (not the production bucket `consumer-product-recalls`) — see [Dev vs. production isolation](#dev-vs-production-isolation) below for why.

When running pipeline code directly, `pydantic-settings` reads `.env` automatically at process boot — no manual sourcing required. For ad-hoc commands that need env vars in your shell (e.g. `psql $NEON_DATABASE_URL`), source the file manually:

```bash
set -a; source .env; set +a
```

This works but is tedious. See Method 2 for an automatic alternative.

### Method 2 — `direnv` (optional, recommended for regular development)

`direnv` is a shell extension that automatically loads and unloads environment variables based on your current directory. When you `cd` into the project, your shell's environment is populated from the project's `.envrc` file; when you `cd` out, the variables are removed.

#### Installing direnv

| Platform | Command |
|---|---|
| macOS (Homebrew) | `brew install direnv` |
| Ubuntu/Debian | `sudo apt install direnv` |
| Fedora | `sudo dnf install direnv` |
| Arch | `sudo pacman -S direnv` |
| Other | See [direnv installation docs](https://direnv.net/docs/installation.html) |

After installation, hook direnv into your shell by adding **one** of the following to your shell rc file:

```bash
# ~/.bashrc
eval "$(direnv hook bash)"

# ~/.zshrc
eval "$(direnv hook zsh)"

# ~/.config/fish/config.fish
direnv hook fish | source
```

Restart your shell.

#### Using the committed `.envrc`

A template `.envrc` is committed at the project root. It contains no secrets — only references to `.env` (gitignored) and venv-bin-path exposure.

```bash
# .envrc (committed, no secrets — references .env which is gitignored)
dotenv .env

# uv manages the virtualenv at .venv/ — create/update it with `uv sync`.
# direnv just adds the venv's bin dir to PATH so python/pytest/dbt/etc. resolve
# without requiring `source .venv/bin/activate`.
PATH_add .venv/bin

export PYTHONPATH="$(pwd)/src:$PYTHONPATH"
```

**Note:** earlier drafts of this doc used `layout python python3.12`, which is direnv's stdlib helper for standard virtualenv-managed venvs. That's incompatible with uv, which manages `.venv/` itself — using both would create two parallel venvs. The `PATH_add` approach above is the correct uv-compatible pattern.

The first time you `cd` into the project after installing direnv, you'll see:

```
direnv: error .envrc is blocked. Run `direnv allow` to approve its content
```

This is direnv's security feature — each `.envrc` must be explicitly trusted once. Run:

```bash
direnv allow
```

From then on, every `cd` into the project auto-loads `.env`, prepends `.venv/bin` to `PATH` (so `pytest`, `dbt`, `python`, etc. resolve to the uv-managed venv), and adds `src/` to `PYTHONPATH`. Every `cd` out unsets them.

If you modify `.envrc`, you must `direnv allow` again — another security feature.

### Method 3 — `direnv` + Proton Pass CLI (optional, for developers who prefer secrets never on disk)

If you don't want credentials sitting in a `.env` file at all, the [Proton Pass CLI](https://protonpass.github.io/pass-cli/) ([repo](https://github.com/protonpass/pass-cli)) can resolve secrets from your Proton Pass vault at shell-entry time (with direnv) or at command-invocation time (without direnv). Either way, secrets live encrypted in your vault; cleartext values exist only transiently in process memory.

Proton Pass represents secrets as `pass://` URIs of the form `pass://<vault>/<item>/<field>`. Vault and item can be referenced by name or by Share ID / Item ID. Fields include the standard ones (`password`, `username`, `email`, `url`, `note`, `totp`) plus any custom fields you add to an item.

References: [secret references](https://protonpass.github.io/pass-cli/commands/contents/secret-references/), [`run` command](https://protonpass.github.io/pass-cli/commands/contents/run/), [`item view`](https://protonpass.github.io/pass-cli/commands/contents/view/).

#### Install the Proton Pass CLI

**macOS (Homebrew):**

```bash
brew install protonpass/tap/pass-cli
```

**macOS / Linux (install script):**

```bash
curl -fsSL https://proton.me/download/pass-cli/install.sh | bash
```

Requires `curl` and `jq` on your system.

**Verify:**

```bash
pass-cli --version
```

#### Log in

Interactive (browser-based, default):

```bash
pass-cli login
```

Unattended / scripted (for use from `.envrc` without opening a browser) — generate a Personal Access Token in Proton Pass, then either pass it inline:

```bash
pass-cli login --personal-access-token "pst_xxxxxxxxxxxx::TOKENKEY"
```

or via environment variable (stored in your shell's private startup file, not in this repo):

```bash
export PROTON_PASS_PERSONAL_ACCESS_TOKEN="pst_xxxxxxxxxxxx::TOKENKEY"
pass-cli login
```

The session persists across command invocations until `pass-cli logout`.

#### Store project secrets in Proton Pass

Create an item in one of your vaults — for example, an item titled `Consumer Product Recalls` in vault `Work` — with the following custom fields:

- `NEON_DATABASE_URL`
- `R2_ACCESS_KEY_ID`
- `R2_SECRET_ACCESS_KEY`
- `FDA_AUTHORIZATION_USER`
- `FDA_AUTHORIZATION_KEY`

Non-secret configuration values (`R2_ACCOUNT_ID`, `R2_BUCKET_NAME`) can either live alongside the secrets in Proton Pass or stay in a plain `.env` — either works.

Two integration patterns follow. Pattern A uses direnv (what you'd expect from this document's flow). Pattern B uses Proton Pass's native `run` command and doesn't need direnv at all. Pattern A is more ergonomic for ad-hoc shell commands; Pattern B keeps secrets out of the shell entirely. Pick whichever fits your workflow.

#### Pattern A — direnv + `pass-cli item view` (secrets in shell env after `cd`)

Each secret is fetched once when direnv loads the environment, then lives in your shell's env vars. Any subsequent command (`uv run pytest`, `dbt build`, `psql $NEON_DATABASE_URL`) sees the values naturally, no wrapping needed.

Put the lookup logic in a **gitignored** `.envrc.local`:

```bash
# .envrc.local — gitignored; holds vault references (not secrets themselves)
set -euo pipefail

# Helper to extract a single field from a Proton Pass item as a bare string.
# Adjust the jq path if Proton Pass changes its JSON shape.
pp_field() {
  pass-cli item view "$1" --output json | jq -r ".fields[] | select(.name == \"$2\") | .value"
}

ITEM="pass://Work/Consumer Product Recalls"

export NEON_DATABASE_URL="$(pp_field "$ITEM" 'NEON_DATABASE_URL')"
export R2_ACCESS_KEY_ID="$(pp_field "$ITEM" 'R2_ACCESS_KEY_ID')"
export R2_SECRET_ACCESS_KEY="$(pp_field "$ITEM" 'R2_SECRET_ACCESS_KEY')"
export FDA_AUTHORIZATION_USER="$(pp_field "$ITEM" 'FDA_AUTHORIZATION_USER')"
export FDA_AUTHORIZATION_KEY="$(pp_field "$ITEM" 'FDA_AUTHORIZATION_KEY')"

# Non-secret config can stay inline (or come from Proton Pass the same way):
export R2_ACCOUNT_ID="your_cloudflare_account_id"
export R2_BUCKET_NAME="consumer-product-recalls-raw"
```

Then in the **committed** `.envrc`, source the local file if it exists (otherwise fall back to `.env`):

```bash
# .envrc (committed; safe — references gitignored files only)
if [ -f .envrc.local ]; then
  source_env .envrc.local
else
  dotenv .env
fi

# uv manages the virtualenv at .venv/ — create/update with `uv sync`.
PATH_add .venv/bin

export PYTHONPATH="$(pwd)/src:$PYTHONPATH"
```

Add `.envrc.local` to `.gitignore` so the vault references don't end up in version control (they aren't secrets, but they're environment-specific).

**Tradeoff:** every `cd` into the project runs one `pass-cli item view` per secret. With five secrets that's five vault round-trips, typically adding ~1–2 seconds to the `cd`. If this gets annoying, switch to Pattern B below.

#### Pattern B — `pass://` URIs in `.env` + `pass-cli run` at invocation time (no direnv needed)

Proton Pass's native pattern: put `pass://` URIs directly in `.env` and wrap your commands with `pass-cli run`. The CLI resolves URIs and injects real values into the child process's environment at the moment of execution. Secrets never enter your shell.

```bash
# .env — gitignored. Contains references, not secret values.
NEON_DATABASE_URL=pass://Work/Consumer Product Recalls/NEON_DATABASE_URL
R2_ACCESS_KEY_ID=pass://Work/Consumer Product Recalls/R2_ACCESS_KEY_ID
R2_SECRET_ACCESS_KEY=pass://Work/Consumer Product Recalls/R2_SECRET_ACCESS_KEY
FDA_AUTHORIZATION_USER=pass://Work/Consumer Product Recalls/FDA_AUTHORIZATION_USER
FDA_AUTHORIZATION_KEY=pass://Work/Consumer Product Recalls/FDA_AUTHORIZATION_KEY

# Non-secret values stay literal:
R2_ACCOUNT_ID=your_cloudflare_account_id
R2_BUCKET_NAME=consumer-product-recalls-raw
```

Then wrap commands that need secrets:

```bash
pass-cli run --env-file .env -- uv run pytest
pass-cli run --env-file .env -- uv run python -m src.cli extract cpsc
pass-cli run --env-file .env -- dbt build
```

`pass-cli run` also masks resolved secret values in the wrapped command's stdout/stderr by default, replacing them with `<concealed by Proton Pass>`. Pass `--no-masking` if you need raw output (careful with logs).

**When to prefer Pattern B:**

- You're allergic to secrets in shell env vars.
- You work on multiple projects and don't want cross-contamination.
- You're running commands in CI-like ways locally (reproducibility matters more than ergonomics).

**When Pattern A is fine:**

- You trust your local shell.
- You value running plain `uv run pytest` over `pass-cli run --env-file .env -- uv run pytest`.
- You're the only developer on this machine.

Either pattern is compatible with `pydantic-settings` — it sees env vars identically regardless of how they got there.

#### Troubleshooting Proton Pass + direnv

- **`pass-cli: command not found`:** Verify the CLI is installed and `~/.local/bin` (or wherever the installer put it) is in your `$PATH`.
- **`pass-cli` prompts for login on every `cd`:** Your session isn't persisting. Try `pass-cli login --personal-access-token "..."` once, then verify with `pass-cli info` before adding to `.envrc.local`.
- **`direnv: error` on `cd`:** Run `direnv allow` to re-trust the `.envrc` after any edit.
- **Slow `cd`:** Pattern A does one vault call per secret. Switch to Pattern B, or cache the values in `.envrc.local` if they rarely change (paste `.env`-style literals temporarily and re-fetch quarterly during secret rotation).
- **`jq: parse error`:** The `pp_field` helper in Pattern A assumes a specific JSON shape from `pass-cli item view --output json`. If Proton Pass changes the shape, adjust the jq query. Run `pass-cli item view "pass://vault/item" --output json | jq .` to inspect the current structure.

### Dev vs. production isolation

Local development never writes to production storage. The isolation is bucket/branch-level, not magic flag–level. Two pieces:

|             | Local (`.env`)                        | CI / production (GitHub Actions secrets) |
|-------------|---------------------------------------|------------------------------------------|
| **Neon**    | `dev` branch URL                      | `main` branch URL                        |
| **R2**      | `consumer-product-recalls-dev` bucket | `consumer-product-recalls` bucket        |

**Neon** has native branching ([ADR 0005](decisions/0005-storage-tier-neon-and-r2.md)). The `dev` branch is forked from `main` and shares no rows once they diverge. Your `.env` `NEON_DATABASE_URL` should point at the dev branch; production GHA secrets point at main.

**R2 has no branch concept**, so dev/prod isolation is bucket-level. Two buckets are provisioned ([ADR 0005](decisions/0005-storage-tier-neon-and-r2.md), [ADR 0016](decisions/0016-secrets-management.md)):

1. **Create the dev bucket** in Cloudflare R2: `consumer-product-recalls-dev`.
2. **Create a separate API token** scoped to that dev bucket only. Critical: a leaked dev token must not be able to reach the production bucket.
3. **Put the dev credentials in `.env`** (`R2_BUCKET_NAME=consumer-product-recalls-dev`, with the dev-scoped access key + secret).
4. **Production bucket credentials** live only in GitHub Actions secrets. Don't put them in any local file.

The application code doesn't care — `R2_BUCKET_NAME` is just a string. Your local extractions land in `consumer-product-recalls-dev`; CI lands in `consumer-product-recalls`. Same object key structure, different bucket. The point is to keep local-development raw payloads from polluting the production audit trail (and to make sure a leaked local credential cannot reach prod).

Same pattern applies to FDA credentials in principle, but FDA only issues one credential per OII Unified Logon account, so the same key is used in both contexts. Be cautious when running FDA extractors locally — they hit the live FDA API and consume your rate-limit budget for the day.

---

## Running tests

```bash
# Full pytest suite (unit + integration + e2e) — what CI runs
uv run pytest

# Subsets:
uv run pytest tests/unit/          # fast, no network
uv run pytest tests/integration/   # VCR-backed, no live network
uv run pytest tests/e2e/           # requires a reachable Neon dev branch

# Single test file or single test:
uv run pytest tests/integration/test_cpsc_live_cassettes.py
uv run pytest tests/integration/test_cpsc_live_cassettes.py::test_happy_path_recent

# dbt tests (requires database + dbt-only env vars per "Design rationale" above)
uv run dbt test --project-dir dbt

# Coverage — the 85% threshold lives in pyproject.toml and is enforced in CI only,
# but you can reproduce the CI run locally:
uv run pytest --cov=src --cov-fail-under=85
```

A few mechanics worth knowing:

- **VCR cassette mode.** Integration tests default to `--vcr-record=none` (replay-only). To re-record after a source API changes, see [Re-recording VCR cassettes](#re-recording-vcr-cassettes) below.
- **`respx` vs. VCR.** Hand-constructed error-path mocks (401, 429, 500, malformed records) use `respx`; happy-path tests use VCR cassettes recorded from the live API. The two coexist in the same test file. See [ADR 0015](decisions/0015-testing-strategy.md).
- **Coverage exclusions.** Files in `[tool.coverage.run]`'s `omit` list don't count toward the 85% threshold. Add to that list when introducing files where coverage isn't meaningful (e.g., `__init__.py`).
- **Test database isolation.** `tests/e2e/` uses a Neon branch via the `test_db_url` fixture (Phase 7 deliverable per [ADR 0015](decisions/0015-testing-strategy.md)). Until that lands, e2e tests run against your dev Neon branch — be aware they may leave bronze rows behind.

See [ADR 0015](decisions/0015-testing-strategy.md) for the full testing strategy.

---

## Re-recording VCR cassettes

When an agency's API changes (new field, renamed field, new enum value), cassettes need to be re-recorded against the live API. The command shape will be:

```bash
uv run pytest tests/integration/test_<source>_extractor.py --record-mode=rewrite
```

Full procedure documented in `documentation/operations.md` once the workflow is built.

### Managing cassette size

Cassette YAML files store the full decoded API response body. For sources that return large flat-array payloads (CPSC ~9,700 records, USDA ~2,001 records), live-recorded cassettes are 13–15 MB each. This is well under GitHub's 100 MB per-file hard limit, but the files accumulate in git history on every re-recording.

**Current state (Phase 5b):** CPSC (~42 MB) + FDA (<1 MB) + USDA (~28 MB) = ~70 MB of cassette data. Within comfortable bounds.

**When to act:** if total cassette data in the repo approaches ~300 MB, or if a single cassette exceeds ~50 MB (GitHub's soft warning threshold), apply one of the two strategies below.

#### Strategy 1 — Truncate cassettes to a representative sample (preferred first move)

The happy-path tests only need enough records to exercise schema coverage — field nullability, type coercions, optional fields, source-specific edge cases (USDA bilingual pairs, CPSC nested arrays, etc.). A sample of 50 records is sufficient; the full 2,001-record or 9,700-record payload is not needed.

**Planned automation:** `scripts/truncate_cassette.py` (not yet implemented — implement when first re-recording is needed). The script will:

1. Accept one or more cassette YAML paths and a `--max-records N` flag (default: 50).
2. Load the YAML, locate `interactions[*].response.body.string`.
3. Parse the body string as a JSON array.
4. Slice to the first N records.
5. Re-serialize to a compact JSON string and write back the YAML in-place.

Invocation after recording:

```bash
# Step 1 — record against the live API
uv run pytest --vcr-record=all tests/integration/test_<source>_live_cassettes.py \
    -k "<cassette tests>"

# Step 2 — truncate before committing
uv run python scripts/truncate_cassette.py \
    tests/fixtures/cassettes/<source>/test_happy_path_*.yaml \
    --max-records 50
```

Record the truncation step in each source's test file docstring alongside the recording command so it's not forgotten. After truncation, re-run the full test file to confirm the sliced cassette still passes all assertions (record count assertions should use `> 0`, not `== 2001`, so they survive truncation without edits).

**USDA note:** the current USDA cassettes are at 14 MB each and have not been truncated. Truncate them during the first re-recording rather than retroactively — there is no urgency at the current size.

#### Strategy 2 — Git LFS (fallback if truncation is insufficient)

If cassette files grow past ~50 MB individually (unlikely given truncation), or if git history bloat becomes an issue after many re-recordings, move cassettes to Git LFS:

```bash
git lfs track "tests/fixtures/cassettes/**/*.yaml"
git add .gitattributes
```

GitHub provides 1 GB LFS storage and 1 GB/month bandwidth on the free tier. Every contributor and CI runner then needs `git lfs install` and the CI workflow needs `git lfs pull` before running tests. This is a meaningful increase in onboarding friction — exhaust truncation first.

To rewrite history and move existing large files retroactively:

```bash
git filter-repo --path tests/fixtures/cassettes/ --use-mailmap
```

Run this on a fresh clone and force-push; coordinate with all active contributors to re-clone.

---

## API exploration with Bruno

[Bruno](https://www.usebruno.com/) is a free, open-source API client (similar to Postman or Insomnia) that stores collections as plain-text `.bru` files committed to git — collections-as-code rather than cloud-synced magic. That property makes it a natural fit for this project.

### Uses in this project

- **Exploring source APIs** before writing an extractor — iteratively call CPSC / FDA / USDA / NHTSA with various filter combinations to understand response shapes, edge cases, and pagination semantics. Faster and more reviewable than ad-hoc `curl` in the terminal.
- **Reference payloads for tests** — captured Bruno responses give the known shape for writing unit tests, before live VCR cassette recording is wired up (see [ADR 0015](decisions/0015-testing-strategy.md)).
- **Living documentation of source APIs** — a committed collection is the executable version of what's in `documentation/<source>/`. A new contributor can load it in Bruno and start poking immediately without reading the PDF specs first.
- **Production troubleshooting** — when an extractor fails, hit the source API directly with the same params/headers to isolate whether it's an API issue or our code.
- **Internal API development** — once the FastAPI serving layer exists (see Phase 8 of [`project_scope/implementation_plan.md`](../project_scope/implementation_plan.md)), Bruno collections document request shapes, save example calls, and support endpoint testing.

### Installation

Desktop app and [CLI](https://docs.usebruno.com/bru-cli/overview) (`bru`) available from [usebruno.com](https://www.usebruno.com/) — macOS, Windows, Linux, MIT licensed.

### Collection layout

Collections live at `bruno/` at the repo root, organized by source and by API consumer:

```
bruno/
  cpsc/              # CPSC API exploration
  fda/               # FDA iRES API (with auth flow)
  usda/              # USDA FSIS API
  nhtsa/             # NHTSA API + flat-file download URLs
  uscg/              # scraping-target pages for manual inspection
  internal-api/      # added in Phase 8 — our FastAPI endpoints
  environments/
    dev.bru          # committed — placeholder values, shape reference
    local.bru        # gitignored — real credentials for local use
```

### Secrets and environments

Bruno environments (`.bru` files under `bruno/environments/`) use `{{VAR}}` templating in URLs, headers, and bodies. Two patterns for populating them:

- **`local.bru`** — a gitignored environment file with real values. Treat it like `.env`.
- **Shell environment variables** via `{{process.env.FDA_AUTHORIZATION_KEY}}` — Bruno reads the calling shell's environment, which pairs naturally with the direnv-loaded or Proton Pass–sourced credentials from the [Environment variables](#environment-variables) section above. Single source of truth for secrets.

Prefer the shell-env approach when you're already using direnv or Proton Pass CLI — avoids credential duplication across `.env` and `local.bru`.

### CLI usage (future)

The `bru` CLI runs collections non-interactively, which opens the door to using Bruno as a layer of API contract tests in CI (e.g., "verify FDA's response shape for our canonical query hasn't drifted"). Out of scope for v1 but worth noting as a future enhancement if schema-drift detection needs to happen earlier than the next scheduled extraction.

---

## Running extractors locally

The CLI is wired via `[project.scripts]` in `pyproject.toml`; the entry point is `recalls`.

```bash
# Show available commands
uv run recalls --help

# Print the version (sanity check)
uv run recalls version

# Incremental extraction — uses the watermark in source_watermarks
uv run recalls extract cpsc
uv run recalls extract fda
uv run recalls extract usda
uv run recalls extract usda_establishments

# Override the watermark with a lookback window (useful when iterating)
uv run recalls extract cpsc --lookback-days 7
uv run recalls extract fda --lookback-days 30

# Inspect the extractor's effect: did rows land?
psql $NEON_DATABASE_URL -c "
  select source, started_at, status, records_extracted, records_inserted, change_type
  from extraction_runs
  order by started_at desc
  limit 10
"
```

**Important defaults and gotchas:**

- **Local runs write to the dev Neon branch and the dev R2 bucket** if your `.env` is configured per [Dev vs. production isolation](#dev-vs-production-isolation). Verify with `echo $R2_BUCKET_NAME` — it should end in `-dev` for local work.
- **FDA extractions hit the live FDA API**, consuming your OII rate-limit budget. Use `--lookback-days 1` to keep the request volume small while iterating.
- **Deep rescans** are separate workflows, not a CLI flag — they live in `.github/workflows/deep-rescan-<source>.yml` and are best invoked via `gh workflow run deep-rescan-cpsc.yml -f LastPublishDateStart=YYYY-MM-DD` or via the GH Actions UI's `workflow_dispatch` button. See [ADR 0010](decisions/0010-ingestion-cadence-and-github-actions-cron.md), [ADR 0028](decisions/0028-backfill-historical-reextraction-semantics.md).
- **Re-baseline runs** (after a Pydantic normalizer or hashing-helper change) require `--change-type=schema_rebaseline` per [ADR 0027](decisions/0027-bronze-storage-forced-transforms-only.md). The CLI flag exists; forgetting it on a real re-baseline pollutes `recall_event_history` with synthesized fake edits.

See [`operations.md`](operations.md) for production troubleshooting (rate limits, rejected-row triage, watermark issues).

---

## Pipeline state

Pipeline state lives in two Neon Postgres tables — `source_watermarks` (domain state: last-seen publication timestamps, ETags, cursors per source) and `extraction_runs` (one row per workflow invocation: status, counts, duration, a link back to the GitHub Actions run). No state files on disk, no state committed to the repo, no reliance on runner-local filesystem state — everything is transactional with the bronze load. See [ADR 0020](decisions/0020-pipeline-state-tracking.md) for full rationale.

For day-to-day development this means:

- `git pull` never needs to reconcile state-file changes (there aren't any).
- A local extractor run against a dev Neon branch reads and writes its own watermark state, so it won't collide with production state unless you point it at the production database.
- Inspecting state during development is a SQL query — see `operations.md` for the canonical queries.

The state tables are intentionally created in the same migration set as bronze/silver/gold schemas so `uv run alembic upgrade head` provisions them together on a fresh Neon branch.

---

## Running dbt locally

dbt reads its connection from `dbt/profiles.yml`, which references env vars that are **separate from** the `Settings`-managed `.env` (see [Design rationale](#design-rationale--strict-scope-on-env) for why). Set these in your shell or in a `.env.dbt` file you source manually:

```bash
export NEON_HOST=ep-xxx-dev.neon.tech
export NEON_USER=...
export NEON_PASSWORD=...
export NEON_DBNAME=recalls
```

These derive from your `NEON_DATABASE_URL`; if you keep that in `.env`, you can split it once and store the parts in a sibling file.

Then run dbt against the dev Neon branch:

```bash
# Compile-only sanity check — fast, doesn't touch the DB
uv run dbt parse --project-dir dbt

# Full build: run all models + tests
uv run dbt build --project-dir dbt

# Run a specific model and its descendants
uv run dbt run --project-dir dbt --select stg_cpsc_recalls+

# Run tests against built models
uv run dbt test --project-dir dbt

# Generate and serve docs locally (helpful for understanding the model graph)
uv run dbt docs generate --project-dir dbt
uv run dbt docs serve --project-dir dbt
```

**A few mechanics:**

- **Source freshness** is configured per [ADR 0015](decisions/0015-testing-strategy.md). Run `uv run dbt source freshness --project-dir dbt` to check whether bronze tables have recent data; the transform workflow runs this in production.
- **Model selection syntax.** `+` is downstream, `+model_name` is upstream, `model_name+` is downstream of model_name. `dbt run --select +recall_event` runs everything `recall_event` depends on but not its downstream gold models.
- **Compile output** lands in `dbt/target/compiled/` after `dbt parse` or `dbt run`. Useful for inspecting the actual SQL when a model surprises you.
- **Profile target.** `profiles.yml` defines a single `dev` target; production CI overrides via env vars or a separate profile path. Don't add a `prod` target to the committed file unless you mean to.

---

## Debugging

Common diagnostic surfaces, in roughly the order you'd reach for them:

### Pipeline state (the SQL surface)

Most "what just happened?" questions are answered by the canonical queries in [`operations.md`](operations.md). Three are most useful during development:

```bash
# Recent runs across all sources
psql $NEON_DATABASE_URL -c "
  select source, started_at, status, records_extracted, records_inserted, change_type, error_message
  from extraction_runs
  order by started_at desc
  limit 20
"

# Why did this source fail? Inspect the rejected table
psql $NEON_DATABASE_URL -c "
  select source_recall_id, failure_stage, failure_reason, rejected_at
  from cpsc_recalls_rejected
  order by rejected_at desc
  limit 20
"

# Watermark state — is the cursor where you expect it?
psql $NEON_DATABASE_URL -c "select * from source_watermarks"
```

For the full canonical-queries set (freshness, bronze counts, content-hash spot checks), see [`operations.md`](operations.md) Monitoring section.

### Structured logs

The pipeline logs JSON to stdout via `structlog` per [ADR 0021](decisions/0021-structured-logging.md). Every line carries a `run_id` correlation ID that ties together a single extraction. To make local logs human-readable while keeping CI logs machine-parseable:

```bash
# Pretty-print local extractor logs with jq
uv run recalls extract cpsc 2>&1 | jq -C .

# Filter to a single run after the fact
uv run recalls extract cpsc 2>&1 | jq -C 'select(.run_id == "<the-run-id>")'

# Filter to errors only
uv run recalls extract cpsc 2>&1 | jq -C 'select(.level == "error")'
```

### VCR cassette inspection

When an integration test fails after a source API change, the cassette and the live API have diverged. Compare:

```bash
# What did the test record expect?
less tests/fixtures/cassettes/<source>/<scenario>.yaml

# What does the live API return now? (Use the Bruno collection — see "API exploration with Bruno" above.)
bru run bruno/<source>/<request>.bru
```

Common patterns when this happens:
- New required field → Pydantic `ValidationError` in the test → update the schema, then re-record cassettes.
- New optional field → cassette test passes silently but bronze starts populating new columns → update the schema with `Optional[T]`, re-record on next live run.
- Renamed field → both old and new are missing-required errors; re-record after updating the schema.

See [Re-recording VCR cassettes](#re-recording-vcr-cassettes) for the procedure.

### Extractor steps in isolation

The `Extractor` ABC has five lifecycle steps (`extract`, `land_raw`, `validate`, `check_invariants`, `load_bronze`). When debugging a specific failure, you can short-circuit by calling them directly in `python -c` or `ipython`, e.g.:

```bash
uv run python - <<'EOF'
from src.config.settings import Settings
from src.extractors.cpsc import CpscExtractor

s = Settings()
e = CpscExtractor(base_url="https://www.saferproducts.gov/RestWebServices/Recall", settings=s)
records = e.extract()  # just the fetch step
print(f"Got {len(records)} records")
print(records[0])
EOF
```

This is faster than full extraction runs when you're hunting a parsing or validation bug. Don't commit code that does this — it's a debugging shortcut, not a tested code path.

### When all else fails

Drop a `breakpoint()` in the extractor and run the failing test with `--pdb`:

```bash
uv run pytest tests/integration/test_cpsc_live_cassettes.py --pdb -k test_happy_path_recent
```

The `--pdb` flag drops you into a debugger on the first failure, with the full call stack and locals available. Use `bt` to see the stack, `up`/`down` to walk frames, `pp variable` to pretty-print, `c` to continue.

---

## References

- [Architecture Decision Records](decisions/) — rationale for every major design choice
- [Operations guide](operations.md) — runbooks for rotation, re-ingestion, monitoring
- [ADR 0016 — Secrets management](decisions/0016-secrets-management.md)
- [ADR 0020 — Pipeline state tracking](decisions/0020-pipeline-state-tracking.md)
- [direnv documentation](https://direnv.net/)
- [Proton Pass CLI documentation](https://protonpass.github.io/pass-cli/)
- [Proton Pass CLI GitHub repository](https://github.com/protonpass/pass-cli)
- [Bruno API client](https://www.usebruno.com/)
- [Bruno CLI (`bru`) documentation](https://docs.usebruno.com/bru-cli/overview)
