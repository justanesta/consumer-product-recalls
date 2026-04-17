# Development guide

This document covers local setup, environment configuration, and day-to-day development workflow. For architectural rationale behind choices described here, see the ADRs in `documentation/decisions/`.

Sections marked **TBD during implementation** describe procedures that depend on code not yet written. They will be filled in as the codebase matures.

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

**TBD during implementation** — exact commands depend on the `pyproject.toml`, CLI entry points, and database migration tooling we land on.

High-level shape:

1. Clone the repository.
2. Install dependencies (`uv sync` or `pip install -e .`).
3. Copy `.env.example` to `.env` and fill in credentials (see [Environment variables](#environment-variables)).
4. Initialize the database schema (migration command TBD).
5. Verify the install with the test suite (see [Running tests](#running-tests)).

---

## Environment variables

All credentials and environment-specific configuration live in a `.env` file at the project root. This file is **gitignored** and must never be committed. See [ADR 0016](decisions/0016-secrets-management.md) for the full rationale.

### Method 1 — `.env` with manual sourcing (simplest, no extra tools)

Copy the template and edit:

```bash
cp .env.example .env
$EDITOR .env
```

Your `.env` should look like:

```bash
NEON_DATABASE_URL=postgresql://user:pass@ep-xxx.neon.tech/recalls?sslmode=require

R2_ACCOUNT_ID=your_cloudflare_account_id
R2_ACCESS_KEY_ID=your_r2_access_key
R2_SECRET_ACCESS_KEY=your_r2_secret_access_key
R2_BUCKET_NAME=consumer-product-recalls-raw

FDA_AUTHORIZATION_USER=your_oii_user
FDA_AUTHORIZATION_KEY=your_oii_key
```

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

---

## Running tests

**TBD during implementation** — test runner configuration depends on the pytest/dbt setup. Expected high-level shape once implemented:

```bash
# Unit tests (fast, no network)
uv run pytest tests/unit/

# Integration tests (VCR-backed; see ADR 0015)
uv run pytest tests/integration/

# End-to-end tests (requires database)
uv run pytest tests/e2e/

# dbt tests (requires database + seeded fixtures)
uv run dbt test
```

See [ADR 0015](decisions/0015-testing-strategy.md) for the full testing strategy.

---

## Re-recording VCR cassettes

When an agency's API changes (new field, renamed field, new enum value), cassettes need to be re-recorded against the live API. The command shape will be:

```bash
uv run pytest tests/integration/test_<source>_extractor.py --record-mode=rewrite
```

Full procedure documented in `documentation/operations.md` once the workflow is built.

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

**TBD during implementation.**

---

## Running dbt locally

**TBD during implementation.**

---

## Debugging

**TBD during implementation.**

---

## References

- [Architecture Decision Records](decisions/) — rationale for every major design choice
- [Operations guide](operations.md) — runbooks for rotation, re-ingestion, monitoring
- [ADR 0016 — Secrets management](decisions/0016-secrets-management.md)
- [direnv documentation](https://direnv.net/)
- [Proton Pass CLI documentation](https://protonpass.github.io/pass-cli/)
- [Proton Pass CLI GitHub repository](https://github.com/protonpass/pass-cli)
- [Bruno API client](https://www.usebruno.com/)
- [Bruno CLI (`bru`) documentation](https://docs.usebruno.com/bru-cli/overview)
