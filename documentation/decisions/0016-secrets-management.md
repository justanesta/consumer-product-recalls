# 0016 — Secrets management

- **Status:** Accepted
- **Date:** 2026-04-16

## Context

The pipeline requires three credential sets:

| Secret | Used by | Sensitivity |
|---|---|---|
| **FDA Authorization-User + Authorization-Key** | FDA extractor (ADR 0012) | Medium — revocable via OII Unified Logon |
| **Neon Postgres connection string** | Bronze loader, dbt, migrations | High — full DB access |
| **Cloudflare R2 credentials** (access key ID + secret access key) | Raw landing writes (ADR 0004) | High — full bucket access |

Two environments need secrets: production (GitHub Actions runners per ADR 0010) and local development. The management approach must:

- Fail loud on missing or malformed values, consistent with the `extra='forbid'` posture everywhere else in the project (ADR 0014).
- Prevent leakage via VCR cassettes (ADR 0015) and logs.
- Not introduce an onboarding barrier — a new developer should be able to clone, fill in a template, and run.
- Make rotation a documented chore rather than a crisis.

Candidate local-dev storage patterns considered:

- **`.env` file + `python-dotenv` / `pydantic-settings`** — ubiquitous, explicit, risk of accidental commit (mitigated via `.gitignore` + pre-commit).
- **`direnv` with `.envrc`** — auto-injects env vars on `cd`, handles venv activation, integrates with password-manager CLIs. Extra shell tool to install.
- **1Password CLI / Doppler / similar secrets SaaS** — secrets never on disk cleartext, extra dependency, overkill for a solo portfolio project.

## Decision

### Production storage

**GitHub Actions repository secrets.** Encrypted at rest, injected into workflow environment only when needed, no file persistence. Single source of truth for production values.

### Local development storage

- **`.env` file** at repo root (gitignored), one value per line.
- **`.env.example`** committed alongside, with placeholder values and comments pointing at where each secret is obtained.
- **`pydantic-settings`** loads `.env` into a typed `Settings` model with `extra='forbid'` and `SecretStr` for credential fields.
- **`direnv` as an optional ergonomics layer** — committed `.envrc` template that `dotenv`-loads `.env` and auto-activates the Python venv on `cd`. Documented in `documentation/development.md`, not required to run the project. Supports sourcing secrets from a password-manager CLI (e.g. Proton Pass, 1Password) for developers who prefer not to keep plaintext secrets in `.env`.

### Loading mechanism

```python
# src/config/settings.py
from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file='.env',
        env_file_encoding='utf-8',
        extra='forbid',
    )
    
    neon_database_url: SecretStr
    r2_account_id: str                  # not secret
    r2_access_key_id: SecretStr
    r2_secret_access_key: SecretStr
    r2_bucket_name: str                 # not secret
    fda_authorization_user: SecretStr
    fda_authorization_key: SecretStr

settings = Settings()  # singleton; raises ValidationError on missing fields at import time
```

Extractor YAML configs (ADR 0012) reference secrets by Settings field name, never by value:

```yaml
# config/sources/fda.yaml
auth:
  header_name: Authorization-Key
  secret_env: fda_authorization_key
```

### Rotation policy

90-day cadence for all three credential sets. Runbooks in `documentation/operations.md`. Quarterly reminder via a scheduled GitHub Actions workflow that auto-opens an issue titled "Rotate secrets" on the first of every third month.

### Pre-commit hooks (two-layer defense)

- **`gitleaks`** (or equivalent like `detect-secrets`) — broad-spectrum scan of every staged diff for patterns that look like credentials (high-entropy strings, AWS-style keys, JWTs, private keys).
- **Custom cassette secret scrub verifier** — scans every staged file under `tests/fixtures/cassettes/` and fails if any of the VCR-filtered header or query-param names appears with a non-placeholder value. This is the second net behind VCR's `before_record_request` filter.

Two hooks rather than one: `gitleaks` is generic and catches credentials from anywhere; the cassette verifier is specific to our known leak path (recorded API responses) and runs faster by only scanning cassette files.

### Missing secrets at runtime

Fail loud at process boot. `Settings()` raises `ValidationError` when required fields are missing, with a clear error naming the missing field. Consistent with ADR 0014's `extra='forbid'` posture.

For secrets that become invalid mid-run (e.g., FDA key expires while a workflow is executing), the extractor's 401/403 handler from ADR 0013 kicks in — fail fast, alert, workflow exits non-zero.

No "skip this source and continue" behavior. A silently-skipped source is worse than a loud failure.

## Consequences

- Standard `.env` + `pydantic-settings` shape — zero onboarding barrier, familiar to any Python developer.
- `SecretStr` prevents accidental logging (repr renders as `**********`).
- direnv as an optional layer gives developers who want it a cleaner experience (auto-env, auto-venv, password-manager integration) without gatekeeping those who don't.
- Rotation becomes a quarterly 20-minute chore, not a crisis response. Runbooks mean rotation is reproducible; quarterly auto-issue prevents forgetting.
- Two-layer pre-commit hooks catch both broad leaks (gitleaks) and the specific cassette-recording leak path; neither is complex enough to be a maintenance burden.
- Consistent fail-loud philosophy across schema validation (ADR 0014), error routing (ADR 0013), and now configuration loading.
- No external secrets SaaS dependency — keeps near-zero-cost constraint intact.

### Open for revision

- **Password-manager CLI integration patterns.** The direnv + Proton Pass pattern documented in `development.md` is one example; if Proton Pass's CLI evolves or other tools prove more ergonomic, the pattern is easy to swap.
- **Rotation cadence.** 90 days is a reasonable starting point; may be lengthened or shortened based on incident data (spoiler: there shouldn't be any).
- **Pre-commit hook choice.** `gitleaks` is the default; swappable with `detect-secrets`, `trufflehog`, or a combination if gaps emerge.
