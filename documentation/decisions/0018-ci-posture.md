# 0018 — CI posture

- **Status:** Accepted
- **Date:** 2026-04-16

## Context

Continuous integration binds the pieces of this project together: quality gates on PRs, publishing on main pushes, scheduled ingestion and transformation per ADR 0010. Decisions here depend on and extend the orchestration choice (ADR 0010 — GitHub Actions), the testing strategy (ADR 0015), the secrets posture (ADR 0016), and the package manager (ADR 0017).

Five sub-decisions to settle:

1. Workflow triggers — what runs on PR vs. main vs. cron vs. manual dispatch
2. How `dbt build` + `dbt test` sequence with extractor workflows
3. Pre-commit hook set — final assembly of everything decided incrementally in prior ADRs
4. Branch protection on `main`
5. Whether to write a separate `architecture.md` given that ADRs already cover the same ground

## Decision

### 1. Workflow triggers

| Trigger | What runs | Purpose |
|---|---|---|
| **PR to `main`** | `ruff check` + `ruff format --check`, `pyright`, `pytest tests/unit/`, `pytest tests/integration/` (VCR cassettes), `dbt parse` (no run), 1–2 e2e smoke tests on an ephemeral Neon branch | Quality gate before merge |
| **Push to `main`** | Same as PR checks + `dbt docs generate` → deploy to Cloudflare Pages | Publish docs, confirm main is healthy |
| **Cron per source** (per ADR 0010) | Per-source extractor workflows (one `.github/workflows/extract-<source>.yml` each) | Production ingestion |
| **Cron for transforms** | `dbt build` + `dbt test` against production Postgres | Silver/gold refresh |
| **`workflow_dispatch`** | Any of the above | Manual trigger for debugging, re-runs, schema-drift re-ingest per ADR 0014 |

### 2. dbt orchestration — time-shifted cron

Extractors run per ADR 0010 schedules. A separate `transform` workflow runs **daily on cron at a time where extractors typically have completed** — e.g., extractors at 01:00–03:00 UTC, transforms at 05:00 UTC.

Alternative rejected: `workflow_run` chaining. More elegant on paper but has real quirks (doesn't trigger on private forks, only runs on the default branch, fan-in from multiple upstream workflows is awkward). The coupling it provides isn't worth its cost at this scale.

If an extractor is late or fails, the transform workflow sees yesterday's bronze for that source. dbt `source freshness:` assertions (per ADR 0015) surface staleness as warnings; nothing crashes.

### 3. Pre-commit hooks — final set

Six hooks, each catching a specific class of bug:

| Hook | Source | What it catches |
|---|---|---|
| `ruff` + `ruff-format` | Astral pre-commit repo | Lint violations, formatting drift |
| `pyright` | pyright-python | Type errors |
| `gitleaks` | gitleaks/gitleaks | Broad-spectrum credential patterns in diffs (ADR 0016) |
| `cassette-secret-scrub` (local) | `scripts/verify_cassette_scrub.py` | Auth headers in committed VCR cassettes (ADR 0015, 0016) |
| `check-pydantic-strict` (local) | `scripts/check_pydantic_strict.py` | Pydantic bronze models missing `ConfigDict(extra='forbid', strict=True)` — ADR 0014 enforcement |
| `uv-lock-check` (local) | `uv lock --check` | `uv.lock` out of sync with `pyproject.toml` — ADR 0017 reproducibility |

Reference `.pre-commit-config.yaml` (to be committed at repo root during implementation):

```yaml
repos:
  - repo: https://github.com/astral-sh/ruff-pre-commit
    rev: v0.8.0
    hooks:
      - id: ruff
        args: [--fix]
      - id: ruff-format

  - repo: https://github.com/RobertCraigie/pyright-python
    rev: v1.1.389
    hooks:
      - id: pyright

  - repo: https://github.com/gitleaks/gitleaks
    rev: v8.21.0
    hooks:
      - id: gitleaks

  - repo: local
    hooks:
      - id: cassette-secret-scrub
        name: Verify no auth headers in committed cassettes
        entry: uv run python scripts/verify_cassette_scrub.py
        language: system
        files: ^tests/fixtures/cassettes/.*\.yaml$

      - id: check-pydantic-strict
        name: Verify Pydantic bronze models declare extra='forbid' + strict=True
        entry: uv run python scripts/check_pydantic_strict.py
        language: system
        files: ^src/schemas/.*\.py$

      - id: uv-lock-check
        name: Verify uv.lock matches pyproject.toml
        entry: uv lock --check
        language: system
        pass_filenames: false
        files: ^(pyproject\.toml|uv\.lock)$
```

**Defense in depth:** the same hooks run in CI via `pre-commit run --all-files` on every PR. If a developer skipped `pre-commit install`, CI still catches the issue.

**Bypass (`git commit --no-verify`)** is documented as emergency-only in `development.md`. CI remains the enforcement net.

### 4. Branch protection on `main`

Settings applied via the GitHub repository's Branch Protection Rules:

- Require a pull request before merging
- Require status checks to pass before merging (specifically: the PR-check workflow)
- Require branches to be up to date before merging
- Disallow force pushes to `main`
- Disallow deletion of `main`

Not enforced at v1:

- **Signed-commit requirement.** Portfolio-visible but extra setup; can add later via the same protection rules.
- **Required reviewer approvals.** Solo project; self-review via PR is the workflow. Adds no value without a second reviewer.

### 5. README.md and ADR index; skip `architecture.md`

- **`README.md`** at repo root — required for a public repo. Covers: what the project is, 5-line architecture summary with a Mermaid diagram of the 4-layer flow, quick start, links to `documentation/` and the ADR index.
- **`documentation/decisions/README.md`** — topical index of all ADRs, updated as new decisions are filed.
- **`architecture.md` is deliberately not written.** The 18 ADRs cover architecture with better rationale than a single overview doc would. A separate `architecture.md` would duplicate content and risk staleness as ADRs evolve. The Mermaid diagram in the main `README.md` provides the at-a-glance view; the ADR index provides the structured deep dive.
- **Draw.io / diagrams.net diagrams** (ERDs for the silver schema, DAGs for the pipeline) are planned for `documentation/diagrams/` during implementation. Managed as `.drawio` XML files (git-friendly, round-trippable) with exported SVG for embedding in documentation.

## Consequences

- PR-time feedback loop is fast: ruff + pyright + unit tests run in seconds; integration tests (VCR-backed) in under a minute; e2e on a Neon branch in under two.
- Main stays green: push-to-main only happens after PR checks pass; branch protection prevents direct pushes.
- Documentation is published automatically (dbt docs on every main push) — keeps docs in sync with code, removes "forgot to regenerate docs" failure mode.
- Production ingestion is decoupled from CI — cron workflows run independently of PRs/pushes, so a broken PR doesn't block scheduled ingestion, and a failed cron doesn't block PRs.
- Six pre-commit hooks cover lint, type, secrets, custom data-contract guards, and lockfile consistency — each earning its place by catching a specific bug class. Defense-in-depth via CI prevents bypass from undermining the policy.
- Branch protection demonstrates disciplined git hygiene without adding solo-project friction.
- `architecture.md` avoided; ADR index + Mermaid-in-README provides navigation without duplication.

### Open for revision

- **Workflow runtime budgets.** If any PR check consistently runs over a threshold (suggested: 10 minutes), split the workflow (e.g., e2e tests moved to a separate optional job, or parallelized).
- **dbt transform cron timing.** The 05:00 UTC placeholder is a starting point; adjust once actual extractor completion times are observed.
- **Branch protection tightness.** If a second contributor joins, add required reviewer approval and consider signed-commit enforcement.
- **Separate `architecture.md`.** If the ADR-only approach proves hard to navigate in practice, revisit — but prefer adding structure to the ADR index over duplicating content in a standalone doc.
