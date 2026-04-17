# 0017 — Package management via `uv`

- **Status:** Accepted
- **Date:** 2026-04-16

## Context

The project needs a Python package/dependency/virtual-environment manager. The choice affects CI speed, developer ergonomics, lockfile reproducibility, and how several other ADRs integrate (notably ADR 0016's direnv pattern and the CI posture in ADR 0018).

`uv` has been the implicit assumption across ADR 0010 (dependency installation in workflows), ADR 0011 (pinning the dbt-postgres adapter), and the development guide, but was not formally decided. This ADR closes that gap.

Candidate tools evaluated:

| Tool | Verdict |
|---|---|
| **`uv`** (Astral) | Rust-based; 10–100× faster than pip; unified tool replacing pip + pip-tools + pyenv + virtualenv; native `pyproject.toml` + `uv.lock`; can manage Python versions via `uv python install`; Apache/MIT; trajectory toward industry-standard status |
| `pip` + `pip-tools` | Classic, reliable, slow; requires multiple tools coordinating; no unified story |
| `poetry` | Popular; opinionated about project structure in ways that conflict with our layout; slower than uv |
| `pdm` | PEP 582 support; niche; smaller ecosystem |
| `hatch` | From PyPA; good concepts; slower momentum and adoption than uv |
| `rye` | Was a competitor; Astral acquired it; its ideas effectively became uv — no reason to pick rye now |

## Decision

- **`uv`** is the canonical package/venv/Python-version manager for this project.
- **Virtual environment:** `.venv/` at repo root, managed by uv. No parallel venvs (no `layout python` in direnv, no `python -m venv`).
- **Lockfile:** `uv.lock` committed to git for reproducibility.
- **Dependency installation:** `uv sync` (honors `pyproject.toml` + `uv.lock`).
- **Canonical command invocation:** `uv run <command>` — e.g., `uv run pytest`, `uv run python -m src.cli`, `uv run dbt build`. Shell-activated venv via `PATH_add .venv/bin` (per ADR 0016's direnv pattern) also works for ad-hoc use.
- **Python version:** pinned via `.python-version` at repo root; uv honors it automatically.
- **CI:** GitHub Actions workflows install uv (via the official `astral-sh/setup-uv` action), then run `uv sync --frozen` to install exactly what's in the lockfile.

## Consequences

- CI is dramatically faster than pip-based equivalents — full dependency install is sub-second in most cases.
- Single tool replaces pip + pip-tools + pyenv + virtualenv + (sometimes) poetry. Less cognitive load, fewer version-mismatch failure modes.
- Lockfile reproducibility is enforced via `uv sync --frozen` in CI; no silent dependency drift between developer machines and production.
- direnv integrates cleanly via `PATH_add .venv/bin`, not `layout python` (see ADR 0016).
- `uv lock --check` is available as a pre-commit hook (see ADR 0018) to enforce lockfile consistency with `pyproject.toml`.
- Developers who prefer not to type `uv run` repeatedly can activate the venv (`source .venv/bin/activate`) or let direnv's `PATH_add` expose binaries automatically.
- If uv's trajectory ever reverses (loss of funding, regression, licensing change), migration to another tool is incremental — `pyproject.toml` is the source of truth, and uv's lockfile format is parseable/convertible.

### Open for revision

- **Python version policy.** `.python-version` is currently single-value; if cross-version testing becomes important (e.g., we want to support 3.12 and 3.13), revisit.
- **`uv run` vs activated venv.** The canonical pattern may shift based on real usage — if team members strongly prefer one, document it here.
