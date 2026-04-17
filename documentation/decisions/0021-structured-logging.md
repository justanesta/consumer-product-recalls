# 0021 — Structured logging with structlog

- **Status:** Accepted
- **Date:** 2026-04-17

## Context

ADR 0012 commits to "structured JSON logging with correlation IDs" on the Extractor ABC. ADR 0013 uses "structured warning log" as the T2 surface for quarantine alerts. ADR 0020 introduces `extraction_runs.run_id` as the pipeline-run identifier and relies on log-to-DB cross-referencing for operational debugging. The implementation plan lists `src/config/logging.py` in Phase 2. None of these name a logging library.

Python has several production-grade options:

- **stdlib `logging` + a JSON formatter** (e.g., `python-json-logger`) — minimal dependency, no magic. Awkward for context propagation: per-run correlation IDs require manual threadlocal or contextvar plumbing layered on top of stdlib.
- **`structlog`** — first-class key/value structured fields, context-local bindings via `structlog.contextvars` (no threadlocal surgery), pluggable processors for JSON rendering, and full stdlib-logging compatibility for third-party libraries (dbt, SQLAlchemy, httpx, tenacity) that log through `logging.Logger`.
- **`loguru`** — opinionated and pleasant for scripts, but its JSON output is less composable, and integrating with stdlib-logging for third-party libraries fights the library's design.

Correlation-ID propagation matters because a single extraction run touches multiple modules (`extract()`, `land_raw()`, `validate()`, `check_invariants()`, `load_bronze()` per ADR 0013), retries fire inside `tenacity` decorators, and dbt runs emit their own logs from a subprocess. The developer experience of "one filter against the structured log archive returns every line from a single run" is valuable for debugging and for the portfolio narrative. Reaching that experience with plain stdlib logging is more glue code than the library choice warrants.

## Decision

- **Library:** `structlog`, configured to render JSON to stdout. GitHub Actions captures stdout into its log UI and file archive; no additional shipping infrastructure is required at v1.
- **Correlation ID:** a `run_id` UUID generated at extractor entry and bound to `structlog.contextvars` at the top of every workflow invocation. All subsequent log calls in that run inherit it automatically — no manual threading through function signatures. The same UUID is written to `extraction_runs.run_id` per ADR 0020, so log lines and database rows cross-reference via a single value.
- **Standard context fields** bound at run start:
  - `run_id` (UUID)
  - `source` (one of `cpsc` / `fda` / `usda` / `nhtsa` / `uscg`)
  - `stage` (one of `extract` / `land_raw` / `validate` / `check_invariants` / `load_bronze`) — rebound as the lifecycle progresses
  - `github_run_url` when available (from `GITHUB_SERVER_URL` + `GITHUB_REPOSITORY` + `GITHUB_RUN_ID` env vars)
- **Stdlib-logging bridge:** `structlog.stdlib.ProcessorFormatter` wraps the stdlib `logging` root. Third-party libraries (SQLAlchemy, httpx, dbt, tenacity) emit into the same JSON stream with the same processors applied — no library needs to be "structlog-aware."
- **Development ergonomics:** when stdout is a TTY, `structlog.dev.ConsoleRenderer` replaces the JSON renderer for human-readable colored output. Detected via `sys.stderr.isatty()` or an explicit `LOG_FORMAT=console` environment variable. CI and production use JSON unconditionally.
- **Log levels:** standard `debug` / `info` / `warning` / `error` / `critical`. Specific conventions:
  - `tenacity` retry attempts → `warning`
  - Bronze rejection (T1 insert per ADR 0013) → `warning` with structured fields for `failure_stage` and `source_recall_id`
  - Auth failure (401/403) → `error`, workflow exits non-zero
  - Rejection rate > 5% of batch per ADR 0013 → `error`, workflow marked partial
- **Secrets hygiene:** auth headers, API keys, and FDA's `signature` parameter are masked at the `httpx` transport layer using the same scrubbing pattern as VCR cassettes (per ADR 0015). Even if a code path logs a raw request object, sensitive values are already replaced with `<redacted>` at the transport boundary.

## Consequences

- Every log line from a run carries `run_id` without manual plumbing. Filtering "all logs from the failed FDA run last Tuesday" is a single JSON-filter query + a lookup of `github_run_url` from `extraction_runs`.
- Third-party library logs (SQLAlchemy query timing, tenacity retry attempts, dbt step output) flow through the same pipeline, so operational dashboards can aggregate across the whole stack without sampling gaps.
- Local development keeps a readable terminal format via the TTY-aware renderer; CI/production use JSON without ceremony.
- Stdlib-logging compatibility means migrating off structlog later is a `src/config/logging.py` rewrite, not a codebase-wide edit — the `Logger` API doesn't change.
- Adds a single runtime dependency (`structlog`); no C extensions. Pairs naturally with `tenacity` and `httpx`, which ADR 0012 and ADR 0013 already adopt.
- Log volume at v1 scale (~200 extractor runs/month × ~100–500 log lines per run) is trivial for GitHub Actions log retention and local inspection.

### Open for revision

- **Log shipping.** v1 logs to stdout; GitHub Actions captures them. If a proper log aggregator is added later (e.g., Grafana Loki, Datadog, BetterStack), the JSON output is already compatible — only the shipping transport changes, not the emission code.
- **Sampling or rate limiting.** Not implemented at v1. If log volume becomes expensive (unlikely at this scale), add a structlog processor that samples non-critical levels before the JSON renderer.
- **Schema contract for log fields.** The standard-fields list above is enforced by convention, not by code, at v1. If downstream log consumers (dashboards, alerting) become brittle to field renames, consider a Pydantic model for log-event shape with a validation processor.
- **Correlation across extractor and transform workflows.** `run_id` today scopes to a single extractor run. If debugging would benefit from correlating an extractor run with the subsequent transform run that consumed its bronze output, extend the context model with a `workflow_chain_id`.
