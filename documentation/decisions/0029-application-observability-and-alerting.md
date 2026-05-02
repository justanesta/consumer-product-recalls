# 0029 — Application observability and alerting: v1 stance and upgrade triggers

- **Status:** Accepted
- **Date:** 2026-05-01
- **Supersedes:** —
- **Superseded by:** —
- **Clarifies:** ADR 0020 (pipeline-state tracking via Postgres tables); ADR 0021 (structured logging with structlog; defers log shipping); `project_scope/implementation_plan.md` line 572 ("Monitoring / alerting beyond GitHub Actions UI" — out of scope for v1).

## Context

Several prior decisions touch observability in part:

- **ADR 0020** establishes `source_watermarks` and `extraction_runs` as the canonical pipeline-state tables. Operators query these directly with SQL to inspect run history, current cursor positions, and failure timestamps.
- **ADR 0021** chooses `structlog` for JSON log emission to stdout. It explicitly defers log *shipping* (CloudWatch, Datadog, Loki, etc.) — logs live in GitHub Actions run output until something pulls them out.
- **`implementation_plan.md` line 572** declares "Monitoring / alerting beyond GitHub Actions UI" out of scope for v1, with the implicit rule "add if/when pipeline noise warrants."
- **`documentation/operations.md`** contains a "canonical queries" section (monitoring queries pasteable into psql) which is the practical operator-facing surface.

Read together, these answer what telemetry exists and where it lives, but leave a gap: **what is the policy for when v1's observability stops being sufficient, and what do we do then?** Without a documented threshold, the project either over-engineers monitoring before it's earned (premature scope) or under-engineers it past the point of actual user pain (silent failure surface).

This is also the natural place to address the inverse question — "is monitoring beyond the GitHub Actions UI an architectural follow-up or a deliberate deferral?" — by stating it explicitly rather than leaving it as an absence.

## Decision

### v1 observability stance

For v1 (Phase 7 cron go-live through the first ~6 months of production), application observability is delivered by the combination of:

1. **GitHub Actions UI** — workflow run history, per-step logs, exit codes, durations. This is the primary operator surface for "did the pipeline run, and did it finish?"
2. **Structured JSON logs to stdout (ADR 0021)** — captured in the GitHub Actions run output. Searchable within a run via the GH Actions log search; not aggregated across runs.
3. **`extraction_runs` and `source_watermarks` tables (ADR 0020)** — SQL-queryable history of every run with status, duration, record counts, error messages. The canonical query set is in `documentation/operations.md`.
4. **`*_rejected` tables (ADR 0013)** — SQL-queryable forensic surface for records that failed validation or invariants. Append-only audit trail.
5. **dbt source-freshness assertions (ADR 0015 / Phase 4)** — `dbt source freshness` flags bronze tables that haven't seen new rows within the configured threshold. Run in the transform workflow.

**Alerting** is implicit: the operator notices a failure when the next manual check reveals a red workflow, a freshness warning, or a rejected-table row count that wasn't there before. There is no push notification. There is no on-call rotation. There is no SLO.

This is a deliberate choice for v1. The pipeline is small (5 sources, daily/weekly cadence, no consumer dependencies on freshness). The blast radius of a missed failure is "stale data for one source for ~24 hours." Adding paging, dashboards, or ticket auto-creation before there is real operational pain is premature scope.

### Upgrade triggers

When any one of the following fires, file a supersession ADR for this one and move to a real observability stack (Sentry / Datadog / Grafana / OpenTelemetry / equivalent — choice deferred to that ADR):

| Trigger | Threshold | Why this is the line |
|---|---|---|
| **Sustained extraction-failure rate** | Any source's daily extract workflow fails on ≥3 consecutive days, OR ≥30% of any source's runs over a 14-day window fail | Indicates a problem that won't self-resolve; manual triage is no longer sufficient. |
| **Multi-source incident** | Two or more sources fail simultaneously due to non-shared causes (i.e., not "Neon is down for everyone") | Suggests systemic operator-attention shortage — one operator can't triage two unrelated failures fast enough by SQL. |
| **Time-to-detection** | A real-world failure (data loss, schema drift, prolonged outage) is detected by a downstream consumer — or the user themselves browsing the data — rather than by the operator first | The current model assumes the operator looks. This trigger means the assumption broke. |
| **Consumer SLO commitment** | Any external user (or ADR-encoded plan, e.g. Phase 8 FastAPI serving layer) commits to "data freshness within X hours of upstream publication" | Implicit alerting on the operator's manual cadence is not a defensible SLO substrate. |
| **Pipeline volume** | Any single workflow runtime exceeds 60 minutes consistently, OR total daily extraction time exceeds 4 hours, OR the `extraction_runs` table exceeds ~100K rows | Same trigger as ADR 0010's "re-evaluation triggers for moving off GitHub Actions" — at that scale, the GH Actions UI stops being a usable observability surface. |
| **Operator change** | Project gains a second operator (Adrian + 1) or hands off to someone else entirely | Tribal knowledge that "check operations.md queries weekly" is a single-operator workflow; a team needs explicit alerting. |

The threshold values are calibrated for a solo-operator personal-portfolio project. They are deliberate and may seem loose; the bar for installing real monitoring is "at least one of these is firing today," not "any of these might fire someday."

### What stays even after upgrade

If/when v2 observability lands, ADRs 0020 and 0021 remain authoritative for the layers below the dashboard:

- `extraction_runs` / `source_watermarks` / `*_rejected` are the *truth*; dashboards aggregate them, alerts fire off them.
- `structlog` JSON logs become the substrate for log shipping (the deferral in ADR 0021 dissolves) but the field shape and correlation-ID semantics don't change.
- dbt source freshness remains the bronze-staleness signal.

The upgrade is additive, not a replacement. v1's observability is not throwaway.

## Consequences

### Positive

- **The v1 deferral has a name and a stopping rule.** "We're not adding monitoring until X" is a defensible answer to "where's the alerting?"
- **Operators don't second-guess themselves.** The runbook in `operations.md` is the workflow; if it stops being sufficient, this ADR's triggers are the gate to upgrade.
- **No premature dependency on observability SaaS.** Sentry / Datadog / Grafana free tiers all have onboarding cost; deferring them keeps the project's near-zero-cost stance intact.
- **The upgrade conversation is pre-planned.** When a trigger fires, the next ADR has a head start: it inherits the layered model (ADR 0020 + 0021 below; new layer on top) rather than reinventing it.

### Negative

- **Slow time-to-detection by design.** A failure on Wednesday may not be noticed until Friday's operator check. For v1's stakes, this is acceptable; if it stops being acceptable, that triggers the upgrade.
- **No paging means no resilience to operator unavailability.** If the operator is on vacation for two weeks, two weeks of failures pile up. Mitigated by the cron's once-daily cadence and content-hash idempotency — a backlog doesn't compound the way streaming systems do.
- **Trigger thresholds are educated guesses.** The "≥3 consecutive failure days" and "≥30% over 14 days" are not data-driven; they're calibrated to operator tolerance. Revisit if either fires for a clearly-not-broken reason.

### Neutral

- **dbt freshness warnings are part of v1.** They are configured per ADR 0015 / Phase 4 deliverables. They are not proactive alerts (they print to the dbt run log) but they are visible during the transform workflow's normal output.

## Alternatives considered

### Alternative 1 — File no ADR; leave the deferral implicit

Continue treating `implementation_plan.md` line 572 ("out of scope for v1") as the authoritative deferral.

- **Pros:** one fewer ADR.
- **Cons:** "out of scope" doesn't say "until what?" Six months in, no one remembers what triggers the upgrade. The implicit deferral becomes architectural debt.
- **Verdict:** rejected. The point of an ADR is to document a decision; "we deferred this until X" is a decision worth documenting.

### Alternative 2 — Adopt a minimum-viable observability stack now

Wire in Sentry (free tier, 5K events/month) for unhandled-exception capture. Wire in a basic uptime-monitor service (UptimeRobot or BetterStack free tier) hitting a `/health` endpoint.

- **Pros:** push notification on failure; reduced time-to-detection.
- **Cons:** there is no `/health` endpoint until Phase 8 (FastAPI). Sentry would catch unhandled exceptions in extractors but those are already surfaced via the GH Actions UI's workflow-failure status. Marginal value above v1's stance for the integration cost.
- **Verdict:** rejected for now; revisit when Phase 8 ships (the FastAPI layer is a natural place for both `/health` and Sentry). At that point, this ADR's triggers may already be firing or close to firing, and a unified v2 ADR makes more sense than retrofitting v1.

### Alternative 3 — Roll our own dashboards on top of `extraction_runs`

Build a dbt-based "operations dashboard" model that compiles into a HTML/Markdown report posted to the repo's GitHub Pages on every run.

- **Pros:** zero external dependencies; uses tooling already in the project.
- **Cons:** dashboards-without-alerts solve the wrong problem — the issue isn't "I can't see the data" (operations.md queries already show it), it's "I forgot to look." A dashboard the operator doesn't visit is no improvement.
- **Verdict:** rejected. If the trigger fires, push alerting (not pull dashboards) is the right answer.

## Implementation

This ADR has no code deliverables. It is a policy decision recorded once and revisited when a trigger fires.

What it does deliver:

- `documentation/operations.md` will reference this ADR in its "Alerting strategy" section (currently a TBD or absent).
- The `implementation_plan.md` "Out of scope for v1" entry for monitoring/alerting links here for context.
- When the first trigger fires, file ADR 003X (next available number) titled "Application observability v2: <chosen stack>" and update this one's Status to "Superseded by ADR 003X."
