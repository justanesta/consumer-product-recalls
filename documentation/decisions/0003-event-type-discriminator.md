# 0003 — `event_type` discriminator on `recall_event`

- **Status:** Accepted
- **Date:** 2026-04-16

## Context

ADR 0001 defers EPA but anticipates it may be reopened if a usable feed (e.g. SSURO orders, or APPRIL cancellations with safety-versus-commercial signal) becomes available. EPA-style data — registration cancellations, enforcement orders — is not a "recall" in the consumer sense; it's a related but semantically distinct regulatory action.

Two forward-compatibility options were considered:

- **Rename the table to `safety_event`** to encompass both recalls and other regulatory actions. Cost: more verbose terminology for the 95% v1 case that *is* recalls; touches every query, view, ORM model, and downstream consumer.
- **Add a discriminator column with a default.** Cost: nearly zero. Most v1 queries don't need to filter on it.

## Decision

Keep the table named `recall_event` for v1. Add an `event_type TEXT NOT NULL DEFAULT 'RECALL'` column. Future values may include `'REGULATORY_ACTION'`, `'ENFORCEMENT_ORDER'`, or others as new event categories are added.

## Consequences

- If EPA (or any other non-recall feed) is reopened, no schema migration is required — only a new `event_type` value and corresponding ingestion logic.
- v1 serving views and dashboards that omit `WHERE event_type = 'RECALL'` will silently include future non-recall events when they exist. Gold-layer views must be explicit about which event types they aggregate.
- The default value preserves backwards compatibility for queries written before the discriminator existed.
- One column of cost today buys avoidance of a meaningful migration later.

## Implementation status — DEFERRED, not yet implemented (2026-W25)

As of 2026-06-19 the `event_type` column has **not** been created on `recall_event` — confirmed absent by
the Silver/Gold provenance audit (`documentation/audit/silver_gold_provenance_audit_2026_w25.md`, the
`event_type_not_null_single_value` check). It was deliberately left out because, with every in-scope source
supplying only consumer recalls, the column would be **100% `'RECALL'`** — a constant carrying no
information today. The decision above stands as the *design* for the column; it ships when either trigger
makes the discriminator meaningful:

1. **USDA/FDA "Public Health Alert" semantics are clarified.** PHA currently rides as a `classification` /
   `lifecycle_status` value, but it may be legally/semantically a *distinct regulatory action* rather than a
   recall class. If USDA/FDA confirm PHA is its own action type, it becomes the first non-`'RECALL'`
   `event_type`.
2. **A recall-adjacent, non-recall feed is ingested** — e.g. FAA Airworthiness Directives (ADs), or EPA
   enforcement/cancellation actions (ADR 0001) — at which point `event_type` discriminates them from recalls
   exactly as this ADR intends.

When either lands: add `'RECALL'::text as event_type` (NOT NULL) to the existing source branches in
`recall_event.sql` + the new value on the new branch, an `accepted_values` test, and the
`WHERE event_type = 'RECALL'` guard on the gold serving models (per Consequences above). Silver-only,
constant column → **no bronze re-baseline**.
