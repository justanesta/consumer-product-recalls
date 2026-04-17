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
