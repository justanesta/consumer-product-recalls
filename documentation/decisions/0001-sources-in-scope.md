# 0001 — Sources in scope

- **Status:** Accepted
- **Date:** 2026-04-16

## Context

The project initially identified seven federal agencies publishing recall-like data: FDA, CPSC, USDA (FSIS), NHTSA, EPA, USCG, and FAA. Including all seven would maximize breadth, but several sources introduce semantic or engineering problems that aren't worth taking on in v1:

- **EPA's APPRIL** (`https://ordspub.epa.gov/ords/pesticides/apprilapi/`) is a pesticide *registration* database, not a recall feed. The closest analog is `STATUS = Canceled`, but cancellations conflate voluntary commercial decisions with safety-driven actions — APPRIL exposes no field that distinguishes them. True EPA pesticide enforcement lives in **SSURO** (Stop Sale, Use, or Removal Orders), a separate program not exposed through APPRIL.
- **FAA Airworthiness Directives** target certificate holders (operators, mechanics) rather than consumers. A grounded aircraft is not a consumer product recall in the same sense as a CPSC toy recall.
- **USCG** publishes recall and defect-notification data on web pages with no API, requiring HTML scraping.

The portfolio goal favors breadth of demonstrable data engineering surfaces, but only where each source contributes meaningfully — not breadth for its own sake.

## Decision

The pipeline ingests recalls from five sources in v1:

- **In scope:** CPSC, FDA (Enforcement Reports), USDA (FSIS), NHTSA, USCG
- **Deferred:** EPA. Awaiting follow-up information from EPA via email about whether SSURO orders or other enforcement-action feeds are available. If a usable feed surfaces, EPA may be reopened in a future ADR.
- **Cut:** FAA. Reopening would require expanding the project's working definition of "consumer product."

USCG is kept despite requiring scraping because it adds a distinct skill surface (HTML parsing, brittle-source resilience) aligned with the portfolio goal.

## Consequences

- Five sources with heterogeneous shapes justify the harmonization layer in ADR 0004.
- USCG ingestion needs defensive scraping: rate limiting, raw HTML archival to landing storage, and schema-drift alarms when the page structure changes.
- The unified schema must anticipate non-recall regulatory actions if EPA is reopened — addressed in ADR 0003.
- `project_scope/project_vision_and_constraints.md` still lists all seven agencies and is now stale relative to this decision; this ADR supersedes its source list.
