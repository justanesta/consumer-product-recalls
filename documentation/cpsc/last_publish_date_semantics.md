# CPSC `LastPublishDate` Semantics

**Investigation date:** 2026-04-20
**Data basis:** 1,193 bronze rows from a 365-day lookback extraction against the CPSC Recall Retrieval Web Services API

---

## Background

ADR 0010 uses `LastPublishDate` as the incremental cursor for CPSC extraction, querying with `LastPublishDateStart=<watermark>`. Before relying on this in production, the plan called for empirical verification of what events actually advance `LastPublishDate` and whether a weekly deep-rescan workflow is necessary or can be relaxed.

---

## Findings

### 1. `LastPublishDate` advances on two event types only

Analysis of the gap between `recall_date` (original announcement) and `last_publish_date` across 1,193 records reveals an almost perfectly bimodal distribution:

| Gap | Records | Interpretation |
|---|---|---|
| 0 days | 307 | Announced and published same day |
| 1 day | 171 | Standard next-day publication lag |
| 2–7 days | 6 | Finalization lag (e.g. one updated Dec 24 after Dec 18 announcement) |
| 8 days – 5 years | **0** | — |
| Over 5 years | 709 | Archive migration (see below) |

There are **zero records** in the 8-day to 5-year range. No evidence was found that `LastPublishDate` advances on genuine mid-life content edits (e.g. remedy updates, classification changes, recalled-product count revisions). The 2–7 day records are consistent with slow finalization of new recalls, not edits to previously published ones.

**Conclusion:** `LastPublishDate` reliably advances when CPSC publishes or re-processes a record. It is not confirmed as a signal for content edits to already-published records.

### 2. An active archive migration is underway (as of April 2026)

709 of the 1,193 fetched records are CPSC recalls from 2000–2004 with `last_publish_date` values in March–April 2026 — gaps of approximately 25 years. These records appear in the watermark-based extraction because CPSC is currently re-processing its pre-2004 archive, advancing their `LastPublishDate` to the current date as it does so.

The migration is proceeding sequentially by `source_recall_id` at roughly 2–3 records per day. As of the extraction date, CPSC had worked through approximately year 2004. This is ongoing and the migration will continue to surface old records in incremental extractions until it completes.

The content-hash dedup in `BronzeLoader` handles this correctly: re-processed records with unchanged content produce no new bronze rows on subsequent runs.

### 3. A 20-year gap in the database

| Year range | Records in bronze |
|---|---|
| 2000–2004 | 709 (captured via archive migration) |
| **2005–2024** | **0** |
| 2025–2026 | 484 |

Records from 2005–2024 exist in the CPSC API but have not been touched since before the 365-day lookback window. They are invisible to the watermark-based incremental strategy. The archive migration is expected to surface these records over time, but it is currently at ~2004 and will take years to reach 2024 at its current pace.

**A one-time deep rescan with a multi-year lookback is the only way to load this historical data.** This is the primary justification for the `deep-rescan-cpsc.yml` workflow — not just a periodic safety net but the mechanism for the initial full historical load.

### 4. Publication cadence

CPSC publishes 20–30 new recalls per week at a very consistent rate throughout the year. Two notable spikes were observed in the 365-day window:

- **Week of 2026-02-23:** 59 recalls (~2× normal) — likely a batch catch-up
- **Week of 2025-11-03:** 42 recalls

Outside of these spikes the cadence is stable and predictable, which supports a daily cron extraction with a 1-day watermark increment as sufficient to stay current.

---

## Implications for ADR 0010

| Decision | Verdict |
|---|---|
| Use `LastPublishDateStart` as incremental cursor | Confirmed correct — advances reliably on new publications |
| Deep-rescan workflow is a safety net | Confirmed — but more importantly it is the mechanism for the initial full historical load of 2005–2024 data |
| Relax deep-rescan if `LastPublishDate` reliably advances on edits | **Cannot relax** — no evidence that edits advance `LastPublishDate`; deep-rescan remains necessary |
| Content-hash dedup sufficient for idempotency | Confirmed — archive migration records re-ingested cleanly with no duplicate rows |

### Recommended next action

Run `deep-rescan-cpsc.yml` via `workflow_dispatch` with a lookback sufficient to cover 2005–present before Phase 7 cron schedules go live. This populates the missing 20 years of recall history. Coordinate timing with the Neon `main` branch migration run to avoid overloading the database during initial setup.
