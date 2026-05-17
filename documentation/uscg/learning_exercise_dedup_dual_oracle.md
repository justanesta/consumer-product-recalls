# Learning Exercise — Why the Listing-Hash Skip-Details Optimization Breaks Bronze Dedup

**Topic:** A subtle two-oracle correctness trap that surfaced during Phase 5d Step 2 design. Discovered via the Plan-agent critique; this is the reason the design *dropped* the listing-hash optimization rather than adopt it.

**Format:** Predict-then-verify. Pause at each `🔍 Predict:` marker, decide your answer, then continue reading. Generalizes to any dedup-adjacent optimization in any future scrape source.

**Estimated time:** 15 minutes.

---

## Setup — the proposed optimization

The naive cost concern: USCG has 1,763 recalls. Always-fetch-details means 1,763 detail-page GETs per run. At a polite 1.0s throttle that's ~30 minutes wall time.

The "obvious" optimization: compute a fingerprint from the **listing-row fields** (6 fields — `Number`, `MIC`, `Company Name`, `Model Name`, `Problem 1`, `Opened On`). On each subsequent run, fetch only the listing pages (~71 fetches), compute the listing-fingerprint per row, compare against the prior bronze row's fingerprint, and **skip the details fetch if the listing fingerprint matches**. Steady-state: save ~1,763 fetches per week.

It feels clean. It feels obviously correct. **It is neither.**

---

## The two-oracle setup

The pipeline already has one oracle answering "did this row change?" — `BronzeLoader`'s `content_hash`, computed from the **full record dict** (per `src/bronze/loader.py:298-418`). When the new content_hash matches the most recent existing bronze row's content_hash for that identity, the row is deduped (no insert).

The proposed optimization introduces a **second oracle**: the listing-fingerprint, computed from only the 6 listing fields.

Two oracles. The same question. Different scopes. The trap is in the disagreement.

---

## Scenario — a silent details-page edit

**State at run T** — bronze contains this row for recall `25CG0017`:

```
source_recall_id    = "25CG0017"
company_name        = "TIDEWATER BOATS LLC"
opened_on           = 2025-06-04
mic                 = "NLP"
model_name          = "TIDEWATER 180 CC BAY"
problem_1           = "Stability test (starboard"
case_open_date      = 6/4/2025
case_close_date     = 7/23/2025         ← currently populated
campaign_open_date  = 8/19/2025
disposition         = "Open"
units               = "401"
... other details fields ...
```

**Run T+1** — USCG editorial action: the recall has been closed and they update the disposition.

Specifically, USCG sets `disposition = "Closed"` on the details page. Nothing on the listing page changes — the listing's 6 fields are byte-identical. The listing-page fingerprint is therefore identical to T's listing-fingerprint.

🔍 **Predict #1:** Without the optimization (always-fetch-details), what does the bronze row look like after Run T+1?

<details>
<summary>Answer</summary>

A new bronze row is inserted because the full content_hash differs (`disposition` changed). The new row has `disposition = "Closed"`, `case_close_date = 7/23/2025`, all other fields unchanged. `BronzeLoader` correctly detects the change via content_hash; bronze now contains two rows for `25CG0017` — the pre-amendment version (still queryable for lineage) and the current version. Silver's "most-recent per `source_recall_id`" projection emits the current version downstream. **This is correct behavior** — bronze captures the state evolution faithfully.

</details>

---

🔍 **Predict #2:** Now imagine the listing-hash optimization is active. On Run T+1: extractor fetches only the listing page, computes the listing-fingerprint for `25CG0017`, sees it matches the prior run's fingerprint, **skips the details fetch**. What does the extractor emit for `25CG0017` to be passed downstream to `BronzeLoader`?

<details>
<summary>Answer</summary>

It depends on the implementation. Two natural options, both broken:

**Option A: Emit a record with details fields = `None`** (because details weren't fetched).

The emitted record has:
```
source_recall_id    = "25CG0017"
company_name        = "TIDEWATER BOATS LLC"
opened_on           = 2025-06-04
mic                 = "NLP"
model_name          = "TIDEWATER 180 CC BAY"
problem_1           = "Stability test (starboard"
case_open_date      = None       ← was 6/4/2025
case_close_date     = None       ← was 7/23/2025
campaign_open_date  = None       ← was 8/19/2025
disposition         = None       ← was "Open"
units               = None       ← was "401"
... details fields all None ...
```

The full content_hash of this record differs from T's bronze row (every details field went from populated → None). `BronzeLoader` sees a hash mismatch and **inserts a new bronze row with all details fields nulled out**. Bronze now contains:

- Row 1 (T): full data, populated.
- Row 2 (T+1): listing fields present, details fields NULL.

Silver's "most-recent" projection emits Row 2 — the **NULL'd version** — masking the actual data. From silver's perspective, recall `25CG0017` lost all its details fields between T and T+1, even though USCG's only change was `disposition: Open → Closed`. **The optimization just regressed real data into nothingness.**

**Option B: Emit no record at all** (skip the row entirely on a fingerprint match).

The bronze loader's content_hash dedup only runs against rows the extractor emits. A skipped row never reaches the loader. Bronze stays at T's state. Silver still shows `disposition = "Open"` and `case_close_date = 7/23/2025` (T's value).

**This is also broken** — silver lies about USCG's current state. The lie is silent and only fixable by the next deep-rescan run (which always fetches details). Until then, downstream consumers of `recall_event_history` see the recall as still-open with no closure date, when USCG's source-of-truth says it's been closed.

</details>

---

## The structural diagnosis

🔍 **Predict #3:** Both options fail. Can you state the failure in one sentence — what is the structural problem?

<details>
<summary>Answer</summary>

**The listing-fingerprint and the full-record content_hash are two oracles answering the same question ("did this row change?") with different scopes, and they will disagree whenever USCG edits a details-only field.** The optimization assumes the two oracles agree — that listing-fingerprint match implies content_hash match. The implication holds only if USCG never edits a details-only field, which is exactly the kind of edit that's most common (`disposition`, `case_close_date`, `campaign_close_date`, `last_date`).

</details>

---

## The fix — what would it take?

🔍 **Predict #4:** Suppose you really wanted to keep the optimization. What infrastructure would you need to add to make it correct?

<details>
<summary>Answer</summary>

You'd need a **sidecar that decouples "fetch optimization" from "dedup correctness":**

1. A new table `uscg_listing_fingerprints (source_recall_id PK, listing_hash, last_seen_at)` tracking the listing-row hash per recall across runs.
2. In `extract()`: fetch listing → compute fingerprint per row → query sidecar.
3. **On fingerprint match:** don't emit a `None`-stuffed record. Instead, *carry forward* the previous bronze row's details fields verbatim — read them from `uscg_recalls_bronze` (most-recent per `source_recall_id`), merge into the emitted dict so the full content_hash matches T's row exactly. `BronzeLoader` then sees identical hash → no insert. ✓ Correctness preserved, ~1,763 details fetches skipped.
4. **On fingerprint miss:** fetch details normally, emit complete row, let content_hash dedup decide.

Now you have three places where the schema lives: the bronze row, the sidecar fingerprint table, and the merge logic that reconstructs the prior row's details. Each is a potential drift surface. The next time someone adds a new details field, all three need to evolve coherently. And you've added a new table to a project that doesn't otherwise have sidecar metadata tables — schema complexity creep.

Meanwhile the cost saved: ~30 minutes once per week of historical seed + ~80 seconds per weekly cron. Trading complexity for ~3 seconds per cron run is a bad deal. **The optimization isn't worth the correctness machinery.**

</details>

---

## Generalizable rule

When considering ANY optimization in or near a dedup mechanism, the test is:

> *Is the optimization introducing a second oracle that answers the same correctness question as the existing oracle, with a smaller scope?*

If yes, the optimization is either incorrect or requires sidecar machinery to bridge the scope gap. Compute the complexity cost before assuming it's worth the savings. Most of the time it isn't, especially when the "savings" amortizes weekly and the cost amortizes never.

This shows up frequently:
- **Caching layers** — a cache hit short-circuits the slow oracle. If the cache key uses fewer dimensions than the slow oracle, you have the same bug.
- **Prefilter indexes** — a "did anything change?" index over a subset of columns. Same trap.
- **Conditional GETs / ETags** — the server's ETag is one oracle, the body content is another. ETag-without-body-validation is the same bug. (USCG actually has zero ETag/Last-Modified — Finding K — so this isn't a Phase 5d concern, but it's the same pattern.)
- **Watermarks** — a date cursor is an oracle; the underlying data is another. They can disagree, e.g. USDA's pre-2026-05-09 ETag-stale-positives covered in `documentation/decisions/0024-...`.

---

## Where this lives in the codebase

- `src/bronze/loader.py:298-418` — the canonical content_hash oracle (`BronzeLoader.load`)
- `src/extractors/uscg.py:_extract` — the chosen design: always-fetch-details, no second oracle
- `documentation/uscg/scraping_observations.md` Finding J — captures the cheap-but-not-load-bearing signal (`Records Found` total) for a future Phase 6+ smart-skip layer; explicitly NOT used as a runtime oracle

The Plan-agent critique that surfaced this (during the Phase 5d planning conversation, 2026-05-16) is the reference for the dual-oracle framing. If you ever revisit the optimization (e.g., if USCG's corpus grows to 50k+ recalls and the always-fetch cost becomes operationally painful), re-read this exercise first — and add the sidecar table, the merge logic, and the "carry-forward prior bronze row" path. Don't take the shortcut.
