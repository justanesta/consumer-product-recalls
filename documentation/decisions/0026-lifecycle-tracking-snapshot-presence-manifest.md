# 0026 — Lifecycle tracking via per-run snapshot-presence manifest

- **Status:** Draft
- **Date:** 2026-05-01
- **Supersedes:** —
- **Superseded by:** —

> **Acceptance criteria** (must be resolved before promoting this ADR to Accepted):
>
> 1. **Confirm scope** — is this needed for USDA only, or for CPSC and FDA as
>    well? See "Applicability check" below; the answer changes the migration shape.
> 2. **Pick the manifest representation** — separate table (`extraction_run_identities`)
>    or JSONB column on `extraction_runs`. Both are sketched below.
> 3. **Decide when this lands** — it isn't a Phase 5b blocker; the natural home
>    is alongside the silver `current_content` / `edit_count` columns in Phase 6,
>    but you may want to land the bronze-side manifest earlier so historical
>    runs start contributing data to the table from day one.

---

## Context

### The lifecycle states

Recall records published by USDA FSIS, and likely the other sources, transition
through five lifecycle states between extraction runs:

| # | State | What the source returns on the next run |
|---|---|---|
| 1 | Newly published | The record appears for the first time |
| 2 | Edited | The record reappears with changed content |
| 3 | Republished unchanged | The record reappears with identical content (no-op) |
| 4 | Retracted | The record is **absent from the response** |
| 5 | Re-published after retraction | The record reappears later, possibly with edits |

USDA's documented behavior — "new recalls get frequently taken up/put down and
edited right after initial posting" — exercises all five states regularly. Phase
5b verification empirically observed states 1, 2, 3, and 4 within a single 4-hour
window: `PHA-04302026-01` was published at 00:51 UTC, then absent at 01:35 UTC,
then republished at 01:47 UTC, then absent again at 01:51 UTC. The same window
captured a state-2 edit on `PHA-04092026-01` (two distinct content hashes for
the same `(source_recall_id, langcode)` identity).

### What bronze handles natively (ADR 0007 + the composite-identity fix)

The bronze layer is **insert-only** with **content-hash-keyed dedup** (ADR 0007),
extended in Phase 5b to use composite identity tuples — `(source_recall_id,
langcode)` for USDA, `(source_recall_id,)` for CPSC/FDA — so bilingual siblings
do not collide on the dedup query. With this design:

| State | Bronze behavior | Correct? |
|---|---|---|
| 1. Newly published | Insert new row | ✓ |
| 2. Edited | Insert new row, prior version preserved as history | ✓ |
| 3. Republished unchanged | Hash matches → no-op | ✓ |
| 4. Retracted | **No signal** — the record is simply absent from the input batch, so the loader does nothing | ⚠ gap |
| 5. Re-published after retraction | Insert if content changed since last seen, dedup if not | ✓ |

### The retraction gap

Bronze cannot distinguish "this record has been retracted upstream" from "this
record's content is unchanged so dedup skipped it." Both produce identical
bronze-layer artifacts: zero new rows, the prior latest-version row remains.
Without a positive signal of presence per run, silver cannot honestly answer:

- *Is this recall currently published upstream?*
- *When did this recall first appear?*
- *When was it last seen in a successful extraction?*
- *Has it ever been retracted and republished?*

These are first-class consumer questions. The "Active recalls dashboard" view
in gold needs to filter on a `is_currently_active` dimension; the "edit cluster
in the first 14 days after publication" view needs `first_seen_at`.

### Why the existing `extraction_runs` table is insufficient

`extraction_runs` (migration 0001) records run-level metadata — `records_extracted`,
`records_inserted`, `started_at`, `status` — but not the **identity tuples**
present in each run. A retraction event is a *change in set membership*; the
present table only records *cardinality*.

### Empirical signals from Phase 5b

Three findings from Phase 5b first-extraction reinforce that this is real,
not theoretical:

- **State-2 edit captured.** `PHA-04092026-01` has two bronze rows with
  identical `(source_recall_id, langcode)` and distinct `content_hash` values,
  4 minutes apart. The deep-rescan loader correctly persisted both versions.
- **State-4 retraction observed but not represented.** The aggregate counts
  shifted between runs (2002 → 2001 → 2002 → 2001) as `PHA-04302026-01`
  toggled in and out of the response. Bronze records the toggling implicitly
  by *not* getting a new insert, but no row says "this record was absent at
  time T."
- **Bilingual pairs are not atomically updated.** Section 10 of
  `scripts/sql/explore_usda_bronze.sql` showed 105/789 bilingual pairs
  (~13.3%) have mismatched `last_modified_date` between EN and ES siblings —
  contradicting Finding F's "atomic update" claim. FSIS sometimes touches one
  language and not the other, so a per-language presence signal matters.

---

## Decision

Add a **per-run identity manifest** that records, for each successful
extraction, the set of `(source_recall_id, identity-tuple-suffix)` values that
were present in the response. Silver consumes the manifest to compute lifecycle
dimensions (`first_seen_at`, `last_seen_at`, `is_currently_active`,
`was_ever_retracted`, `edit_count`) on top of the bronze content store.

### Manifest representation: two options

**Option A — Separate table.**

```sql
CREATE TABLE extraction_run_identities (
    run_id          TEXT NOT NULL REFERENCES extraction_runs(run_id),
    source          TEXT NOT NULL,
    source_recall_id TEXT NOT NULL,
    -- additional identity columns per source (e.g. langcode for USDA);
    -- nullable for sources without composite identity.
    langcode        TEXT NULL,
    PRIMARY KEY (run_id, source, source_recall_id, langcode)
);

CREATE INDEX ix_eri_source_recall_lookup
    ON extraction_run_identities (source, source_recall_id, langcode);
```

Pros: indexable, queryable from dbt without parsing JSONB, scales to large
volumes (NHTSA could push 80K+ identities per run).

Cons: explicit migration, ~2K rows/run for USDA, more rows for FDA/NHTSA.

**Option B — JSONB column on `extraction_runs`.**

```sql
ALTER TABLE extraction_runs
    ADD COLUMN identities JSONB NULL;
-- Stored as: [["004-2020","English"],["004-2020","Spanish"], ...]
```

Pros: no new table, atomic with the run row, easy to populate.

Cons: unindexed access patterns are slow (every silver query has to expand the
array), JSONB scaling cliffs around ~80K entries per row (NHTSA again).

**Recommendation: Option A** if scope includes more than USDA. Option B is
acceptable if scope is USDA-only and stays under ~5K identities per run.

### Bronze-layer change

`BronzeLoader.load()` currently writes bronze rows + rejected rows in a single
transaction. Extend it to also write the manifest in the same transaction
(ADR 0020 — pipeline-state tracking via single-transaction commits). The
manifest write is constructed from the same identity tuples already computed
during dedup, so there is no duplicate work.

### Silver-layer derivations enabled

Once the manifest is populated, silver gains these dimensions on top of
bronze's `current_content`-projection:

| Dimension | How computed |
|---|---|
| `first_seen_at` | `MIN(extraction_runs.started_at)` per identity tuple |
| `last_seen_at` | `MAX(extraction_runs.started_at)` per identity tuple |
| `edit_count` | `COUNT(DISTINCT content_hash)` per identity in bronze |
| `is_currently_active` | identity tuple is in the manifest of the most recent successful run |
| `was_ever_retracted` | gap between `first_seen_at` and `last_seen_at` covers a successful run where the identity was absent |

These dimensions are silver-layer derivations — bronze remains an immutable log
of "what we saw at extraction time T."

### Gold-layer consequences

Gold serving views (Phase 8) filter by silver's lifecycle dimensions. Concrete
examples:

- "Active recalls dashboard": `WHERE is_currently_active`
- "Edits in first 14 days post-publication": temporal join against `first_seen_at`
- "Recall history detail page": all bronze rows for the identity, ordered by `extraction_timestamp`

---

## Applicability check (resolve before acceptance)

This pattern is needed for any source whose response semantics include
**implicit deletion** — records can disappear from the response without an
explicit `is_deleted` flag or a tombstone signal. Each source needs a quick
audit:

### USDA — confirmed needed

- Empirical evidence above. State-4 retractions observed within hours of new
  publication. Bilingual non-atomic updates compound the "what's currently
  published" question. The manifest is load-bearing for accurate silver
  projections.

### CPSC — likely needed, verify before deciding

CPSC uses `LastPublishDate` as the watermark (ADR 0010). The watermark is
intended to be monotonically advancing on edits, but Phase 3's first-extraction
findings document CPSC may quietly drop records too. Specifically: does CPSC's
SaferProducts API ever return a record one week and not the next? Two ways to
answer:

1. **Empirical:** snapshot the full SaferProducts dataset on day N, repeat on
   day N+30, diff the `RecallID` sets. Any IDs missing from day N+30 = retraction.
2. **API documentation review:** the SaferProducts docs (in
   `documentation/cpsc/`) — does it describe a deletion semantic?

If CPSC retracts, the manifest applies. If it never retracts (records are
append-only upstream), the manifest is unneeded but cheap; landing it anyway
costs little and gives uniform silver dimensions across sources.

### FDA — probably not needed, verify before deciding

FDA iRES uses `EVENTLMD` as a monotonically-advancing watermark (ADR 0010,
Findings J/M in `documentation/fda/api_observations.md`). Records do not
appear to be retracted from the bulk POST response — the documented lifecycle is
phase transitions (Ongoing → Terminated), not removal. The deep-rescan workflow
(`deep-rescan-fda.yml`, ADR 0023) handles edits-without-watermark-advance.

That said, the same empirical check applies: snapshot the full bulk POST result
twice over a meaningful window and diff. If no `PRODUCTID` ever disappears, FDA
does not need this manifest. If even rare retractions occur, the manifest applies.

### NHTSA — TBD, evaluate at Phase 5c

NHTSA is a full-snapshot flat file. Each release supersedes the previous one;
records absent from the new file are retracted by definition. The manifest
applies trivially — every flat file *is* a manifest. Implementation may collapse
to "the raw payload's identity set, computed at land time."

### USCG — TBD, evaluate at Phase 5d

HTML scrape; pagination + structural parsing. Retraction is "the recall no
longer appears on the listing pages." The manifest applies, but the scrape's
own brittleness probably dominates the architectural concerns at that point.

### Cross-source decision

**Recommended scope:** populate the manifest for all five sources from
day-one of the implementation, treating "no retractions ever observed" as a
discovered property rather than an architectural assumption. Cost is small
(an extra batch insert per run) and the silver dimensions land uniformly.

If cost becomes a concern (NHTSA's 80K+ identities/run scaling), the manifest
can be made source-conditional via a per-source `track_presence: bool` flag on
the extractor config.

---

## Consequences

### Positive

- **Closes the bronze retraction gap** — silver can answer "is this record
  currently published" without inferential heuristics.
- **Unlocks lifecycle dimensions in silver** — `first_seen_at`, `last_seen_at`,
  `is_currently_active`, `edit_count`, `was_ever_retracted`. All five are
  consumer-grade dimensions for gold serving views.
- **Source-uniform lifecycle model** — once the manifest is in place, silver's
  lifecycle dimensions look the same regardless of source-specific extractor
  quirks (USDA's full-dump vs FDA's incremental-with-watermark vs NHTSA's
  flat-file replacement). The manifest abstracts over those differences.
- **Compatible with the existing single-transaction commit pattern** (ADR 0020)
  — the manifest is written in the same `engine.begin()` block as bronze and
  watermark updates.
- **Cheap audit history** — diffing two manifests answers "what changed between
  these two runs?" with one SQL query, no R2 reads required.

### Negative

- **Manifest writes scale with `records_fetched × runs`.** USDA: ~2K rows/run.
  FDA incremental: ~50/run. FDA deep-rescan: ~3K/run. NHTSA: ~80K/run if we
  use Option A. If retention becomes an issue, pruning policy (keep last 90
  days of manifests) is straightforward.
- **Schema growth** — one new table or one new column. Migration cost is small
  but it's another piece of state to back up, monitor, and reason about.
- **Silver query complexity** — lifecycle dimensions require joining bronze
  against the manifest against `extraction_runs`. The dbt model is non-trivial
  but well-bounded.
- **Cost for sources that don't need it** — if CPSC and FDA never retract,
  their manifests are dead weight that costs disk + writes for no behavioral
  benefit.

---

## Alternatives considered

### Alternative 1 — Implicit retraction inference

Treat "absent from the last N consecutive runs" as a presumed retraction.

- **Pros:** no schema changes, no extra writes.
- **Cons:** approximation; misses fast retract/republish cycles (we observed two
  toggles inside 4 hours during Phase 5b verification — N=2 would be wrong here);
  introduces timing-dependent silver semantics; debug story is bad.
- **Verdict:** rejected. The retraction behavior matters too much to model with
  a heuristic.

### Alternative 2 — R2 manifest reads

Have silver dbt models read the raw R2 payloads, extract identity tuples, and
build the presence map from raw landed data.

- **Pros:** no schema changes; raw payloads are already authoritative.
- **Cons:** dbt has to read R2 (or a Postgres replica of R2), substantial
  pipeline complexity; runs read large blobs to answer "did identity X appear
  in run Y?"; dbt's incremental modeling becomes harder.
- **Verdict:** rejected. Over-uses R2 as a query substrate. R2 is the immutable
  history layer; silver should consume from Postgres.

### Alternative 3 — dbt Type 2 SCD snapshot on bronze

Use dbt's built-in `snapshots/` with `unique_key=(source_recall_id, langcode)`
and `strategy='check'` or `strategy='timestamp'`.

- **Pros:** off-the-shelf dbt feature; produces `dbt_valid_from` /
  `dbt_valid_to` columns; well-documented pattern.
- **Cons:** dbt snapshots assume the source query reflects "current truth."
  Bronze does not — bronze includes history rows from prior extractions. We'd
  need to first project bronze to "latest version per identity," then snapshot
  that, which is more layers than the manifest approach. Also doesn't answer
  the "currently active" question without an explicit presence signal.
- **Verdict:** rejected as a primary mechanism, but worth considering as a
  silver-internal pattern *on top of* the manifest for SCD-style consumers.

### Alternative 4 — Per-run CTE over raw payloads

Compute the manifest on-demand at silver build time by parsing R2 raw payloads
(via Postgres `jsonb` parsing or external Python).

- **Pros:** no new state; computed lazily.
- **Cons:** silver builds become slow (read N R2 objects per build); R2 access
  patterns from dbt are awkward; reproducibility is iffy if R2 objects are ever
  retention-pruned.
- **Verdict:** rejected. Same shape as Alternative 2 with worse ergonomics.

---

## Implementation sketch (if Option A is chosen)

1. **Migration 00xx:** add `extraction_run_identities` table per the schema
   above; add the supporting index.
2. **`BronzeLoader.load()`:** after computing identity tuples for the current
   batch, write them to `extraction_run_identities` with `(run_id, source,
   identity_tuple)` in the same transaction as the bronze inserts.
3. **`Extractor` ABC:** thread `run_id` through to `load_bronze()` so the
   loader has the context to populate the manifest. Currently `run_id` is
   private to `Extractor.run()` — it'd need to be either passed explicitly or
   set as a `PrivateAttr` on the extractor before `load_bronze` is called.
4. **Silver dbt model `recall_lifecycle.sql`:** join bronze against the
   manifest against `extraction_runs` to produce the five lifecycle columns
   above.
5. **Retention:** decide on a TTL for old manifest rows. Recommendation: keep
   forever for now; revisit if disk cost becomes meaningful.

---

## Open questions

- **Q1:** Is the ABC change (thread `run_id` to `load_bronze`) acceptable, or
  should the manifest write happen *after* `load_bronze` returns, in the
  template `Extractor.run()`? The latter is less invasive but loses the
  single-transaction property if any failure occurs between `load_bronze` and
  the manifest write.

- **Q2:** Should the manifest also record records that were *quarantined*?
  Argument for: a quarantined record is "present at the source," even if we
  couldn't validate it. Argument against: the manifest is about
  bronze-table-presence, not response-presence; raw R2 is the source of truth
  for response-presence. Recommendation: only record records that successfully
  landed in bronze; raw R2 is the residual log for everything else.

- **Q3:** Should we backfill the manifest for runs that already happened
  pre-ADR? The data exists in R2; we'd need to write a one-shot job that reads
  each historical R2 payload and synthesizes manifest rows. Probably worth doing
  if we believe historical lifecycle data is valuable; cheap to skip if not.
