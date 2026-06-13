# CPSC products[] array-stability findings

Empirical baseline for the load-bearing assumptions that CPSC's `Products[]`
JSONB array is append-only (assumption C2 in `documentation/source_assumption_audit.md`)
and that product `name`/`model` strings are not character-normalized after
publication (assumption C3).

As of the 2026-06-13 key migration (ADR 0031 amendment), the silver
`recall_product_id` derivation (`cpsc_products` CTE, `recall_product.sql`) is the
stable (event, ordinal) anchor:

```
md5('CPSC' || '|' || source_recall_id || '|' || product_ordinal)
```

This splits the two assumptions' stakes:

- **C2 (append-only) is now an identity *invariant*.** With name/model out of the
  key, the ordinal alone carries product identity, so a reorder / mid-array insert
  no longer *fragments* (loud) — it *conflates* (silent): a later product inherits an
  earlier slot's id. `assert_products_array_append_only` is the guard.
- **C3 (name/model normalization) no longer touches the key.** A post-publication
  name/model edit re-versions a Type-1 latest-wins *attribute* (the current view
  shows the latest via `stg_cpsc_recalls`); it does **not** change the surrogate or
  fragment. C3 is now an informational editorial monitor (input to the deferred
  CPSC product-grain history — TODO "Move 2").

(Pre-migration, both fed `md5(event_id|name|model|ordinal)` and a C3 edit churned the
id; the dated sections below predate the migration and are kept as provenance.)

## Detection scripts

- **C2 (array append-only):** `scripts/sql/cpsc/bronze/assert_products_array_append_only.sql`
- **C3 (name/model normalization):** `scripts/sql/cpsc/bronze/assert_name_model_normalization_stable.sql`

Both are also wrapped as dbt singular tests at `severity=warn` under
`dbt/tests/source_assumptions/`.

## Baseline as of 2026-05-08

### C2 — products[] append-only

| Output cell | Observed | Notes |
|---|---|---|
| Q1 `drift_group_count` | **0** | Headline: no observed array reorder or mid-array insert. |
| Q2 sample violations | (0 rows) | Empty as expected given Q1 = 0. |
| Q3 array-length distribution | **n_products=1: 1,360 rows / 1,357 distinct recalls** | All observed CPSC recalls have exactly 1 product. |

**Interpretation**: Q3 confirms the single-product-per-recall observation
from `first_extraction_findings.md` Section A still holds at the
larger corpus size (1,357 vs. the original 1,193). The Q1 = 0 result
therefore does NOT validate the C2 assumption — it means **the failure
mode physically cannot fire on a 1-element array**. The assumption
remains an unfalsified-but-untested trust claim until bronze accumulates
multi-product recalls.

**Side observation — bronze excess of 3 rows over distinct recalls.**
Bronze contains 1,360 rows for 1,357 distinct `source_recall_id`s.
Per ADR 0007, bronze keeps every distinct content snapshot — the
3-row excess means three recalls have been observed with >1 distinct
`content_hash` over time. Attribution (2026-05-08):

| `source_recall_id` | bronze rows | `change_types` | First seen | Last seen | Class |
|---|---|---|---|---|---|
| `00015` | 2 | `{routine}` | 2026-05-07 11:03 UTC | 2026-05-08 19:58 UTC | **Genuine CPSC edit** |
| `02012` | 2 | `{schema_rebaseline, NULL}` | 2026-04-21 02:45 UTC | 2026-05-02 23:54 UTC | Infrastructure rebaseline (ADR 0027); NULL is the pre-`change_type`-column snapshot (migration 0009) |
| `26428` | 2 | `{schema_rebaseline, NULL}` | 2026-04-21 02:45 UTC | 2026-05-02 23:54 UTC | Infrastructure rebaseline; same NULL explanation |

**Significance — first observed CPSC edit.** Recall `00015` is the
first genuine source-side edit detected on CPSC bronze in the project's
history. `documentation/cpsc/first_extraction_findings.md` Section E
documented "zero edits detected" across the 1,193-row corpus snapshot
that predated this work. As of 2026-05-08, that finding is superseded:
edit detection now works and has fired once.

The edit landed via the `routine` (daily incremental) extraction path,
not via deep-rescan — confirming that whatever CPSC changed was visible
through the normal `LastPublishDate`-filtered query window, not buried
in the historical archive.

**What changed in recall `00015` (inspected 2026-05-08).** A single
copy-edit on `retailers[0].name`:

| Field | 2026-05-07 snapshot | 2026-05-08 snapshot |
|---|---|---|
| `retailers[0].name` (excerpt) | `…suggested retail price of between $125 **to** $175.` | `…suggested retail price of between $125 **and** $175.` |
| `products[]` array | (unchanged, byte-identical) | (unchanged, byte-identical) |
| `last_publish_date` | `2026-05-07T00:00:00+00:00` | `2026-05-07T00:00:00+00:00` (did NOT advance) |

**What this confirms / refines:**

- **C2 (products[] append-only): not falsified.** The edit didn't touch
  `products`. The first observed CPSC edit happened to land on a
  peripheral text field, not a silver-key-input field.
- **C3 (name/model normalization): not falsified.** `products[0].name`
  and `products[0].model` byte-identical between snapshots.
- **C4 (LastPublishDate advances on edits): re-confirmed false in real
  time.** CPSC fixed a typo without advancing `last_publish_date`.
  Mandatory weekly deep-rescan (ADR 0010 + `last_publish_date_semantics.md`)
  remains correct policy — without it, same-day edits like this would
  be missed across runs that happen to fall outside the incremental
  query window.

**Subtle extraction observation.** The edit was captured via the
`routine` (incremental) extraction path despite `last_publish_date` not
advancing. This means the routine query window is wider than a strict
`> watermark` filter — likely `>=` at the boundary, which catches
same-day re-publishes opportunistically. The deep-rescan policy is
still load-bearing because routine-path opportunism only catches edits
that happen within roughly one extraction cycle of the publish date;
older edits would only surface via deep-rescan.

**Net empirical position on CPSC C2/C3 after this edit:** the
assumptions remain unfalsified-but-untested. The first observed edit
landed outside the surrogate-key surface, so we still have zero
evidence either way on whether CPSC normalizes name/model or reorders
products[]. Continued monitoring as more edits accumulate.

### C3 — name/model normalization

| Output cell | Observed | Notes |
|---|---|---|
| Q1 `drift_group_count` (name)  | **0** | No `(source_recall_id, ordinal)` slot had its `name` character-normalized across runs. |
| Q1 `drift_group_count` (model) | **0** | Same, for `model`. |
| Q1 `TOTAL` | **0** | Headline assertion holds. |
| Q2 sample name-drift cases  | (0 rows) | Empty as expected given Q1 = 0. |
| Q3 sample model-drift cases | (0 rows) | Same. |

**Interpretation**: This is **NOT a vacuous result**. Recall `00015`
provided one real cross-run scenario (2 bronze rows with different
`raw_landing_path` and different `content_hash`). The script grouped
those two rows under `(source_recall_id='00015', product_ordinal=1)`
and confirmed both `name` and `model` were byte-stable across the
edit — see the recall `00015` payload diff in the bronze-excess
section above. The edit was on `retailers[0].name`, not on any
products[] field.

So as of 2026-05-08, **C3 has one tiny positive empirical signal**:
the only observed CPSC source-side edit did not character-normalize
name or model. One data point isn't validation, but it's no longer
zero. Continued monitoring as more edits accumulate.

## Corpus-scale update — 2026-06-02 (full-corpus seed, 9,828 rows)

The Phase 6a.5 full-corpus deep-rescan seed (extraction run 2026-05-31;
9,828 rows / 9,828 distinct recalls) **falsifies the single-product premise**
the 2026-05-08 "vacuous" interpretation rested on. Source:
`scripts/sql/cpsc/bronze/explore_bronze_shape.sql` Q8 +
`inspect_array_field_population.sql` Q3, folded into
`documentation/audit/bronze_corpus_profile.md` §2/§6.

### Multi-product recalls now exist at scale — the ordinal key is load-bearing

| `products[]` length | recalls | % |
|---|---|---|
| 1 | 9,011 | 91.69% |
| 2 | 520 | 5.29% |
| 3 | 124 | 1.26% |
| ≥4 | 173 | 1.76% |
| **max** | **57** | one recall |

11,836 product elements across 9,828 recalls (mean ~1.20). **8.3% of recalls
carry >1 product.** The silver `recall_product` model now genuinely fans out to
multiple rows per recall, so `product_ordinal` in the md5 surrogate key at
`recall_product.sql:38-46` is **load-bearing, not decorative** — and the C2/C3
failure mode is now *physically possible* (it could not fire on a uniformly
1-element corpus). The "failure mode physically cannot fire on a 1-element
array" reasoning from the 2026-05-08 baseline is **retired**.

### But C2/C3 are still untested — the blocker shifted, it didn't clear

Detecting an append-only violation (C2) or a name/model re-normalization (C3)
requires **two content-hash snapshots of the same recall** to diff. The fresh
single-shot seed has **0 edit-versions** (`explore` Q4/Q5: 9,828 rows / 9,828
distinct, 0 multi-hash recalls). Consequences:

- The C2/C3 assert scripts still return `drift_group_count = 0` — but now
  because there is **nothing to compare**, not because arrays are 1-element.
- The re-seed reset bronze to one row per recall, so the lone 2026-05-08 recall
  `00015` C3 data point **predates this seed and is no longer in the live
  table** (it stands above as provenance only). C3 is back to zero observed
  cross-run scenarios in current bronze.
- The assumptions become genuinely testable only once **multi-version ×
  multi-product** recalls accumulate (daily incrementals + weekly deep-rescans
  over time).

**Net position (2026-06-02):** the silver product surrogate key is now exercised
by real fan-out — validating that the ordinal-based recipe is *necessary* — while
the append-only / normalization-stability guarantees on that key remain
**unproven and now non-vacuously untested**. Keep both assert scripts running at
`severity=warn`; the first multi-version multi-product recall is the real test.

## First genuine C2/C3 test — 2026-06-13

The "first multi-version × multi-product recall" trigger (below) has **fired**:
daily incrementals + deep-rescans since the 2026-06-02 reseed have accumulated
cross-version recalls, so both assertions now run on real comparison data.

### C2 — append-only: validated at scale (0 violations)

`assert_cpsc_products_array_append_only` (dbt) **PASS**; the psql script's
`drift_group_count = 0`, with Q3 showing multi-product recalls present in quantity
(520 × 2-product, 124 × 3, … up to one 57-product recall). For the first time the
failure mode is *both physically possible and exercised on cross-version data* — and
it holds. This is what de-risked the 2026-06-13 migration of CPSC product identity
onto the `(event, ordinal)` anchor (ADR 0031 amendment): the invariant the new key
depends on is now empirically validated, not merely trusted.

### C3 — name/model: first real drift observed (9 name cases)

`assert_cpsc_name_model_normalization_stable` returns **9** (`name` 9, `model` 0).
All 9 sit at `product_ordinal = 1` on a contiguous block of low-numbered (old)
recalls (`00079`–`00083` in the sampled five), and 3 of the 5 sampled are
empty-string → populated transitions — the signature of a **one-time
early-capture/backfill** (an early seed banked sparse/empty product names for a block
of old recalls; a later run populated them), not scattered organic CPSC editorial
churn. *(Inference — confirm by comparing the two `raw_landing_path` /
`extraction_timestamp` per case; if the empty one is the earliest seed, that's it.)*

Post-migration this is **informational, not a fragmentation event**: the current
`recall_product` already shows the populated (latest) name via latest-wins, and the
surrogate is unaffected. The 9 are the natural input to the deferred Move-2 history
surface.

## ADR 0031 threshold reconciliation

ADR 0031's CPSC row currently reads:

> Phase 6 revisit threshold: >0.1% silver row count fragmented per quarter

After running the scripts, update the ADR row with:

- **Detection status cell**: replace "TBD" with `scripts/sql/cpsc/bronze/assert_products_array_append_only.sql` + the C3 sibling.
- **Threshold cell**: if both Q1s are 0 and Q3 confirms array-length=1 dominates, note that the threshold is "vacuous so far — assumption physically cannot fail at observed array sizes" alongside the existing 0.1% figure. Revisit when multi-product recalls land. **Updated 2026-06-02:** multi-product recalls have landed (8.3%, max 57 — see the corpus-scale update above), so retire the "vacuous" qualifier; the failure mode is now physically possible and testability is blocked on edit-versions (0 in the current seed), not array size. The 0.1%-per-quarter revisit threshold stands.

## Follow-up triggers

- If Q1 (C2 append-only) ever returns >0: **identity conflation, not fragmentation** (since the 2026-06-13 ordinal-only key). Treat as a correctness incident — a later product has inherited an earlier slot's `recall_product_id`. Remediate with a deterministic in-slot tiebreaker or a re-key, rebuilding silver from bronze (the immutable all-versions source of truth, ADR 0007), and escalate the `assert_products_array_append_only` wrapper to a hard gate. This **supersedes** the old "switch to a content-based surrogate" fix — the project deliberately moved the *other* way (content out of the key) for id durability.
- If Q1 (C3, either field) ever returns >0: **informational** since the 2026-06-13 migration — name/model are out of the key, so drift re-versions a latest-wins attribute, not the surrogate. No Phase 6 reconciliation trigger. These rows are the input to the deferred CPSC product-grain history (TODO "Move 2"); product-level fuzzy resolution remains a separate Phase 6 firm-parallel item (`implementation_plan.md:606-610`).
- ~~If Q3 shows the corpus is starting to accumulate multi-product recalls (max > 1)~~ **FIRED 2026-06-02** (8.3% multi-product, max 57). ~~Next trigger: the first recall with **both** >1 product **and** >1 content-hash version~~ **FIRED 2026-06-13** — both assertions now run on real cross-version data: C2 holds at 0 across the multi-product corpus (validating the new key), C3 surfaced its first 9 name-drift cases (see the "First genuine C2/C3 test — 2026-06-13" section).
