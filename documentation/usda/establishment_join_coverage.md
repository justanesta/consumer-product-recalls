# Recall → Establishment Join Coverage

> **Status: Coverage measured.** Probe results 2026-05-01 against
> `usda_fsis_recalls_bronze` (1,213 English recalls) and
> `usda_fsis_establishments_bronze` (7,945 establishments).
> Source query: `scripts/sql/usda_establishments/bronze/probe_recall_join_coverage.sql`.

## Purpose

Phase 5b.2 Step 3's gating question: **of the USDA recall events that name an
establishment, what fraction match a known establishment in the listing API?**
The answer drives the silver join shape (Step 5) — whether to require DBA
fallback, whether fuzzy matching is needed in v1, and whether the join is
worth building at all if coverage is too low.

The pre-extraction observations doc (`establishment_api_observations.md`,
Finding F) confirmed a 1:1 join on a single test record. This probe extends
that to all 1,213 records.

---

## Top-line counts

| Metric | Value |
|---|---|
| Total English recalls | 1,213 |
| Recalls with `establishment` populated | 788 (64.96%) |
| Distinct recall establishment names | 548 |
| Total establishments in listing | 7,945 |
| Distinct `establishment_name` values in listing | 6,841 |

The 788/1,213 ≈ 65% recall-establishment populated rate matches the dbt
spot-check from the Phase 5b silver PR exactly (788 USDA establishment
firm rows out of 1,213 USDA recall events). 425 recalls have no
establishment field at all and are unjoinable regardless of join shape.

---

## Match rates

### Per distinct recall name

| Strategy | Matched | Unmatched | Match % |
|---|---|---|---|
| Name-only (`upper(trim(establishment))` ↔ `upper(trim(establishment_name))`) | 454 | 94 | **82.85%** |
| Name + DBA fallback (also try `dbas` array elements) | 454 | 94 | **82.85%** (no improvement) |

### Per recall record

| Match status | Count | % of all recalls |
|---|---|---|
| `matched` (recall.establishment maps to a known establishment_name or DBA) | 667 | 54.99% |
| `no_establishment_field` (`establishment is null` or empty) | 425 | 35.04% |
| `unmatched` (establishment populated but no match in listing) | 121 | 9.98% |

Of the 788 recalls *that name an establishment*, 667 (84.6%) join cleanly.
The 121 unmatched recalls (9.98% of total, 15.4% of those with an
establishment field) are the actionable gap.

---

## Why DBA fallback adds nothing (Q3 = Q2)

Two non-exclusive explanations both held up under spot-checking:

1. **The recall API never references DBA names** — when a recall mentions an
   establishment, it uses the legal `establishment_name` value, not a
   doing-business-as alias. The DBA field on the establishment side carries
   information for other use cases, but not for this join.
2. **The 67.6% of establishments without any DBA** (per Finding §F in
   `establishment_first_extraction_findings.md`) means the fallback set is
   small to begin with — only 32.4% of establishments contribute any
   alternate name.

The silver staging join can skip DBA fallback at the Step 5 first cut. If a
future recall ever references a DBA-only name, it'll surface as a new
unmatched record and we can re-evaluate.

---

## The 94 unmatched names — three failure modes

Sample of the top 20 (Q5 in the probe SQL, sorted by recalls referenced)
reveals three distinct classes:

### 1. HTML entity encoding — dominant cause (~80% of unmatched)

The recall API returns establishment names with HTML-encoded special
characters: `&#039;` for `'`, `&amp;` for `&`. The establishment listing API
returns them as plain text. A simple `upper(trim(...))` normalization misses
these.

| Unmatched recall name | Plain-text equivalent | Recalls referencing |
|---|---|---|
| `Pilgrim&#039;s Pride Corporation` | `Pilgrim's Pride Corporation` | 8 |
| `Ukrop&#039;s Homestyle Foods` | `Ukrop's Homestyle Foods` | 4 |
| `King&#039;s Command Foods, LLC` | `King's Command Foods, LLC` | 3 |
| `Boar&#039;s Head Provisions Co., Inc.` | `Boar's Head Provisions Co., Inc.` | 2 |
| `F&amp;S Produce West LLC` | `F&S Produce West LLC` | 2 |
| `J&amp;J Distributing` | `J&J Distributing` | 2 |
| `B &amp; R Meat Processing` | `B & R Meat Processing` | 2 |
| ...etc | | |

Estimated impact of fixing this: of the 17 single-name entries in the top-20
sample, 14 (82%) carry HTML entities. Extrapolating to the full 94
unmatched: ~75–80 newly matched, taking the per-distinct-name rate from
82.85% → ~97% and the per-record matched count from 667 → ~720 (~59% of
all recalls, ~91% of recalls-with-an-establishment).

### 2. Name variations / suffix drift (~10% of unmatched)

Same establishment, slightly different name string between systems:

| Recall name | Establishment listing form (probable) |
|---|---|
| `Suzanna&#039;s Kitchen Inc` | `Suzanna's Kitchen, Inc.` (after HTML decode + comma normalization) |
| `Suzanna&#039;s Kitchen` | Same (after HTML decode + suffix tolerance) |

Some of these are fixable by HTML-decode; others need fuzzy matching
(RapidFuzz per ADR 0002 — Phase 6 firm entity resolution). The two Suzanna's
Kitchen variants likely fall to the post-HTML-decode pass.

### 3. Multi-establishment fields (~5% of unmatched)

A small number of recalls pack multiple establishments into the `establishment`
field as a comma-separated list:

> `Ajinomoto Foods North America, Ajinomoto Toyo Frozen Noodle, Inc., Ajinomoto Foods North America`

Three distinct establishments, one recall, one delimited string. Splitting
on `,\s*` would let each component match independently. Edge case — defer
to a follow-up unless it's more common than the sample suggests.

---

## Multi-hit popularity (Q6)

The 11 most-recalled establishments (5+ recalls each) collectively account
for 76 recall events. Notably, two of the top four (Pilgrim's Pride and the
HTML-encoded entries) are in the unmatched set today and would jump to
matched on HTML-decode:

| Establishment | Recall count | Currently matched? |
|---|---|---|
| TYSON FOODS, INC. | 12 | yes |
| CONAGRA BRANDS (CONAGRA FOODS PACKAGED FOODS, LLC) | 9 | yes |
| RUIZ FOOD PRODUCTS, INC. | 8 | yes |
| **PILGRIM&#039;S PRIDE CORPORATION** | 8 | **no — HTML encoding** |
| ADVANCEPIERRE FOODS, INC. | 7 | yes |
| PERDUE FOODS LLC | 6 | yes |
| GOLD CREEK FOODS, LLC | 5 | yes |
| KRAFT HEINZ FOODS COMPANY | 5 | yes |
| HORMEL FOODS CORPORATION | 5 | yes |
| WAYNE FARMS LLC | 5 | yes |
| FRATELLI BERETTA USA, INC. | 5 | yes |

The unmatched-popularity-bias finding strengthens the case for the HTML-decode
fix: 8 of the 121 unmatched per-record results (Q4) are Pilgrim's Pride alone.

---

## Recommendations for Phase 5b.2 Step 5 (silver join shape)

1. **Apply HTML-entity decoding to the recall side before the join.** Either
   in the `stg_usda_fsis_recalls.sql` view (`replace(replace(establishment,
   '&#039;', E'\''), '&amp;', '&')`) or as a project-level macro if the
   pattern recurs across other sources. Estimated lift: 82.85% → ~97%
   per-distinct-name match.
2. **Skip DBA fallback for v1.** Q3 confirms zero additional matches at
   today's data. Document the finding in the silver staging model so a
   future regression (recall API starting to use DBA names) is traceable.
3. **Defer fuzzy matching to Phase 6** firm entity resolution per ADR 0002.
   The remaining ~3% of unmatched names after HTML-decode are name-variation
   drift; not blocking for v1.
4. **Defer multi-establishment field splitting.** Edge case; one observed
   instance in the top-20 sample. Re-evaluate if the Phase 6 fuzzy pass
   surfaces more.

The silver join produces an `establishment_id` FK on USDA recall events with
projected post-fix coverage of:

- ~720 / 1,213 recalls (~59% of total)
- ~720 / 788 recalls-with-an-establishment (~91%)

The remaining ~6% of recalls-with-an-establishment unmatched even after
HTML-decode fall into the fuzzy-match / multi-establishment-split residual.

---

## Step 5 empirical residual (2026-05-02)

After Step 5 shipped (HTML-entity decode in `stg_usda_fsis_recalls.sql` + silver join in `firm.sql`), the per-distinct-name match rate measured against dev:

| Metric | Pre-Step-5 prediction | Pre-Step-5 actual | Post-Step-5 actual |
|---|---|---|---|
| Per-distinct-name match rate | ~97% (lift forecast) | 82.85% (454/548) | **99.27% (543/547)** |
| Unmatched distinct names | ~17 forecast | 94 | **4** |

The 99.27% beat the ~97% projection — the HTML-decode lift was even cleaner than the heuristic forecast suggested, because nearly all single-name unmatches turned out to carry HTML entities (the projection only sampled the top-20).

The 547 vs 548 distinct-recall-name discrepancy versus the pre-decode count is a result of HTML-decode collapsing one pair (e.g., a recall using `Pilgrim&#039;s Pride Corporation` and another using the plain-text `Pilgrim's Pride Corporation`) into a single normalized name.

### The 4 residual unmatched names — all Finding 3 (multi-establishment fields)

Source query: `scripts/sql/usda_establishments/silver/list_unmatched_recall_names.sql`.

| # | `establishment` value (verbatim) | Distinct firms encoded |
|---|---|---|
| 1 | `Ajinomoto Foods North America, Ajinomoto Toyo Frozen Noodle, Inc., Ajinomoto Foods North America` | 2 |
| 2 | `FreshRealm, FreshRealm, FreshRealm` | 1 |
| 3 | `Freshrealm, FreshRealm` | 1 |
| 4 | `Freshrealm, FreshRealm, California Ranch Food Company` | 2 |

**All 4 unmatched residuals are the multi-establishment-field shape from Finding 3 above.** The other failure modes the pre-Step-5 audit anticipated (HTML entities, name-variation drift) were resolved by the HTML-decode and contributed zero residual. Finding 3 — speculated as ~5% of unmatched in the projection — turned out to be 100% of the post-decode residual.

The individual firms encoded in these 4 strings (Ajinomoto Foods North America, Ajinomoto Toyo Frozen Noodle, Inc., FreshRealm, California Ranch Food Company) are all almost certainly present in `firm_establishment_attributes` — a per-firm match rate after splitting these strings would land at essentially 100%. Only the comma-concatenated string can't be matched as a unit.

### Phase 6 hand-off

When firm entity resolution is built (Phase 6 per ADR 0002), the multi-establishment shape needs a split-and-match pass on the recall side. **Mind the embedded comma in entity suffixes** — `Ajinomoto Toyo Frozen Noodle, Inc.` contains a comma inside the firm name, so naive `string_split(establishment, ', ')` produces wrong fragments. Suggested approaches:

- Regex split on `,\s*(?!Inc|Co|LLC|Corp|Ltd|LP)\b` — split only on commas not followed by corporate suffixes.
- Or: tolerate the false-split risk and rely on fuzzy matching to recover (`Ajinomoto Toyo Frozen Noodle` and `Inc.` would each fuzzy-match, with `Inc.` getting low scores everywhere and dropping out).
- Or: build a `recall_establishment_link` bridge table at silver where each (recall, establishment) pair is one row, populated by parsing the multi-value field. More invasive but produces clean joins.

The 4 records currently produce `firm` rows with comma-concatenated `canonical_name` values. Those rows are findable in `firm` but won't naturally match cross-source (CPSC/FDA wouldn't write a multi-firm name). Phase 6 firm resolution can either re-collapse these or accept them as v1 noise.

---

## Open items

- [x] Confirm recall→establishment match rate (Step 3 deliverable) — done.
- [x] Apply HTML-decode in `stg_usda_fsis_recalls.sql` (Step 5) — done 2026-05-02.
- [x] Implement the silver join in `firm.sql` to populate `observed_company_ids` with `establishment_number` for matched USDA rows (Step 5) — done 2026-05-02.
- [x] Confirm the ADR 0027 refactor didn't change the join numbers — confirmed via dev rebaseline 2026-05-02; bronze representation differences for `establishment` were absent (text field, no `""` sentinel issues).
- [ ] **Phase 6 follow-up:** handle multi-establishment-field splits to recover the 4 residual unmatched recalls (see "Phase 6 hand-off" above).
