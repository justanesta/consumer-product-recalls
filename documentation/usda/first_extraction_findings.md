# USDA FSIS First Extraction Findings

**Extraction window:** all-time (USDA returns the full ~2,002-record archive on every call; no incremental filter exists per Finding D)
**Total rows in bronze:** 2,003 (2,002 unique identities + 1 history row from a captured edit)
**Unique recall numbers:** 1,213
**Bilingual pairs:** 789
**English-only:** 424
**Spanish-only orphans:** 0
**Extraction date (final verification run):** 2026-05-01

---

## A. Data Model: Composite Identity, Bilingual Pairs, No Nesting

USDA's bronze row is structurally between FDA's and CPSC's. Like CPSC, every row corresponds to a single recall record (no header/line split). Unlike either, USDA has **bilingual sibling rows**: every recall published in both English and Spanish appears as two rows sharing `field_recall_number` (`source_recall_id`) but distinguished by `langcode`. The natural identity is the composite `(source_recall_id, langcode)`; bronze dedup keys on that tuple (per the BronzeLoader composite-identity refactor in Phase 5b — see "L. Phase 5b detours" below).

Per the bilingual completeness probe:

| Group | Recall numbers |
|---|---|
| Bilingual (EN + ES) | 789 |
| English-only | 424 |
| Spanish-only orphans | 0 |
| **Total unique recall numbers** | **1,213** |

Zero Spanish-only orphans confirms the bilingual pairing invariant (`check_usda_bilingual_pairing` in `src/bronze/invariants.py`) is doing nothing in practice — the upstream rarely or never publishes a Spanish record without its English counterpart. The invariant remains as a safety net.

**Silver implication:** silver "current state" must key on `(source_recall_id, langcode)` and resolve bilingual pairs explicitly. ADR 0026 captures the lifecycle-dimension story; see section K below.

---

## B. Cadence

USDA is **ultra-low volume** — substantially lower than CPSC (~5–6/day) or FDA (~40/day):

| Metric | USDA | CPSC | FDA |
|---|---|---|---|
| Records per active day | ~1 | ~5–6 | ~40 |
| Records per week (recent) | 1–3 | 7–59 | 50–325 |
| Active days per week (recent) | 1–2 | 5 | 5–6 |

Recent weekly cadence (last 16 weeks):

| week_start | records | active_days |
|---|---|---|
| 2026-04-27 | 1 | 1 |
| 2026-04-06 | 3 | 2 |
| 2026-03-23 | 2 | 2 |
| 2026-03-09 | 1 | 1 |
| 2026-03-02 | 1 | 1 |
| 2026-02-23 | 1 | 1 |
| 2026-02-16 | 3 | 2 |
| 2026-02-09 | 1 | 1 |
| 2026-02-02 | 1 | 1 |
| 2026-01-12 | 1 | 1 |

Most weekdays have **zero publications**. The dataset is overwhelmingly historical — 91.4% of records have `archive_recall=true`.

---

## C. Publication Frequency vs. Weekday Gaps

The CPSC/FDA "weekday gaps = federal holidays" pattern does not translate to USDA. Because USDA publishes on average ~1 day per week, the gap-analysis query returns 115 weekdays without publications in the last 6 months — the gaps are dominated by *no publishable events*, not infrastructure-level pauses. Federal holidays are buried in the noise.

**Operational note:** the incremental extractor is full-dump every run (Finding D), so there is no watermark drift to worry about during long publication-free stretches. Idempotency comes from bronze content-hash dedup, not from advancing watermarks.

---

## D. Volume Spikes — Historical Only

The five highest single-day volumes are all from 2015–2019:

| day | records |
|---|---|
| 2018-10-19 | 12 |
| 2017-06-09 | 12 |
| 2015-01-17 | 10 |
| 2018-10-17 | 8 |
| 2019-10-18 | 8 |

Modern (2020–present) USDA shows no multi-record days above ~3. The historical bursts likely reflect FSIS's earlier publication cadence; current cadence is one recall at a time.

**Implication for the count guard:** `_MAX_INCREMENTAL_RECORDS = 5_000` (in `UsdaExtractor`) catches a watermark/API-shape failure but won't fire on any plausible organic spike — the highest day in the entire dataset is 12 records.

---

## E. Edit Detection (Composite Content Hash Dedup)

**1 edit captured** at the time of the verification snapshot. The composite identity ensures bilingual sibling rows are never falsely treated as edits of each other (the original "Phase 5b detour" bug — see section L).

```
 source_recall_id | langcode | hash_versions | total_rows
------------------+----------+---------------+------------
 PHA-04092026-01  | English  |             2 |          2
```

Reference reconstruction of the captured edit:

```
 source_recall_id | langcode | recall_date |    lmd     | content_hash_prefix |     extraction_timestamp
------------------+----------+-------------+------------+---------------------+-------------------------------
 PHA-04092026-01  | English  | 2026-04-09  | 2026-04-09 | 5bd4f33649c06cf8    | 2026-05-01 01:51:37.562208+00
 PHA-04092026-01  | English  | 2026-04-09  | 2026-04-09 | 27dcc93491f1f019    | 2026-05-01 01:47:19.502479+00
```

Both rows have the same `recall_date` and `last_modified_date`, but distinct `content_hash` values 4 minutes apart — FSIS made a content-level edit (we suspect to `active_notice`, given the `active_notice IS TRUE` count moved from 2 → 3 across that window) without advancing `last_modified_date`. This is exactly the failure mode the deep-rescan workflow exists to catch (cf. ADR 0023's argument for FDA): client-side hash dedup is the only signal, since the upstream watermark is unreliable.

**Bronze growth from edits:** total rows (2,003) exceed unique identities (2,002) by 1, attributable entirely to this one captured edit.

---

## F. Bilingual Pair Behavior — Finding F revision

The `recall_api_observations.md` Finding F claimed FSIS updates bilingual pairs **atomically** (same `last_modified_date` across EN and ES). At scale this is **false**:

```
 bilingual_pairs_checked | aligned_or_both_null | mismatched
-------------------------+----------------------+------------
                     789 |                  684 |        105
```

**~13.3% of bilingual pairs have mismatched `last_modified_date`** between EN and ES siblings. FSIS sometimes updates one language and not the other.

A separate cross-check found one minor anomaly:

```
 claims_es_has_es | claims_es_missing_es | claims_no_es_but_has_es | claims_no_es_correct
------------------+----------------------+-------------------------+----------------------
              789 |                    1 |                       0 |                  423
```

One English record has `field_has_spanish=true` but no Spanish row exists in bronze. Possibilities: the Spanish counterpart was never published, was retracted before our extraction, or `field_has_spanish` is stale. Worth investigating during silver development; not a blocker for bronze.

**Silver implications:**

- Bilingual pair resolution can't assume EN and ES siblings are at the same point in time. The silver "current state" projection should pick the latest version per `(source_recall_id, langcode)` independently, not assume the pair moves together.
- `field_has_spanish` is a hint, not a guarantee. Don't make it a referential constraint in silver.
- The 13.3% mismatch rate is a strong reason to track per-language presence in the lifecycle manifest (ADR 0026) rather than per-pair.

---

## G. Active / Archived / `active_notice` Cross-tab

```
 archive_recall | active_notice |   n
----------------+---------------+------
 f              | f             |  171
 f              | t             |    3
 t              | f             | 1640
 t              | (NULL)        |  189
```

Two clean correlations:

1. **`active_notice IS NULL` is exclusive to archived records.** All 189 nulls have `archive_recall=true`. Active records always have `active_notice` populated. This explains the schema bug we hit on first extraction (originally `active_notice` was declared required; ~9.4% of records — exactly the archived ones with null values — were rejected). The post-fix schema makes the field `Optional[bool]`, and the empirical correlation suggests silver can safely treat `NULL → False` for archived records if a non-null projection is needed.
2. **Only 3 active records have `active_notice=true`** (out of 174 active total). The flag is a much narrower "currently being prominently promoted" signal, not a synonym for `NOT archive_recall`. Silver consumers wanting a "live recalls" filter should use `archive_recall=false`, not `active_notice=true`.

`last_modified_date` population aligns with archived status (Finding J at scale):

```
 archive_recall | lmd_populated | lmd_null | total
----------------+---------------+----------+-------
 f              |           172 |        2 |   174
 t              |           986 |      843 |  1829
```

98.9% of active records have a `last_modified_date`; only 53.9% of archived do. The 845 records with empty `last_modified_date` from the Bruno-era exploration (Finding C) are accounted for: 843 are archived (legacy data predating consistent date population), 2 are active (recent records FSIS hasn't yet stamped). The field is reliable for active records; archived records need archived-aware silver handling.

---

## H. Category Distributions

### `recall_type`
```
     recall_type     |  n
---------------------+------
 Closed Recall       | 1837 (91.7%)
 Public Health Alert |  162 (8.1%)
 Active Recall       |    4 (0.2%)
```

Only 4 records are flagged as `Active Recall` (vs. 174 with `archive_recall=false`). The two fields measure different things: `archive_recall` is the publication-state flag; `recall_type` is a workflow-state flag. `Public Health Alert` is its own first-class type, not a sub-type of recall.

### `recall_classification`
```
 recall_classification |  n
-----------------------+------
 Class I               | 1432 (71.5%)
 Class II              |  326 (16.3%)
 Public Health Alert   |  162 (8.1%)
 Class III             |   83 (4.1%)
```

Class I (highest severity — health hazard) dominates. Public Health Alerts appear as their own classification, parallel to Class I/II/III. This duplicates `recall_type` for PHAs.

### `recall_reason` (top reasons)
```
 recall_reason                                            |  n
---------------------------------------------------------+-----
 Product Contamination                                   | 748 (37.4%)
 Misbranding, Unreported Allergens                       | 549 (27.4%)
 Produced Without Benefit of Inspection                  | 234 (11.7%)
 Misbranding                                             | 121 (6.0%)
 Import Violation                                        | 119 (5.9%)
 Processing Defect                                       |  64 (3.2%)
 (empty)                                                 |  30 (1.5%)
```

**Multi-reason values are stored as comma-separated strings**, not arrays. `Misbranding, Unreported Allergens` is a single value, not two. Silver normalization will need to split this to a one-row-per-reason table for accurate filtering.

### `processing`
Top values: `Fully Cooked - Not Shelf Stable` (38.1%), `Raw - Non Intact` (12.6%), `Heat Treated - Not Fully Cooked - Not Shelf Stable` (11.0%), `Products with Secondary Inhibitors - Not Shelf Stable` (9.2%), `Raw - Intact` (8.6%). Same comma-separated multi-value pattern as `recall_reason`.

### `risk_level` — fully redundant with `recall_classification`
```
      risk_level      |  n
----------------------+------
 High - Class I       | 1432
 Low - Class II       |  326
 Public Health Alert  |  162
 Marginal - Class III |   83
```

Identical row counts to `recall_classification`. `risk_level` adds a verbal severity prefix (`High - `, `Low - `, `Marginal - `) but otherwise carries no new information. **Silver should drop one of the two columns** or treat one as a derived field.

---

## I. Null Field Rates (validates Finding C empirically at scale)

| field | null % | notes |
|---|---|---|
| `title` / `recall_date` / `recall_type` / `recall_classification` | 0.00% | Required (post-Phase-5b schema). Always populated. |
| `recall_url` | 0.00% | Always populated despite being undocumented (Finding H). |
| `media_contact` | 0.00% | Always populated. |
| `risk_level` | 0.00% | Always populated. |
| `summary` | 0.05% | Effectively always populated. |
| `recall_reason` / `processing` | 1.50% | Rarely missing. |
| `closed_year` | 8.94% | Approximately matches `closed_date` (8.49%). |
| `closed_date` | 8.49% | |
| `qty_recovered` | 8.69% | Free text — see "Free-text fields" below. |
| `active_notice` | 9.44% | All nulls are archived records (section G). |
| `labels` | 13.58% | |
| `related_to_outbreak` | 25.01% | |
| `states` | 28.76% | |
| `establishment` | 34.40% | Lower than expected; ~1 in 3 records lack an establishment name. |
| `last_modified_date` | 42.19% | Concentrated on archived records (section G). |
| `product_items` | 42.49% | **Surprise — Finding C did not flag this.** Finding C addendum candidate. |
| `company_media_contact` | 48.88% | |
| `distro_list` | 82.53% | Sparsely populated. |
| `press_release` | 99.95% | Effectively always empty. |
| `en_press_release` | 100.00% | **Always empty — dead field** (excluded from content hash). |

The two surprises versus the original Bruno-era Finding C audit:

- **`product_items` 42.49% null** — Finding C did not probe this field (it was not in the cardinality probe's empty-rate sample). 42% null is a meaningful gap; silver should treat `product_items` as `Optional[str]` and not assume it's always present.
- **`active_notice` 9.44% null** — already absorbed into the schema fix; documented in detail in the Finding C addendum in `recall_api_observations.md`.

Everything else aligns with Finding C within rounding.

---

## J. Free-Text Fields — Silver Cleaning Preview

### `establishment` — HTML-encoded entities

Top establishments by recall count:

| establishment | recall_rows |
|---|---|
| Tyson Foods, Inc. | 24 |
| Pilgrim&#039;s Pride Corporation | 15 |
| Conagra Brands (Conagra Foods Packaged Foods, LLC) | 15 |
| Ruiz Food Products, Inc. | 13 |
| AdvancePierre Foods, Inc. | 12 |
| Gold Creek Foods, LLC | 10 |
| Perdue Foods LLC | 10 |
| WILLOW TREE POULTRY FARM, INC. | 7 |
| Ukrop&#039;s Homestyle Foods | 7 |
| ... | ... |

**FSIS HTML-encodes apostrophes in free-text fields** (`Pilgrim&#039;s` instead of `Pilgrim's`). Silver must HTML-decode these for accurate firm matching in Phase 6's establishment resolution. Likely affects `establishment`, `summary`, `product_items`, `media_contact`, and any other free-text field.

### `qty_recovered` — free text, no enforced format

Top values:

| qty_recovered | occurrences |
|---|---|
| `0 pounds` | 235 |
| `0 pounds ` (trailing space) | 17 |
| `0 lbs` | 12 |
| `390,584 pounds` | 8 |
| `0 (zero) pounds` | 7 |
| `109 pounds` | 6 |
| `9 pounds` | 6 |
| `500 pounds` | 6 |
| `120 lbs` | 5 |

Silver normalization will need to handle:

- **Whitespace variation** — `0 pounds` and `0 pounds ` (with trailing space) are stored as distinct values. A `TRIM()` pass at silver build time collapses these.
- **Unit variation** — `pounds` vs `lbs` vs `(zero) pounds`.
- **Numeric extraction** — pull the digits, normalize to a canonical unit.

Same shape as FDA's `product_distributed_quantity` parsing problem.

---

## K. Pipeline Performance

| run | mode | fetched | inserted | duration |
|---|---|---|---|---|
| 2026-05-01 00:51 | full extract (initial) | 2002 | 2002 | 3.0 s |
| 2026-05-01 01:35 | full extract (post FSIS retraction) | 2001 | 790¹ | 4.3 s |
| 2026-05-01 01:47:18 | full extract (post-fix, fresh DB) | 2002 | 2002 | 3.0 s |
| 2026-05-01 01:47:27 | full extract (idempotency check) | 2002 | 0 | 1.5 s |
| 2026-05-01 01:51 | deep-rescan | 2001 | 1² | 1.7 s |

¹ Spurious — caused by the bilingual-dedup bug, fixed before the next runs.
² Genuine FSIS edit (`PHA-04092026-01`).

The 304 short-circuit run earlier in the day (01:15) returned `count=0` in 0.5s — empirical evidence that the ETag conditional-GET works against our request shape, but it stays disabled in production until multi-day probes confirm consistency (Finding N addendum).

USDA is the fastest source to extract, by an order of magnitude — 1.5–3 seconds per full run vs. CPSC (~1s per incremental but small) and FDA (~2s for 2,500 records). Single GET, no pagination, ~1.6 MB compressed.

---

## L. Phase 5b Detours — Lessons from the First Extraction

Three issues surfaced during first-extraction verification, each captured as a code/schema/architecture artifact:

### L1. `active_notice` schema bug — Finding C blind spot

The original Pydantic schema declared `active_notice` as required `bool`. The Bruno-era Finding C empty-rate audit (`recall_api_observations.md`) didn't include this field, so the gap wasn't caught at design time. First extraction rejected 189 records (~9.4%) on `field_active_notice == ""`. Diagnostic + fix:

1. Queried `usda_fsis_recalls_rejected` grouped by `failure_reason` — single failure mode confirmed.
2. Made `active_notice` `Optional[bool]` in `src/schemas/usda.py`.
3. Made the `active_notice` column `nullable=true` in the migration (edited 0005 in place since unmerged).
4. Added Finding C addendum row to `recall_api_observations.md`.

Re-extraction: 0 rejections.

**Lesson:** Bruno-era empty-rate audits should enumerate every field, not just the obvious-suspect ones. For future sources, run an exhaustive field-nullability probe against the cardinality response before writing the Pydantic schema.

### L2. Bilingual-dedup bug in `BronzeLoader`

After the schema fix, an idempotent re-run reported `loaded=790` instead of the expected `loaded=0`. Cause: `BronzeLoader._fetch_existing_hashes` queried by `source_recall_id` alone. USDA's bilingual siblings share `source_recall_id` (English and Spanish rows of the same recall), so the dedup query returned both, the `dict` comprehension collapsed them non-deterministically, and ~789 of 1,578 bilingual records appeared "changed" on every re-run.

The migration's index comment had documented this concern but mis-routed the fix to "Phase 6 silver model resolves bilingual pairs explicitly." That punt was wrong — bronze idempotency is a bronze-layer correctness property and has to be fixed here.

Resolution: extended `BronzeLoader` to accept `identity_fields: tuple[str, ...] = ("source_recall_id",)`. CPSC and FDA inherit the existing default; USDA passes `("source_recall_id", "langcode")`. The query now keys on the composite tuple, the dedup dict keys on tuples, and bilingual siblings dedup independently. After the fix: idempotent re-runs report `loaded=0` exactly.

**Lesson:** "Natural identity is multi-column" is a real source property. Bronze loaders should be designed to support composite identity from day one even if the first source doesn't need it. The default behavior can stay single-column.

### L3. ETag kill-switch wiring — YAML config not loaded

After Finding N concluded ETag was unreliable and we wrote `etag_enabled: false` into `config/sources/usda.yaml`, the next extraction *still* sent `If-None-Match` and short-circuited on 304. Cause: **the YAML file is not loaded by any code path**. ADR 0012's declarative-source-config pattern (YAML loader + Pydantic-discriminated-union dispatch + registry) was a Phase 1 *deliverable artifact* but the *consuming code* was never implemented; CLI dispatch in `src/cli/main.py` instantiates extractors with hardcoded constructor kwargs.

Resolution for Phase 5b: changed `UsdaExtractor.etag_enabled` class default to `False` (the live kill-switch); annotated `config/sources/usda.yaml` with a header comment documenting the YAML-not-loaded state.

The architectural debt — ADR 0012's loader/registry — is filed as a follow-up. Affects all five sources equally; merits its own branch and ADR.

**Lesson:** Phase-1 deliverable artifacts are easy to misinterpret as wired functionality. When in doubt, grep for callers of the artifact before relying on it as a runtime knob.

### L4. Akamai bot-manager fingerprinting (Finding O)

Initial extraction hung indefinitely. Diagnostic curls revealed Akamai was slowloris-throttling Python's default httpx User-Agent. Fix: vendor a Firefox/Linux User-Agent + matching `Accept` / `Accept-Language` / `Accept-Encoding` headers, refreshed weekly via `.github/workflows/refresh-user-agents.yml` from Mozilla product-details and Chromium Dash. Documented in full as Finding O in `recall_api_observations.md`.

**Lesson:** Bot-manager fingerprinting is a real production concern even for public-data government APIs. CDN/WAF posture should be treated as part of the request shape, not as infrastructure noise.

---

## M. Silver Layer Implications

1. **Composite identity throughout.** Silver "current state" tables key on `(source_recall_id, langcode)`. Bilingual EN/ES siblings are sibling rows in silver as well as bronze.
2. **Latest-version projection.** Bronze may have multiple rows per `(source_recall_id, langcode)` from edits. Silver uses `MAX(extraction_timestamp)` per identity. Picks per-identity, not per-pair (Finding F revision: pairs do not move atomically).
3. **Lifecycle dimensions per ADR 0026.** Silver should expose `first_seen_at`, `last_seen_at`, `is_currently_active`, `edit_count`, `was_ever_retracted` — all derivable from a per-run identity manifest. ADR 0026 is in Draft pending scope and implementation decisions.
4. **HTML-decode pass on free-text fields.** `establishment`, `summary`, `product_items`, `media_contact`, `company_media_contact` all contain HTML entities (`&#039;`, others likely). One Python `html.unescape()` step in the silver transform handles all of them.
5. **Multi-value field splitting.** `recall_reason` and `processing` are stored as comma-separated strings in bronze. Silver should split to one-row-per-value tables for filterable analytics. Both fields use `, ` (comma + space) as the separator empirically; sample to confirm before committing.
6. **`risk_level` is redundant with `recall_classification`.** Pick one for the silver public surface. Probably `recall_classification` since it's the documented field; demote `risk_level` to a derived label if needed.
7. **`recall_type` is parallel to `archive_recall` and `active_notice`, not a sub-classification.** All three encode different facets of the recall lifecycle; silver should expose all three as separate dimensions, not collapse them.
8. **`active_notice` = "currently prominently promoted," not "active recall."** Silver consumers asking "is this a live recall?" should filter on `archive_recall = false`. `active_notice = true` filters to a much narrower 3-record set.
9. **`product_items` is `Optional[str]`** despite the original schema sketch implying it's required. Silver projections can't assume it's populated.
10. **`qty_recovered` cleaning** — TRIM whitespace, normalize units (`pounds`/`lbs`/`(zero) pounds` → canonical), extract numeric. Same problem class as FDA's `product_distributed_quantity`.
11. **Deep-rescan necessity.** USDA has no working server-side watermark filter (Finding D), so every incremental run is functionally a full snapshot. The deep-rescan workflow exists to populate the lifecycle manifest under a "force full pull, never short-circuit" posture (ADR 0023's argument generalized) and to provide a weekly safety net against any silent ETag-cache misbehavior if the optimization is ever re-enabled.
12. **The one orphan `field_has_spanish=true`-without-ES-row is a data-quality flag, not a referential constraint.** Silver should not error on it; surface it as a quality metric.

---

## N. SQL Reference

All queries used to produce this analysis are in `scripts/sql/explore_usda_bronze.sql` (data exploration) and `scripts/sql/verify_usda_first_extraction.sql` (post-load verification). Both are re-runnable read-only and produce headed, human-readable output via `psql -f`.

```bash
# After any USDA extraction, for verification:
psql "$NEON_DATABASE_URL" -f scripts/sql/verify_usda_first_extraction.sql

# To re-derive the empirical numbers in this document:
psql "$NEON_DATABASE_URL" -f scripts/sql/explore_usda_bronze.sql
```

The 21 queries in `explore_usda_bronze.sql` are organized into seven sections:

1. **Cadence & volume** — daily/weekly counts, top spike days, weekday-gap analysis.
2. **Edit detection (composite identity)** — multi-hash identities, total vs. unique counts, single-record edit-history reconstruction.
3. **Bilingual pair model** — pair completeness, `field_has_spanish` cross-check, last-modified-date alignment.
4. **Category distributions** — `recall_type`, `recall_classification`, `recall_reason`, `processing`, `risk_level`.
5. **Active / archived / `active_notice` cross-tabs** — explains the 3/1811/189 split, validates Finding J's archived-records correlation.
6. **Undocumented field validation** — `recall_url` prefix split by langcode (Finding H).
7. **Null/empty rates + free-text samples** — full field audit, top establishments, `qty_recovered` value distribution.

Section 1 of the verification script provides:

- Bronze cardinality
- Langcode breakdown
- Archive breakdown
- Active-Spanish breakdown
- `active_notice` nullability check
- Rejected table counts
- Watermark state (with `\x on` for readability)
- Recent extraction-run history
- A sample bronze row by most recent `recall_date`
