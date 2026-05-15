# NHTSA Incremental Delta Findings

**Source run:** `8fb4e268-0769-4ce7-8913-1557c288aae1` (2026-05-08)
**Command:** `recalls extract nhtsa --since=2023-12-01` (re-run of identical command from 2026-05-07)
**Fetched:** 72,567 rows
**Landed in bronze:** 194 rows
**Rejected by content-hash dedup:** 72,373 rows (99.7%)
**Methodology:** `scripts/sql/nhtsa/bronze/explore_incremental_delta.sql` (run with no args defaults to most recent successful nhtsa run)

These findings describe what NHTSA's POST_2010 incremental dump looks
like *between successive runs* — i.e., what actually mutates day-to-day
once `ADR 0007` content-hash dedup is doing its job. Complement to
`flat_file_observations.md`, which describes the static shape of a
single dump.

---

## A. Dedup architecture is functioning as designed

99.7% rejection rate on the second run validates the bronze ingestion
contract end-to-end:

- **`ADR 0007` content-hash dedup** is short-circuiting idle rows. Of
  72,567 fetched, only 194 had either a new 11-tuple identity or a
  changed content_hash relative to the prior run's bronze state.
- **`ADR 0030` (amended) 11-tuple identity + `hash_exclude_fields={source_recall_id}`**
  is necessary for this to work — see Finding C for the empirical
  proof that `source_recall_id` churns on every run.

**Operational implication:** daily incremental runs on Neon free tier
are sustainable. Even at the high end, expect a few hundred net inserts
per day after the initial seed; bandwidth tax is the ~14 MB ZIP
download, not bronze writes.

---

## B. Net-new vs. amendment split

Of the 194 landed rows:

| kind | count | share |
|---|---|---|
| net_new (no prior 11-tuple in bronze) | 133 | 69% |
| amendment (existing 11-tuple, content_hash differs) | 61 | 31% |

NHTSA actively amends existing recall records. Amendment traffic is
**not negligible** — roughly 1 in 3 daily-delta rows is a re-edit of a
previously-loaded record. Silver/gold layers must account for this.

---

## C. RECORD_ID (`source_recall_id`) is a per-build sequence number

**100%** of amendments (61/61) had a different `source_recall_id`
than the prior bronze row sharing the same 11-tuple. This empirically
confirms what `RCL.txt` line 30 says about RECORD_ID being a "Running
Sequence Number" reassigned at file-generation time.

Direct consequence: if `source_recall_id` were *not* in
`hash_exclude_fields`, every fetched row would land as an "amendment"
every run, and bronze would grow by ~72k rows/day without any actual
data change. The exclusion is load-bearing.

**Silver implication:** never use `source_recall_id` as a stable
identifier. The natural alternative is the 11-tuple identity with
`extraction_timestamp` to disambiguate versions:

```sql
-- Latest version of recall component X
select distinct on (
    campno, maketxt, modeltxt, yeartxt, compname, rcl_cmpt_id,
    mfr_comp_ptno, mfr_comp_desc, mfr_comp_name, endman, bgman
) *
from nhtsa_recalls_bronze
order by
    campno, maketxt, modeltxt, yeartxt, compname, rcl_cmpt_id,
    mfr_comp_ptno, mfr_comp_desc, mfr_comp_name, endman, bgman,
    extraction_timestamp desc;
```

**However, the 11-tuple grain has a documented drift hazard** —
any of the identity fields can be edited by NHTSA across runs (the
2026-05-08 baseline observed `endman` and `maketxt` drift, but the
mechanism is general), fragmenting silver into multiple rows for a
single physical part. See Section G for the mechanism, the empirical
baseline, and the silver-design resolution. The decision landed in
2026-05-08 — **silver `recall_product_id` is `md5(11-tuple)`**, with
fragmentation acknowledged as a v1 limitation and Phase 6 product-level
reconciliation deferred to ADR 0031's tier framework.

---

## D. Which fields drive amendments

Across the 61 amendments, change rates per field:

| field | amendments where field changed | rate | interpretation |
|---|---|---|---|
| `source_recall_id` | 61 | 100% | Hash-excluded; running sequence (Finding C) |
| `odate` | 40 | 66% | Owner-notification date; flips when mailings actually go out |
| `corrective_action` | 39 | 64% | Tense / date rewrites as recall lifecycle progresses |
| `conequence_defect` | 21 | 34% | Consequence text refined |
| `potaff` | 21 | 34% | "Population affected" estimate narrows as VIN ranges firm up |
| `mfgcampno` | 2 | 3% | Manufacturer campaign number — stable, as expected |
| All 11 identity fields | 0 | 0% | 11-tuple identity is empirically stable across this run |
| `mfgname`, `compname` (also identity), `rcltype`, `desc_defect`, `notes`, `do_not_drive`, `park_outside`, `mfgtxt`, `rcdate`, `datea`, `rpno`, `fmvss` | 0 | 0% | No changes observed in this delta — does not prove they're immutable |

Q4 sample inspection confirmed `corrective_action` amendments are
**real semantic edits**, not whitespace/casing churn. Example pattern:

> *"Owner notification letters are expected to be mailed by May 11, 2026"* → *"Owner notification letters were mailed May 4, 2026"*

No upstream canonicalization is needed.

---

## E. Amendment backdating window — implications for `--since`

Despite a `--since=2023-12-01` floor, the rows that landed had RCDATE
between **2025-11-06 and 2026-05-01** (≈6 months back from extraction
date). NHTSA can amend records whose RCDATE is months in the past:
the amendment lifecycle (consequence text refinement, owner-notification
dates flipping past-tense) extends well after the recall was first
issued.

**Operational implication:** `--since` must be set conservatively
enough to capture late amendments. A naive floor like
`--since=<extraction_date - 30 days>` would silently drop ~half of the
amendment traffic. Stable floors (`2023-12-01` or `2024-01-01`) are
correct for free-tier dev workflows; production uses `NhtsaDeepRescanLoader`
which has no `--since` filter at all.

---

## F. Bursty distribution

In this 194-row delta, two concentrations dominated:

| dimension | top value | share |
|---|---|---|
| `mfgname` | Mercedes-Benz USA, LLC | 130 / 194 = 67% |
| `compname` | `ELECTRICAL SYSTEM: INSTRUMENT CLUSTER/PANEL` | 122 / 194 = 63% |

Almost certainly a single Mercedes-Benz instrument-cluster campaign
covering ~120 model/year combinations. NHTSA daily deltas are *bursty*,
not uniformly distributed across manufacturers.

**Downstream implication:** rolling-window analytics (e.g., "recalls
this week by manufacturer") will be heavy-tailed. A single big campaign
can dominate a day's metrics. Plan dashboard logic and anomaly
thresholds accordingly.

---

## G. Structural multiplicities in the 11-tuple identity

The post-load assertion `scripts/sql/nhtsa/bronze/assert_eleven_tuple_identity_stable.sql`
was originally written to flag pairs of bronze rows that match on 10 of
11 identity fields (a probable NHTSA-side identity edit). On its first
run against a 72k-row bronze, it surfaced **5,176 such "drift groups"**
— far more than expected. Eyeballing the samples revealed that none of
them are identity drift in the bad sense; they reveal that NHTSA's data
has natural **one-to-many structure within a single run**.

Per-field breakdown (2026-05-08, before adding the cross-run filter to the assertion):

| dropped field | drift groups | what the multiplicity captures |
|---|---|---|
| `mfr_comp_ptno` | 4,359 (84%) | One recall component → many part numbers (original + remedy parts; multiple parts under one component, e.g. HALDEX inversion valve has 5 distinct part numbers under one rcl_cmpt_id; Kia airbag lists original `80410-K0000` and remedy `80410-Q5000`) |
| `rcl_cmpt_id` | 745 (14%) | One part → many lot/instance IDs (Fortune Tormenta tire size LT235/75R15 has 26 distinct rcl_cmpt_ids sharing identical part number, desc, name, and manufacturing dates) |
| `mfr_comp_name` | 30 | One rcl_cmpt_id → multiple component name variants (Shepard steering gears: 7 model names — HD94P, HD94S, M100P, … — under one component ID) |
| `mfr_comp_desc` | 26 | One rcl_cmpt_id → multiple component description variants |
| `endman`, `bgman` | 9 / 7 | One part number → multiple manufacturing date ranges (production batches) |
| `maketxt`, `modeltxt`, `yeartxt`, `compname` | 0 | Empirically stable — none of these fields multiply within a 10-tuple-fixed group |

### The hierarchy NHTSA bronze identity captures

```
campno (recall campaign)
└── rcl_cmpt_id (recall component)
    └── mfr_comp_name + mfr_comp_desc (component model variant)
        └── mfr_comp_ptno (manufacturer part number)
            └── bgman + endman (manufacturing date range / production batch)
```

Each level is one-to-many to the next. The 11-tuple identity is the
**leaf** of this hierarchy — that's why it's row-unique. The "drift
groups" the naive assertion found are simply higher-level groupings
folded across the lower levels.

### Silver implication

When modeling NHTSA recalls in silver, this hierarchy can be normalized
into separate tables (`recall_campaign`, `recall_component`,
`component_part`, `component_part_batch`, etc.) with foreign-key
relationships mirroring the natural multiplicities. Alternatively, kept
denormalized at the leaf grain. Either is valid; the structure
determines join cardinality and dedup logic for cross-source firm /
product entity resolution.

**Naive 11-tuple silver grain has a known drift hazard** — see the
2026-05-08 baseline below. When NHTSA edits an identity field (e.g.,
`endman`) for an existing recall, the corrected row lands as a "net-new"
11-tuple rather than as an amendment, so `distinct on (11-tuple)`
silver lookups return both versions as if they were two distinct
physical things. Three Phase 6 design responses to consider:

- **(a) Coarser silver grain** — roll up to `(campno, maketxt, modeltxt, yeartxt)` or `campno` and aggregate batch-level fields away. Simplest; matches likely dashboard / search needs; loses batch-level granularity.
- **(b) 11-tuple silver + reconciliation pass** — dbt model that detects 10-of-11 matches across `extraction_timestamp` and merges them with a "latest identity wins" policy. Preserves leaf grain; adds complexity.
- **(c) Normalized silver mirroring NHTSA's hierarchy** — `recall_component` at the 9-field grain (drops `endman`/`bgman`) joined to a child `recall_component_batch` table. Drift on the batch-level fields absorbs into the child without fragmenting the parent. Cleanest architecturally — recommended for Phase 6 modeling. See Section C's "latest version" lookup; with normalized silver, that lookup runs against `recall_component`, where the 9-tuple is empirically stable.

This is a Phase 6 decision, not Phase 5c — but it's worth fixing the
choice before the first silver model lands so the public silver schema
doesn't have to change later.

### Assertion implication — fix applied 2026-05-08

The assertion needs an additional filter to distinguish *structural*
multiplicity (rows from the same run, NHTSA's data shape) from true
*identity drift* (rows from different runs differing on a single
identity field — a likely NHTSA edit). Implemented by adding
`count(distinct raw_landing_path) > 1` to the HAVING clause in
`assert_eleven_tuple_identity_stable.sql`. With one historical-seed run
of bronze, the refined assertion currently expects ≈0 results; as more
incremental runs accumulate, any non-zero count is a real edit to
investigate.

### Baseline — 2026-05-08, two runs into bronze (TOTAL = 3)

After the cross-run filter, the assertion against bronze produced by
two `--since=2023-12-01` runs (2026-05-07 and 2026-05-08) returned:

| drifting_field | drift_group_count | n_rows | nature |
|---|---|---|---|
| `mfr_comp_ptno` | 2 | 4 | **False positive — benign.** Ferrari `26V152000` 12Cilindri side window. Same `rcl_cmpt_id`, two ptnos (`000788416`, `000788418`); both runs reported both ptnos, and both got amendment-inserted (likely from `odate`/`corrective_action` lifecycle changes per Section D). The 11-tuple is stable per ptno — silver `(11-tuple → max(extraction_timestamp))` lookup works correctly. The assertion currently can't distinguish "value set varies by run" from "value set matches but content_hash changed." |
| `endman` | 1 | 2 | **Real drift candidate.** Western Star `26V079000` 47X 2027 battery stud. Same rcl_cmpt_id, same `mfr_comp_ptno=23-13718-006`, NULL bgman; two endman values (`2026-02-03` → `2026-04-10`) one per run. NHTSA appears to have revised the end-of-manufacturing date. This is the failure mode the silver-implication note above describes — without normalization, silver would surface this as two distinct production batches. |
| `bgman`, `compname`, `maketxt`, `mfr_comp_desc`, `mfr_comp_name`, `modeltxt`, `rcl_cmpt_id`, `yeartxt` | 0 | — | Stable across these two runs. |

**Use this baseline for trend-tracking.** Re-run the assertion after
each `recalls extract nhtsa`; net-new drift events beyond these three
warrant investigation. If the false-positive class (`mfr_comp_ptno`-style)
grows, that's a signal to refine the assertion with a "value-set varies
by run" check. If `endman`/`bgman` drift accumulates, that's a signal
to commit to the normalized silver design (option c above) sooner.

### 9-tuple stability validated against the full corpus — 2026-05-08

Companion validation at full TSV grain via three runs of
`scripts/nhtsa/tsv_analysis/`:

**Within-corpus uniqueness — PRE_2010** (`uniqueness_at_tuple.py`):

```
Total rows:                  81709
Distinct tuples:             81709
Multi-row collision groups:  0
Row-unique:                  yes
```

The 9-tuple `(campno, maketxt, modeltxt, yeartxt, compname, rcl_cmpt_id, mfr_comp_ptno, mfr_comp_desc, mfr_comp_name)` is row-unique in PRE_2010 despite four of its fields being systemically NULL (rcl_cmpt_id added 2008-03-14, mfr_comp_ptno/desc/name added 2020-03-23 per Finding F). The remaining populated fields — `(campno, maketxt, modeltxt, yeartxt, compname)` — are themselves enough to disambiguate PRE_2010's coarser-grained data.

**Within-corpus uniqueness — POST_2010** (`uniqueness_at_tuple.py` + `find_differentiator.py`):

```
Total rows:                  240126
Distinct tuples:             238987
Multi-row collision groups:  1004
  Byte-identical:            987      (handled by within-batch dedup; not a 9-tuple issue)
  Anomaly:                   17       (real multi-batch pattern)
```

Of the 17 true anomaly groups, **every single one is differentiated solely by `endman` and/or `bgman`** — confirming that the 9-tuple's residual non-uniqueness in POST_2010 is exactly the structural multi-batch pattern documented above (one part number → multiple production date ranges). Zero non-batch surprises. Rate: 17 / 238,987 ≈ 0.007% of components have multiple batches.

**Cross-corpus stability — `cross_corpus_stability.py`**: comparing the 2026-05-07 and 2026-05-08 full POST_2010 captures:

```
ZIP A rows: 240158
ZIP B rows: 240126
TOTAL drift: 1
  maketxt: 1     ('AC DELCO' in A → 'ACDELCO' in B for campno 22E002000)
  all other 7 fields: 0
```

The single drift event is **NHTSA performing a string normalization** (collapsing the space between `"AC DELCO"` and `"DELCO"`). It's the canonical "natural keys change" failure mode that motivates surrogate-key best practice. **No deterministic surrogate built from natural identity fields can be drift-immune** — the drift can land on any of the 9 fields, not just batch-level ones. This is a downstream-reconciliation problem, not a key-design problem.

### Silver decision (2026-05-08) — option 3b: 11-tuple hash for `recall_product_id`

Given the empirical evidence above, the silver `recall_product_id` derivation for NHTSA is:

```sql
md5('NHTSA' || '|' || campno
   || '|' || coalesce(maketxt, '')      || '|' || coalesce(modeltxt, '')
   || '|' || coalesce(yeartxt, '')      || '|' || coalesce(compname, '')
   || '|' || coalesce(rcl_cmpt_id, '')  || '|' || coalesce(mfr_comp_ptno, '')
   || '|' || coalesce(mfr_comp_desc, '')|| '|' || coalesce(mfr_comp_name, '')
   || '|' || coalesce(bgman::text, '')  || '|' || coalesce(endman::text, ''))
```

This **mirrors CPSC's `recall_product_id` recipe structurally** (`recall_product.sql:31-46`): both use `md5(parent_id || distinguishing_fields || disambiguator)`. CPSC's disambiguator is the JSONB array's `WITH ORDINALITY` position; NHTSA's is the bronze 11-tuple's batch-level fields (bgman, endman) included directly in the hash. Including the values directly rather than via `ROW_NUMBER()` avoids the batch-insertion vulnerability — a newly-discovered production batch gets its own surrogate without shifting existing ones. Each batch is its own silver `recall_product` row with all batch-level metadata in `source_specific_attrs`.

**Documented v1 fragmentation:** the AC DELCO case (and any future similar normalization edits by NHTSA) will produce two `recall_product` rows under the same `recall_event_id`. v1 explicitly accepts this as a known limitation. Reconciliation is the responsibility of Phase 6 entity resolution — see ADR 0031 for the multi-source fragmentation strategy and tier framing.

The earlier "Phase 6 decision options (a/b/c)" framing above is **superseded by ADR 0031**. Option 3b lands as the silver shape now (Phase 5c step 5) so cross-source `recall_event_history` partitioning by `(source, source_recall_id)` works for NHTSA from day one.

---

## H. 2026-05-12 — assertion refactor and updated daily-delta sample

7 observation days into bronze (2026-05-05 → 2026-05-12; see `scripts/sql/nhtsa/_pipeline/inner_content_cadence.sql`), the dbt assertion at `dbt/tests/source_assumptions/assert_nhtsa_eleven_tuple_identity_stable.sql` was refactored to use per-path-value-set semantics, and a fresh daily-delta sample (today's `recalls extract nhtsa --since=2023-12-01` against existing bronze) provided a second datapoint for Sections B/D/F's distributional findings.

### H.1 Assertion refactor — per-path-value-set semantics

The original assertion flagged any 10-tuple group where the rotated 11th field took >1 distinct value across >1 `raw_landing_path`. That filter conflated two phenomena from Section G: (a) **structural multi-batch** — one physical component legitimately reporting multiple values, identical value sets across archives; and (b) **real drift** — one component with different value sets per archive. The refactor compares per-archive value sets via `string_agg(distinct ... order by ...)`:

```sql
having count(distinct raw_landing_path) > 1
   and count(distinct path_value_set) > 1
```

Effect on the same bronze corpus: dbt warn count **104 → 9**. No bronze or silver data changes; the suppression is at the assertion layer only. The diagnostic remains available at `scripts/sql/nhtsa/bronze/decompose_eleven_tuple_drift.sql`, which exposes the structural-vs-real-drift split per field for any future investigation.

### H.2 Reclassification of the Section G 2026-05-08 baseline

The Section G "Baseline — 2026-05-08, two runs into bronze (TOTAL = 3)" table had two `mfr_comp_ptno` entries marked "False positive — benign" (Ferrari 12Cilindri side-window, ptnos `000788416 + 000788418`) and one `endman` entry marked "Real drift candidate" (Western Star 47X 2027 battery stud, `2026-02-03 → 2026-04-10`). Under the refactored assertion:

- The **Ferrari `mfr_comp_ptno` cases** are now formally classified as structural multi-batch and suppressed. Both ptnos appear in every archive's per-path value set; silver's `(11-tuple → max(extraction_timestamp))` lookup materializes both correctly. The "False positive — benign" framing was right in spirit; the refactor makes the suppression explicit. ADR 0031's 2026-05-09 baseline characterization of these as "typo correction" is reclassified — the most likely interpretation is that the prior measurement caught a transitional state (pre-correction archive listed only one ptno, post-correction archive listed only the other), and subsequent archives have re-emitted both, converging the per-path value sets and revealing the structural nature of the duplication.
- The **Western Star `endman` case** remains real_drift, unchanged.

### H.3 Updated baseline — 2026-05-12 (95 structural + 9 real_drift)

After the refactor, against current bronze (~250k rows accumulated over 7 observation days):

| drifting_field | structural_multi_batch | real_drift | total |
|---|---|---|---|
| `mfr_comp_ptno` | 95 | 0 | 95 |
| `bgman` | 0 | 6 | 6 |
| `endman` | 0 | 3 | 3 |
| `compname`, `maketxt`, `mfr_comp_desc`, `mfr_comp_name`, `modeltxt`, `rcl_cmpt_id`, `yeartxt` | 0 | 0 | 0 |
| **TOTAL** | **95** | **9** | **104** |

The 9 real_drift cases concentrate on the batch-window fields exactly as ADR 0031's "Why option 3b" predicted. Cumulative rate: 9 / ~250k ≈ 0.0036%, well under the >0.01%/month threshold. Per-year extrapolation deferred until ≥30 observation days accumulate.

Representative real_drift cases:

- **Chrysler Pacifica `26V189000` airbag** (4 cases): `bgman: 2022-05-10 → 2022-05-17` across 4 ptno variants (`68224526AI/AJ`, `68224527AG/AH`) — one upstream editorial event synced across the side-curtain airbag family.
- **Western Star 47X `26V079000` battery stud** (1 case): carried forward from the 2026-05-08 baseline; still drifting.
- **Mack `26V261000` brake-modulator** (4 cases counted; see H.4 + H.5): 5 vehicle tuples with `bgman`/`endman` populated → NULL, all on the same recall.

### H.4 Mack 26V261000 — NULL-regression resolved as H1 (upstream depopulation)

A new real_drift sub-class surfaced on 2026-05-12 and was resolved end-to-end: NHTSA emitting empty `BGMAN`/`ENDMAN` cells in the 2026-05-09 archive for the Mack brake-modulator component (`mfr_comp_ptno = '24710104'`, `compname = 'SERVICE BRAKES, AIR:ANTILOCK:CONTROL UNIT/MODULE'`) under recall 26V261000.

Three candidate mechanisms were considered:

- **H1** — Upstream depopulation: NHTSA emitted empty cells; recall scope-amended to units of unknown manufacturing date.
- **H2** — Extractor mis-parse: Raw TSV has populated dates, our `FlatFileExtractor` produces NULL — would be a pipeline bug.
- **H3** — Scope expansion adding rows (not replacement): The new archive added rows at NULL bgman/endman without removing the populated-bgman rows.

Triage workflow:

1. **`scripts/sql/nhtsa/bronze/diagnose_null_regression.sql`** — discriminates H3 from H1/H2 via `rows_in_path` analysis. Q1 showed `rows_in_path = 1` in every (10-tuple, path) cell across the 5 affected tuples → replacement, not additive. H3 ruled out.
2. **`scripts/nhtsa/tsv_analysis/inspect_archive_row.py`** — discriminates H1 from H2 via TSV-byte inspection. Run against `nhtsa/2026-05-09/78530d14-...zip.gz` (inner SHA `fee0bd2d55fae636`, change_type=routine, records_inserted=130): all NULL bronze cells correspond to literal empty cells in the inner TSV (`BGMAN (raw): '' (len=0)`). The same archive contains both populated (`'20250708'` → `2025-07-08`) and empty (`''` → NULL) cells, and bronze materializes both consistently. H2 ruled out — the extractor is symmetric and correct.

Sanity-check inspection of the prior 2026-05-08 archive (`nhtsa/2026-05-08/4c2d381e-...zip.gz`, inner SHA `c955c37153d1cb1e`) shows all 5 rows with both BGMAN and ENDMAN populated, confirming the pre-amendment state was uniform.

The depopulation pattern is **asymmetric by model-year**, suggesting a considered editorial decision rather than blanket overwrite:

| (maketxt, modeltxt, yeartxt) | 2026-05-08 BGMAN | 2026-05-09 BGMAN | 2026-05-08 ENDMAN | 2026-05-09 ENDMAN |
|---|---|---|---|---|
| PIONEER PR 2027 | `20241022` | (empty) | `20260408` | `20260408` |
| PIONEER PR 2026 | `20241022` | (empty) | `20260408` | (empty) |
| PIONEER PR 2025 | `20241022` | `20241022` | `20260408` | (empty) |
| ANTHEM AN 2027 | `20250708` | (empty) | `20260408` | `20260408` |
| ANTHEM AN 2026 | `20250708` | `20250708` | `20260408` | (empty) |

Reading column-by-column: 2027 model-years lost BGMAN, kept ENDMAN; 2025 model-years kept BGMAN, lost ENDMAN; 2026 model-years split. The pattern argues NHTSA discovered the original date window didn't bound the affected population at one or both edges, and depopulated rather than guess. Operationally normal — exactly the "Why option 3b" trade-off ADR 0031:84 accepted.

This NULL-regression sub-class is also recorded in ADR 0031's "Re-baseline 2026-05-12" subsection; future similar clusters should be triaged through the same `diagnose_null_regression.sql` + `inspect_archive_row.py` pair.

### H.5 Simultaneous multi-field drift — assertion blind spot (n=1)

PIONEER PR 2026 in the H.4 table has **both** BGMAN and ENDMAN regressing in the same amendment. This case falls through the refactored assertion's per-field rotation: when BGMAN is rotated out, the 10-tuple includes ENDMAN; but ENDMAN also differs across paths, so the 10-tuple doesn't match between path A and path B → not flagged. Symmetric reasoning for the ENDMAN rotation. Contributes 0 to the H.3 count of 9.

The "9 real_drift" headline is an undercount by at least 1; the true count of physically regressing rows in the Mack cluster is **5 tuples, not 4**. The case is visible in `diagnose_null_regression.sql` Q1 (which doesn't rotate fields) but invisible to the dbt assertion. Bronze and silver still materialize both versions correctly — Tier 2 detection is just methodologically incomplete on this shape. A future hardening could add an OR condition catching simultaneous multi-field drift, but at n=1 it's not urgent.

### H.6 Daily-delta sample — 2026-05-12 235-row load

Today's `recalls extract nhtsa --since=2023-12-01` against existing bronze inserted 235 rows. Distributional shape parallel to Sections B/D/F (which characterized the 2026-05-08 run):

**B-analogue — net_new vs amendment** (Section B's 2026-05-08 split was 133 / 61 = 69%/31%):

| kind | count | share |
|---|---|---|
| net_new (no prior 11-tuple) | 23 | 10% |
| amendment (existing 11-tuple, content_hash differs) | 212 | 90% |

Near-mirror reversal of the 2026-05-08 ratio. The net_new/amendment split is **not a stable feature** — daily samples should not be calibrated against it.

**D-analogue — fields driving amendments** (Section D's 2026-05-08 leaders were `odate` / `corrective_action` / `conequence_defect`):

| field | amendments where field changed | rate | notes |
|---|---|---|---|
| `source_recall_id` | 212 | 100% | Hash-excluded per Section C |
| `influenced_by` | 122 | 58% | **New dominant field** — Mercedes-Benz instrument-cluster campaign amended `influenced_by` linkage on every component-row |
| `corrective_action` | 68 | 32% | Carries the KTM `25V598000` Husqvarna Svartpilen/Vitpilen 401 + KTM 390 Adventure **interim→final-remedy lifecycle transition** (4-row family: "Interim notification mailed October 20, 2025" → "Owner notification letters were mailed April 30, 2026") |
| `odate` | 54 | 25% | Same mechanism as Section D — owner-notification dates flipping past-tense |
| `rcdate` | 12 | 6% | **First appearance** — recall date corrections |
| `potaff` | 10 | 5% | Same mechanism as Section D — VIN range firming |

The `corrective_action` change on the KTM family is the highest-value lifecycle signal in the day's load — exactly the kind of state-change the Phase 6 `recall_event_history` model (ADR 0022) is designed to surface. NHTSA's `corrective_action` field appears to be the primary text carrier of recall-stage transitions (interim → final → closed).

**F-analogue — burst distribution** (Section F's 2026-05-08 top: Mercedes-Benz instrument cluster at 122/194 = 63%):

| dimension | top value | share |
|---|---|---|
| `mfgname` | Mercedes-Benz USA, LLC | 122 / 235 = 52% |
| `compname` | `ELECTRICAL SYSTEM: INSTRUMENT CLUSTER/PANEL` | 124 / 235 = 53% |
| Secondary `mfgname` | KTM North America, Inc. | 31 / 235 = 13% |
| Secondary `compname` | `VEHICLE SPEED CONTROL:THROTTLE` | 31 / 235 = 13% |

The Mercedes-Benz / instrument-cluster pairing repeats from 2026-05-08 on a *different* recall campaign — reaffirming Section F's burstiness conclusion. The secondary KTM concentration shows that single days can carry multiple distinct bursts.

## I. 2026-05-13 — Nissan CUBE 26V230000 NULL-regression (per-yeartxt asymmetric)

Second H1 NULL-regression cluster surfaced 2026-05-13, fitting the same upstream-depopulation pattern as H.4 (Mack 26V261000) but with a cleaner per-yeartxt asymmetry. The driver-airbag-inflator component `mfr_comp_ptno = '98560-7991C'` (`compname = 'AIR BAGS:FRONTAL:DRIVER SIDE:INFLATOR MODULE'`, `mfr_comp_name = 'Driver Airbag Inflator'`) under recall `26V230000` — a Takata-cascade-style Nissan CUBE 2009-2010 inflator recall — had `bgman`/`endman` cells depopulated across both yeartxt tuples in some upstream archive published between 2026-05-09 and 2026-05-13. (Our bronze captured only the May 8 pre-amendment and May 13 post-amendment snapshots; the exact upstream publication date is bracketed but not pinpointed in current bronze.)

The cases (boundary depopulated per yeartxt):

| (maketxt, modeltxt, yeartxt) | 2026-05-08 BGMAN | 2026-05-13 BGMAN | 2026-05-08 ENDMAN | 2026-05-13 ENDMAN | Boundary lost |
|---|---|---|---|---|---|
| NISSAN CUBE 2009 | `20081010` | `20081010` | `20100925` | (empty) | **LATE** (endman) |
| NISSAN CUBE 2010 | `20081010` | (empty) | `20100925` | `20100925` | **EARLY** (bgman) |

### The asymmetric-flip wrinkle

Unlike H.4 (Mack) where multiple yeartxts lost the *same* fields with some yeartxts losing both (the H.5 multi-field blind spot), Nissan's pattern is cleanly **one field per yeartxt with opposite boundaries**:

- 2009 CUBEs retained the early manufacturing boundary and lost the late one — meaning NHTSA's amendment effectively extends the recall *past* `20100925` to cover units of unknown end-date.
- 2010 CUBEs retained the late manufacturing boundary and lost the early one — meaning NHTSA's amendment extends the recall *before* `20081010` to cover units of unknown start-date.

The most parsimonious reading: NHTSA discovered the original date window was too narrow at *different* edges for different model years, and depopulated each model year's least-trusted boundary individually. Operationally a considered editorial decision, not a blanket overwrite. Same H1 class as Mack, different shape.

### Classification — H1 high-confidence (pending TSV byte confirmation)

Three converging signals support H1:

1. **`diagnose_null_regression.sql` Q1** — `rows_in_path = 1` for every (10-tuple, path) cell across all 4 affected rows. Replacement, not additive. **H3 ruled out.**
2. **Inner-content SHA changed** between archives — 2026-05-08 inner SHA `c955c37153d1cb1e` (65,732-record initial seed) → 2026-05-13 inner SHA `65c78969d64bddc4` (48-record amendment, the small daily delta). NHTSA genuinely republished different bytes, not a re-fetch artifact.
3. **Pattern matches H.4 Mack** — same script trace, same direction (populated → NULL), same Takata-cascade-style amendment fingerprint on an old airbag-inflator recall.

The Nissan case differs from Mack in that **H2 ruling-out has not yet been confirmed at the byte level**. The Mack triage included a TSV-byte inspection via `scripts/nhtsa/tsv_analysis/inspect_archive_row.py` against the 2026-05-09 archive (`BGMAN (raw): '' (len=0)` confirming literal empty cells in the inner TSV). The Nissan triage so far is **pattern-match only**.

### TODO — TSV byte confirmation (deferred)

To upgrade Nissan from "H1 high-confidence" to "H1 confirmed" — and rule out the edge case of a parser regression triggered only by the Nissan rows — run:

```bash
python -m scripts.nhtsa.tsv_analysis.inspect_archive_row \
    --archive nhtsa/2026-05-13/09dcca74-21d2-4ba8-bb22-e2f108a4bf7b.zip.gz \
    --campno 26V230000 \
    --mfr_comp_ptno 98560-7991C
```

(Confirm the exact CLI flags against `scripts/nhtsa/tsv_analysis/inspect_archive_row.py --help` — the H.4 invocation was the precedent.)

**Expected:** 2 rows returned (yeartxts 2009, 2010), with the depopulated cells showing `BGMAN (raw): ''` or `ENDMAN (raw): ''` per the table above. If instead the TSV bytes are populated and bronze is NULL, classification flips to **H2 (parser bug)** and warrants a follow-up — the asymmetric-flip pattern (different field per yeartxt) is unusual enough to be worth byte-level verification before classifying it definitively as upstream-driven.

If H1 is confirmed, fold the result into this section (replace "H1 high-confidence" with "H1 confirmed" + the byte-evidence line).

### Cumulative contribution and silver-grain context

The 2 Nissan cases bring the 2026-05-13 11-tuple `real_drift` count to **11** (9 May-12 baseline + 2 new Nissan), still well within the `>0.01%/month` threshold from ADR 0031:84. Both Nissan cases live in `bgman`/`endman` (the batch-window fields), consistent with 100% of real_drift to date being in batch-window fields.

This is the second data point feeding **ADR 0031's "Silver-grain migration evaluation" tracking subsection** (added 2026-05-13). The 9-tuple measurement on this same bronze corpus was `TOTAL = 0` real_drift — all 11 cases would collapse to zero fragmentation at the 9-tuple grain. Future tracking via daily snapshots (cadence revised 2026-05-15 from monthly to daily); see ADR 0031 for criteria.

## J. 2026-05-15 — daily-delta sample (healthy weekly amendment baseline)

Third tracking-snapshot day. 111 rows inserted out of 72,622 extracted, run_id `955e2e8d-5fd6-44d1-b120-83f526ba9315`. Cleanest baseline observed so far: no new identity-drift clusters, no NULL-regression events, no phantom-edit churn — and a single dominant real-world recall driving 74% of the wave.

### J.1 Wave shape

| Metric | Value |
|---|---|
| Rows inserted | 111 |
| Net_new 11-tuples | 9 (8.1%) |
| Amendments (existing 11-tuple, content_hash differs) | 102 (91.9%) |
| RCDATE span | 2025-10-10 to 2026-05-07 |
| Null RCDATE | 0 |

The 91.9% amendment / 8.1% net_new split sits between Section B's 2026-05-08 ratio (69% / 31%) and Section H.6's 2026-05-12 ratio (10% / 90%). Reaffirms H.6's note that "the net_new/amendment split is **not a stable feature** — daily samples should not be calibrated against it."

### J.2 Driver-field breakdown

Per `explore_incremental_delta.sql` Q3:

| field | amendments where field changed | rate | notes |
|---|---|---|---|
| `source_recall_id` | 102 | 100% | Hash-excluded per Section C — confirms NHTSA's RECORD_ID instability at scale, doesn't drive re-versioning |
| `corrective_action` | 90 | 88% | **Primary driver.** Remedy expansions + lifecycle progression (interim → owner-notification) |
| `odate` | 20 | 20% | Owner Notification Date progression |

Math check: 90 + 20 = 110, with 102 amendments → 8 amendments changed both, 82 only corrective_action, 12 only odate. **Zero phantom edits.**

Notable lifecycle events visible in Q4 samples:

- **Ford 25V685000** (Lincoln MKC + Explorer + Fusion + Ranger + Lincoln Corsair + Escape + Bronco + Bronco Sport + Maverick engine-block-heater recall): `corrective_action` change adds an alternative remedy option — *"Owners will also have a[n] alternative option to replace engine block heater element with a threaded blanking plug, and remove the block heater electrical cord."* A real remedy expansion, semantically meaningful for downstream consumers.
- **Chrysler 25V766000** (Jeep Grand Cherokee 4XE + Wrangler 4XE engine recall): `corrective_action` change reflects lifecycle stage progression from "Interim notification letters mailed December 29, 2025" → "Owner notification letters were mailed beginning May 7, 2026." The recall advanced from interim-notice stage to owner-notification stage.

### J.3 Burst distribution — Ford 25V685000 dominates

| dimension | top value | share |
|---|---|---|
| `mfgname` | Ford Motor Company | 82 / 111 = **73.9%** |
| `compname` | `EQUIPMENT:ELECTRICAL:ENGINE BLOCK HEATER` | 82 / 111 = **73.9%** |
| Secondary `mfgname` | General Motors, LLC | 12 / 111 = 10.8% |
| Tertiary `mfgname` | Chrysler (FCA US, LLC) | 8 / 111 = 7.2% |

Ford and ENGINE BLOCK HEATER counts are identical (82 each) because all 82 Ford rows belong to recall `25V685000`, a major multi-make multi-year engine-block-heater recall. Reaffirms Section F's burstiness conclusion: single days can be dominated by a single recall campaign's amendment.

### J.4 Identity-stability assertion — no new drift

Per `decompose_eleven_tuple_drift.sql` and `decompose_nine_tuple_drift.sql` against full bronze (~72.7k rows):

| Grain | structural_multi_batch | real_drift | total | Δ vs 2026-05-13 |
|---|---|---|---|---|
| 11-tuple | 137 | **11** | 148 | +44 structural (Ford ptno variants), real_drift unchanged |
| 9-tuple | 139 | **0** | 139 | +42 structural, real_drift unchanged |

**All 11 real_drift cases match the prior documented clusters one-for-one** (Q2 samples verified against H.3 / H.4 / Section I):

- Western Star 26V079000 battery stud: 1 endman case
- Nissan CUBE 26V230000: 1 endman (2009) + 1 bgman (2010) = 2 cases
- Mack 26V261000 brake-modulator: 4 cases (2 endman visible in Q2, 2 bgman beyond the limit-5)
- Chrysler Pacifica 26V189000 airbag: 4 bgman cases

**No new H1/H3 NULL-regression clusters surfaced.** The 11-tuple real_drift count holds steady at 11 across 3 consecutive observation days (5/12: 9, 5/13: 11, 5/15: 11).

### J.5 Ford 25V685000 mfr_comp_ptno structural-multi-batch contribution

The +42 structural_multi_batch increase on `mfr_comp_ptno` (95 → 137 at 11-tuple grain) is dominantly attributable to today's 82-row Ford wave. Q2 samples (`decompose_eleven_tuple_drift.sql` does NOT show samples for structural_multi_batch — those are silent — but `assert_eleven_tuple_identity_stable.sql` Q2 does) show the canonical pattern: same vehicle, same component, two part-number variants both appearing in every archive's per-path value set:

- Ford Bronco 2021 / EQUIPMENT:ELECTRICAL:ENGINE BLOCK HEATER: `GJ7T-6A051-AA` + `GJ7T-6A051-BA`
- Nissan LEAF 2019/2020 / ELECTRICAL SYSTEM:PROPULSION SYSTEM: `295B0 5SA1C` + `295B0 5SF0A`

Both pairs are textbook supersession patterns — supplier published a revised part number suffix and NHTSA's amended archive lists both as in-scope. Silver's `(11-tuple → max(extraction_timestamp))` lookup materializes both correctly. Identical structural pattern to the Ferrari `12Cilindri` case reclassified in H.2.

### J.6 Third data point feeding the ADR 0031 silver-grain migration evaluation

This is the **third consecutive observation day** in the ADR 0031:160 tracking table (2026-05-12, 2026-05-13, 2026-05-15) and the **second consecutive day with 9-tuple real_drift = 0**. Per the revised cadence (daily snapshots, decision after ≥14 consecutive clean snapshots per ADR 0031 Go-Criterion #1), this is the third row of an expected ~14-21 row evaluation window.

The picture remains coherent: **every observed real_drift case lives in batch-window fields** (`bgman`/`endman`). At the 9-tuple grain those collapse away entirely, validating the hypothesis that `md5(9-tuple)` would yield zero silver fragmentation for the patterns we currently see.

---

## Methodology pointer

This document is reproducible from any NHTSA bronze state via:

```bash
psql "$NEON_DATABASE_URL" \
    -f scripts/sql/nhtsa/bronze/explore_incremental_delta.sql
```

Defaults to the most recent successful nhtsa run. Pass
`-v run_id='<uuid>'` to target a specific run.
