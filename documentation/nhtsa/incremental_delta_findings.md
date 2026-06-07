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

### Classification — H1 confirmed (byte-level, 2026-05-15)

Four converging signals support H1:

1. **`diagnose_null_regression.sql` Q1** — `rows_in_path = 1` for every (10-tuple, path) cell across all 4 affected rows. Replacement, not additive. **H3 ruled out.**
2. **Inner-content SHA changed** between archives — 2026-05-08 inner SHA `c955c37153d1cb1e` (65,732-record initial seed) → 2026-05-13 inner SHA `65c78969d64bddc4` (48-record amendment, the small daily delta). NHTSA genuinely republished different bytes, not a re-fetch artifact.
3. **Pattern matches H.4 Mack** — same script trace, same direction (populated → NULL), same Takata-cascade-style amendment fingerprint on an old airbag-inflator recall.
4. **TSV byte inspection confirms H1 with parser symmetry.** Both archives inspected via `scripts/nhtsa/tsv_analysis/inspect_archive_row.py` on 2026-05-15:
   - **Pre-amendment (`c955c37153d1…`):** 2 matched rows (yeartxt 2009 + 2010), both with `BGMAN (raw): '20081010'` (len=8) and `ENDMAN (raw): '20100925'` (len=8).
   - **Post-amendment (`65c78969d64b…`):** 2 matched rows; yeartxt=2009 shows `BGMAN: '20081010'`, `ENDMAN: ''` (len=0); yeartxt=2010 shows `BGMAN: ''` (len=0), `ENDMAN: '20100925'`. Asymmetric flip confirmed at the byte level.
   - Inner-TSV SHA prefixes match `extraction_runs.response_inner_content_sha256` for both paths — tested the exact bytes that produced bronze.
   - Parser-symmetry rules out H2: the same post-amendment archive contains both populated dates (`'20081010'`, `'20100925'`) and empty cells (`''`), and bronze materializes both consistently — the FlatFileExtractor is not selectively breaking on Nissan rows.

### Byte-level confirmation (2026-05-15)

Confirmation run command (post-amendment archive):

```bash
python scripts/nhtsa/tsv_analysis/inspect_archive_row.py \
    --raw-landing-path nhtsa/2026-05-13/09dcca74-21d2-4ba8-bb22-e2f108a4bf7b.zip.gz \
    --campno 26V230000 \
    --mfr-comp-ptno 98560-7991C
```

Default `--show-field bgman,endman` is correct for this NULL-regression case. Sanity-check run against the pre-amendment archive (`nhtsa/2026-05-08/4c2d381e-91a4-435c-a52e-8f853044f925.zip.gz`) confirmed uniform pre-amendment population — every matched row had both BGMAN and ENDMAN populated, consistent with the asymmetric depopulation being localized to the 2026-05-13 amendment regen.

The sanity-check pattern is now the standing precedent for confirming an H1 cluster end-to-end: post-amendment inspection demonstrates the regression *and* parser symmetry within one archive; pre-amendment inspection demonstrates the editorial event is localized to the post-amendment regen rather than a pre-existing condition.

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

**Postscript (2026-05-15, later same day):** This observation describes the run_id `955e2e8d` capture. A second 2026-05-15 NHTSA run (run_id `07af8eb4`) added 96 `mfr_comp_desc` real_drift cases on Pierce ARROW XT family recall 26V217000 — a non-batch-window field that *is* part of the 9-tuple. See Section K. The "every observed real_drift case lives in batch-window fields" claim was true at the J capture but no longer holds after run_id `07af8eb4`; the 9-tuple migration evaluation is materially disqualified per Stop criterion #1.

## K. 2026-05-15 (later run) — Pierce ARROW XT family 26V217000 mfr_comp_desc population (field-population class)

Second 2026-05-15 NHTSA run (run_id `07af8eb4-0568-4286-8b1e-00565f3f784d`, 120 rows inserted) surfaced the first **field-population** event in the real_drift taxonomy — a class structurally distinct from the existing depopulation (Mack H.4, Nissan I) and boundary-edit (Western Star 26V079000, Chrysler Pacifica 26V189000) classes. NHTSA backfilled `mfr_comp_desc` empty → `'Software'` across all 96 vehicle×component rows of Pierce ARROW XT family recall `26V217000`, in a single archive regeneration.

This run followed run_id `955e2e8d` (the 111-row daily-delta capture documented in Section J) by a few hours within the same UTC day. Both ran against the same NHTSA upstream URL but downloaded different inner-TSV bytes — confirming NHTSA published an additional archive between the two runs, consistent with Finding H's intra-day publication observation. The Pierce population event landed only in the second archive.

### Scope: complete 4 × 12 × 2 grid

The 96 affected rows form an exact 4 × 12 × 2 grid covering the entire campaign:

| Dimension | Values | Count |
|---|---|---|
| `maketxt × modeltxt` | PIERCE ARROW XT, PIERCE ENFORCER, PIERCE IMPEL, PIERCE VELOCITY | 4 |
| `yeartxt` | 2015, 2016, …, 2026 | 12 |
| `compname` | `ELECTRICAL SYSTEM:SOFTWARE`, `EQUIPMENT:MECHANICAL:BOOM/CRANE/LADDER` | 2 |
| **Total combinations** | | **96** |

Every combination got the population — no partial fill, no per-model variance, no per-yeartxt selection. NHTSA's editorial action treated the whole campaign as a single unit.

### Classification — H1 confirmed (byte-level, 2026-05-15)

The four-signal H1 rubric from Section I applies here; per-signal evidence:

1. **`diagnose_null_regression.sql` Q1** — every `(10-tuple, raw_landing_path)` cell shows `rows_in_path = 1` across all 192 affected rows (96 logical products × 2 archives). H3 ruled out. (Q1/Q2 `bgman`/`endman` columns are NULL throughout — Pierce's software-component rows don't carry batch-window dates — but Q3's `extraction_runs` metadata join is class-agnostic and correctly identifies the archive pair.) Invoked via `psql -f diagnose_null_regression.sql -v campno=26V217000 -v "mfr_comp_ptno=Any version prior to 08.15"`.
2. **Inner-content SHA changed**: 2026-05-08 `c955c37153d1cb1e` (65,732-record seed; same archive as the Nissan I baseline) → 2026-05-15 `945cac1b3b0bdf19` (120-record amendment, today's later wave).
3. **TSV byte inspection** via `inspect_archive_row.py --show-field mfr_comp_desc,mfr_comp_name`: pre-amendment, `mfr_comp_desc` empty on all 96 rows while `mfr_comp_name` already held `'Software'`; post-amendment, `mfr_comp_desc` populated to `'Software'` across all 96 rows. Inner-TSV SHA prefixes match `extraction_runs.response_inner_content_sha256` for both paths. Parser symmetry confirmed (post-amendment archive contains both populated and empty `mfr_comp_desc` cells across the broader corpus, bronze materializes all consistently).
4. **New real_drift class**: empty → populated on a non-batch-window field. Distinct from depopulation (H.4 Mack, I Nissan; populated → NULL), boundary edit (Western Star, Pacifica; populated A → populated B on batch-window fields), and string normalization (AC DELCO → ACDELCO).

### Byte-level confirmation (2026-05-15)

Post-amendment inspection command:

```bash
python scripts/nhtsa/tsv_analysis/inspect_archive_row.py \
    --raw-landing-path nhtsa/2026-05-15/fe43db1f-9d6f-4b22-bbbc-4a2a1d7a63d1.zip.gz \
    --campno 26V217000 \
    --mfr-comp-ptno "Any version prior to 08.15" \
    --show-field mfr_comp_desc,mfr_comp_name
```

Pre-amendment sanity check uses the same command against `nhtsa/2026-05-08/4c2d381e-91a4-435c-a52e-8f853044f925.zip.gz` (already cached locally from the Nissan I inspection earlier the same day).

### Structural observations

**Asymmetric population.** `mfr_comp_name` was already populated as `'Software'` across all 96 rows in the pre-amendment seed. Only `mfr_comp_desc` was empty. NHTSA's editorial action specifically backfilled the description column, leaving the name column untouched. This asymmetry makes the event a clean **field-population** class rather than a paired structural change (e.g., adding both `name` and `desc` together for a previously-missing component).

**Contiguous RECORD_ID block in post-amendment.** All 96 affected rows occupy RECORD_IDs `319889` through `319984` in the post-amendment archive — exactly 96 consecutive integers. The pre-amendment archive shows Pierce rows scattered across the TSV with multiple smaller contiguous segments (e.g., 320164-320223, 320320-320327) interleaved with other corpus content. Per Finding K's "RECORD_ID is reassigned per file build", the contiguous block in the post-amendment regen reflects NHTSA's amendment running a single internal query against this campaign and serializing all 96 results back-to-back into the new TSV. Strong structural confirmation that this was **one editorial action**, not 96 independent edits.

**Co-occurrence with other amendments.** The same 120-record amendment archive carried both Pierce's `mfr_comp_desc` population AND additional Ford engine-block-heater amendments (continuation of the J.5 Ford wave). NHTSA's daily regen pipeline batches multiple unrelated editorial actions into a single archive — the Pierce event and the Ford amendments share no logical connection beyond being committed in the same NHTSA-side workflow window.

### Silver fragmentation impact

Per ADR 0031:84's `recall_product_id = md5(11-tuple)` recipe, each of the 96 logical Pierce products now appears twice in silver:

- 96 `recall_product` rows with `mfr_comp_desc = ''` (pre-amendment versions, semantically stale)
- 96 `recall_product` rows with `mfr_comp_desc = 'Software'` (post-amendment versions, current)

All 192 share the same `recall_event_id` (`md5('NHTSA' || '26V217000')`). `stg_nhtsa_recalls.sql`'s `DISTINCT ON (11-tuple) ORDER BY ... extraction_timestamp DESC` retains both because they're distinct 11-tuples; `DISTINCT ON` cannot disambiguate stale-vs-current when the only differing field is itself a surrogate-key input. Downstream `select count(*) from recall_product where recall_event_id = md5('NHTSA|26V217000')` returns 192 instead of 96.

This is the documented v1-accepted fragmentation from ADR 0031:83-86, but at a much larger scale than the original baseline anticipated. ADR 0031:110's "~150 fragmented NHTSA `recall_product` rows per year" extrapolation is materially stale — a single editorial event added 96 fragmented rows in one day. Phase 6 product-level reconciliation will need a "populated supersedes empty" rule for the population class specifically.

### 9-tuple migration evaluation — disqualified

The 9-tuple migration tracking subsection in ADR 0031:148-216 was monitoring whether `md5(9-tuple)` would eliminate observed fragmentation. The premise at ADR 0031:170: *"Eliminates the only fragmentation class we actually observe in production."* Today's Pierce event materially refutes this — `mfr_comp_desc` is **in the 9-tuple**, so the 96 events fragment silver equally at both 9-tuple and 11-tuple grains. Stop criterion #1 at ADR 0031:199 ("Any daily snapshot surfaces non-zero 9-tuple real_drift") has fired.

The 9-tuple migration alternative loses its compelling case: expected benefit drops from "eliminate 100% of observed fragmentation" to "eliminate ~10% (the 11 batch-window cases out of 107 total)" at the same migration cost. v1 `md5(11-tuple)` stands not because it's structurally better against this event (both grains fragment identically), but because the migration alternative is no longer attractive. Formal sunset of the migration tracking subsection lands in the ADR 0031 2026-05-15 amendment.

### Cumulative contribution

The Pierce event brings the 2026-05-15 11-tuple `real_drift` count to **107** (J.4's 11 batch-window cases + Pierce's 96 `mfr_comp_desc` cases), and the 9-tuple `real_drift` count from `0` to **96**. Per-field breakdown via `decompose_eleven_tuple_drift.sql`:

| Field | structural_multi_batch | real_drift | Total | Drift class |
|---|---|---|---|---|
| `mfr_comp_ptno` | 137 | 0 | 137 | structural (Ford supersession + Ferrari + others) |
| `mfr_comp_desc` | 0 | 96 | 96 | **population (new — Pierce 26V217000)** |
| `bgman` | 0 | 7 | 7 | depopulation + boundary edit (Pacifica, Nissan, Mack) |
| `endman` | 0 | 4 | 4 | depopulation + boundary edit (Western Star, Nissan, Mack) |
| **TOTAL** | **137** | **107** | **244** | — |

Cumulative real_drift rate: 107 / ~250k = 0.043% on the 11-tuple (vs. ADR 0031:84's prior 0.0036%). 11-fold increase from a single editorial event. ADR 0031:84's ">0.01% silver row count fragmented per month" threshold is materially exceeded depending on how the campaign-burst is weighted — one campaign with 96 events is structurally different from 96 campaigns with 1 event each, and ADR 0031 will likely need to revise the threshold definition to account for this in a future amendment.

## L. 2026-05-16 — first post-Pierce daily snapshot (real_drift stable, structural wave continues)

Fourth tracking-snapshot day, first daily observation following K's editorial event. 259 rows inserted out of 72,658 extracted, run_id `e6d0753f-0771-4333-baac-492d25d0ec8b`. Still under the `--since=2023-12-01` dev-mode RCDATE filter — bronze remains date-bounded.

### L.1 Identity-stability assertion — real_drift bit-identical to K, structural wave continues

| Grain | structural_multi_batch | real_drift | total | Δ vs 2026-05-15 K |
|---|---|---|---|---|
| 11-tuple | 194 | **107** | 301 | +57 structural `mfr_comp_ptno`, real_drift unchanged |

### L.2 Real_drift composition matches K one-for-one

Every per-field count and every Q2 sample row matches K's post-event state:

- `mfr_comp_desc` = 96 (Pierce ARROW XT family 26V217000 4×12×2 grid — Q2 samples show the `'' || Software'` per-path value sets)
- `bgman` = 7 (Chrysler Pacifica 26V189000 ×4 airbag-ptno variants + Nissan CUBE 26V230000 ×1 + Mack 26V261000 ×2)
- `endman` = 4 (Western Star 26V079000 ×1 + Nissan CUBE 26V230000 ×1 + Mack 26V261000 ×2)

**No new editorial bursts followed K within one day.** This is one negative data point against K being the start of a cluster, not yet a "stable" claim. The field-population class warrants several more daily snapshots before it can be reclassified one-off vs behavior-change.

### L.3 Structural `mfr_comp_ptno` +57 delta

Structural `mfr_comp_ptno` grew 137 → 194 (+57 groups) while real_drift held at 0 for that field. Same shape J.5 anticipated (legit supersession multi-batch, silver-correct, suppressed at the assertion layer). The 259-row wave is the largest in this branch's daily series so far (J=111, K=120, L=259), so a +57 structural increment is consistent with routine amendment activity — but worth attributing to confirm.

New script `scripts/sql/nhtsa/bronze/attribute_structural_drift_by_campno.sql` aggregates structural `mfr_comp_ptno` groups by campno (with maketxt / compname / RCDATE identification baked into the same query). Expected top contributor: Ford 25V685000 engine-block-heater wave (J.5). If a previously-unseen campno appears with non-trivial group count, it merits a brief L.4 follow-up.

### L.4 Structural attribution: L.3 expectation falsified, recall composition broader than predicted

Running `attribute_structural_drift_by_campno.sql` against the 194 structural `mfr_comp_ptno` groups:

| campno | n_groups | makes | sample compname | RCDATE | n_rows |
|---|---|---|---|---|---|
| 26V281000 | **61** | Mercedes-Benz, Mercedes-Maybach | ELECTRICAL SYSTEM: INSTRUMENT CLUSTER/PANEL | 2026-05-01 | 244 |
| 26V191000 | **53** | Van Hool | VISIBILITY:GLASS, SIDE/REAR | 2026-03-26 | 318 |
| 25V685000 | 36 | Ford, Lincoln | EQUIPMENT:ELECTRICAL:ENGINE BLOCK HEATER (J.5) | 2025-10-10 | 144 |
| 26V189000 | 18 | Chrysler | AIR BAGS:SIDE/WINDOW:CURTAIN | 2026-03-26 | 80 |
| 26V012000 | 5 | Ford | EQUIPMENT:ELECTRICAL:ENGINE BLOCK HEATER | 2026-01-15 | 20 |
| 24V700000 | 4 | Nissan | LEAF propulsion (2 compnames) | 2024-09-19 → 2024-11-14 | 24 |
| 25V756000 | 4 | Mack | EXTERIOR LIGHTING:HEADLIGHTS | 2025-11-03 | 24 |
| 26V160000 | 4 | Hyundai | SEATS:MID/REAR ASSEMBLY:POWER ADJUST | 2026-03-17 | 30 |
| 26V228000 | 4 | Thomas Built Buses | EXTERIOR LIGHTING:HAZARD FLASHING WARNING LIGHTS | 2026-04-09 | 16 |
| 26V152000 | 2 | Ferrari | VISIBILITY:GLASS, SIDE/REAR (H.2) | 2026-03-16 | 12 |
| 26V278000 | 2 | Mercedes-Benz | SEAT BELTS:FRONT:RETRACTOR | 2026-05-01 | 16 |
| 26E021000 | 1 | MOPAR | ENGINE | 2026-04-16 | 4 |
| **TOTAL** | **194** | | | | |

**L.3's "Ford 25V685000 is the expected top contributor" claim is materially wrong.** Ford is #3 (36 groups, 19% of structural total). The top two — Mercedes 26V281000 (61, 31%) and Van Hool 26V191000 (53, 27%) — together contribute 59% and were undocumented in J.5 or earlier.

Structural observations:

- **Three of the top four campnos have 2026 RCDATEs** (26V281000 on 2026-05-01, 26V191000 on 2026-03-26, 26V189000 on 2026-03-26). Multi-batch is not exclusively an aging-amendment phenomenon — recent recalls are being published multi-batch from the start. Reframes J.5's "Ford supersession wave" as one shape among several rather than the canonical case. The structural pattern's underlying mechanism is supplier supersession at publication time, and that can happen on day-1 just as readily as months in.
- **26V189000 contributes to two drift classes simultaneously.** The Chrysler Pacifica recall appears here with 18 structural `mfr_comp_ptno` groups AND in L.2's real_drift `bgman` cluster (4 cases) — same recall, independent identity facets. H.3's narrative on Pacifica focused on the bgman boundary edit; the ptno-multiplicity component was not flagged. Worth noting as a precedent: a single recall can land in both structural and real_drift columns of the decomposition without any contradiction.
- **26V278000 (Mercedes seat belts) has `avg_distinct_ptnos_per_group = 4.0`**, double the corpus norm of 2.0. Could be a 4-variant supplier-line shape (e.g., left/right × front-row × two suppliers) rather than the typical 2-variant supersession pair. n=2 groups so far — worth monitoring in future snapshots.
- **Manufacturer diversity is broad** — Mercedes (twice), Van Hool, Ford (twice), Chrysler, Nissan, Mack, Hyundai, Thomas Built, Ferrari, MOPAR. No single OEM dominates the way Ford did in J.5's 82-row engine-block-heater wave. Van Hool's appearance is notable — a Belgian bus manufacturer with low US fleet count but apparently amendment-active enough to contribute 53 structural groups (318 bronze rows on a single side-glass recall).

**+57 attribution remains underdetermined.** Without a 2026-05-15 snapshot of this same script, the day-over-day delta can't be pinned to a specific campno. The most plausible RCDATE-recency candidates are the two Mercedes recalls (both 2026-05-01) freshly crossing the multi-batch threshold today — but partial growth on any of the existing campnos is equally consistent with +57. **Operational recommendation:** add this script to the daily-snapshot reflex (run after `decompose_eleven_tuple_drift.sql`) so future per-campno deltas are mechanical.

No drift-class change: every campno's `avg_distinct_ptnos_per_group ∈ [2.0, 4.0]` and `max_landing_paths_per_group ∈ {2, 3}` — same silver-correct supplier-supersession shape across the board. ADR 0031 implication (next section) is unchanged.

### L.5 ADR 0031 implication

The migration-tracking subsection sunset in K — this snapshot doesn't reopen it. No ADR 0031 amendment warranted from today's results; post-K monitoring continues in this file via further Section-L-style daily rows if a subsequent snapshot adds anything novel.

## M. 2026-05-19 → -25 — three-day amendment series, rcdate-mutable surface, and methodology refinements

Five tracking-snapshot days following Section L. The intervening 5/17 + 5/18 days returned zero inserts (bronze content_hash dedup absorbed both archives — the no-change pattern that motivates M.5's inner-hash refinement). Three substantive days (5/20, 5/21, 5/25) plus the 5/19 forward-edge baseline make up the M series.

### M.1 Five-day shape table

| Date | run_id (short) | n loaded | net_new / amend | RCDATE span | top non-`source_recall_id` Q3 driver | comment |
|---|---|---|---|---|---|---|
| 2026-05-19 | `f8d26cc3` | 28 | — | — | — | forward-edge baseline (Q3 not retrieved) |
| 2026-05-20 | `ce0b1826` | 42 | 25 / 17 | 2025-12-18 → 2026-05-18 | `odate 13 (76%)` | routine forward-edge (J/L template) |
| 2026-05-21 | `f15b2008` | 368 | 18 / 350 | 2025-08-08 → 2026-05-18 | `corrective_action 343 (98%)` | JLR 160-row concentration + Oxford-comma sub-edits (M.3) |
| 2026-05-25 | `217e753d` | 478 | 115 / 363 | 2024-10-01 → 2026-05-20 | `rcdate 189 (52%)` | 3 per-recall rcdate corrections; row count inflated by broadcast (M.2) |

Bronze corpus at 5/25 end: 74,107 rows. 5/19 and 5/20 are J/L-template forward-edge baselines (no new pattern). 5/21 and 5/25 surface novel structure documented in M.3 and M.2.

### M.2 2026-05-25 — `rcdate` corrections on three specific recalls (not an archive-regen)

The 5/25 wave reported `rcdate` as the second-most-modified non-`source_recall_id` field — 189 of 363 amendments (52%) shifted the recall date. Section H.6's 5/12 snapshot was the only prior instance with non-zero rcdate amendments (12 of 235 = 6%, explicitly flagged as "first appearance"). Sections I/J/K/L did not see rcdate as a driver. 52% looked like a step-change worth chasing — initially read as an archive-republish event (NHTSA regenerating `RCL_Annual_Rpts.txt` from a master database). **Empirically falsified by `attribute_rcdate_shifts_by_campno.sql` (run_id `217e753d`):**

| campno | makes | n_rows_shifted | n_distinct_pairs | shift |
|---|---|---|---|---|
| 25V315000 | FORD, LINCOLN | 88 | 1 | 2025-05-14 → 2025-05-09 (−5 days) |
| 24V733000 | ITASCA, WINNEBAGO | 83 | 1 | 2024-10-09 → 2024-10-01 (−8 days) |
| 25V343000 | FORD, LINCOLN | 18 | 1 | 2025-06-13 → 2025-05-23 (−21 days) |
| **TOTAL** | | **189** | — | — |

**Three recalls, three rcdate corrections, all backward (earlier dates), each with a single uniform shift propagated to every 11-tuple row of the recall.** The 189 / 363 = 52% headline reflects the broadcast mechanism (M.3) inflating a small recall-count into a large row-count, not a bulk regen affecting the whole corpus.

Refinements to the inferential reading above:

1. **The "broad RCDATE backreach into 2024" is one recall** — Winnebago/Itasca 24V733000 contributes all 83 of the 2024 rows. Not a 2024 cohort republish; one specific 2024 recall had its rcdate corrected backward by 8 days.
2. **The "Q1d year histogram is broad-spectrum" framing was misleading** — the histogram shape reflects three specific recalls (24V733000 / 25V315000 / 25V343000) at three different RCDATE points + the 174 non-rcdate-touching amendments distributed across whatever other recalls hold the corpus's amendment baseline.
3. **`mfgcampno`'s 14 amendments** (3.9% of 363) are now empirically characterized (`scripts/sql/nhtsa/bronze/attribute_mfgcampno_shifts_by_campno.sql`, 2026-05-25). One recall: Tesla 26V283000, all 14 rows shifted uniformly from `SB-26-00-016` to `SB-26-00-001` (length-delta = 0; prefix `SB-26-00-0` preserved; last 3 chars `016` → `001`). Same per-recall-broadcast pattern as the rcdate cases, on the manufacturer-side service-bulletin identifier rather than NHTSA's metadata. Looks like a sequence-number correction — the interim "016" position renumbered to a canonical "001" on Tesla's side. The 14 mfgcampno amendments and the 189 rcdate amendments are structurally independent (different recalls, different editorial mechanisms) but exhibit the identical "single edit propagated to every 11-tuple row" shape Section M.3 documents for `corrective_action`. Confirms the broadcast mechanism generalizes across all hash-included payload fields.

**Mechanism (revised, empirically grounded):** NHTSA does per-recall editorial corrections, including rcdate corrections, propagated through the daily archive regeneration. The corrections appear to be *backdating* corrections — adjusting a published rcdate to an earlier date (the "true" recall-issuance date as opposed to the date the entry appeared in the public archive). Three independent corrections happened to ship in the same 5/25 archive. Coincidence of timing, not a unifying editorial event.

**rcdate is mutable in practice.** The Sections D / H.6 framing of rcdate as a *rare* amendment field is empirically supported (3 recalls over a 13-day window = ~0.23 recalls/day affected); the 52% row-level rate is a multiplicative artifact of the row-broadcast mechanism, not a frequency increase.

**Reconciliation impact** — `rcdate` is a payload attribute, not part of any candidate identity tuple (11-tuple per ADR 0030/0031, 6-tuple per ADR 0033). It does not fragment silver in either v1 or v1.5. The 189 amendment inserts on these 3 recalls are a **bronze write-volume cost** that ADR 0033's 6-tuple + Type-1-latest-wins design absorbs as attribute updates without fragmentation. The class slots cleanly into the existing "value edit on attribute field" reconciliation rule (`project_scope/archive/silver_v15_migration_plan.md`); no taxonomy extension warranted. The "amendment row count vs. distinct-recalls-affected" gap is a useful metric to surface in `recall_event_history` (Phase 6c) so consumers can see "this rcdate appears to have changed 88 times" vs. "this rcdate changed once and propagated to 88 product-rows of the same recall."

**Empirical closure (Q2 + Q3 captures, 2026-05-25):**

| metric | observed |
|---|---|
| `rows_with_rcdate_shift` | 189 |
| `distinct_campnos_with_shift` | 3 |
| `distinct_shift_pairs` | 3 |
| `avg_year_delta` | 0.00 (all shifts intra-year) |
| Q3 direction | **`backward shift` (100%, n=189)** — zero forward, zero NULL transitions |
| Q3 day-delta range | min −21, max −5, avg −7.8 |

The Q3 unanimous-backward result is the strongest empirical signal: every one of the 189 rows is a backward correction. If any subset of the 363 amendments were a true archive-regen artifact, we would expect at least a few forward shifts (rcdate forward-corrected on currently-published recalls) or NULL transitions (regen filling previously-missing rcdates). Neither appears. The wave is unambiguously **per-recall editorial backdating** propagated through the M.3 broadcast mechanism.

**`mfgcampno` follow-up closed**: see M.2 narrative point #3 above. Single Tesla recall (26V283000), uniform `SB-26-00-016 → SB-26-00-001` shift via the broadcast mechanism. Structurally independent of the rcdate corrections; same per-recall-edit shape.

### M.3 2026-05-21 — JLR 160-row concentration and the Oxford-comma sub-edit class

The 5/21 wave's burst signature was extreme — **160 of 368 rows (43%) are a single recall** (Jaguar Land Rover STEERING:LINKAGES:KNUCKLE:SPINDLE:ARM). Largest single-recall absolute concentration in the observation window — bigger than J.3's Ford 25V685000 (82 rows of a 111-row day).

The driver-field signature was equally extreme — `corrective_action 343 (98% of amendments)` plus `odate 185 (53%)`. Section J.2's 5/15 sample saw 88% / 20% on the same two fields. The 98% rate on 350 amendments means substantively every amendment touched `corrective_action`.

**The Oxford-comma class** — Q4's BMW X5 Takata sample showed the smallest hash-significant `desc_defect` edit observed yet: *"humidity, temperature, **and** temperature cycling"* (5/21 archive) vs. *"humidity, temperature and temperature cycling"* (prior). One Oxford comma plus its preceding whitespace shift. Both rows of a 2-year-make pair received the change, and the recall's full 160-row roster received parallel `corrective_action` edits. Two implications:

- **NHTSA's amendment workflow broadcasts text edits across every 11-tuple row of a recall**, including microscopic punctuation edits. The Section K mechanism ("single editorial action → 96 bronze rows") generalizes from population events to text-edit amendments. Operationally normal for NHTSA's daily-regen-from-master-database pattern, but worth documenting as a baseline expectation.
- **Silver must normalize whitespace and punctuation before reaching consumer surfaces** — without it, every Oxford-comma-class edit propagates as N silver-level `field_edited` events (where N is the recall's row count, here 160). Empirical reinforcement of the per-field whitespace-normalization deliverable scoped under Phase 6c's `feature/recall-event-history` work stream (see `project_scope/archive/silver_v15_migration_plan.md` and ADR 0022).

The JLR concentration also qualifies Section F's burst framing: bursts are best characterized by **single-recall (campno) concentration**, not `mfgname` or `compname` concentration. The JLR 160 rows span multiple yeartxts but one campno + one compname; J.3's Ford 82 rows span nine model-makes but one campno + one compname. The unifying primitive is the campno, and burst metrics should center it.

### M.4 Identity assertion at 380 drift groups — framing refinement

`assert_eleven_tuple_identity_stable.sql` returns 380 total drift groups against the current corpus (74,107 rows):

| field | groups | mapping to documented clusters |
|---|---|---|
| `mfr_comp_ptno` | 267 | J.5 Ford supersession + L.4's per-campno table (Mercedes 26V281000, Van Hool 26V191000, Ford 25V685000, Chrysler 26V189000, etc.) + structural growth across the 5/16 → 5/25 window |
| `mfr_comp_desc` | 96 | Pierce ARROW XT 4×12×2 grid (Section K); no growth, class quiescent for 10 days |
| `bgman` | 10 | H.3 Pacifica ×4 + I Nissan ×1 + H.4 Mack ×2 + new BMW K 1600 + Western Star |
| `endman` | 7 | H.3 Western Star + I Nissan + L.4 BMW K 1600 + Mack |
| natural-key core (compname, maketxt, modeltxt, yeartxt, rcl_cmpt_id, mfr_comp_name) | **0** | — |

The 0 on the natural-key core is the structural invariant the 11-tuple was designed to preserve. 380 is **expected steady-state**, not failure: it reflects (a) accumulated structural multi-batch from supplier supersession (mostly `mfr_comp_ptno`), (b) Section K's Pierce population event still resident in bronze, and (c) the small real_drift cluster on batch-window fields documented across H/I/L.

**Framing refinement (applied 2026-05-25)**: the assert script's *"TOTAL = 0 means the 11-tuple identity is stable across runs"* line is overly binary. The true invariant is **natural-key core stability** (the 6 core fields = 0); secondary descriptors (`mfr_comp_ptno`, `mfr_comp_desc`, `bgman`, `endman`) are expected to accumulate drift and are decomposed by `decompose_eleven_tuple_drift.sql` into structural-multi-batch (silver-correct) and real_drift (silver-fragmenting) classes. Script header updated to reflect this; logic unchanged.

**Empirical decomposition (`decompose_eleven_tuple_drift.sql`, 2026-05-25)**:

| field | structural_multi_batch | real_drift | total |
|---|---|---|---|
| `mfr_comp_ptno` | 267 | 0 | 267 |
| `mfr_comp_desc` | 0 | 96 | 96 |
| `bgman` | 0 | 10 | 10 |
| `endman` | 0 | 7 | 7 |
| natural-key core | 0 | 0 | 0 |
| **TOTAL** | **267** | **113** | **380** |

The 113 real_drift breaks down as 96 `mfr_comp_desc` (Pierce ARROW XT 4×12×2 grid from Section K — the field-population class) + 17 batch-window real_drift across `bgman`+`endman` (H.3 / I / H.4 / L plus the BMW K 1600 cluster surfaced below). The 267 structural is entirely `mfr_comp_ptno` supplier-supersession multi-batch — silver-correct under both v1 `md5(11-tuple)` and v1.5 `md5(6-tuple)` per ADR 0033.

**Novel cluster surfaced 2026-05-25** — `26V214000` (BMW K 1600 B/GT/GTL motorcycles, `POWER TRAIN:MANUAL TRANSMISSION:SEALS/GASKETS`, Reverse Gear Control Unit ptno `8524078`) is contributing to **both** `endman` (3 cases — K 1600 B/GT/GTL each with endman populated → NULL) and `bgman` (1 case — K 1600 B with bgman populated → NULL) real_drift. Section L.4 explicitly flagged 26V214000's structural `mfr_comp_ptno` involvement (4 groups) as "worth monitoring," but the simultaneous H1-class NULL-regression in batch-window fields wasn't yet documented. Same multi-class-on-one-recall shape Section L.4 noted for 26V189000 Pacifica (structural ptno + real_drift bgman). One recall, three independent drift facets.

**ADR 0031 silver-fragmentation rate**: 113 real_drift / 74,107 bronze rows = 0.15%. Materially above ADR 0031:84's `>0.01% per month` trigger threshold, but **entirely attributable to two well-documented editorial events**: Section K Pierce population (96, ≈85% of real_drift) and the cumulative batch-window cluster from Sections H/I/L plus today's 26V214000 BMW K 1600 (17, ≈15%). K remains the single largest source of fragmentation in the corpus by ~6×. ADR 0033's v1.5 6-tuple architecture eliminates ~99% of this — 113 → 1 (the AC DELCO normalization case that fragments at all grains).

### M.5 Inner-hash sensitivity refinement — necessary but not sufficient

`spot_check_extraction_runs.sql` Section 3 reports day-over-day `response_inner_content_sha256` transitions. Migration 0011's commentary and the script's prior header both characterized inner_hash as *"the authoritative 'did the data change?' oracle"*. The 5/11 → 5/25 daily run sequence empirically refines this claim.

Every day in 5/11 → 5/25 shows `inner_transition = CHANGED`. This includes:
- Days with substantial loads (5/12, 5/15, 5/16, 5/19, 5/20, 5/21, 5/25)
- **Days with zero loads (5/17, 5/18)**

The 5/17 and 5/18 zero-load days are existence proofs that inner-TSV bytes can differ across daily archives without representing any real data change. **Empirical investigation 2026-05-25 (`scripts/nhtsa/tsv_analysis/diff_inner_tsv.py`) identifies the actual mechanism**: NHTSA reassigns RECORD_ID values across every daily archive build, even for unchanged recall content. Section C documented this at the cell level (RECORD_ID is a per-build sequence number); the diff tooling confirms it at byte-and-set-level scale.

**Empirical evidence** — running the diff in raw-bytes mode against the 2026-05-16 → 2026-05-17 → 2026-05-18 archives (each ~240,381 lines, all three days having identical bronze content_hash dedup outcomes):

| pair | raw-bytes verdict | lines differing | lines identical |
|---|---|---|---|
| 5/16 ↔ 5/17 | `REAL_CHANGE` | 240,371 (99.996%) | 10 |
| 5/17 ↔ 5/18 | `REAL_CHANGE` | 240,379 (99.999%) | 2 |

Same-slot RECORD_ID examples illustrating the artifact:

```
5/16 row 81715: 81715  10V080000  GEM         eL          2009  PARKING BRAKE  ...
5/17 row 81715: 81715  10V484000  CARRIAGE    CARRI-LITE  2004  EQUIPMENT      ...
5/18 row 81715: 81715  10V456000  DOUBLE TREE ELITE       2003  EQUIPMENT      ...
```

Same RECORD_ID slot (`81715`), three different recalls. The byte-level diff classifies every such line as a "real change" because field 0 is part of the byte signature.

**Stripped-column diff confirms the mechanism unambiguously** — running the script with default `--strip-record-id` (drops column 0 before diffing):

| pair | verdict | sorted-SHA A | sorted-SHA B | lines only in A | lines only in B |
|---|---|---|---|---|---|
| 5/16 ↔ 5/17 | `REORDER` | `4821a45b615dddbe…` | `4821a45b615dddbe…` (match) | 0 | 0 |
| 5/17 ↔ 5/18 | `REORDER` | `4821a45b615dddbe…` | `4821a45b615dddbe…` (match) | 0 | 0 |

The sorted-SHA-256 is **identical** across all three days. After stripping RECORD_ID, the multisets of lines are byte-equal across 5/16, 5/17, and 5/18 — only the physical row order shuffled. Zero whitespace artifacts, zero column-padding artifacts, zero real content changes. Pure RECORD_ID reassignment plus physical row reordering, both of which bronze content_hash dedup correctly canonicalizes via the 11-tuple identity and `hash_exclude_fields={source_recall_id}`.

**ADR 0030's design choice empirically vindicated**: bronze's `hash_exclude_fields={source_recall_id}` instruction means the 11-tuple + content_hash dedup canonicalizes the row content without RECORD_ID. Every "different" byte-level line maps to the same logical 11-tuple identity in bronze, which is why `records_inserted = 0` on these days. The architecture handles per-build sequence-number reassignment correctly; the script's previous "necessary but not sufficient" framing of `inner_hash` was the right call.

**Correct framing:**
- `inner_hash` matches prior → **sufficient** evidence of no change (TSV bytes are byte-equal; nothing changed)
- `inner_hash` differs from prior → **necessary but not sufficient** evidence of change (bytes differ, but could be reorder / whitespace / padding OR real data change)
- Bronze content_hash dedup (`records_inserted > 0`) is the **only authoritative oracle** for real change

Script header (`scripts/sql/nhtsa/_pipeline/spot_check_extraction_runs.sql`) and Migration 0011 commentary refined 2026-05-25 to reflect this. Section J's wrapper-hash narrative is unaffected — wrapper hash was always known to be daily-noisy; this refinement only constrains the claim about inner hash.

**Open follow-up closed 2026-05-25**: mechanism identified empirically as RECORD_ID column-0 reassignment (see "Empirical evidence" subsection above). Bronze content_hash dedup correctly absorbs the artifact via `hash_exclude_fields={source_recall_id}` (ADR 0030). For future flat-file sources, the design intent crystallizes as: if the source emits a per-build sequence column, capture the inner-content sha for forensic continuity but treat it as a *necessary-not-sufficient* change oracle, and use a stripped-column SHA (or the bronze content_hash count) as the authoritative one.

### M.6 Phase 6 / ADR 0033 implication

The findings in this section reinforce the v1.5 SCD-2 design decision in ADR 0033 without requiring extension:

- **rcdate-mutable (M.2)** — slots into "value edit on attribute field" → Type-1 latest-wins under v1.5. No taxonomy change.
- **Oxford-comma class (M.3)** — empirical evidence the per-field whitespace-normalization deliverable in `feature/recall-event-history` is mission-critical, not nice-to-have. Without it, the 5/21 wave alone produces 160 silver-level `field_edited` events on a single conceptual edit. With it, those collapse to a single canonical-text-comparison no-op.
- **Identity drift at 380 (M.4)** — affirms ADR 0033's premise that the 11-tuple's secondary descriptors (`mfr_comp_ptno`, `mfr_comp_desc`, `bgman`, `endman`) are mutable in practice and should not be in the silver anchor key. The 6-tuple anchor proposed in ADR 0033:47–86 maps to the natural-key core fields that show 0 drift across the full corpus.
- **Inner-hash refinement (M.5)** — independent of ADR 0033, but reinforces the architectural choice to make bronze content_hash dedup the canonical change oracle rather than relying on upstream hashes.

No ADR 0033 amendment warranted. Future Phase 6c `feature/recall-event-history` work should include the M.2/M.3 patterns as test fixtures (a synthetic rcdate-shift archive and a synthetic Oxford-comma archive) to validate the Type-1/Type-2 mechanism handles them as designed.

---

## Methodology pointer

This document is reproducible from any NHTSA bronze state via:

```bash
psql "$NEON_DATABASE_URL" \
    -f scripts/sql/nhtsa/bronze/explore_incremental_delta.sql
```

Defaults to the most recent successful nhtsa run. Pass
`-v run_id='<uuid>'` to target a specific run.
