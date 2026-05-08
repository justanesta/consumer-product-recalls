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

## Methodology pointer

This document is reproducible from any NHTSA bronze state via:

```bash
psql "$NEON_DATABASE_URL" \
    -f scripts/sql/nhtsa/bronze/explore_incremental_delta.sql
```

Defaults to the most recent successful nhtsa run. Pass
`-v run_id='<uuid>'` to target a specific run.
