# USDA FSIS Establishment Listing — First Extraction Findings

> **Status: Bronze validation complete.** All findings confirmed 2026-05-01 against
> `usda_fsis_establishments_bronze` after the second extraction pass landed 14,804
> total rows (7,945 distinct establishments × 1.86 average versions).

This doc records what the live API actually returned versus what the
pre-extraction Bruno findings (`establishment_api_observations.md`, Findings
A–G, dated 2026-04-29) predicted. Methodology mirrors
`first_extraction_findings.md` for the recall API. Numeric inputs from
`scripts/sql/usda_establishments/bronze/explore_bronze_shape.sql`.

---

## A. Cardinality

Confirmed Finding B exactly. 7,945 distinct establishments returned in a
single full-dump GET, no pagination.

| Metric | Value |
|---|---|
| Distinct `source_recall_id` (= establishment_id) | 7,945 |
| Total bronze rows | 14,804 (two extraction generations — see §H) |
| Distinct extraction dates | 1 |

---

## B. Status enum exhaustiveness — Finding C confirmed

`status_regulated_est` returned exactly two values across all 7,945 records:

| Value | Count | % |
|---|---|---|
| `""` (active MPI) | 7,168 | 90.22% |
| `"Inactive"` | 777 | 9.78% |

No third value. Finding C's two-value enumeration is exhaustive at the
sample size of 7,945 records. The schema declares this as `str` rather than
`Literal["", "Inactive"]` so a future third value lands in quarantine instead
of crashing validation; the strict-quarantine posture remains the right call
even though no third value exists today.

---

## C. Field nullability — mostly confirms Finding D, one new finding

Per-field empty rates on the latest version of each of the 7,945 records.
Note: "null" here includes false-sentinel values that the schema's
`_normalize_false_sentinel` validator coerced to NULL (county, geolocation).

| Field | Observed % null | Finding D claim | Verdict |
|---|---|---|---|
| `duns_number` | 85.51% | 85.5% | ✓ exact |
| `fips_code` | 4.27% | 4.3% | ✓ exact |
| `phone` | 3.91% | 3.9% | ✓ exact |
| `size` | 2.67% | unknown | new datum |
| `county` | 1.54% | 1.5% | ✓ exact (incl. 122 false-sentinels normalized to NULL) |
| `circuit` | 1.18% | unknown | new datum |
| `geolocation` | 1.18% | "~1.5%+" | actually slightly lower (94 false-sentinels normalized to NULL) |
| `district` | 1.18% | unknown | new datum |
| `grant_date` | 0.00% | unknown | **100% populated — new finding (§D)** |

`size`, `district`, `circuit` cluster around 1–3% null rates. None of these
were enumerated in the pre-extraction probe; bronze treats them all as
`Optional[str]` already, so the rates are documentation-only.

---

## D. New finding — `grant_date` is 100% populated

Bruno exploration didn't enumerate `grant_date` nullability. Live data shows
0 nulls across all 7,945 records, including the 777 inactive establishments.
Could promote to required (`_UsdaDate`, not `_UsdaNullableDate`) in the
schema, but the strict-quarantine posture would then reject any future
record where the API drops this field. Recommend leaving as
`_UsdaNullableDate` per the "be conservative on required" principle from the
ADR 0014 commentary.

---

## E. Finding G confirmed — `latest_mpi_active_date` 100% populated on all records

| Metric | Value |
|---|---|
| Total | 7,945 |
| Null count | 0 |
| Date range | 2020-03-24 → 2026-04-27 |
| Distinct dates | 134 |

Confirms Finding G — the field is populated on every record including
inactive ones. The newest date (2026-04-27) corresponds to the last weekly
MPI directory refresh per Finding G's interpretation. The 6-year tail of
distinct dates reflects when each currently-listed inactive establishment
last appeared in the active directory before going inactive.

---

## F. JSONB array shapes — `activities` and `dbas`

| Field | Records | Empty array | Avg length | Max length |
|---|---|---|---|---|
| `activities` | 7,945 | 34 (0.43%) | 2.34 | 13 |
| `dbas` | 7,945 | 5,370 (67.59%) | 0.80 | 100 |

**`activities`** matches Finding C's "true JSON array, can be empty"
description. 99.6% of records carry at least one activity, average ~2.

**`dbas`** is the more interesting number for the recall→establishment join
(Phase 5b.2 Step 5). **Only 32.4% of establishments operate under any
doing-business-as alias**, which means the DBA-fallback strategy in the
silver join helps a minority of records. The max-length-100 case is worth
investigating — that's an establishment with 100 distinct DBA names, almost
certainly a holding company; not a problem for the join shape but
interesting context.

---

## G. State distribution — sanity check

Top 10 states by establishment count, in descending order:
CA (811), TX (556), FL (450), PA (429), IL (426), NY (402), GA (336), NJ (302), WI (263), NC (234).

Reasonable geographic spread — concentration matches population centers + traditional meatpacking regions. No outliers indicating a malformed state column.

---

## H. Re-version pattern — confirms ADR 0027's hypothesis

| Versions per record | Record count |
|---|---|
| 1 | 1,086 |
| 2 | 6,859 |

7,945 records total: 1,086 unchanged across both extraction generations,
6,859 re-versioned by the v2 extractor's `_normalize_str` addition.
Re-version rate of 86.3% is within 1pp of `duns_number`'s 85.5% empty rate
(§C above) — strong evidence that `duns_number` is the dominant axis driving
re-versioning, with `fips_code`, `phone`, `size`, `county`, `circuit`,
`geolocation`, `district` contributing the rest in approximately additive
fashion (any record carrying `""` in *any* of those fields had its
content-hash change).

This is exactly the architectural concern that motivated ADR 0027 (bronze
keeps storage-forced transforms only). Once the refactor lands and removes
`_normalize_str` from the establishment schema, a third extraction will
produce yet another wave (this one tagged `change_type=schema_rebaseline`
per the playbook at `documentation/operations/re_baseline_playbook.md`).
Estimated wave size: similar (~85% of records carry empty-string sentinels
that will land as `""` instead of `NULL`).

---

## I. False-sentinel observations (post-ADR-0027 prediction)

| Field | Current `false_text_count` | Current `null_count` | Total |
|---|---|---|---|
| `county` | 0 | 122 | 7,945 |
| `geolocation` | 0 | 94 | 7,945 |

Today, the `_normalize_false_sentinel` validator coerces JSON `false` to
Python `None`, so bronze stores NULL. Post-refactor (per ADR 0027 storage-type
choice = option 3), the same records will land as the literal text string
`'false'`:

- `county = 'false'`: 122 rows expected.
- `geolocation = 'false'`: 94 rows expected.

The silver staging model picks them up via `nullif(county, 'false')` and
`nullif(geolocation, 'false')`.

---

## Implications for downstream phases

- **Phase 5b.2 Step 5 (silver join):** see the companion document
  `establishment_join_coverage.md` for the recall→establishment match
  evidence and the join-shape recommendation.
- **Phase 5b.2 Step 4.5 (ADR 0027 refactor):** estimated re-baseline wave is
  ~85% of records (driven by `duns_number`); plan accordingly.
- **Schema:** no changes recommended at the schema level beyond the planned
  ADR 0027 refactor. The two-value `status_regulated_est` enum stays
  permissive (`str`, not `Literal`) per the strict-quarantine posture; the
  `grant_date` field stays nullable per conservative-on-required principle.
- **Bronze invariants:** none added — the `check_null_source_id` invariant
  is sufficient. No date-sanity check applies (`latest_mpi_active_date` is
  administrative, not a publication timestamp).

---

## Open items

- [x] Confirm Finding A (no pagination) — confirmed via single GET returning 7,945 records in §A.
- [x] Confirm Finding B (cardinality) — exact match in §A.
- [x] Confirm Finding C (status enum exhaustiveness, false-sentinel handling, array shapes) — confirmed in §B, §F, §I.
- [x] Confirm Finding D (per-field nullability rates) — confirmed for documented fields, three new fields enumerated, one new finding for `grant_date` in §C.
- [x] Confirm Finding G (`latest_mpi_active_date` 100% populated on all records) — confirmed in §E.
- [ ] Recall→establishment join coverage measurement → `establishment_join_coverage.md` (in flight).
- [ ] Phase 5b.2 Step 4.5 — ADR 0027 refactor PR (gates Phase 5c).
