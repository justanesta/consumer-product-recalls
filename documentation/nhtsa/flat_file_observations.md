# NHTSA Flat-File Source — Empirical Observations

> **Status: Step 1 complete (2026-05-05); Step 3 first-extraction analysis (2026-05-07) added Findings K and L; TSV-level full-corpus analysis (2026-05-08) widened ADR 0030's identity tuple from 7 to 11 fields.**
> Findings A, B, C, D, E, F, G, I confirmed 2026-05-04 / 2026-05-05.
> Finding J (ZIP wrapper non-determinism) added 2026-05-05.
> Findings K (RECORD_ID is regenerated per file build) and L (TSV ships
> byte-duplicate rows) added 2026-05-07 from Step 3 first-extraction
> analysis. Finding L's evidence updated 2026-05-08 with full-POST_2010
> corpus measurements; the bronze-narrow scope underestimated the
> residue. Architectural response: **ADR 0030** (NHTSA bronze identity:
> composite tuple + within-batch dedup) — initial 7-tuple amended to
> 11-tuple on 2026-05-08 after TSV-level analysis converged on the
> minimum row-unique identity for the full corpus.
> Architecture decision **resolved as Option A (TSV-only)** after Finding I
> revealed CSV files are a structurally divergent document-attachment index
> rather than recall data. Finding H's update-cadence sub-question
> **closed 2026-05-08** via `extraction_runs.response_inner_content_sha256`
> day-over-day diffs across 7 successful runs: NHTSA publishes content
> intermittently, can update intra-day (≥1 update on 2026-05-08), and is
> idle on most consecutive runs (4/6 transitions) — confirming daily cron +
> content-hash dedup as the right operational shape.
> Evidence accumulates in `documentation/nhtsa/watermark_probes.jsonl`
> plus Step 2 download artifacts in `data/exploratory/nhtsa/` (gitignored).

## Background

NHTSA publishes safety-related recall data as flat files served from
`https://static.nhtsa.gov/odi/ffdd/rcl/`. The directory contains a
documentation PDF, an inline data dictionary (`RCL.txt`), and **two
parallel corpora in different formats** (Finding I):

- **TSV family** (`.txt` inner, tab-delimited per RCL.txt's documented schema):
  - `FLAT_RCL_PRE_2010.zip` (~7 MB compressed, 80 MB uncompressed) — 1967–2009
  - `FLAT_RCL_POST_2010.zip` (~14 MB compressed, 290 MB uncompressed) — 2010–present
  - `FLAT_RCL_Annual_Rpts.zip` / `FLAT_RCL_Qrtly_Rpts.zip` — periodic rollups
- **CSV family** (`.csv` inner, undocumented in RCL.txt):
  - `RCL_FROM_<startYear>_<endYear>.csv` — year-band slices and rolling-current
    files, sizes from 1.7 KB to 84 MB uncompressed

The source is hosted directly from S3 (no public CDN cache layer
detectable in HEAD responses — see Finding G for header inventory). The
inline data dictionary `RCL.txt` documents 29 tab-delimited fields with
dates in YYYYMMDD format and a max record length of 17,108 bytes.

The five questions Step 1 must answer per
`project_scope/implementation_plan.md`:

1. URL pattern stability (single canonical incremental URL or rotating
   per-year filename?)
2. File size, row count, and encoding
3. Column count + types vs. RCL.txt's documented schema
4. Update cadence (re-download diff-based)
5. Schema-drift history
6. `Last-Modified` reliability for watermarking

These are answered across Findings A–H below.

## Source mapping (decided: Option A)

The eventual `NhtsaExtractor` will use the same TSV corpus for both code
paths required by the plan's standing architectural requirement (lines
145–152). Options B and C were eliminated by Finding I: the CSV files are
a document-attachment index, not recall data — they do not carry the
fields the extractor needs.

| Path | URL | Format | Size | Cadence |
|---|---|---|---|---|
| Incremental | `https://static.nhtsa.gov/odi/ffdd/rcl/FLAT_RCL_POST_2010.zip` | 29-field TSV per RCL.txt | ~14 MB compressed, 290 MB uncompressed | Daily download; bronze content-hash dedup short-circuits idle days (ADR 0007) |
| Historical seed | `https://static.nhtsa.gov/odi/ffdd/rcl/FLAT_RCL_PRE_2010.zip` + `FLAT_RCL_POST_2010.zip` | 29-field TSV per RCL.txt | ~21 MB compressed, ~370 MB uncompressed | One-time at seed; weekly defense-in-depth via `deep-rescan-nhtsa.yml` per ADR 0010 |

Combined coverage: 1967ish–present, **~321,800 rows** (81,714 in PRE_2010 +
240,126 in POST_2010 as of 2026-05-04). Bandwidth tax of ~14 MB/day is
absorbed cheaply by ADR 0007 content-hashing on idle days.

---

## Findings

### Finding A — ETag is content-MD5

Confirmed 2026-05-04 via direct comparison between `md5sum` of the
downloaded body and the ETag returned in HEAD.

NHTSA's S3 ETags for files in `static.nhtsa.gov/odi/ffdd/rcl/` are the
MD5 hash of the file body, the default behavior for non-multipart S3
uploads.

**Verification:**

```
$ md5sum <(curl -sL https://static.nhtsa.gov/odi/ffdd/rcl/RCL.txt) | awk '{print $1}'
436e400b92a4d15deee70feff4fa4d88

$ curl -sI https://static.nhtsa.gov/odi/ffdd/rcl/RCL.txt | grep -i etag
etag: "436e400b92a4d15deee70feff4fa4d88"
```

The 32-hex-character ETag matches the file's MD5 exactly.

**Implications:**

- ETag stability across probes ≡ content stability, with effectively-zero
  collision risk for non-adversarial federal data publishing.
- A future `NhtsaExtractor` can use `If-None-Match: "<etag>"` for
  conditional GET — assuming Finding B confirms that ETag is also stable
  across NHTSA's daily regen job (still under investigation).
- The `body_sha256` capture in the watermark probe is now confirmatory
  rather than primary. ETag-comparison alone is sufficient evidence of
  content-change for analytical purposes.

**Caveat:** if NHTSA ever switches to multipart S3 uploads, ETag becomes
`<md5-of-md5s>-<part-count>` (no longer plain MD5). The `body_sha256`
field in the probe JSONL would catch this regression — if `body_md5` ever
stops matching ETag, the schema has shifted.

---

### Finding B — Last-Modified watermark reliability

> **Status: Confirmed unreliable, 2026-05-04 via inner-file mtime; further
> corroborated 2026-05-05 via 24-hour probe diff on `RCL.txt`.**

**Question:** Does NHTSA's `Last-Modified` header track real content
changes, or is it re-stamped daily by a regeneration job regardless of
content?

**Result: HTTP `Last-Modified` is unreliable.** Two independent lines of
evidence:

**(1) Inner-file mtime evidence from Step 2 download.** Each ZIP's inner
mtime (preserved via `curl --remote-time`) reveals a discrepancy that
the wrapper's HTTP `Last-Modified` hides:

| File | Wrapper HTTP `Last-Modified` | Inner-file mtime |
|---|---|---|
| `RCL_FROM_2025_2025.csv` | `Mon, 04 May 2026 07:04:23 GMT` | `2025-12-31 08:02` |
| `RCL_FROM_2025_2026.csv` | `Mon, 04 May 2026 07:04:23 GMT` | `2026-05-04 07:01` |
| `FLAT_RCL_POST_2010.txt`  | `Mon, 04 May 2026 07:04:23 GMT` | `2026-05-04 07:02` |
| Other archives | `Mon, 04 May 2026 07:04:23 GMT` | `2026-05-04 07:01` |

`RCL_FROM_2025_2025.csv` was last actually regenerated on 2025-12-31 —
~125 days ago — but its wrapping ZIP shows today's `Last-Modified`.

**(2) 24-hour probe diff for `RCL.txt`.** Across 2026-05-04 13:33Z →
2026-05-05 13:21Z, `Last-Modified` advanced ~23 hours while every
content fingerprint stayed bit-identical:

| Field | 2026-05-04 | 2026-05-05 |
|---|---|---|
| `Last-Modified` | `Mon, 04 May 2026 07:04:23 GMT` | `Tue, 05 May 2026 07:05:14 GMT` |
| `ETag` | `"436e400b92a4d15deee70feff4fa4d88"` | `"436e400b92a4d15deee70feff4fa4d88"` |
| `body_sha256` (prefix) | `9ec6414ae51bc633…` | `9ec6414ae51bc633…` |
| `bytes_observed` | 3053 | 3053 |

Same content, fresh `Last-Modified`. The daily regen job re-stamps
wrapper metadata regardless of whether content changed.

**Implications:**

- The `NhtsaExtractor` must NOT use `If-Modified-Since` conditional GETs.
  `Last-Modified` advances on idle days for files whose contents haven't
  changed in months.
- Bronze dedup must rely on content-hash (ADR 0007). For plain-text files
  (`RCL.txt`, `RCL_Annual_Rpts.txt`, `RCL_Qtrly_Rpts.txt`) the wrapper
  body_sha256 is itself a stable content fingerprint. **For ZIPs the
  wrapper is non-deterministic across re-archives — see Finding J — so
  dedup must hash the *decompressed inner content*, not the wrapper.**
- The inner-file mtime is a strong watermark candidate by itself. Once
  the extractor has decompressed the wrapper, it can read the inner mtime
  via `zipfile.ZipInfo.date_time` and skip extraction work entirely if
  the value matches the prior run's recorded mtime. Worth implementing as
  an optimization in `_FlatFileExtractor`.

---

### Finding C — `x-amz-version-id` behavior

> **Status: Confirmed 2026-05-05 via 24-hour probe diff.**
> `x-amz-version-id` is the regen-PUT signal, not the content-change
> signal. Same disqualification as `Last-Modified`.

**Question:** Does NHTSA's S3 mint a new version ID on every regen
(blind re-upload) or only on real content change?

**Result: blind re-upload.** Across 2026-05-04 → 2026-05-05, every
regen-managed file got a fresh `x-amz-version-id` even when its content
stayed bit-identical. The static `Import_Instructions_Recalls.pdf`
serves as a control: it's outside the regen pipeline, so its
`x-amz-version-id` is stable across days.

| File | bytes Δ | ETag Δ | body_sha256 Δ | `x-amz-version-id` Δ |
|---|---|---|---|---|
| `Import_Instructions_Recalls.pdf` (control) | none | none | none | **none** (`JPSw2I…5Pk0` both days) |
| `RCL.txt` | none | none | none | **changed** (`J1Yxnj…WS08` → `IplECX…X4YM`) |
| `RCL_FROM_2025_2025.zip` | none (91307 → 91307) | none | none | **changed** (`MYtbvV…9NMl` → `MEi41d…CTQi`) |

Mapping back to the decision matrix:

| `x-amz-version-id` | `body_sha256` | Interpretation | Observed in |
|---|---|---|---|
| changes daily | stable | Regen blindly re-uploads identical bytes | `RCL.txt`, `RCL_FROM_2025_2025.zip` |
| changes daily | changes | Real content update *or* ZIP non-determinism | All other ZIPs (see Finding J — ambiguous at the wrapper level) |
| stable | stable | No upload, file genuinely static | `Import_Instructions_Recalls.pdf` |
| stable | changes | Impossible (S3 invariant) | (not observed) |

**Implications:**

- `x-amz-version-id` is **not** a content-change signal — it answers
  "did NHTSA's regen pipeline run today" (always yes), which the
  extractor doesn't need to ask.
- The signal *is* useful as an audit anchor in bronze: persisting it
  alongside each fetch records the exact S3 object version we ingested,
  which lets us replay or compare a future fetch to a specific historical
  upload. Worth capturing in `bronze.nhtsa_recalls_raw` even though it's
  not a watermark.

---

### Finding D — Year-band URL pattern (2025_2025 vs 2025_2026)

> **Status: Confirmed 2026-05-04 via inner-file mtime evidence from
> Step 2 download.**

**Question:** What is the naming convention for `RCL_FROM_<YYYY>_<YYYY>.zip`,
and which file is the "current rolling window" that `NhtsaExtractor` should
hit incrementally?

**Result:** The naming convention is `RCL_FROM_<startYear>_<endYear>.zip`,
where the file with the highest `endYear` is the **rolling current**.
Files with `endYear` < current calendar year are **frozen snapshots**:

| File | Inner-file mtime | Interpretation |
|---|---|---|
| `RCL_FROM_2025_2025.zip` | 2025-12-31 08:02 | Frozen — final 2025 snapshot, regenerated only on year-close |
| `RCL_FROM_2025_2026.zip` | 2026-05-04 07:01 | Rolling current — regenerated daily as new records arrive |

The rolling-current file is the incremental candidate (Option B / Option C
in the source-mapping table). Sizes are similar (~1.3 MB uncompressed each)
because the rolling file mostly contains 2025 records plus a small 2026 tail.

**Cache-control corroboration (Finding G):** the cache-control max-age
TTL for `RCL_FROM_2025_2026.zip` is regen-aware (~17.8 hours, expiring
just after the next 07:04 GMT regen) while `RCL_FROM_2025_2025.zip` has
the default ~24h TTL — consistent with the frozen-vs-rolling distinction.

**Open caveat — rotation rule on year transitions:** we cannot tell from
one observation whether NHTSA will publish `RCL_FROM_2025_2027.zip` in
January 2027 (start year stays at 2025) or `RCL_FROM_2026_2027.zip` (start
year advances annually). The `config/sources/nhtsa.yaml` URL must be
templated to handle whichever pattern emerges, with a fallback probe in
the extractor for the alternative naming. Reconfirm in early 2027.

**Implications:**

- For Option B / Option C architecture: incremental URL is
  `RCL_FROM_2025_2026.zip` until end of calendar 2026.
- Year-transition handling: schedule a calendar reminder for late 2026
  to re-probe the directory and update the YAML once NHTSA's 2027 naming
  is observable.

**Implications (when resolved):**

- Confirms whether `config/sources/nhtsa.yaml` can hardcode the URL or
  needs to template it by date (e.g., `RCL_FROM_<prev>_<current>.zip`).

---

### Finding E — TSV column count, encoding, and embedded HTML

> **Status: Confirmed 2026-05-04 via direct inspection of
> `FLAT_RCL_POST_2010.txt`.**

**Question:** Does the live file match RCL.txt's documented 29-field
shape? What text encoding is used? Are there parser-relevant surprises
in the field contents?

**Result:**

| Property | Value |
|---|---|
| Field count | **29 fields** — matches RCL.txt's documented schema exactly |
| Delimiter | tab (`\t`) |
| Header row | **none** — first line is a data record |
| Encoding | **UTF-8** (NOT CP1252; iconv UTF-8 round-trip succeeds, CP1252 fails) |
| Line terminator | **CRLF** (Windows-style; `file` heuristic reports CRLF). Parser splits on `\r\n` and `\n` only — narrower than `str.splitlines()`, so cells containing form feed / vertical tab / NEL / Unicode line-paragraph separators are preserved as cell content rather than treated as row boundaries. Tightened in `_iter_tab_delimited` 2026-05-06 after a hypothesis test surfaced the broader-than-documented behavior; relevant for any future flat-file source whose cells legitimately contain those characters. |
| Row count (POST_2010) | 240,126 |
| Row count (PRE_2010) | 81,714 |
| Field positions | RECORD_ID/CAMPNO/MAKETXT/MODELTXT/YEARTXT/MFGCAMPNO/COMPNAME/MFGNAME/... per RCL.txt — verified via spot-check of the first record (`81715 │ 10V407000 │ DAMON │ INTRUDER │ 2005 │ RC000018 │ EQUIPMENT:RECREATIONAL VEHICLE/TRAILER:LPG SYSTEMS:TANK ASSEMBLY │ THOR MOTOR COACH │ ...`) |

**Embedded HTML in description fields (parser-relevant surprise):**

The narrative fields (`DESC_DEFECT`, `CONEQUENCE_DEFECT`,
`CORRECTIVE_ACTION`, `NOTES`) contain **inline HTML anchor tags**, e.g.:

```
DAMON SAFETY RECALL NO. RC000018.OWNERS MAY ALSO CONTACT THE NATIONAL HIGHWAY
TRAFFIC SAFETY ADMINISTRATION'S VEHICLE SAFETY HOTLINE AT 1-888-327-4236
(TTY 1-800-424-9153), OR GO TO
<A HREF=HTTP://WWW.SAFERCAR.GOV>HTTP://WWW.SAFERCAR.GOV</A> .
```

This tripped `file`'s content-type heuristic (it reported "HTML document"
on the file because of these tags). The file is plain UTF-8 text with
embedded HTML fragments inside specific fields — not actual HTML.

**Schema-design implications (Phase 5c Step 2 input):**

- **Bronze layer:** preserve the raw text as-is, including the HTML tags.
  ADR 0014's `extra='forbid', strict=True` covers shape; preserving
  embedded markup as bytes-faithful storage matches the bronze-as-raw
  principle and lets silver decide how to render.
- **Silver staging (`stg_nhtsa_recalls.sql`):** strip or decode HTML before
  presenting to downstream consumers. Two approaches:
  - **Quick fix:** regex-strip `<A HREF=...>...</A>` to bare URLs.
  - **Robust fix:** call a dbt macro that wraps Postgres `regexp_replace`
    or a UDF for full HTML decoding (handles entities, malformed tags).
- **Per ADR 0027:** this is value-level normalization that belongs in
  staging, not bronze. The bronze schema accepts the field as-is; the
  silver staging model produces the cleaned version.
- **Test cassette must include an HTML-bearing record.** The Damon recall
  shown above is a representative example. Without one in the suite, the
  parser's HTML handling never gets exercised under test.

**Other field-content observations from the spot-check:**

- Empty fields are **literal empty strings between consecutive tabs**, not
  any sentinel value. Confirmed for fields 18 (`RPNO`), 19 (`FMVSS`),
  25-27 (manufacturer-supplied component fields) on the spot-checked
  record.
- `DO_NOT_DRIVE` and `PARK_OUTSIDE` (fields 28-29, added May 2025 per
  RCL.txt) appear as `No` strings, not booleans. The Pydantic schema
  needs `_to_bool`-style coercion (string-yes/no → Python bool) similar
  to USDA's pattern in `src/schemas/usda.py`.
- `RCL_CMPT_ID` (field 24) appears as a fixed-width-style identifier
  (`000037237000216701000000332`) — looks like concatenated numeric
  codes. RCL.txt documents it as "Number That Uniquely Identifies A
  Recalled Component" but the structure (multiple sub-fields?) isn't
  documented further. Treat as opaque string at bronze, investigate at
  silver if needed.

**Caveats not yet probed:**

- Whether the description fields contain **literal newlines or tabs**
  inside their text (which would break naïve line-by-line / column-split
  parsing). The single record observed appears clean, but RCL.txt's
  6,000-char field width and free-text origin make this plausible. Either
  worth probing explicitly OR deferring to the cassette suite (Step 4) to
  catch via real failures.

---

### Finding F — Documented schema-drift history

Documented from `documentation/nhtsa/RCL.txt` change log (the data
dictionary distributed alongside the data files).

| Date | Change |
|---|---|
| 2007-09-14 | Field #23 (NOTES) added; flat-file extension changed `.lst` → `.txt` |
| 2008-03-14 | Field #24 (RCL_CMPT_ID) added |
| 2020-03-23 | Fields #25, #26, #27 added (manufacturer-supplied component metadata) |
| 2025-05    | Field #19 (FMVSS) shrunk to CHAR(3); fields #20, #22 widened to 6000; fields #28, #29 added (DO_NOT_DRIVE, PARK_OUTSIDE) |

**Pattern observed:** four drift events in 18 years, three of them
adding columns at the **right edge** of the row. Field-shape changes
(reductions in column width, extensions of others) occurred once in May
2025.

**Implications:**

- Bronze schema must follow ADR 0014's `extra='forbid', strict=True`
  directive: a 30th column appearing breaks the schema and triggers
  re-ingest per ADR 0014. Document this as the explicit choice rather
  than a tolerance for trailing fields.
- The May 2025 width reduction on field #19 is precedent that fields can
  shrink too — Pydantic schema should validate field length where
  documented (e.g., FMVSS as `Annotated[str, StringConstraints(max_length=3)]`),
  catching a regression to a wider value.
- Historical archives may contain records with the older 23-field /
  24-field / 27-field shapes if NHTSA retains them. Pre-2007 records
  cannot have NOTES, pre-2008 cannot have RCL_CMPT_ID, etc. The bronze
  schema for a unified table must allow nullability on fields added
  after the file's coverage start.

---

### Finding G — Header inventory and CDN/cache layer

> **Status: Partial — confirmed 2026-05-04.** May be revisited if NHTSA's
> stack changes.

**Headers observed in HEAD responses across all probed files:**

| Header | Present | Notes |
|---|---|---|
| `last-modified` | yes | All files |
| `etag` | yes | All files; content-MD5 per Finding A |
| `content-length` | PDF only | Data files use chunked transfer encoding |
| `content-type` | yes | `application/octet-stream` (zips), `application/pdf`, `text/plain` |
| `cache-control` | yes | Per-file TTL; data files ~18 hours, PDF 24 hours |
| `accept-ranges` | PDF only | Data files don't advertise byte-range support |
| `date` | yes | Server response timestamp |
| `x-amz-version-id` | yes | S3 PUT-versioning anchor (per Finding C) |
| `x-amz-replication-status` | yes | `REPLICA` — cross-region S3 replication |

**Headers checked and confirmed absent:**

| Header | Implication |
|---|---|
| `server` | No public server identifier — direct S3, no CloudFront fronting |
| `via` | No proxy hop visibility |
| `age` | No public CDN cache layer (or it's stripped) |
| `x-cache` | No CloudFront/CDN cache hit/miss reporting |

**Conclusion:** `static.nhtsa.gov/odi/ffdd/rcl/` is served directly from
S3 with no public CDN cache layer detectable. Cache-busting and bot
detection (Akamai-style) issues that affect FDA / USDA do not appear to
apply here. `etag` and `last-modified` are the only public watermark
surfaces; `x-amz-version-id` is the unique-upload anchor.

---

### Finding H — Update cadence and historical coverage

> **Status: Historical coverage fully confirmed 2026-05-04 via refined
> date-bound probes (DATEA, RCDATE, BGMAN, ODATE). Update cadence
> CONFIRMED 2026-05-08 via `extraction_runs.response_inner_content_sha256`
> day-over-day diffs across 7 successful NHTSA runs (2026-05-05 → 2026-05-08).
> Standing closure mechanism: `scripts/sql/nhtsa/_pipeline/inner_content_cadence.sql`.**

**Question 1 (update cadence):** How often does NHTSA actually publish
new content vs re-stamp idle data? Daily? Weekly? In bursts?

**Result 1: NHTSA publishes content updates *intermittently* and *can update intra-day*. Daily cron is the right cadence; idle days are absorbed at near-zero cost by content-hash dedup.**

Empirical evidence (`scripts/sql/nhtsa/_pipeline/inner_content_cadence.sql`, run 2026-05-08):

```
total_runs:                  7
distinct_inner_snapshots:    3   (edae1d2193478bcd, c955c37153d1cb1e, bf43d58588cbc608)
content_change_transitions:  2 / 6  (33% of consecutive transitions)
idle_transitions:            4 / 6
```

| Started (UTC) | Inner hash (prefix) | Transition | Notes |
|---|---|---|---|
| 2026-05-05 22:39 | `edae1d21` | first_run | Initial bronze load (66,057 rows) |
| 2026-05-05 22:44 | `edae1d21` | unchanged | Re-run minutes later — bronze dedup correctly skipped (0 inserted) |
| 2026-05-07 11:44 | `c955c371` | **CHANGED** | NHTSA published new content between 2026-05-05 22:44 and 2026-05-07 11:44 (≥1 update in this ~37-hour window) |
| 2026-05-08 02:46 | `c955c371` | unchanged | Idle re-fetch |
| 2026-05-08 03:16 | `c955c371` | unchanged | Idle re-fetch |
| 2026-05-08 03:29 | `c955c371` | unchanged | Idle re-fetch |
| 2026-05-08 20:05 | `bf43d585` | **CHANGED** | NHTSA published new content within UTC day 2026-05-08 (between 03:29 and 20:05 — likely the 07:05 daily regen window per Finding C) |

**Key observations:**

- **Real content changes happen** (refutes the null hypothesis that wrappers are re-stamped without content changes). 2 out of 6 transitions in this window carry actual new content.
- **Content can change within a single UTC day.** 2026-05-08 had two distinct inner-content snapshots from runs at 03:29 and 20:05. The change probably aligns with NHTSA's ~07:05 UTC daily regen window observed in `watermark_probes.jsonl` Last-Modified headers (Finding B).
- **Content is NOT updated every day.** Multiple consecutive idle transitions (May 5 → 5, May 7 → 8 morning) confirm idle-day stability — important because it means daily cron + content-hash dedup is the right cost-shape (no wasted bronze writes on idle days; observed 0 net bronze inserts across the 4 idle transitions).
- **Sample is small but consistent with expectation.** 4 days isn't enough to characterize weekly seasonality (e.g., Mon-Fri publishing) or burst patterns. Longer-term observation lives in `extraction_runs` once Phase 7 cron is on.

**Operational implications:**

- **Daily cron is correct.** ADR 0010's daily incremental cadence catches at least the 07:05 UTC regen window. ADR 0007 content-hash dedup absorbs idle days at near-zero cost (validated empirically: 4 idle transitions = 4 × ~14 MB downloads + 0 bronze writes).
- **Daily cron will miss intra-day updates.** If a recall is published at 09:00 UTC and the cron runs at 06:00 UTC, the next observation is the following day. This is acceptable for v1; downstream consumers requiring lower latency would need either (a) higher-frequency cron, or (b) a NHTSA notification subscription if such exists.
- **The cadence-monitor SQL script (`inner_content_cadence.sql`) is now standing tooling.** Re-run periodically (or wire into a Tier 2 / DQ-framework hook per ADR 0031) to catch any change in NHTSA's publication cadence.

**Closure date:** 2026-05-08. Standing closure mechanism: `scripts/sql/nhtsa/_pipeline/inner_content_cadence.sql`. No further investigation required for v1.

**Question 2 (historical coverage):** What is the actual date range and
total record count of the TSV archive corpus we're committing to?

**Result 2 (confirmed 2026-05-04):**

Total: **321,840 rows** across PRE_2010 (81,714) + POST_2010 (240,126).

Date bounds vary by which date field you measure — DATEA is record-creation
in NHTSA's database, not the date of the recall event. The most meaningful
"recall coverage" measure is RCDATE (Part 573 Defect/Noncompliance Report
Received Date):

| Field (RCL.txt) | PRE_2010 lower bound | PRE_2010 upper bound | Notes |
|---|---|---|---|
| `RCDATE` (field 16) | **1966-01-19** | 2009-12-31 | Cleanest proxy for "when did the recall happen." Predates RCL.txt's "since 1967" prose by 11 months. |
| `BGMAN` (field 9) | 1949-08-01 | 2009-11-12 | Earliest manufacturing date subject to recall — a 1949 vehicle. |
| `ODATE` (field 13) | 1901-01-01 ⚠️ | 2012-04-24 | Lower bound is a **placeholder/sentinel for unknown notification date**; upper bound exceeds 2010 because owner mailings continue years after Part 573 filing. |
| `DATEA` (field 17) | 1979-10-12 | (per POST_2010 probe: 20260429) | NHTSA's database started 1979-10-12 with a bulk-load of pre-1979 historical recalls (~11,500 records stamped Oct-Dec 1979). 5 records (0.01%) have null DATEA. |

**Decade distribution (DATEA, PRE_2010 only):**

| Decade | Records | Notes |
|---|---|---|
| 1970s | 11,571 | Almost entirely the Oct-Dec 1979 bulk-load of historical data going back to 1966 |
| 1980s | 8,577 | |
| 1990s | 16,844 | |
| 2000s | 44,717 | |
| (empty) | 5 | 0.01% null — Pydantic schema must allow null DATEA |

**Coverage claim:** the NHTSA recall corpus reaches back to **January 1966**
by RCDATE, with manufacturer-side build dates as early as 1949. RCL.txt's
"since 1967" prose is conservative — actual coverage starts with the very
first Part 573 reports filed under the 1966 National Traffic and Motor
Vehicle Safety Act.

**Implication for Phase 5c Step 2 schema design (cross-reference Finding E):**

- `DATEA` is nullable (5 records in PRE_2010 confirm).
- `RCDATE` is **also nullable** (5 PRE_2010 records — almost certainly the
  same cohort as the empty-DATEA records, from the 1979 bulk-load of
  pre-1979 historical recalls). Surfaced by the 2026-05-05 sentinel
  probe; schema relaxed accordingly. Marking required would quarantine
  real recall records over a missing date field.
- `ODATE` uses **`19010101` as an unknown-date sentinel.** Bronze
  preserves the literal value per ADR 0027; `stg_nhtsa_recalls.sql` maps
  `19010101` → NULL during silver normalization.
- Other date fields likely have their own sentinels — worth probing
  systematically before locking the schema. Check `BGMAN`, `ENDMAN`,
  `RCDATE` for analogous outliers (`19010101`, `99999999`, all-zeros).
- The PRE_2010 archive can contain records with ODATE values past 2010
  (one record has 2012-04-24). The archive partition is by DATEA, not by
  any other date field. Don't assume "PRE_2010 → all dates < 2010."

**Sentinel-date probe results (2026-05-05) — Finding H follow-up closure:**

The systematic probe of fields 9, 10, 13, 16, 17 (BGMAN/ENDMAN/ODATE/RCDATE/DATEA)
plus boolean fields 28/29 and FMVSS (field 19) ran via
`scripts/nhtsa/probe_date_sentinels.sh`. Three categories of finding:

*Step 2 schema-blocking (resolved by relaxing required → nullable):*

| Field | Issue | Count | Archive | Resolution |
|---|---|---|---|---|
| `RCDATE` | empty values | 5 | PRE_2010 | Schema relaxed to `_NhtsaNullableDate`; migration 0011 column relaxed to `nullable=True` |

*New silver-staging sentinels discovered (bronze parses cleanly; map to NULL in `stg_nhtsa_recalls.sql`):*

| Field | Sentinel | Count | Archive | Notes |
|---|---|---|---|---|
| `ODATE` | `11111111` | 67 rows | POST_2010 | Second ODATE sentinel pattern — parses to 1111-11-11 |
| `ODATE` | `19010101` | 7 rows | PRE_2010 | Original Finding H sentinel |
| `BGMAN` | `19000101` | 7 rows | POST_2010 | Manufacturing-date "unknown" sentinel |
| `ENDMAN` | `19000101` | 7 rows | POST_2010 | Same — likely paired with BGMAN sentinel rows |

*Wild manufacturing dates in POST_2010 (NOT sentinels — data-entry errors that parse via `strptime("%Y%m%d")`):*

- `BGMAN` range: `00220729` (year 22 CE!) to `30190514` (year 3019)
- `ENDMAN` range: `00240102` (year 24 CE) to `30190430` (year 3019)

These pass the validator because `strptime` validates only the *shape*,
not year reasonableness. Per ADR 0027 the bronze schema preserves them
as-is; silver staging needs a date-sanity filter:
```sql
case when extract(year from bgman) between 1900 and extract(year from now()) + 1
     then bgman else null end as bgman
```

*Confirmed clean (no schema change needed):*

- Boolean fields 28/29 (DO_NOT_DRIVE / PARK_OUTSIDE): only `Yes` / `No` / empty observed
- FMVSS: all values are 0, 2, or 3 chars (length-0 are empty strings); no width drift

**Question 3 (year-band CSV stubs):** Are the small `RCL_FROM_*.zip`
files actual recall data slices or different products?

**Result 3 (corrects an earlier hypothesis):** They are **not** stubs —
they are the **CSV document-attachment index** from a different data
product entirely (see Finding I). Each row is a `(recall × document ×
make/model/year)` tuple, not a recall record. The earlier "stub
hypothesis" (~1.7 KB for 2000-2004 looks suspicious) was correct on the
size observation but wrong on the cause — the file is small because old
recalls have few attached documents, not because it's a placeholder.

This question is therefore moot for Option A: we don't use the year-band
CSVs at all. They're documented under Finding I for completeness.

---

### Finding I — Format heterogeneity (TSV historical vs CSV recent)

> **Status: Confirmed 2026-05-04 via Step 2 download + `unzip -l`
> inspection.**

**Question:** What inner-file formats does NHTSA publish in this directory?
RCL.txt documents only the tab-delimited shape — does the live data match?

**Result:** **Two parallel corpora are published in different formats.**
The directory is heterogeneous, not a single set of size-variant slices.

| File family | Inner extension | Delimiter | Documented in RCL.txt? |
|---|---|---|---|
| `FLAT_RCL_PRE_2010.zip`, `FLAT_RCL_POST_2010.zip` | `.txt` | tab | yes |
| `FLAT_RCL_Annual_Rpts.zip`, `FLAT_RCL_Qrtly_Rpts.zip` | `.txt` (assumed) | tab | yes |
| `RCL_FROM_<startYear>_<endYear>.zip` (all 7 of them) | `.csv` | comma (quoted) | **no** |

**Evidence — selected `unzip -l` output from Step 2 download:**

```
FLAT_RCL_POST_2010.zip:  FLAT_RCL_POST_2010.txt   304,822,880 bytes
FLAT_RCL_PRE_2010.zip:   FLAT_RCL_PRE_2010.txt     83,774,519 bytes
RCL_FROM_2025_2026.zip:  RCL_FROM_2025_2026.csv     1,299,285 bytes
RCL_FROM_2020_2024.zip:  RCL_FROM_2020_2024.csv    67,494,751 bytes
RCL_FROM_2000_2004.zip:  RCL_FROM_2000_2004.csv         1,764 bytes
```

**Sub-question — CSV-vs-TSV column shape:** does the CSV carry the same
29-field schema with a different delimiter, or a structurally divergent
schema?

**Sub-question result (confirmed 2026-05-04 via direct inspection):**
**Structural fork — the two formats are different products, not delimiter
variants.**

| Property | TSV (`FLAT_RCL_POST_2010.txt`) | CSV (`RCL_FROM_2025_2026.csv`) |
|---|---|---|
| Field count | 29 | 6 |
| Header row | none | `"NHTSA ID","DOCUMENT NAME","MAKE","MODEL","MODEL YEAR","SUMMARY"` |
| Row meaning | one row per recall × make × model × year affected | one row per recall × associated PDF document × make × model × year |
| Row count (POST_2010 / 2025_2026) | 240,126 rows | 8,201 rows |
| Carries recall data fields | yes (defect description, manufacturer, classification, dates, etc.) | no |

The CSV is a **document-attachment index**. Each row references an
associated PDF (recall notification letter, dealer service bulletin,
owner letter, etc.) and the vehicles that PDF covers. The `SUMMARY`
field describes the *document*, not the recall — sample rows from the
2000-2004 CSV show `"04014 recall; fuel may leak and may cause engine
fire; owner outreach mailing"` (describing the mailing event), not the
defect details that the TSV's `DESC_DEFECT` field carries.

**Implications:**

- **Architecture decision: Option A (TSV-only) is the only viable
  choice.** The CSV does not contain the recall data fields the
  extractor needs to populate `cpsc_recalls_bronze`'s analog. Options B
  and C (which routed the incremental path through CSV) are eliminated.
- The CSV files are out of scope for the production extractor. They
  remain documented here for completeness — if a future feature wants
  to surface "what supporting documents are attached to this recall,"
  the CSV is where to look. Out of scope for v1.
- The "stub hypothesis" for the small year-band CSVs (1-4 KB) was wrong
  on cause but right on observation: those files are small because old
  recalls have few attached documents, not because they're placeholders.

---

### Finding J — ZIP wrapper bytes are non-deterministic across re-archives

> **Status: Confirmed 2026-05-05 via 24-hour probe diff.** Wrapper-level
> content hashing is unreliable for `*.zip` files. Plain-text wrappers
> (`RCL.txt`, `RCL_*_Rpts.txt`) remain reliable.

**Question:** When NHTSA's daily regen blindly re-uploads a ZIP whose
inner content hasn't changed, do the wrapper bytes stay byte-identical
(deterministic re-archive) or shift (timestamps in ZIP metadata,
non-deterministic compression)?

**Result: wrapper bytes shift every day, even when inner content cannot
have changed.** Across 2026-05-04 → 2026-05-05, multiple historical-only
year-band ZIPs whose inner content is logically frozen produced
different ETags and body_sha256s:

| File | bytes 05-04 | bytes 05-05 | ETag changed? | body_sha256 changed? | Inner content can have changed? |
|---|---|---|---|---|---|
| `RCL_FROM_2000_2004.zip` | 402 | 402 | yes | yes | no — pre-2005 closed window |
| `RCL_FROM_2010_2014.zip` | 114,001 | 114,001 | yes | yes | no — closed window |
| `FLAT_RCL_PRE_2010.zip` | 7,395,562 | **7,378,968** ↓ | yes | yes | no — historical-only archive that *shrank* by 16,594 bytes |

`FLAT_RCL_PRE_2010.zip` is the cleanest proof: a historical-only archive
covering 1966–2009 cannot have grown new records overnight, yet the
wrapper *shrank*. The only consistent explanation is that NHTSA re-zips
every file daily and the resulting wrapper bytes vary because of
embedded ZIP-metadata timestamps and/or non-deterministic compression
choices.

**Implications:**

- **Wrapper-level ADR 0007 dedup will essentially never short-circuit
  for `*.zip` files in this corpus** — every probe will see a fresh
  ETag/body_sha256 for the wrapper. Bronze dedup for ZIPs must hash the
  **decompressed inner content** (`unzip -p` → `sha256sum`) rather than
  the wrapper bytes.
- Plain-text wrappers (`RCL.txt`, `RCL_Annual_Rpts.txt`,
  `RCL_Qtrly_Rpts.txt`) remain deterministic — when the text doesn't
  change, the wrapper bytes don't either. Wrapper-hash dedup works for
  these files.
- `_FlatFileExtractor` should record both wrapper-hash and inner-content
  hash in bronze metadata. The wrapper hash captures "what NHTSA served
  byte-for-byte today" (useful for audit). The inner-content hash drives
  "did anything actually change" gating.
- This finding contradicts an earlier hopeful framing in Finding B
  ("stable-bytes files short-circuit at the wrapper hash") for the ZIP
  case. The Finding B implications have been updated accordingly.

---

### Finding K — RECORD_ID is regenerated per file build (not a stable per-row identifier)

> **Status: Confirmed 2026-05-07 via Step 3 first-extraction analysis.**
> RCL.txt field 1 RECORD_ID is a regen-time row counter, not a stable
> identifier. Architectural response in ADR 0030.

**Question:** Does RCL.txt field 1 (RECORD_ID, *"Running Sequence Number,
Which Uniquely Identifies The Record"*) provide a stable per-row natural
key that survives across NHTSA file regenerations?

**Result: No.** NHTSA reassigns RECORD_ID on each file build. The same
logical row gets a different RECORD_ID across consecutive snapshots, *and*
RECORD_ID values are reused across regenerations to refer to unrelated
recalls.

**Evidence — cross-regen instability** (`scripts/sql/nhtsa/bronze/verify_natural_key_candidate.sql`
Q-A): the Vermeer BC900XL 2019 lug-nut recall (CAMPNO 24V357000), pinned
in both the May 5 and May 7 bronze snapshots:

| Logical row (Vermeer recall) | May 5 RECORD_ID | May 7 RECORD_ID |
|---|---:|---:|
| Lug Nut (`mfr_comp_ptno=131334019`) | 255795 | 267221 |
| Wheel Stud (`131334017`) | 257610 | 267222 |
| Service Manual (`105400DP8`) | 257611 | 267223 |
| Maintenance Manual (`105400DN6`) | 257612 | 267224 |
| Lugs/Nuts/Bolts/Studs (rcl_cmpt_id `…000312`) | 257617 | 267228 |

Same campaign, same vehicle, same component, same part numbers — different
RECORD_ID on each row, every regeneration.

**Evidence — RECORD_ID reuse across regenerations**
(`scripts/sql/nhtsa/bronze/diagnose_full_reinsert.sql` Q4): RECORD_ID
`255795` describes:

- May 5: Vermeer BC900XL 2019, lug-nut defect (CAMPNO `24V357000`)
- May 7: Mercedes-Benz Sprinter 2500 2022, instrument-cluster defect (CAMPNO `24V930000`)

Same RECORD_ID, completely unrelated recalls. The counter resets and gets
reassigned to whatever row happens to occupy that position in the new
file's row order.

**Empirical impact:** the original `identity_fields=("source_recall_id",)`
configuration produced 132,135 bronze rows after two consecutive
`recalls extract nhtsa --since 2024-01-01` runs that should have produced
~66,000. Every prior-run row was treated as a new row because RECORD_ID
had shifted, and the loader's content-hash check (which included
RECORD_ID) saw new hashes for rows the source described identically.

**Why RCL.txt's wording was misleading:** the field description's
*"Running Sequence Number"* prefix is the load-bearing phrase — that means
counter, not stable identifier. The trailing *"Which Uniquely Identifies
The Record"* qualifier refers to within-file uniqueness (one row per
RECORD_ID within a given TSV) and is silent on cross-file stability. The
schema docstring at `src/schemas/nhtsa.py:148` originally read the
qualifier as a stability claim, ignoring the prefix.

**NHTSA's own confirmation:** `documentation/nhtsa/Import_Instructions_Recalls.pdf`
step 17 instructs MS Access importers to *"Let Access add primary key"* —
official documentation that no TSV field is a row-natural primary key.
NHTSA expects importers to synthesize a row identity rather than rely on
field 1.

**Implications:**

- `RECORD_ID` cannot be used as an `identity_fields` component in
  `BronzeLoader`. ADR 0030 specifies a composite 7-tuple identity
  (`campno`, `maketxt`, `modeltxt`, `yeartxt`, `compname`, `rcl_cmpt_id`,
  `mfr_comp_ptno`) as the replacement.
- `RECORD_ID` must also be excluded from the bronze content_hash via
  `hash_exclude_fields={"source_recall_id"}` — otherwise its instability
  would produce a different hash for the same logical row across
  regenerations, making dedup fail in a different way.
- The schema docstring at `src/schemas/nhtsa.py:148` and the extractor
  docstring at `src/extractors/nhtsa.py:414-416` must be rewritten to
  reflect this finding and reference ADR 0030.
- **Cross-source lesson:** future flat-file and HTML-scrape sources
  (Phase 5d USCG, future) must empirically verify field-stability claims
  across at least two regenerations before trusting them. Source-published
  field descriptions can mislead.

---

### Finding L — TSV ships byte-duplicate rows for some recalls

> **Status: Confirmed 2026-05-07 via bronze SQL analysis + raw-TSV
> verification against the May 7 R2 wrapper. Inner-TSV SHA `c955c37153d1…`
> matches `extraction_runs.response_inner_content_sha256` for the May 7
> extract, so the verification covers the exact bytes that produced
> bronze. Updated 2026-05-08 with full-POST_2010-corpus measurements
> from `scripts/nhtsa/tsv_analysis/identity_search.py` — the bronze-narrow
> diagnostics underestimated the residue (the bronze data was
> `--since 2024-01-01` filtered, but the deep-rescan path needs the full
> corpus). Corpus-wide: 1,805 collision groups on the original ADR-0030
> 7-tuple, of which 822 are anomalies (rows differ on a non-RECORD_ID
> field) and 983 are byte-identical. The 4 additional fields needed to
> reach 0 anomalies (`mfr_comp_desc`, `mfr_comp_name`, `endman`, `bgman`)
> drove the 7→11 tuple amendment in ADR 0030.**

**Question:** Does NHTSA's TSV ever emit multiple rows that carry
identical content for the same logical (recall × vehicle × component ×
part) tuple?

**Result: Yes.** Within a single TSV regeneration, ~0.7% of rows are
byte-identical to at least one other row in the file (modulo RECORD_ID,
which differs per row by the Finding K mechanism). NHTSA's TSV format
does not enforce row-uniqueness on the data axis.

**Evidence — bronze-side measurement (post-2024 slice)**
(`scripts/sql/nhtsa/bronze/investigate_residual_collisions.sql` Q3, May 7
snapshot, `--since 2024-01-01`):

| Pattern | Collision groups | Total colliding rows | Excess rows |
|---|---:|---:|---:|
| Populated `mfr_comp_ptno` | 337 | 728 | 391 |
| Empty `mfr_comp_ptno` | 57 | 143 | 86 |
| **Total** | **394** | **871** | **477** |

477 out of 66,078 rows in the post-2024 slice (~0.7%) are duplicates of
another row on the original 7-tuple
`(campno, maketxt, modeltxt, yeartxt, compname, rcl_cmpt_id, mfr_comp_ptno)`.

**Evidence — TSV-level measurement (full POST_2010 corpus, added 2026-05-08)**
(`scripts/nhtsa/tsv_analysis/identity_search.py`, both May 7 SHA `c955c37`
and older SHA `f11119e` regenerations):

| Tuple shape | Collisions | Byte-identical | Anomalies |
|---|---:|---:|---:|
| 7-tuple (original ADR 0030) | 1,805 | 983 | 822 |
| + `mfr_comp_desc` (8-tuple) | 1,132 | 987 | 145 |
| + `mfr_comp_name` (9-tuple) | 1,004 | 987 | 17 |
| + `endman` (10-tuple) | 994 | 987 | 7 |
| + `bgman` (11-tuple, final) | 987 | 987 | 0 |

The 11-tuple is row-unique on POST_2010 (240,158 rows → 239,036 distinct
identities → 1,122 byte-duplicate rows in 987 groups, all collapsed by
within_batch_dedup). The bronze-narrow analysis missed the 822
populated-`mfr_comp_ptno` anomalies because they're spread across the
full corpus (many predate the post-2024 slice). PRE_2010 has zero
collisions at any tuple width — the 4 added fields are constant-empty
for pre-2010 rows, so the wider tuple is harmless there. ADR 0030's
amendment-on-2026-05-08 captures the 7→11 widening rationale.

**Evidence — column-by-column distinct-count** within representative
collision sets (`investigate_residual_collisions.sql` Q1 and Q2):

- NISSAN TITAN 2021 air bag recall (4-row collision): every column has
  `distinct_count=1` except `source_recall_id` (= 4) and `content_hash`
  (= 4, derived from source_recall_id since RECORD_ID was in the hash
  input). Same campaign, same vehicle, same component, same narrative
  text, same dates, same everything else.
- ACHILLES ATR SPORT 2 tire recall (5-row empty-`mfr_comp_ptno` collision):
  identical pattern — every column constant except the regen-unstable
  RECORD_ID.

**Evidence — raw-TSV byte verification** (`scripts/nhtsa/verify_collisions_raw_tsv.sh`
against the May 7 R2 wrapper at
`nhtsa/2026-05-07/2180b301-844c-4ab2-9fb1-98848642a57f.zip.gz`):

- Inner-TSV SHA-256 `c955c37153d1…` matches
  `extraction_runs.response_inner_content_sha256` for the May 7 extract,
  confirming we tested the exact bytes that produced bronze.
- 4 NISSAN raw TSV lines collapse to 1 unique line after stripping field 1
  (RECORD_ID). 5 ACHILLES raw TSV lines collapse to 1 unique line under
  the same operation.

The duplicates are NHTSA's, not parsing artifacts — bronze faithfully
preserves what the source served (per ADR 0027).

**NHTSA's own acknowledgment:** the same `Import_Instructions_Recalls.pdf`
step 17 referenced in Finding K — *"Let Access add primary key"* — is
also implicit acknowledgment of this pattern. If the TSV were row-unique
on its data fields, NHTSA could direct importers to choose a natural
primary key. They don't; they direct importers to synthesize one. The
byte-duplicate pattern is a known consequence of NHTSA's denormalized TSV
export from their internal relational database.

**Pattern interpretation:** the byte-duplicates likely arise from joins
in NHTSA's internal export query. A recall row like `Vermeer BC900XL 2019
× lug-nut` may join to multiple internal table rows (e.g., manufacturing
plant codes, DOT batch identifiers, owner-mailing tier records) that are
not surfaced as TSV columns. The join multiplies the recall row by
however many internal rows match, but the surfaced TSV columns are
identical because the joined-but-not-surfaced dimension doesn't appear
in any of the 29 fields.

**Implications:**

- A within-batch dedup step in `BronzeLoader._dedup_within_batch()` is
  required. Without it, the loader's existing-hash check (against bronze,
  not within-batch) would insert all duplicates on first extract.
  Combined with Finding K's `hash_exclude_fields={"source_recall_id"}`,
  the dedup groups records by `(identity_tuple, content_hash)` after
  RECORD_ID-aware hashing and emits one record per group.
- The **11-tuple identity** (per ADR 0030 amendment) is needed for the
  full corpus, not just the post-2024 slice. The bronze-narrow analysis
  found 477 collisions of which spot-checked samples were byte-identical;
  TSV-level analysis revealed 1,805 collisions of which 822 are
  legitimately-different rows that need additional fields
  (`mfr_comp_desc`, `mfr_comp_name`, `endman`, `bgman`) to disambiguate.
- **Loader implementation surfaced two SQL-level issues** during the
  rollout (both fixed in `BronzeLoader._fetch_existing_hashes`):
  TIMESTAMPTZ columns in identity_fields can't accept empty-string bind
  parameters → text-canonical IN comparison via `_identity_text_expr`;
  composite IN clauses on 65k+ rows exceed Postgres' bind-parameter
  ceiling and stress planner memory → chunked existing-hash lookup at
  ~5,400 keys per query. Both apply to any future source with typed
  identity columns or large extraction batches. See ADR 0030
  Implementation section.
- **Silver consequence:** silver staging models can rely on bronze
  representing one row per logical fact. The dedup work happens at
  extract; silver doesn't repeat it.
- **Cross-source lesson:** future flat-file sources may exhibit the same
  pattern. The TSV-level analysis suite at `scripts/nhtsa/tsv_analysis/`
  generalizes to any tab-delimited source — `identity_search.py` is the
  load-bearing tool for finding the minimum row-unique identity tuple
  empirically rather than guessing from documentation.

---

## Open items

None of these gate Step 2. Each closes during or after Step 2/3 work as
a side-effect of writing the extractor or running it against live data.

- **Finding H update-cadence sub-question:** deferred to Step 3 as a
  side-effect of `_FlatFileExtractor` logging `inner_content_sha256` to
  `extraction_runs`. Day-over-day transitions on that column produce
  the cadence verdict; no probe-script change needed. Write a small
  follow-up edit ~7 days after first extraction.
- **Finding E follow-up — embedded newlines/tabs in description fields:**
  the 6,000-char free-text fields could plausibly contain literal
  newlines or tabs. Defer to the cassette suite (Step 4) — if naïve
  line-by-line / column-split parsing breaks, the failing test surfaces
  it against real data.
- ~~**Finding H follow-up — sentinel-date discovery in other fields:**~~
  **Closed 2026-05-05** via `scripts/nhtsa/probe_date_sentinels.sh`.
  Surfaced one Step-2 blocker (RCDATE 5 empty rows — schema relaxed to
  nullable + migration column relaxed) and three new silver-staging
  sentinels (ODATE `11111111`, BGMAN `19000101`, ENDMAN `19000101`).
  Plus identified wild manufacturing dates in POST_2010 (year 22 to
  year 3019) that pass `strptime` shape validation and need a silver
  date-sanity filter. Full results documented in Finding H above.

## Evidence

- **Probe script (Step 1):** `scripts/nhtsa/probe_watermarks.sh`
- **Probe data (committed):** `documentation/nhtsa/watermark_probes.jsonl`
- **Download script (Step 1):** `scripts/nhtsa/download_archives.sh`
- **Step 2 download artifacts (gitignored):** `data/exploratory/nhtsa/`
- **Data dictionary:** `documentation/nhtsa/RCL.txt`
- **Source directory listing (manual capture):** referenced in conversation
  notes 2026-05-04; not committed.
- **Step 3 bronze diagnostic SQL** (used in Findings K and L):
  - `scripts/sql/nhtsa/bronze/diagnose_full_reinsert.sql` — surfaced the
    66,078-row re-insert + RECORD_ID reuse across regenerations.
  - `scripts/sql/nhtsa/bronze/verify_natural_key_candidate.sql` — Vermeer
    cross-regen RECORD_ID table; rcl_cmpt_id within-snapshot non-uniqueness.
  - `scripts/sql/nhtsa/bronze/find_row_differentiator.sql` — initial
    column-by-column distinct-count diagnostic (Vermeer + tire cases).
  - `scripts/sql/nhtsa/bronze/verify_six_tuple_identity.sql` — mfr_comp_ptno
    set-equality test across regenerations; null-rate per rcltype + year.
  - `scripts/sql/nhtsa/bronze/investigate_tire_collision.sql` — rcl_cmpt_id
    set-equality stability test; 7-tuple uniqueness measure (65,601 / 66,078).
  - `scripts/sql/nhtsa/bronze/investigate_residual_collisions.sql` — Q1/Q2
    column-by-column proof that NISSAN/ACHILLES collisions are byte-identical
    except RECORD_ID; Q3 population breakdown of the 477-row residue.
- **Step 3 raw-TSV verification:** `scripts/nhtsa/verify_collisions_raw_tsv.sh`
  — runs against the May 7 R2 wrapper (`nhtsa/2026-05-07/2180b301-844c-4ab2-9fb1-98848642a57f.zip.gz`)
  and confirms the bronze duplicates are NHTSA-shipped, not parsing artifacts.
- **TSV-level analysis suite (added 2026-05-08):**
  `scripts/nhtsa/tsv_analysis/` — pure-Python tools that operate on the
  raw zipped TSV rather than bronze:
  - `_lib.py` — TSV streaming, FIELD_NAMES lookup, group-by, differing-fields helpers.
  - `identity_search.py` — iterative widening that surfaced the
    POST_2010 822-anomaly residue and converged on the 11-tuple identity.
  - `uniqueness_at_tuple.py` — single-tuple uniqueness check for ad-hoc
    spot-checks against any chosen group key.
  - `find_differentiator.py` — column-by-column distinct-count for
    chosen group keys with optional row filter; replaces the one-off
    `verify_empty_ptno_groups_byte_identical.py` and generalizes
    `investigate_residual_collisions.sql` Q1/Q2.
- **End-to-end validation:**
  `scripts/sql/nhtsa/bronze/verify_eleven_tuple_row_unique.sql` — confirms
  the 11-tuple is row-unique on bronze post-extraction (`excess_rows = 0`),
  and dedup-on-rerun works (re-extract against existing bronze inserts
  only genuinely-new records).
- **Architectural response:**
  `documentation/decisions/0030-nhtsa-bronze-identity-composite-tuple-and-within-batch-dedup.md`
  (initial 7-tuple 2026-05-07; amended to 11-tuple 2026-05-08).

## References

- `project_scope/implementation_plan.md` Phase 5c (NHTSA flat-file)
- `project_scope/implementation_plan.md` lines 145–152 (incremental vs.
  historical load-path standing requirement)
- ADR 0007 (bronze content hashing — fallback if Findings B/C disqualify ETag/LM)
- ADR 0010 (deep-rescan workflows — historical seeding mechanism)
- ADR 0014 (`extra='forbid'` Pydantic strict mode)
- ADR 0027 (bronze storage-forced transforms only — basis for "bronze
  faithfully preserves NHTSA's TSV including duplicates")
- ADR 0030 (NHTSA bronze identity: composite tuple + within-batch dedup —
  architectural response to Findings K and L)
- `documentation/usda/recall_api_observations.md` (sibling source's
  observations doc, structural template for this one)
- `documentation/cpsc/last_publish_date_semantics.md` (sibling watermark
  verdict doc, model for what Finding B will look like once resolved)
