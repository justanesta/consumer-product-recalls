# NHTSA Flat-File Source — Empirical Observations

> **Status: Step 1 complete (2026-05-05).**
> Findings A, B, C, D, E, F, G, I confirmed 2026-05-04 / 2026-05-05.
> Finding J (ZIP wrapper non-determinism) added 2026-05-05.
> Architecture decision **resolved as Option A (TSV-only)** after Finding I
> revealed CSV files are a structurally divergent document-attachment index
> rather than recall data. Finding H's update-cadence sub-question is
> **deferred** — it closes implicitly once `_FlatFileExtractor` lands in
> Step 2 and starts logging per-run inner-content SHA-256 to
> `extraction_runs`. None of the open items gate Step 2.
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
| Line terminator | **CRLF** (Windows-style; `file` heuristic reports CRLF) |
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
> deferred 2026-05-05 — closes implicitly once `_FlatFileExtractor`
> lands in Step 2 with inner-content SHA-256 logging.**

**Question 1 (update cadence):** How often does NHTSA actually publish
new content vs re-stamp idle data? Daily? Weekly? In bursts?

**Result 1:** Deferred. The wrapper-level watermark probe cannot answer
this — per Finding J, ZIP wrapper bytes shift every day regardless of
inner content, so wrapper-level diffs can't separate "real new recall
records" from "non-deterministic re-zip of identical inner content."

Closing the question requires inner-content hashing across days, which
is *exactly* the primitive `_FlatFileExtractor` will run on every
incremental fetch (per Finding J's mandate that ZIP dedup must operate
on decompressed inner content). Once Step 2 lands and the extractor
starts logging `inner_content_sha256` to `extraction_runs`, day-over-day
diffs on that column close this question as a free side-effect of
production runs — no separate probe extension needed.

The cadence answer is **operational characterization**, not a Step 2/3
design input: the schema, extractor lifecycle, watermark strategy, and
bronze migration shape are fully specified by Findings A–G, I, and J
regardless of whether NHTSA actually updates content daily, weekly, or
in bursts. Daily cron is the right cadence either way (ADR 0007's
content-hash dedup absorbs no-op days at near-zero cost). The eventual
cadence verdict primarily informs Phase 7 monitoring/alerting baselines
and a portfolio-narrative bandwidth-vs-staleness tradeoff note — neither
of which is on the Step 2 critical path.

**Closure target:** ~7 days after Step 3's first extraction, write the
verdict as a small follow-up edit referencing `extraction_runs`
inner-hash transition data. No probe-script change required.

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
- `ODATE` uses **`19010101` as an unknown-date sentinel.** Bronze
  preserves the literal value per ADR 0027; `stg_nhtsa_recalls.sql` maps
  `19010101` → NULL during silver normalization.
- Other date fields likely have their own sentinels — worth probing
  systematically before locking the schema. Check `BGMAN`, `ENDMAN`,
  `RCDATE` for analogous outliers (`19010101`, `99999999`, all-zeros).
- The PRE_2010 archive can contain records with ODATE values past 2010
  (one record has 2012-04-24). The archive partition is by DATEA, not by
  any other date field. Don't assume "PRE_2010 → all dates < 2010."

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
- **Finding H follow-up — sentinel-date discovery in other fields:**
  ODATE confirmed to use `19010101` as an unknown-date sentinel. Worth a
  systematic probe of `BGMAN`, `ENDMAN`, `RCDATE` for analogous outliers
  before locking the Pydantic schema. Quick check (run during Step 2
  schema design, not before):
  ```bash
  for field in 9 10 13 16 17; do
    echo "field $field min:"
    unzip -p data/exploratory/nhtsa/FLAT_RCL_PRE_2010.zip '*.txt' \
      | awk -F'\t' -v f=$field '$f != "" {print $f}' | sort -u | head -3
  done
  ```

## Evidence

- **Probe script:** `scripts/nhtsa/probe_watermarks.sh`
- **Probe data (committed):** `documentation/nhtsa/watermark_probes.jsonl`
- **Download script:** `scripts/nhtsa/download_archives.sh`
- **Step 2 download artifacts (gitignored):** `data/exploratory/nhtsa/`
- **Data dictionary:** `documentation/nhtsa/RCL.txt`
- **Source directory listing (manual capture):** referenced in conversation
  notes 2026-05-04; not committed.

## References

- `project_scope/implementation_plan.md` Phase 5c (NHTSA flat-file)
- `project_scope/implementation_plan.md` lines 145–152 (incremental vs.
  historical load-path standing requirement)
- ADR 0007 (bronze content hashing — fallback if Findings B/C disqualify ETag/LM)
- ADR 0010 (deep-rescan workflows — historical seeding mechanism)
- ADR 0014 (`extra='forbid'` Pydantic strict mode)
- `documentation/usda/recall_api_observations.md` (sibling source's
  observations doc, structural template for this one)
- `documentation/cpsc/last_publish_date_semantics.md` (sibling watermark
  verdict doc, model for what Finding B will look like once resolved)
