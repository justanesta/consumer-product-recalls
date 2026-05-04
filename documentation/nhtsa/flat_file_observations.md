# NHTSA Flat-File Source — Empirical Observations

> **Status: Exploration in progress (Phase 5c Step 1).** Finding A confirmed
> 2026-05-04. Findings B–C gated on the multi-day watermark study now running
> via `scripts/nhtsa/probe_watermarks.sh`. Findings D–H pending probe.
> Evidence accumulates in `documentation/nhtsa/watermark_probes.jsonl`.

## Background

NHTSA publishes safety-related recall data as flat files served from
`https://static.nhtsa.gov/odi/ffdd/rcl/`. The directory contains a
documentation PDF, an inline data dictionary (`RCL.txt`), and a set of
TSV-inside-ZIP archives organized by year band:

- `FLAT_RCL_PRE_2010.zip` (~7 MB) — historical 1967–2009
- `FLAT_RCL_POST_2010.zip` (~14 MB) — historical 2010–present
- `RCL_FROM_<YYYY>_<YYYY>.zip` — year-band slices, sizes from 1 KB to 6 MB
- `FLAT_RCL_Annual_Rpts.zip` / `FLAT_RCL_Qrtly_Rpts.zip` — periodic rollups

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

## Source mapping (working hypothesis)

The eventual `NhtsaExtractor` will need both code paths required by the
plan's standing architectural requirement (lines 145–152):

| Path | Candidate URL | Size | Confirmed |
|---|---|---|---|
| Incremental | `RCL_FROM_2025_2026.zip` | ~90 KB | Pending Finding D |
| Historical seed | `FLAT_RCL_POST_2010.zip` + `FLAT_RCL_PRE_2010.zip` | ~21 MB total | Pending Finding G |

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

> **Status: Pending.** Multi-day probe running since 2026-05-04. Verdict
> requires ≥7 daily probes, ideally bracketing one observed real upstream
> content update.

**Question:** Does NHTSA's `Last-Modified` header track real content
changes, or is it re-stamped daily by a regeneration job regardless of
content?

**Hypothesis (informed by single-probe observation):** Every data file in
the directory shares `Last-Modified: Mon, 04 May 2026 07:04:23 GMT` while
`Import_Instructions_Recalls.pdf` retains its true 2023-10-27 stamp. This
suggests the regen job selectively re-stamps data files but preserves
documentation PDFs. If the multi-day study confirms data-file
`Last-Modified` advances daily while ETag and body sha256 remain stable,
`Last-Modified` is unreliable for `If-Modified-Since` conditional GETs.

**Method:** Run `scripts/nhtsa/probe_watermarks.sh` daily for ≥7 days.
Then run analysis snippets at the bottom of the script to compare:

- distinct `last_modified` values per file
- distinct `etag` values per file
- distinct `body_sha256` values per file

If `etag` distinct count = 1 but `last_modified` distinct count > 1 for
a given file, regen-stamp confirmed.

**Result:** Pending.

**Implications (when resolved):**

- If `Last-Modified` is content-blind: drop conditional-GET via
  `If-Modified-Since`; rely on bronze content-hash dedup (ADR 0007).
- If `Last-Modified` is content-bound: viable as a watermark surface,
  reducing per-run bandwidth.

---

### Finding C — `x-amz-version-id` behavior

> **Status: Pending.** Capture added to probe script 2026-05-04. Verdict
> alongside Finding B.

**Question:** Does NHTSA's S3 mint a new version ID on every regen
(blind re-upload) or only on real content change?

**Why it matters:** `x-amz-version-id` is the cleanest possible "did
NHTSA upload today?" signal. It's set by S3 on every PUT regardless of
content. Combined with `body_sha256`:

| `x-amz-version-id` | `body_sha256` | Interpretation |
|---|---|---|
| changes daily | stable | Regen blindly re-uploads identical bytes |
| changes daily | changes | Real content update |
| stable | stable | No upload, file genuinely static |
| stable | changes | Impossible (S3 invariant) |

**Method:** Multi-day probe data already capturing the field. Analysis
TBD.

**Result:** Pending.

---

### Finding D — Year-band URL pattern (2025_2025 vs 2025_2026)

> **Status: Pending.** Inspection probe to be run before extractor work.

**Question:** What is the naming convention for `RCL_FROM_<YYYY>_<YYYY>.zip`,
and which file is the "current rolling window" that `NhtsaExtractor` should
hit incrementally?

**Observed shape:** Both `RCL_FROM_2025_2025.zip` (89 KB) and
`RCL_FROM_2025_2026.zip` (90 KB) coexist in the directory. Plausible
explanations:

- 2025_2025 = "calendar year 2025 only" (frozen snapshot once 2025 closed),
  2025_2026 = rolling window through current year.
- 2025_2025 is the previous current-window file that NHTSA hasn't
  deprecated.

**Method:**

```bash
cd data/exploratory/nhtsa/
curl -O https://static.nhtsa.gov/odi/ffdd/rcl/RCL_FROM_2025_2025.zip
curl -O https://static.nhtsa.gov/odi/ffdd/rcl/RCL_FROM_2025_2026.zip
unzip -p RCL_FROM_2025_2025.zip <inner.txt> | wc -l
unzip -p RCL_FROM_2025_2026.zip <inner.txt> | wc -l
unzip -p RCL_FROM_2025_2025.zip <inner.txt> | awk -F'\t' '{print $17}' | sort -u | tail
unzip -p RCL_FROM_2025_2026.zip <inner.txt> | awk -F'\t' '{print $17}' | sort -u | tail
```

(Field 17 is `DATEA`, record creation date YYYYMMDD per RCL.txt.)

**Result:** Pending.

**Implications (when resolved):**

- Confirms whether `config/sources/nhtsa.yaml` can hardcode the URL or
  needs to template it by date (e.g., `RCL_FROM_<prev>_<current>.zip`).

---

### Finding E — TSV column count and encoding

> **Status: Pending.** File inspection to be run before extractor work.

**Question:** Does the live file match RCL.txt's documented 29-field
shape? What text encoding is used (UTF-8, Windows-1252)?

**Method:**

```bash
unzip -p FLAT_RCL_POST_2010.zip <inner.txt> | head -1 | awk -F'\t' '{print NF}'
unzip -p FLAT_RCL_POST_2010.zip <inner.txt> | head -c 4096 | file -
unzip -p FLAT_RCL_POST_2010.zip <inner.txt> | iconv -f UTF-8 -t UTF-8 >/dev/null
unzip -p FLAT_RCL_POST_2010.zip <inner.txt> | iconv -f CP1252 -t UTF-8 >/dev/null
```

**Result:** Pending.

**Implications (when resolved):**

- Encoding determines `_FlatFileExtractor` decode strategy.
- Column count divergence from RCL.txt's 29 would indicate undocumented
  schema drift since the May 2025 RCL.txt update.

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

> **Status: Pending.** Update-cadence verdict alongside Finding B.
> Historical-coverage probe alongside Finding D/E.

**Question 1 (update cadence):** How often does NHTSA actually publish
new content vs re-stamp idle data? Daily? Weekly? In bursts?

**Method:** After multi-day probe accumulates, plot `body_sha256`
change events per file. The interval between content changes is the
true publication cadence.

**Question 2 (historical coverage):** Does `FLAT_RCL_PRE_2010.zip` cover
the full 1967–2009 range RCL.txt claims, or does it have its own floor?
Are the small `RCL_FROM_2000_2004.zip` (1 KB) and `RCL_FROM_2005_2009.zip`
(4 KB) files real data slices or stubs?

**Method:**

```bash
unzip -p FLAT_RCL_PRE_2010.zip <inner.txt> | awk -F'\t' '{print $17}' | sort -u | head
unzip -p FLAT_RCL_PRE_2010.zip <inner.txt> | wc -l
unzip -p RCL_FROM_2000_2004.zip <inner.txt> | wc -l
unzip -p RCL_FROM_2005_2009.zip <inner.txt> | wc -l
```

**Result:** Pending.

---

## Open items

- **Finding B/C verdict:** awaiting ≥7-day probe accumulation, ideally bracketing one real upstream update.
- **Finding D resolution:** inspect `RCL_FROM_2025_2025.zip` vs `RCL_FROM_2025_2026.zip` for overlap and naming pattern.
- **Finding E resolution:** download a representative archive, verify column count and encoding.
- **Finding H resolution:** verify pre-2010 archive coverage; investigate small year-band files.

## Evidence

- **Probe script:** `scripts/nhtsa/probe_watermarks.sh`
- **Probe data (committed):** `documentation/nhtsa/watermark_probes.jsonl`
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
