# FDA iRES API — Empirical Observations

**Investigation date:** 2026-04-26
**Data basis:** Interactive Bruno exploration of the iRES API against three endpoints (`GET /search/producttypes`, `GET /recalls/event/{eventid}`, `POST /recalls/`) using a real `Authorization-User` / `Authorization-Key` credential pair.
**Scope:** Behaviors and shapes that diverge from — or are not documented in — the iRES API Usage PDF and the Enforcement Report API Definitions PDF. These observations directly inform the bronze Pydantic schema, extractor pagination logic, and VCR cassette matcher design in Phase 5a.
**Canonical source per finding:** Each finding cross-references the `.bru.yml` Bruno request file whose `docs:` block contains the test-level reproduction. This document consolidates; the Bruno files are the executable proof.

---

## Background

ADR 0010 specifies FDA iRES as a daily-cron source using `eventlmd >= yesterday` as the incremental watermark. ADR 0007 specifies bronze content-hashing for idempotent re-ingestion across all sources, with FDA additionally backed by native field-level history endpoints (`/search/producthistory/{productid}`, `/search/eventproducthistory/{eventid}`).

Before writing the Pydantic bronze schema, extractor, or VCR cassettes for Phase 5a, the plan called for empirical exploration of:

1. Whether the iRES API behaves as the PDFs describe.
2. The actual response shape and field types so the bronze schema can target the live API rather than the docs.
3. Pagination and filter semantics for the bulk POST endpoint, since that is what the production extractor will call.

Findings A–C surfaced from the simplest endpoint (`GET /search/producttypes`); D–I from the bulk POST (`POST /recalls/`); J from the first single-record lookup (`GET /recalls/event/{eventid}`). Subsequent endpoint exploration may extend the list.

---

## Universal API quirks (apply to every iRES request)

These four behaviors are documented in the PDF but easy to miss; they apply to every endpoint and must be implemented once in the extractor base class.

### 1. Authentication via headers, not query params

`Authorization-User` and `Authorization-Key` are HTTP request headers, not query string parameters. Credentials are issued via the OII Unified Logon application and revoked there.

### 2. `STATUSCODE` of 400 means SUCCESS

The HTTP status will be 200 on a successful request, but the API-level outcome lives in a `STATUSCODE` field in the response body:

| `STATUSCODE` | Meaning |
|---|---|
| 400 | Success |
| 401 | Authorization denied |
| 402–418 | Various payload / parameter errors (see Response Messages tables in the iRES Usage PDF) |
| 421 | Path parameter not numeric (e.g., quoted string passed as `eventid`) |

Note: the PDF's "Response Class" examples for every endpoint show this field as `APIRETURNCODE`. The actual API returns it as `STATUSCODE`. See finding A.

### 3. Signature cache-busting is mandatory

The iRES server caches responses keyed by the full request URL, including the `signature=` parameter. Without a unique value:

- A 401 returned for a bad credential is **cached** and replayed even after credentials are fixed
- Repeated polls within the cache TTL return stale results
- Test/debug iteration cycles can return last-call's data instead of reflecting parameter changes

Bruno requests use `{{$timestamp}}` (a runtime variable resolving to the current Unix epoch). Production extractor should inject `int(time.time())` or `uuid.uuid4()` into every request URL.

**Implication for VCR cassettes:** the recorded `signature=` value will never match the timestamp generated at replay time. A custom matcher that strips `signature` from the query string before matching is required for FDA cassettes only — captured as a deliverable in `project_scope/implementation_plan.md` Phase 5a.

**No HTTP cache validators (verified 2026-05-10).** As an explicit corollary of "caching is API-internal, keyed by request URL + signature": the iRES server emits no `ETag`, no `Last-Modified`, and no `Cache-Control` on responses. Confirmed via grep of all FDA cassettes under `tests/fixtures/cassettes/fda/` (zero matches for `etag|cache-control|last-modified|expires`) and via the `bruno/fda/lookup/get_product_types.yml:131` exploration note. The conditional-GET optimization (`If-None-Match` / `If-Modified-Since`) that USDA uses is structurally unavailable for FDA on three independent grounds: (1) no validators emitted, (2) the `signature=` query parameter forces a unique URL per request which would defeat any URL-keyed cache, and (3) the bulk endpoint takes POST, not GET, so HTTP/1.1 conditional-request semantics don't apply. The `extraction_runs.response_etag` column will remain NULL for `source='fda'` by design — that's the correct state, not a wiring bug. The capture infrastructure stays valuable for the other forensic columns (`response_status_code`, `response_body_sha256`, `response_headers` JSONB) which do populate.

### 4. Base URL is `/rest/iresapi/`

The interactive docs page lives at `/scripts/ires/apidocs/`, which is **not** the API host. The actual API endpoints live under `/rest/iresapi/` per the footer of the iRES API Usage PDF (`api version: 2.0.0`).

---

## Docs-vs-actual discrepancies

The PDFs are stale or imprecise on the following points. The bronze Pydantic schema and parsers must target the actual API behavior, not the documented behavior.

### A. Success-code field is `STATUSCODE`, not `APIRETURNCODE`

Every "Response Class" example in the iRES Usage PDF — for every endpoint, all 9 — shows the field as `APIRETURNCODE`. The live API returns `STATUSCODE`. Treat the PDF as stale on this point and use `STATUSCODE` everywhere in code.

### B. RESULT keys and column names are UPPERCASE

The Implementation Notes describe columns in lowercase prose (e.g., "centercd, producttypeshorttxt"), but the API returns them as uppercase (`CENTERCD`, `PRODUCTTYPESHORTTXT`). Pydantic schemas should either lowercase keys at validation time or use case-insensitive aliases.

### C. `MESSAGE` is the literal string `"success"` on success

The PDF shows `MESSAGE: "string"` as a placeholder; in practice, a successful response always has `MESSAGE: "success"`. Failure responses contain a human-readable error string (e.g., `"Event Id provided should be numeric."` for STATUSCODE 421).

### D. Response shape for `POST /recalls/` is OBJECT-ARRAY, not COLUMNAR

Every "Response Class" example in the PDF — for every endpoint, including the bulk POST — shows the same columnar envelope:

```json
{ "RESULT": { "COLUMNS": ["..."], "DATA": [["..."]] } }
```

The actual `POST /recalls/` response is fundamentally different:

```json
{ "RESULT": [ {"KEY": "value", ...}, ... ] }
```

i.e., `RESULT` is an array of objects keyed by uppercase column name. The lookup endpoints (`GET /search/producttypes`, `GET /recalls/event/{eventid}`) DO return the documented columnar shape.

**Working hypothesis** (to validate as more endpoints are exercised): GET endpoints return columnar; the bulk POST returns object-array. See finding J for the second data point and the open question.

### E. `RESULTCOUNT` is the total matching dataset, not the rows returned

A request with `rows: 5` returns `DATA` of length 5 but `RESULTCOUNT` in the tens of thousands. The PDF's terse `"RESULTCOUNT": 0` placeholder in every Response Class doesn't make this clear. Confirmed empirically:

| Filter | RESULTCOUNT | Rows returned |
|---|---|---|
| `[]` (none) | 133,841 | 5 (or whatever `rows` is set to) |
| `eventlmdfrom: 01/01/2026` | 3,012 | 5 |
| `eventlmdfrom: 02/01/2026, eventlmdto: 02/28/2026` | 833 | 5 |

### F. `RID` is auto-injected with deterministic-but-opaque tiebreaker behavior

`RID` is listed in the PDF as a possible value in `displaycolumns` but is always returned regardless. Two empirical observations:

- The first row's `RID` equals the `start` parameter exactly (`start: 1000` → first row `RID: 1000`). So `RID` is the position in the server-side sorted resultset.
- Within a page, `RID` is *usually* sequential with array order, BUT multi-product events under the same `RECALLEVENTID` can produce out-of-order `RID`s. Example (rows=50, sort=recalleventid desc), reproduced identically across unfiltered and date-filtered runs:
  - `recalleventid 98724` (3 products): `RIDs 20, 22, 21` (array order)
  - `recalleventid 98670` (3 products): `RIDs 46, 48, 47` (array order)
- The behavior is deterministic per `RECALLEVENTID` group (same input → same output across runs). The actual tiebreaker rule is opaque: not productid asc, not RID asc, not RID desc.

**Implication for the extractor:** do not use `RID` as a pagination cursor — out-of-order RIDs can straddle page boundaries. Use the PDF-prescribed `start` offset, which is 1-based, inclusive, and behaves deterministically.

### G. `start` is 1-based and inclusive

A request with `start: 1000` returns rows whose first `RID` is 1000. Pagination math: `start_for_page_N = (N - 1) * rows + 1`. Extractor pagination loop:

```python
start = 1
while True:
    response = post(..., payload={..., "start": start, "rows": rows, ...})
    yield from response["RESULT"]
    if len(response["RESULT"]) < rows:
        break
    start += rows
```

### H. Date fields use `MM/DD/YYYY` format with no time component

The PDF doesn't specify a format. Empirically, every date field in the response (`RECALLINITIATIONDT`, `CENTERCLASSIFICATIONDT`, `DETERMINATIONDT`, `ENFORCEMENTREPORTDT`, `CREATEDT`, `EVENTLMD`, `PRODUCTLMD`, `TERMINATIONDT` when non-null) uses `MM/DD/YYYY`. The bronze Pydantic schema should parse with an explicit format and normalize to UTC datetime per ADR 0007's canonical serialization rules.

ADR 0007 / ADR 0010 reference these last-modified columns with a `dt` suffix (`eventlmddt`, `productlmddt`); the actual API returns them without the suffix as `EVENTLMD` and `PRODUCTLMD`. Confirmed across both columns:

| ADR reference | Actual API column | Confirmed via |
|---|---|---|
| `eventlmddt` | `EVENTLMD` | `get_event_by_id.yml` (event detail), `post_recalls_seed_event_ids.yml` (bulk) |
| `productlmddt` | `PRODUCTLMD` | `get_product_by_id.yml` (product detail) |

Update both ADRs to drop the `dt` suffix before bronze schema implementation.

Additional observation about edit-timestamp semantics: in `get_product_by_id.yml`, `PRODUCTLMD` was `null` for product 219875 even though the product has a recent recall (initiated 2026-04-21, classified 2026-04-24). This is consistent with FDA's documented claim that the `*lmd` columns advance **on edits only**, not on initial creation — un-edited records have `null`. This means the field can serve as a useful signal for content-hash dedup: a non-null `PRODUCTLMD` / `EVENTLMD` value indicates the record has been edited since publication, and the timestamp itself moves forward on each edit. (Empirical verification of "moves forward on each edit" is the eventual job of the field-history endpoints; see Open items.)

### I. Filter syntax works as the PDF prescribes (single and compound forms confirmed)

Filter date format is `MM/DD/YYYY` (matches the PDF's example payloads). Single-quoted JSON-ish keys/values inside an array of objects is the correct shape.

```text
single: "filter":"[{'eventlmdfrom':'01/01/2026'}]"
compound: "filter":"[{'eventlmdfrom':'02/01/2026'},{'eventlmdto':'02/28/2026'}]"
```

The compound form correctly intersects bounds. All EVENTLMD values in filtered responses fell within the requested window. The mixed quoting (double-quoted JSON with single-quoted internal filter strings) creates non-trivial multi-layer escaping when stored in YAML — see TODO.md blog post item.

**Implication for the extractor:**
- Daily incremental: `eventlmdfrom = yesterday` (single filter, 1–2 days of delta)
- Historical load / deep rescan: `eventlmdfrom + eventlmdto` for date windows per ADR 0010's deep-rescan addendum

### J. GET lookup endpoints return COLUMNAR shape (single and plural); field values returned as strings

All three GET endpoints exercised return the documented columnar shape (`RESULT.COLUMNS` + `RESULT.DATA`), inconsistent with `POST /recalls/` which returns object-array. The hypothesis from the original investigation now stands confirmed across both single-record and plural-row GET lookups:

| Endpoint | Method | Cardinality | Shape |
|---|---|---|---|
| `/search/producttypes` | GET | 8 rows | COLUMNAR |
| `/recalls/event/{eventid}` | GET | 1 row | COLUMNAR |
| `/recalls/eventproducts/{eventid}` | GET | 2 rows | COLUMNAR |
| `/recalls/` | POST | 5–5,000 rows | OBJECT-ARRAY |

Additional sub-finding for lookup endpoints: **`RESULTCOUNT === DATA.length`** (no pagination). The bulk POST endpoint's "RESULTCOUNT = total matching dataset" semantics (finding E) does NOT generalize to lookups — for GET endpoints the two values are the same and there is no concept of paging through a larger dataset.

Observations from the empirical response payloads that affect the bronze schema:

- **All field values returned as strings**, even numeric-looking ones. Examples: `FIRMFEINUM: "1610287"`, `RECALLNUM: "D-0491-2026"`, `CENTERCLASSIFICATIONTYPETXT: "2"`. Pydantic must allow string input and coerce to `int` / `Decimal` where appropriate. `RECALLNUM` follows a `<center-letter>-<sequential>-<year>` format (e.g., `D-0491-2026` for a CDER recall) and stays a string in the schema.
- **Booleans serialize as string `"false"` / `"true"`** (e.g., `DISTRIBUTIONPATTERNINDICATOR: "false"`, `RECALLREASONINDICATOR: "false"`, `CODEINFOINDICATOR: "false"`). Pydantic should coerce via `BeforeValidator` or `Literal["true", "false"]` with a string-to-bool step.
- **Nullable fields use BOTH JSON `null` AND empty string `""` — these are distinct sentinels.** The same response can contain `CODEINFOSHORT: ""` and `PRODUCTDESCRIPTIONSHORT: null` for different fields. This matters for content-hash dedup (ADR 0007): the canonical-serialization helper strips `None` values via `{k: v for k, v in record.items() if v is not None}` but leaves `""` intact. If FDA inconsistently returns `""` vs `null` for the same field across runs, the bronze hash will churn even when the semantic content is unchanged. The bronze schema should normalize `""` → `None` at validation time for nullable string fields, OR the canonical serializer should be extended to strip both sentinels for FDA. Decision deferred to schema implementation.
- **`COLUMNS` ordering appears stable** — matches the order documented in the PDF. By-name extraction (zipping `COLUMNS` with each `DATA` row) is more resilient than positional extraction if FDA ever reorders.

### K0. Bulk POST `displaycolumns` exposes only a subset of fields

Empirical: requesting `productlmd` in the bulk POST `displaycolumns` returns `STATUSCODE: 406` ("The payload displaycolumns does not match with the datagroup"). Confirmed 2026-04-26 against `POST /recalls/`.

The PDF lists 32 valid `displaycolumns` values for the bulk POST (productid, recalleventid, ..., postedinternetdt, rid, codeinformation), notably **excluding** `productlmd`. The lookup endpoints (`/recalls/product/{productid}`) DO expose `productlmd`. So `PRODUCTLMD` is a lookup-only field for client purposes.

**Implication for the extractor:** `EVENTLMD` is the production watermark for daily incremental queries (per ADR 0010), and that field IS in the bulk POST displaycolumns/filter list. Product-level edit timestamps (`PRODUCTLMD`) are visible only via per-product lookups, which makes them useful for enrichment but unsuitable as a top-level extraction watermark. This is consistent with the architecture: bulk POST drives the sweep, per-product lookups enrich.

### K0.1. `productlmd` capture decision (closure of `TODO.md:42`)

Codifies the cumulative empirical evidence on whether to add `PRODUCTLMD` to `fda_recalls_bronze`. **Decision: do not capture.**

**Bulk POST surface.** 406 when requested (Finding K0 above), confirmed 2026-04-26.

**Lookup endpoint surface** (`/recalls/product/{productid}` via `bruno/fda/lookup/get_product_by_id.yml`). Exposes `PRODUCTLMD` as a column (no `dt` suffix; Finding H, lines 137-142 — ADR 0007 already amended for the column-name correction). Empirically null for product 219875 (Alcon Research, recent recall — line 146); the table at line 306 extends the null observation across additional probed events.

**FDA's native field-history surface** (`/search/producthistory/{productid}`, `/search/eventproducthistory/{eventid}`). `RESULTCOUNT: 0` for every product tested across 4 distinct events spanning 2002-2026, ongoing/terminated/archive-era, multiple firms (Finding L, lines 300-328). The endpoints work correctly — the dataset is empirically empty for the records we can reach.

**Architectural fallout already absorbed.** ADR 0007 was amended 2026-04-26 to abandon the FDA-history-endpoints lineage strategy in favor of bronze-snapshot synthesis with `LAG()`, treating FDA the same as CPSC/USDA/NHTSA/USCG. Phase 6 `recall_event_history` (per `project_scope/implementation_plan.md:611`) is the production lineage signal for FDA edits.

**Cost-benefit of capture.** Adding `PRODUCTLMD` to bronze would require a per-product GET pass after the bulk POST: ~134k extra requests on backfill, ~20/day incremental. The data-quality return on that work is empirically zero — `PRODUCTLMD` has been null for every product probed via every available FDA surface. Bronze-snapshot synthesis already captures real edits via `content_hash` deltas, independent of whether FDA's lookup-endpoint metadata happens to populate.

**Reopen condition.** If any product whose `content_hash` has changed across snapshots in `fda_recalls_bronze` is observed to return a non-null `PRODUCTLMD` from the lookup endpoint, reopen this decision.

**Re-verification 2026-05-09: probe executed, conclusion strengthened to F3 falsification.** SQL on `fda_recalls_bronze` identified 5 productids whose `content_hash` had changed across snapshots — products we have *empirically observed* FDA editing in our own corpus. Bruno re-runs of `get_product_by_id.yml` against each:

| productid | recall_event_id | recall_num | center | `PRODUCTLMD` |
|---|---|---|---|---|
| 219025 | 98540 (Philips Respironics Trilogy Evo) | Z-1646-2026 | Devices (CDRH) | **null** |
| 219026 | 98540 (Philips Respironics Trilogy Evo O2) | Z-1647-2026 | Devices (CDRH) | **null** |
| 218950 | 98497 (American Laboratories Pepsin Full Strength) | H-0610-2026 | Food (CFSAN) | **null** |
| 218952 | 98497 (American Laboratories Pepsin 1:10,000) | H-0611-2026 | Food (CFSAN) | **null** |
| 218953 | 98497 (American Laboratories Pepsin 1:3000) | H-0612-2026 | Food (CFSAN) | **null** |

Two distinct events, two distinct FDA centers, multiple recall types — and all 5 returned `PRODUCTLMD: null`. **This falsifies F3's "populates on first edit" half**: FDA does not populate `PRODUCTLMD` even on records that our bronze proves it has edited (`content_hash` deltas across `extraction_runs`). The PDF-documented contract is not honored on the lookup-endpoint surface.

Implications:
- Capture decision is **strengthened**, not reopened: PRODUCTLMD via lookup-enrichment would carry zero information even for measurably-edited records.
- ADR 0007's bronze-snapshot synthesis with `LAG()` over `content_hash` is empirically the right lineage signal — `content_hash` captures real FDA edits that FDA's own `PRODUCTLMD` metadata fails to surface.
- F3 status updated in `documentation/source_assumption_audit.md` from "untestable" → "empirically falsified on the lookup-endpoint surface."

**Rebaseline-filtered re-verification 2026-05-09 (methodological caveat resolved).** The unfiltered probe above raised a concern: hash deltas could be `schema_rebaseline`/`hash_helper_rebaseline` artifacts (ADR 0027) rather than real FDA edits. The follow-up script `scripts/sql/fda/bronze/find_real_edit_productids.sql` applies the same rebaseline filter as `dbt/tests/source_assumptions/assert_fda_eventlmd_correlates_with_content_change.sql:26-27` to surface only productids whose `content_hash` changed across routine (non-rebaseline) extractions. Bruno re-probe results:

| productid | recall_event_id | recall_num | center | in filtered Q1? | in unfiltered Q2? | `PRODUCTLMD` |
|---|---|---|---|---|---|---|
| 218950 | 98497 American Laboratories Pepsin Full Strength | H-0610-2026 | Food (CFSAN) | ✓ | ✓ | **null** |
| 218952 | 98497 American Laboratories Pepsin 1:10,000 | H-0611-2026 | Food (CFSAN) | ✓ | ✓ | **null** |
| 219025 | 98540 Philips Respironics Trilogy Evo | Z-1646-2026 | Devices (CDRH) | ✓ | ✓ | **null** |
| 219026 | 98540 Philips Respironics Trilogy Evo O2 | Z-1647-2026 | Devices (CDRH) | ✓ | ✓ | **null** |
| 219028 | 98540 Philips Respironics Trilogy Evo Universal | Z-1649-2026 | Devices (CDRH) | ✓ (newly surfaced) | — (below top 5) | **null** |
| 218953 | 98497 American Laboratories Pepsin 1:3000 | H-0612-2026 | Food (CFSAN) | — (rebaseline-only phantom) | ✓ | (was null on unfiltered probe) |

The filter did real work: productid 218953 dropped out of the filtered population (its hash deltas were `schema_rebaseline`/`hash_helper_rebaseline` artifacts, not FDA edits), and productid 219028 swapped in. The remaining 5 productids are all in the rebaseline-clean population — records where `fda_recalls_bronze` carries direct evidence of FDA editing — and *all 5 again returned `PRODUCTLMD: null`*.

**F3 falsification is now airtight.** On a rebaseline-filtered population of records where FDA edits are empirically observable in our own bronze content, FDA's lookup endpoint returns `PRODUCTLMD: null` for every probe. The PDF-documented "populates on first edit" contract is contradicted on the only edit-detection surface available to clients. The K0.1 capture decision stands at higher confidence than before: capturing `PRODUCTLMD` via lookup-enrichment would carry zero information even on records of demonstrated edits.

**R2 landing sanity check (2026-05-09).** Confirms end-to-end exclusion of `PRODUCTLMD` from the bulk POST extraction path. The most recent FDA landing in R2 at the time of writing (`fda/2026-05-09/d4ee85d6-522a-4f56-8da0-925c954d450c.json.gz`) yielded zero case-insensitive `productlmd` matches when grepped via `aws s3 cp <key> - | gunzip | grep -ic productlmd`. Consistent with `_DISPLAY_COLUMNS` at `src/extractors/fda.py:117-123`, which excludes the field from the request payload — FDA's bulk POST response honors the exclusion. Rules out the alternate hypothesis that FDA returns unrequested fields anyway and that bronze projection silently drops them. Closes the third potential information path: lookup endpoint (null), history endpoint (empty), bulk POST landing (field absent). All three are empirically silent on `PRODUCTLMD`.

### K0.2. Bulk POST `sort` column must appear in `displaycolumns` or Akamai returns HTTP 204

Empirical: a POST `/recalls/` request whose `sort` value is **not** present in the `displaycolumns` list returns **HTTP 204 No Content** at the edge — not a STATUSCODE 406 from the FDA app, not a 4xx from the iRES API, just a silent 204 from Akamai with `_abck=~-1~` (unvalidated bot-manager slot) in the response headers. Confirmed 2026-05-29 by controlled binary search:

| Probe | `displaycolumns` | `sort` | Result |
|---|---|---|---|
| Step 1 (production-set) | 21-col production set (includes `eventlmd`) | `eventlmd` | ✓ 100 records |
| Step 2 | `recalleventid,productid,producttypeshort,codeinformation` (no `eventlmd`) | `eventlmd` | HTTP 204 |
| Step 1 re-run | 21-col production set | `eventlmd` | ✓ 100 records (refutes progressive-scoring hypothesis) |
| Test C | `recalleventid,productid,producttypeshort,phasetxt` (no `eventlmd`, no `codeinformation`) | `eventlmd` | HTTP 204 |
| Test E | `recalleventid,productid,producttypeshort,phasetxt,eventlmd` | `eventlmd` | ✓ 100 records |
| Test D | `recalleventid,productid,producttypeshort,codeinformation,eventlmd` | `eventlmd` | ✓ 100 records (codeinformation works once `eventlmd` is added) |

Single-variable difference between Test C and Test E (adding `eventlmd` to displaycolumns) flips 204 → 200. Same for Step 2 → Test D (same flip after adding `eventlmd`). Decisive.

**Mechanism (inferred, not directly observed).** The malformed-payload check appears to live at Akamai's edge WAF — Akamai detects the unfulfillable `sort` reference and returns 204 with the unvalidated bot-manager cookie slot, rather than forwarding to the FDA app which would have returned STATUSCODE 406 ("The payload sort does not match with the datagroup" per PDF page 8 line 631). Two pieces of evidence point to Akamai rather than FDA: (a) no STATUSCODE 407 in the body — there is no body, (b) the `_abck=~-1~` slot pattern matches Akamai's documented "unvalidated request" state.

**Implications.**

- The 204 was previously misdiagnosed as an "impersonation paradox" (curl_cffi Chrome131 making us look *more* like a bot than vanilla httpx). That hypothesis was refuted when the simple-stack probe (no impersonation, no cookies) also 204'd on a sort/displaycolumns mismatch. The misdiagnosis cost approximately a session's worth of debug time; the lesson is to check single-variable controlled tests early when the failure mode looks like bot-detection.
- Probe budget at the Akamai edge is **not** burned by 204s on this rule — Akamai catches the malformed payload before it reaches the FDA app's per-key budget. This makes iterating on `displaycolumns` cheap as long as the `sort` rule is satisfied.
- `scripts/fda/audit/probe_displaycolumns.py` enforces this rule via a `--sort` pre-flight check at argument-parse time (errors before sending the request), so the probe never burns even an Akamai roundtrip on a known-malformed payload.
- Production `src/extractors/fda.py` is structurally immune: `_DISPLAY_COLUMNS` ends with `eventlmd` and the payload's `sort: "eventlmd"` matches. Any future `_DISPLAY_COLUMNS` edit must preserve the invariant `sort_field ∈ displaycolumns`.

### K0.3. Tier-2 per-product GET carries no net-new content — `*short` are whitespace-normalized truncations, `*indicator` are presence flags

Codifies whether the lookup-only Tier-2 fields (`productdescriptionshort`, `recallreasonshort`, `codeinfoshort`, and the three `*indicator` flags) are worth capturing via per-product GET. **Decision: do not capture — derive the shorts in silver if needed; the indicators are redundant with `short IS NOT NULL`.**

These fields 406 on the bulk POST (Finding K0), have never touched our bronze, and were inferred — from the Definitions PDF, via the audit docs — to be "truncations / UI toggle flags." That inference was tested directly on 2026-06-03 with `scripts/fda/audit/probe_tier2_shorts.py` (a random sample of 60 products from the 134,450-product bronze, plus length-extreme diagnostics; full payloads under `data/exploratory/fda/probes/`, gitignored).

**`*short` = whitespace-normalized truncations of full fields already in bronze.** For each populated short, the probe tested whether its text — ignoring all whitespace — is a prefix of the corresponding full field (`content ⊆ full`):

| short field | full field (in bronze) | prevalence (sample) | `content ⊆ full` |
|---|---|---|---|
| `productdescriptionshort` | `productdescriptiontxt` | 8/60 (13.3%) | **8/8** |
| `recallreasonshort` | `productshortreasontxt` | 7/60 (11.7%) | **7/7** |
| `codeinfoshort` | `codeinformation` | 5/60 (8.3%) | **5/5** |

A raw character-prefix test flagged ~5 cases as `not_prefix`, but the bounded head dumps showed the only divergence was FDA stripping newlines and collapsing tabs (`Molift⏎Product` → `MoliftProduct`, `INJECTABLE\t10` → `INJECTABLE 10`) — i.e. the same text reflowed, then truncated. **Zero net-new content.** The cut length varies (`codeinfoshort` 84/522/543; `productdescriptionshort` 55–480), so it is *not* reproducible as `LEFT(full, N)`, but it does not need to be — the full field is stored.

**`*indicator` = pure "was-truncated" presence flags.** The indicator value tracked short-presence with zero exceptions across all 60 records:

| indicator | observed values × short-presence |
|---|---|
| `productdescriptionindicator` | `'false'`→short_null ×52, `'true'`→short_present ×8 |
| `recallreasonindicator` | `'false'`→short_null ×53, `'true'`→short_present ×7 |
| `codeinfoindicator` | `None`→null ×13, `'false'`→short_null ×42, `'true'`→short_present ×5 |

So `indicator='true' ⟺ a short exists`; the flag is the "show more…" UI toggle and is equivalent to `short IS NOT NULL`. Not collected; carries nothing beyond the short's nullness.

**Cost-benefit.** Capturing the shorts would be ~134K Akamai-paced per-product GETs for content already in bronze (or derivable). The decision: **skip all Tier-2** (this finding), consistent with the `productlmd` skip (K0.1). **Bug 2** (FDA `product_name` should prefer the truncated `productdescriptionshort`) → derive in silver via `regexp_replace(product_description_txt,'\s+',' ','g')` then truncate, not a fetch.

**Reopen condition.** If a populated `*short` is ever observed whose whitespace-stripped content is *not* a prefix of its full field (genuinely curated text), reopen — re-run `probe_tier2_shorts.py --sample N`, which reports the `content ⊆ full` count and dumps any genuine net-new cases.

**Side finding.** `codeinformation` reaches **8,867,432 chars** in full bronze (vs the 205,424 max in the 2026-05-29 100-record window) — the `src/schemas/fda.py` comment and `capture_expansion_backlog.md` figure were updated. Its silver treatment (parse into product-code records) is deferred to `project_scope/freetext-enrichment-backlog.md`.

The capture decision is homed in `project_scope/silver-field-capture-expansion-plan.md` (the "(b) PR" — Tier-2 excluded).

### K. GET lookup endpoints handle the empty-result case cleanly

Confirmed against `get_press_release_urls.yml` for `event_id=98815` (which has zero press releases):

```json
{
  "MESSAGE": "success",
  "STATUSCODE": 400,
  "RESULTCOUNT": 0,
  "RESULT": {
    "COLUMNS": ["RECALLEVENTID", "PRESSRELEASETYPE", "PRESSRELEASEISSUEDT", "PRESSRELEASEURL"],
    "DATA": []
  }
}
```

Notable properties:

- **`STATUSCODE` stays 400 (success).** FDA does not use a separate status code for "no results" on GET lookup endpoints. Compare to the bulk POST `/recalls/` endpoint, where the PDF documents `STATUSCODE: 412` for "No results found" — that asymmetry needs verification (see Open items) but at minimum, the lookup-endpoint and bulk-POST empty-result handling differ and the extractor must accommodate both.
- **`RESULT.COLUMNS` is still populated** with the full documented schema even when there are zero data rows. The bronze loader can discover the column list from an empty response — useful for schema-first extraction patterns and unit testing without recorded data.
- **`RESULT.DATA: []`** (empty array, not `null` or missing). The `rowsFromResult()` parser pattern used in the Bruno collection handles this without special-casing.
- **`RESULTCOUNT === 0`** consistent with finding J's "RESULTCOUNT === DATA.length for lookup endpoints."

**Implication for the extractor:** zero-result responses from lookup endpoints are a successful no-op, not an error. The bronze loader should treat them as `INSERT 0 rows` and continue without retry / quarantine. Only `STATUSCODE != 400 AND != 412` should trigger error-handling per ADR 0013.

**Bulk POST `/recalls/` empty-result case (extends finding K):**

Confirmed 2026-04-26 by querying with `eventlmdfrom: 04/25/2026, eventlmdto: 04/26/2026` (no edits in this window — most recent edit observed in earlier exploration was 04/24/2026). Response:

```json
{
  "MESSAGE": "No results found",
  "STATUSCODE": 412
}
```

Critical asymmetry with lookup endpoints:

| Endpoint type | Empty-result STATUSCODE | RESULT key | DATA shape |
|---|---|---|---|
| GET lookups (`/recalls/event/...`, `/search/...`) | **400 (success)** | present | `{COLUMNS: [...], DATA: []}` |
| POST `/recalls/` (bulk) | **412 (No results found)** | **absent entirely** | n/a |

**Implications for the extractor's error-handling matrix (per ADR 0013):**

- STATUSCODE 400 → success; process `RESULT` (object-array for POST, columnar for GET).
- STATUSCODE 412 from bulk POST → success with zero rows. Treat as no-op, advance the watermark, do NOT retry or quarantine.
- STATUSCODE 401 → auth failure; refresh credentials, retry once.
- STATUSCODE 402–411, 413–418 → payload/parameter error; quarantine, alert, do not retry (these indicate a code bug, not a transient failure).
- STATUSCODE 421, 427 → numeric-id parse error on path params; should never occur if the extractor is correctly typing `event_id` / `product_id` as integers (see the env-var quoting bug we hit early in this exploration).

The 412 case is particularly worth highlighting because **the response does not include a `RESULT` key at all** — code that does `response.RESULT.length` will throw `TypeError`. The extractor's response parser must check `STATUSCODE` first and short-circuit on 412 before touching `RESULT`.

### M. Active archive migration — FDA re-touches old recall records, bumping EVENTLMD

Confirmed 2026-04-26 via deep-rescan query (`eventlmdfrom: 01/26/2026, eventlmdto: 04/26/2026`). Every record returned in the "90-day window" was actually an old recall whose `EVENTLMD` had been bumped to a recent date:

| Row | `RECALLINITIATIONDT` | `EVENTLMD` | Age |
|---|---|---|---|
| 1 | 2002-10-25 | 2026-04-02 | 24 years |
| 2 | 2008-10-18 | 2026-02-24 | 18 years |
| 5 | 2010-08-26 | 2026-01-27 | 16 years |
| 28 | 2018-12-04 | 2026-02-23 | 8 years |

This is the **same archive-migration phenomenon CPSC exhibits** (documented in `documentation/cpsc/last_publish_date_semantics.md` finding 2). It is currently happening on the FDA dataset, surfacing thousands of old records into the incremental window.

Total dataset breakdown for the 90-day window:
- RESULTCOUNT: 2,590 records
- Vast majority appear to be archive-migration touches (records initiated >1 year ago, with very recent EVENTLMD)
- Only a small fraction are true new recalls (records initiated within the window)

**Implications for the extractor:**

1. **Daily incremental query will sweep up archive-migration records along with new recalls.** That's correct behavior; the bronze layer should land all of them.
2. **Content-hash dedup (ADR 0007) handles this correctly.** Records with unchanged content but bumped `EVENTLMD` will produce duplicate hashes and be no-op-inserted. Records with actually-changed content get a new bronze row.
3. **Without bronze-snapshot synthesis, we cannot tell what changed.** This further weakens the case for relying on FDA's native history endpoints (which finding L showed are sparsely populated). Bronze-snapshot synthesis becomes the primary lineage mechanism for FDA, same as CPSC/USDA/NHTSA/USCG.
4. **Daily delta sizing will be noisier than expected.** ADR 0010 estimated ~25-50 records/day for FDA; archive migration may push this higher (~100-300/day) until the migration completes. Most additional records will be content-hash-dedup'd to no-ops, but the API call volume and bronze-row scanning is real.

### M-extension. Many older records have null metadata fields that newer records populate

Observed in the same deep-rescan response:

```
Row 1 (Sutton Place Gourmet, 2002):
  RECALLINITIATIONDT: "10/25/2002"
  CENTERCLASSIFICATIONDT: null     ← classified (CENTERCLASSIFICATIONTYPETXT: "1") but no date
  ENFORCEMENTREPORTDT: null
  INITIALFIRMNOTIFICATIONTXT: null
  DETERMINATIONDT: null

Row 14 (Norpac 2013, newer):
  RECALLINITIATIONDT: "05/22/2013"
  CENTERCLASSIFICATIONDT: "06/24/2013"   ← all populated
  ENFORCEMENTREPORTDT: "07/03/2013"
  INITIALFIRMNOTIFICATIONTXT: "Combination"
  DETERMINATIONDT: null
```

Pattern: older records (especially pre-2010) have null values for fields that became standard in later records. FDA likely added these columns over time and didn't backfill historical data. The bronze Pydantic schema must mark **the following as `Optional[T]`** beyond the already-noted nullables:

- `CENTERCLASSIFICATIONDT`
- `ENFORCEMENTREPORTDT`
- `INITIALFIRMNOTIFICATIONTXT`
- `DETERMINATIONDT`

In addition to the previously documented nullables (`RECALLNUM`, `TERMINATIONDT`, `PRODUCTDISTRIBUTEDQUANTITY`, `PRODUCTLMD`, `FIRMLINE2ADR`, `FIRMSURVIVINGNAM`, `FIRMSURVIVINGFEI`, `FIRMSTATEPRVNCNAM`, `PRODUCTDESCRIPTIONSHORT`, `RECALLREASONSHORT`, `CODEINFOSHORT`).

Effectively, **almost every field except the core identifiers (`PRODUCTID`, `RECALLEVENTID`) and the descriptive long-form fields (`PRODUCTDESCRIPTIONTXT`, `PRODUCTSHORTREASONTXT`) must be treated as nullable in the bronze schema.** This is consistent with FDA having evolved the iRES schema over decades — the schema-evolution policy in ADR 0014 should accommodate this with permissive bronze and stricter silver.

> **Correction (2026-05-31, Finding P):** an earlier version of this paragraph also
> listed `EVENTLMD`, `FIRMLEGALNAM`, `CENTERCD`, and `PRODUCTTYPESHORT` as never-null
> "core identifiers." That was an **inference** the `eventlmdfrom`-windowed extraction
> masked — a server-side `>=` filter structurally excludes null-`EVENTLMD` rows, so the
> nulls were invisible, never absent. The full-corpus probe (Finding P) found 197
> null-`EVENTLMD` records, falsifying the claim for all four; they are nullable in
> bronze as of migration 0020. Only `PRODUCTID` / `RECALLEVENTID` (and `RID`) are
> genuinely always-present.

### L. FDA's native field-level history endpoints are sparsely populated in practice

Empirical: both `/search/producthistory/{productid}` and `/search/eventproducthistory/{eventid}` returned `RESULTCOUNT: 0` for every event tested across the time spectrum. Sample candidates tested:

| Event | Phase | Era | Firm | Product tested | Product history | Event-product history |
|---|---|---|---|---|---|---|
| 98815 | Ongoing | 2026 (recent) | Alcon Research LLC | 219875 | (not tested — PRODUCTLMD null implies empty) | (not tested) |
| 98279 | Terminated | 2026 | Karison Foods & Snacks | 218703 | 0 rows | 0 rows |
| 98286 | Terminated | 2026 | American Red Cross | 218151 | 0 rows | 0 rows |
| 25159 | Completed | 2002 (archive migration) | Sutton Place Gourmet | 25314 | 0 rows | 0 rows |

Three distinct events spanning recent ongoing, recently terminated, and 24-year-old archive-migration records ALL show zero history. The pattern is consistent across the dataset — not specific to recent records, not specific to terminated records, not specific to product type or center.

The endpoints **work correctly** (return the documented 6-column schema, clean empty `RESULT.DATA: []`, STATUSCODE 400). They just don't have data for the records tested.

Possible explanations (none confirmed):

1. FDA only records edits for specific field types — e.g., data corrections, not lifecycle transitions like phase changes. The PDF doesn't enumerate which fields trigger history rows.
2. The history endpoints are scoped to a recent time window — older edits may have been pruned.
3. FDA records edits internally but doesn't expose them via these specific public endpoints — there may be richer history available in a non-public iRES interface.
4. Only certain centers (e.g., CDER for drugs) populate history; CFSAN food recalls might not.

**Major implication for ADR 0007.** The lineage strategy assumed FDA would feed `recall_event_history` natively via these endpoints, with the other 4 sources synthesizing history from bronze snapshots via `LAG()` window functions. Empirically, FDA cannot be relied on for native history. The lineage architecture needs revision:

- **Treat FDA the same as CPSC/USDA/NHTSA/USCG** — synthesize history from successive bronze snapshots via `LAG()` over the consumer-meaningful field allowlist.
- **Still query the FDA history endpoints as a supplemental signal** — when they DO return rows, those rows are higher-fidelity than synthesized diffs (they have real `oldvalue`/`newvalue` directly from FDA's internal database). Bronze tables for `fda_product_history_bronze` and `fda_event_product_history_bronze` should still be created — they'll be mostly empty but cheap, and they surface real edits when present.
- **Update ADR 0007** to reflect that FDA's "field-level history endpoints provide a richer signal" claim was over-stated. The endpoints exist but aren't reliably populated.

**Resolved 2026-04-26:** Tested against `RECALLEVENTID 25159` (Sutton Place Gourmet, 2002) — also returned zero history. The "test against an older event" follow-up confirmed the general-sparseness conclusion across time. **The history endpoints are not a viable lineage signal at any age of record, on any phase, on any product type.** The architectural revision to ADR 0007 stands as written: FDA gets bronze-snapshot synthesis like the other four sources.

### N. Anti-abuse throttling: rapid request bursts trigger an HTML "Apology" redirect, not a 429

Surfaced 2026-04-28 while attempting to record live VCR cassettes for the Phase 5a integration tests. Five rapid pytest invocations (5 test scenarios × ~5 tenacity retries × 2 hops per attempt = ~50 requests in ~90 seconds) triggered an FDA-side IP throttle. Threshold and recovery time are not yet known.

**Observed throttle response shape:**

The bulk POST endpoint stops returning JSON. Instead, it returns:

```
HTTP/1.1 302 Moved Temporarily
Location: /apology_objects/abuse-detection-apology.html
Content-Type: text/html
```

with an HTML body whose `<title>` is `FDA Apology` and whose JS body redirects to `/apology_objects/excessive-requests-apology.html`. Following the 302 lands on `/apology_objects/abuse-detection-apology.html`, which itself returns **HTTP 404** (also `text/html`, also "FDA Apology" body). So a typical client experiences:

```
POST /rest/iresapi/recalls/ → 302 → GET /apology_objects/abuse-detection-apology.html → 404
```

Notable properties of the throttle response:

- **No `Retry-After` header.** Standard RFC 6585 / RFC 7231 throttle signaling is absent. Clients cannot back off based on a server hint.
- **No `429 Too Many Requests`.** The status code is 302 (then 404 after the redirect), not 429. Code paths that key off 429 to detect rate-limiting will miss this entirely.
- **Cache-busting via the response query string.** The redirect destination URL contains a unique random suffix (e.g., `?0.6f0c2e17.1777379833.d552bfcd`), suggesting per-request server-side state.
- **Persists across cache-busted POSTs.** Adding `signature=` (per finding 3) does not bypass the throttle — it's tied to client identity (IP, possibly auth user), not URL.
- **Affects production credentials with valid auth.** The 401 auth-failure path is distinct; this throttle fires before any auth check.

**Why this didn't surface during Bruno exploration (2026-04-26):** Bruno is interactive. Click → response → think → click again. Natural cadence is 5–10 requests/minute over a session. The pytest+tenacity recording attempt produced ~33 requests/minute sustained for 90 seconds — at least 3× the Bruno rate. The threshold sits between those two rates.

**Implications for the extractor and production extraction:**

1. **Detect by Content-Type, not status code.** `_fetch_page` checks `response.headers["Content-Type"]` for `text/html` and raises `ExtractionError` (not `TransientExtractionError`) so tenacity does **not** retry. Retrying makes the throttle worse and extends the block.
2. **Production daily extraction will not trigger this** at the documented ~20 records/day cadence — one POST per cron run, well under any reasonable abuse threshold. The deep-rescan workflow (ADR 0023) is the higher-risk surface: paginating ~134K records at 5,000 rows/page = ~27 sequential POSTs. That should still be safe, but worth verifying empirically the first time it runs.
3. **Cassette recording requires sequential, paced runs** rather than `-k "scenario1 or scenario2 or ..."` batching. One cassette at a time, with a delay between, is the safe pattern until the threshold is characterized.
4. **Recovery is time-based, not request-based.** Retrying immediately after a throttle persists the block. Empirically: at least 30 minutes; characterizing this precisely is a follow-up.

**Open questions (to be resolved as the API is exercised more, or via direct contact with FDA):**

- What is the actual request-rate threshold? (e.g., requests per minute, per hour, per credential)
- What is the recovery time? Fixed (e.g., 1 hour) or escalating with repeat offenses?
- Is the throttle keyed on IP, on `Authorization-User`, or both?
- Is there an FDA-supported way to request a higher rate limit for production extractors?

These will be filled in when surfaced in normal use, or via email back from FDA's iRES support contact (if pursued). Until then, treat any HTML response from `/recalls/` as a hard "stop and wait" signal.

### O. The cassette suite was trimmed: `multi_page` and `partial_last_page` removed as redundant

Surfaced 2026-04-28 immediately after live cassette recording. The original Phase 5a plan specified four happy-path live cassettes — `single_page`, `multi_page`, `partial_last_page`, plus `empty_result` — assuming the matrix would meaningfully exercise different code paths in `_paginate`. Empirical recording showed otherwise.

**Recorded reality at FDA's actual data volume:**

| Test | Filter window | Records returned | HTTP calls in cassette | `_paginate` iterations |
|---|---|---|---|---|
| `test_happy_path_single_page` | 7 days | 168 | 1 | 1 |
| `test_happy_path_partial_last_page` (deleted) | 27 days | ~700 | 1 | 1 |
| `test_happy_path_multi_page` (deleted) | 4 months | 3,036 | 1 | 1 |

All three terminated on the first iteration (`len(page) < PAGE_SIZE` immediately true). With `_PAGE_SIZE = 5000` and a daily delta of ~20 records per finding M's cardinality observations (~750/month, ~30K/year including archive-migration noise), a window has to be roughly **18+ months wide** before the loop iterates more than once. No realistic incremental-extraction window comes close.

**Why the original matrix made sense in the plan but failed in practice:**

The plan was authored before the empirical investigation that produced this document. It assumed FDA was a "paginated API" whose test matrix should mirror that — single / partial / multi pages. Once the cardinality numbers were measured, the only path that genuinely paginates against the live API is the deep-rescan loader (134K records → ~27 pages), which inherits the same `_paginate` from `FdaExtractor` and is therefore covered transitively when the incremental path is.

**What replaces the deleted cassettes:**

- Pagination loop logic is unit-tested via `tests/extractors/test_fda_extractor.py::TestPaginateExtractor::test_multi_page_accumulates_records`, which patches `_fetch_page` to return a 5,000-row page followed by a 1-row page and asserts the accumulator sums to 5,001. That is the actual loop logic, exercised deterministically.
- The retained `test_happy_path_single_page` cassette catches schema-shape drift against the real API (the original purpose of cassette tests per ADR 0015) just as well as three near-identical cassettes would have, with one-third the bytes committed and zero misleading scenario names.

**Implication for other Phase 5 sources (USDA, NHTSA, USCG):**

Per-source cassette suites should be designed *after* Bruno exploration and an initial empirical extraction, not from a projected matrix. The plan's per-source-shape table (paginated vs flat-file vs HTML) is a good starting heuristic, but the precise scenario count and naming should be informed by what the API actually does at the source's data volume. See the corresponding update to the Phase 5 standing requirement in `project_scope/implementation_plan.md`.

---

### P. Full-corpus probe (2026-05-31): the `eventlmd` window silently misses 197 null-`EVENTLMD` records; the historical seed must use `filter:"[]"`

Surfaced while sizing the Phase 6a.5 FDA historical seed. Two live `rows=1` bulk-POST
probes (`scripts/fda/audit/probe_corpus_completeness.py`) compared the unfiltered corpus
against an `eventlmdfrom` window:

| Query | `RESULTCOUNT` |
|---|---|
| `filter:"[]"` (whole dataset, Finding E) | **134,450** |
| `eventlmdfrom:01/01/1900` window | **134,253** |
| **gap (window silently misses)** | **197** |

**Root cause** is Finding H: the `*lmd` columns advance *on edits only*, so un-edited
records have null `EVENTLMD`, and a server-side `eventlmdfrom` `>=` comparison cannot
match a null. The 197 were never *absent* from the corpus — only *invisible* to every
windowed extraction (incremental and date-windowed deep-rescan) to date. This falsifies
the "`EVENTLMD` is a never-null core identifier" inference at the top of Finding M-extension
(see the correction note there).

**Consequences for the seed (implemented; see `project_scope/archive/fda-historical-seed-plan.md`):**
- The full-corpus historical seed pulls `filter:"[]"` (no `eventlmd` window), so the 197 are
  included. `FdaDeepRescanLoader.set_full_corpus()` selects this path; `recalls deep-rescan
  fda` with **no** `--start-date/--end-date` triggers it.
- Migration 0020 drops `NOT NULL` on `event_lmd`, `center_cd`, `product_type_short`,
  `firm_legal_nam` (the four falsified "core" fields) so the no-window seed lands those rows
  instead of *silently quarantining* them (197/134,450 = 0.15% « the 5% rejection threshold,
  and `records_landed` reports the fetched count, not the inserted count — the failure would
  read as "success, 134,450" with only 134,253 in bronze).
- The 197 are **seed-only**: the daily incremental's `eventlmdfrom = watermark` filter can
  never re-sweep a null-`EVENTLMD` row, and the watermark only advances forward. They enter
  bronze only via the full-corpus seed (or once FDA edits a record, populating `EVENTLMD`).

**OPEN production-cadence question (Phase 7) — the null-`EVENTLMD` incremental blind spot.**
The same null-`EVENTLMD` blind spot affects ongoing extraction, not just the one-time seed:
*if a genuinely new recall can arrive with null `EVENTLMD`*, the daily incremental misses it
on arrival and only catches it later (when FDA edits it, populating `EVENTLMD`) — or never, if
never edited. Critically, the scheduled `deep-rescan-fda.yml` backstop does **not** close this
gap: it passes a `--start-date/--end-date` window (an `eventlmdfrom`/`eventlmdto` filter), so it
shares the identical blind spot. Only the no-window `filter:"[]"` full-corpus path sees
null-`EVENTLMD` rows.

Whether this actually bites is **gated on the post-seed census** (`seed_completeness_gate.sql`
queries 5-7): if the 197 are an old archive tail (initiation years spread across decades, 0
recent), modern new recalls evidently get `EVENTLMD` at creation (consistent with Finding M:
the daily window already catches a "small fraction" of genuinely-new records, which requires
their `EVENTLMD` to be populated) and the incremental is sound. If query 6 finds *recent*
null-`EVENTLMD` recalls, the leak is real and needs a fix before the Phase 7 cron.

**Fix lever if confirmed:** `recallinitiationdt` is itself a **filterable** field — the iRES
usage PDF (p.7) lists `recallinitiationdtfrom`/`recallinitiationdtto` among the filterable
date ranges (alongside `eventlmd`, `centerclassificationdt`, `terminationdt`, `determinationdt`,
`enforcementreportdt`, `postedinternetdt`). A genuinely-new recall inherently *has* a
`recallinitiationdt`, so a `recallinitiationdtfrom = watermark` filter would catch new
null-`EVENTLMD` recalls on the **first** incremental run. The two are **complementary, not
substitutes**: `eventlmd` catches *edits* to old records (archive migration, Ongoing→Terminated
— which a fixed-in-the-past `recallinitiationdt` cannot), while `recallinitiationdt` catches
*new arrivals* regardless of `EVENTLMD`. The API filters one value at a time, so this is two
calls unioned via content-hash dedup (cheap). A periodic `filter:"[]"` full-corpus rescan is
the simpler alternative backstop (≈5-6 min, 99.85% dedup no-ops). Decision deferred to Phase 7
cron design; do NOT change the incremental for the 6a.5 seed. (`createdt` is NOT a fix lever —
it is neither filterable nor a bulk-POST displaycolumn.)
- `recalleventid asc` is a stable total order but its tie boundary is **non-deterministic**
  across requests (a `productid` can straddle two adjacent pages on a re-run). `rid` is
  hash-excluded, so straddle copies are byte-identical and collapse under
  `within_batch_dedup=True`; a rare tie-boundary *drop* is caught by the post-seed
  `COUNT(DISTINCT)` gate (`scripts/sql/fda/bronze/seed_completeness_gate.sql`) and healed by
  an idempotent re-run.
- dbt: `event_lmd`'s staging `not_null` test became a bounded warn-tripwire
  (`warn_if: ">300"`, `error_if: ">1000"`), and silver `published_at` coalesces
  `event_lmd → recall_initiation_dt` so the 197 don't fail the strict-silver not_null.

**Methodological caveat (do not re-derive a date floor from a sorted top-N):** FDA date
strings are `MM/DD/YYYY`, which sort **lexically**, not chronologically (`12/31/2015` sorts
after `01/15/2026`). An earlier "earliest `eventlmd` 01/01/2023 / earliest recall 01/01/2003"
reading was lexically poisoned and is **unsound**. Characterize the null-`EVENTLMD` rows
*post-seed* via SQL on `fda_recalls_bronze` (sort-immune; the gate's year histogram), not via
a sorted API page.

**Page-size note:** the bulk-POST page cap is **2,500** rows (not the 5,000 cited in Finding
O's table) whenever `codeinformation` is in `displaycolumns` — which it now always is after the
migration-0019 capture expansion (`_PAGE_SIZE`, `src/extractors/fda.py`). Finding O's
cassette-trim reasoning is unaffected (its windows still terminate in one page).

---

## Cardinality observations

Useful for sizing decisions in Phase 5a (batch sizes, historical-load runtime estimates, daily-delta expectations):

| Filter | RESULTCOUNT | Notes |
|---|---|---|
| No filter | 133,841 | Entire iRES history, all phases / centers / dates |
| `eventlmdfrom: 01/01/2026` | 3,012 | ~4 months of 2026 |
| `eventlmdfrom: 02/01/2026, eventlmdto: 02/28/2026` | 833 | Just February 2026 |
| `eventlmdfrom: 04/20/2026, eventlmdto: 04/26/2026` | 141 | 7-day production-window simulation |

Rough rate: **~20 records/day**, **~750–1,000 records/month**, **~30K/year**. Daily delta is small enough that a single page request (rows=5000) will always cover it — pagination loop in the extractor will normally exit after one iteration. Historical load (~134K records at 5,000 rows/page) is ~27 paginated requests for full backfill.

### Pydantic schema implications surfaced from the 7-day production-window query

- **`RECALLNUM` is nullable.** Recalls with `CENTERCLASSIFICATIONTYPETXT: "NC"` (Not Classified) appear with `RECALLNUM: null`, `CENTERCLASSIFICATIONDT: null`. Once the recall is classified, FDA assigns a number in the `<center-letter>-<sequential>-<year>` format. The extractor must accept `RECALLNUM: Optional[str]` and re-extract on subsequent runs to capture the eventual number assignment (this is one of the canonical "edit" cases ADR 0007's lineage view should surface — initial null → later string).
- **`CENTERCLASSIFICATIONDT` is nullable** — same reason; not assigned until classification happens.
- **`PRODUCTDISTRIBUTEDQUANTITY` is sometimes a string with units, sometimes null, sometimes plain numeric strings.** Examples observed: `"2324 units"`, `"4,291,797"`, `"21,557 cases (30 lbs/case)"`, `"685,776 units"`, `null`. This is a free-text field, not a parseable quantity. Bronze schema lands as `Optional[str]`; downstream silver may attempt structured parsing for analytics but should expect ~30% unparseable.
- **`VOLUNTARYTYPETXT` enum values empirically observed:** `"Voluntary: Firm Initiated"`, `"Firm Initiated"` (without prefix). Suggest treating as free-text `str` rather than a strict enum until more data accumulates.
- **`INITIALFIRMNOTIFICATIONTXT` enum values observed:** `"Letter"`, `"E-Mail"`, `"Telephone"`, `"Combination"`, `null`. Reasonably bounded; could be `Literal[...] | None`.

---

## Implications for Phase 5a deliverables

### Pydantic bronze schema (`src/schemas/fda.py`)

- Use `ConfigDict(extra='forbid', strict=True)` per ADR 0014.
- All input fields typed as `str | None` initially, with `BeforeValidator`s for type coercion:
  - String-to-`int` for `FIRMFEINUM`, `RECALLEVENTID`, `PRODUCTID`, `RID`
  - String-to-`bool` for `DISTRIBUTIONPATTERNINDICATOR` (and any other boolean-coded fields)
  - String-to-`datetime` for date fields (`MM/DD/YYYY` → UTC datetime)
- Use lowercase aliases or normalize keys at validation time (uppercase keys from API → lowercase Pydantic fields).
- Schema targets the **bulk POST object-array shape** since that is the production extraction path. A separate columnar parser handles lookup endpoints (used for backfill enrichment, not the daily extract).

### Extractor (`src/extractors/fda.py`)

- Inherits from `RestApiExtractor` (Phase 2 base class).
- Two distinct response parsers:
  - Object-array parser for `POST /recalls/` (production path)
  - Columnar parser for `GET /recalls/event/{eventid}`, `GET /search/producthistory/{productid}`, `GET /search/eventproducthistory/{eventid}` (lineage/enrichment path per ADR 0007)
- Pagination loop uses `start += rows` until `len(RESULT) < rows`.
- Signature cache-busting: inject `int(time.time())` into every request URL.
- Headers: `Authorization-User` and `Authorization-Key` from settings (per ADR 0016).
- Two code paths per the Phase 5 standing requirement:
  - **Incremental** (`FdaExtractor.extract()`): `eventlmdfrom = watermark` filter, with response-count guard
  - **Historical load** (`deep-rescan-fda.yml` — though FDA doesn't currently have one per ADR 0010; if added later, follows the same separate-class pattern as CPSC)

### VCR cassettes (`tests/fixtures/cassettes/fda/`)

- Custom request matcher excludes `signature` param from comparison (FDA-only; other sources keep VCR defaults).
- 9 scenarios per ADR 0015's integration matrix, tuned to FDA's API shape:
  - Live-recorded happy paths: single-page, multi-page, partial last page, empty
  - Live-recorded with bad credentials: 401
  - Hand-constructed via respx: 429 rate limit, 500 transient, malformed record
  - Shared cassette for content-hash dedup scenario

---

## References

Canonical test-level documentation lives in the Bruno collection request files. Each `.yml` file's `docs:` block contains the executable proof and full reasoning for the findings cross-referenced above:

| Finding | Source request |
|---|---|
| Universal quirks 1–4, findings A–C | `bruno/fda/lookup/get_product_types.yml` |
| Findings D–I, cardinality observations | `bruno/fda/data_exploration/post_recalls_seed_event_ids.yml` |
| Finding J | `bruno/fda/lookup/get_event_by_id.yml` |

External documentation:

- iRES API Usage PDF: `documentation/fda/iRES_enforcement_reports_api_usage_documentation.pdf`
- Enforcement Report API Definitions PDF: `documentation/fda/enforcement_report_api_definitions.pdf`
- Both confirmed to describe the same API; the Definitions PDF is the column-level reference linked from the Usage PDF (api docs page itself).

Related ADRs:

- ADR 0007 — Lineage via bronze snapshots and content-hashing (FDA history endpoints feed the unified `recall_event_history` view)
- ADR 0010 — Ingestion cadence and orchestration (FDA daily incremental on `eventlmd >= yesterday`)
- ADR 0012 — Extractor pattern (FDA inherits from `RestApiExtractor`)
- ADR 0014 — Schema evolution policy (Pydantic strict mode for bronze)
- ADR 0015 — Testing strategy (9-scenario VCR cassette matrix)
- ADR 0016 — Secrets management (Authorization-User / Authorization-Key handling)

---

## Open items

These will be filled in as more endpoints are exercised and as Phase 5a empirical-verification work progresses:

- ~~**Field-history endpoint sparseness — verify on older events.**~~ RESOLVED 2026-04-26. The 2002 Sutton Place Gourmet event (`25159`) also returned zero history. General sparseness confirmed across the time spectrum. ADR 0007 needs revision: FDA gets bronze-snapshot synthesis like the other four sources.
- **`eventlmd` / `productlmd` edit semantics** — see finding L. The `*LMD` advance-on-edit claim from ADR 0010 / ADR 0007 was not testable against the candidates we tried because no candidates had recorded edits. Resolve in Phase 5a if/when populated history surfaces.
- ~~**Bulk POST empty-result behavior**~~ — RESOLVED 2026-04-26 via the incremental-extraction test. Bulk POST returns STATUSCODE 412 with no `RESULT` key for empty results (vs. lookups returning 400 + empty DATA). Documented in finding K.
- **`firmfeinum` cross-source firm anchor** — confirm `FIRMFEINUM` is consistently populated and stable across recalls for the same firm (ADR 0002 designates it the firm-resolution anchor).
- **`displaycolumns` exhaustiveness** — empirically map which columns return what types when included in the `displaycolumns` parameter, especially for fields not yet exercised (e.g., `productdistributedquantity`, `codeinformation` text BLOB).
- **ADR cleanup** — drop the `dt` suffix from `eventlmddt` and `productlmddt` references in ADR 0007 and ADR 0010 (the actual API columns are `EVENTLMD` and `PRODUCTLMD`).
- **Anti-abuse throttle threshold and recovery time (finding N)** — observed 2026-04-28 during cassette recording. Need to characterize: actual requests-per-minute threshold, recovery duration, whether it keys on IP / `Authorization-User` / both, and whether FDA offers a higher-rate-limit tier for credentialed production extractors. Resolve via continued use of the API or direct email to FDA iRES support.
