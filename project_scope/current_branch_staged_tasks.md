# Staged tasks — `feature/phase-6a-foundation-audit`

- **Branch:** `feature/phase-6a-foundation-audit`
- **Status:** Paused 2026-05-28 — FDA Akamai per-IP cooldown in progress
- **Phase:** 6a foundation audit (per `project_scope/phase-6-execution-plan.md`)

## Context: where we paused and why

The FDA portion of the source-by-source field audit is **~95% complete**. We have:

- All three silver-only mismappings confirmed against the iRES API definitions PDF + a 447-record R2 corpus inspection. Empirical NULL rates, value distributions, and surprises (`HFP` center, `OCS` for cosmetics, `VOLUNTARYTYPETXT` dual-format) all documented in `documentation/fda/field_audit_2026_w22.md` §8.
- The three-tier endpoint architecture (`POST /recalls/` + per-product GET + per-event GET) confirmed against `bruno/fda/` and now reflected in `documentation/audit/capture_expansion_backlog.md` § FDA.

The remaining 5% — empirical populate-rate verification for the tier-2/tier-3 fields that would feed the (b) PR — is **blocked on FDA's Akamai bot-detection cooldown**. Our probes triggered per-IP scoring because (1) the first probe sent an invalid displaycolumns set (press release fields → STATUSCODE 406), and (2) every subsequent probe used a different displaycolumns shape, which looks like security-scanner behavior to Akamai. The IP is silent-blocked (HTTP 204 + `_abck=...~-1~...` invalid-session cookies). Cooldown is typically 24h+.

## Tasks (in priority order)

### Task 1 — Wait through Akamai's per-IP cooldown

**What:** Don't issue any more FDA bulk-POST probes from this IP until at least **2026-05-29 21:00 EDT** (24 hours after the last 204).

**Why:** Continued probing while the IP's score is elevated extends the block (per Finding N, retries deepen the throttle). The probe script's `--clear-cookies` flag clears local Akamai cookies but doesn't reset the per-IP score on FDA's side — that only decays with time.

**How:** Nothing to do. Move to Task 2 / Task 5 in parallel.

### Task 2 — Production-shape probe to verify the shape-variance hypothesis

**What:** After the cooldown, run **one** probe that mirrors `bruno/fda/incremental_extraction/post_recalls_eventlmd_range.yml` exactly — same 21-field displaycolumns, bounded `eventlmdfrom` + `eventlmdto`, `rows=50`, `sort=eventlmd desc`. No `codeinformation`, no firm-address fields, no open-ended filter.

**Why:** Confirms whether the 204s were caused by shape-variance (probing-like pattern) or by Akamai IP-class scoring. If the production-shape probe returns HTTP 200 + FDA STATUSCODE 400, shape-variance was the trigger and we can plan tier-1 probes more carefully. If it still 204s, the per-IP block hasn't decayed; extend the wait.

**How:**

```bash
python scripts/fda/audit/probe_displaycolumns.py \
    --columns "recalleventid,productid,producttypeshort,recallnum,phasetxt,centercd,centerclassificationtypetxt,firmlegalnam,firmfeinum,recallinitiationdt,centerclassificationdt,terminationdt,enforcementreportdt,determinationdt,initialfirmnotificationtxt,distributionareasummarytxt,voluntarytypetxt,productdescriptiontxt,productshortreasontxt,productdistributedquantity,eventlmd" \
    --eventlmdfrom 04/20/2026 --eventlmdto 04/26/2026 \
    --rows 50 --no-save --clear-cookies
```

`--clear-cookies` purges the prior invalid Akamai session so we start fresh.

**Expected:** HTTP 200 + STATUSCODE 400 + ~150 records returned. Per-field summary printed.

### Task 3 — Tier-1 probe (firm address + survivors + posted date)

**What:** Only run if Task 2 succeeds. One probe with production-shape baseline plus the tier-1 additions.

**Why:** Validates that the firm address fields actually populate in real responses before we commit to the bronze schema migration in the (b) PR.

**How:**

```bash
python scripts/fda/audit/probe_displaycolumns.py \
    --columns "recalleventid,productid,producttypeshort,firmlegalnam,firmcitynam,firmstatecd,firmcountrynam,firmline1adr,firmline2adr,firmpostalcd,firmsurvivingnam,firmsurvivingfei,postedinternetdt,eventlmd" \
    --eventlmdfrom 04/20/2026 --eventlmdto 04/26/2026 \
    --rows 50
```

**Expected:** HTTP 200 + per-field summary showing NULL rates for the address fields. If NULL rates exceed ~80%, downgrade those fields from MEDIUM to LOW in the backlog.

### Task 4 — Tier-2 and tier-3 probe scripts (write, don't run yet)

**What:** Two new scripts that exercise the per-product and per-event GET endpoints:

- `scripts/fda/audit/probe_per_product_endpoints.py` — exercises `GET /search/codeinfo/{productid}` and `GET /recalls/product/{productid}` against a sample of `productid`s from the existing bronze
- `scripts/fda/audit/probe_per_event_endpoints.py` — exercises `GET /search/pressreleaseurls/{eventid}` and `GET /recalls/event/{eventid}` against a sample of `recalleventid`s from existing bronze

**Why:** Lets us validate tier-2 and tier-3 populate rates without depending on Akamai approval of varying bulk-POST shapes. The lookup endpoints are per-id GETs, not pattern-matching POSTs — much less likely to trigger probing-detection.

**How:** Defer to USDA audit completion. We need the cross-source consolidation context before deciding the (b) PR's tier selection (B1/B2/B3), so building these probes now would be premature. Add as TODO note in `documentation/fda/field_audit_2026_w22.md` §8.

### Task 5 — Send the follow-up email to Kevin at FDA OII

**What:** Reply to Kevin's 2026-04-30 email with a static IP + intended usage description. (Draft was composed in-conversation 2026-05-28 and removed from the repo after sending — the IP address it carried is sensitive; check your email outbox for the sent copy.)

**Why:** A whitelisted static IP would let us probe freely during audit work without tripping Akamai. Kevin offered this in his original reply; we just hadn't needed it before. Sending now gives FDA time to process before the next audit cycle.

**How:** Open the draft, fill in your static IP, send via your normal email. Block out the response time (FDA's prior reply was ~2 days).

### Task 5b — Process Kevin's reply when it arrives

**What:** Branch the downstream tasks based on which of three plausible responses we get.

**Why:** The reply materially changes which downstream probes are feasible, what shape the (b) PR can take, and whether the per-source-probe methodology needs adjustment for USDA / future sources.

**How:** Three decision branches —

- **Branch A — Static IP whitelisted.** Resume probing freely. Run the production-shape probe (Task 2), then directly proceed to **writing tier-2 + tier-3 probe scripts now (originally Task 4)** rather than deferring to post-USDA. With whitelist in hand, the engineering tax of per-product / per-event GETs is just a request-volume question, not an Akamai-scoring question. Also note the whitelist in `documentation/fda/api_observations.md` so future-us doesn't re-probe blindly. The (b) PR can plausibly ship as B3 (all three tiers).
- **Branch B — Pacing/usage guidance only (no whitelist).** Update `scripts/fda/audit/probe_displaycolumns.py` and any future per-source probe scripts to honor whatever pacing/throttling pattern Kevin recommends (e.g., max N req/min, mandatory delay between varied shapes, etc.). Capture in `documentation/fda/api_observations.md` as a Finding N supplement. The (b) PR likely ships as B1 or B2 depending on how forgiving the pacing guidance is.
- **Branch C — Declined or no reply by 2026-06-11.** Treat as confirmation that the audit-pattern probing isn't an FDA-supported path. Move tier-2/3 probe execution to CI (one-off GitHub Actions workflow that runs probes from a datacenter IP). The (b) PR ships as B1 only (bulk-POST tier 1 fields), and tier-2/3 enrichment is deferred to a Phase 7 lookup-endpoint workstream when production runtime is on GitHub Actions anyway.

Document whichever branch we end up in by appending a "**Resolution**" subsection to `documentation/fda/api_observations.md` Finding N supplement (per Task 7) — with the specific guidance Kevin provided and the chosen branch.

### Task 6 — Pivot to USDA source audit

**What:** Begin USDA recalls + USDA establishments audit per `documentation/audit/methodology.md`. The deliverable plan flagged the firm-table relationship as a structural question (USDA's separate establishment API vs CPSC/FDA/NHTSA having firm inline with recalls) — that's the highest-value audit topic remaining.

**Why:** Two reasons to pivot now rather than wait on FDA: (1) USDA audit work proceeds independently of any FDA blocker, (2) the cross-source consolidation that resolves the (b) PR's B1/B2/B3 architecture decision needs all five sources audited anyway.

**How:**

1. Create the USDA audit doc: `documentation/usda/field_audit_2026_w22.md` (or `w23` if it slips). Use the FDA audit doc as the template.
2. Read the USDA API definitions PDFs at `documentation/usda/usda_fsis_recall_api_documentation.pdf` and `documentation/usda/usda_fsis_establishment_listing_api_data_documentation.pdf`.
3. Read the USDA bronze schemas (`src/schemas/usda.py`, `src/schemas/usda_establishment.py`) and staging models.
4. Build per-source inspect/probe scripts under `scripts/usda_recalls/audit/` and `scripts/usda_establishments/audit/`. Mirror the FDA pattern from `scripts/fda/audit/` (DEFAULT_CACHE_DIR pattern, three source modes, `--clear-cookies` if USDA also fronts Akamai per Finding O).
5. Validate against R2-landed payloads with `inspect_landed_payloads.py` analog.
6. Document findings against the firm-table-relationship question in particular: how does the USDA establishment API map to silver `firm` entries, and is the current `establishment_name` join (per `dbt/models/silver/firm.sql:75-86`) the right approach?

### Task 6b — Update audit methodology with the Akamai playbook

**What:** Append a "Probing Akamai-fronted APIs" section to `documentation/audit/methodology.md` capturing the learnings from this FDA cycle that generalize to USDA (also Akamai-fronted per `documentation/usda/recall_api_observations.md` Finding O) and possibly NHTSA / USCG when those audits happen.

**Why:** Without this, each per-source audit will re-discover the same Akamai-shaped problems. Codifying the playbook avoids reburning days on it.

**How:** Add a section to `methodology.md` covering:

- **Always start probing from the source's Bruno-tested request shape.** If the source has `bruno/<source>/` files exercising the API, the request shape there is empirically known to work. Vary from it cautiously — one parameter at a time, with at least 5+ minute gaps if exploring multiple shapes.
- **Don't request fields outside the source's "stable bulk" displaycolumns/equivalent set.** For FDA, that meant `codeinformation` lives on a separate per-product endpoint; mixing it into bulk POST got us STATUSCODE 406 and tripped Akamai's per-IP scoring. Other sources likely have analogous tier-2/3 fields.
- **Recognize the `_abck=...~-1~...` cookie signal.** If a response sets an `_abck` cookie with all `-1` values across validation slots, we're in Akamai's flagged-session state. Stop probing until cooldown — continued requests deepen the block.
- **Recognize `AkamaiGHost` + `x-reference-error: <id>` in response headers.** That's Akamai's documented "blocked request" reference. Capture the ID for any future outreach.
- **Cooldown is time-based, not request-based.** ≥24 hours from the last block is a safe wait. Cookies (`--clear-cookies` in our probe scripts) don't reset the per-IP score.
- **Static IP whitelist is the supported escape valve.** FDA OII offered this on 04/30; USDA may have an analogous program. Worth pursuing per source.

### Task 7 — Update `documentation/fda/api_observations.md` Finding N supplement

**What:** Append a new section to Finding N (or new Finding) documenting the 2026-05-28 observations.

**Why:** Future-us / future-Claude will hit similar issues and waste time rediscovering. The Bruno file's "codeinformation is per-product, not bulk" insight + the Akamai 204-silent-block + cookie `~-1~` signal pattern all deserve preservation.

**How:** Append to `documentation/fda/api_observations.md` after Finding O. Title: "**N supplement — 2026-05-28 probe-validation observations.**" Contents:

- 204 silent-block is a distinct Akamai failure mode from Finding N's documented 302→HTML→404 path; both signal bot-detection but at different severity tiers
- `_abck` cookie's trailing `~-1~-1~-1~-1~-1` indicates an unsolved Akamai sensor challenge
- Per-IP scoring is triggered by *shape-variance*, not just request rate; an invalid request producing STATUSCODE 406 (FDA API rejection) elevates the score noticeably
- `bruno/fda/incremental_extraction/post_recalls_eventlmd_range.yml` line 161-162 establishes the architectural rule: `codeinformation` is fetched per-product, never in bulk POST
- Mitigation tiers (in order): (a) probe with stable bulk-POST shapes, (b) use lookup endpoints for tier-2/3 fields, (c) get static IP whitelisted by FDA OII

### Task 8 — Cross-source consolidation + the (a) silver-remap PR

**What:** Once all five sources (CPSC, FDA, USDA recalls, USDA establishments, NHTSA, USCG) have per-source `field_audit_<period>.md` docs, build `documentation/audit/cross_source_consolidation.md` and use it to drive the (a) silver-remap PR.

**Why:** The (a) PR is the user-visible payoff of Phase 6a — it fixes the silver field mismappings without requiring extraction changes. It needs cross-source alignment (e.g., does `recall_event.description` stay named that, or rename to `recall_event.recall_reason`?) which only the consolidation step can answer.

**How:**

1. Create `documentation/audit/cross_source_consolidation.md` with one section per semantic concept (product description, recall reason / hazard narrative, distribution area, classification / severity, lifecycle dates, firm address, etc.). For each concept, populate a row per source showing the contributing field name, its current silver mapping, and the proposed silver column.
2. Decide column renames (e.g., `description` → `recall_reason`) based on what the data actually represents across the union of sources.
3. Build the (a) PR on a new branch off main: `feature/silver-field-remap` per the convention in `documentation/audit/capture_expansion_backlog.md`. Edits limited to `dbt/models/silver/*.sql` (and possibly `dbt/models/staging/*.sql`). No bronze schema migration, no extractor change.
4. `dbt build` to verify silver populates correctly with the new mappings.
5. PR review against the per-source audit docs' Decisions-locked sections — every change in (a) PR should trace to a decision in some source's audit.

This task closes Phase 6a. The (b) PR (capture expansion) is a separate workstream, possibly on a different branch, and likely follows after Phase 6b (firm resolution) per `project_scope/phase-6-execution-plan.md`.

## Cross-references

- `documentation/audit/methodology.md` — methodology for source audits
- `documentation/audit/capture_expansion_backlog.md` § FDA — three-tier architecture, B1/B2/B3 decision pending consolidation
- `documentation/fda/field_audit_2026_w22.md` — FDA audit findings + §8 R2 validation status
- `documentation/fda/api_observations.md` Finding N — original 2026-04-28 anti-abuse documentation
- `project_scope/phase-6-execution-plan.md` — Phase 6a foundation audit deliverables
- `prompts/phase_6_deliverable_plan.md` — Phase 6 add-on workstreams (field-association audit is #2)
