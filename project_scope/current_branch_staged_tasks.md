# For main?
Concrete daily workflow                                                          
# Morning: continue NHTSA work
`git checkout feature/nhtsa-extractor`
# ... edit, commit ...
                                     
# You spot a monitoring drift while running daily extracts                                                                
`git stash` # park NHTSA WIP                                                     
`git checkout main && git pull`                                  
# ... fix the SQL bug, commit, push ...
`git checkout feature/nhtsa-extractor`

`git rebase main`                            # pick up the fix
`git stash pop`                              # resume NHTSA work

Quick interaction with the NHTSA workstream 

Since you'll have feature/nhtsa-extractor open in parallel:                 

# Morning: monitoring + maybe small fixes
`git checkout main && git pull`                  
`recalls extract <sources>`
# ... fix anything that comes up via tiny PR ...                                                                                                                                                                                                          
# Switch to NHTSA work later
`git checkout feature/nhtsa-extractor`                     
`git rebase main`   # pick up any monitoring fixes you just merged                                                                                                                                                                                          
# ... NHTSA work ...                                       
The rebase step is what keeps the NHTSA branch from drifting. Do it whenever you switch back, takes a second when there are no conflicts.

# For `feature/nhtsa-exploration-extractor-schema-migration
Concretely, Step 1 deliverables for NHTSA look like:                                                                                                                                                                                                      
```
mkdir documentation/nhtsa                                                                                                                                                                                                                                 
curl -O <NHTSA recall ZIP URL>                                                                                                                                                                                                                            
unzip -l <file>.zip                 # what's inside
unzip -p <file>.zip <inner.tsv> | head -50   # column headers + first rows                                                                                                                                                                                
unzip -p <file>.zip <inner.tsv> | wc -l      # row count                                                                                                                                                                                                  
```                                                                                                                                                                                                                                                     
Then write documentation/nhtsa/flat_file_observations.md capturing:                                                                                                                                                                  
- Download URL pattern (and whether it's stable or rotating)
- File size, row count encoding                                                                                                                                                    
- Column count + names + types observed
- Update cadence (re-download a day later, diff sizes/hashes)                                                                                                                       
- Any schema-drift history visible from prior versions if NHTSA archives them
- Whether Last-Modified HTTP header on the download is reliable for watermarking                                                                                                                                                                          
                         
That doc is your Step 1. Step 2 (schema + extractor + migration) builds against the documented shape.

1. Probe the watermark. Re-run curl -sI on the directory (or 2-3 files) tomorrow and confirm all Last-Modified values      
advance in lockstep. That nails down the watermark verdict.
2. Download and inspect. Pull these into data/exploratory/:                                                                
`cd data/exploratory/`
```
for f in FLAT_RCL_POST_2010 FLAT_RCL_PRE_2010 RCL_FROM_2025_2025 RCL_FROM_2025_2026 RCL_FROM_2000_2004; do                 
    curl -O https://static.nhtsa.gov/odi/ffdd/rcl/${f}.zip                                                                   
done
```                                                                                                                 
3. Confirm column count + encoding. Run the §2 + §3 probes from earlier on FLAT_RCL_POST_2010.zip (the canonical big file).
Verify it's 29 tab-delimited columns matching RCL.txt and document the encoding verdict.                                  
4. Confirm the rolling-window naming convention (the 2025_2025 vs 2025_2026 question). This shapes your
config/sources/nhtsa.yaml.                                                                                                 
5. Confirm the small year-band files' purpose. If they're stubs, ignore them and document why. If they're real slices, you
have an even cleaner per-year incremental option.                                                                          
6. Grab Import_Instructions_Recalls.pdf — it's the official format spec from 2023. RCL.txt is the data dictionary; the PDF
likely covers parsing rules, escape conventions, and known-edge-cases. Read it once, capture relevant findings into        
flat_file_observations.md, then drop the PDF (don't commit a 1 MB PDF). --> **UPDATE 2026-05-04**: This is a 12 page instruction file last updated Februrary 2014 of how to upload the data in FLAT_RCL.zip to a Microsoft Access database. Not sure how helpful it will be.
7. Write up findings. Each of the 6 bullets in the doc gets answered with evidence from these probes.

# Step 3 blocker — RECORD_ID is not a stable per-row natural key

Discovered 2026-05-07 during Step 3 first-extraction analysis. Two `recalls extract nhtsa --since 2024-01-01` runs (May 5 + May 7) produced 132,135 bronze rows where every one of the 66,057 prior `source_recall_id` values appears with a different content hash AND describes a different real-world recall. Evidence: `scripts/sql/nhtsa/bronze/diagnose_full_reinsert.sql` Q4 — id `255795` is a Vermeer BC900XL on May 5 and a Mercedes-Benz Sprinter 2500 on May 7. Q5 confirms the mechanism is NHTSA's file regeneration, not anything in the pipeline (May 5 22:39 + 22:44 same-day double run shared `inner_sha=edae1d2…` and dedup correctly produced 0 inserts; May 7 had `inner_sha=c955c37…` and dedup completely failed → 66,078 inserts). Hashing and `BronzeLoader` dedup logic are correct; the identity key choice is wrong. `src/schemas/nhtsa.py:148` and `src/extractors/nhtsa.py:414-416` docstrings asserting "RECORD_ID is NHTSA's stable per-row natural key per RCL.txt" are empirically wrong — NHTSA reassigns RECORD_ID on each file rebuild.

**Do not run another `recalls extract nhtsa` until this is resolved.** Each run dumps ~66k rows with rebinding RECORD_IDs.

Action plan, in order:

1. **Verify against RCL.txt.** Re-read `documentation/nhtsa/RCL.txt` for what field 1 actually documents. There's a real chance it's a transient row index, not a natural key, and the docstring misread it. The Phase 5c Step 1 source-exploration findings doc may be wrong on this point — flag for revision.

2. **Find a stable composite identity.** CAMPNO (e.g., `24V930000`) is the public NHTSA recall ID and is stable. The 1-to-many fan-out within a campaign is across `(maketxt, modeltxt, yeartxt, compname)`. Verify the right tuple is row-unique within a single TSV by running this against today's load only:

    ```sql
    -- Find composite-key collisions within May 7's snapshot.
    -- 0 rows = the composite uniquely identifies a TSV row.
    select campno, maketxt, modeltxt, yeartxt, compname, count(*)
    from nhtsa_recalls_bronze
    where extraction_timestamp::date = '2026-05-07'
    group by 1,2,3,4,5
    having count(*) > 1
    order by count(*) desc
    limit 20;
    ```

    If that returns rows, widen the tuple (add `mfgcampno` or `rcltype`) until it doesn't. The minimal unique tuple is the new identity.

3. **Truncate NHTSA bronze.** The polluted 132,135 rows have to go before re-extracting; otherwise the new identity-key dedup will compare against bogus prior content. Stage on the dev branch only:

    ```sql
    truncate table nhtsa_recalls_bronze;
    truncate table nhtsa_recalls_rejected;
    ```

    `extraction_runs` rows can stay — they're forensic and the change_type/inner_sha history is genuinely useful evidence for the Finding doc.

4. **Update schema + loader config.** Two reasonable shapes:
    - **(a)** Drop `source_recall_id` from the schema entirely; pass the composite as `identity_fields=("campno","maketxt","modeltxt","yeartxt","compname")` to `BronzeLoader`. Most honest — there is no per-row natural key from NHTSA. USDA already uses a 2-tuple per `loader.py` docstring lines 33-39, 65-70, so the composite-tuple pattern is supported.
    - **(b)** Keep `source_recall_id` as a synthetic key derived from the composite (SHA-prefix or concatenation done in `extract()`). Preserves cross-source uniformity at the cost of a bit of synthesis.

    Lean toward (a) — matches reality, and RECORD_ID stops being load-bearing and probably shouldn't even be stored.

5. **Document as a Finding** in `documentation/nhtsa/flat_file_observations.md`. Include the Q4 + Q5 evidence verbatim — the inner-SHA control comparison is what makes this airtight. This becomes the most consequential Step 3 finding, and Phase 5d/USCG should inherit the lesson: do not trust any field's "stable" claim in a flat-file source until you've observed two regenerations.

The watermark-correlation goal came out strengthened: Q5 already shows the inner-SHA stability oracle works (same content → same hash → dedup skip; different content → different hash → re-load). Once the identity key is right, that oracle correctly counts only the genuine 21-row delta as new data.