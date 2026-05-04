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