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
                         
That doc is your Step 1 PR. Step 2 (schema + extractor + migration) builds against the documented shape.