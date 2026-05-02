Migration: migrations/versions/0009_add_extraction_runs_change_type.py adds change_type TEXT NOT NULL DEFAULT 'routine'    
with a CHECK constraint on (routine, schema_rebaseline, hash_helper_rebaseline, historical_seed).
                                                                                                                            
CLI: recalls extract and recalls deep-rescan both now accept --change-type=<value>, validated against the enum before the  
run starts.
                                                                                                                            
Threading: Extractor.run(change_type='routine') → _record_run(..., change_type=change_type) → extraction_runs.change_type  
insert. The _extraction_runs SQLAlchemy Table metadata in each of the four concrete extractors (cpsc, fda, usda,
usda_establishment) gained the change_type column.                                                                         
                                                        
Schemas (ADR 0027):                                                                                                        
- fda.py — dropped _normalize_str, _FdaNullableStr; nullable text fields are now plain str | None.
- usda.py — dropped _normalize_str, _UsdaNullableStr; same pattern.                                                        
- usda_establishment.py — dropped _strip_list_elements, _FsisStrippedStrList, _FsisNullableStr; renamed
_normalize_false_sentinel → _coerce_false_to_text returning "false" (string) instead of None. activities/dbas are now plain
list[str]. county/geolocation default to "" (column is TEXT NOT NULL).                                                    
- cpsc.py — unchanged per audit.                                       
                                                                                                                            
Staging models (silver-side normalization):               
- stg_fda_recalls.sql — nullif(col, '') wrappers on every nullable text column.                                            
- stg_usda_fsis_recalls.sql — same.                                                                                        
- stg_cpsc_recalls.sql — unchanged.                                                                                        
                                                                                                                            
Tests: schema unit tests flipped to expect '' preserved on nullable text fields; FDA's nullable-date test stays
(storage-forced); USDA establishment's TestNormalizeFalseSentinel → TestCoerceFalseToText with assertion flipped to ==     
"false"; TestStripListElements deleted.                   
                                                                                                                            
TODO 37 audit: documented as a negative result in project_scope/implementation_plan.md "Architectural follow-ups → Shared  
annotated types and invariants audit." Net: post-ADR-0027, the remaining patterns are mostly source-specific; the bar for
shared extraction is "evidence from three sources that the abstraction is real." Discipline guidance recorded for NHTSA /  
USCG.                                                     

ADR 0013 verification: confirmed already implemented at src/extractors/fda.py:331-344. No code change.                     

What you run next                                                                                                          
                                                        
# 1. Apply the new migration to your dev Neon branch                                                                       
uv run alembic upgrade head                                                                                                

# 2. Quality gates                                                                                                         
uv run ruff check                                         
uv run ruff format --check                                                                                                 
uv run pyright src tests scripts                          
uv run pytest

TODO: STOPPED HERE ON 2026-05-01. DO THIS FIRST.
1. Edit .envrc — append the four AWS exports after the existing dotenv .env line. I can make the edit if you want; or you can paste them in by hand if you'd rather see exactly what changes.                                                             
2. direnv allow — every .envrc modification requires re-trusting it. Until you do, direnv will refuse to load the file and the new exports won't take effect. You'll see a message like direnv: error .envrc is blocked. Run \direnv allow` to approve its
content` if you forget.                                                                                                                                                                                                                                  
1. Smoke-test — quickest confirmation that the aliases worked:
echo $AWS_ENDPOINT_URL   # should print https://<account_id>.r2.cloudflarestorage.com                                                                                                                                                                     
aws s3 ls                # should list your dev bucket without any flags
1. If aws s3 ls errors with Could not connect to the endpoint URL, the export didn't take — re-check direnv allow. If it errors with An error occurred (InvalidAccessKeyId) or similar, the credential alias is wrong (probably a typo in the var name).  
                                                                                                                                                                                                                                                         
Then you're back on the original sequence — the re-run of uv run pytest to confirm the test fix is green, the rest of the quality gates (ruff / pyright / dbt parse), the alembic upgrade if not yet applied, then the re-baseline waves.  
                                                                                                                            
# 3. dbt parse to confirm staging SQL compiles                                                                             
uv run dbt parse --project-dir dbt                                                                                         
                                                                                                                            
Then the re-baseline waves on dev (per ADR 0027 §"Re-extract wave"):                                                       

uv run recalls extract cpsc                                            # expect zero new bronze rows                       
uv run recalls extract fda --change-type=schema_rebaseline             # medium wave                                       
uv run recalls extract usda --change-type=schema_rebaseline            # medium wave                                       
uv run recalls extract usda_establishments --change-type=schema_rebaseline  # ~14% second wave                             
                                                                                                                            
Verify wave sizes with:                                                                                                    
                                                                                                                            
psql $NEON_DATABASE_URL -c "                                                                                               
select source, change_type, started_at, records_extracted, records_inserted
from extraction_runs where started_at >= now() - interval '2 hours'                                                      
order by started_at desc"
                                                                                                                            
Then uv run dbt build --project-dir dbt to confirm the silver layer recomputes cleanly off the new bronze rows. Once you're
satisfied, open the PR — the description should paste the actual wave sizes plus the src/extractors/fda.py:331-344        
evidence for the ADR 0013 verification. 