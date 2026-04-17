# Should I have broken out/parameters only dev Neon env variables in my .env file if /src/config/settings.py will not accept them and only dbt is looking for them?
You added those four split vars to your .env — pydantic-settings read them all, then extra="forbid" rejected
   any key it didn't recognize as a declared field.

  That reveals the contract extra="forbid" enforces: the Settings class becomes the complete specification of what's allowed
  in your environment. Nothing can sneak in undeclared. If a dev accidentally adds a typo'd variable like
  NEON_DATABSE_URL=..., they get an immediate loud error rather than a silent misconfiguration.

  The two failure modes are now clear:
  - Missing required field → something declared in Settings isn't in .env
  - Extra input → something in .env isn't declared in Settings

  Since the NEON split vars are for dbt only (not consumed by Python code), they shouldn't be in Settings at all. They just
  need to live in .env without pydantic knowing about them. There are two ways to handle that:

  1. Change extra="forbid" to extra="ignore" — but that defeats the whole protection
  2. Tell pydantic-settings to only look at a specific env prefix, ignoring other vars

  The correct fix here is actually a third option: add an env_prefix or use env_ignore_empty — but most cleanly, we just need
   to not use extra="forbid" on vars that aren't Python-facing.

  The real solution: move the dbt split vars out of .env into a separate .env.dbt file that pydantic-settings never reads, OR
   declare them as optional fields in Settings. Since dbt reads them directly from the environment (not through pydantic),
  the cleanest move is to keep them out of .env entirely and set them only where dbt needs them.

  # Do I need 2 R2 buckets, one for local development and one for production?
  Yes, R2 credentials should be in .env for local development — but not the same credentials as production. This mirrors     
  exactly the Neon pattern:  
                                                                                                                             
  ┌──────┬────────────────────────┬─────────────────────────────┐
  │      │      Local (.env)      │ CI (GitHub Actions secrets) │                                                            
  ├──────┼────────────────────────┼─────────────────────────────┤                                                            
  │ Neon │ dev branch URL         │ main branch URL             │                                                            
  ├──────┼────────────────────────┼─────────────────────────────┤                                                            
  │ R2   │ dev bucket credentials │ prod bucket credentials     │                                                            
  └──────┴────────────────────────┴─────────────────────────────┘
                                                                                                                             
  Since R2 has no branch concept, you create a second bucket to get the same separation. The setup:                          
   
  1. Go to Cloudflare R2 → create a second bucket named consumer-product-recalls-dev                                         
  2. Create a new API token scoped to that dev bucket
  3. Put those dev credentials in your .env (R2_BUCKET_NAME=consumer-product-recalls-dev, etc.)                              
  4. Your GitHub Actions secrets already have the prod bucket — leave those as-is                                            
                                                                                                                             
  The reason this matters: when you run extractors locally in Phase 3+, raw payloads land in R2. You don't want local dev    
  runs writing into your production landing zone and polluting the audit trail.                                              
                                                                                                                             
  The code doesn't care — R2_BUCKET_NAME is just a string. Your local runs land in consumer-product-recalls-dev, CI lands in 
  consumer-product-recalls. Same object key structure, different bucket.
                                                                                                                             
  For right now (Phase 1): you don't have any code that actually writes to R2 yet. You can add those dev credentials to .env 
  now to get Settings() working, or wait until Phase 2 when R2 is first used. Either is fine — just don't block on it.