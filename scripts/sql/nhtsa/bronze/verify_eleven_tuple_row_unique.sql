-- Phase 5c — verify the 11-tuple identity is row-unique in bronze after
-- the post-ADR-0030 dedup architecture is wired up.
--
-- Context: ADR 0030 (amended) specifies an 11-tuple bronze identity for
-- NHTSA, with hash_exclude_fields={"source_recall_id"}, within_batch_dedup,
-- and allow_null_identity. After the loader changes land and a successful
-- ``recalls extract nhtsa --since <date>`` completes, every bronze row's
-- 11-tuple should be unique — that's the correctness contract.
--
-- The diff between count(*) and count(distinct 11-tuple) tells us how
-- many "duplicate" identities exist in bronze:
--
--   excess_rows = 0 → 11-tuple is row-unique. Architecture validated;
--                     dedup-on-rerun is working end-to-end.
--   excess_rows > 0 → at least that many identities have multiple bronze
--                     rows. Either dedup-on-rerun failed (existing-hash
--                     match didn't fire), or the records are legitimate
--                     edits across runs (same identity, different
--                     content_hash). Q2 in the follow-up diagnostic
--                     (verify_eleven_tuple_row_unique_followup.sql, if
--                     written) disambiguates by inspecting per-collision
--                     timestamps + hash prefixes.
--
-- Specifically applies to checking whether the 19 May-2026 rows from the
-- earlier ``--since 2026-05-01`` extract correctly deduped against the
-- new ``--since 2024-01-01`` extract that includes them.

select
  count(*) as total_rows,
  count(distinct (
    campno, maketxt, modeltxt, yeartxt, compname,
    rcl_cmpt_id, mfr_comp_ptno, mfr_comp_desc, mfr_comp_name,
    endman, bgman
  )) as distinct_eleven_tuples,
  count(*) - count(distinct (
    campno, maketxt, modeltxt, yeartxt, compname,
    rcl_cmpt_id, mfr_comp_ptno, mfr_comp_desc, mfr_comp_name,
    endman, bgman
  )) as excess_rows
from nhtsa_recalls_bronze;


select                                                                                                                     
    count(*) as bronze_rows,                                
    count(distinct (campno, maketxt, modeltxt, yeartxt, compname, rcl_cmpt_id,                                               
                    mfr_comp_ptno, mfr_comp_desc, mfr_comp_name, endman, bgman))                                             
      as distinct_11_tuples                                                                                                  
  from nhtsa_recalls_bronze;

select                                                                                                                   
      date_trunc('month', rcdate)::date as month,
      count(*) as rows                                                                                                       
    from nhtsa_recalls_bronze
    where rcdate is not null                                                                                                 
    group by 1
    order by 1                                                                                                               
    limit 30;