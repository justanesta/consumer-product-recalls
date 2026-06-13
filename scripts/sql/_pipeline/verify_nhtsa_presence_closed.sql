-- #71 acceptance gate: confirm a full-corpus NHTSA deep-rescan has banked the
-- presence manifest AND that recall_lifecycle's NHTSA presence dims
-- (is_currently_active / was_ever_retracted) are now populated. Until BOTH hold,
-- NHTSA presence is NULL-by-design (C16, ADR 0026) and TODO #71 must stay open.
--
-- The green dbt suite does NOT prove this — assert_recall_lifecycle_presence_
-- tracked_sources_only only checks that the UNTRACKED sources are NULL and stays
-- green whether or not NHTSA presence is populated. This script is the real gate.
--
-- Run AFTER `recalls deep-rescan nhtsa --change-type historical_seed` + a `dbt build`.
-- Read-only. No params. Reusable for the monthly presence-refresh verification.
--   pwr psql -f scripts/sql/_pipeline/verify_nhtsa_presence_closed.sql
--
-- Note the source-label case split: extraction_run_identities.source is the
-- pipeline name 'nhtsa' (lowercase); recall_lifecycle.source is the silver
-- canonical 'NHTSA' (uppercase).

\set ON_ERROR_STOP on
\pset null '<NULL>'

\echo
\echo '=== 1. NHTSA rows in the presence manifest (extraction_run_identities) ==='
\echo 'Expect > 0 (one row per enumerated campno). 0 = no full-corpus deep-rescan'
\echo 'has banked the manifest -> NHTSA presence stays NULL by design.'

select
    count(*)                    as nhtsa_manifest_rows,
    count(distinct eri.run_id)  as writing_runs
from extraction_run_identities eri
where eri.source = 'nhtsa';

\echo
\echo '=== 2. which run(s) wrote them — should be a historical_seed deep-rescan ==='

select
    er.run_id,
    er.change_type,
    er.started_at,
    count(*)                    as manifest_rows
from extraction_run_identities eri
join extraction_runs er on er.run_id = eri.run_id
where eri.source = 'nhtsa'
group by er.run_id, er.change_type, er.started_at
order by er.started_at desc;

\echo
\echo '=== 3. recall_lifecycle NHTSA presence-dim population (the #71 payoff) ==='
\echo 'nhtsa_with_active > 0 means the manifest flowed into silver. THIS is the gate.'

select
    count(*)                                                as nhtsa_total,
    count(*) filter (where is_currently_active is not null) as nhtsa_with_active,
    count(*) filter (where was_ever_retracted is not null)  as nhtsa_with_retracted,
    count(*) filter (where is_currently_active)             as nhtsa_currently_active
from recall_lifecycle
where source = 'NHTSA';

\echo
\echo '=== 4. guard: untracked sources (CPSC/FDA/USCG) must stay NULL ==='
\echo 'Mirrors assert_recall_lifecycle_presence_tracked_sources_only. Expect 0 rows.'

select source, count(*) as offending_rows
from recall_lifecycle
where source not in ('USDA', 'NHTSA')
  and (is_currently_active is not null or was_ever_retracted is not null)
group by source;

\echo
\echo '=== VERDICT (single-query gate) ==='

select case
    when (select count(*) from extraction_run_identities where source = 'nhtsa') > 0
     and (select count(*) from recall_lifecycle
          where source = 'NHTSA' and is_currently_active is not null) > 0
     and not exists (
            select 1 from recall_lifecycle
            where source not in ('USDA', 'NHTSA')
              and (is_currently_active is not null or was_ever_retracted is not null))
    then 'PASS — NHTSA presence populated; TODO #71 is closeable'
    else 'FAIL — NHTSA presence NOT populated (see sections above); #71 stays open'
end as verdict;
