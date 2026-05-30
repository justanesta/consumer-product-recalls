-- Phase 5d Step 7 Step 5/6 — empirical validation of the USCG manufacturer
-- directory landing + the §3 Bug 3 firm-rescue measurement.
--
-- Run AFTER `dbt build --select stg_uscg_manufacturers+` has succeeded; the
-- output numbers feed the Step 6 audit-doc fold-in (specifically
-- documentation/uscg/field_audit_2026_w22.md §3 Bug 3 final rescue count and
-- §6 firm-relationship update).
--
-- Companion to documentation/uscg/manufacturer_scraping_observations.md —
-- the open questions deferred to Step 3 corpus-scale validation are
-- empirically answered by these queries:
--   • Q2 answers: how many of the §3 Bug 3 "0-10 mic-only-no-name" rows
--     actually rescue via the directory (collapses the range to a number)
--   • Q3 answers: cross-source coverage % (recall MICs that resolve)
--   • Q4 answers: did the staging sentinel coercion catch UNK / dash / ''?
--   • Q5 answers: MIC namespace distribution (Finding I — digit block
--     101-126 reserved for engine makers vs 3-letter alpha for others)
--   • Q6 answers: state distribution (Finding G — Canadian provinces
--     present despite search-form dropdown gap)
--
-- Run with:
--   PGPASSWORD="$NEON_PASSWORD" psql -h "$NEON_HOST" -U "$NEON_USER" -d "$NEON_DBNAME" \
--     -f scripts/sql/uscg_manufacturers/silver/measure_rescue_and_coverage.sql

\echo '=== Q1: Silver row count + per-field NULL rates ==='
-- Confirms the bronze→staging pass-through preserves all 16,263 records and
-- shows the empirical NULL rate per field after staging's sentinel coercion.
-- Per Finding F.3 these reflect post-coercion state ('UNK' and '-' and ''
-- all map to NULL in stg_uscg_manufacturers).
select
  count(*)                                                                    as silver_row_count,
  round(100.0 * sum(case when company_name is null then 1 else 0 end) / count(*), 2)
                                                                              as company_name_null_pct,
  round(100.0 * sum(case when address is null then 1 else 0 end) / count(*), 2)
                                                                              as address_null_pct,
  round(100.0 * sum(case when city is null then 1 else 0 end) / count(*), 2)
                                                                              as city_null_pct,
  round(100.0 * sum(case when state is null then 1 else 0 end) / count(*), 2)
                                                                              as state_null_pct,
  round(100.0 * sum(case when uscg_directory_id is null then 1 else 0 end) / count(*), 2)
                                                                              as uscg_directory_id_null_pct
from stg_uscg_manufacturers;

\echo ''
\echo '=== Q2: §3 Bug 3 rescue measurement (the load-bearing number for Step 6) ==='
-- Counts USCG recalls that had populated MIC + NULL company_name in bronze,
-- and resolved to a directory company_name via the LEFT JOIN. These are the
-- "mic-only-no-name rescue" rows from the Phase 6a audit §3 Bug 3 framing.
-- Audit predicted 0-10; this resolves it to an actual integer.
--
-- If rescued = 0: the §3 Bug 3 Option 3 soft-fail still drops these recalls
--   from firm dim (no directory entry for the MICs in question). The dbt
--   relationships test on recall_event_firm passes trivially since nothing
--   changed.
-- If rescued > 0: those firm_ids changed in the firm dim during the
--   dbt build. recall_event_firm must be rebuilt with
--   `dbt build --select recall_event_firm` so its firm_id references
--   match the new firm dim.
-- Case-insensitive JOIN matches the firm.sql / recall_event_firm.sql
-- USCG branches (which case-normalize per the Step 3 finding that recalls
-- bronze contains lowercase MICs like 'blb' that match directory 'BLB').
select count(distinct upper(trim(r.mic))) as bug_3_rescued_recall_mics
from stg_uscg_recalls r
join stg_uscg_manufacturers m on upper(trim(r.mic)) = upper(trim(m.mic))
where r.company_name is null
  and r.mic is not null;

\echo ''
\echo '=== Q2b: Rescue MIC enumeration (which MICs got the rescue) ==='
-- Companion to Q2 — names the specific MICs that benefited from the rescue.
-- Useful for spot-checking the §3 Bug 3 example assertions ("YDV → YAMAHA
-- DEALER VENTURES" style claims in the audit doc).
select distinct
  upper(trim(r.mic))          as rescued_mic,
  m.company_name              as directory_company_name
from stg_uscg_recalls r
join stg_uscg_manufacturers m on upper(trim(r.mic)) = upper(trim(m.mic))
where r.company_name is null
  and r.mic is not null
order by upper(trim(r.mic));

\echo ''
\echo '=== Q3: Cross-source coverage (recalls→directory match rate) ==='
-- Phase 6b firm-resolution input — measures the % of distinct USCG recall
-- MICs that resolve against the live directory. Per Finding S, ~93.2% of
-- recalls have populated mic; this query asks "of those, how many find a
-- matching directory entry?" Orphans (recall MIC not in directory) suggest
-- either retired MICs or directory staleness.
-- Case-insensitive JOIN — see Q2 comment for rationale.
select
  count(distinct upper(trim(r.mic)))                                              as recalls_mics_total,
  count(distinct case when m.mic is not null then upper(trim(r.mic)) end)         as recalls_mics_matched_in_directory,
  count(distinct upper(trim(r.mic))) -
    count(distinct case when m.mic is not null then upper(trim(r.mic)) end)       as recalls_mics_orphaned,
  round(100.0 * count(distinct case when m.mic is not null then upper(trim(r.mic)) end)
        / nullif(count(distinct upper(trim(r.mic))), 0), 2)                       as match_pct
from stg_uscg_recalls r
left join stg_uscg_manufacturers m on upper(trim(r.mic)) = upper(trim(m.mic))
where r.mic is not null;

\echo ''
\echo '=== Q3b: Orphaned recall MICs (in recalls but not in directory) ==='
-- Companion to Q3 — names the specific recall MICs that did NOT find a
-- directory entry. Two plausible explanations: (a) directory snapshot is
-- lagging an active MIC (unlikely — we just ran an extract), (b) the MIC
-- was retired and removed from the directory while recall rows referencing
-- it persist. Worth a small sample for the audit doc.
-- Case-insensitive JOIN — see Q2 comment. Orphans here are the genuine
-- unresolvable MICs (retired regulatory codes like '111', sentinel values
-- like '999' / 'N/A', not case-mismatch artifacts).
select upper(trim(r.mic)) as orphan_mic, count(*) as recall_rows_referencing
from stg_uscg_recalls r
left join stg_uscg_manufacturers m on upper(trim(r.mic)) = upper(trim(m.mic))
where r.mic is not null
  and m.mic is null
group by upper(trim(r.mic))
order by recall_rows_referencing desc
limit 20;

\echo ''
\echo '=== Q4: Sentinel coercion sanity (should all be 0) ==='
-- Verifies the staging-layer sentinel coercion ('UNK' / '-' / '' → NULL).
-- All four counters should return 0; any non-zero indicates a sentinel
-- pattern leaked through to silver, which means stg_uscg_manufacturers.sql
-- needs an additional CASE WHEN branch.
select
  sum(case when company_name in ('UNK', '-') then 1 else 0 end)  as company_sentinels_leaked,
  sum(case when address in ('UNK', '-') then 1 else 0 end)       as address_sentinels_leaked,
  sum(case when city in ('UNK', '-') then 1 else 0 end)          as city_sentinels_leaked,
  sum(case when state = '' then 1 else 0 end)                    as state_empty_strings_leaked
from stg_uscg_manufacturers;

\echo ''
\echo '=== Q5: MIC namespace distribution (Finding I — digit block + alpha block) ==='
-- Finding I observed three sub-namespaces in Step 1 probes:
--   • 100-199 numeric reserved for major engine/component makers
--   • 3-letter alpha for all others (US + foreign per regulation)
--   • Gaps from retired/withdrawn MICs (e.g., 111 missing on page 0)
-- Corpus-scale confirmation of the sub-namespace shape.
select
  case
    when mic ~ '^[0-9]{3}$'         then 'digit_block (engine makers)'
    when mic ~ '^[A-Z]{3}$'         then 'alpha_block (US + foreign hulls)'
    when mic ~ '^[A-Z0-9]{3}$'      then 'mixed_alphanumeric'
    when length(mic) <> 3           then 'wrong_length (drift suspect)'
    else 'other'
  end                                                                          as mic_format_class,
  count(*)                                                                     as rows,
  round(100.0 * count(*) / sum(count(*)) over (), 2)                           as pct,
  min(mic)                                                                     as first_mic,
  max(mic)                                                                     as last_mic
from stg_uscg_manufacturers
group by 1
order by rows desc;

\echo ''
\echo '=== Q6: State distribution top 20 (Finding G — Canadian provinces present) ==='
-- Finding G observed Canadian provinces (BC, ON, AB, NS, NL, QC) in State
-- despite their absence from the search-form dropdown. Corpus-scale view
-- shows the actual top-20 and which jurisdictions dominate.
select
  coalesce(state, '<NULL>')                                                    as state,
  count(*)                                                                     as rows,
  round(100.0 * count(*) / sum(count(*)) over (), 2)                           as pct
from stg_uscg_manufacturers
group by state
order by rows desc
limit 20;

\echo ''
\echo '=== Q7: Firm dim USCG impact (firms reachable via USCG MIC) ==='
-- After the LEFT JOIN rescue, how many distinct firms in the firm dim
-- have a company_id matching a USCG MIC? Proxy for "USCG firms in the
-- dim after the rescue". Compare against the §3 Bug 3 baseline implicit
-- in the Phase 6a audit (the lost ~33 mic-only-no-name rows). If the
-- delta from Q2 above matches the rescue count, the LEFT JOIN is
-- structurally working as designed.
--
-- Caveat: USCG MIC values are 3-char alphanumeric and could theoretically
-- collide with a CPSC CompanyID; not a concern in practice given CPSC's
-- CompanyID empirical emptiness (CPSC §3 Bug 3), but documented for
-- completeness.
select
  count(*)                                                  as firms_with_uscg_mic_match,
  count(distinct canonical_name)                            as distinct_canonical_names
from firm
where observed_company_ids ?| (select array_agg(distinct mic) from stg_uscg_manufacturers);

\echo ''
\echo '=== Q7b: Firm dim USCG impact (via recall_event_firm join chain — authoritative) ==='
-- Authoritative measurement: distinct firms reachable from USCG recall
-- events via the recall_event_firm bridge. Requires recall_event_firm
-- to be current with the rebuilt firm dim — if Q2 > 0 and you have not
-- run `dbt build --select recall_event_firm` yet, this query may return
-- a stale count or be inconsistent with Q7. Run dbt build first.
select count(distinct ref.firm_id) as uscg_reachable_firms
from recall_event_firm ref
join recall_event re on re.recall_event_id = ref.recall_event_id
where re.source = 'USCG';
