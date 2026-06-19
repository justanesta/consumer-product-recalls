-- Silver/Gold Provenance Audit — COVERAGE-GAP queries (completeness critic, 2026-06-19)
-- READ-ONLY. These close gaps the per-group reconcile pass did NOT cover. Schemas as in the other files.
-- Each block states the CONFORMING result. Blocks tagged [CONFIRM NAMES] reference objects this audit did
-- not statically verify (bronze.* tables, firm_crosswalk, replayed_from_run_id); confirm the table/column
-- names against the live catalog before running, or skip.
-- ============================================================================================

-- ----------------------------------------------------------------------------------
-- event_type_not_null_single_value   [enum_domain]   target: public.recall_event.event_type
-- purpose : ADR 0003 — event_type is TEXT NOT NULL DEFAULT 'RECALL'; v1 domain is the single value {'RECALL'}.
--           Entirely uncovered by the 123 per-group queries. (If the column does not exist, that is itself a finding.)
-- CONFORMS: exactly one row, event_type='RECALL', n_null=0, across all sources.
-- ----------------------------------------------------------------------------------
-- FINDING (2026-W25): event_type is ABSENT from public.recall_event — ADR 0003's
-- `event_type TEXT NOT NULL DEFAULT 'RECALL'` was never implemented. This existence probe
-- returns 0 (graceful) instead of erroring; event_type_column_exists = 0 IS the finding.
select count(*) as event_type_column_exists
from information_schema.columns
where table_schema = 'public' and table_name = 'recall_event' and column_name = 'event_type';

-- ----------------------------------------------------------------------------------
-- gold_serving_event_type_filter   [enum_domain]   target: gold serving model SQL (STATIC check)
-- purpose : ADR 0003 — gold views must be explicit about which event_types they aggregate, else a future
--           non-RECALL event silently inflates every mart/fact. This is a STATIC-ANALYSIS check (today all
--           rows are RECALL, so live data cannot reveal a missing filter). Run the grep below against the repo:
--             grep -L "event_type" dbt/models/gold/mart_*.sql dbt/models/gold/fct_*.sql
--           Any gold model that joins recall_event but does NOT constrain event_type is a forward-compat gap.
-- CONFORMS: every gold model sourced from recall_event either carries WHERE event_type='RECALL' or documents
--           why the filter is unnecessary (e.g. it only ever sees RECALL rows by construction).
-- ----------------------------------------------------------------------------------
-- (no SQL — static repo check; see grep above)

-- ----------------------------------------------------------------------------------
-- recall_event_id_md5_recipe   [key_recipe]   target: public.recall_event.recall_event_id
-- purpose : ADR 0031/0042 — the API computes recall_event_id = md5(UPPER(source) || '|' || source_recall_id)
--           from URL path params. Uniqueness tests do NOT prove the RECIPE (a wrong separator/casing is still
--           unique). Byte-verify the recipe directly. NB: source is already UPPERCASE in silver, so md5(source||...).
-- CONFORMS: bad_recipe = 0.
-- ----------------------------------------------------------------------------------
select count(*) as bad_recipe
from public.recall_event
where recall_event_id <> md5(source || '|' || source_recall_id);

-- ----------------------------------------------------------------------------------
-- distribution_states_vs_state_codes_non_conflation   [type]   target: public.mart_recall_summary
-- purpose : ADR 0042 — distribution_states (scalar text) must not be conflated with distribution_state_codes
--           (text[]). Detect any array-formatted string leaking into the scalar column.
-- CONFORMS: n_scalar_looks_like_array = 0.
-- ----------------------------------------------------------------------------------
select count(*) filter (where distribution_states ~ '^\s*\{')          as n_scalar_looks_like_array,
       count(*) filter (where distribution_states is not null)          as n_scalar_present,
       count(*) filter (where distribution_state_codes is not null)     as n_codes_present
from public.mart_recall_summary;

-- ----------------------------------------------------------------------------------
-- mart_distribution_arrays_null_vs_empty   [nullability]   target: public.mart_recall_summary
-- purpose : LEFT JOIN to recall_distribution_area re-introduces NULL on the two array columns that silver
--           coalesces to '{}'. Quantify NULL vs '{}' so the API contract (NULL allowed here, unlike the O1
--           always-non-null product_names/models/hins/firms) is documented from data, not assumed.
-- CONFORMS: informational — record the NULL count; decide coalesce-to-'{}' vs documented-nullable (fix #6).
-- ----------------------------------------------------------------------------------
select count(*) filter (where distribution_state_codes is null)             as state_codes_null,
       count(*) filter (where distribution_state_codes = '{}')              as state_codes_empty,
       count(*) filter (where distribution_country_codes is null)           as country_codes_null,
       count(*) filter (where distribution_country_codes = '{}')            as country_codes_empty
from public.mart_recall_summary;

-- ----------------------------------------------------------------------------------
-- press_release_grain_key_and_release_type   [grain + enum_domain]   target: public.recall_event_press_release
-- purpose : Verify the surrogate-key grain + that url (a key component) is never null/'' , and enumerate the
--           open release_type domain (flagged as empty-string risk with no verification query).
-- CONFORMS: n_rows = n_distinct_id; n_null_id = 0; n_bad_url = 0; release_type has no '' bucket.
-- ----------------------------------------------------------------------------------
select count(*)                                                  as n_rows,
       count(distinct recall_event_press_release_id)             as n_distinct_id,
       count(*) filter (where recall_event_press_release_id is null) as n_null_id,
       count(*) filter (where url is null or url = '')           as n_bad_url
from public.recall_event_press_release;

select release_type, count(*) as n
from public.recall_event_press_release
group by release_type
order by n desc;

-- ----------------------------------------------------------------------------------
-- recall_event_history_intentional_empty_present   [normalization]   target: public.recall_event_history
-- purpose : old_value/new_value carry '' BY DESIGN (a tracked field value -> '' is a real clearing edit).
--           Confirm '' is PRESENT (not accidentally normalized away). This is the inverse of the other
--           empty-string checks: here zero would be the anomaly.
-- CONFORMS: n_empty_old_value + n_empty_new_value > 0 (the deliberate clearing-edit signal survives).
-- ----------------------------------------------------------------------------------
select count(*) filter (where old_value = '') as n_empty_old_value,
       count(*) filter (where new_value = '') as n_empty_new_value
from public.recall_event_history;

-- ----------------------------------------------------------------------------------
-- firm_crosswalk_match_confidence_vocabulary   [enum_domain]   [CONFIRM NAMES]
-- purpose : firm_crosswalk.match_confidence flows via coalesce(...) into recall_event_firm.match_confidence,
--           which carries an error-severity 17-value accepted_values. A stray crosswalk value (the documented
--           'rapidfuzz_high' vs 'rapidfuzz_rollup' mismatch) hard-FAILS the dbt build at the bridge, not here.
--           Confirm the crosswalk table name/schema (public.firm_crosswalk?) before running.
-- CONFORMS: every value is within recall_event_firm's accepted_values set; no surprise label.
-- ----------------------------------------------------------------------------------
select match_confidence, count(*) as n
from public.firm_crosswalk            -- [CONFIRM NAMES]
where match_confidence is not null
group by match_confidence
order by n desc;

-- ----------------------------------------------------------------------------------
-- bronze_content_hash_populated   [other]   [CONFIRM NAMES]
-- purpose : ADR 0007 — the latest-per-identity content_hash must always be populated (regression guard on
--           hashing.py). Bronze got ZERO coverage in this audit. Replace <bronze_table>/<identity> per source.
-- CONFORMS: n_null_hash = 0 for every source's bronze table.
-- ----------------------------------------------------------------------------------
-- select count(*) filter (where content_hash is null or content_hash = '') as n_null_hash
-- from bronze.<source>_bronze;        -- [CONFIRM NAMES] repeat per source
