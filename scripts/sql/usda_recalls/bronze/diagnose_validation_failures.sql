-- Diagnose a USDA validation-failure wave (ADR 0014 schema-drift triage).
--
-- Context: `recalls extract usda` on 2026-06-06 rejected 2006/2006 records at the
-- `validate` stage (UsdaFsisRecord uses extra='forbid' + strict=True, so a new API field
-- OR a type change on a required field rejects every record). This isolates the latest
-- run's rejects and pinpoints the drift. Independent of Phase 6c — validation runs before
-- any presence-manifest code.
--
-- Usage:
--   psql "$NEON_DATABASE_URL" -f scripts/sql/usda_recalls/bronze/diagnose_validation_failures.sql
--
-- Read-only; re-runnable. All queries scope to the most recent rejection batch via its
-- shared raw_landing_path.

\set ON_ERROR_STOP on

\echo
\echo '=== 0) Latest USDA rejection batch (the run under diagnosis) ==='
SELECT raw_landing_path, count(*) AS rejected, min(rejected_at) AS first, max(rejected_at) AS last
  FROM usda_fsis_recalls_rejected
 WHERE raw_landing_path = (
         SELECT raw_landing_path FROM usda_fsis_recalls_rejected
          ORDER BY rejected_at DESC LIMIT 1
       )
 GROUP BY raw_landing_path;

\echo
\echo '=== 1) Rejections by stage (expect all at validate_records) ==='
SELECT failure_stage, count(*) AS n
  FROM usda_fsis_recalls_rejected
 WHERE raw_landing_path = (
         SELECT raw_landing_path FROM usda_fsis_recalls_rejected
          ORDER BY rejected_at DESC LIMIT 1
       )
 GROUP BY failure_stage
 ORDER BY n DESC;

\echo
\echo '=== 2) Error class tally (which kind of drift) ==='
SELECT
  CASE
    WHEN failure_reason ILIKE '%extra_forbidden%' THEN 'extra_forbidden — new/unexpected API field'
    WHEN failure_reason ILIKE '%type=missing%'    THEN 'missing — a required field is now absent'
    WHEN failure_reason ILIKE '%_type%' OR failure_reason ILIKE '%value_error%'
                                                  THEN 'type/value — strict-mode mismatch on a field'
    ELSE 'other — read the full reason in Q3'
  END AS error_class,
  count(*) AS n
  FROM usda_fsis_recalls_rejected
 WHERE raw_landing_path = (
         SELECT raw_landing_path FROM usda_fsis_recalls_rejected
          ORDER BY rejected_at DESC LIMIT 1
       )
 GROUP BY 1
 ORDER BY n DESC;

\echo
\echo '=== 3) Three full failure_reason examples (the Pydantic error names the field) ==='
SELECT source_recall_id, failure_reason
  FROM usda_fsis_recalls_rejected
 WHERE raw_landing_path = (
         SELECT raw_landing_path FROM usda_fsis_recalls_rejected
          ORDER BY rejected_at DESC LIMIT 1
       )
 ORDER BY id DESC
 LIMIT 3;

-- Q4/Q5: set-diff the actual API field set (from one rejected raw_record) against the
-- input keys UsdaFsisRecord accepts. `known` MUST mirror src/schemas/usda.py's
-- validation_aliases (+ the un-aliased `langcode`). Update it if the schema changes.
\echo
\echo '=== 4) NEW API field(s) not in the schema — the extra=forbid culprit ==='
WITH one AS (
  SELECT raw_record::jsonb AS rr
    FROM usda_fsis_recalls_rejected
   WHERE raw_landing_path = (
           SELECT raw_landing_path FROM usda_fsis_recalls_rejected
            ORDER BY rejected_at DESC LIMIT 1
         )
   ORDER BY id DESC LIMIT 1
),
api_keys AS (SELECT jsonb_object_keys(rr) AS k FROM one),
known(k) AS (VALUES
  ('field_recall_number'), ('langcode'), ('field_title'), ('field_recall_date'),
  ('field_recall_type'), ('field_recall_classification'), ('field_archive_recall'),
  ('field_has_spanish'), ('field_active_notice'), ('field_last_modified_date'),
  ('field_closed_date'), ('field_related_to_outbreak'), ('field_closed_year'),
  ('field_year'), ('field_risk_level'), ('field_recall_reason'), ('field_processing'),
  ('field_states'), ('field_establishment'), ('field_labels'), ('field_qty_recovered'),
  ('field_summary'), ('field_product_items'), ('field_distro_list'), ('field_media_contact'),
  ('field_company_media_contact'), ('field_recall_url'), ('field_en_press_release'),
  ('field_press_release'), ('field_recall_number_export')
)
SELECT k AS unexpected_api_field FROM api_keys
EXCEPT SELECT k FROM known
 ORDER BY 1;

\echo
\echo '=== 5) Schema fields MISSING from the API (renamed/removed?) ==='
WITH one AS (
  SELECT raw_record::jsonb AS rr
    FROM usda_fsis_recalls_rejected
   WHERE raw_landing_path = (
           SELECT raw_landing_path FROM usda_fsis_recalls_rejected
            ORDER BY rejected_at DESC LIMIT 1
         )
   ORDER BY id DESC LIMIT 1
),
api_keys AS (SELECT jsonb_object_keys(rr) AS k FROM one),
known(k) AS (VALUES
  ('field_recall_number'), ('langcode'), ('field_title'), ('field_recall_date'),
  ('field_recall_type'), ('field_recall_classification'), ('field_archive_recall'),
  ('field_has_spanish'), ('field_active_notice'), ('field_last_modified_date'),
  ('field_closed_date'), ('field_related_to_outbreak'), ('field_closed_year'),
  ('field_year'), ('field_risk_level'), ('field_recall_reason'), ('field_processing'),
  ('field_states'), ('field_establishment'), ('field_labels'), ('field_qty_recovered'),
  ('field_summary'), ('field_product_items'), ('field_distro_list'), ('field_media_contact'),
  ('field_company_media_contact'), ('field_recall_url'), ('field_en_press_release'),
  ('field_press_release'), ('field_recall_number_export')
)
SELECT k AS missing_expected_field FROM known
EXCEPT SELECT k FROM api_keys
 ORDER BY 1;

\echo
\echo '=== 6) Per-field TYPE distribution across the whole batch (the decisive view) ==='
\echo '    string = old scalar shape · array = new shape · a field showing BOTH = heterogeneous'
WITH batch AS (
  SELECT raw_record::jsonb AS rr
    FROM usda_fsis_recalls_rejected
   WHERE raw_landing_path = (
           SELECT raw_landing_path FROM usda_fsis_recalls_rejected
            ORDER BY rejected_at DESC LIMIT 1
         )
),
kv AS (
  SELECT key, jsonb_typeof(value) AS jtype
    FROM batch, LATERAL jsonb_each(rr)
)
SELECT key AS api_field, jtype, count(*) AS n
  FROM kv
 GROUP BY key, jtype
 ORDER BY key, jtype;
