  psql "$NEON_DATABASE_URL" -f scripts/sql/cpsc/bronze/assert_products_array_append_only.sql
  psql "$NEON_DATABASE_URL" -f scripts/sql/cpsc/bronze/assert_name_model_normalization_stable.sql
  psql "$NEON_DATABASE_URL" -f scripts/sql/fda/bronze/assert_productid_stable.sql
  psql "$NEON_DATABASE_URL" -f scripts/sql/fda/bronze/assert_eventlmd_correlates_with_content_change.sql
  psql "$NEON_DATABASE_URL" -f scripts/sql/usda_recalls/bronze/assert_bilingual_atomic_update.sql
  psql "$NEON_DATABASE_URL" -f scripts/sql/usda_recalls/bronze/assert_field_last_modified_date_advances_on_edit.sql
  dbt test --select test_type:singular path:tests/source_assumptions
