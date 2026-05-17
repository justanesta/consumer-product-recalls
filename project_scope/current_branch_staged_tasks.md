  psql "$NEON_DATABASE_URL" -f scripts/sql/cpsc/bronze/assert_products_array_append_only.sql
  psql "$NEON_DATABASE_URL" -f scripts/sql/cpsc/bronze/assert_name_model_normalization_stable.sql
  psql "$NEON_DATABASE_URL" -f scripts/sql/fda/bronze/assert_productid_stable.sql
  psql "$NEON_DATABASE_URL" -f scripts/sql/fda/bronze/assert_eventlmd_correlates_with_content_change.sql
  psql "$NEON_DATABASE_URL" -f scripts/sql/usda_recalls/bronze/assert_bilingual_atomic_update.sql
  psql "$NEON_DATABASE_URL" -f scripts/sql/usda_recalls/bronze/assert_field_last_modified_date_advances_on_edit.sql
  dbt test --select test_type:singular path:tests/source_assumptions


Do I still need to dbt build and do what's in the screenshot for the acceptance-criterion test for wave 2? Will doing another recall that I think will get tagged as routine but without the etag_enabled mess up our ETag viability and audit check? Is there another more ideal way we can verify a yaml edit takes effect with no code change?


Suggested operator sequence

  uv sync                                                      # install bs4 + lxml
  pyright src/extractors/_html_scraping.py src/extractors/uscg.py src/schemas/uscg.py   # confirm bs4 errors clear
  pytest tests/extractors/test_html_scraping_base.py tests/extractors/test_uscg_extractor.py -v
  alembic upgrade head                                         # apply migration 0013
  recalls deep-rescan uscg --change-type=historical_seed       # ~30 min initial seed (1,763 records)
  recalls extract uscg                                         # steady-state verify (~72s, 0 inserts)
