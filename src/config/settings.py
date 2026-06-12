from __future__ import annotations

from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="forbid",
    )

    neon_database_url: SecretStr
    r2_account_id: str
    r2_access_key_id: SecretStr
    r2_secret_access_key: SecretStr
    r2_bucket_name: str
    dbt_project_dir: str = "dbt"
    dbt_profiles_dir: str = "dbt"
    # FDA iRES credentials — optional; only the FDA extractor reads them.
    fda_authorization_user: SecretStr | None = None
    fda_authorization_key: SecretStr | None = None


class DbSettings(BaseSettings):
    """Database-only settings for runtime commands that touch Postgres but never R2.

    ``resolve-firms`` / ``parse-quantities`` / ``audit-firm-rollups`` read the ``stg_*`` views and
    (re)build an enrichment crosswalk; they land no raw bytes, so they need only the Neon DSN.
    Constructing the full ``Settings`` would demand the four ``r2_*`` credentials even though they
    are unused — which forced the transform/audit crons to carry R2 secrets they never consume, and
    made a step that passed only ``NEON_DATABASE_URL`` fail at construction. ``extra="ignore"`` (vs
    ``Settings``' ``forbid``) lets the shared ``.env`` — which also holds ``R2_*`` / ``FDA_*`` —
    load cleanly when only the DSN is wanted.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    neon_database_url: SecretStr


# No module-level Settings()/DbSettings() instance: each CLI command constructs its settings at
# call time so env vars are read lazily (construction at import time would require the secrets to be
# present just to import the module).
