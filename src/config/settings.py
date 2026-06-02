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


# No module-level Settings() instance: each CLI command constructs Settings() at call time
# so env vars are read lazily (construction at import time would require all secrets to be
# present just to import the module).
