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
    # Phase 5a: FDA iRES credentials — not required until FDA extractor is implemented
    fda_authorization_user: SecretStr | None = None
    fda_authorization_key: SecretStr | None = None


# No module-level Settings() instance here.
# R2 secrets are not in .env until Phase 1 quality gates are met.
# Add `settings = Settings()` singleton in Phase 2 once all secrets are provisioned.
