from __future__ import annotations

import pytest
from pydantic import ValidationError

from src.config.settings import Settings

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_REQUIRED_ENV = {
    "NEON_DATABASE_URL": "postgresql://user:pass@host/db",
    "R2_ACCOUNT_ID": "abc123",
    "R2_ACCESS_KEY_ID": "access-key",
    "R2_SECRET_ACCESS_KEY": "secret-key",
    "R2_BUCKET_NAME": "my-bucket",
}


def _set_required(monkeypatch: pytest.MonkeyPatch) -> None:
    for key, val in _REQUIRED_ENV.items():
        monkeypatch.setenv(key, val)


def _make_settings(**overrides: str) -> Settings:
    """Instantiate Settings with env file disabled, using overrides as kwargs."""
    return Settings(_env_file=None, **overrides)  # type: ignore[call-arg]


# ---------------------------------------------------------------------------
# Required fields — successful instantiation
# ---------------------------------------------------------------------------


def test_settings_instantiates_with_all_required_fields(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_required(monkeypatch)
    settings = Settings(_env_file=None)  # type: ignore[call-arg]
    assert settings.r2_account_id == "abc123"
    assert settings.r2_bucket_name == "my-bucket"


def test_settings_secret_str_fields_readable_via_get_secret_value(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_required(monkeypatch)
    settings = Settings(_env_file=None)  # type: ignore[call-arg]
    assert settings.neon_database_url.get_secret_value() == "postgresql://user:pass@host/db"
    assert settings.r2_access_key_id.get_secret_value() == "access-key"
    assert settings.r2_secret_access_key.get_secret_value() == "secret-key"


# ---------------------------------------------------------------------------
# Required fields — missing raises ValidationError
# ---------------------------------------------------------------------------


def test_settings_raises_validation_error_when_neon_database_url_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_required(monkeypatch)
    monkeypatch.delenv("NEON_DATABASE_URL", raising=False)
    with pytest.raises(ValidationError):
        Settings(_env_file=None)  # type: ignore[call-arg]


def test_settings_raises_validation_error_when_r2_account_id_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_required(monkeypatch)
    monkeypatch.delenv("R2_ACCOUNT_ID", raising=False)
    with pytest.raises(ValidationError):
        Settings(_env_file=None)  # type: ignore[call-arg]


def test_settings_raises_validation_error_when_r2_bucket_name_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_required(monkeypatch)
    monkeypatch.delenv("R2_BUCKET_NAME", raising=False)
    with pytest.raises(ValidationError):
        Settings(_env_file=None)  # type: ignore[call-arg]


# ---------------------------------------------------------------------------
# extra="forbid"
# ---------------------------------------------------------------------------


def test_settings_raises_validation_error_on_extra_field(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_required(monkeypatch)
    with pytest.raises(ValidationError):
        Settings(_env_file=None, UNKNOWN_FIELD="should-fail")  # type: ignore[call-arg]


# ---------------------------------------------------------------------------
# Optional FDA fields
# ---------------------------------------------------------------------------


def test_settings_fda_fields_default_to_none(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_required(monkeypatch)
    monkeypatch.delenv("FDA_AUTHORIZATION_USER", raising=False)
    monkeypatch.delenv("FDA_AUTHORIZATION_KEY", raising=False)
    settings = Settings(_env_file=None)  # type: ignore[call-arg]
    assert settings.fda_authorization_user is None
    assert settings.fda_authorization_key is None


def test_settings_fda_fields_populated_when_env_vars_set(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_required(monkeypatch)
    monkeypatch.setenv("FDA_AUTHORIZATION_USER", "fda-user")
    monkeypatch.setenv("FDA_AUTHORIZATION_KEY", "fda-key")
    settings = Settings(_env_file=None)  # type: ignore[call-arg]
    assert settings.fda_authorization_user is not None
    assert settings.fda_authorization_user.get_secret_value() == "fda-user"
    assert settings.fda_authorization_key is not None
    assert settings.fda_authorization_key.get_secret_value() == "fda-key"
