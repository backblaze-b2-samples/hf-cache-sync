"""Tests for config loading."""

from hf_cache_sync.config import (
    CRED_SOURCE_AWS_ENV,
    CRED_SOURCE_B2_ENV,
    CRED_SOURCE_CONFIG,
    CRED_SOURCE_NONE,
    AppConfig,
    _credentials_from_env,
    _default_hf_cache_dir,
    has_env_credentials,
    load_config,
)


def test_load_missing_config(tmp_path):
    cfg = load_config(tmp_path / "nonexistent.yaml")
    assert isinstance(cfg, AppConfig)
    assert cfg.cache.max_local_gb == 50.0


def test_load_config(tmp_path):
    p = tmp_path / ".hf-cache-sync.yaml"
    p.write_text("""\
storage:
  endpoint: https://s3.example.com
  bucket: test-bucket
  region: us-east-1

cache:
  max_local_gb: 100

team:
  prefix: myteam/
  allow_gated: true
""")
    cfg = load_config(p)
    assert cfg.storage.bucket == "test-bucket"
    assert cfg.storage.endpoint == "https://s3.example.com"
    assert cfg.cache.max_local_gb == 100.0
    assert cfg.team.prefix == "myteam/"
    assert cfg.team.allow_gated is True
    assert cfg.remote_prefix == "myteam/"


def test_remote_prefix_empty():
    cfg = AppConfig()
    assert cfg.remote_prefix == ""


def test_hf_hub_cache_env(tmp_path, monkeypatch):
    """HF_HUB_CACHE should take priority."""
    custom = tmp_path / "custom_cache"
    monkeypatch.setenv("HF_HUB_CACHE", str(custom))
    assert _default_hf_cache_dir() == custom


def test_hf_home_env(tmp_path, monkeypatch):
    """HF_HOME should be used when HF_HUB_CACHE is not set."""
    monkeypatch.delenv("HF_HUB_CACHE", raising=False)
    hf_home = tmp_path / "hf_home"
    monkeypatch.setenv("HF_HOME", str(hf_home))
    assert _default_hf_cache_dir() == hf_home / "hub"


def _clear_env(monkeypatch):
    for var in (
        "B2_APPLICATION_KEY_ID",
        "B2_APPLICATION_KEY",
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "B2_ENDPOINT",
        "B2_BUCKET",
        "B2_REGION",
    ):
        monkeypatch.delenv(var, raising=False)


def test_b2_env_credentials_preferred(monkeypatch):
    """B2_* aliases take precedence over AWS_* when both are set."""
    _clear_env(monkeypatch)
    monkeypatch.setenv("B2_APPLICATION_KEY_ID", "b2-id")
    monkeypatch.setenv("B2_APPLICATION_KEY", "b2-secret")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "aws-id")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "aws-secret")
    assert _credentials_from_env() == ("b2-id", "b2-secret", CRED_SOURCE_B2_ENV)
    assert has_env_credentials() is True


def test_aws_env_credentials_fallback(monkeypatch):
    """AWS_* are used when B2_* are absent."""
    _clear_env(monkeypatch)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "aws-id")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "aws-secret")
    assert _credentials_from_env() == ("aws-id", "aws-secret", CRED_SOURCE_AWS_ENV)


def test_b2_pair_atomic(monkeypatch):
    """A lone B2_APPLICATION_KEY_ID must not mix with AWS_SECRET_ACCESS_KEY."""
    _clear_env(monkeypatch)
    monkeypatch.setenv("B2_APPLICATION_KEY_ID", "b2-id")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "aws-id")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "aws-secret")
    # B2 pair is incomplete, so we fall through to the AWS pair entirely.
    assert _credentials_from_env() == ("aws-id", "aws-secret", CRED_SOURCE_AWS_ENV)


def test_no_env_credentials(monkeypatch):
    _clear_env(monkeypatch)
    assert _credentials_from_env() == ("", "", CRED_SOURCE_NONE)
    assert has_env_credentials() is False


def test_load_config_b2_env_fills_missing_yaml_creds(tmp_path, monkeypatch):
    """B2_* env vars hydrate creds when the storage block omits them."""
    _clear_env(monkeypatch)
    monkeypatch.setenv("B2_APPLICATION_KEY_ID", "b2-id")
    monkeypatch.setenv("B2_APPLICATION_KEY", "b2-secret")
    p = tmp_path / ".hf-cache-sync.yaml"
    p.write_text("storage:\n  endpoint: https://s3.example.com\n  bucket: b\n  region: r\n")
    cfg = load_config(p)
    assert cfg.storage.access_key == "b2-id"
    assert cfg.storage.secret_key == "b2-secret"
    assert cfg.storage.credentials_source == CRED_SOURCE_B2_ENV


def test_load_config_env_creds_without_storage_block(tmp_path, monkeypatch):
    """Env-only setup works even when no storage: block exists in YAML."""
    _clear_env(monkeypatch)
    monkeypatch.setenv("B2_APPLICATION_KEY_ID", "b2-id")
    monkeypatch.setenv("B2_APPLICATION_KEY", "b2-secret")
    p = tmp_path / ".hf-cache-sync.yaml"
    p.write_text("cache:\n  max_local_gb: 10\n")
    cfg = load_config(p)
    assert cfg.storage.access_key == "b2-id"
    assert cfg.storage.secret_key == "b2-secret"
    assert cfg.storage.credentials_source == CRED_SOURCE_B2_ENV


def test_env_credentials_override_yaml(tmp_path, monkeypatch):
    """Env credentials must override YAML even when YAML has values set."""
    _clear_env(monkeypatch)
    monkeypatch.setenv("B2_APPLICATION_KEY_ID", "env-id")
    monkeypatch.setenv("B2_APPLICATION_KEY", "env-secret")
    p = tmp_path / ".hf-cache-sync.yaml"
    p.write_text(
        "storage:\n"
        "  endpoint: https://s3.example.com\n"
        "  bucket: b\n"
        "  region: r\n"
        "  access_key: yaml-id\n"
        "  secret_key: yaml-secret\n"
    )
    cfg = load_config(p)
    assert cfg.storage.access_key == "env-id"
    assert cfg.storage.secret_key == "env-secret"
    assert cfg.storage.credentials_source == CRED_SOURCE_B2_ENV


def test_yaml_credentials_when_no_env(tmp_path, monkeypatch):
    """YAML creds are used when no env credentials are set; source=config."""
    _clear_env(monkeypatch)
    p = tmp_path / ".hf-cache-sync.yaml"
    p.write_text(
        "storage:\n"
        "  endpoint: https://s3.example.com\n"
        "  bucket: b\n"
        "  region: r\n"
        "  access_key: yaml-id\n"
        "  secret_key: yaml-secret\n"
    )
    cfg = load_config(p)
    assert cfg.storage.access_key == "yaml-id"
    assert cfg.storage.secret_key == "yaml-secret"
    assert cfg.storage.credentials_source == CRED_SOURCE_CONFIG


def test_b2_storage_env_overrides_yaml(tmp_path, monkeypatch):
    """B2_ENDPOINT / B2_BUCKET / B2_REGION override their YAML counterparts."""
    _clear_env(monkeypatch)
    monkeypatch.setenv("B2_ENDPOINT", "https://env.example.com")
    monkeypatch.setenv("B2_BUCKET", "env-bucket")
    monkeypatch.setenv("B2_REGION", "env-region")
    p = tmp_path / ".hf-cache-sync.yaml"
    p.write_text(
        "storage:\n"
        "  endpoint: https://yaml.example.com\n"
        "  bucket: yaml-bucket\n"
        "  region: yaml-region\n"
    )
    cfg = load_config(p)
    assert cfg.storage.endpoint == "https://env.example.com"
    assert cfg.storage.bucket == "env-bucket"
    assert cfg.storage.region == "env-region"


def test_b2_storage_env_only_no_yaml(tmp_path, monkeypatch):
    """Storage settings can come from env even when no YAML exists."""
    _clear_env(monkeypatch)
    # Make sure default-path discovery doesn't pick up the dev's own YAML.
    monkeypatch.setattr("hf_cache_sync.config._default_config_paths", lambda: [])
    monkeypatch.setenv("B2_ENDPOINT", "https://env.example.com")
    monkeypatch.setenv("B2_BUCKET", "env-bucket")
    monkeypatch.setenv("B2_REGION", "env-region")
    monkeypatch.setenv("B2_APPLICATION_KEY_ID", "b2-id")
    monkeypatch.setenv("B2_APPLICATION_KEY", "b2-secret")
    cfg = load_config(tmp_path / "nonexistent.yaml")
    assert cfg.storage.endpoint == "https://env.example.com"
    assert cfg.storage.bucket == "env-bucket"
    assert cfg.storage.region == "env-region"
    assert cfg.storage.credentials_source == CRED_SOURCE_B2_ENV
