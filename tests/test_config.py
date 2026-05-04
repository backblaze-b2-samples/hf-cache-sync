"""Tests for config loading."""

from hf_cache_sync.config import (
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


def _clear_cred_env(monkeypatch):
    for var in (
        "B2_APPLICATION_KEY_ID",
        "B2_APPLICATION_KEY",
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
    ):
        monkeypatch.delenv(var, raising=False)


def test_b2_env_credentials_preferred(monkeypatch):
    """B2_* aliases take precedence over AWS_* when both are set."""
    _clear_cred_env(monkeypatch)
    monkeypatch.setenv("B2_APPLICATION_KEY_ID", "b2-id")
    monkeypatch.setenv("B2_APPLICATION_KEY", "b2-secret")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "aws-id")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "aws-secret")
    assert _credentials_from_env() == ("b2-id", "b2-secret")
    assert has_env_credentials() is True


def test_aws_env_credentials_fallback(monkeypatch):
    """AWS_* are used when B2_* are absent."""
    _clear_cred_env(monkeypatch)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "aws-id")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "aws-secret")
    assert _credentials_from_env() == ("aws-id", "aws-secret")


def test_b2_pair_atomic(monkeypatch):
    """A lone B2_APPLICATION_KEY_ID must not mix with AWS_SECRET_ACCESS_KEY."""
    _clear_cred_env(monkeypatch)
    monkeypatch.setenv("B2_APPLICATION_KEY_ID", "b2-id")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "aws-id")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "aws-secret")
    # B2 pair is incomplete, so we fall through to the AWS pair entirely.
    assert _credentials_from_env() == ("aws-id", "aws-secret")


def test_no_env_credentials(monkeypatch):
    _clear_cred_env(monkeypatch)
    assert _credentials_from_env() == ("", "")
    assert has_env_credentials() is False


def test_load_config_b2_env_fills_missing_yaml_creds(tmp_path, monkeypatch):
    """B2_* env vars hydrate creds when the storage block omits them."""
    _clear_cred_env(monkeypatch)
    monkeypatch.setenv("B2_APPLICATION_KEY_ID", "b2-id")
    monkeypatch.setenv("B2_APPLICATION_KEY", "b2-secret")
    p = tmp_path / ".hf-cache-sync.yaml"
    p.write_text("storage:\n  endpoint: https://s3.example.com\n  bucket: b\n  region: r\n")
    cfg = load_config(p)
    assert cfg.storage.access_key == "b2-id"
    assert cfg.storage.secret_key == "b2-secret"


def test_load_config_env_creds_without_storage_block(tmp_path, monkeypatch):
    """Env-only setup works even when no storage: block exists in YAML."""
    _clear_cred_env(monkeypatch)
    monkeypatch.setenv("B2_APPLICATION_KEY_ID", "b2-id")
    monkeypatch.setenv("B2_APPLICATION_KEY", "b2-secret")
    p = tmp_path / ".hf-cache-sync.yaml"
    p.write_text("cache:\n  max_local_gb: 10\n")
    cfg = load_config(p)
    assert cfg.storage.access_key == "b2-id"
    assert cfg.storage.secret_key == "b2-secret"


def test_yaml_creds_win_over_env(tmp_path, monkeypatch):
    """Explicit YAML creds are not overridden by env vars."""
    _clear_cred_env(monkeypatch)
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
    assert cfg.storage.access_key == "yaml-id"
    assert cfg.storage.secret_key == "yaml-secret"
