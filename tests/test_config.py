"""Tests for config loading."""

from hf_cache_sync.config import AppConfig, _default_hf_cache_dir, load_config


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
