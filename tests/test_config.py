"""Tests for config loading."""

from pathlib import Path

from hf_cache_sync.config import AppConfig, load_config


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
