"""Configuration loading and defaults."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

import yaml


def _default_hf_cache_dir() -> Path:
    """Resolve the default HF hub cache dir, respecting HF env vars."""
    if hub_cache := os.environ.get("HF_HUB_CACHE"):
        return Path(hub_cache)
    hf_home = Path(os.environ.get("HF_HOME", Path.home() / ".cache" / "huggingface"))
    return hf_home / "hub"


def _default_config_paths() -> list[Path]:
    """Config search paths, evaluated at call time (not import time)."""
    return [
        Path.cwd() / ".hf-cache-sync.yaml",
        Path.home() / ".hf-cache-sync.yaml",
    ]


@dataclass
class StorageConfig:
    endpoint: str = ""
    bucket: str = ""
    region: str = ""
    access_key: str = ""
    secret_key: str = ""


@dataclass
class CacheConfig:
    max_local_gb: float = 50.0
    sync_xet: bool = False
    hf_cache_dir: str = ""


@dataclass
class TeamConfig:
    prefix: str = ""
    allow_gated: bool = False


@dataclass
class AppConfig:
    storage: StorageConfig = field(default_factory=StorageConfig)
    cache: CacheConfig = field(default_factory=CacheConfig)
    team: TeamConfig = field(default_factory=TeamConfig)

    @property
    def hf_cache_dir(self) -> Path:
        if self.cache.hf_cache_dir:
            return Path(self.cache.hf_cache_dir)
        return _default_hf_cache_dir()

    @property
    def remote_prefix(self) -> str:
        return self.team.prefix.strip("/") + "/" if self.team.prefix else ""


def load_config(path: Path | None = None) -> AppConfig:
    """Load config from YAML file. Returns defaults if no file found."""
    if path and path.exists():
        return _parse_config(path)
    for p in _default_config_paths():
        if p.exists():
            return _parse_config(p)
    return AppConfig()


def _parse_config(path: Path) -> AppConfig:
    raw = yaml.safe_load(path.read_text()) or {}
    cfg = AppConfig()

    if s := raw.get("storage"):
        cfg.storage = StorageConfig(
            endpoint=s.get("endpoint", ""),
            bucket=s.get("bucket", ""),
            region=s.get("region", ""),
            access_key=s.get("access_key", os.environ.get("AWS_ACCESS_KEY_ID", "")),
            secret_key=s.get("secret_key", os.environ.get("AWS_SECRET_ACCESS_KEY", "")),
        )

    if c := raw.get("cache"):
        cfg.cache = CacheConfig(
            max_local_gb=float(c.get("max_local_gb", 50)),
            sync_xet=bool(c.get("sync_xet", False)),
            hf_cache_dir=c.get("hf_cache_dir", ""),
        )

    if t := raw.get("team"):
        cfg.team = TeamConfig(
            prefix=t.get("prefix", ""),
            allow_gated=bool(t.get("allow_gated", False)),
        )

    return cfg
