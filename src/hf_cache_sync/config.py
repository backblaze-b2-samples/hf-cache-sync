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


def _credentials_from_env() -> tuple[str, str]:
    """Resolve (access_key, secret_key) from env, preferring B2 aliases.

    Treats each pair atomically — never mixes a B2 id with an AWS secret —
    because the two sets typically belong to different accounts.
    """
    if (b2_id := os.environ.get("B2_APPLICATION_KEY_ID")) and (
        b2_key := os.environ.get("B2_APPLICATION_KEY")
    ):
        return b2_id, b2_key
    return (
        os.environ.get("AWS_ACCESS_KEY_ID", ""),
        os.environ.get("AWS_SECRET_ACCESS_KEY", ""),
    )


def has_env_credentials() -> bool:
    """True iff a complete credential pair is available via env vars."""
    access, secret = _credentials_from_env()
    return bool(access and secret)


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
    """Load config from YAML file. Returns defaults if no file found.

    Credentials missing from YAML are filled from env (B2_APPLICATION_KEY_ID /
    B2_APPLICATION_KEY take precedence over AWS_ACCESS_KEY_ID /
    AWS_SECRET_ACCESS_KEY) so an env-only setup works without a storage block.
    """
    if path and path.exists():
        cfg = _parse_config(path)
    else:
        cfg = AppConfig()
        for p in _default_config_paths():
            if p.exists():
                cfg = _parse_config(p)
                break

    if not (cfg.storage.access_key and cfg.storage.secret_key):
        env_access, env_secret = _credentials_from_env()
        if env_access and env_secret:
            cfg.storage.access_key = cfg.storage.access_key or env_access
            cfg.storage.secret_key = cfg.storage.secret_key or env_secret
    return cfg


def _parse_config(path: Path) -> AppConfig:
    raw = yaml.safe_load(path.read_text()) or {}
    cfg = AppConfig()
    env_access, env_secret = _credentials_from_env()

    if s := raw.get("storage"):
        cfg.storage = StorageConfig(
            endpoint=s.get("endpoint", ""),
            bucket=s.get("bucket", ""),
            region=s.get("region", ""),
            access_key=s.get("access_key", env_access),
            secret_key=s.get("secret_key", env_secret),
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
