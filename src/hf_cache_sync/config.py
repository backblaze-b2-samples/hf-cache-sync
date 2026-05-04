"""Configuration loading and defaults."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

import yaml

# Credential source labels exposed via doctor.
CRED_SOURCE_B2_ENV = "b2_env"
CRED_SOURCE_AWS_ENV = "aws_env"
CRED_SOURCE_CONFIG = "config"
CRED_SOURCE_NONE = "none"


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


def _credentials_from_env() -> tuple[str, str, str]:
    """Resolve (access_key, secret_key, source) from env.

    Order: B2_APPLICATION_KEY_ID/B2_APPLICATION_KEY first, then
    AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY. Each pair is treated atomically
    — never mix a B2 id with an AWS secret — because the two sets typically
    belong to different accounts.
    """
    if (b2_id := os.environ.get("B2_APPLICATION_KEY_ID")) and (
        b2_key := os.environ.get("B2_APPLICATION_KEY")
    ):
        return b2_id, b2_key, CRED_SOURCE_B2_ENV
    if (aws_id := os.environ.get("AWS_ACCESS_KEY_ID")) and (
        aws_key := os.environ.get("AWS_SECRET_ACCESS_KEY")
    ):
        return aws_id, aws_key, CRED_SOURCE_AWS_ENV
    return "", "", CRED_SOURCE_NONE


def has_env_credentials() -> bool:
    """True iff a complete credential pair is available via env vars."""
    _, _, source = _credentials_from_env()
    return source != CRED_SOURCE_NONE


@dataclass
class StorageConfig:
    endpoint: str = ""
    bucket: str = ""
    region: str = ""
    access_key: str = ""
    secret_key: str = ""
    # Where the resolved credentials came from. Set by load_config; manual
    # construction (e.g. in tests) leaves this at "none" unless overridden.
    credentials_source: str = CRED_SOURCE_NONE


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
    """Load config from YAML, then overlay env overrides.

    Precedence (highest first):
        1. B2_APPLICATION_KEY_ID / B2_APPLICATION_KEY (credentials)
        2. AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY (credentials)
        3. YAML access_key / secret_key (credentials)
        4. B2_ENDPOINT / B2_BUCKET / B2_REGION (storage settings)
        5. YAML storage block (storage settings)

    Env credentials override YAML even when YAML fields are populated, so a
    `.env` file is enough to drive the whole tool. Doctor surfaces the
    resolved source so users can see exactly which path was taken.
    """
    if path and path.exists():
        cfg = _parse_config(path)
    else:
        cfg = AppConfig()
        for p in _default_config_paths():
            if p.exists():
                cfg = _parse_config(p)
                break

    _apply_storage_env_overrides(cfg.storage)
    _resolve_credentials(cfg.storage)
    return cfg


def _apply_storage_env_overrides(storage: StorageConfig) -> None:
    """Overlay B2_ENDPOINT / B2_BUCKET / B2_REGION onto the storage config.

    Env wins over YAML when set. Empty/unset env vars are ignored so the
    YAML value (if any) is preserved.
    """
    if endpoint := os.environ.get("B2_ENDPOINT"):
        storage.endpoint = endpoint
    if bucket := os.environ.get("B2_BUCKET"):
        storage.bucket = bucket
    if region := os.environ.get("B2_REGION"):
        storage.region = region


def _resolve_credentials(storage: StorageConfig) -> None:
    """Set storage.access_key/secret_key/credentials_source per precedence.

    Env credentials override YAML when present. The atomic-pair rule means
    a partial env pair (e.g. only B2_APPLICATION_KEY_ID) is ignored entirely.
    """
    env_access, env_secret, env_source = _credentials_from_env()
    if env_source != CRED_SOURCE_NONE:
        storage.access_key = env_access
        storage.secret_key = env_secret
        storage.credentials_source = env_source
        return

    if storage.access_key and storage.secret_key:
        storage.credentials_source = CRED_SOURCE_CONFIG
        return

    storage.credentials_source = CRED_SOURCE_NONE


def _parse_config(path: Path) -> AppConfig:
    raw = yaml.safe_load(path.read_text()) or {}
    cfg = AppConfig()

    if s := raw.get("storage"):
        cfg.storage = StorageConfig(
            endpoint=s.get("endpoint", ""),
            bucket=s.get("bucket", ""),
            region=s.get("region", ""),
            access_key=s.get("access_key", ""),
            secret_key=s.get("secret_key", ""),
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
