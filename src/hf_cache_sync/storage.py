"""S3-compatible object storage backend."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING
from urllib.parse import urlparse

import boto3
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client

from hf_cache_sync import __version__
from hf_cache_sync.config import AppConfig

B2_USER_AGENT = "b2ai-hfcache"
DEFAULT_USER_AGENT = "hf-cache-sync"

# S3 / B2 codes that mean "the object does not exist" rather than a real failure.
NOT_FOUND_CODES = {"404", "NoSuchKey", "NoSuchBucket"}


def _is_backblaze_endpoint(endpoint: str) -> bool:
    """Check if the endpoint is a Backblaze B2 S3-compatible endpoint."""
    if not endpoint:
        return False
    host = urlparse(endpoint).hostname or ""
    return "backblazeb2.com" in host


def get_user_agent(endpoint: str) -> str:
    """Return the appropriate user-agent string for the storage endpoint."""
    if _is_backblaze_endpoint(endpoint):
        return f"{B2_USER_AGENT}/{__version__}"
    return f"{DEFAULT_USER_AGENT}/{__version__}"


def is_not_found(err: ClientError) -> bool:
    """True iff the ClientError is a 404 / NoSuchKey class of error."""
    code = err.response.get("Error", {}).get("Code", "")
    return code in NOT_FOUND_CODES


class StorageBackend:
    def __init__(self, config: AppConfig):
        self.config = config
        self.bucket = config.storage.bucket
        self.prefix = config.remote_prefix
        self._client: S3Client | None = None

    @property
    def client(self) -> S3Client:
        if self._client is None:
            user_agent = get_user_agent(self.config.storage.endpoint)
            boto_config = BotoConfig(
                user_agent_extra=user_agent,
                # Standard mode covers throttling + transient 5xx with sensible backoff.
                retries={"max_attempts": 5, "mode": "standard"},
            )
            kwargs: dict = {
                "service_name": "s3",
                "region_name": self.config.storage.region or None,
                "config": boto_config,
            }
            if self.config.storage.endpoint:
                kwargs["endpoint_url"] = self.config.storage.endpoint
            if self.config.storage.access_key:
                kwargs["aws_access_key_id"] = self.config.storage.access_key
            if self.config.storage.secret_key:
                kwargs["aws_secret_access_key"] = self.config.storage.secret_key
            self._client = boto3.client(**kwargs)
        return self._client

    def _key(self, key: str) -> str:
        return f"{self.prefix}{key}" if self.prefix else key

    def exists(self, key: str) -> bool:
        try:
            self.client.head_object(Bucket=self.bucket, Key=self._key(key))
            return True
        except ClientError as e:
            if is_not_found(e):
                return False
            raise

    def upload_file(self, local_path: Path, key: str) -> None:
        self.client.upload_file(str(local_path), self.bucket, self._key(key))

    def upload_bytes(self, data: bytes, key: str) -> None:
        self.client.put_object(Bucket=self.bucket, Key=self._key(key), Body=data)

    def download_file(self, key: str, local_path: Path) -> None:
        local_path.parent.mkdir(parents=True, exist_ok=True)
        self.client.download_file(self.bucket, self._key(key), str(local_path))

    def download_bytes(self, key: str) -> bytes:
        resp = self.client.get_object(Bucket=self.bucket, Key=self._key(key))
        return resp["Body"].read()

    def list_keys(self, prefix: str = "") -> list[str]:
        full_prefix = self._key(prefix)
        keys: list[str] = []
        paginator = self.client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=full_prefix):
            for obj in page.get("Contents", []):
                k = obj["Key"]
                if self.prefix and k.startswith(self.prefix):
                    k = k[len(self.prefix) :]
                keys.append(k)
        return keys
