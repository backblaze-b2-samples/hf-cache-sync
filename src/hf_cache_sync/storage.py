"""S3-compatible object storage backend."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import boto3
from botocore.exceptions import ClientError

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client

from hf_cache_sync.config import AppConfig


class StorageBackend:
    def __init__(self, config: AppConfig):
        self.config = config
        self.bucket = config.storage.bucket
        self.prefix = config.remote_prefix
        self._client: S3Client | None = None

    @property
    def client(self) -> S3Client:
        if self._client is None:
            kwargs: dict = {
                "service_name": "s3",
                "region_name": self.config.storage.region or None,
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
        except ClientError:
            return False

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
                    k = k[len(self.prefix):]
                keys.append(k)
        return keys
