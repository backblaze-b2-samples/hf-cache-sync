"""Manifest creation and serialization."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field


@dataclass
class ManifestFile:
    path: str
    blob: str
    size: int


@dataclass
class Manifest:
    repo: str
    revision: str
    files: list[ManifestFile] = field(default_factory=list)

    def to_json(self) -> str:
        return json.dumps(asdict(self), indent=2)

    @classmethod
    def from_json(cls, data: str) -> Manifest:
        raw = json.loads(data)
        files = [ManifestFile(**f) for f in raw.get("files", [])]
        return cls(repo=raw["repo"], revision=raw["revision"], files=files)

    @property
    def remote_key(self) -> str:
        safe_repo = self.repo.replace("/", "__")
        return f"manifests/{safe_repo}@{self.revision}.json"

    def blob_keys(self) -> list[str]:
        return [f"blobs/{f.blob}" for f in self.files]
