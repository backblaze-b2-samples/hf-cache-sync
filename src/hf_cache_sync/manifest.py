"""Manifest creation and serialization."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field


def repo_to_safe_key(repo_id: str) -> str:
    """Encode a repo_id (e.g. ``org/name``) into a key-safe form (``org__name``).

    Used in remote keys for refs and manifests so the slash doesn't create
    spurious folder boundaries.
    """
    return repo_id.replace("/", "__")


def manifest_key(repo_id: str, revision: str) -> str:
    return f"manifests/{repo_to_safe_key(repo_id)}@{revision}.json"


def ref_key(repo_id: str, ref_name: str) -> str:
    return f"refs/{repo_to_safe_key(repo_id)}/{ref_name}"


@dataclass
class ManifestFile:
    path: str
    blob: str
    size: int


@dataclass
class Manifest:
    repo: str
    revision: str
    repo_type: str = "model"
    files: list[ManifestFile] = field(default_factory=list)

    def to_json(self) -> str:
        return json.dumps(asdict(self), indent=2)

    @classmethod
    def from_json(cls, data: str) -> Manifest:
        raw = json.loads(data)
        files = [ManifestFile(**f) for f in raw.get("files", [])]
        return cls(
            repo=raw["repo"],
            revision=raw["revision"],
            repo_type=raw.get("repo_type", "model"),
            files=files,
        )

    @property
    def remote_key(self) -> str:
        return manifest_key(self.repo, self.revision)

    def blob_keys(self) -> list[str]:
        return [f"blobs/{f.blob}" for f in self.files]
