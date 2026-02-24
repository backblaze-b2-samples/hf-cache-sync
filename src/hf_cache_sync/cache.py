"""Local Hugging Face hub cache scanner."""

from __future__ import annotations

import hashlib
import os
import time
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class BlobInfo:
    path: Path
    hash: str
    size: int
    atime: float


@dataclass
class FileEntry:
    relative_path: str
    blob_hash: str
    size: int


@dataclass
class RevisionInfo:
    repo_id: str
    revision: str
    snapshot_dir: Path
    files: list[FileEntry] = field(default_factory=list)
    atime: float = 0.0
    total_size: int = 0


@dataclass
class RepoInfo:
    repo_id: str
    repo_dir: Path
    revisions: list[RevisionInfo] = field(default_factory=list)
    blobs: dict[str, BlobInfo] = field(default_factory=dict)


def repo_dir_to_id(dirname: str) -> str:
    """Convert directory name like 'models--org--name' to 'org/name'."""
    parts = dirname.split("--", 1)
    if len(parts) < 2:
        return dirname
    # strip type prefix (models, datasets, spaces)
    return parts[1].replace("--", "/")


def scan_cache(cache_dir: Path) -> list[RepoInfo]:
    """Scan the HF hub cache directory and return structured repo info."""
    repos: list[RepoInfo] = []

    if not cache_dir.is_dir():
        return repos

    for entry in sorted(cache_dir.iterdir()):
        if not entry.is_dir():
            continue
        name = entry.name
        if not (name.startswith("models--") or name.startswith("datasets--") or name.startswith("spaces--")):
            continue

        repo = _scan_repo(entry)
        if repo:
            repos.append(repo)

    return repos


def _scan_repo(repo_dir: Path) -> RepoInfo | None:
    repo_id = repo_dir_to_id(repo_dir.name)
    repo = RepoInfo(repo_id=repo_id, repo_dir=repo_dir)

    blobs_dir = repo_dir / "blobs"
    if blobs_dir.is_dir():
        for blob_path in blobs_dir.iterdir():
            if blob_path.is_file():
                stat = blob_path.stat()
                repo.blobs[blob_path.name] = BlobInfo(
                    path=blob_path,
                    hash=blob_path.name,
                    size=stat.st_size,
                    atime=stat.st_atime,
                )

    snapshots_dir = repo_dir / "snapshots"
    if snapshots_dir.is_dir():
        for snap in sorted(snapshots_dir.iterdir()):
            if not snap.is_dir():
                continue
            rev = _scan_revision(repo_id, snap, repo.blobs)
            repo.revisions.append(rev)

    return repo if repo.revisions or repo.blobs else None


def _scan_revision(repo_id: str, snap_dir: Path, blobs: dict[str, BlobInfo]) -> RevisionInfo:
    rev = RevisionInfo(
        repo_id=repo_id,
        revision=snap_dir.name,
        snapshot_dir=snap_dir,
    )
    latest_atime = 0.0

    for root, _dirs, files in os.walk(snap_dir):
        for fname in files:
            fpath = Path(root) / fname
            rel = str(fpath.relative_to(snap_dir))

            blob_hash = _resolve_blob_hash(fpath, blobs)
            if not blob_hash:
                continue

            stat = fpath.stat()
            size = stat.st_size
            atime = stat.st_atime
            latest_atime = max(latest_atime, atime)

            # Use the blob's actual size if available
            if blob_hash in blobs:
                size = blobs[blob_hash].size

            rev.files.append(FileEntry(
                relative_path=rel,
                blob_hash=blob_hash,
                size=size,
            ))
            rev.total_size += size

    rev.atime = latest_atime or time.time()
    return rev


def _resolve_blob_hash(fpath: Path, blobs: dict[str, BlobInfo]) -> str | None:
    """Resolve a snapshot file to its blob hash."""
    # If it's a symlink, resolve to blob name
    if fpath.is_symlink():
        target = fpath.resolve()
        if target.parent.name == "blobs":
            return target.name
        return None

    # If it's a real file, compute sha256
    if fpath.is_file():
        return _sha256_file(fpath)

    return None


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8 * 1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def get_active_refs(repo_dir: Path) -> dict[str, str]:
    """Return {ref_name: commit_hash} for a repo's refs."""
    refs: dict[str, str] = {}
    refs_dir = repo_dir / "refs"
    if refs_dir.is_dir():
        for ref_file in refs_dir.iterdir():
            if ref_file.is_file():
                refs[ref_file.name] = ref_file.read_text().strip()
    return refs


def total_cache_size(repos: list[RepoInfo]) -> int:
    """Sum all unique blob sizes across repos."""
    seen: set[str] = set()
    total = 0
    for repo in repos:
        for h, blob in repo.blobs.items():
            if h not in seen:
                seen.add(h)
                total += blob.size
    return total
