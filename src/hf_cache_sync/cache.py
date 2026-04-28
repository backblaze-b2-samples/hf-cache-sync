"""Local Hugging Face hub cache scanner."""

from __future__ import annotations

import hashlib
import os
import time
from dataclasses import dataclass, field
from pathlib import Path

# HF hub uses pluralized type prefixes in directory names.
REPO_TYPE_FROM_DIR: dict[str, str] = {
    "models": "model",
    "datasets": "dataset",
    "spaces": "space",
}
DIR_PREFIX_FROM_TYPE: dict[str, str] = {v: f"{k}--" for k, v in REPO_TYPE_FROM_DIR.items()}


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
    repo_type: str
    repo_dir: Path
    revisions: list[RevisionInfo] = field(default_factory=list)
    blobs: dict[str, BlobInfo] = field(default_factory=dict)

    @property
    def dir_prefix(self) -> str:
        return DIR_PREFIX_FROM_TYPE.get(self.repo_type, "models--")


def parse_repo_dirname(dirname: str) -> tuple[str, str]:
    """Parse 'models--org--name' into (repo_type, repo_id).

    Returns:
        (repo_type, repo_id) e.g. ("model", "org/name")
    """
    parts = dirname.split("--", 1)
    if len(parts) < 2:
        return "model", dirname
    type_prefix = parts[0]  # "models", "datasets", "spaces"
    repo_type = REPO_TYPE_FROM_DIR.get(type_prefix, "model")
    repo_id = parts[1].replace("--", "/")
    return repo_type, repo_id


def repo_id_to_dirname(repo_id: str, repo_type: str = "model") -> str:
    """Convert repo_id and type to HF cache directory name.

    Example: ("org/name", "model") -> "models--org--name"
    """
    prefix = DIR_PREFIX_FROM_TYPE.get(repo_type, "models--")
    return prefix + repo_id.replace("/", "--")


def scan_cache(cache_dir: Path) -> list[RepoInfo]:
    """Scan the HF hub cache directory and return structured repo info."""
    repos: list[RepoInfo] = []

    if not cache_dir.is_dir():
        return repos

    for entry in sorted(cache_dir.iterdir()):
        if not entry.is_dir():
            continue
        name = entry.name
        if not any(name.startswith(f"{p}--") for p in REPO_TYPE_FROM_DIR):
            continue

        repo = _scan_repo(entry)
        if repo:
            repos.append(repo)

    return repos


def _scan_repo(repo_dir: Path) -> RepoInfo | None:
    repo_type, repo_id = parse_repo_dirname(repo_dir.name)
    repo = RepoInfo(repo_id=repo_id, repo_type=repo_type, repo_dir=repo_dir)

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

            rev.files.append(
                FileEntry(
                    relative_path=rel,
                    blob_hash=blob_hash,
                    size=size,
                )
            )
            rev.total_size += size

    rev.atime = latest_atime or time.time()
    return rev


def _resolve_blob_hash(fpath: Path, blobs: dict[str, BlobInfo]) -> str | None:
    """Resolve a snapshot file to its blob hash.

    HF blobs may be sha1 (git-stored) or sha256 (LFS-stored).
    For symlinks we use the target filename directly.
    For real files we check against known blob names.
    """
    if fpath.is_symlink():
        target = fpath.resolve()
        # Validate the resolved name is a known blob
        if target.name in blobs:
            return target.name
        return None

    # Real file (e.g. Windows fallback) — match by content against known blobs
    if fpath.is_file():
        # First try matching by filename if it's in blobs (hardlink case)
        if fpath.name in blobs:
            return fpath.name
        # Fall back to sha256 for unknown files
        return sha256_file(fpath)

    return None


def sha256_file(path: Path) -> str:
    """Compute SHA-256 hex digest of a file."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8 * 1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


XET_POINTER_MAX_SIZE = 1024
XET_POINTER_MARKERS = (b"version https://", b"oid sha256:", b"oid xet:")

GATED_LICENSE_KEYWORDS = (
    "agreement",
    "meta llama",
    "acceptable use",
    "community license",
    "you must accept",
    "gated",
)


def is_xet_pointer(blob_path: Path) -> bool:
    """Check if a blob file is a Git-LFS / Xet pointer rather than real content."""
    try:
        size = blob_path.stat().st_size
    except OSError:
        return False
    if size > XET_POINTER_MAX_SIZE:
        return False
    try:
        head = blob_path.read_bytes()
    except OSError:
        return False
    return any(marker in head for marker in XET_POINTER_MARKERS)


def is_likely_gated(repo_dir: Path) -> bool:
    """Best-effort detection of gated/restricted models from local cache.

    Checks LICENSE-style files in snapshots for keywords that indicate
    the model requires agreement acceptance before use.
    """
    snapshots_dir = repo_dir / "snapshots"
    if not snapshots_dir.is_dir():
        return False

    license_names = ("LICENSE", "LICENSE.md", "LICENSE.txt", "USE_POLICY.md")
    for snap in snapshots_dir.iterdir():
        if not snap.is_dir():
            continue
        for name in license_names:
            license_file = snap / name
            if not license_file.is_file():
                continue
            try:
                content = license_file.read_text(errors="ignore").lower()
            except OSError:
                continue
            if any(kw in content for kw in GATED_LICENSE_KEYWORDS):
                return True
    return False


def get_active_refs(repo_dir: Path) -> dict[str, str]:
    """Return {ref_path: commit_hash} for a repo's refs.

    Handles nested refs like refs/pr/1.
    """
    refs: dict[str, str] = {}
    refs_dir = repo_dir / "refs"
    if not refs_dir.is_dir():
        return refs
    for ref_file in refs_dir.rglob("*"):
        if ref_file.is_file():
            rel = str(ref_file.relative_to(refs_dir))
            refs[rel] = ref_file.read_text().strip()
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
