"""Tests for local cache scanning."""

import os
from pathlib import Path

from hf_cache_sync.cache import repo_dir_to_id, scan_cache, total_cache_size


def test_repo_dir_to_id():
    assert repo_dir_to_id("models--org--name") == "org/name"
    assert repo_dir_to_id("models--user--my-model") == "user/my-model"
    assert repo_dir_to_id("datasets--squad") == "squad"


def _make_fake_cache(tmp_path: Path) -> Path:
    """Create a minimal fake HF cache structure."""
    cache = tmp_path / "hub"
    repo_dir = cache / "models--test--model"
    blobs = repo_dir / "blobs"
    snap = repo_dir / "snapshots" / "abc123"
    refs = repo_dir / "refs"

    blobs.mkdir(parents=True)
    snap.mkdir(parents=True)
    refs.mkdir(parents=True)

    # Create a blob
    blob_content = b"fake model weights"
    import hashlib
    h = hashlib.sha256(blob_content).hexdigest()
    blob_file = blobs / h
    blob_file.write_bytes(blob_content)

    # Create symlink in snapshot
    link = snap / "weights.bin"
    link.symlink_to(blob_file)

    # Create ref
    (refs / "main").write_text("abc123")

    return cache


def test_scan_cache(tmp_path):
    cache = _make_fake_cache(tmp_path)
    repos = scan_cache(cache)
    assert len(repos) == 1
    assert repos[0].repo_id == "test/model"
    assert len(repos[0].revisions) == 1
    assert len(repos[0].blobs) == 1


def test_scan_empty(tmp_path):
    repos = scan_cache(tmp_path / "nonexistent")
    assert repos == []


def test_total_cache_size(tmp_path):
    cache = _make_fake_cache(tmp_path)
    repos = scan_cache(cache)
    size = total_cache_size(repos)
    assert size == len(b"fake model weights")
