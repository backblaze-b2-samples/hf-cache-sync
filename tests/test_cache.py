"""Tests for local cache scanning."""

import hashlib
from pathlib import Path

from hf_cache_sync.cache import (
    get_active_refs,
    parse_repo_dirname,
    repo_id_to_dirname,
    scan_cache,
    total_cache_size,
)


def test_parse_repo_dirname():
    assert parse_repo_dirname("models--org--name") == ("model", "org/name")
    assert parse_repo_dirname("models--user--my-model") == ("model", "user/my-model")
    assert parse_repo_dirname("datasets--squad") == ("dataset", "squad")
    assert parse_repo_dirname("spaces--org--app") == ("space", "org/app")


def test_repo_id_to_dirname():
    assert repo_id_to_dirname("org/name", "model") == "models--org--name"
    assert repo_id_to_dirname("squad", "dataset") == "datasets--squad"
    assert repo_id_to_dirname("org/app", "space") == "spaces--org--app"


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
    assert repos[0].repo_type == "model"
    assert len(repos[0].revisions) == 1
    assert len(repos[0].blobs) == 1


def test_scan_cache_dataset(tmp_path):
    cache = tmp_path / "hub"
    repo_dir = cache / "datasets--myorg--mydata"
    blobs = repo_dir / "blobs"
    snap = repo_dir / "snapshots" / "rev1"
    blobs.mkdir(parents=True)
    snap.mkdir(parents=True)

    content = b"data"
    h = hashlib.sha256(content).hexdigest()
    (blobs / h).write_bytes(content)
    (snap / "train.parquet").symlink_to(blobs / h)

    repos = scan_cache(cache)
    assert len(repos) == 1
    assert repos[0].repo_type == "dataset"
    assert repos[0].repo_id == "myorg/mydata"


def test_scan_empty(tmp_path):
    repos = scan_cache(tmp_path / "nonexistent")
    assert repos == []


def test_total_cache_size(tmp_path):
    cache = _make_fake_cache(tmp_path)
    repos = scan_cache(cache)
    size = total_cache_size(repos)
    assert size == len(b"fake model weights")


def test_get_active_refs_nested(tmp_path):
    """Nested refs like refs/pr/1 should be discovered."""
    repo_dir = tmp_path / "models--org--model"
    refs_dir = repo_dir / "refs"
    pr_dir = refs_dir / "pr"
    pr_dir.mkdir(parents=True)

    (refs_dir / "main").write_text("abc123")
    (pr_dir / "1").write_text("def456")

    refs = get_active_refs(repo_dir)
    assert refs["main"] == "abc123"
    assert refs["pr/1"] == "def456"
