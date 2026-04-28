"""Integration tests for push/pull/prune using moto S3 mock."""

import hashlib
import json
import os
from pathlib import Path

import boto3
import pytest
from moto import mock_aws

from hf_cache_sync.cache import repo_id_to_dirname
from hf_cache_sync.config import AppConfig, CacheConfig, StorageConfig, TeamConfig
from hf_cache_sync.prune import prune
from hf_cache_sync.pull import pull, pull_all
from hf_cache_sync.push import push
from hf_cache_sync.storage import StorageBackend

BUCKET = "test-hf-cache"
REGION = "us-east-1"


@pytest.fixture
def s3(tmp_path):
    """Yield a moto-mocked S3 environment with bucket created."""
    with mock_aws():
        client = boto3.client("s3", region_name=REGION)
        client.create_bucket(Bucket=BUCKET)
        yield client


@pytest.fixture
def config(tmp_path):
    """Return an AppConfig pointing at tmp_path cache and the moto bucket."""
    cache_dir = tmp_path / "hub"
    cache_dir.mkdir()
    return AppConfig(
        storage=StorageConfig(bucket=BUCKET, region=REGION),
        cache=CacheConfig(max_local_gb=50, hf_cache_dir=str(cache_dir)),
        team=TeamConfig(),
    )


def _build_repo(
    cache_dir: Path,
    repo_id: str,
    revision: str,
    files: dict[str, bytes],
    repo_type: str = "model",
):
    """Build a fake HF cache repo with blobs and symlinked snapshots."""
    dir_name = repo_id_to_dirname(repo_id, repo_type)
    repo_dir = cache_dir / dir_name
    blobs_dir = repo_dir / "blobs"
    snap_dir = repo_dir / "snapshots" / revision
    refs_dir = repo_dir / "refs"

    blobs_dir.mkdir(parents=True)
    snap_dir.mkdir(parents=True)
    refs_dir.mkdir(parents=True)

    for filename, content in files.items():
        blob_hash = hashlib.sha256(content).hexdigest()
        blob_path = blobs_dir / blob_hash
        blob_path.write_bytes(content)
        link_path = snap_dir / filename
        link_path.symlink_to(blob_path)

    (refs_dir / "main").write_text(revision)
    return repo_dir


# ── Push Tests ──────────────────────────────────────────────────────


class TestPush:
    def test_push_uploads_blobs_and_manifest(self, s3, config, tmp_path):
        _build_repo(
            config.hf_cache_dir,
            "org/model",
            "abc123",
            {"weights.bin": b"model data here", "config.json": b'{"key": "val"}'},
        )

        push(config)

        backend = StorageBackend(config)

        # Blobs uploaded
        blob_hash = hashlib.sha256(b"model data here").hexdigest()
        assert backend.exists(f"blobs/{blob_hash}")

        config_hash = hashlib.sha256(b'{"key": "val"}').hexdigest()
        assert backend.exists(f"blobs/{config_hash}")

        # Manifest uploaded with repo_type
        assert backend.exists("manifests/org__model@abc123.json")
        data = json.loads(backend.download_bytes("manifests/org__model@abc123.json"))
        assert data["repo"] == "org/model"
        assert data["repo_type"] == "model"
        assert len(data["files"]) == 2

        # Ref uploaded
        ref = backend.download_bytes("refs/org__model/main").decode()
        assert ref == "abc123"

    def test_push_dataset_repo(self, s3, config):
        _build_repo(
            config.hf_cache_dir,
            "org/data",
            "rev1",
            {"train.parquet": b"data"},
            repo_type="dataset",
        )

        push(config)

        backend = StorageBackend(config)
        data = json.loads(backend.download_bytes("manifests/org__data@rev1.json"))
        assert data["repo_type"] == "dataset"

    def test_push_skips_existing_blobs(self, s3, config, tmp_path):
        content = b"already uploaded"
        blob_hash = hashlib.sha256(content).hexdigest()

        backend = StorageBackend(config)
        backend.upload_bytes(content, f"blobs/{blob_hash}")

        _build_repo(config.hf_cache_dir, "org/model", "rev1", {"file.bin": content})

        push(config)

        keys = backend.list_keys("blobs/")
        assert len(keys) == 1

    def test_push_skips_gated_repo(self, s3, config):
        repo_dir = _build_repo(
            config.hf_cache_dir,
            "meta/llama",
            "rev1",
            {"model.bin": b"weights"},
        )
        snap = repo_dir / "snapshots" / "rev1"
        (snap / "LICENSE.txt").write_text(
            "META LLAMA 2 COMMUNITY LICENSE AGREEMENT\nYou must accept."
        )

        push(config)

        backend = StorageBackend(config)
        assert backend.list_keys("blobs/") == []

    def test_push_includes_gated_when_allowed(self, s3, config):
        config.team.allow_gated = True
        repo_dir = _build_repo(
            config.hf_cache_dir,
            "meta/llama",
            "rev1",
            {"model.bin": b"weights"},
        )
        snap = repo_dir / "snapshots" / "rev1"
        (snap / "LICENSE.txt").write_text(
            "META LLAMA 2 COMMUNITY LICENSE AGREEMENT\nYou must accept."
        )

        push(config)

        backend = StorageBackend(config)
        assert len(backend.list_keys("blobs/")) > 0

    def test_push_skips_xet_pointers(self, s3, config):
        real_content = b"x" * 2048
        xet_content = b"version https://git-lfs.github.com/spec/v1\noid sha256:abc\nsize 100\n"

        real_hash = hashlib.sha256(real_content).hexdigest()
        xet_hash = hashlib.sha256(xet_content).hexdigest()

        dir_name = repo_id_to_dirname("org/mixed", "model")
        repo_dir = config.hf_cache_dir / dir_name
        blobs_dir = repo_dir / "blobs"
        snap_dir = repo_dir / "snapshots" / "rev1"
        refs_dir = repo_dir / "refs"
        for d in (blobs_dir, snap_dir, refs_dir):
            d.mkdir(parents=True)

        (blobs_dir / real_hash).write_bytes(real_content)
        (blobs_dir / xet_hash).write_bytes(xet_content)
        (snap_dir / "real.bin").symlink_to(blobs_dir / real_hash)
        (snap_dir / "pointer.bin").symlink_to(blobs_dir / xet_hash)
        (refs_dir / "main").write_text("rev1")

        push(config)

        backend = StorageBackend(config)
        keys = backend.list_keys("blobs/")
        assert f"blobs/{real_hash}" in keys
        assert f"blobs/{xet_hash}" not in keys

        # Empty manifest should NOT be uploaded if all files are xet
        # (In this case, one real file exists so manifest should have 1 file)
        data = json.loads(backend.download_bytes("manifests/org__mixed@rev1.json"))
        assert len(data["files"]) == 1

    def test_push_includes_xet_when_enabled(self, s3, config):
        config.cache.sync_xet = True
        xet_content = b"version https://git-lfs.github.com/spec/v1\noid sha256:abc\nsize 100\n"
        _build_repo(config.hf_cache_dir, "org/model", "rev1", {"pointer.bin": xet_content})

        push(config)

        backend = StorageBackend(config)
        xet_hash = hashlib.sha256(xet_content).hexdigest()
        assert backend.exists(f"blobs/{xet_hash}")

    def test_push_dry_run_uploads_nothing(self, s3, config):
        _build_repo(config.hf_cache_dir, "org/model", "rev1", {"f.bin": b"data"})

        push(config, dry_run=True)

        backend = StorageBackend(config)
        assert backend.list_keys("blobs/") == []
        assert backend.list_keys("manifests/") == []
        assert backend.list_keys("refs/") == []

    def test_push_skips_empty_manifest(self, s3, config):
        """When all files in a revision are xet pointers, no manifest should be uploaded."""
        xet_content = b"version https://git-lfs.github.com/spec/v1\noid sha256:abc\nsize 100\n"
        _build_repo(config.hf_cache_dir, "org/allxet", "rev1", {"pointer.bin": xet_content})

        push(config)

        backend = StorageBackend(config)
        manifests = backend.list_keys("manifests/")
        assert len(manifests) == 0


# ── Pull Tests ──────────────────────────────────────────────────────


class TestPull:
    def _push_from(self, config, tmp_path, repo_id, revision, files, repo_type="model"):
        src_cache = tmp_path / "src_hub"
        src_cache.mkdir(exist_ok=True)
        src_config = AppConfig(
            storage=config.storage,
            cache=CacheConfig(hf_cache_dir=str(src_cache)),
            team=TeamConfig(),
        )
        _build_repo(src_cache, repo_id, revision, files, repo_type)
        push(src_config)

    def test_pull_hydrates_cache(self, s3, config, tmp_path):
        self._push_from(config, tmp_path, "org/model", "abc123", {"weights.bin": b"model weights"})

        pull(config, "org/model", "abc123")

        dir_name = repo_id_to_dirname("org/model", "model")
        snap = config.hf_cache_dir / dir_name / "snapshots" / "abc123"
        assert (snap / "weights.bin").exists()

        blob_path = (snap / "weights.bin").resolve()
        assert blob_path.read_bytes() == b"model weights"

    def test_pull_dataset_uses_correct_dir(self, s3, config, tmp_path):
        """Pull should use datasets-- prefix for dataset repos."""
        self._push_from(config, tmp_path, "org/data", "rev1", {"train.parquet": b"data"}, "dataset")

        pull(config, "org/data", "rev1", repo_type="dataset")

        dir_name = repo_id_to_dirname("org/data", "dataset")
        snap = config.hf_cache_dir / dir_name / "snapshots" / "rev1"
        assert (snap / "train.parquet").exists()
        # Should NOT create models-- dir
        assert not (config.hf_cache_dir / "models--org--data").exists()

    def test_pull_verifies_hash(self, s3, config):
        from hf_cache_sync.pull import PullError

        backend = StorageBackend(config)
        # 64-char hash (sha256-length) triggers verification.
        bad_hash = "deadbeef" * 8
        manifest = {
            "repo": "org/bad",
            "revision": "rev1",
            "repo_type": "model",
            "files": [{"path": "f.bin", "blob": bad_hash, "size": 5}],
        }
        backend.upload_bytes(json.dumps(manifest).encode(), "manifests/org__bad@rev1.json")
        backend.upload_bytes(b"hello", f"blobs/{bad_hash}")

        with pytest.raises(PullError, match="Hash mismatch"):
            pull(config, "org/bad", "rev1")

        blob_path = config.hf_cache_dir / "models--org--bad" / "blobs" / bad_hash
        # Atomic download: corrupt blob never lands at its final path.
        assert not blob_path.exists()
        # The .tmp staging file must also be cleaned up.
        assert not blob_path.with_suffix(".tmp").exists()

    def test_pull_resolves_ref(self, s3, config, tmp_path):
        self._push_from(config, tmp_path, "org/model", "rev999", {"data.bin": b"data"})

        pull(config, "org/model")

        dir_name = repo_id_to_dirname("org/model", "model")
        snap = config.hf_cache_dir / dir_name / "snapshots" / "rev999"
        assert (snap / "data.bin").exists()
        # Should write refs/main since it was resolved from ref
        ref = (config.hf_cache_dir / dir_name / "refs" / "main").read_text()
        assert ref == "rev999"

    def test_pull_explicit_revision_does_not_overwrite_ref(self, s3, config, tmp_path):
        """Explicit --revision should NOT overwrite refs/main."""
        self._push_from(config, tmp_path, "org/model", "rev1", {"a.bin": b"aaa"})

        # Pre-create refs/main pointing at something else
        dir_name = repo_id_to_dirname("org/model", "model")
        refs_dir = config.hf_cache_dir / dir_name / "refs"
        refs_dir.mkdir(parents=True)
        (refs_dir / "main").write_text("original_main")

        pull(config, "org/model", "rev1")

        ref = (refs_dir / "main").read_text()
        assert ref == "original_main"

    def test_pull_creates_relative_symlinks(self, s3, config, tmp_path):
        self._push_from(config, tmp_path, "org/model", "rev1", {"f.bin": b"content"})
        pull(config, "org/model", "rev1")

        dir_name = repo_id_to_dirname("org/model", "model")
        link = config.hf_cache_dir / dir_name / "snapshots" / "rev1" / "f.bin"
        assert link.is_symlink()
        target = os.readlink(link)
        # Should be a relative path, not absolute
        assert not os.path.isabs(target)

    def test_pull_all_hydrates_everything(self, s3, config, tmp_path):
        self._push_from(config, tmp_path, "org/a", "rev1", {"a.bin": b"aaa"})
        self._push_from(config, tmp_path, "org/b", "rev2", {"b.bin": b"bbb"})

        pull_all(config)

        assert (config.hf_cache_dir / "models--org--a" / "snapshots" / "rev1" / "a.bin").exists()
        assert (config.hf_cache_dir / "models--org--b" / "snapshots" / "rev2" / "b.bin").exists()

    def test_pull_all_writes_refs_main(self, s3, config, tmp_path):
        """pull_all should write a local refs/main so huggingface_hub can resolve."""
        self._push_from(config, tmp_path, "org/model", "rev1", {"f.bin": b"x"})

        pull_all(config)

        ref = (config.hf_cache_dir / "models--org--model" / "refs" / "main").read_text()
        assert ref == "rev1"

    def test_pull_all_filters(self, s3, config, tmp_path):
        self._push_from(config, tmp_path, "org/keep", "rev1", {"f.bin": b"k"})
        self._push_from(config, tmp_path, "org/skip", "rev1", {"f.bin": b"s"})

        pull_all(config, include=["org/keep"])

        assert (config.hf_cache_dir / "models--org--keep" / "snapshots" / "rev1" / "f.bin").exists()
        assert not (config.hf_cache_dir / "models--org--skip").exists()

    def test_pull_all_exclude(self, s3, config, tmp_path):
        self._push_from(config, tmp_path, "org/keep", "rev1", {"f.bin": b"k"})
        self._push_from(config, tmp_path, "org/skip", "rev1", {"f.bin": b"s"})

        pull_all(config, exclude=["org/skip"])

        assert (config.hf_cache_dir / "models--org--keep").exists()
        assert not (config.hf_cache_dir / "models--org--skip").exists()

    def test_pull_all_limit(self, s3, config, tmp_path):
        for n in range(3):
            self._push_from(config, tmp_path, f"org/m{n}", "rev1", {"f.bin": f"x{n}".encode()})

        pull_all(config, limit=2)

        existing = sorted(p.name for p in config.hf_cache_dir.iterdir() if p.is_dir())
        # 2 of 3 repos should be hydrated.
        assert len(existing) == 2

    def test_pull_dry_run(self, s3, config, tmp_path):
        self._push_from(config, tmp_path, "org/model", "rev1", {"f.bin": b"data"})
        # Wipe local cache the source push touched.
        for child in list(config.hf_cache_dir.iterdir()):
            import shutil as _shutil

            _shutil.rmtree(child) if child.is_dir() else child.unlink()

        pull(config, "org/model", "rev1", dry_run=True)

        assert not (config.hf_cache_dir / "models--org--model").exists()


# ── Prune Tests ─────────────────────────────────────────────────────


class TestPrune:
    def test_prune_removes_detached_revisions(self, config):
        content = b"x" * 1024

        dir_name = repo_id_to_dirname("org/model", "model")
        repo_dir = config.hf_cache_dir / dir_name
        blobs_dir = repo_dir / "blobs"
        refs_dir = repo_dir / "refs"

        blobs_dir.mkdir(parents=True)
        refs_dir.mkdir(parents=True)

        for rev_name in ("active_rev", "old_rev"):
            snap = repo_dir / "snapshots" / rev_name
            snap.mkdir(parents=True)
            blob_content = content + rev_name.encode()
            actual_hash = hashlib.sha256(blob_content).hexdigest()
            (blobs_dir / actual_hash).write_bytes(blob_content)
            (snap / "weights.bin").symlink_to(blobs_dir / actual_hash)

        (refs_dir / "main").write_text("active_rev")

        # Touch old_rev snapshot files to make them oldest
        old_snap = repo_dir / "snapshots" / "old_rev" / "weights.bin"
        os.utime(old_snap, (0, 0))

        prune(config, max_gb=0)

        assert not (repo_dir / "snapshots" / "old_rev").exists()
        assert (repo_dir / "snapshots" / "active_rev").exists()

    def test_prune_noop_under_budget(self, config):
        _build_repo(config.hf_cache_dir, "org/small", "rev1", {"tiny.bin": b"small"})

        prune(config, max_gb=100)

        dir_name = repo_id_to_dirname("org/small", "model")
        snap = config.hf_cache_dir / dir_name / "snapshots" / "rev1"
        assert snap.exists()

    def test_prune_dry_run_does_not_delete(self, config):
        dir_name = repo_id_to_dirname("org/model", "model")
        repo_dir = config.hf_cache_dir / dir_name
        blobs_dir = repo_dir / "blobs"
        snap_dir = repo_dir / "snapshots" / "rev1"
        refs_dir = repo_dir / "refs"
        for d in (blobs_dir, snap_dir, refs_dir):
            d.mkdir(parents=True)

        # An orphan blob and a referenced one — prune normally cleans the orphan.
        (blobs_dir / "orphan_hash").write_bytes(b"orphan")
        content = b"used"
        used_hash = hashlib.sha256(content).hexdigest()
        (blobs_dir / used_hash).write_bytes(content)
        (snap_dir / "used.bin").symlink_to(blobs_dir / used_hash)
        (refs_dir / "main").write_text("rev1")

        prune(config, max_gb=0, dry_run=True)

        # Nothing should be deleted in dry-run, even the orphan.
        assert (blobs_dir / "orphan_hash").exists()
        assert snap_dir.exists()

    def test_prune_cleans_orphaned_blobs(self, config):
        dir_name = repo_id_to_dirname("org/model", "model")
        repo_dir = config.hf_cache_dir / dir_name
        blobs_dir = repo_dir / "blobs"
        snap_dir = repo_dir / "snapshots" / "rev1"
        refs_dir = repo_dir / "refs"

        for d in (blobs_dir, snap_dir, refs_dir):
            d.mkdir(parents=True)

        (blobs_dir / "orphan_hash").write_bytes(b"orphan")

        content = b"used"
        used_hash = hashlib.sha256(content).hexdigest()
        (blobs_dir / used_hash).write_bytes(content)
        (snap_dir / "used.bin").symlink_to(blobs_dir / used_hash)
        (refs_dir / "main").write_text("rev1")

        prune(config, max_gb=100)

        assert not (blobs_dir / "orphan_hash").exists()
        assert (blobs_dir / used_hash).exists()


# ── Storage Error Wrapping ──────────────────────────────────────────


class TestStorageErrors:
    def test_pull_surfaces_storage_error_on_missing_bucket(self, config):
        """Bucket-level errors must come through humanized, not as raw boto."""
        from hf_cache_sync.pull import pull
        from hf_cache_sync.storage import StorageError

        config.storage.bucket = "definitely-not-this-bucket"
        with mock_aws():
            with pytest.raises(StorageError) as excinfo:
                pull(config, "org/whatever", "rev1")
            msg = str(excinfo.value).lower()
            assert "bucket" in msg or "not found" in msg


# ── Team Prefix Tests ───────────────────────────────────────────────


class TestTeamPrefix:
    def test_push_pull_with_prefix(self, s3, config, tmp_path):
        config.team.prefix = "myteam/"
        _build_repo(config.hf_cache_dir, "org/model", "rev1", {"f.bin": b"data"})

        push(config)

        resp = s3.list_objects_v2(Bucket=BUCKET, Prefix="myteam/")
        actual_keys = [obj["Key"] for obj in resp.get("Contents", [])]
        assert any(k.startswith("myteam/blobs/") for k in actual_keys)
        assert any(k.startswith("myteam/manifests/") for k in actual_keys)

        dest_cache = tmp_path / "dest_hub"
        dest_cache.mkdir()
        dest_config = AppConfig(
            storage=config.storage,
            cache=CacheConfig(hf_cache_dir=str(dest_cache)),
            team=TeamConfig(prefix="myteam/"),
        )
        pull(dest_config, "org/model", "rev1")

        dir_name = repo_id_to_dirname("org/model", "model")
        snap = dest_cache / dir_name / "snapshots" / "rev1"
        assert (snap / "f.bin").exists()
