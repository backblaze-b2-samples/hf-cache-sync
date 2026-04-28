"""Tests for diff and list --remote."""

from __future__ import annotations

import boto3
import pytest
from moto import mock_aws

from hf_cache_sync.config import AppConfig, CacheConfig, StorageConfig, TeamConfig
from hf_cache_sync.diff import collect_local, collect_remote, diff_status
from hf_cache_sync.manifest import parse_manifest_key
from hf_cache_sync.push import push
from hf_cache_sync.storage import StorageBackend

BUCKET = "diff-bucket"
REGION = "us-east-1"


@pytest.fixture
def s3():
    with mock_aws():
        client = boto3.client("s3", region_name=REGION)
        client.create_bucket(Bucket=BUCKET)
        yield client


@pytest.fixture
def config(tmp_path):
    cache = tmp_path / "hub"
    cache.mkdir()
    return AppConfig(
        storage=StorageConfig(bucket=BUCKET, region=REGION),
        cache=CacheConfig(hf_cache_dir=str(cache)),
        team=TeamConfig(),
    )


def _build_repo(cache_dir, repo_id, revision, files, repo_type="model"):
    import hashlib

    from hf_cache_sync.cache import repo_id_to_dirname

    dir_name = repo_id_to_dirname(repo_id, repo_type)
    repo_dir = cache_dir / dir_name
    blobs_dir = repo_dir / "blobs"
    snap_dir = repo_dir / "snapshots" / revision
    refs_dir = repo_dir / "refs"
    for d in (blobs_dir, snap_dir, refs_dir):
        d.mkdir(parents=True)
    for filename, content in files.items():
        h = hashlib.sha256(content).hexdigest()
        (blobs_dir / h).write_bytes(content)
        (snap_dir / filename).symlink_to(blobs_dir / h)
    (refs_dir / "main").write_text(revision)
    return repo_dir


def test_parse_manifest_key_round_trip():
    from hf_cache_sync.manifest import manifest_key

    repo, rev = "org/name", "abc123"
    parsed = parse_manifest_key(manifest_key(repo, rev))
    assert parsed == (repo, rev)


def test_parse_manifest_key_rejects_garbage():
    assert parse_manifest_key("blobs/whatever") is None
    assert parse_manifest_key("manifests/no-at-sign.json") is None
    assert parse_manifest_key("manifests/repo@rev.txt") is None


def test_collect_local_empty(tmp_path):
    assert collect_local(tmp_path / "empty") == {}


def test_collect_local_returns_revisions(config):
    _build_repo(config.hf_cache_dir, "org/a", "rev1", {"f.bin": b"x"})
    _build_repo(config.hf_cache_dir, "org/b", "rev2", {"g.bin": b"y"})
    local = collect_local(config.hf_cache_dir)
    assert local == {"org/a": {"rev1"}, "org/b": {"rev2"}}


def test_collect_remote_returns_revisions(s3, config, tmp_path):
    src = tmp_path / "src"
    src.mkdir()
    src_config = AppConfig(
        storage=config.storage,
        cache=CacheConfig(hf_cache_dir=str(src)),
        team=TeamConfig(),
    )
    _build_repo(src, "org/a", "rev1", {"f.bin": b"x"})
    _build_repo(src, "org/b", "rev2", {"g.bin": b"y"})
    push(src_config)

    backend = StorageBackend(config)
    remote = collect_remote(backend)
    assert remote == {"org/a": {"rev1"}, "org/b": {"rev2"}}


def test_diff_status_classifies_each_bucket():
    local = {"org/a": {"rev1", "rev2"}, "org/local-only": {"x"}}
    remote = {"org/a": {"rev2", "rev3"}, "org/remote-only": {"y"}}
    rows = diff_status(local, remote)

    by_status: dict[str, set[tuple[str, str]]] = {
        "in-sync": set(),
        "local-only": set(),
        "remote-only": set(),
    }
    for repo, rev, status in rows:
        by_status[status].add((repo, rev))

    assert by_status["in-sync"] == {("org/a", "rev2")}
    assert by_status["local-only"] == {("org/a", "rev1"), ("org/local-only", "x")}
    assert by_status["remote-only"] == {("org/a", "rev3"), ("org/remote-only", "y")}


def test_diff_status_stable_ordering():
    local = {"z/last": {"r"}, "a/first": {"r"}}
    remote = {"a/first": {"r"}}
    rows = diff_status(local, remote)
    # Sorted by repo first, so a/first comes before z/last regardless of dict order.
    assert rows[0][0] == "a/first"
    assert rows[-1][0] == "z/last"


def test_list_remote_cli_empty(tmp_path):
    """--remote against an empty bucket prints a friendly message, exits 0."""
    from click.testing import CliRunner

    from hf_cache_sync.cli import cli

    cfg = tmp_path / ".hf-cache-sync.yaml"
    (tmp_path / "hub").mkdir()
    cfg.write_text(f"""\
storage:
  bucket: {BUCKET}
  region: {REGION}
cache:
  hf_cache_dir: {tmp_path / "hub"}
""")

    with mock_aws():
        boto3.client("s3", region_name=REGION).create_bucket(Bucket=BUCKET)
        result = CliRunner().invoke(cli, ["--config", str(cfg), "list", "--remote"])

    assert result.exit_code == 0
    assert "No manifests" in result.output


def test_diff_cli_smoke(s3, config, tmp_path):
    """Diff command runs end-to-end."""
    from click.testing import CliRunner

    from hf_cache_sync.cli import cli

    _build_repo(config.hf_cache_dir, "org/local", "rev1", {"f.bin": b"x"})
    src = tmp_path / "src"
    src.mkdir()
    src_config = AppConfig(
        storage=config.storage,
        cache=CacheConfig(hf_cache_dir=str(src)),
        team=TeamConfig(),
    )
    _build_repo(src, "org/remote", "rev2", {"g.bin": b"y"})
    push(src_config)

    cfg = tmp_path / ".hf-cache-sync.yaml"
    cfg.write_text(f"""\
storage:
  bucket: {BUCKET}
  region: {REGION}
cache:
  hf_cache_dir: {config.hf_cache_dir}
""")
    result = CliRunner().invoke(cli, ["--config", str(cfg), "diff"])
    assert result.exit_code == 0
    assert "local-only" in result.output
    assert "remote-only" in result.output
