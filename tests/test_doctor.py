"""Tests for the doctor preflight checks."""

from __future__ import annotations

import boto3
import pytest
from moto import mock_aws

from hf_cache_sync.config import AppConfig, CacheConfig, StorageConfig
from hf_cache_sync.doctor import PROBE_PREFIX, doctor, run_checks

BUCKET = "doctor-bucket"
REGION = "us-east-1"


@pytest.fixture
def cache_dir(tmp_path):
    d = tmp_path / "hub"
    d.mkdir()
    return d


@pytest.fixture
def good_config(cache_dir):
    return AppConfig(
        storage=StorageConfig(
            bucket=BUCKET,
            region=REGION,
            access_key="ak",
            secret_key="sk",
        ),
        cache=CacheConfig(hf_cache_dir=str(cache_dir)),
    )


def test_doctor_all_checks_pass(good_config):
    with mock_aws():
        boto3.client("s3", region_name=REGION).create_bucket(Bucket=BUCKET)
        results = run_checks(good_config)

    assert all(r.ok for r in results), [r for r in results if not r.ok]
    # Must have run all 6 checks (fields, creds, hf_dir, bucket, read, write).
    assert len(results) == 6


def test_doctor_missing_bucket_field_skips_network_checks(cache_dir):
    config = AppConfig(
        storage=StorageConfig(bucket="", region=REGION, access_key="ak", secret_key="sk"),
        cache=CacheConfig(hf_cache_dir=str(cache_dir)),
    )
    results = run_checks(config)
    # Required fields fails; bucket reachable is reported as skipped (ok=False).
    by_name = {r.name: r for r in results}
    assert by_name["Required config fields"].ok is False
    assert by_name["Bucket reachable"].ok is False
    assert "Skipped" in by_name["Bucket reachable"].detail


def test_doctor_missing_credentials(cache_dir, monkeypatch):
    monkeypatch.delenv("AWS_ACCESS_KEY_ID", raising=False)
    monkeypatch.delenv("AWS_SECRET_ACCESS_KEY", raising=False)
    config = AppConfig(
        storage=StorageConfig(bucket=BUCKET, region=REGION),
        cache=CacheConfig(hf_cache_dir=str(cache_dir)),
    )
    results = run_checks(config)
    by_name = {r.name: r for r in results}
    assert by_name["Credentials configured"].ok is False


def test_doctor_credentials_from_env(cache_dir, monkeypatch):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "envak")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "envsk")
    config = AppConfig(
        storage=StorageConfig(bucket=BUCKET, region=REGION),
        cache=CacheConfig(hf_cache_dir=str(cache_dir)),
    )
    results = run_checks(config)
    creds = next(r for r in results if r.name == "Credentials configured")
    assert creds.ok is True
    assert "env" in creds.detail


def test_doctor_missing_hf_cache_dir(tmp_path):
    config = AppConfig(
        storage=StorageConfig(bucket=BUCKET, region=REGION, access_key="ak", secret_key="sk"),
        cache=CacheConfig(hf_cache_dir=str(tmp_path / "does-not-exist")),
    )
    with mock_aws():
        boto3.client("s3", region_name=REGION).create_bucket(Bucket=BUCKET)
        results = run_checks(config)
    hf_dir = next(r for r in results if r.name == "HF cache dir")
    assert hf_dir.ok is False


def test_doctor_bucket_unreachable(good_config):
    """Bucket that doesn't exist must fail the bucket check, not crash."""
    with mock_aws():
        # No bucket created.
        results = run_checks(good_config)

    by_name = {r.name: r for r in results}
    assert by_name["Bucket reachable"].ok is False
    # Read/write checks should be skipped when bucket is unreachable.
    assert "Read permission" not in by_name
    assert "Write permission" not in by_name


def test_doctor_write_check_cleans_up_sentinel(good_config):
    """The probe object must NOT be left behind in the bucket."""
    with mock_aws():
        s3 = boto3.client("s3", region_name=REGION)
        s3.create_bucket(Bucket=BUCKET)

        results = run_checks(good_config)
        assert all(r.ok for r in results)

        # No probe-* keys should remain.
        resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=PROBE_PREFIX)
        assert resp.get("KeyCount", 0) == 0


def test_doctor_probe_does_not_pollute_blobs_namespace(good_config):
    """Critical safety: the sentinel must not appear under the blobs/ prefix
    or it would leak into push.py's existing-blobs prefetch set."""
    with mock_aws():
        s3 = boto3.client("s3", region_name=REGION)
        s3.create_bucket(Bucket=BUCKET)
        run_checks(good_config)
        resp = s3.list_objects_v2(Bucket=BUCKET, Prefix="blobs/")
        assert resp.get("KeyCount", 0) == 0


def test_doctor_returns_true_when_all_pass(good_config):
    with mock_aws():
        boto3.client("s3", region_name=REGION).create_bucket(Bucket=BUCKET)
        assert doctor(good_config) is True


def test_doctor_returns_false_when_any_fail(cache_dir):
    config = AppConfig(
        storage=StorageConfig(bucket="", region=REGION),
        cache=CacheConfig(hf_cache_dir=str(cache_dir)),
    )
    assert doctor(config) is False


def test_doctor_cli_exits_nonzero_on_failure(tmp_path, monkeypatch):
    """Smoke test the CLI wiring."""
    from click.testing import CliRunner

    from hf_cache_sync.cli import cli

    monkeypatch.delenv("AWS_ACCESS_KEY_ID", raising=False)
    monkeypatch.delenv("AWS_SECRET_ACCESS_KEY", raising=False)

    cfg_path = tmp_path / ".hf-cache-sync.yaml"
    cfg_path.write_text(f"""\
storage:
  bucket: ""
  region: ""
cache:
  hf_cache_dir: {tmp_path / "hub"}
""")
    (tmp_path / "hub").mkdir()
    result = CliRunner().invoke(cli, ["--config", str(cfg_path), "doctor"])
    assert result.exit_code == 1


def test_doctor_cli_exits_zero_on_success(tmp_path, monkeypatch):
    from click.testing import CliRunner

    from hf_cache_sync.cli import cli

    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "ak")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "sk")

    cfg_path = tmp_path / ".hf-cache-sync.yaml"
    (tmp_path / "hub").mkdir()
    cfg_path.write_text(f"""\
storage:
  bucket: {BUCKET}
  region: {REGION}
cache:
  hf_cache_dir: {tmp_path / "hub"}
""")

    with mock_aws():
        boto3.client("s3", region_name=REGION).create_bucket(Bucket=BUCKET)
        result = CliRunner().invoke(cli, ["--config", str(cfg_path), "doctor"])

    assert result.exit_code == 0, result.output
