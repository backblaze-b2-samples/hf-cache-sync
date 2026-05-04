"""Tests for the doctor preflight checks."""

from __future__ import annotations

import boto3
import pytest
from moto import mock_aws

from hf_cache_sync.config import (
    CRED_SOURCE_AWS_ENV,
    CRED_SOURCE_B2_ENV,
    CRED_SOURCE_CONFIG,
    AppConfig,
    CacheConfig,
    StorageConfig,
)
from hf_cache_sync.doctor import PROBE_PREFIX, doctor, run_checks

BUCKET = "doctor-bucket"
REGION = "us-east-1"
# Informational endpoint string for tests that don't hit the network.
# Moto-based tests leave endpoint unset so boto3 uses the mocked AWS default.
ENDPOINT = "https://s3.us-east-1.example.com"


@pytest.fixture
def cache_dir(tmp_path):
    d = tmp_path / "hub"
    d.mkdir()
    return d


@pytest.fixture
def good_config(cache_dir):
    # Endpoint left unset so moto's mock_aws can intercept the default AWS host.
    return AppConfig(
        storage=StorageConfig(
            bucket=BUCKET,
            region=REGION,
            access_key="ak",
            secret_key="sk",
            credentials_source=CRED_SOURCE_CONFIG,
        ),
        cache=CacheConfig(hf_cache_dir=str(cache_dir)),
    )


def test_doctor_all_checks_pass(good_config):
    with mock_aws():
        boto3.client("s3", region_name=REGION).create_bucket(Bucket=BUCKET)
        results = run_checks(good_config)

    assert all(r.ok for r in results), [r for r in results if not r.ok]
    # endpoint, bucket, region, creds, hf_dir, bucket-reachable, read, write
    assert len(results) == 8


def test_doctor_missing_bucket_field_skips_network_checks(cache_dir):
    config = AppConfig(
        storage=StorageConfig(
            bucket="",
            region=REGION,
            access_key="ak",
            secret_key="sk",
            credentials_source=CRED_SOURCE_CONFIG,
        ),
        cache=CacheConfig(hf_cache_dir=str(cache_dir)),
    )
    results = run_checks(config)
    by_name = {r.name: r for r in results}
    assert by_name["Bucket"].ok is False
    assert by_name["Bucket reachable"].ok is False
    assert "Skipped" in by_name["Bucket reachable"].detail


def _clear_cred_env(monkeypatch):
    for var in (
        "B2_APPLICATION_KEY_ID",
        "B2_APPLICATION_KEY",
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "B2_ENDPOINT",
        "B2_BUCKET",
        "B2_REGION",
    ):
        monkeypatch.delenv(var, raising=False)


def _yaml_with(tmp_path, **storage):
    """Write a YAML config and return its path."""
    body = "storage:\n"
    for k, v in storage.items():
        body += f"  {k}: {v}\n"
    body += f"cache:\n  hf_cache_dir: {tmp_path / 'hub'}\n"
    (tmp_path / "hub").mkdir(exist_ok=True)
    p = tmp_path / ".hf-cache-sync.yaml"
    p.write_text(body)
    return p


def test_doctor_missing_credentials(tmp_path, monkeypatch):
    """No creds anywhere — must NOT crash trying to head_bucket.

    Regression guard: in CI (no ~/.aws/credentials, no IAM role), running
    head_bucket without credentials raises boto's NoCredentialsError. Doctor
    must short-circuit network probes when source=none and report a clean
    skip instead.
    """
    from hf_cache_sync.config import load_config

    _clear_cred_env(monkeypatch)
    # Also clear AWS shared-credentials discovery so this works on dev machines.
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", "/nonexistent")
    monkeypatch.setenv("AWS_CONFIG_FILE", "/nonexistent")
    p = _yaml_with(tmp_path, bucket=BUCKET, region=REGION)
    config = load_config(p)
    results = run_checks(config)
    by_name = {r.name: r for r in results}
    assert by_name["Credentials configured"].ok is False
    assert by_name["Bucket reachable"].ok is False
    assert "Skipped" in by_name["Bucket reachable"].detail
    assert "credentials" in by_name["Bucket reachable"].detail.lower()
    # Read/Write probes must not run when no creds — would crash.
    assert "Read permission" not in by_name
    assert "Write permission" not in by_name


def test_doctor_credentials_from_aws_env(tmp_path, monkeypatch):
    """source=aws_env is reported when only AWS_* vars are set."""
    from hf_cache_sync.config import load_config

    _clear_cred_env(monkeypatch)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "envak")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "envsk")
    p = _yaml_with(tmp_path, bucket=BUCKET, region=REGION)
    config = load_config(p)
    results = run_checks(config)
    creds = next(r for r in results if r.name == "Credentials configured")
    assert creds.ok is True
    assert creds.detail == f"source={CRED_SOURCE_AWS_ENV}"


def test_doctor_credentials_from_b2_env(tmp_path, monkeypatch):
    """source=b2_env is reported when B2_* vars are set."""
    from hf_cache_sync.config import load_config

    _clear_cred_env(monkeypatch)
    monkeypatch.setenv("B2_APPLICATION_KEY_ID", "b2-id")
    monkeypatch.setenv("B2_APPLICATION_KEY", "b2-secret")
    p = _yaml_with(tmp_path, bucket=BUCKET, region=REGION)
    config = load_config(p)
    results = run_checks(config)
    creds = next(r for r in results if r.name == "Credentials configured")
    assert creds.ok is True
    assert creds.detail == f"source={CRED_SOURCE_B2_ENV}"


def test_doctor_b2_env_overrides_yaml(tmp_path, monkeypatch):
    """Env credentials must win over YAML and source must reflect that."""
    from hf_cache_sync.config import load_config

    _clear_cred_env(monkeypatch)
    monkeypatch.setenv("B2_APPLICATION_KEY_ID", "b2-id")
    monkeypatch.setenv("B2_APPLICATION_KEY", "b2-secret")
    p = _yaml_with(
        tmp_path,
        bucket=BUCKET,
        region=REGION,
        access_key="yaml-id",
        secret_key="yaml-secret",
    )
    config = load_config(p)
    assert config.storage.access_key == "b2-id"
    assert config.storage.credentials_source == CRED_SOURCE_B2_ENV
    creds = next(r for r in run_checks(config) if r.name == "Credentials configured")
    assert creds.detail == f"source={CRED_SOURCE_B2_ENV}"


def test_doctor_storage_env_overrides_yaml(tmp_path, monkeypatch):
    """B2_ENDPOINT/B2_BUCKET/B2_REGION override their YAML counterparts.

    Pure config-resolution test — no network calls, so a custom endpoint
    that moto can't intercept is fine here.
    """
    from hf_cache_sync.config import load_config

    _clear_cred_env(monkeypatch)
    monkeypatch.setenv("B2_ENDPOINT", "https://env-endpoint.example.com")
    monkeypatch.setenv("B2_BUCKET", "env-bucket")
    monkeypatch.setenv("B2_REGION", "env-region")
    p = _yaml_with(
        tmp_path,
        endpoint="https://yaml-endpoint.example.com",
        bucket="yaml-bucket",
        region="yaml-region",
        access_key="ak",
        secret_key="sk",
    )
    config = load_config(p)
    assert config.storage.endpoint == "https://env-endpoint.example.com"
    assert config.storage.bucket == "env-bucket"
    assert config.storage.region == "env-region"

    # Don't run all checks — bucket-reachable would hit the fake endpoint.
    from hf_cache_sync.doctor import _check_bucket, _check_endpoint, _check_region

    assert _check_endpoint(config).detail == "https://env-endpoint.example.com"
    assert _check_bucket(config).detail == "env-bucket"
    assert _check_region(config).detail == "env-region"


def test_doctor_endpoint_informational_when_unset(cache_dir):
    """Endpoint row shows the AWS-default note when no endpoint is set."""
    config = AppConfig(
        storage=StorageConfig(
            bucket=BUCKET,
            region=REGION,
            access_key="ak",
            secret_key="sk",
            credentials_source=CRED_SOURCE_CONFIG,
        ),
        cache=CacheConfig(hf_cache_dir=str(cache_dir)),
    )
    with mock_aws():
        boto3.client("s3", region_name=REGION).create_bucket(Bucket=BUCKET)
        results = run_checks(config)
    endpoint = next(r for r in results if r.name == "Endpoint")
    assert endpoint.ok is True
    assert "default" in endpoint.detail.lower()


def test_doctor_missing_hf_cache_dir(tmp_path):
    config = AppConfig(
        storage=StorageConfig(
            bucket=BUCKET,
            region=REGION,
            access_key="ak",
            secret_key="sk",
            credentials_source=CRED_SOURCE_CONFIG,
        ),
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

    _clear_cred_env(monkeypatch)

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
    # No endpoint — moto's mock_aws intercepts the AWS default host.
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
