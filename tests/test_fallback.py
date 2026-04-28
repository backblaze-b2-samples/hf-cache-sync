"""Tests for the --fallback hf-hub path.

We mock huggingface_hub.snapshot_download — the value of these tests is the
*decision logic* (when do we fall back?), not the hf-hub library itself.
"""

from __future__ import annotations

from unittest.mock import patch

import boto3
import pytest
from moto import mock_aws

from hf_cache_sync.config import AppConfig, CacheConfig, StorageConfig, TeamConfig
from hf_cache_sync.fallback import should_fallback
from hf_cache_sync.pull import PullError, pull
from hf_cache_sync.storage import StorageError

BUCKET = "fb-bucket"
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


# ── should_fallback decision logic ─────────────────────────────────


def test_should_fallback_transient_yes():
    err = StorageError("503", code="ServiceUnavailable", transient=True)
    assert should_fallback(err) is True


def test_should_fallback_auth_no():
    err = StorageError("403", code="AccessDenied", auth_failure=True)
    assert should_fallback(err) is False


def test_should_fallback_unknown_no():
    err = StorageError("weird", code="WeirdCode")
    assert should_fallback(err) is False


def test_should_fallback_endpoint_yes():
    err = StorageError("net", code="EndpointConnectionError", transient=True)
    assert should_fallback(err) is True


# ── pull() interaction ─────────────────────────────────────────────


def test_pull_no_fallback_surfaces_pull_error(s3, config):
    """Without --fallback, missing manifest raises PullError."""
    with pytest.raises(PullError):
        pull(config, "org/missing", "rev1")


def test_pull_fallback_handles_missing_manifest(s3, config):
    """With --fallback, a missing manifest invokes hf-hub instead."""
    with patch("hf_cache_sync.fallback.pull_via_hf_hub") as mock_hf:
        pull(config, "org/missing", "rev1", fallback="hf-hub")
        mock_hf.assert_called_once()
        args, _ = mock_hf.call_args
        assert args[1] == "org/missing"
        assert args[2] == "rev1"


def test_pull_fallback_does_not_fire_on_dry_run(s3, config):
    with patch("hf_cache_sync.fallback.pull_via_hf_hub") as mock_hf:
        # Dry-run with a missing manifest should still fail loudly, not fall back.
        with pytest.raises(PullError):
            pull(config, "org/missing", "rev1", fallback="hf-hub", dry_run=True)
        mock_hf.assert_not_called()


def test_pull_fallback_skips_auth_errors(config):
    """With --fallback, auth errors must still bubble up — never silently bypass."""
    # Bucket doesn't exist → NoSuchBucket → not auth, but not transient either
    # → should NOT fall back.
    with (
        mock_aws(),
        patch("hf_cache_sync.fallback.pull_via_hf_hub") as mock_hf,
        pytest.raises(StorageError),
    ):
        pull(config, "org/missing", "rev1", fallback="hf-hub")
    mock_hf.assert_not_called()


def test_pull_fallback_missing_extra_raises_clickexception(s3, config, monkeypatch):
    """If the [fallback] extra isn't installed, --fallback must give an actionable error."""
    # Simulate the import failing.
    import builtins

    import click

    from hf_cache_sync.fallback import pull_via_hf_hub

    real_import = builtins.__import__

    def fake_import(name, *args, **kwargs):
        if name == "huggingface_hub":
            raise ImportError("No module named 'huggingface_hub'")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fake_import)

    with pytest.raises(click.ClickException) as excinfo:
        pull_via_hf_hub(config, "org/x", "rev1")
    assert "hf-cache-sync[fallback]" in str(excinfo.value)
