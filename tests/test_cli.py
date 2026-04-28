"""Tests for CLI commands."""

import boto3
from click.testing import CliRunner
from moto import mock_aws

from hf_cache_sync.cli import cli


def test_version():
    runner = CliRunner()
    result = runner.invoke(cli, ["--version"])
    assert result.exit_code == 0
    assert "0.1.0" in result.output


def test_status(tmp_path):
    runner = CliRunner()
    config = tmp_path / ".hf-cache-sync.yaml"
    config.write_text(f"""\
cache:
  hf_cache_dir: {tmp_path / "hub"}
  max_local_gb: 10
""")
    (tmp_path / "hub").mkdir()
    result = runner.invoke(cli, ["--config", str(config), "status"])
    assert result.exit_code == 0
    assert "0.00 GB" in result.output


def test_init(tmp_path):
    runner = CliRunner()
    with runner.isolated_filesystem(temp_dir=tmp_path):
        result = runner.invoke(cli, ["init"])
        assert result.exit_code == 0
        assert "Created" in result.output


def test_list_empty(tmp_path):
    runner = CliRunner()
    config = tmp_path / ".hf-cache-sync.yaml"
    config.write_text(f"""\
cache:
  hf_cache_dir: {tmp_path / "hub"}
""")
    (tmp_path / "hub").mkdir()
    result = runner.invoke(cli, ["--config", str(config), "list"])
    assert result.exit_code == 0
    assert "No cached repos" in result.output


def test_pull_missing_manifest_exits_nonzero(tmp_path):
    """A failed pull (manifest not found) must exit non-zero so CI catches it."""
    runner = CliRunner()
    cache_dir = tmp_path / "hub"
    cache_dir.mkdir()
    config = tmp_path / ".hf-cache-sync.yaml"
    config.write_text(f"""\
storage:
  bucket: test-bucket
  region: us-east-1
cache:
  hf_cache_dir: {cache_dir}
""")

    with mock_aws():
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket="test-bucket")
        result = runner.invoke(cli, ["--config", str(config), "pull", "org/missing", "-r", "abc"])

    assert result.exit_code != 0
    assert "Manifest not found" in result.output


def test_help_short_flag():
    """-h should work as an alias for --help."""
    runner = CliRunner()
    result = runner.invoke(cli, ["-h"])
    assert result.exit_code == 0
    assert "hf-cache-sync" in result.output
