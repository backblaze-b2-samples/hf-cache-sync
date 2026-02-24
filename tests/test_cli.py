"""Tests for CLI commands."""

from click.testing import CliRunner

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
