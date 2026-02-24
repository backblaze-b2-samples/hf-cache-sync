"""CLI entry point."""

from __future__ import annotations

from pathlib import Path

import click
from rich.console import Console
from rich.table import Table

from hf_cache_sync import __version__
from hf_cache_sync.cache import scan_cache, total_cache_size
from hf_cache_sync.config import load_config

console = Console()

GB = 1 << 30


@click.group()
@click.version_option(__version__)
@click.option("--config", "config_path", type=click.Path(exists=True, path_type=Path), default=None)
@click.pass_context
def cli(ctx: click.Context, config_path: Path | None) -> None:
    """hf-cache-sync: Shared Hugging Face Cache Manager."""
    ctx.ensure_object(dict)
    ctx.obj["config"] = load_config(config_path)


@cli.command()
@click.pass_context
def init(ctx: click.Context) -> None:
    """Initialize configuration file."""
    target = Path.cwd() / ".hf-cache-sync.yaml"
    if target.exists():
        console.print(f"[yellow]{target} already exists.[/yellow]")
        return

    template = """\
storage:
  endpoint: https://s3.us-west-000.backblazeb2.com
  bucket: my-hf-cache
  region: us-west-000
  access_key: ""
  secret_key: ""

cache:
  max_local_gb: 50
  sync_xet: false

team:
  prefix: ""
  allow_gated: false
"""
    target.write_text(template)
    console.print(f"[green]Created {target}[/green]")


@cli.command()
@click.pass_context
def push(ctx: click.Context) -> None:
    """Push local cache to remote storage."""
    from hf_cache_sync.push import push as do_push
    do_push(ctx.obj["config"])


@cli.command()
@click.argument("repo_id")
@click.option("--revision", "-r", default=None, help="Specific revision hash.")
@click.pass_context
def pull(ctx: click.Context, repo_id: str, revision: str | None) -> None:
    """Pull a repo from remote storage into local cache."""
    from hf_cache_sync.pull import pull as do_pull
    do_pull(ctx.obj["config"], repo_id, revision)


@cli.command("pull-all")
@click.pass_context
def pull_all(ctx: click.Context) -> None:
    """Pull all available repos from remote storage."""
    from hf_cache_sync.pull import pull_all as do_pull_all
    do_pull_all(ctx.obj["config"])


@cli.command()
@click.option("--max-gb", type=float, default=None, help="Max local cache size in GB.")
@click.pass_context
def prune(ctx: click.Context, max_gb: float | None) -> None:
    """Evict least-recently-used revisions to stay within disk budget."""
    from hf_cache_sync.prune import prune as do_prune
    do_prune(ctx.obj["config"], max_gb)


@cli.command()
@click.pass_context
def status(ctx: click.Context) -> None:
    """Show local cache status summary."""
    config = ctx.obj["config"]
    repos = scan_cache(config.hf_cache_dir)
    size = total_cache_size(repos)

    console.print(f"Cache dir: {config.hf_cache_dir}")
    console.print(f"Repos:     {len(repos)}")
    console.print(f"Size:      {size / GB:.2f} GB")
    console.print(f"Budget:    {config.cache.max_local_gb:.0f} GB")

    if config.storage.bucket:
        console.print(f"Bucket:    {config.storage.bucket}")
    else:
        console.print("[yellow]No remote storage configured.[/yellow]")


@cli.command("list")
@click.pass_context
def list_repos(ctx: click.Context) -> None:
    """List cached repos and revisions."""
    config = ctx.obj["config"]
    repos = scan_cache(config.hf_cache_dir)

    if not repos:
        console.print("[yellow]No cached repos found.[/yellow]")
        return

    table = Table(title="Local HF Cache")
    table.add_column("Repo", style="cyan")
    table.add_column("Revisions", justify="right")
    table.add_column("Size (GB)", justify="right")
    table.add_column("Blobs", justify="right")

    for repo in repos:
        size = sum(b.size for b in repo.blobs.values())
        table.add_row(
            repo.repo_id,
            str(len(repo.revisions)),
            f"{size / GB:.2f}",
            str(len(repo.blobs)),
        )

    console.print(table)
