"""CLI entry point."""

from __future__ import annotations

import logging
from pathlib import Path

import click
from rich.console import Console
from rich.logging import RichHandler
from rich.table import Table

from hf_cache_sync import __version__
from hf_cache_sync.cache import scan_cache, total_cache_size
from hf_cache_sync.config import load_config

console = Console()

GB = 1 << 30

# Use Click's standard help options on every command/group.
CONTEXT_SETTINGS = {"help_option_names": ["-h", "--help"]}


def _configure_logging(verbose: bool) -> None:
    """Stream logs through Rich; -v lifts to DEBUG, otherwise WARNING."""
    level = logging.DEBUG if verbose else logging.WARNING
    logging.basicConfig(
        level=level,
        format="%(message)s",
        datefmt="[%X]",
        handlers=[RichHandler(console=console, rich_tracebacks=True, show_time=False)],
        force=True,
    )


@click.group(context_settings=CONTEXT_SETTINGS)
@click.version_option(__version__)
@click.option(
    "--config",
    "config_path",
    type=click.Path(exists=True, path_type=Path),
    default=None,
    help="Path to a config YAML. Defaults to ./.hf-cache-sync.yaml then ~/.hf-cache-sync.yaml.",
)
@click.option("-v", "--verbose", is_flag=True, help="Enable debug logging.")
@click.pass_context
def cli(ctx: click.Context, config_path: Path | None, verbose: bool) -> None:
    """hf-cache-sync: Shared Hugging Face Cache Manager."""
    _configure_logging(verbose)
    ctx.ensure_object(dict)
    ctx.obj["config"] = load_config(config_path)


@cli.command()
def init() -> None:
    """Initialize configuration file in the current directory."""
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
@click.option("--dry-run", is_flag=True, help="Show what would be uploaded without writing.")
@click.option("--workers", type=int, default=8, show_default=True, help="Concurrent blob uploads.")
@click.pass_context
def push(ctx: click.Context, dry_run: bool, workers: int) -> None:
    """Push local cache to remote storage."""
    from hf_cache_sync.push import push as do_push

    try:
        do_push(ctx.obj["config"], dry_run=dry_run, workers=workers)
    except Exception as e:
        raise click.ClickException(str(e)) from e


@cli.command()
@click.argument("repo_id")
@click.option("--revision", "-r", default=None, help="Specific revision hash.")
@click.option("--dry-run", is_flag=True, help="Report what would be downloaded.")
@click.option(
    "--workers", type=int, default=8, show_default=True, help="Concurrent blob downloads."
)
@click.pass_context
def pull(
    ctx: click.Context, repo_id: str, revision: str | None, dry_run: bool, workers: int
) -> None:
    """Pull a repo from remote storage into local cache."""
    from hf_cache_sync.pull import PullError
    from hf_cache_sync.pull import pull as do_pull

    try:
        do_pull(ctx.obj["config"], repo_id, revision, dry_run=dry_run, workers=workers)
    except PullError as e:
        raise click.ClickException(str(e)) from e


@cli.command("pull-all")
@click.option("--dry-run", is_flag=True, help="Report what would be downloaded.")
@click.option("--limit", type=int, default=None, help="Pull at most N repos.")
@click.option(
    "--include",
    "include_patterns",
    multiple=True,
    help="fnmatch glob to include (repeatable). Defaults to all.",
)
@click.option(
    "--exclude",
    "exclude_patterns",
    multiple=True,
    help="fnmatch glob to exclude (repeatable).",
)
@click.option(
    "--workers", type=int, default=8, show_default=True, help="Concurrent blob downloads per repo."
)
@click.pass_context
def pull_all(
    ctx: click.Context,
    dry_run: bool,
    limit: int | None,
    include_patterns: tuple[str, ...],
    exclude_patterns: tuple[str, ...],
    workers: int,
) -> None:
    """Pull all available repos from remote storage."""
    from hf_cache_sync.pull import pull_all as do_pull_all

    try:
        do_pull_all(
            ctx.obj["config"],
            dry_run=dry_run,
            limit=limit,
            include=list(include_patterns) or None,
            exclude=list(exclude_patterns) or None,
            workers=workers,
        )
    except Exception as e:
        raise click.ClickException(str(e)) from e


@cli.command()
@click.option("--max-gb", type=float, default=None, help="Max local cache size in GB.")
@click.option("--dry-run", is_flag=True, help="Report what would be evicted.")
@click.pass_context
def prune(ctx: click.Context, max_gb: float | None, dry_run: bool) -> None:
    """Evict least-recently-used revisions to stay within disk budget."""
    from hf_cache_sync.prune import prune as do_prune

    try:
        do_prune(ctx.obj["config"], max_gb, dry_run=dry_run)
    except Exception as e:
        raise click.ClickException(str(e)) from e


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
