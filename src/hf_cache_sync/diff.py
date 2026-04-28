"""Compare local cache contents against remote storage.

Cost note: the remote view comes from a single paginated ``ListObjectsV2``
on the ``manifests/`` prefix — O(N/1000) requests, no per-object GETs. We
deliberately do *not* download manifest bodies; sizes/file-counts would
require N GETs and aren't shown here.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from rich.console import Console
from rich.table import Table

from hf_cache_sync.cache import scan_cache
from hf_cache_sync.config import AppConfig
from hf_cache_sync.manifest import parse_manifest_key
from hf_cache_sync.storage import StorageBackend

console = Console()


@dataclass(frozen=True)
class RevisionRef:
    repo_id: str
    revision: str


def collect_local(cache_dir: Path) -> dict[str, set[str]]:
    """Return ``{repo_id: {revision, ...}}`` for everything in the local HF cache."""
    out: dict[str, set[str]] = {}
    for repo in scan_cache(cache_dir):
        out.setdefault(repo.repo_id, set()).update(rev.revision for rev in repo.revisions)
    return out


def collect_remote(backend: StorageBackend) -> dict[str, set[str]]:
    """Return ``{repo_id: {revision, ...}}`` for everything in remote storage."""
    out: dict[str, set[str]] = {}
    for key in backend.list_keys("manifests/"):
        parsed = parse_manifest_key(key)
        if parsed is None:
            continue
        repo_id, revision = parsed
        out.setdefault(repo_id, set()).add(revision)
    return out


def diff_status(
    local: dict[str, set[str]], remote: dict[str, set[str]]
) -> list[tuple[str, str, str]]:
    """Return rows of ``(repo_id, revision, status)`` for every revision.

    Status is one of ``local-only``, ``remote-only``, ``in-sync``.
    Rows are sorted (repo_id, revision) for stable output.
    """
    repos = sorted(set(local) | set(remote))
    rows: list[tuple[str, str, str]] = []
    for repo in repos:
        local_revs = local.get(repo, set())
        remote_revs = remote.get(repo, set())
        for rev in sorted(local_revs & remote_revs):
            rows.append((repo, rev, "in-sync"))
        for rev in sorted(local_revs - remote_revs):
            rows.append((repo, rev, "local-only"))
        for rev in sorted(remote_revs - local_revs):
            rows.append((repo, rev, "remote-only"))
    return rows


def render_diff(rows: list[tuple[str, str, str]]) -> Table:
    table = Table(title="hf-cache-sync diff")
    table.add_column("Repo", style="cyan")
    table.add_column("Revision")
    table.add_column("Status")
    color = {
        "in-sync": "[green]in-sync[/green]",
        "local-only": "[yellow]local-only[/yellow]",
        "remote-only": "[blue]remote-only[/blue]",
    }
    for repo, rev, status in rows:
        table.add_row(repo, rev[:12], color.get(status, status))
    return table


def render_remote_list(remote: dict[str, set[str]]) -> Table:
    table = Table(title="Remote manifests")
    table.add_column("Repo", style="cyan")
    table.add_column("Revisions", justify="right")
    table.add_column("Latest revision (short)")
    for repo in sorted(remote):
        revs = sorted(remote[repo])
        table.add_row(repo, str(len(revs)), revs[-1][:12] if revs else "—")
    return table


def diff(config: AppConfig) -> None:
    """Print a ``repo / revision / status`` diff between local and remote."""
    local = collect_local(config.hf_cache_dir)
    backend = StorageBackend(config)
    remote = collect_remote(backend)
    rows = diff_status(local, remote)
    if not rows:
        console.print("[yellow]No repos local or remote.[/yellow]")
        return
    console.print(render_diff(rows))
    counts = {"in-sync": 0, "local-only": 0, "remote-only": 0}
    for _, _, s in rows:
        counts[s] = counts.get(s, 0) + 1
    console.print(
        f"[green]{counts['in-sync']} in-sync[/green], "
        f"[yellow]{counts['local-only']} local-only[/yellow], "
        f"[blue]{counts['remote-only']} remote-only[/blue]."
    )


def list_remote(config: AppConfig) -> None:
    """Print remote-available repos and their revision counts."""
    backend = StorageBackend(config)
    remote = collect_remote(backend)
    if not remote:
        console.print("[yellow]No manifests found in remote storage.[/yellow]")
        return
    console.print(render_remote_list(remote))
