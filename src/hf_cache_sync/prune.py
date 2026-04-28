"""LRU eviction for local HF cache."""

from __future__ import annotations

import logging
import shutil

from rich.console import Console

from hf_cache_sync.cache import (
    RepoInfo,
    RevisionInfo,
    get_active_refs,
    scan_cache,
    total_cache_size,
)
from hf_cache_sync.config import AppConfig

console = Console()
log = logging.getLogger(__name__)

GB = 1 << 30


def prune(config: AppConfig, max_gb: float | None = None, *, dry_run: bool = False) -> None:
    """Evict least-recently-used revisions until cache fits within budget.

    Budget accounting tracks *unique blob bytes* freed (a blob shared by
    multiple revisions only counts once), matching what's actually reclaimed
    on disk after orphan blob cleanup.
    """
    budget = max_gb if max_gb is not None else config.cache.max_local_gb
    budget_bytes = int(budget * GB)

    repos = scan_cache(config.hf_cache_dir)
    current_size = total_cache_size(repos)

    # Always clean orphaned blobs, even when under budget (unless dry-run).
    orphaned = _cleanup_orphaned_blobs(repos, dry_run=dry_run)

    if current_size <= budget_bytes:
        msg = f"[green]Cache is {current_size / GB:.1f} GB, within {budget:.0f} GB budget.[/green]"
        if orphaned:
            verb = "Would remove" if dry_run else "Removed"
            msg += f" {verb} {orphaned} orphaned blobs."
        console.print(msg)
        return

    console.print(
        f"Cache is [yellow]{current_size / GB:.1f} GB[/yellow], "
        f"budget is {budget:.0f} GB. {'Would prune' if dry_run else 'Pruning'}..."
    )

    # Index blobs once, so we can resolve sizes per blob hash across the cache.
    blob_size_by_repo: dict[str, dict[str, int]] = {
        repo.repo_id: {h: b.size for h, b in repo.blobs.items()} for repo in repos
    }

    candidates: list[tuple[RepoInfo, RevisionInfo, set[str]]] = []
    for repo in repos:
        active_hashes = set(get_active_refs(repo.repo_dir).values())
        for rev in repo.revisions:
            candidates.append((repo, rev, active_hashes))

    # Oldest access time first.
    candidates.sort(key=lambda x: x[1].atime)

    freed_blobs: set[str] = set()
    freed_bytes = 0
    removed_count = 0

    for repo, rev, active_refs in candidates:
        if current_size - freed_bytes <= budget_bytes:
            break

        # Skip revisions pointed to by active refs.
        if rev.revision in active_refs:
            continue

        if not rev.snapshot_dir.exists():
            continue

        # Compute incremental bytes that this revision uniquely contributes —
        # blobs shared with surviving revisions are NOT freed.
        rev_blobs = {f.blob_hash for f in rev.files}
        new_unique = rev_blobs - freed_blobs
        sizes = blob_size_by_repo.get(repo.repo_id, {})
        rev_freed = sum(sizes.get(h, 0) for h in new_unique)

        if dry_run:
            log.info(
                "DRY-RUN would remove %s@%s (%.2f GB)",
                rev.repo_id,
                rev.revision[:12],
                rev_freed / GB,
            )
        else:
            shutil.rmtree(rev.snapshot_dir)

        freed_blobs |= new_unique
        freed_bytes += rev_freed
        removed_count += 1
        verb = "Would remove" if dry_run else "Removed"
        console.print(f"  {verb} {rev.repo_id}@{rev.revision[:12]} ({rev_freed / GB:.2f} GB)")

    # Re-scan to clean newly orphaned blobs after eviction.
    if removed_count and not dry_run:
        repos = scan_cache(config.hf_cache_dir)
        orphaned += _cleanup_orphaned_blobs(repos)

    label = "[blue]Dry run.[/blue]" if dry_run else "[green]"
    suffix = "[/green]" if not dry_run else ""
    console.print(
        f"{label} {'Would prune' if dry_run else 'Pruned'} {removed_count} revisions, "
        f"{'would free' if dry_run else 'freed'} {freed_bytes / GB:.1f} GB. "
        f"{'Would remove' if dry_run else 'Removed'} {orphaned} orphaned blobs.{suffix}"
    )


def _cleanup_orphaned_blobs(repos: list[RepoInfo], *, dry_run: bool = False) -> int:
    """Remove blobs not referenced by any snapshot. Operates on in-memory repo data."""
    count = 0

    for repo in repos:
        referenced: set[str] = set()
        for rev in repo.revisions:
            for f in rev.files:
                referenced.add(f.blob_hash)

        for blob_hash, blob_info in repo.blobs.items():
            if blob_hash not in referenced:
                if not dry_run:
                    blob_info.path.unlink(missing_ok=True)
                count += 1

    return count
