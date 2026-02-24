"""LRU eviction for local HF cache."""

from __future__ import annotations

import shutil

from rich.console import Console

from hf_cache_sync.cache import (
    RevisionInfo,
    get_active_refs,
    scan_cache,
    total_cache_size,
)
from hf_cache_sync.config import AppConfig

console = Console()

GB = 1 << 30


def prune(config: AppConfig, max_gb: float | None = None) -> None:
    """Evict least-recently-used revisions until cache fits within budget."""
    budget = max_gb if max_gb is not None else config.cache.max_local_gb
    budget_bytes = int(budget * GB)

    repos = scan_cache(config.hf_cache_dir)
    current_size = total_cache_size(repos)

    if current_size <= budget_bytes:
        console.print(
            f"[green]Cache is {current_size / GB:.1f} GB, within {budget:.0f} GB budget.[/green]"
        )
        return

    console.print(
        f"Cache is [yellow]{current_size / GB:.1f} GB[/yellow], "
        f"budget is {budget:.0f} GB. Pruning..."
    )

    # Collect all revisions with their repo context
    candidates: list[tuple[RevisionInfo, set[str]]] = []
    for repo in repos:
        active_hashes = set(get_active_refs(repo.repo_dir).values())
        for rev in repo.revisions:
            candidates.append((rev, active_hashes))

    # Sort by access time ascending (oldest first)
    candidates.sort(key=lambda x: x[0].atime)

    freed = 0
    removed_count = 0

    for rev, active_refs in candidates:
        if current_size - freed <= budget_bytes:
            break

        # Skip revisions pointed to by active refs
        if rev.revision in active_refs:
            continue

        # Remove the snapshot directory
        if rev.snapshot_dir.exists():
            shutil.rmtree(rev.snapshot_dir)
            freed += rev.total_size
            removed_count += 1
            console.print(
                f"  Removed {rev.repo_id}@{rev.revision[:12]} "
                f"({rev.total_size / GB:.2f} GB)"
            )

    # Clean up orphaned blobs
    orphaned = _cleanup_orphaned_blobs(config)

    console.print(
        f"[green]Pruned {removed_count} revisions, freed {freed / GB:.1f} GB. "
        f"Removed {orphaned} orphaned blobs.[/green]"
    )


def _cleanup_orphaned_blobs(config: AppConfig) -> int:
    """Remove blobs not referenced by any snapshot."""
    repos = scan_cache(config.hf_cache_dir)
    count = 0

    for repo in repos:
        referenced: set[str] = set()
        for rev in repo.revisions:
            for f in rev.files:
                referenced.add(f.blob_hash)

        for blob_hash, blob_info in repo.blobs.items():
            if blob_hash not in referenced:
                blob_info.path.unlink(missing_ok=True)
                count += 1

    return count
