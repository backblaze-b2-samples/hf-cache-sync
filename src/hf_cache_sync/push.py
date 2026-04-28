"""Push local cache to remote storage."""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from rich.console import Console
from rich.progress import Progress

from hf_cache_sync.cache import (
    RepoInfo,
    RevisionInfo,
    get_active_refs,
    is_likely_gated,
    is_xet_pointer,
    scan_cache,
)
from hf_cache_sync.config import AppConfig
from hf_cache_sync.manifest import Manifest, ManifestFile, ref_key
from hf_cache_sync.storage import StorageBackend

console = Console()
log = logging.getLogger(__name__)


def push(config: AppConfig, *, dry_run: bool = False, workers: int = 8) -> None:
    """Push all local cache blobs and manifests to remote storage."""
    repos = scan_cache(config.hf_cache_dir)
    if not repos:
        console.print("[yellow]No cached repos found.[/yellow]")
        return

    backend = StorageBackend(config)

    # One LIST instead of N HEAD: we know up front which blobs are already remote.
    # Race note: a concurrent pusher may upload an overlapping blob after this
    # snapshot — that's fine, S3 PUT of a content-addressed blob is idempotent.
    if dry_run:
        existing_blobs: set[str] = set()
    else:
        existing_blobs = {k for k in backend.list_keys("blobs/") if k.startswith("blobs/")}
    log.debug("Found %d existing remote blobs", len(existing_blobs))

    total_blobs = 0
    uploaded_blobs = 0
    skipped_blobs = 0
    skipped_gated = 0
    skipped_xet = 0

    with Progress(console=console) as progress:
        for repo in repos:
            # Gate check: skip repos that look gated unless opted-in.
            if not config.team.allow_gated and is_likely_gated(repo.repo_dir):
                skipped_gated += 1
                console.print(
                    f"[yellow]Skipping {repo.repo_id} (likely gated). "
                    f"Set allow_gated: true to include.[/yellow]"
                )
                continue

            task = progress.add_task(f"[cyan]{repo.repo_id}", total=len(repo.blobs))

            # Pre-filter blobs into (skip_existing, skip_xet, to_upload) buckets
            # so we can fan out the actual uploads concurrently.
            to_upload: list[tuple[str, Path]] = []
            for blob_hash, blob_info in repo.blobs.items():
                total_blobs += 1

                if not config.cache.sync_xet and is_xet_pointer(blob_info.path):
                    skipped_xet += 1
                    progress.advance(task)
                    continue

                key = f"blobs/{blob_hash}"
                if key in existing_blobs:
                    skipped_blobs += 1
                    progress.advance(task)
                    continue

                to_upload.append((key, blob_info.path))

            if dry_run:
                for key, _ in to_upload:
                    log.info("DRY-RUN would upload %s", key)
                    progress.advance(task)
                uploaded_blobs += len(to_upload)
            elif to_upload:
                # Rich Progress.advance is thread-safe; boto3 client too.
                with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
                    futures = {
                        pool.submit(backend.upload_file, path, key): key for key, path in to_upload
                    }
                    for fut in as_completed(futures):
                        key = futures[fut]
                        # Surface worker exceptions to the caller; ClickException
                        # in the CLI converts to non-zero exit.
                        fut.result()
                        existing_blobs.add(key)
                        uploaded_blobs += 1
                        progress.advance(task)

            # Upload manifests for each revision.
            for rev in repo.revisions:
                _upload_manifest(
                    backend,
                    repo,
                    rev,
                    skip_xet=not config.cache.sync_xet,
                    dry_run=dry_run,
                )

            # Upload refs.
            _upload_refs(backend, repo, dry_run=dry_run)

    parts = [
        f"{'Would upload' if dry_run else 'Uploaded'} {uploaded_blobs} blobs",
        f"skipped {skipped_blobs} existing",
    ]
    if skipped_gated:
        parts.append(f"skipped {skipped_gated} gated repos")
    if skipped_xet:
        parts.append(f"skipped {skipped_xet} xet pointers")
    label = "[blue]Dry run.[/blue]" if dry_run else "[green]Done.[/green]"
    console.print(f"{label} {', '.join(parts)}. Total: {total_blobs}.")


def _upload_manifest(
    backend: StorageBackend,
    repo: RepoInfo,
    rev: RevisionInfo,
    *,
    skip_xet: bool = False,
    dry_run: bool = False,
) -> None:
    files = []
    for f in rev.files:
        if skip_xet:
            blob_info = repo.blobs.get(f.blob_hash)
            if blob_info and is_xet_pointer(blob_info.path):
                continue
        files.append(ManifestFile(path=f.relative_path, blob=f.blob_hash, size=f.size))

    if not files:
        return

    manifest = Manifest(
        repo=repo.repo_id,
        revision=rev.revision,
        repo_type=repo.repo_type,
        files=files,
    )
    if dry_run:
        log.info("DRY-RUN would upload manifest %s", manifest.remote_key)
        return
    backend.upload_bytes(manifest.to_json().encode(), manifest.remote_key)


def _upload_refs(backend: StorageBackend, repo: RepoInfo, *, dry_run: bool = False) -> None:
    refs = get_active_refs(repo.repo_dir)
    for ref_name, commit_hash in refs.items():
        key = ref_key(repo.repo_id, ref_name)
        if dry_run:
            log.info("DRY-RUN would upload ref %s -> %s", key, commit_hash)
            continue
        backend.upload_bytes(commit_hash.encode(), key)
