"""Push local cache to remote storage."""

from __future__ import annotations

from rich.console import Console
from rich.progress import Progress

from hf_cache_sync.cache import RevisionInfo, RepoInfo, get_active_refs, scan_cache
from hf_cache_sync.config import AppConfig
from hf_cache_sync.manifest import Manifest, ManifestFile
from hf_cache_sync.storage import StorageBackend

console = Console()


def push(config: AppConfig) -> None:
    """Push all local cache blobs and manifests to remote storage."""
    repos = scan_cache(config.hf_cache_dir)
    if not repos:
        console.print("[yellow]No cached repos found.[/yellow]")
        return

    backend = StorageBackend(config)

    total_blobs = 0
    uploaded_blobs = 0
    skipped_blobs = 0

    with Progress(console=console) as progress:
        for repo in repos:
            task = progress.add_task(f"[cyan]{repo.repo_id}", total=len(repo.blobs))

            for blob_hash, blob_info in repo.blobs.items():
                total_blobs += 1
                key = f"blobs/{blob_hash}"

                if backend.exists(key):
                    skipped_blobs += 1
                else:
                    backend.upload_file(blob_info.path, key)
                    uploaded_blobs += 1

                progress.advance(task)

            # Upload manifests for each revision
            for rev in repo.revisions:
                _upload_manifest(backend, repo, rev)

            # Upload refs
            _upload_refs(backend, repo)

    console.print(
        f"[green]Done.[/green] "
        f"Uploaded {uploaded_blobs} blobs, skipped {skipped_blobs} existing. "
        f"Total: {total_blobs}."
    )


def _upload_manifest(backend: StorageBackend, repo: RepoInfo, rev: RevisionInfo) -> None:
    manifest = Manifest(
        repo=repo.repo_id,
        revision=rev.revision,
        files=[
            ManifestFile(path=f.relative_path, blob=f.blob_hash, size=f.size)
            for f in rev.files
        ],
    )
    backend.upload_bytes(manifest.to_json().encode(), manifest.remote_key)


def _upload_refs(backend: StorageBackend, repo: RepoInfo) -> None:
    refs = get_active_refs(repo.repo_dir)
    safe_repo = repo.repo_id.replace("/", "__")
    for ref_name, commit_hash in refs.items():
        key = f"refs/{safe_repo}/{ref_name}"
        backend.upload_bytes(commit_hash.encode(), key)
