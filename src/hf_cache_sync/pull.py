"""Pull (hydrate) cache from remote storage."""

from __future__ import annotations

import hashlib
import os
import sys
from pathlib import Path

from rich.console import Console
from rich.progress import Progress

from hf_cache_sync.config import AppConfig
from hf_cache_sync.manifest import Manifest
from hf_cache_sync.storage import StorageBackend

console = Console()


def pull(config: AppConfig, repo_id: str, revision: str | None = None) -> None:
    """Pull a repo from remote storage into local HF cache."""
    backend = StorageBackend(config)

    # Resolve revision from ref if not given
    if not revision:
        revision = _resolve_ref(backend, repo_id, "main")
        if not revision:
            console.print(f"[red]No revision found for {repo_id}. Specify --revision.[/red]")
            return

    # Fetch manifest
    manifest = _fetch_manifest(backend, repo_id, revision)
    if not manifest:
        console.print(f"[red]Manifest not found for {repo_id}@{revision}[/red]")
        return

    cache_dir = config.hf_cache_dir
    safe_name = "models--" + repo_id.replace("/", "--")
    repo_dir = cache_dir / safe_name

    blobs_dir = repo_dir / "blobs"
    snapshot_dir = repo_dir / "snapshots" / revision
    refs_dir = repo_dir / "refs"

    blobs_dir.mkdir(parents=True, exist_ok=True)
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    refs_dir.mkdir(parents=True, exist_ok=True)

    # Download blobs
    with Progress(console=console) as progress:
        task = progress.add_task(f"[cyan]Pulling {repo_id}", total=len(manifest.files))

        for entry in manifest.files:
            blob_path = blobs_dir / entry.blob
            blob_key = f"blobs/{entry.blob}"

            if not blob_path.exists():
                backend.download_file(blob_key, blob_path)

                # Verify hash
                actual = _sha256_file(blob_path)
                if actual != entry.blob:
                    blob_path.unlink()
                    console.print(
                        f"[red]Hash mismatch for {entry.path}: "
                        f"expected {entry.blob[:16]}... got {actual[:16]}...[/red]"
                    )
                    return

            progress.advance(task)

    # Reconstruct snapshot symlinks
    for entry in manifest.files:
        blob_path = blobs_dir / entry.blob
        link_path = snapshot_dir / entry.path
        link_path.parent.mkdir(parents=True, exist_ok=True)

        if link_path.exists() or link_path.is_symlink():
            link_path.unlink()

        _create_link(blob_path, link_path)

    # Write ref
    ref_file = refs_dir / "main"
    ref_file.write_text(revision)

    console.print(f"[green]Hydrated {repo_id}@{revision[:12]}[/green] ({len(manifest.files)} files)")


def pull_all(config: AppConfig) -> None:
    """Pull all repos available in remote storage."""
    backend = StorageBackend(config)
    manifest_keys = [k for k in backend.list_keys("manifests/") if k.endswith(".json")]

    if not manifest_keys:
        console.print("[yellow]No manifests found in remote storage.[/yellow]")
        return

    seen: set[str] = set()
    for key in manifest_keys:
        data = backend.download_bytes(key)
        manifest = Manifest.from_json(data.decode())
        pair = f"{manifest.repo}@{manifest.revision}"
        if pair in seen:
            continue
        seen.add(pair)
        console.print(f"Pulling {manifest.repo}@{manifest.revision[:12]}...")
        pull(config, manifest.repo, manifest.revision)


def _resolve_ref(backend: StorageBackend, repo_id: str, ref: str) -> str | None:
    safe_repo = repo_id.replace("/", "__")
    key = f"refs/{safe_repo}/{ref}"
    try:
        return backend.download_bytes(key).decode().strip()
    except Exception:
        return None


def _fetch_manifest(backend: StorageBackend, repo_id: str, revision: str) -> Manifest | None:
    safe_repo = repo_id.replace("/", "__")
    key = f"manifests/{safe_repo}@{revision}.json"
    try:
        data = backend.download_bytes(key)
        return Manifest.from_json(data.decode())
    except Exception:
        return None


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8 * 1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _create_link(target: Path, link: Path) -> None:
    """Create symlink, falling back to hardlink or copy on Windows."""
    try:
        link.symlink_to(target)
    except OSError:
        # Windows without symlink privilege — try hardlink
        try:
            os.link(target, link)
        except OSError:
            # Last resort: copy
            import shutil
            shutil.copy2(target, link)
