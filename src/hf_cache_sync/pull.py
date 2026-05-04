"""Pull (hydrate) cache from remote storage."""

from __future__ import annotations

import logging
import os
import shutil
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from rich.console import Console
from rich.progress import Progress

from hf_cache_sync.cache import is_likely_gated, repo_id_to_dirname, sha256_file
from hf_cache_sync.config import AppConfig
from hf_cache_sync.manifest import Manifest, manifest_key, ref_key
from hf_cache_sync.storage import NOT_FOUND_CODES, StorageBackend, StorageError

console = Console()
log = logging.getLogger(__name__)


class PullError(RuntimeError):
    """Raised when a pull operation fails. Carries an exit-worthy message."""


def pull(
    config: AppConfig,
    repo_id: str,
    revision: str | None = None,
    repo_type: str = "model",
    *,
    dry_run: bool = False,
    workers: int = 8,
    fallback: str | None = None,
) -> None:
    """Pull a repo from remote storage into local HF cache.

    Raises PullError / StorageError on remote / manifest errors. The CLI
    converts those into a non-zero exit code unless ``fallback="hf-hub"`` is
    set, in which case the call is retried against ``huggingface_hub`` for
    transient or "missing in remote" failures (auth errors still surface).
    """
    try:
        _pull_native(config, repo_id, revision, repo_type, dry_run=dry_run, workers=workers)
    except (StorageError, PullError) as e:
        if fallback != "hf-hub" or dry_run:
            raise
        # Auth/permission errors are config bugs — never paper over them.
        if isinstance(e, StorageError) and e.auth_failure:
            raise
        from hf_cache_sync.fallback import pull_via_hf_hub, should_fallback

        # StorageError must be transient; PullError ("not found", "hash mismatch",
        # etc.) is always allowed since the user explicitly opted into fallback.
        if isinstance(e, StorageError) and not should_fallback(e):
            raise
        log.warning("primary pull failed (%s); falling back to hf-hub", e)
        pull_via_hf_hub(config, repo_id, revision, repo_type)


def _pull_native(
    config: AppConfig,
    repo_id: str,
    revision: str | None,
    repo_type: str,
    *,
    dry_run: bool,
    workers: int,
) -> None:
    backend = StorageBackend(config, workers=workers)
    resolved_from_ref = False

    # Resolve revision from ref if not given.
    if not revision:
        revision = _resolve_ref(backend, repo_id, "main")
        if not revision:
            raise PullError(f"No revision found for {repo_id}. Specify --revision.")
        resolved_from_ref = True

    manifest = _fetch_manifest(backend, repo_id, revision)
    if not manifest:
        raise PullError(f"Manifest not found for {repo_id}@{revision}")

    # Use repo_type from manifest (authoritative) over the default.
    effective_type = manifest.repo_type or repo_type

    dir_name = repo_id_to_dirname(repo_id, effective_type)
    cache_dir = config.hf_cache_dir
    repo_dir = cache_dir / dir_name

    # Gate check: if the repo already exists locally and looks gated, block unless opted-in.
    if not config.team.allow_gated and repo_dir.is_dir() and is_likely_gated(repo_dir):
        console.print(
            f"[yellow]Skipping {repo_id} (likely gated). Set allow_gated: true to include.[/yellow]"
        )
        return

    blobs_dir = repo_dir / "blobs"
    snapshot_dir = repo_dir / "snapshots" / revision
    refs_dir = repo_dir / "refs"

    if dry_run:
        missing = sum(1 for f in manifest.files if not (blobs_dir / f.blob).exists())
        console.print(
            f"[blue]DRY-RUN[/blue] {repo_id}@{revision[:12]}: "
            f"{missing}/{len(manifest.files)} blobs would be downloaded."
        )
        return

    blobs_dir.mkdir(parents=True, exist_ok=True)
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    refs_dir.mkdir(parents=True, exist_ok=True)

    # Deduplicate blobs that need fetching — manifests can list the same blob
    # under multiple paths (rare but possible for symlinked files).
    blobs_to_fetch: dict[str, Path] = {}
    for entry in manifest.files:
        bp = blobs_dir / entry.blob
        if not bp.exists() and entry.blob not in blobs_to_fetch:
            blobs_to_fetch[entry.blob] = bp

    with Progress(console=console) as progress:
        task = progress.add_task(f"[cyan]Pulling {repo_id}", total=len(manifest.files))
        # Files already present (or duplicates of in-flight downloads) advance immediately.
        progress.advance(task, advance=len(manifest.files) - len(blobs_to_fetch))

        if blobs_to_fetch:
            with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
                futures = {
                    pool.submit(_download_and_verify_blob, backend, h, p): h
                    for h, p in blobs_to_fetch.items()
                }
                try:
                    for fut in as_completed(futures):
                        # Surface PullError (hash mismatch) and any ClientError up.
                        fut.result()
                        progress.advance(task)
                except BaseException:
                    # Cancel pending downloads on first failure so we exit fast;
                    # in-flight ones still need to finish before the pool joins.
                    for f in futures:
                        f.cancel()
                    raise

    # Reconstruct snapshot symlinks.
    for entry in manifest.files:
        blob_path = blobs_dir / entry.blob
        link_path = snapshot_dir / entry.path
        link_path.parent.mkdir(parents=True, exist_ok=True)

        # Use is_symlink() too — exists() returns False for broken symlinks.
        if link_path.exists() or link_path.is_symlink():
            link_path.unlink()

        _create_link(blob_path, link_path)

    # Only write refs/main if the revision was resolved from the main ref.
    # Explicit --revision pulls should not overwrite an existing main ref.
    if resolved_from_ref:
        ref_file = refs_dir / "main"
        ref_file.write_text(revision)

    console.print(
        f"[green]Hydrated {repo_id}@{revision[:12]}[/green] ({len(manifest.files)} files)"
    )


def pull_all(
    config: AppConfig,
    *,
    dry_run: bool = False,
    limit: int | None = None,
    include: list[str] | None = None,
    exclude: list[str] | None = None,
    workers: int = 8,
    fallback: str | None = None,
) -> None:
    """Pull every repo whose ``refs/main`` is present in remote storage.

    We iterate refs (not manifests) so each ``pull`` resolves from a ref and
    therefore writes the local ``refs/main`` — keeping the cache layout
    consistent with what huggingface_hub expects.
    """
    import fnmatch

    # pull-all only uses this backend for the initial refs/ listing on the
    # main thread; per-repo pulls inside the loop construct their own
    # backend sized to ``workers``.
    backend = StorageBackend(config)
    ref_keys = [k for k in backend.list_keys("refs/") if k.endswith("/main")]

    if not ref_keys:
        console.print("[yellow]No refs/main found in remote storage.[/yellow]")
        return

    repos: list[str] = []
    seen: set[str] = set()
    for key in ref_keys:
        parts = key.split("/")
        if len(parts) != 3:
            continue
        repo_id = parts[1].replace("__", "/")
        if repo_id in seen:
            continue
        seen.add(repo_id)

        if include and not any(fnmatch.fnmatch(repo_id, p) for p in include):
            continue
        if exclude and any(fnmatch.fnmatch(repo_id, p) for p in exclude):
            continue
        repos.append(repo_id)

    if limit is not None:
        repos = repos[:limit]

    if not repos:
        console.print("[yellow]No repos matched after filtering.[/yellow]")
        return

    for repo_id in repos:
        console.print(f"Pulling {repo_id} ...")
        try:
            pull(
                config,
                repo_id,
                revision=None,
                dry_run=dry_run,
                workers=workers,
                fallback=fallback,
            )
        except PullError as e:
            console.print(f"[red]  {e}[/red]")


def _download_and_verify_blob(backend: StorageBackend, blob_hash: str, blob_path: Path) -> None:
    """Atomically download a blob and verify its sha256.

    Writes to ``<blob_path>.tmp`` first; renames into place only if the hash
    matches. An interrupted download cannot leave a corrupt blob with the
    correct filename.
    """
    tmp_path = blob_path.with_suffix(blob_path.suffix + ".tmp")
    try:
        backend.download_file(f"blobs/{blob_hash}", tmp_path)

        # Verify hash for sha256-length names (64 hex chars). Sha1 blobs (40 chars)
        # are git-stored small files where the name is the git object hash —
        # we can't sha256-verify those.
        if len(blob_hash) == 64:
            actual = sha256_file(tmp_path)
            if actual != blob_hash:
                tmp_path.unlink(missing_ok=True)
                raise PullError(f"Hash mismatch for blob {blob_hash[:16]}...: got {actual[:16]}...")

        tmp_path.replace(blob_path)
    except BaseException:
        # Includes KeyboardInterrupt / SystemExit so a Ctrl+C also cleans up.
        tmp_path.unlink(missing_ok=True)
        raise


def _resolve_ref(backend: StorageBackend, repo_id: str, ref: str) -> str | None:
    """Return the commit hash at ``refs/<repo>/<ref>``, or None if absent.

    Auth / endpoint / 5xx errors are re-raised as ``StorageError`` so they
    aren't silently masked as "ref missing".
    """
    key = ref_key(repo_id, ref)
    try:
        return backend.download_bytes(key).decode().strip()
    except StorageError as e:
        if e.code in NOT_FOUND_CODES:
            return None
        raise


def _fetch_manifest(backend: StorageBackend, repo_id: str, revision: str) -> Manifest | None:
    key = manifest_key(repo_id, revision)
    try:
        data = backend.download_bytes(key)
    except StorageError as e:
        if e.code in NOT_FOUND_CODES:
            return None
        raise
    return Manifest.from_json(data.decode())


def _create_link(target: Path, link: Path) -> None:
    """Create relative symlink, falling back to hardlink or copy on Windows."""
    try:
        # Use relative symlinks like huggingface_hub does.
        rel_target = os.path.relpath(target, link.parent)
        link.symlink_to(rel_target)
    except OSError:
        try:
            os.link(target, link)
        except OSError:
            shutil.copy2(target, link)
