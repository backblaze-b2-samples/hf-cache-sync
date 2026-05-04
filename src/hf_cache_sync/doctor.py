"""``hf-cache-sync doctor`` — preflight checks for config, creds, and connectivity.

The sentinel object used for the write-permission probe lives under a dedicated
``_hf_cache_sync_doctor/`` prefix so it can never accidentally collide with
``blobs/`` keys (which ``push.py`` enumerates and trusts as a content-addressed
index).
"""

from __future__ import annotations

import secrets
from dataclasses import dataclass
from pathlib import Path

from rich.console import Console
from rich.table import Table

from hf_cache_sync.config import AppConfig, has_env_credentials
from hf_cache_sync.storage import StorageBackend, StorageError

console = Console()

PROBE_PREFIX = "_hf_cache_sync_doctor/"


@dataclass
class CheckResult:
    name: str
    ok: bool
    detail: str = ""
    hint: str = ""


def run_checks(config: AppConfig) -> list[CheckResult]:
    """Run every check independently and return all results.

    Independent checks let us print the full picture rather than short-
    circuiting at the first failure, which is what users actually want when
    debugging a setup.
    """
    results: list[CheckResult] = []

    results.append(_check_required_fields(config))
    results.append(_check_credentials(config))
    results.append(_check_hf_cache_dir(config))

    # Network-bound checks share a backend; only run them when we have enough
    # config to make the attempt meaningful.
    can_reach_storage = bool(config.storage.bucket)
    if can_reach_storage:
        backend = StorageBackend(config)
        bucket_check = _check_bucket_reachable(backend)
        results.append(bucket_check)
        if bucket_check.ok:
            results.append(_check_read_permission(backend))
            results.append(_check_write_permission(backend))
    else:
        results.append(
            CheckResult(
                name="Bucket reachable",
                ok=False,
                detail="Skipped — storage.bucket is not configured.",
                hint="Set storage.bucket in your config or run `hf-cache-sync init`.",
            )
        )

    return results


def doctor(config: AppConfig) -> bool:
    """Run all checks, print a Rich table, return True iff every check passed."""
    results = run_checks(config)
    _print_results(results)
    return all(r.ok for r in results)


def _check_required_fields(config: AppConfig) -> CheckResult:
    missing: list[str] = []
    if not config.storage.bucket:
        missing.append("storage.bucket")
    if not config.storage.region:
        missing.append("storage.region")
    if missing:
        return CheckResult(
            name="Required config fields",
            ok=False,
            detail=f"Missing: {', '.join(missing)}",
            hint="Edit .hf-cache-sync.yaml and fill these in.",
        )
    return CheckResult(
        name="Required config fields",
        ok=True,
        detail=f"bucket={config.storage.bucket}, region={config.storage.region}",
    )


def _check_credentials(config: AppConfig) -> CheckResult:
    has_yaml = bool(config.storage.access_key and config.storage.secret_key)
    has_env = has_env_credentials()
    if not (has_yaml or has_env):
        return CheckResult(
            name="Credentials configured",
            ok=False,
            detail="No access_key/secret_key found in config or env.",
            hint=(
                "Set storage.access_key/secret_key in your config OR export "
                "B2_APPLICATION_KEY_ID/B2_APPLICATION_KEY (or "
                "AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY)."
            ),
        )
    source = "config" if has_yaml else "env"
    return CheckResult(name="Credentials configured", ok=True, detail=f"source={source}")


def _check_hf_cache_dir(config: AppConfig) -> CheckResult:
    cache_dir: Path = config.hf_cache_dir
    if not cache_dir.is_dir():
        return CheckResult(
            name="HF cache dir",
            ok=False,
            detail=f"Not found at {cache_dir}",
            hint=(
                "Run any `huggingface_hub` download to create it, or set "
                "HF_HOME / HF_HUB_CACHE / cache.hf_cache_dir."
            ),
        )
    return CheckResult(name="HF cache dir", ok=True, detail=str(cache_dir))


def _check_bucket_reachable(backend: StorageBackend) -> CheckResult:
    try:
        backend.head_bucket()
    except StorageError as e:
        return CheckResult(
            name="Bucket reachable",
            ok=False,
            detail=str(e),
            hint="Use the hint above to fix the bucket / endpoint / credentials.",
        )
    return CheckResult(name="Bucket reachable", ok=True, detail=backend.bucket)


def _check_read_permission(backend: StorageBackend) -> CheckResult:
    try:
        backend.list_keys("")
    except StorageError as e:
        return CheckResult(
            name="Read permission",
            ok=False,
            detail=str(e),
            hint="Your application key needs s3:ListBucket on this bucket.",
        )
    return CheckResult(name="Read permission", ok=True, detail="ListObjectsV2 OK")


def _check_write_permission(backend: StorageBackend) -> CheckResult:
    """Put-then-delete a sentinel under a dedicated prefix.

    The prefix is intentionally outside ``blobs/`` so a leaked sentinel
    can't pollute ``push``'s prefetched key set. The delete runs in a
    finally so a transient delete failure doesn't leave the object behind
    silently — we surface the leak in the hint.
    """
    sentinel_key = f"{PROBE_PREFIX}probe-{secrets.token_hex(8)}"
    delete_failed = False
    try:
        try:
            backend.upload_bytes(b"hf-cache-sync doctor probe", sentinel_key)
        except StorageError as e:
            return CheckResult(
                name="Write permission",
                ok=False,
                detail=str(e),
                hint="Your application key needs s3:PutObject on this bucket.",
            )
        return CheckResult(name="Write permission", ok=True, detail=f"PutObject {sentinel_key} OK")
    finally:
        try:
            backend.delete(sentinel_key)
        except StorageError:
            delete_failed = True
        if delete_failed:
            console.print(
                f"[yellow]warning: could not delete probe sentinel "
                f"{sentinel_key} — you may need to remove it manually.[/yellow]"
            )


def _print_results(results: list[CheckResult]) -> None:
    table = Table(title="hf-cache-sync doctor")
    table.add_column("", width=2)
    table.add_column("Check", style="cyan")
    table.add_column("Detail", overflow="fold")
    for r in results:
        mark = "[green]✓[/green]" if r.ok else "[red]✗[/red]"
        table.add_row(mark, r.name, r.detail or "—")
    console.print(table)

    failures = [r for r in results if not r.ok]
    if failures:
        console.print()
        for r in failures:
            if r.hint:
                console.print(f"[red]✗[/red] {r.name}: [yellow]{r.hint}[/yellow]")
        console.print()
        console.print(f"[red]{len(failures)} check(s) failed.[/red]")
    else:
        console.print("[green]All checks passed.[/green]")
