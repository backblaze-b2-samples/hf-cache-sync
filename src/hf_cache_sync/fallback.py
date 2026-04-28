"""HF-hub fallback path for ``pull --fallback hf-hub``.

Only fires for *transient* failures (5xx, connection errors). Auth and config
errors deliberately bubble up so users actually fix the misconfiguration
instead of silently routing around it.

The huggingface_hub dependency is loaded lazily — installs that don't opt into
the ``[fallback]`` extra never pay the import cost or grow the dep graph.
"""

from __future__ import annotations

import logging

import click
from rich.console import Console

from hf_cache_sync.cache import DIR_PREFIX_FROM_TYPE
from hf_cache_sync.config import AppConfig
from hf_cache_sync.storage import StorageError

console = Console()
log = logging.getLogger(__name__)


def should_fallback(err: BaseException) -> bool:
    """Return True iff ``err`` is the kind of failure we'd rather reroute
    around than surface — i.e. transient remote unreachability, never auth.
    """
    if isinstance(err, StorageError):
        if err.auth_failure:
            return False
        return err.transient
    return False


def pull_via_hf_hub(
    config: AppConfig,
    repo_id: str,
    revision: str | None,
    repo_type: str = "model",
) -> None:
    """Hydrate the local HF cache by delegating to ``huggingface_hub.snapshot_download``.

    huggingface_hub manages its own ``blobs/`` and ``snapshots/`` layout under
    the same root, so any partial blobs we already wrote stay reusable — they
    just won't be linked into the new snapshot dir until hf-hub matches them
    by hash on its next pass. We point it at the same cache root so subsequent
    `hf-cache-sync push`/`pull` see what hf-hub wrote.
    """
    try:
        from huggingface_hub import snapshot_download
    except ImportError as e:
        raise click.ClickException(
            "huggingface_hub is not installed. Install it with: "
            "pip install 'hf-cache-sync[fallback]'"
        ) from e

    # huggingface_hub expects HF_HOME/HF_HUB_CACHE-style root. Our config's
    # `hf_cache_dir` already points at the `hub/` subfolder; that's what
    # snapshot_download's `cache_dir=` param wants.
    console.print(
        f"[yellow]Falling back to Hugging Face Hub for {repo_id}"
        f"{'@' + revision[:12] if revision else ''}[/yellow]"
    )
    snapshot_download(
        repo_id=repo_id,
        revision=revision,
        cache_dir=str(config.hf_cache_dir),
        repo_type=repo_type,
    )


# Map our internal repo_type values to huggingface_hub's expected literals.
# (We use "model" | "dataset" | "space"; hf-hub uses the same.)
_HF_TYPES = set(DIR_PREFIX_FROM_TYPE.keys())


def normalize_repo_type(repo_type: str) -> str:
    return repo_type if repo_type in _HF_TYPES else "model"
