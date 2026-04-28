"""``hf-cache-sync watch`` — auto-push new blobs as the HF cache writes them.

**Experimental.** The watchdog-based observer is platform-sensitive (FSEvents
on macOS coalesces events; inotify on Linux fires per-event; Windows behaves
differently again). The current implementation:

* Subscribes to *moved* events only — i.e. the atomic rename that completes a
  download. Created/modified events on the in-progress ``.incomplete`` files
  are deliberately ignored, so we don't push partial blobs.
* Uses an *idle* debounce: the timer resets on every relevant event, so the
  push only fires after activity has been quiet for ``debounce_seconds``.
  This handles 20 GB downloads correctly (events keep coming as long as the
  rename is happening) at the cost of a delay between "download done" and
  "push started".
* Holds a lock file at ``<hf_cache_dir>/.hf-cache-sync.lock`` so a manual
  ``hf-cache-sync push`` running in parallel does not produce overlapping
  uploads.

There is no way to *prove* a download is complete from filesystem events
alone; the lock file plus rename-only subscription is best-effort. If you
need stronger guarantees, run ``push`` manually after each model download.
"""

from __future__ import annotations

import logging
import os
import threading
import time
from pathlib import Path

from rich.console import Console

from hf_cache_sync.config import AppConfig

console = Console()
log = logging.getLogger(__name__)

DEFAULT_DEBOUNCE_SECONDS = 30.0
LOCK_FILENAME = ".hf-cache-sync.lock"


def _try_lock(lock_path: Path) -> bool:
    """Atomic ``O_CREAT | O_EXCL`` lock file. Returns False if held elsewhere."""
    try:
        fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
    except FileExistsError:
        return False
    os.write(fd, f"{os.getpid()}\n".encode())
    os.close(fd)
    return True


def _release_lock(lock_path: Path) -> None:
    import contextlib

    with contextlib.suppress(FileNotFoundError):
        lock_path.unlink()


def watch(
    config: AppConfig,
    *,
    debounce_seconds: float = DEFAULT_DEBOUNCE_SECONDS,
    workers: int = 8,
) -> None:
    """Block, watching the HF cache and pushing newly-completed blobs.

    Stops cleanly on Ctrl+C.
    """
    import click

    cache_dir: Path = config.hf_cache_dir
    if not cache_dir.is_dir():
        raise click.ClickException(f"HF cache dir not found: {cache_dir}")

    try:
        from watchdog.events import FileMovedEvent, FileSystemEventHandler
        from watchdog.observers import Observer
    except ImportError as e:
        raise click.ClickException(
            "watchdog is not installed. Install it with: pip install 'hf-cache-sync[watch]'"
        ) from e

    lock_path = cache_dir / LOCK_FILENAME

    console.print(
        "[yellow]hf-cache-sync watch is experimental.[/yellow] "
        "Subscribing to file-rename events only; "
        f"debounce={debounce_seconds:.0f}s. Press Ctrl+C to stop."
    )
    console.print(f"Watching: {cache_dir}")

    push_lock = threading.Lock()
    timer: list[threading.Timer | None] = [None]
    last_event_at: list[float] = [0.0]
    stop_event = threading.Event()

    def _do_push() -> None:
        """Run a push. Holds the file lock for the duration."""
        if not push_lock.acquire(blocking=False):
            log.debug("push already in progress in this process; skipping")
            return
        try:
            if not _try_lock(lock_path):
                log.debug("another hf-cache-sync holds the lock; skipping this debounce window")
                return
            try:
                from hf_cache_sync.push import push as do_push

                console.print("[cyan]watch: triggering push…[/cyan]")
                do_push(config, workers=workers)
            finally:
                _release_lock(lock_path)
        finally:
            push_lock.release()

    def _arm_timer() -> None:
        """Reset / start the idle-debounce timer."""
        if timer[0] is not None:
            timer[0].cancel()
        t = threading.Timer(debounce_seconds, _do_push)
        t.daemon = True
        t.start()
        timer[0] = t

    class _Handler(FileSystemEventHandler):
        def on_moved(self, event: FileMovedEvent) -> None:
            # Only blob renames matter — snapshots/refs are derived state.
            dest = getattr(event, "dest_path", "")
            if "/blobs/" not in dest and "\\blobs\\" not in dest:
                return
            last_event_at[0] = time.time()
            log.debug("rename observed: %s", dest)
            _arm_timer()

    observer = Observer()
    observer.schedule(_Handler(), str(cache_dir), recursive=True)
    observer.start()
    try:
        # Block on the stop_event so Ctrl+C is responsive.
        while not stop_event.is_set():
            stop_event.wait(timeout=1.0)
    except KeyboardInterrupt:
        console.print("\n[yellow]watch: stopping…[/yellow]")
    finally:
        observer.stop()
        observer.join(timeout=5)
        if timer[0] is not None:
            timer[0].cancel()
        _release_lock(lock_path)
