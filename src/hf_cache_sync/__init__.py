"""hf-cache-sync: Shared Hugging Face Cache Manager with S3 Backend and LRU Eviction."""

from __future__ import annotations

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("hf-cache-sync")
except PackageNotFoundError:
    # Package is not installed (e.g. running from a checkout without `pip install -e .`).
    __version__ = "0.0.0+unknown"
