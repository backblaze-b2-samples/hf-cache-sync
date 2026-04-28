# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- `--workers` flag on `push` and `pull` for concurrent blob transfers (default 8).
- `--dry-run` flag on `push`, `pull`, and `prune`.
- `-v` / `--verbose` flag enables debug-level logging via stdlib `logging`.
- `--limit`, `--include`, `--exclude` filters on `pull-all`.
- Dev tooling: ruff (lint + format), mypy, GitHub Actions CI matrix on Python 3.9–3.13.
- `LICENSE`, `CONTRIBUTING.md`, `CHANGELOG.md`, `.gitignore`.

### Changed
- `push` now lists existing remote blobs once instead of issuing one HEAD per blob.
- `pull` writes blobs atomically (download to `<hash>.tmp`, then rename) so an interrupted pull cannot leave a corrupt blob masquerading as complete.
- `pull_all` now resolves `refs/main` for each repo and pulls via the ref so local `refs/main` is written correctly.
- `prune` accounts budget by unique blob bytes rather than per-revision file totals.
- CLI errors now exit non-zero via `click.ClickException` so CI catches failures.
- `__version__` is sourced from installed package metadata.
- Storage backend now uses standard boto retry mode (`max_attempts=5`).

### Fixed
- `pull` no longer swallows credential / network errors as "manifest not found"; `ClientError` is narrowed and surfaced.
- Hash-mismatch on `pull` deletes only the corrupt blob, preserving content-addressed blobs that may be reused by other revisions.

## [0.1.0] - 2025-02-24

Initial alpha release.

### Added
- `push`, `pull`, `pull-all`, `prune`, `status`, `list`, `init` CLI commands.
- S3-compatible storage backend with Backblaze B2 user-agent.
- Content-addressed blob layout, JSON manifests per revision, ref tracking.
- Xet-pointer detection and gated-license heuristic.
- LRU eviction with orphan-blob cleanup.
- Per-team bucket prefix.
