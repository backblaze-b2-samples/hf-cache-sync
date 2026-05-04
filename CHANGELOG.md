# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Full env-driven configuration: `B2_ENDPOINT`, `B2_BUCKET`, `B2_REGION`, `B2_APPLICATION_KEY_ID`, `B2_APPLICATION_KEY` (and AWS-style aliases) can now drive the tool with no YAML required. A `.env.example` file ships at the repo root.
- Credential source detection: `doctor` now reports the resolved source explicitly (`source=b2_env`, `source=aws_env`, or `source=config`) so users can tell at a glance which credential path was taken.
- `doctor` output now surfaces resolved Endpoint, Bucket, and Region as separate informational rows.

### Changed
- Credential precedence is now env-first: B2 env vars > AWS env vars > YAML. Env credentials override YAML even when YAML has values set, so a `.env` file is sufficient for env-only setups (e.g. CI). Storage settings (`B2_ENDPOINT`/`B2_BUCKET`/`B2_REGION`) likewise override their YAML counterparts when set.
- Each credential pair is treated atomically — a partial pair (e.g. only `B2_APPLICATION_KEY_ID`) is skipped entirely so a B2 id can never silently combine with an AWS secret.
- `doctor` command — preflight checks for config, credentials, bucket reachability, read/write permission, and HF cache dir presence. Each check runs independently and prints a ✓/✗ summary; non-zero exit on any failure.
- `diff` command — per-revision comparison of local cache vs remote bucket (in-sync / local-only / remote-only).
- `list --remote` flag — list repos available in remote storage (single paginator, no manifest body downloads).
- `watch` command [experimental] — daemon that auto-pushes new blobs when the HF cache writes them. Uses `watchdog`, subscribes to atomic-rename events only, idle-debounces, and holds a lock file at `<cache_dir>/.hf-cache-sync.lock` to serialize against manual `push`.
- `pull --fallback hf-hub` — on transient remote failures (5xx, network), fall back to `huggingface_hub.snapshot_download`. Auth errors still surface.
- Optional extras: `[fallback]` (huggingface_hub), `[watch]` (watchdog).
- `examples/github-action.yml` — copy-pasteable CI workflow for prewarming an HF cache.
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
- Boto3 `ClientError` and `EndpointConnectionError` are now translated to a humanized `StorageError` with actionable hints (bad creds, wrong endpoint, missing bucket, region mismatch, transient outage) before they reach the user. The original error chain is preserved via `__cause__` for debugging.
- `NoSuchBucket` is no longer treated as "key not found" — it now surfaces as a config error instead of being silently masked as "manifest not found".

## [0.1.0] - 2025-02-24

Initial alpha release.

### Added
- `push`, `pull`, `pull-all`, `prune`, `status`, `list`, `init` CLI commands.
- S3-compatible storage backend with Backblaze B2 user-agent.
- Content-addressed blob layout, JSON manifests per revision, ref tracking.
- Xet-pointer detection and gated-license heuristic.
- LRU eviction with orphan-blob cleanup.
- Per-team bucket prefix.
