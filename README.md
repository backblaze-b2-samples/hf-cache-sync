# hf-cache-sync — Hugging Face Cache Sync for Backblaze B2 & S3

[![PyPI](https://img.shields.io/pypi/v/hf-cache-sync.svg)](https://pypi.org/project/hf-cache-sync/)
[![Python](https://img.shields.io/pypi/pyversions/hf-cache-sync.svg)](https://pypi.org/project/hf-cache-sync/)
[![CI](https://github.com/backblaze-labs/hf-cache-sync/actions/workflows/ci.yml/badge.svg)](https://github.com/backblaze-labs/hf-cache-sync/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

> Sync your Hugging Face model cache (`~/.cache/huggingface/hub`) to Backblaze B2 or any S3-compatible object storage, share it across machines and CI runners, and cap local disk usage with LRU eviction.

## What is hf-cache-sync? A shared Hugging Face cache for ML teams

`hf-cache-sync` is a CLI that turns the local Hugging Face Hub cache into a two-tier cache: local disk plus a shared remote bucket on Backblaze B2 (or any S3-compatible store such as AWS S3 or Cloudflare R2). One machine downloads a model from the Hugging Face Hub, pushes the content-addressed blobs and revision manifests to the bucket, and every other machine — workstation, GPU cluster node, or CI runner — hydrates from the bucket instead of re-downloading from the Hub.

Built for ML teams and CI/CD pipelines that waste bandwidth and disk re-fetching the same 5–30 GB transformer, diffusion, or LLM weights on every machine.

### Problem

Hugging Face models and datasets are cached locally. Disks fill up fast, teams re-download identical artifacts on every machine, CI runners fetch the same weights every run, and Windows symlink limitations cause blob duplication. There is no native way to back the cache with shared object storage.

### Solution

hf-cache-sync uploads content-addressed blobs and revision manifests to a Backblaze B2 bucket (or any S3-compatible store). Other machines pull from the bucket to reconstruct the local cache. An LRU prune command enforces a local disk budget by evicting old revisions.

### Who is hf-cache-sync for?

- ML teams (5–50 engineers) sharing Hugging Face models across workstations
- CI/CD pipelines that repeatedly download the same model weights from the Hub
- GPU cluster operators preloading models across training/inference nodes
- Solo developers hitting local disk limits from accumulated `~/.cache/huggingface` data

## Key features

- **Push** — Upload local cache blobs, manifests, and refs to remote storage. Content-addressed by SHA-256; existing blobs are skipped.
- **Pull** — Hydrate local cache from remote. Downloads blobs, reconstructs snapshot directories with symlinks. Hash-verified on download.
- **Pull-all** — Pull every model available in the remote bucket.
- **Prune** — LRU eviction to stay within a local disk budget. Evicts by revision, preserves active refs, cleans orphaned blobs.
- **Team namespacing** — Per-team prefix isolation within a single bucket. Read-only keys for devs, read-write for CI.
- **Windows fallback** — Symlink-first, with automatic hardlink/copy fallback when symlinks are unavailable.

## Architecture

```
[Local HF Cache] --push--> [Backblaze B2 Bucket]
                 <--pull--
```

| Component | Description | Path |
|-----------|-------------|------|
| CLI | Click-based command interface | `src/hf_cache_sync/cli.py` |
| Cache scanner | Reads local HF hub cache structure (blobs, snapshots, refs) | `src/hf_cache_sync/cache.py` |
| Storage backend | Backblaze B2 / S3-compatible client via boto3. Sets `b2ai-hfcache` user-agent for B2 endpoints. | `src/hf_cache_sync/storage.py` |
| Manifest | JSON manifest per repo/revision mapping files to blob hashes | `src/hf_cache_sync/manifest.py` |
| Push | Uploads missing blobs + manifests + refs | `src/hf_cache_sync/push.py` |
| Pull | Downloads blobs, reconstructs snapshots | `src/hf_cache_sync/pull.py` |
| Prune | LRU eviction + orphan blob cleanup | `src/hf_cache_sync/prune.py` |

### Remote storage layout

```
blobs/<sha256>                          # content-addressed model files
manifests/<repo>@<revision>.json        # file list per revision
refs/<repo>/<ref>                       # ref -> commit hash
```

## Quick Start

Prerequisites:
- Python >= 3.9
- A [Backblaze B2](https://www.backblaze.com/sign-up/ai-cloud-storage?utm_source=github&utm_medium=referral&utm_campaign=ai_artifacts&utm_content=hfcache) bucket (or any S3-compatible store)
- B2 application key ID and key (or `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` env vars)

```bash
pip install hf-cache-sync

# Option A — env-only (recommended for CI/CD and quick demos):
cp .env.example .env
# edit .env with your Backblaze B2 / S3 settings
export $(grep -v '^#' .env | xargs)

# Option B — YAML config:
hf-cache-sync init          # creates .hf-cache-sync.yaml
# edit .hf-cache-sync.yaml with your bucket details

hf-cache-sync doctor        # confirm config + connectivity
hf-cache-sync push          # upload local cache to remote
hf-cache-sync pull mistralai/Mistral-7B-v0.1   # hydrate on another machine
hf-cache-sync prune --max-gb 50                 # enforce disk budget
```

## Step-by-Step Setup

### 1. Install

```bash
pip install hf-cache-sync
```

### 2. Create a Backblaze B2 Bucket

1. [Sign up for Backblaze B2](https://www.backblaze.com/sign-up/ai-cloud-storage?utm_source=github&utm_medium=referral&utm_campaign=ai_artifacts&utm_content=hfcache) — the AI Cloud Storage onboarding flow
2. [Create a bucket](https://www.backblaze.com/docs/cloud-storage-create-and-manage-buckets) (private recommended)
3. [Create an application key](https://www.backblaze.com/docs/cloud-storage-create-and-manage-app-keys) scoped to that bucket
4. Note the key ID, the key, and the bucket's Backblaze B2 S3-compatible endpoint (e.g. `https://s3.us-west-000.backblazeb2.com`)

### 3. Configure

```bash
hf-cache-sync init
```

Edit `.hf-cache-sync.yaml`:

```yaml
storage:
  endpoint: https://s3.us-west-000.backblazeb2.com
  bucket: team-hf-cache
  region: us-west-000
  access_key: ""   # B2 application key ID
  secret_key: ""   # B2 application key

cache:
  max_local_gb: 50
  sync_xet: false

team:
  prefix: ""
  allow_gated: false
```

Or skip the YAML entirely and run from environment variables. A starter
[`.env.example`](.env.example) lives at the repo root:

```bash
cp .env.example .env
# edit .env with your values
export $(grep -v '^#' .env | xargs)
```

Supported variables:

| Variable                | Purpose                          |
|-------------------------|----------------------------------|
| `B2_APPLICATION_KEY_ID` | B2 application key ID            |
| `B2_APPLICATION_KEY`    | B2 application key (secret)      |
| `B2_ENDPOINT`           | Backblaze B2 / S3 endpoint URL   |
| `B2_BUCKET`             | Bucket name                      |
| `B2_REGION`             | Bucket region                    |
| `AWS_ACCESS_KEY_ID`     | AWS-style credential ID fallback |
| `AWS_SECRET_ACCESS_KEY` | AWS-style credential secret      |

**Precedence (highest first):**
1. `B2_APPLICATION_KEY_ID` + `B2_APPLICATION_KEY` — credentials
2. `AWS_ACCESS_KEY_ID` + `AWS_SECRET_ACCESS_KEY` — credentials
3. YAML `storage.access_key` / `storage.secret_key` — credentials
4. `B2_ENDPOINT` / `B2_BUCKET` / `B2_REGION` — storage settings (override YAML)
5. YAML `storage.endpoint` / `storage.bucket` / `storage.region`

Env credentials always override YAML, even when YAML has values set. Each
credential pair is treated atomically — a partial B2 pair (e.g. only
`B2_APPLICATION_KEY_ID`) is ignored entirely and the next source is tried.

Run `hf-cache-sync doctor` to see exactly which source was picked
(`source=b2_env`, `source=aws_env`, or `source=config`).

### 4. Push Your Local Cache

```bash
hf-cache-sync push
```

### 5. Pull on Another Machine

```bash
hf-cache-sync pull mistralai/Mistral-7B-v0.1
hf-cache-sync pull-all   # pull everything available
```

### 6. Prune Old Revisions

```bash
hf-cache-sync prune --max-gb 50
```

## Configuration

| Variable | Description | Default | Required |
|----------|-------------|---------|----------|
| `storage.endpoint` | Backblaze B2 / S3-compatible endpoint URL | — | Yes |
| `storage.bucket` | Bucket name | — | Yes |
| `storage.region` | Bucket region | — | Yes |
| `storage.access_key` | Access key (or `AWS_ACCESS_KEY_ID` env) | — | Yes |
| `storage.secret_key` | Secret key (or `AWS_SECRET_ACCESS_KEY` env) | — | Yes |
| `cache.max_local_gb` | Local disk budget in GB for prune | `50` | No |
| `cache.sync_xet` | Sync xet-format blobs | `false` | No |
| `team.prefix` | Namespace prefix within the bucket | `""` | No |
| `team.allow_gated` | Allow pushing gated model blobs | `false` | No |

Config file search order: `./.hf-cache-sync.yaml`, then `~/.hf-cache-sync.yaml`.

## Common Tasks

```bash
hf-cache-sync init                        # Generate config template
hf-cache-sync doctor                      # Verify config, creds, bucket, cache dir
hf-cache-sync push                        # Upload cache to remote
hf-cache-sync push --dry-run              # Preview without uploading
hf-cache-sync push --workers 16           # Parallelize uploads (default 8)
hf-cache-sync pull <repo_id>              # Hydrate a specific model
hf-cache-sync pull <repo_id> -r <hash>    # Hydrate a specific revision
hf-cache-sync pull <repo_id> --fallback hf-hub  # Fall back to HF hub if remote is unreachable
hf-cache-sync pull-all                    # Hydrate all remote models
hf-cache-sync pull-all --include 'org/*'  # Filter by fnmatch glob
hf-cache-sync pull-all --limit 5          # Cap the number of repos
hf-cache-sync prune --max-gb 50           # Evict old revisions
hf-cache-sync prune --dry-run             # Preview eviction
hf-cache-sync status                      # Show cache size and config
hf-cache-sync list                        # List local cached repos
hf-cache-sync list --remote               # List repos available in the bucket
hf-cache-sync diff                        # Show local vs remote per revision
hf-cache-sync watch                       # [experimental] auto-push new blobs
hf-cache-sync -v <command>                # Debug logging
```

Failed commands (auth errors, missing manifests, hash mismatches) exit with a
non-zero status so they can be detected in CI. `pull --fallback hf-hub` retries
through `huggingface_hub` on transient remote failures (it never papers over
auth errors — those still surface).

### Optional extras

| Extra      | Adds                                       | Used by                  |
|------------|--------------------------------------------|--------------------------|
| `[fallback]` | `huggingface_hub`                        | `pull --fallback hf-hub` |
| `[watch]`    | `watchdog`                               | `hf-cache-sync watch`    |
| `[dev]`      | pytest, ruff, mypy, moto, type stubs     | local development        |

```bash
pip install "hf-cache-sync[fallback]"          # CI runners
pip install "hf-cache-sync[watch]"             # workstations
pip install "hf-cache-sync[fallback,watch]"    # both
```

### CI integration

A ready-to-use GitHub Actions workflow lives at
[`examples/github-action.yml`](examples/github-action.yml). It runs `doctor`
to fail fast on misconfiguration and uses `--fallback hf-hub` so jobs survive
B2 outages.

## Testing

```bash
pip install -e ".[dev]"
pytest                  # 60+ tests, ~90% coverage
ruff check .            # lint
ruff format --check .   # format check
mypy src                # type check
```

CI runs the full matrix on Python 3.9–3.13 against every PR.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). Bug reports and small features are welcome.

## Troubleshooting

| Symptom | Fix |
|---------|-----|
| `hf-cache-sync push` uploads nothing | Verify HF cache exists at `~/.cache/huggingface/hub` and contains model directories |
| Authentication errors on push/pull | Check `access_key` / `secret_key` in config, or `B2_APPLICATION_KEY_ID` / `B2_APPLICATION_KEY` (or `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`) env vars |
| Hash mismatch on pull | Blob was corrupted in transit or storage. Delete the remote blob and re-push from a known-good machine. |
| Prune removes nothing | All revisions are pointed to by active refs. Detached revisions are evicted first. |
| Windows symlink errors | hf-cache-sync falls back to hardlinks, then file copies automatically. No action needed. |

## Security

- Gated-license models are skipped by default via a best-effort heuristic
  (LICENSE / USE_POLICY content is scanned for keywords like "agreement",
  "you must accept"). The check is **not** a substitute for honoring license
  terms — review what your bucket contains before sharing access. Override
  with `allow_gated: true` if you have the rights.
- All sha256-named blobs are hash-verified on pull. Downloads land in a
  `.tmp` file and are renamed atomically only after verification, so an
  interrupted pull can never leave a corrupt blob behind.
- Use B2 application keys scoped to a single bucket. Create separate read-only
  keys for distribution; restrict write keys to CI runners.
- Never commit `.hf-cache-sync.yaml` with credentials — `.gitignore` excludes
  it by default. Prefer `B2_APPLICATION_KEY_ID` / `B2_APPLICATION_KEY` (or
  `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`) env vars in CI.

## License

MIT — See [LICENSE](LICENSE) for details.
