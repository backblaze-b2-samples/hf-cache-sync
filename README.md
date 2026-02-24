# hf-cache-sync

Shared Hugging Face Cache Manager with S3 Backend and LRU Eviction.

## Fix Hugging Face Disk Full Errors

Hugging Face models and datasets cached under `~/.cache/huggingface` fill up disks fast — 5-30 GB per model. Teams re-download the same models on every machine, CI runners fetch identical artifacts repeatedly, and Windows symlink issues cause duplication.

**hf-cache-sync** introduces a two-tier cache: local cache + shared S3-compatible object storage.

- Push local cache blobs to a shared bucket
- Hydrate any machine from the shared cache
- LRU eviction enforces local disk budgets
- Team namespacing with prefix isolation
- Windows symlink fallback (hardlink/copy)

## Install

```bash
pip install hf-cache-sync
```

## Quick Start

```bash
# Create config
hf-cache-sync init

# Edit .hf-cache-sync.yaml with your bucket details

# Push local cache to remote
hf-cache-sync push

# Pull a model on another machine
hf-cache-sync pull mistralai/Mistral-7B-v0.1

# Pull all available models
hf-cache-sync pull-all

# Enforce 50 GB local budget
hf-cache-sync prune --max-gb 50

# Check cache status
hf-cache-sync status

# List cached repos
hf-cache-sync list
```

## Configuration

Create `~/.hf-cache-sync.yaml`:

```yaml
storage:
  endpoint: https://s3.us-west-000.backblazeb2.com
  bucket: team-hf-cache
  region: us-west-000
  access_key: ""
  secret_key: ""

cache:
  max_local_gb: 50
  sync_xet: false

team:
  prefix: org1/
  allow_gated: false
```

## How It Works

### Push
Scans local HF hub cache, uploads missing blobs (content-addressed by SHA-256), uploads revision manifests and refs.

### Pull
Fetches the manifest for a repo/revision, downloads missing blobs, reconstructs the snapshot directory with symlinks (or hardlink/copy fallback on Windows).

### Prune
Computes total local cache size, evicts least-recently-accessed revisions (skipping active refs), cleans orphaned blobs.

## Use S3-Compatible Storage as Hugging Face Cache Backend

Works with any S3-compatible storage: AWS S3, Backblaze B2, MinIO, Cloudflare R2, etc.

## Shared Hugging Face Cache for Teams

Configure per-team prefixes for namespace isolation. Use read-only keys for developers, write keys for CI/preload jobs.

## LRU Eviction for Hugging Face Models

Set a local disk budget with `max_local_gb`. The prune command evicts old revisions while preserving pinned refs.

## License

MIT
