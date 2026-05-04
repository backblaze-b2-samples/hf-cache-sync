# Contributing to hf-cache-sync

Thanks for your interest. Bug reports, fixes, and small features are welcome.

## Development setup

```bash
git clone https://github.com/backblaze-labs/hf-cache-sync.git
cd hf-cache-sync
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
```

## Running tests, lint, and type checks

```bash
pytest                  # run the suite (52+ tests, 90%+ coverage expected)
pytest --cov=hf_cache_sync --cov-report=term-missing
ruff check .            # lint
ruff format --check .   # format check
mypy src                # type check
```

CI runs all of these on Python 3.9–3.13 against every PR.

## Pull requests

- Branch off `main`. One logical change per PR.
- Add or update tests alongside behavior changes — no skipping.
- Run `pytest`, `ruff check`, `ruff format`, and `mypy` locally before pushing.
- Use [Conventional Commits](https://www.conventionalcommits.org/) for the PR title (`fix:`, `feat:`, `refactor:`, `docs:`, etc.).
- Keep the README and `CHANGELOG.md` in sync with user-visible changes.

## Reporting bugs

Open a GitHub issue with:
- the command you ran,
- expected vs actual behavior,
- `hf-cache-sync --version`, Python version, OS, and S3 endpoint vendor (B2, AWS, MinIO, etc.).

Never include credentials in issues. The `.hf-cache-sync.yaml` is excluded by `.gitignore` for that reason.

## Scope

The tool intentionally stays focused on syncing the HF hub cache to S3-compatible storage. Out-of-scope: alternate cache layouts, training-checkpoint sync, dataset transformations.
