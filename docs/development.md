# Development

Normal users should run through Docker. Local Python overrides are only for
developing scripts and tests.

## Python Environment

```bash
python3 -m venv .venv
. .venv/bin/activate
python -m pip install -r requirements-dev.txt
python -m pytest -q
python -m ruff check .
```

## Local Runtime Overrides

`config/runtime.env.example` is Docker-first. For local script development,
override these values in `config/runtime.env`:

```env
MWN_PYTHON_BIN=.venv/bin/python
MWN_WINDNINJA_CLI=/path/to/WindNinja_cli
MWN_OPENFOAM_BASHRC=/path/to/openfoam/etc/bashrc
```

Do not commit machine-specific paths.

## Rebuild Rules

No rebuild needed for changes under `scripts/`, `config/`, or `docker/`; these
directories are bind-mounted into the container.

Rebuild after changes to:

- `Dockerfile`
- `requirements.txt`
- compiled WindNinja/OpenFOAM/GDAL behavior
- `docker/patch_windninja_public_pastcast.py`

```bash
./deploy/gcp/mwn.sh build-local
```

## Tests

Fast local checks:

```bash
python -m pytest -q
python -m ruff check .
```

Container readiness checks:

```bash
./deploy/gcp/mwn.sh check
./deploy/gcp/mwn.sh smoke --keep-temp
```

## Worktree Hygiene

Generated outputs are ignored and should stay out of commits:

- `runtime/`
- `static_data/`
- `.pytest_cache/`
- `.ruff_cache/`
- `__pycache__/`
- `config/runtime.env`

Source inputs are intentionally trackable:

- `config/stations/*.csv`
- `config/studies/*.json`

Before committing, remove stale ignored artifacts from the repo root:

```bash
./deploy/gcp/mwn.sh clean
find runtime -mindepth 1 -maxdepth 1 -exec rm -rf {} +
rm -rf .pytest_cache .ruff_cache
mkdir -p runtime/archives runtime/forcing runtime/forecasts runtime/logs runtime/state runtime/temp runtime/validation
find . -path ./.git -prune -o -path ./.venv -prune -o -name '__pycache__' -type d -prune -exec rm -rf {} +
find . -path ./.git -prune -o -path ./.venv -prune -o -name '.DS_Store' -type f -delete
```

For a validation handoff, also remove WindNinja weather archive caches that can
land beside terrain files:

```bash
find static_data -maxdepth 1 -type d -name 'PASTCAST-*' -exec rm -rf {} +
```

Do not remove `config/runtime.env`, `.venv/`, or non-reproducible terrain files
such as `static_data/*.tif`, `static_data/*.lcp`, and `static_data/*.prj`
unless you have intentionally backed them up outside the repo.
