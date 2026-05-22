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

## ML Offshoot

The residual U-Net research code lives under `ml/residual_unet/`. It is separate
from the operational `mwn.sh` path and should not be required for normal
forecast, terrain, or validation runs.

Install ML dependencies only when working on that path:

```bash
python -m pip install -r ml/residual_unet/requirements.txt
```

Run focused checks:

```bash
python -m ruff check ml/residual_unet tests/test_residual_unet.py
python -m pytest tests/test_residual_unet.py -q
```

See [Residual U-Net ML guide](ml_residual_unet.md) for dataset, Colab, and
HRRR-pair expansion workflows.

## Worktree Hygiene

Generated outputs are ignored and should stay out of commits:

- `runtime/`
- `static_data/`
- `ml/residual_unet/data/processed/`
- `ml/residual_unet/outputs/`
- `ml/residual_unet/colab/`
- `.pytest_cache/`
- `.ruff_cache/`
- `__pycache__/`
- `config/runtime.env`

Source inputs are intentionally trackable:

- `config/stations/*.csv`
- `config/studies/*.json`

Before committing, remove stale ignored artifacts from the repo root only after
confirming no WindNinja/OpenFOAM job is active:

```bash
docker ps
pgrep -af 'WindNinja_cli|daily_run.py|validate-study|gridded_run.py|mwn.sh|ml.residual_unet.hrrr_pair_runs'
./deploy/gcp/mwn.sh clean
rm -rf runtime/temp/*
find runtime/forecasts runtime/forcing runtime/archives runtime/logs -mindepth 1 -maxdepth 1 -exec rm -rf {} +
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
unless you have intentionally backed them up outside the repo. Treat
`runtime/validation/` as result storage; remove old study roots only when they
are no longer needed.
