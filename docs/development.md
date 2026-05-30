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
- `docker/patch_windninja_generic_warp.py`

```bash
./deploy/gcp/mwn.sh build-local
```

Herbie support changes the Docker image because it adds Herbie, cfgrib, xarray,
netCDF4, and ecCodes. Before publishing a GHCR image for a Herbie change:

1. Run the Python test suite.
2. Rebuild locally with `./deploy/gcp/mwn.sh build-local`.
3. Run `./deploy/gcp/mwn.sh check` and `./deploy/gcp/mwn.sh smoke --keep-temp`.
4. Run at least these opt-in Herbie smokes on a small domain:

```bash
./deploy/gcp/mwn.sh run --weather-source herbie --model HRRR --hours 1 --keep-temp --no-upload
./deploy/gcp/mwn.sh run --weather-source herbie --model RRFS --hours 1 --keep-temp --no-upload
./deploy/gcp/mwn.sh run --weather-source herbie --model GFS --hours 1 --keep-temp --no-upload
```

Publish a new `ghcr.io/austfi/mountain-windninja:<version>` only after those
pass. Before making Herbie the default, also compare native HRRR against Herbie
HRRR on a small domain and confirm the output is close enough for operations.
Then update `DEFAULT_REMOTE_IMAGE` and operator docs to reference the new
version where they use a published GHCR image.

When expanding the Herbie model list, do not add a template directly to the
public `HERBIE_MODEL_MAP` just because Herbie can find it. First verify the
field access pattern:

- NCEP and ECMWF indexed files: inspect `H.inventory(search=...)` and use raw,
  anchored regex strings that match the intended surface layer.
- ECCC single-message files: use Herbie `variable` and `level` kwargs, not
  regex search, because those products do not provide index files.
- Multi-step xarray datasets: confirm the adapter selects the requested
  `valid_time` before writing the WindNinja generic NetCDF.
- Regional models: run a domain-overlap check so Alaska or Canada grids cannot
  silently feed a Colorado domain.

Only promote a candidate model after the source-only matrix and a one-hour VM
run pass, then publish a new GHCR image version and update the docs in the same
PR.

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
